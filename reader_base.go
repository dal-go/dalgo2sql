package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
)

type executeQueryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

// connLease holds the connection one read runs on, from the catalog lookup to the
// last row of its statement. It is given back, once, when the reader is done with the
// rows: closed, or read to its end. It is also given back when the context of the read
// ends, as the pool does for a read of its own: database/sql closes the rows then, and a
// caller that stopped on ctx.Done() need not close the reader to free the connection.
type connLease struct {
	once sync.Once
	// mu is held while a statement is issued on conn and while conn is closed. Closing a
	// *sql.Conn while a statement is being issued on it is a race inside database/sql: the
	// statement can be handed a connection that is already gone, and fail with a nil
	// dereference. With the lock, a statement either runs on the open connection or is
	// refused as database/sql refuses it once the connection is closed (sql.ErrConnDone).
	mu   sync.Mutex
	conn *sql.Conn
	// ctx is the context of the read, which ends the lease (see explain).
	ctx context.Context
	// stopHook unregisters the hook that gives the connection back when the context
	// ends. Only release reads it, and only after the lease is built; the hook itself
	// calls giveBack.
	stopHook func() bool
}

// newConnLease takes conn for the read that runs on ctx.
func newConnLease(ctx context.Context, conn *sql.Conn) *connLease {
	l := &connLease{conn: conn, ctx: ctx}
	l.stopHook = context.AfterFunc(ctx, l.giveBack)
	return l
}

// explainByContext returns the error of a read whose context has ended. A driver whose
// connection was broken by the end of the context says only that the connection is gone
// (driver.ErrBadConn), and so does database/sql once the lease has given the connection
// back (sql.ErrConnDone); the pool never says either of a read whose context ended, it
// says why the read stopped. So an error that is one of these, or wraps one, is the
// context's error when the context has ended, and any other error is left as it is.
func explainByContext(ctx context.Context, err error) error {
	if err != nil && (errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn)) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	return err
}

// explain is explainByContext for the context of the read the lease belongs to, for the
// error of a reader that has the lease; a read on the pool has none, and its errors are
// left as they are.
func (l *connLease) explain(err error) error {
	if l == nil {
		return err
	}
	return explainByContext(l.ctx, err)
}

// query runs one statement of the read on the leased connection. The lease gives the
// connection back when the context of the read ends, and that can happen between two
// statements of one read (the catalog lookup and the statement it compiles); the next
// statement then finds a closed connection, or a driver that reports its connection
// broken. The pool never says that of a read whose context ended, it says why the read
// stopped, so the error is the context's (explainByContext).
func (l *connLease) query(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
	l.mu.Lock()
	rows, err := l.conn.QueryContext(ctx, text, args...)
	l.mu.Unlock()
	if err != nil {
		return nil, explainByContext(ctx, err)
	}
	return rows, nil
}

// giveBack closes the connection, which returns it to the pool, once. Closing waits for
// the rows that ran on the connection, which close themselves when their context ends.
func (l *connLease) giveBack() {
	l.once.Do(func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		_ = l.conn.Close()
	})
}

// streamError is the error of a result that broke in the middle of its stream, as a reader
// returns it from Next. database/sql takes only io.EOF itself for the end of a result,
// so a driver error that wraps io.EOF is the stream's. dal.ErrNoMoreRecords wraps io.EOF
// too, and callers of the readers end a result on any error that matches it, so such an
// error is returned without the chain (its text kept): nothing takes it for the end. The
// error of a read whose context ended is the context's (connLease.explain).
func streamError(lease *connLease, err error) error {
	err = lease.explain(err)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%v", err)
	}
	return err
}

// release gives the connection back to the pool. It is safe on a nil lease (a read on
// the pool holds none) and to call twice. It must follow the close of the rows that
// ran on the connection: closing a connection waits for them.
func (l *connLease) release() {
	if l == nil {
		return
	}
	l.stopHook()
	l.giveBack()
}

// columnTypesOf reads the column types of rows. It is a variable so that a test can end the
// context of a read at the one point between its two lookups on the rows, the names and the
// types: database/sql closes the rows from its own goroutine when the context ends, and which
// lookup it meets first is the scheduler's choice.
var columnTypesOf = func(rows *sql.Rows) ([]*sql.ColumnType, error) { return rows.ColumnTypes() }

type readerBase struct {
	// lease, when set, is the connection the rows run on; the reader releases it when
	// it is done (see connLease).
	lease          *connLease
	rows           *sql.Rows
	colNames       []string
	colTypes       []*sql.ColumnType
	scanColNames   []string
	scanColTypes   []*sql.ColumnType
	visibleIndexes []int
	// baseColumns, when the reader was asked for them and the statement is a select-all
	// over joins, lists the columns of the base source in the order they lead the result.
	baseColumns []string
}

func getReaderBase(ctx context.Context, query dal.Query, execute executeQueryFunc) (readerBase, error) {
	return getReaderBaseWithOptions(ctx, query, execute, DbOptions{})
}

func getReaderBaseWithDialect(ctx context.Context, query dal.Query, execute executeQueryFunc, dialect string) (readerBase, error) {
	return getReaderBaseWithOptions(ctx, query, execute, DbOptions{StructuredQueryDialect: dialect})
}

func getReaderBaseWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions) (readerBase, error) {
	return getReaderBaseFor(ctx, query, execute, options, false, nil)
}

// getReaderBaseFor runs the query. wantBaseColumns asks, for a select-all over joins, for
// the columns of the base source (readerBase.baseColumns), which the SQLite dialect reads
// from its own catalog in one more statement, so it is asked for only by a read that needs
// them. facts, when the typed PostgreSQL compiler runs the read, are the catalog facts of
// the sources of the query that the caller has read already (typedFactsForQuery); nil
// reads them here.
func getReaderBaseFor(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions, wantBaseColumns bool, facts *typedCatalogFacts) (readerBase, error) {
	if err := rejectRawRecursiveStructuredQuery(query); err != nil {
		return readerBase{}, err
	}
	var a []any
	var text string
	var projection *wildcardProjectionPlan
	// askedNames, when set, names each column of the result as the query asked for
	// it (typedStatement.outputs), in place of the name the server returns.
	var askedNames []string
	var baseColumns []string
	switch q := query.(type) {
	case dal.TextQuery:
		text = q.Text()
		args := q.Args()
		a = make([]any, len(args))
		for i, arg := range args {
			// dal.QueryArg{Name, Value} is dalgo's own bind-value shape, not
			// something database/sql understands directly — passing the
			// struct itself as a[i] fails every call with a non-empty Args()
			// ("unsupported type dal.QueryArg, a struct"). A named arg
			// (Name != "") becomes a sql.NamedArg via sql.Named, so an
			// @name/:name/$name placeholder in the query text binds by
			// name; a positional arg (Name == "") passes its Value straight
			// through for ordinary ?/$N placeholders.
			if arg.Name != "" {
				a[i] = sql.Named(arg.Name, arg.Value)
			} else {
				a[i] = arg.Value
			}
		}
	case dal.StructuredQuery:
		var err error
		if projection, err = planWildcardProjection(q); err != nil {
			return readerBase{}, err
		}
		if options.NativeStructuredQueryCompiler != nil {
			fragments, err := translateNativeJoinHints(q.From(), options.NativeJoinHintTranslator)
			if err != nil {
				return readerBase{}, err
			}
			text, a, err = options.NativeStructuredQueryCompiler.CompileNativeStructuredQuery(q, fragments)
			if err != nil {
				return readerBase{}, fmt.Errorf("native structured query compiler: %w", err)
			}
		} else {
			switch options.StructuredQueryDialect {
			case "":
				if text, err = emitSQL(q); err != nil {
					return readerBase{}, err
				}
			case "sqlite":
				var err error
				text, a, err = compileStructuredSQL(q)
				if err != nil {
					return readerBase{}, &access.DeniedError{Decision: access.Decision{Operation: access.Query, Code: access.CodeEnforcementUnsupported, Scope: access.DecisionScopeOperation, Explanation: "structured SQLite query is unsupported"}}
				}
				if projection != nil {
					var names []string
					if names, err = sqliteSourceColumns(ctx, q, execute); err != nil {
						return readerBase{}, fmt.Errorf("failed to inspect SQLite wildcard source: %w", err)
					}
					if expanded, ok := projection.expandSQLiteWildcard(q, names); ok {
						text, a, _ = compileStructuredSQL(expanded)
						projection = nil
					}
				}
				if wantBaseColumns {
					if baseColumns, err = sqliteSourceColumns(ctx, q, execute); err != nil {
						return readerBase{}, fmt.Errorf("failed to inspect SQLite join source: %w", err)
					}
				}
			case "postgres":
				dialect, err := postgresDialectFor(options)
				if err != nil {
					return readerBase{}, err
				}
				// Every source's catalog facts, then the typed compiler. The compiler
				// lists a wildcard's columns from the facts and applies its exclusions
				// itself, so no column is filtered out of the result afterwards. The facts
				// are read through execute, which the caller binds to the connection or
				// transaction the statement runs on.
				var statement typedStatement
				if facts != nil {
					statement, err = compileTypedStatement(q, dialect, *facts)
				} else {
					statement, err = compileTypedRead(ctx, q, dialect, execute)
				}
				if err != nil {
					return readerBase{}, err
				}
				text, a, askedNames, projection, baseColumns = statement.text, statement.args, statement.outputs, nil, statement.baseColumns
			default:
				return readerBase{}, fmt.Errorf("unsupported structured query dialect %q", options.StructuredQueryDialect)
			}
		}
	}

	rows, err := execute(ctx, text, a...)
	if err != nil {
		return readerBase{}, err
	}
	rb := readerBase{
		rows:        rows,
		baseColumns: baseColumns,
	}
	if rb.scanColNames, err = rb.rows.Columns(); err != nil {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("failed to read column names: %w", err)
	}
	if askedNames != nil && len(askedNames) != len(rb.scanColNames) {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("the statement returned %d columns where the query asked for %d", len(rb.scanColNames), len(askedNames))
	}
	// database/sql closes the rows when the context ends, between the lookup of the names
	// and this one, and then answers with no types and the context's error: a read without
	// its types cannot go on, and a result whose columns have no types is never taken for one
	// that has none.
	if rb.scanColTypes, err = columnTypesOf(rb.rows); err != nil {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("failed to read column types: %w", explainByContext(ctx, err))
	}
	rb.visibleIndexes = make([]int, len(rb.scanColNames))
	for i := range rb.visibleIndexes {
		rb.visibleIndexes[i] = i
	}
	if projection != nil {
		if rb.visibleIndexes, err = projection.visibleIndexes(rb.scanColNames); err != nil {
			_ = rb.rows.Close()
			return rb, err
		}
	}
	rb.colNames = make([]string, len(rb.visibleIndexes))
	rb.colTypes = make([]*sql.ColumnType, len(rb.visibleIndexes))
	for i, sourceIndex := range rb.visibleIndexes {
		rb.colNames[i] = rb.scanColNames[sourceIndex]
		if askedNames != nil {
			rb.colNames[i] = askedNames[sourceIndex]
		}
		rb.colTypes[i] = rb.scanColTypes[sourceIndex]
	}
	return rb, nil
}

// rejectRawRecursiveStructuredQuery keeps recursive DTQL evaluation in DALgo's
// generic executor. This adapter only compiles one SQL statement at a time, so
// attempting a nested query here could bypass the framework's planner and
// policy-aware leaf execution.
func rejectRawRecursiveStructuredQuery(query dal.Query) error {
	q, ok := query.(dal.StructuredQuery)
	if !ok || !dal.HasSubquery(q) {
		return nil
	}
	return &access.DeniedError{Decision: access.Decision{
		Operation:   access.Query,
		Effect:      "deny",
		Code:        access.CodeEnforcementUnsupported,
		Scope:       access.DecisionScopeOperation,
		Explanation: "raw dalgo2sql adapter does not support recursive structured queries",
	}}
}

func sqliteSourceColumns(ctx context.Context, q dal.StructuredQuery, execute executeQueryFunc) ([]string, error) {
	source, err := sqliteCollectionSource(q.From().Base())
	if err != nil {
		return nil, err
	}
	// table_xinfo gives the actual identifiers, not SELECT * result labels,
	// which SQLite may prefix when full_column_names is enabled. It also
	// includes generated columns (hidden 2/3), which SELECT * returns. A source in a
	// schema (an attached database) is asked for there: by its name alone, SQLite
	// answers with the first table of that name, which can be another one, with other
	// columns in another order.
	statement, args := "SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid", []any{source.Name()}
	if schema := source.Schema(); schema != "" {
		statement, args = "SELECT name, hidden FROM pragma_table_xinfo(?, ?) ORDER BY cid", []any{source.Name(), schema}
	}
	rows, err := execute(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		var hidden int
		if err := rows.Scan(&name, &hidden); err != nil {
			return nil, err
		}
		if hidden == 0 || hidden == 2 || hidden == 3 { // virtual-table hidden columns (1) are absent from SELECT *
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func (rb readerBase) scanValues() (values []any, err error) {
	rawValues := make([]any, len(rb.scanColNames))
	scanArgs := make([]any, len(rb.scanColNames))
	for i := range rawValues {
		scanArgs[i] = &rawValues[i]
	}
	if err = rb.rows.Scan(scanArgs...); err != nil {
		return nil, err
	}
	values = make([]any, len(rb.visibleIndexes))
	for i, sourceIndex := range rb.visibleIndexes {
		values[i] = rawValues[sourceIndex]
	}
	return values, nil
}
