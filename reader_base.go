package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
)

type executeQueryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

// connLease holds the connection one read runs on, from the catalog lookup to the
// last row of its statement. It is given back, once, when the reader is done with the
// rows: closed, or read to its end.
type connLease struct {
	once sync.Once
	conn *sql.Conn
}

// release gives the connection back to the pool. It is safe on a nil lease (a read on
// the pool holds none) and to call twice. It must follow the close of the rows that
// ran on the connection: closing a connection waits for them.
func (l *connLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() { _ = l.conn.Close() })
}

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
}

func getReaderBase(ctx context.Context, query dal.Query, execute executeQueryFunc) (readerBase, error) {
	return getReaderBaseWithOptions(ctx, query, execute, DbOptions{})
}

func getReaderBaseWithDialect(ctx context.Context, query dal.Query, execute executeQueryFunc, dialect string) (readerBase, error) {
	return getReaderBaseWithOptions(ctx, query, execute, DbOptions{StructuredQueryDialect: dialect})
}

func getReaderBaseWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions) (readerBase, error) {
	if err := rejectRawRecursiveStructuredQuery(query); err != nil {
		return readerBase{}, err
	}
	var a []any
	var text string
	var projection *wildcardProjectionPlan
	// askedNames, when set, names each column of the result as the query asked for
	// it (typedStatement.outputs), in place of the name the server returns.
	var askedNames []string
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
				statement, err := compileTypedRead(ctx, q, dialect, execute)
				if err != nil {
					return readerBase{}, err
				}
				text, a, askedNames, projection = statement.text, statement.args, statement.outputs, nil
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
		rows: rows,
	}
	if rb.scanColNames, err = rb.rows.Columns(); err != nil {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("failed to read column names: %w", err)
	}
	if askedNames != nil && len(askedNames) != len(rb.scanColNames) {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("the statement returned %d columns where the query asked for %d", len(rb.scanColNames), len(askedNames))
	}
	rb.scanColTypes, _ = rb.rows.ColumnTypes()
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
	source := q.From().Base()
	// table_xinfo gives the actual identifiers, not SELECT * result labels,
	// which SQLite may prefix when full_column_names is enabled. It also
	// includes generated columns (hidden 2/3), which SELECT * returns.
	rows, err := execute(ctx, "SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid", source.Name())
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
