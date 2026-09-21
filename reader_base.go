package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
)

type executeQueryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

type readerBase struct {
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
	var a []any
	var text string
	var projection *wildcardProjectionPlan
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
				text = emitSQL(q)
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
						text, a, err = compileStructuredSQL(expanded)
						if err != nil {
							return readerBase{}, fmt.Errorf("failed to compile expanded SQLite projection: %w", err)
						}
						projection = nil
					}
				}
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
	if rb.scanColTypes, err = rb.rows.ColumnTypes(); err != nil {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("failed to read column types: %w", err)
	}
	if len(rb.scanColNames) != len(rb.scanColTypes) {
		_ = rb.rows.Close()
		return rb, fmt.Errorf("length if column names and column types don't match")
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
		rb.colTypes[i] = rb.scanColTypes[sourceIndex]
	}
	return rb, nil
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
