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
	rows     *sql.Rows
	colNames []string
	colTypes []*sql.ColumnType
}

func getReaderBase(ctx context.Context, query dal.Query, execute executeQueryFunc) (readerBase, error) {
	return getReaderBaseWithDialect(ctx, query, execute, "")
}

func getReaderBaseWithDialect(ctx context.Context, query dal.Query, execute executeQueryFunc, dialect string) (readerBase, error) {
	var a []any
	var text string
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
		switch dialect {
		case "":
			text = emitSQL(q)
		case "sqlite":
			var err error
			text, a, err = compileStructuredSQL(q)
			if err != nil {
				return readerBase{}, &access.DeniedError{Decision: access.Decision{Operation: access.Query, Code: access.CodeEnforcementUnsupported, Scope: access.DecisionScopeOperation, Explanation: "structured SQLite query is unsupported"}}
			}
		default:
			return readerBase{}, fmt.Errorf("unsupported structured query dialect %q", dialect)
		}
	}

	rows, err := execute(ctx, text, a...)
	if err != nil {
		return readerBase{}, err
	}
	rb := readerBase{
		rows: rows,
	}
	if rb.colNames, err = rb.rows.Columns(); err != nil {
		return rb, fmt.Errorf("failed to read column names: %w", err)
	}
	if rb.colTypes, err = rb.rows.ColumnTypes(); err != nil {
		return rb, fmt.Errorf("failed to read column types: %w", err)
	}
	if len(rb.colNames) != len(rb.colTypes) {
		return rb, fmt.Errorf("length if column names and column types don't match")
	}
	return rb, nil
}

func (rb readerBase) scanValues() (values []any, err error) {
	values = make([]any, len(rb.colNames))
	scanArgs := make([]any, len(rb.colNames))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err = rb.rows.Scan(scanArgs...); err != nil {
		return nil, err
	}
	return values, nil
}
