package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dal-go/dalgo/dal"
)

type executeQueryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

type readerBase struct {
	rows     *sql.Rows
	colNames []string
	colTypes []*sql.ColumnType
}

func getReaderBase(ctx context.Context, query dal.Query, execute executeQueryFunc) (readerBase, error) {
	var a []any
	var text string
	switch q := query.(type) {
	case dal.TextQuery:
		text = q.Text()
		args := q.Args()
		a = make([]any, len(args))
		for i, arg := range args {
			a[i] = arg
		}
	case dal.StructuredQuery:
		text = q.String()
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
