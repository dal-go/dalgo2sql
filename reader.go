package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/dal-go/dalgo/dal"
)

var _ dal.Reader = (*reader)(nil)

func getReader(ctx context.Context, query dal.Query, execute func(ctx context.Context, query string, args ...any) (*sql.Rows, error)) (*reader, error) {
	var a []any
	var text string
	var newRecord func() dal.Record
	switch q := query.(type) {
	case dal.TextQuery:
		text = q.Text()
		args := q.Args()
		a := make([]any, len(args))
		for i, arg := range args {
			a[i] = arg
		}
	case dal.StructuredQuery:
		text = q.String()
		newRecord = q.Into()
	}
	rows, err := execute(ctx, text, a...)
	if err != nil {
		return nil, err
	}
	return &reader{rows: rows, newRecord: newRecord}, nil
}

type reader struct {
	rows      *sql.Rows
	colNames  []string
	colTypes  []*sql.ColumnType
	newRecord func() dal.Record
}

func (r reader) Next() (record dal.Record, err error) {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	if r.colNames == nil || r.colTypes == nil {
		if r.colNames, err = r.rows.Columns(); err != nil {
			return nil, fmt.Errorf("failed to read column names: %w", err)
		}
		if r.colTypes, err = r.rows.ColumnTypes(); err != nil {
			return nil, fmt.Errorf("failed to read column types: %w", err)
		}
		if len(r.colNames) != len(r.colTypes) {
			return nil, fmt.Errorf("length if column names and column types don't match")
		}
	}
	record = r.newRecord()
	data := record.Data()
	switch d := data.(type) {
	case map[string]any:
		values := make([]any, len(r.colNames)) // TODO: read rows into value
		for i, n := range r.colNames {
			d[n] = values[i]
		}
	default:
		// TODO: implement Scan into `*struct` and into `[]any`

		err = fmt.Errorf("unsupported data type %T", data)
		return nil, err
	}
	return
}

func (r reader) Cursor() (string, error) {
	return "", dal.ErrNotSupported
}

func (r reader) Close() error {
	return r.rows.Close()
}

// recordsReaderProvider is embedded into database and transaction
type recordsReaderProvider struct {
	executeQuery func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (rrp recordsReaderProvider) GetReader(ctx context.Context, query dal.Query) (dal.Reader, error) {
	return getReader(ctx, query, rrp.executeQuery)
}

func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]dal.Record, error) {
	r, err := rrp.GetReader(ctx, query)
	if err != nil {
		return nil, err
	}
	return dal.ReadAllRecords(r, options...)
}
