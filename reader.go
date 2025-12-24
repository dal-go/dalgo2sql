package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/dal-go/dalgo/dal"
)

var _ dal.RecordsReader = (*recordsReader)(nil)

func getReader(ctx context.Context, query dal.Query, execute func(ctx context.Context, query string, args ...any) (*sql.Rows, error)) (rr *recordsReader, err error) {
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
		newRecord = q.IntoRecord
	}

	var rows *sql.Rows
	rows, err = execute(ctx, text, a...)
	if err != nil {
		return
	}
	rr = &recordsReader{rows: rows, newRecord: newRecord}
	if rr.colNames, err = rows.Columns(); err != nil {
		return nil, fmt.Errorf("failed to read column names: %w", err)
	}
	if rr.colTypes, err = rows.ColumnTypes(); err != nil {
		return nil, fmt.Errorf("failed to read column types: %w", err)
	}
	if len(rr.colNames) != len(rr.colTypes) {
		return nil, fmt.Errorf("length if column names and column types don't match")
	}
	return
}

type recordsReader struct {
	rows      *sql.Rows
	colNames  []string
	colTypes  []*sql.ColumnType
	newRecord func() dal.Record
}

func (r recordsReader) Next() (record dal.Record, err error) {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	record = r.newRecord()
	record.SetError(nil)
	data := record.Data()
	switch d := data.(type) {
	case map[string]any:
		values := make([]any, len(r.colNames))
		scanArgs := make([]any, len(r.colNames))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err = r.rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
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

func (r recordsReader) Cursor() (string, error) {
	return "", dal.ErrNotSupported
}

func (r recordsReader) Close() error {
	return r.rows.Close()
}

// recordsReaderProvider is embedded into database and transaction
type recordsReaderProvider struct {
	executeQuery func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (rrp recordsReaderProvider) GetRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return getReader(ctx, query, rrp.executeQuery)
}

func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]dal.Record, error) {
	r, err := rrp.GetRecordsReader(ctx, query)
	if err != nil {
		return nil, err
	}
	return dal.ReadAllToRecords(ctx, r, options...)
}
