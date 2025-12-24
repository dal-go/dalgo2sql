package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/dal-go/dalgo/dal"
)

var _ dal.RecordsReader = (*recordsReader)(nil)

func getRecordsReader(ctx context.Context, query dal.Query, execute executeQueryFunc) (rr *recordsReader, err error) {
	var newRecord func() dal.Record

	rr = &recordsReader{
		newRecord: newRecord,
	}

	if rr.readerBase, err = getReaderBase(ctx, query, execute); err != nil {
		err = fmt.Errorf("failed to get SQL reader: %w", err)
		return
	}

	return
}

type recordsReader struct {
	readerBase
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
		var values []any
		if values, err = r.scanValues(); err != nil {
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
	return getRecordsReader(ctx, query, rrp.executeQuery)
}

func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]dal.Record, error) {
	r, err := rrp.GetRecordsReader(ctx, query)
	if err != nil {
		return nil, err
	}
	return dal.ReadAllToRecords(ctx, r, options...)
}
