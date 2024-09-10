package dalgo2sql

import (
	"context"
	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, dtb.options, record, dtb.db.ExecContext)
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext)
}

func insertSingle(ctx context.Context, options Options, record dal.Record, exec statementExecutor, opts ...dal.InsertOption) error {
	query := buildSingleRecordQuery(insert, options, record)
	if _, err := exec(ctx, query.text, query.args...); err != nil {
		return err
	}
	return nil
}

// InsertMulti inserts multiple records in a single transaction at once. TODO: Implement batched multi-insert
func (t transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	for _, record := range records {
		if err := insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext); err != nil {
			return err
		}
	}
	return nil
}
