package dalgo2sql

import (
	"context"

	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, dtb.options, record, dtb.db.ExecContext, opts...)
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, opts...)
}

func insertSingle(ctx context.Context, options DbOptions, record dal.Record, exec statementExecutor, _ ...dal.InsertOption) error {
	q := buildSingleRecordQuery(insertOperation, options, record)
	if _, err := exec(ctx, q.text, q.args...); err != nil {
		return err
	}
	return nil
}

// InsertMulti inserts multiple records in a single transaction at once. TODO: Implement batched multi-insertOperation
func (t transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	for _, record := range records {
		if err := insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, opts...); err != nil {
			return err
		}
	}
	return nil
}
