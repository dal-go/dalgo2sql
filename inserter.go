package dalgo2sql

import (
	"context"

	"github.com/dal-go/dalgo/dal"
)

// maxIDGenerationAttempts bounds retries when an ID generator is used
// and the generated ID is already taken by an existing row.
const maxIDGenerationAttempts = 10

func (dtb *database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, dtb.options, record, dtb.db.ExecContext, dtb.db.Query, opts...)
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.Query, opts...)
}

// insertSingle inserts a single record honoring dal.InsertOptions:
//   - an explicit ID generator (e.g. dal.WithRandomStringKey) is run with bounded
//     retries while the generated ID is already taken by an existing row;
//   - dal.WithAdapterGeneratedID falls back to the default random-string generator
//     (per the dal contract), as generic SQL has no portable native ID allocation
//     for arbitrary key types;
//   - otherwise the record is inserted as is.
func insertSingle(ctx context.Context, options DbOptions, record dal.Record, exec statementExecutor, execQuery queryExecutor, opts ...dal.InsertOption) error {
	insertOptions := dal.NewInsertOptions(opts...)
	generateID := insertOptions.IDGenerator()
	if generateID == nil && insertOptions.PreferAdapterGeneratedID() {
		generateID = dal.NewInsertOptions(dal.WithRandomStringKey(dal.DefaultRandomStringIDLength, 5)).IDGenerator()
	}
	if generateID != nil {
		return dal.InsertWithIdGenerator(ctx, record, generateID, maxIDGenerationAttempts,
			func(key *dal.Key) error {
				exists, err := executeExists(ctx, options, key, execQuery)
				if err != nil {
					return err
				}
				if !exists {
					return dal.NewErrNotFoundByKey(key, nil)
				}
				return nil
			},
			func(r dal.Record) error {
				return execInsert(ctx, options, r, exec)
			},
		)
	}
	return execInsert(ctx, options, record, exec)
}

func execInsert(ctx context.Context, options DbOptions, record dal.Record, exec statementExecutor) error {
	q := buildSingleRecordQuery(insertOperation, options, record)
	if _, err := exec(ctx, q.text, q.args...); err != nil {
		return err
	}
	return nil
}

// InsertMulti inserts multiple records in a single transaction at once. TODO: Implement batched multi-insertOperation
func (t transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	for _, record := range records {
		if err := insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.Query, opts...); err != nil {
			return err
		}
	}
	return nil
}
