package dalgo2sql

import (
	"context"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// maxIDGenerationAttempts bounds retries when an ID generator is used
// and the generated ID is already taken by an existing row.
const maxIDGenerationAttempts = 10

func (dtb *database) Insert(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, dtb.options, record, dtb.db.ExecContext, dtb.db.Query, opts...)
}

func (t transaction) Insert(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.Query, opts...)
}

// insertSingle inserts a single record honoring dal.InsertOptions:
//   - an explicit ID generator (e.g. dal.WithRandomStringKey) is run with bounded
//     retries while the generated ID is already taken by an existing row;
//   - dal.WithAdapterGeneratedID falls back to the default random-string generator
//     (per the dal contract), as generic SQL has no portable native ID allocation
//     for arbitrary key types;
//   - otherwise the record is inserted as is.
func insertSingle(ctx context.Context, options DbOptions, record dalrecord.Record, exec statementExecutor, execQuery queryExecutor, opts ...dal.InsertOption) error {
	insertOptions := dal.NewInsertOptions(opts...)
	generateID := insertOptions.IDGenerator()
	if generateID == nil && insertOptions.PreferAdapterGeneratedID() {
		generateID = dal.NewInsertOptions(dal.WithRandomStringKey(dal.DefaultRandomStringIDLength, 5)).IDGenerator()
	}
	if generateID != nil {
		return dal.InsertWithIdGenerator(ctx, record, generateID, maxIDGenerationAttempts,
			func(key *dalrecord.Key) error {
				exists, err := executeExists(ctx, options, key, execQuery)
				if err != nil {
					return err
				}
				if !exists {
					return dal.NewErrNotFoundByKey(key, nil)
				}
				return nil
			},
			func(r dalrecord.Record) error {
				return execInsert(ctx, options, r, exec)
			},
		)
	}
	return execInsert(ctx, options, record, exec)
}

// execInsert issues the INSERT statement and is the single choke point every
// insert path in this package funnels through: (*database).Insert and
// (transaction).Insert call it directly via insertSingle; InsertMulti calls
// it once per record, also via insertSingle; and the dal.InsertWithIdGenerator
// retry loop (used when an ID generator or dal.WithAdapterGeneratedID is
// requested) calls it as its final "write" step. Classifying the driver error
// here therefore covers all of them without needing to duplicate the check at
// each call site — verified by reading every caller of insertSingle and
// execInsert in this file.
func execInsert(ctx context.Context, options DbOptions, record dalrecord.Record, exec statementExecutor) error {
	q := buildSingleRecordQuery(insertOperation, options, record)
	if _, err := exec(ctx, q.text, q.args...); err != nil {
		if options.IsAlreadyExists != nil && options.IsAlreadyExists(err) {
			return fmt.Errorf("%w: %w", dalrecord.ErrRecordExists, err)
		}
		return err
	}
	return nil
}

// InsertMulti inserts multiple records in a single transaction at once. TODO: Implement batched multi-insertOperation
func (t transaction) InsertMulti(ctx context.Context, records []dalrecord.Record, opts ...dal.InsertOption) error {
	for _, record := range records {
		if err := insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.Query, opts...); err != nil {
			return err
		}
	}
	return nil
}
