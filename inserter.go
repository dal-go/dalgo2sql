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
	return insertSingle(ctx, dtb.options, record, dtb.db.ExecContext, dtb.db.QueryContext, opts...)
}

func (t transaction) Insert(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
	return insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.QueryContext, opts...)
}

// insertSingle inserts a single record honoring dal.InsertOptions:
//   - an explicit ID generator (e.g. dal.WithRandomStringKey) is run with bounded
//     retries while the generated ID is already taken by an existing row;
//   - dal.WithAdapterGeneratedID falls back to the default random-string generator
//     (per the dal contract), as generic SQL has no portable native ID allocation
//     for arbitrary key types;
//   - otherwise the record is inserted as is.
func insertSingle(ctx context.Context, options DbOptions, record dalrecord.Record, exec statementExecutor, execQuery executeQueryFunc, opts ...dal.InsertOption) error {
	// An ID generator checks for a taken ID with a statement of its own before
	// the insert is built, so the names the insert will carry are checked first.
	if err := options.checkRecordNames(record); err != nil {
		return err
	}
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
	q, err := buildSingleRecordQuery(insertOperation, options, record)
	if err != nil {
		return err
	}
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
	// The whole batch is checked before its first statement: an earlier record
	// would otherwise be inserted before a later one is refused.
	insertOptions := dal.NewInsertOptions(opts...)
	idIsGenerated := insertOptions.IDGenerator() != nil || insertOptions.PreferAdapterGeneratedID()
	for _, record := range records {
		if err := t.sqlOptions.checkRecordNames(record); err != nil {
			return err
		}
		// A generated ID is a column of its own, so a record without a field is
		// still an insert.
		if idIsGenerated {
			continue
		}
		if err := t.sqlOptions.checkRecordColumns(record, insertOperation); err != nil {
			return err
		}
		// What the insert will send is built now, so that a record the builder refuses
		// (a key ID that does not fit a composite primary key, a recordset with no
		// primary key to write it to) is refused before the first record is written.
		if _, err := buildSingleRecordQuery(insertOperation, t.sqlOptions, record); err != nil {
			return err
		}
	}
	for _, record := range records {
		if err := insertSingle(ctx, t.sqlOptions, record, t.tx.ExecContext, t.tx.QueryContext, opts...); err != nil {
			return err
		}
	}
	return nil
}
