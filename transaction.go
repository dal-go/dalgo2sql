package dalgo2sql

import (
	"context"
	"database/sql"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

var _ dal.Transaction = (*transaction)(nil)

func newTransaction(tx *sql.Tx, sqlOptions DbOptions, txOptions dal.TransactionOptions) transaction {
	return transaction{
		tx:                    tx,
		recordsReaderProvider: recordsReaderProvider{executeQuery: tx.QueryContext},
		sqlOptions:            sqlOptions,
		txOptions:             txOptions,
	}
}

type transaction struct {
	tx *sql.Tx
	recordsReaderProvider
	sqlOptions DbOptions // TODO: document why & how to use
	txOptions  dal.TransactionOptions
}

func (t transaction) Options() dal.TransactionOptions {
	return t.txOptions
}

func (t transaction) ID() string {
	return ""
}

func (t transaction) Select(ctx context.Context, query dal.Query) (dal.Reader, error) {
	reader, err := t.ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		return nil, err // a literal nil, as ExecuteQueryToRecordsReader returns it
	}
	return reader, nil
}

func (t transaction) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	reader, err := getRecordsReaderWithOptions(ctx, query, t.tx.QueryContext, t.sqlOptions)
	if err != nil {
		// A literal nil: returning the typed nil pointer would make a non-nil interface
		// whose Close panics.
		return nil, err
	}
	return reader, nil
}

var _ dal.ReadTransaction = (*readTransaction)(nil)

type readTransaction = transaction

func (t readTransaction) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	reader, err := getRecordsetReaderWithOptions(ctx, query, t.tx.QueryContext, t.sqlOptions, options...)
	if err != nil {
		// A literal nil: returning the typed nil pointer would make a non-nil interface
		// whose Close panics.
		return nil, err
	}
	return reader, nil
}

var _ dal.ReadwriteTransaction = (*readwriteTransaction)(nil)

type readwriteTransaction = readTransaction

func newReadwriteTransaction(tx *sql.Tx, sqlOptions DbOptions, txOptions dal.TransactionOptions) readwriteTransaction {
	return newTransaction(tx, sqlOptions, txOptions)
}

func (t transaction) UpdateRecord(ctx context.Context, record dalrecord.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	return t.Update(ctx, record.Key(), updates, preconditions...)
}
