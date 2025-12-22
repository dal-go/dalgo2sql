package dalgo2sql

import (
	"context"
	"database/sql"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

var _ dal.Transaction = (*transaction)(nil)

func newTransaction(tx *sql.Tx, sqlOptions Options) transaction {
	return transaction{
		tx:                    tx,
		recordsReaderProvider: recordsReaderProvider{executeQuery: tx.QueryContext},
		sqlOptions:            sqlOptions,
	}
}

type transaction struct {
	tx *sql.Tx
	recordsReaderProvider
	sqlOptions Options // TODO: document why & how to use
	txOptions  dal.TransactionOptions
}

func (t transaction) Options() dal.TransactionOptions {
	return t.txOptions
}

func (t readwriteTransaction) ID() string {
	return ""
}

func (t transaction) Select(ctx context.Context, query dal.Query) (dal.Reader, error) {
	panic("implement me") // TODO: implement me
}

var _ dal.ReadTransaction = (*readTransaction)(nil)

type readTransaction = transaction

var _ dal.ReadwriteTransaction = (*readwriteTransaction)(nil)

type readwriteTransaction = readTransaction

func newReadwriteTransaction(tx *sql.Tx, sqlOptions Options) readwriteTransaction {
	return newTransaction(tx, sqlOptions)
}

func (t transaction) UpdateRecord(ctx context.Context, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	return t.Update(ctx, record.Key(), updates, preconditions...)
}
