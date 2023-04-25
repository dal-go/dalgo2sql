package dalgo2sql

import (
	"context"
	"database/sql"
	"github.com/dal-go/dalgo/dal"
)

var _ dal.Transaction = (*transaction)(nil)

type transaction struct {
	tx         *sql.Tx
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

func (t readTransaction) QueryReader(c context.Context, query dal.Query) (dal.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (t readTransaction) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	//TODO implement me
	panic("implement me")
}

var _ dal.ReadwriteTransaction = (*readwriteTransaction)(nil)

type readwriteTransaction = readTransaction
