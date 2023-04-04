package dalgo2sql

import (
	"context"
	"database/sql"
	"github.com/dal-go/dalgo/dal"
)

type transaction struct {
	tx         *sql.Tx
	sqlOptions Options // TODO: document why & how to use
	txOptions  dal.TransactionOptions
}

func (t transaction) Options() dal.TransactionOptions {
	return t.txOptions
}

func (t transaction) Select(ctx context.Context, query dal.Select) (dal.Reader, error) {
	panic("implement me") // TODO: implement me
}

var _ dal.Transaction = (*transaction)(nil)
