package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

var _ dal.DB = (*database)(nil)

type database struct {
	recordsReaderProvider
	id              string
	db              *sql.DB
	schema          dal.Schema
	onlyReadWriteTx bool

	// Deprecated - replaced by schema
	options DbOptions
}

func (dtb *database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	return getRecordsetReader(ctx, query, dtb.executeQuery, options...)
}

//func (dtb *database) Connect(ctx context.Context) (dal.Connection, error) {
//	return connection{database: dtb}, nil
//}

func (dtb *database) ID() string {
	return dtb.id
}

func (dtb *database) Adapter() dal.Adapter {
	return dal.NewAdapter("dalgo2sql", Version)
}

func (dtb *database) Schema() dal.Schema {
	return dtb.schema
}

func (dtb *database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	dalgoTxOptions := dal.NewTransactionOptions(append(options, dal.TxWithReadonly())...)
	var sqlTxOptions sql.TxOptions
	if dalgoTxOptions.IsReadonly() {
		sqlTxOptions.ReadOnly = !dtb.onlyReadWriteTx
	} else {
		return fmt.Errorf("attemt to run readonly transation without readonly option")
	}
	dbTx, err := dtb.db.BeginTx(ctx, &sqlTxOptions)
	if err != nil {
		if err.Error() == "sql: driver does not support read-only transactions" {
			dtb.onlyReadWriteTx = true
			sqlTxOptions.ReadOnly = false
			dbTx, err = dtb.db.BeginTx(ctx, &sqlTxOptions)
		}
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
	}
	if dbTx == nil {
		return fmt.Errorf("sql driver returned nil transaction")
	}
	if err = f(ctx, newTransaction(dbTx, dtb.options, dalgoTxOptions)); err != nil {
		if rollbackErr := dbTx.Rollback(); rollbackErr != nil {
			return dal.NewRollbackError(rollbackErr, err)
		}
		return err
	}
	if err := dbTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (dtb *database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	dalgoTxOptions := dal.NewTransactionOptions(options...)
	sqlTxOptions := sql.TxOptions{}
	if dalgoTxOptions.IsReadonly() {
		return fmt.Errorf("attemt to run readwrite transation with readonly=true option")
	}
	dbTx, err := dtb.db.BeginTx(ctx, &sqlTxOptions)
	if err != nil {
		return err
	}
	if err = f(ctx, newReadwriteTransaction(dbTx, dtb.options, dalgoTxOptions)); err != nil {
		if rollbackErr := dbTx.Rollback(); rollbackErr != nil {
			return dal.NewRollbackError(rollbackErr, err)
		}
		return err
	}
	if err := dbTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (dtb *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return getRecordsReader(ctx, query, dtb.db.QueryContext)
}

var _ dal.DB = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter to SQL database
func NewDatabase(db *sql.DB, schema dal.Schema, options DbOptions) dal.DB {
	if db == nil {
		panic("db is a required parameter, got nil")
	}
	if schema == nil {
		panic("schema is a required parameter, got nil")
	}
	return &database{
		recordsReaderProvider: recordsReaderProvider{
			executeQuery: db.QueryContext,
		},
		id:      options.ID,
		db:      db,
		schema:  schema,
		options: options,
	}
}
