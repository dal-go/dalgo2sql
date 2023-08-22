package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

// Field defines field
type Field struct {
	Name string
}

// RecordsetType defines type of a database recordset
type RecordsetType = int

const (
	// Table identifies a table in a database
	Table RecordsetType = iota
	// View identifies a view in a database
	View
	// StoredProcedure identifies a stored procedure in a database
	StoredProcedure
)

// Recordset hold recordset settings
type Recordset struct {
	Type       RecordsetType
	Name       string
	PrimaryKey []Field // Primary keys by table name
}

var _ dal.Database = (*database)(nil)

type database struct {
	id              string
	db              *sql.DB
	options         Options
	onlyReadWriteTx bool
}

func (dtb *database) ID() string {
	return dtb.id
}

func (dtb *database) Client() dal.ClientInfo {
	return dal.NewClientInfo("dalgo2sql", Version)
}

func (dtb *database) QueryReader(c context.Context, query dal.Query) (dal.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (dtb *database) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	//TODO implement me
	panic("implement me")
}

// Options provides database sqlOptions for DALgo - // TODO: document why & how to use
type Options struct {
	PrimaryKey []string
	Recordsets map[string]Recordset
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
	if err = f(ctx, transaction{tx: dbTx, sqlOptions: dtb.options}); err != nil {
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
	if err = f(ctx, readwriteTransaction{tx: dbTx, sqlOptions: dtb.options}); err != nil {
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

func (dtb *database) Select(ctx context.Context, query dal.Query) (dal.Reader, error) {
	panic("implement me")
}

var _ dal.Database = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter for BungDB
func NewDatabase(db *sql.DB, options Options) dal.Database {
	if db == nil {
		panic("db is a required parameter, got nil")
	}
	return &database{
		db:      db,
		options: options,
	}
}
