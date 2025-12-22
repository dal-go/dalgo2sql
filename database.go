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

func (v Field) String() string {
	return v.Name
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
	name       string
	t          RecordsetType
	primaryKey []dal.FieldRef // Primary keys by table name
}

func (v *Recordset) Name() string {
	return v.name
}

func (v *Recordset) PrimaryKey() []dal.FieldRef {
	if v == nil {
		return nil
	}
	pk := make([]dal.FieldRef, len(v.primaryKey))
	copy(pk, v.primaryKey)
	return pk
}

func (v *Recordset) PrimaryKeyFieldNames() []string {
	pk := make([]string, len(v.primaryKey))
	for i, f := range v.primaryKey {
		pk[i] = f.Name()
	}
	return pk
}

func NewRecordset(name string, t RecordsetType, primaryKey []dal.FieldRef) *Recordset {
	return &Recordset{
		name:       name,
		t:          t,
		primaryKey: primaryKey,
	}
}

func (v *Recordset) Type() RecordsetType {
	return v.t
}

var _ dal.DB = (*database)(nil)

type database struct {
	recordsReaderProvider
	id              string
	db              *sql.DB
	schema          dal.Schema
	onlyReadWriteTx bool

	// Deprecated - replaced by schema
	options Options
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

// Options provides database sqlOptions for DALgo - // TODO: document why & how to use
type Options struct {
	PrimaryKey []string
	Recordsets map[string]*Recordset
}

func (o Options) GetRecordsetByKey(key *dal.Key) *Recordset {
	rsName := getRecordsetName(key)
	return o.Recordsets[rsName]
}

func (o Options) PrimaryKeyFieldNames(key *dal.Key) (primaryKey []string) {
	rs := o.GetRecordsetByKey(key)
	if pk := rs.PrimaryKey(); len(pk) > 0 {
		primaryKey = make([]string, len(pk))
		for i, f := range pk {
			primaryKey[i] = f.Name()
		}
		return
	}
	return nil
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
	if err = f(ctx, newTransaction(dbTx, dtb.options)); err != nil {
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
	if err = f(ctx, newReadwriteTransaction(dbTx, dtb.options)); err != nil {
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

var _ dal.DB = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter to SQL database
func NewDatabase(db *sql.DB, schema dal.Schema, options Options) dal.DB {
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
		db:      db,
		schema:  schema,
		options: options,
	}
}
