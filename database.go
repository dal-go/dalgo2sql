package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

var _ dal.Backend = (*database)(nil)

type database struct {
	dal.ConcurrencyAvailable // SupportsConcurrentConnections() = true (standard SQL pool)
	recordsReaderProvider
	id              string
	db              *sql.DB
	schema          dal.Schema
	onlyReadWriteTx bool

	// Deprecated - replaced by schema
	options DbOptions
}

// QueryCapabilities advertises what the structured-query dialect runs on the server.
// With the PostgreSQL dialect that is GROUP BY, HAVING, ORDER BY, COUNT, SUM and AVG
// with their DISTINCT forms, MIN and MAX; with SQLite, the same subset. Any other
// dialect declares nothing, and DALgo aggregates in its own engine. FIRST and LAST
// are not advertised until aggregate-local ORDER BY can be rendered without relying on
// unspecified row order, and neither dialect promises a group-key order or a stable
// row order: DALgo's planner refuses a query that uses one for this adapter, as it
// needs a provider-declared stable input order, and nothing runs it.
func (dtb *database) QueryCapabilities() dal.QueryCapabilities {
	switch dtb.options.StructuredQueryDialect {
	case "postgres":
		return postgresDialect{}.capabilities()
	case "sqlite":
		return dal.QueryCapabilities{
			GroupBy: true,
			Having:  true,
			OrderBy: true,
			Aggregate: dal.AggregateCapabilities{
				Count: true, CountDistinct: true,
				Sum: true, SumDistinct: true,
				Avg: true, AvgDistinct: true,
				Min: true, Max: true,
			},
		}
	}
	return dal.QueryCapabilities{}
}

func (dtb *database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	execute, lease, err := dtb.readExecutor(ctx, query, dtb.executeQuery)
	if err != nil {
		return nil, err
	}
	reader, err := getRecordsetReaderWithOptions(ctx, query, execute, dtb.options, options...)
	if err != nil {
		// A read that fails has closed its rows (see getRecordsetReaderWithOptions), which
		// is what lets the connection go back: closing it waits for them.
		lease.release()
		return nil, err
	}
	reader.lease = lease
	return reader, nil
}

// readExecutor says where a read runs its statements. With the PostgreSQL dialect a
// structured read runs two: the catalog lookup that tells the compiler what the
// sources are, and the statement it compiles from the answer. Each PostgreSQL
// connection has its own search_path, and database/sql hands each call of a pool to
// any connection, so the two could describe different relations. The read therefore
// takes one connection for both, and the lease the reader releases when it is done
// with the rows: closed, read to its end, or, as on the pool, when ctx ends. Any other
// read runs on pool, as before, and holds no lease.
func (dtb *database) readExecutor(ctx context.Context, query dal.Query, pool executeQueryFunc) (executeQueryFunc, *connLease, error) {
	if _, structured := query.(dal.StructuredQuery); !structured || dtb.options.StructuredQueryDialect != "postgres" {
		return pool, nil, nil
	}
	conn, err := dtb.db.Conn(ctx)
	if err != nil {
		return nil, nil, explainByContext(ctx, err)
	}
	lease := newConnLease(ctx, conn)
	return lease.query, lease, nil
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
	return finishTransaction(dbTx, func() error {
		return f(ctx, newTransaction(dbTx, dtb.options, dalgoTxOptions))
	})
}

// finishTransaction runs worker in dbTx and ends the transaction: it commits when
// the worker returns no error, and rolls back when it returns one. A worker that
// panics, or ends its goroutine (runtime.Goexit, as t.FailNow does), is rolled back
// too and then left to go on: the transaction is not left open, where it would hold
// its connection and, on SQLite, the lock of the file for as long as its context lives,
// which with context.Background() is for good.
func finishTransaction(dbTx *sql.Tx, worker func() error) error {
	returned := false
	defer func() {
		if !returned {
			_ = dbTx.Rollback()
		}
	}()
	err := worker()
	returned = true
	if err != nil {
		if rollbackErr := dbTx.Rollback(); rollbackErr != nil {
			return dal.NewRollbackError(rollbackErr, err)
		}
		return err
	}
	if err = dbTx.Commit(); err != nil {
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
	return finishTransaction(dbTx, func() error {
		return f(ctx, newReadwriteTransaction(dbTx, dtb.options, dalgoTxOptions))
	})
}

func (dtb *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	execute, lease, err := dtb.readExecutor(ctx, query, dtb.db.QueryContext)
	if err != nil {
		return nil, err
	}
	reader, err := getRecordsReaderWithOptions(ctx, query, execute, dtb.options)
	if lease == nil {
		return reader, err
	}
	if err != nil {
		lease.release()
		return nil, err
	}
	reader.lease = lease
	return reader, nil
}

// NewDatabase creates a new instance of DALgo adapter to SQL database.
//
// The returned dal.DB is sealed by dal.NewDB: every read-write transaction it
// starts hands the worker a transaction whose writes run the framework's
// BeforeSave validation and hooks before reaching this adapter's code.
func NewDatabase(db *sql.DB, schema dal.Schema, options DbOptions) dal.DB {
	if db == nil {
		panic("db is a required parameter, got nil")
	}
	if schema == nil {
		panic("schema is a required parameter, got nil")
	}
	wrapped := dal.NewDB(&database{
		recordsReaderProvider: recordsReaderProvider{
			executeQuery: db.QueryContext,
		},
		id:      options.ID,
		db:      db,
		schema:  schema,
		options: options,
	})
	if options.StructuredQueryDialect == "sqlite" {
		return newSQLiteProtectedFactory(wrapped, db, options)
	}
	return wrapped
}
