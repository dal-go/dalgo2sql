package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

// A recordset read that fails after its statement ran, on a column type the recordset
// cannot hold, must close the rows it opened on every path, not only on the leased
// connection of the PostgreSQL dialect (where closing them is what lets the connection go
// back). On the pool path (no dialect, sqlite) and inside a transaction nothing else
// ever closes them: the caller gets an error and no reader, so the rows, and with them a
// pool connection or the transaction's, stay held until the process ends.
func TestRecordsetReadClosesItsRowsWhenAColumnTypeIsUnsupported(t *testing.T) {
	ctx := context.Background()
	textQuery := dal.NewTextQuery("SELECT * FROM Album", nil)
	structured := typedTestFrom("Album", "").NewQuery().SelectColumns()
	for _, scan := range []struct {
		name     string
		scanType reflect.Type
		want     string
	}{
		{"a struct it does not know", reflect.TypeOf(struct{}{}), "unsupported type for column"},
		{"a pointer it does not know", reflect.TypeOf(new(int)), "unsupported pointer type for column"},
		{"a kind it does not know", reflect.TypeOf(map[string]int{}), "unsupported column type kind"},
	} {
		for _, path := range []struct {
			name string
			run  func(t *testing.T, db *sql.DB) (dal.RecordsetReader, error)
		}{
			{"the pool, no dialect", func(t *testing.T, db *sql.DB) (dal.RecordsetReader, error) {
				backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db}
				return backend.ExecuteQueryToRecordsetReader(ctx, textQuery)
			}},
			{"the pool, sqlite", func(t *testing.T, db *sql.DB) (dal.RecordsetReader, error) {
				backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: DbOptions{StructuredQueryDialect: "sqlite"}}
				return backend.ExecuteQueryToRecordsetReader(ctx, structured)
			}},
			{"the reader function itself", func(t *testing.T, db *sql.DB) (dal.RecordsetReader, error) {
				reader, err := getRecordsetReaderWithOptions(ctx, textQuery, db.QueryContext, DbOptions{})
				if err != nil {
					return nil, err
				}
				return reader, nil
			}},
		} {
			t.Run(path.name+", "+scan.name, func(t *testing.T) {
				recorder := &connectionRecorder{statementScanType: scan.scanType}
				db := recorder.open(t)
				reader, err := path.run(t, db)
				if err == nil || !strings.Contains(err.Error(), scan.want) {
					t.Fatalf("the read returned %v, %v; want an error that mentions %q", reader, err, scan.want)
				}
				if reader != nil {
					t.Fatalf("the read returned a reader %v with its error: a caller who sees the error never closes it", reader)
				}
				if open := recorder.openRowCount(); open != 0 {
					t.Fatalf("%d result sets are still open after the failed read", open)
				}
				if stats := db.Stats(); stats.InUse != 0 {
					t.Fatalf("%d connections are still in use after the failed read: %+v", stats.InUse, stats)
				}
			})
		}
	}
}

// The same inside a transaction, which has its own entry point.
func TestTransactionRecordsetReadClosesItsRowsWhenAColumnTypeIsUnsupported(t *testing.T) {
	ctx := context.Background()
	recorder := &connectionRecorder{statementScanType: reflect.TypeOf(struct{}{}), transactions: true}
	db := recorder.open(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var reader dal.RecordsetReader
	reader, err = newTransaction(tx, DbOptions{}, dal.NewTransactionOptions()).ExecuteQueryToRecordsetReader(ctx, dal.NewTextQuery("SELECT * FROM Album", nil), recordset.WithName("album"))
	if err == nil || !strings.Contains(err.Error(), "unsupported type for column") {
		t.Fatalf("the read returned %v, %v; want the unsupported type error", reader, err)
	}
	// A literal nil, not a typed nil pointer inside the interface: a caller who checks
	// the error never closes the reader, and one who closes it anyway must not panic.
	if reader != nil {
		t.Fatalf("the read returned the reader %#v with its error", reader)
	}
	if open := recorder.openRowCount(); open != 0 {
		t.Fatalf("%d result sets are still open after the failed read", open)
	}
}

// The records reader is the twin of the recordset reader: a read that fails returns a literal
// nil reader with its error, on every entry point. The reader the failed read allocated holds
// no rows, and its Close does not panic, but it is not nil, so a caller who tests the reader
// instead of the error would take it for a result.
func TestARecordsReadThatFailsReturnsNoReader(t *testing.T) {
	ctx := context.Background()
	textQuery := dal.NewTextQuery("SELECT * FROM Album", nil)
	recorder := &statementRecorder{} // fails every statement
	db := recorder.open(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	pool := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db}
	leased := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
	transaction := newTransaction(tx, DbOptions{}, dal.NewTransactionOptions())
	provider := recordsReaderProvider{executeQuery: db.QueryContext}
	for _, tc := range []struct {
		name string
		read func() (any, error)
	}{
		{"the pool", func() (any, error) { return pool.ExecuteQueryToRecordsReader(ctx, textQuery) }},
		{"the pool, on a leased connection", func() (any, error) {
			return leased.ExecuteQueryToRecordsReader(ctx, typedTestFrom("Album", "").NewQuery().SelectColumns())
		}},
		{"a transaction", func() (any, error) { return transaction.ExecuteQueryToRecordsReader(ctx, textQuery) }},
		{"a transaction's Select", func() (any, error) { return transaction.Select(ctx, textQuery) }},
		{"the provider", func() (any, error) { return provider.ExecuteQueryToRecordsReader(ctx, textQuery) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := tc.read()
			if !errors.Is(err, errReachedDatabase) {
				t.Fatalf("the read returned %v, %v; want the error of the statement", reader, err)
			}
			if reader != nil {
				t.Fatalf("the read returned the reader %#v with its error: a caller who sees the error never closes it", reader)
			}
		})
	}
}

// A reader that never had rows closes without a panic: its caller can close it whatever the
// read did.
func TestARecordsReaderWithoutRowsCloses(t *testing.T) {
	if err := (recordsReader{}).Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}
