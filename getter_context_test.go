package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"modernc.org/sqlite"
)

func keyedContextOptions() DbOptions {
	return DbOptions{PrimaryKey: []string{"id"}, StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")})}}
}
func keyedContextRecords(table string) []dalrecord.Record {
	return []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, int64(-1)), map[string]any{}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, int64(-2)), map[string]any{}),
	}
}

type keyedContextReader interface {
	Get(context.Context, dalrecord.Record) error
	GetMulti(context.Context, []dalrecord.Record) error
	Exists(context.Context, *dalrecord.Key) (bool, error)
}

func keyedContextOperations() map[string]func(context.Context, keyedContextReader) error {
	return map[string]func(context.Context, keyedContextReader) error{
		"Get": func(ctx context.Context, r keyedContextReader) error {
			return r.Get(ctx, keyedContextRecords("items")[0])
		},
		"GetMulti": func(ctx context.Context, r keyedContextReader) error {
			return r.GetMulti(ctx, keyedContextRecords("items"))
		},
		"Exists": func(ctx context.Context, r keyedContextReader) error {
			_, err := r.Exists(ctx, keyedContextRecords("items")[0].Key())
			return err
		},
	}
}

// The occupied one-connection modernc pool proves cancellation during acquisition,
// rather than merely rejecting a context that ended before the operation began.
func TestKeyedReadContextPoolWait(t *testing.T) {
	for name, op := range keyedContextOperations() {
		t.Run(name, func(t *testing.T) {
			raw := openTestSQLiteDB(t, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
			raw.SetMaxOpenConns(1)
			held, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Close() }()
			backend := &database{db: raw, options: keyedContextOptions()}
			assertPoolWaitCanceled(t, raw, func(ctx context.Context) error { return op(ctx, backend) }, func() { _ = held.Close() })
			assertKeyedPoolReusable(t, raw)
		})
	}
	for _, readonly := range []bool{false, true} {
		t.Run(fmt.Sprintf("BeginTx/readonly=%t", readonly), func(t *testing.T) {
			raw := openTestSQLiteDB(t, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
			raw.SetMaxOpenConns(1)
			held, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Close() }()
			backend := &database{db: raw, options: keyedContextOptions()}
			assertPoolWaitCanceled(t, raw, func(ctx context.Context) error {
				if readonly {
					return backend.RunReadonlyTransaction(ctx, func(context.Context, dal.ReadTransaction) error { t.Error("worker ran after cancellation"); return nil })
				}
				return backend.RunReadwriteTransaction(ctx, func(context.Context, dal.ReadwriteTransaction) error {
					t.Error("worker ran after cancellation")
					return nil
				})
			}, func() { _ = held.Close() })
			assertKeyedPoolReusable(t, raw)
		})
	}
}

func assertPoolWaitCanceled(t *testing.T, raw *sql.DB, run func(context.Context) error, release func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := raw.Stats().WaitCount
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for raw.Stats().WaitCount == before {
		select {
		case err := <-done:
			t.Fatalf("operation returned before waiting: %v", err)
		case <-deadline.C:
			t.Fatal("operation never waited for the occupied pool")
		default:
			runtime.Gosched()
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want cancellation", err)
		}
	case <-deadline.C:
		release() // Let an uncancelable baseline finish without leaking a goroutine.
		err := <-done
		t.Fatalf("pool wait ignored cancellation and needed connection release: %v", err)
	}
	release()
}

var keyedProbeSerial atomic.Uint64

// The scalar only announces actual SQLite execution. Cancellation is external;
// the driver must interrupt the recursive statement. The fallback error bounds
// a failing contextless baseline without relying on sleeps or running services.
func TestKeyedReadContextInterruptsSQLite(t *testing.T) {
	for name, op := range keyedContextOperations() {
		for _, inTx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/transaction=%t", name, inTx), func(t *testing.T) {
				started := make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var once sync.Once
				var afterCancel int
				probe := fmt.Sprintf("keyed_probe_%d", keyedProbeSerial.Add(1))
				fallback := errors.New("contextless statement continued after cancellation")
				err := sqlite.RegisterScalarFunction(probe, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					once.Do(func() { close(started) })
					if ctx.Err() != nil {
						afterCancel++
						if afterCancel > 100000 {
							return nil, fallback
						}
					}
					return args[0], nil
				})
				if err != nil {
					t.Fatal(err)
				}
				raw := openTestSQLiteDB(t, fmt.Sprintf("CREATE VIEW items AS WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<1000000000) SELECT %s(x) AS id, 'value' AS label FROM seq", probe))
				raw.SetMaxOpenConns(1)
				var reader keyedContextReader = &database{db: raw, options: keyedContextOptions()}
				var tx *sql.Tx
				if inTx {
					tx, err = raw.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback() }()
					reader = newTransaction(tx, keyedContextOptions(), dal.NewTransactionOptions())
				}
				done := make(chan error, 1)
				go func() { done <- op(ctx, reader) }()
				select {
				case <-started:
				case <-time.After(2 * time.Second):
					t.Fatal("SQLite statement never started")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("actual SQLite execution returned %v, want context.Canceled", err)
					}
					if afterCancel > 100000 {
						t.Fatal("driver did not interrupt execution")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("SQLite statement did not stop")
				}
				if tx != nil {
					// Canceling the read context does not terminate the independent transaction.
					var one int
					if err := tx.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
						t.Fatalf("transaction unusable: %d, %v", one, err)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				assertKeyedPoolReusable(t, raw)
			})
		}
	}
}
func assertKeyedPoolReusable(t *testing.T, raw *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var one int
	if err := raw.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("pool unusable after cancellation: %d, %v", one, err)
	}
	if n := raw.Stats().InUse; n != 0 {
		t.Fatalf("%d connections leaked", n)
	}
}

func TestKeyedReadIterationErrors(t *testing.T) {
	failure := errors.New("iteration failure")
	for _, name := range []string{"Exists", "Get before row", "Get after row", "Get scan", "GetMulti map", "GetMulti struct"} {
		t.Run(name, func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = raw.Close() }()
			backend := &database{db: raw, options: keyedContextOptions()}
			rows := sqlmock.NewRows([]string{"id", "label"}).AddRow(int64(-1), "value")
			if name == "Get after row" {
				rows.AddRow(int64(-2), "value").RowError(1, failure)
			} else if name != "Get scan" {
				rows.RowError(0, failure)
			}
			mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()
			var got error
			records := keyedContextRecords("items")
			switch name {
			case "Exists":
				_, got = backend.Exists(context.Background(), records[0].Key())
			case "GetMulti map":
				got = backend.GetMulti(context.Background(), records)
			case "GetMulti struct":
				for i := range records {
					records[i] = dalrecord.NewRecordWithData(records[i].Key(), &struct{ Label string }{})
				}
				got = backend.GetMulti(context.Background(), records)
			case "Get scan":
				records[0] = dalrecord.NewRecordWithData(records[0].Key(), &struct{ Label int }{})
				got = backend.Get(context.Background(), records[0])
			default:
				got = backend.Get(context.Background(), records[0])
			}
			if name == "Get scan" {
				if got == nil {
					t.Fatal("scan error lost")
				}
			} else if !errors.Is(got, failure) {
				t.Fatalf("got %v, want iteration failure", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if raw.Stats().InUse != 0 {
				t.Fatal("rows leaked connection")
			}
		})
	}
}

func TestKeyedReadCanceledBeforeExecution(t *testing.T) {
	for _, expired := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		want := context.Canceled
		if expired {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		} else {
			cancel()
		}
		for _, name := range []string{"Exists", "Get", "GetMulti", "table helper"} {
			t.Run(fmt.Sprintf("%s/deadline=%t", name, expired), func(t *testing.T) {
				records := keyedContextRecords("items")
				exec := func(context.Context, string, ...any) (*sql.Rows, error) {
					t.Fatal("canceled read started a statement")
					return nil, nil
				}
				var got error
				switch name {
				case "Exists":
					_, got = executeExists(ctx, keyedContextOptions(), records[0].Key(), exec)
				case "Get":
					got = getSingle(ctx, keyedContextOptions(), records[0], exec)
				case "GetMulti":
					got = getMulti(ctx, keyedContextOptions(), records, exec)
				default:
					got = getMultiFromSingleTable(ctx, keyedContextOptions(), records, exec)
				}
				if !errors.Is(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			})
		}
		cancel()
	}
}

// Map iteration can choose any table first. Cancel the first successful executor
// call, then require the batch to stop regardless of which group was selected.
func TestKeyedReadMixedBatchStopsAfterCancellation(t *testing.T) {
	for _, groups := range [][]int{{1, 1}, {1, 2}, {1}} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = raw.Close() }()
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"})).RowsWillBeClosed()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			exec := func(got context.Context, query string, args ...any) (*sql.Rows, error) {
				calls++
				if got != ctx {
					t.Fatal("request context replaced")
				}
				rows, err := raw.Query(query, args...)
				cancel()
				return rows, err
			}
			options := keyedContextOptions()
			var records []dalrecord.Record
			for i, count := range groups {
				table := fmt.Sprintf("table%d", i)
				options.Recordsets[table] = NewRecordset(table, Table, []dal.FieldRef{dal.Field("id")})
				for n := 0; n < count; n++ {
					records = append(records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, n), map[string]any{}))
				}
			}
			if err := getMulti(ctx, options, records, exec); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want canceled", err)
			}
			if calls != 1 {
				t.Fatalf("%d statements started, want one", calls)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			assertKeyedNoRowsInUse(t, raw)
		})
	}
}

func TestKeyedReadSingletonBatchPropagatesQueryError(t *testing.T) {
	failure := errors.New("query failure")
	raw, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	mock.ExpectQuery("SELECT").WillReturnError(failure)
	records := keyedContextRecords("items")[:1]
	options := keyedContextOptions()
	options.Recordsets["other"] = NewRecordset("other", Table, []dal.FieldRef{dal.Field("id")})
	records = append(records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("other", 1), map[string]any{}))
	if err := getMulti(context.Background(), options, records, raw.QueryContext); !errors.Is(err, failure) {
		t.Fatalf("got %v, want query failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	assertKeyedNoRowsInUse(t, raw)
}
func assertKeyedNoRowsInUse(t *testing.T, raw *sql.DB) {
	t.Helper()
	if n := raw.Stats().InUse; n != 0 {
		t.Fatalf("%d connections leaked", n)
	}
}

func TestKeyedReadDeadlineDuringPoolWait(t *testing.T) {
	for name, op := range keyedContextOperations() {
		t.Run(name, func(t *testing.T) {
			raw := openTestSQLiteDB(t, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
			raw.SetMaxOpenConns(1)
			held, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Close() }()
			before := raw.Stats().WaitCount
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- op(ctx, &database{db: raw, options: keyedContextOptions()}) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("got %v, want deadline exceeded", err)
				}
			case <-time.After(2 * time.Second):
				_ = held.Close()
				<-done
				t.Fatal("pool wait ignored deadline")
			}
			if raw.Stats().WaitCount <= before {
				t.Fatal("read never waited in the modernc pool")
			}
			_ = held.Close()
			assertKeyedPoolReusable(t, raw)
		})
	}
}
