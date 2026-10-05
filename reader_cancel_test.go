package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// A read that runs on a leased connection gives the connection back when its context
// ends, and a statement that runs after that must say why it did not run. On the pool a
// read whose context has ended fails with the context's error; the lease must say the
// same, not "sql: connection is already closed", which blames the connection for what
// the caller did. The hook that gives the connection back runs in its own goroutine, so a
// context that ends between the catalog query and the statement can meet it either way;
// the test runs what the hook does, to completion, before the statement, so it meets it
// every time.
func TestAStatementAfterTheContextEndedReportsTheContextsError(t *testing.T) {
	newLease := func(t *testing.T) (context.Context, context.CancelFunc, executeQueryFunc, *connLease) {
		t.Helper()
		recorder := &connectionRecorder{}
		db := recorder.open(t)
		backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		execute, lease, err := backend.readExecutor(ctx, postgresConnectionQuery(), db.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		return ctx, cancel, execute, lease
	}

	t.Run("the context ended: its error, not a closed connection", func(t *testing.T) {
		ctx, cancel, execute, lease := newLease(t)
		cancel()
		lease.giveBack() // what the hook does when the context ends
		rows, err := execute(ctx, "SELECT 1")
		if rows != nil {
			_ = rows.Close()
			t.Fatal("a statement ran on a connection the lease had given back")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want the context's error", err)
		}
		if errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("error = %v: the closed connection is not what the caller should hear about", err)
		}
	})
	t.Run("a connection given back while the context is live is still a closed connection", func(t *testing.T) {
		ctx, _, execute, lease := newLease(t)
		lease.giveBack()
		_, err := execute(ctx, "SELECT 1")
		if !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("error = %v, want the closed connection: only an ended context explains it away", err)
		}
	})
	t.Run("any other error is the server's", func(t *testing.T) {
		boom := errors.New("syntax error")
		recorder := &connectionRecorder{failStatementWith: func(string) error { return boom }}
		db := recorder.open(t)
		backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
		ctx := context.Background()
		execute, lease, err := backend.readExecutor(ctx, postgresConnectionQuery(), db.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.release()
		if _, err := execute(ctx, "SELECT 1"); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want the server's", err)
		}
	})
	t.Run("a read whose context ended returns the context's error, whichever reader", func(t *testing.T) {
		for _, read := range []struct {
			name string
			run  func(context.Context, *database) error
		}{
			{"records reader", func(ctx context.Context, backend *database) error {
				_, err := backend.ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery())
				return err
			}},
			{"recordset reader", func(ctx context.Context, backend *database) error {
				_, err := backend.ExecuteQueryToRecordsetReader(ctx, postgresConnectionQuery())
				return err
			}},
		} {
			t.Run(read.name, func(t *testing.T) {
				recorder := &connectionRecorder{}
				db := recorder.open(t)
				backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := read.run(ctx, backend); !errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrConnDone) {
					t.Fatalf("error = %v, want the context's error", err)
				}
				if stats := db.Stats(); stats.InUse != 0 {
					t.Fatalf("%d connections are still in use: %+v", stats.InUse, stats)
				}
			})
		}
	})
}
