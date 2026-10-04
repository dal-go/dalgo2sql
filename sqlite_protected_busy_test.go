package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// The ctx.Done() branch of the busy-retry wait used to be reached only by a
// test that raced a 20ms deadline against a 5ms timer, so on a loaded runner
// the deadline could expire inside conn.ExecContext instead and the branch went
// unmeasured (coverage 99.9% against the 100% floor on a YAML-only diff). These
// tests cover each outcome of the wait, and the retry loop's reaction to it,
// without depending on timing.

func TestWaitBeforeSQLiteBusyRetry(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitBeforeSQLiteBusyRetry(cancelled, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
	if err := waitBeforeSQLiteBusyRetry(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("elapsed wait: %v", err)
	}
}

func TestSQLiteProtectedBusyRetryStopsWhenWaitFails(t *testing.T) {
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err = raw.Exec("CREATE TABLE t (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	lock, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	if _, err = lock.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = lock.ExecContext(context.Background(), "ROLLBACK") }()

	sentinel := errors.New("wait failed")
	calls := 0
	original := sqliteBusyWait
	sqliteBusyWait = func(context.Context, time.Duration) error {
		calls++
		if calls < 2 {
			return nil // first busy answer: retry the BEGIN
		}
		return sentinel // second busy answer: the wait gives up
	}
	t.Cleanup(func() { sqliteBusyWait = original })

	storage := &sqliteProtectedStorage{db: raw}
	err = storage.within(context.Background(), nil, true, func(*sqliteProtectedSession) error {
		t.Fatal("callback ran while the database was locked")
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("within: %v", err)
	}
	if calls != 2 {
		t.Fatalf("busy waits: got %d, want 2", calls)
	}
}
