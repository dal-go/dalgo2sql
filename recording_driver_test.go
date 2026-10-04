package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
)

// errReachedDatabase is what the recording driver answers to every statement:
// a test that sees it in an error chain knows the statement left the adapter.
var errReachedDatabase = errors.New("recording driver: statement reached the database")

// statementRecorder is the recording executor of the name-safety tests. It
// counts every statement that reaches the database/sql driver, whichever
// database/sql entry point (Query, Exec, a prepared statement or a
// transaction) the adapter used.
type statementRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *statementRecorder) record(text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, text)
}

func (r *statementRecorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statements...)
}

// open returns a database whose driver records every statement and fails it.
func (r *statementRecorder) open(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(recordingConnector{recorder: r})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type recordingConnector struct{ recorder *statementRecorder }

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return recordingConn(c), nil
}

func (c recordingConnector) Driver() driver.Driver { return recordingDriver{} }

type recordingDriver struct{}

func (recordingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("recording driver: open through the connector")
}

type recordingConn struct{ recorder *statementRecorder }

func (c recordingConn) Prepare(text string) (driver.Stmt, error) {
	c.recorder.record(text)
	return nil, errReachedDatabase
}

func (recordingConn) Close() error { return nil }

func (recordingConn) Begin() (driver.Tx, error) { return recordingTx{}, nil }

func (c recordingConn) ExecContext(_ context.Context, text string, _ []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(text)
	return nil, errReachedDatabase
}

func (c recordingConn) QueryContext(_ context.Context, text string, _ []driver.NamedValue) (driver.Rows, error) {
	c.recorder.record(text)
	return nil, errReachedDatabase
}

type recordingTx struct{}

func (recordingTx) Commit() error   { return nil }
func (recordingTx) Rollback() error { return nil }
