package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
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
	// rowExists, when set, makes the driver succeed instead of failing every
	// statement: a statement that writes affects one row, and a query returns one
	// row when rowExists reports true for its text and none when it reports false.
	// Unset, the driver fails every statement with errReachedDatabase.
	rowExists func(text string) bool
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

// answered reports whether the driver answers statements rather than failing
// them.
func (c recordingConn) answered() bool { return c.recorder.rowExists != nil }

func (recordingConn) Close() error { return nil }

func (recordingConn) Begin() (driver.Tx, error) { return recordingTx{}, nil }

func (c recordingConn) ExecContext(_ context.Context, text string, _ []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(text)
	if c.answered() {
		return driver.RowsAffected(1), nil
	}
	return nil, errReachedDatabase
}

func (c recordingConn) QueryContext(_ context.Context, text string, _ []driver.NamedValue) (driver.Rows, error) {
	c.recorder.record(text)
	if c.answered() {
		rows := &recordedRows{}
		if c.recorder.rowExists(text) {
			rows.remaining = 1
		}
		return rows, nil
	}
	return nil, errReachedDatabase
}

// recordedRows is the one-column result of a query the recording driver
// answers: the given number of rows, each holding the text "1".
type recordedRows struct{ remaining int }

func (*recordedRows) Columns() []string { return []string{"id"} }

func (*recordedRows) Close() error { return nil }

func (r *recordedRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0] = "1"
	return nil
}

type recordingTx struct{}

func (recordingTx) Commit() error   { return nil }
func (recordingTx) Rollback() error { return nil }
