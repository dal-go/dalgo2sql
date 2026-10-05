package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// A read whose context has ended returns an error that matches the context's, never the
// one of a connection that is gone (driver.ErrBadConn, sql.ErrConnDone), wherever the
// context ends in the life of a leased connection, and the connection is back in the pool
// whatever happened. The driver below is scripted to end the context at one point and, as a
// driver does whose connection broke with its context, to answer driver.ErrBadConn there.

// endableContext is a context the test ends, with the error it picks: Canceled or
// DeadlineExceeded.
type endableContext struct {
	context.Context
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func newEndableContext() *endableContext {
	return &endableContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *endableContext) Done() <-chan struct{} { return c.done }

func (c *endableContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// end ends the context with err, once.
func (c *endableContext) end(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
		close(c.done)
	}
}

// The points of a read at which its context can end.
const (
	endBeforeTheRead      = "before the read"
	endWhileConnecting    = "while the connection is opened"
	endAfterConnecting    = "right after the connection is taken"
	endDuringTheCatalog   = "during the catalog statement"
	endWhileReadingFacts  = "while the catalog answer is read"
	endBetweenStatements  = "between the catalog statement and the statement"
	endDuringTheStatement = "during the statement"
	// endWhileColumnTypesAreRead is the point between the statement's answer and the
	// reader having the column types of the result: database/sql closes the rows from its own
	// goroutine when the context ends, and the lookup of the column types then fails.
	endWhileColumnTypesAreRead = "while the column types of the result are read"
	endWhileStreaming          = "while the result is streamed"
)

// endingScript drives the driver: it ends ctx, with cause, at point.
type endingScript struct {
	ctx   *endableContext
	cause error
	point string
	// statementRowsClosed is closed when the rows of the statement have been closed, by
	// the reader or by database/sql when the context ends.
	statementRowsClosed chan struct{}
	closeOnce           sync.Once
}

func (s *endingScript) end() { s.ctx.end(s.cause) }

type endingConnector struct{ script *endingScript }

func (c endingConnector) Driver() driver.Driver { return endingDriver{} }

func (c endingConnector) Connect(context.Context) (driver.Conn, error) {
	switch c.script.point {
	case endWhileConnecting:
		c.script.end()
		return nil, driver.ErrBadConn
	case endAfterConnecting:
		c.script.end()
	}
	return &endingConn{script: c.script}, nil
}

type endingDriver struct{}

func (endingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open through the connector")
}

type endingConn struct{ script *endingScript }

func (*endingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("QueryContext only") }
func (*endingConn) Close() error                        { return nil }
func (*endingConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }

func (c *endingConn) QueryContext(ctx context.Context, text string, _ []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, driver.ErrBadConn // the connection broke with the context
	}
	catalog := strings.HasPrefix(text, "WITH RECURSIVE")
	if (catalog && c.script.point == endDuringTheCatalog) || (!catalog && c.script.point == endDuringTheStatement) {
		c.script.end()
		return nil, driver.ErrBadConn
	}
	return &endingRows{script: c.script, catalog: catalog}, nil
}

// endingRows is the catalog's answer, Album(AlbumId primary key, Title), or the two rows
// of the statement.
type endingRows struct {
	script  *endingScript
	catalog bool
	next    int
}

func (r *endingRows) Columns() []string {
	if r.catalog {
		return postgresCatalogColumns
	}
	return []string{"AlbumId", "Title"}
}

// ColumnTypeScanType gives the recordset reader the type of each column, as a real driver
// does from the server: the statement's are an integer and a text.
func (r *endingRows) ColumnTypeScanType(index int) reflect.Type {
	if !r.catalog && index == 0 {
		return reflect.TypeOf(int64(0))
	}
	return reflect.TypeOf("")
}

func (r *endingRows) Close() error {
	if r.catalog && r.script.point == endBetweenStatements {
		r.script.end()
	}
	if !r.catalog {
		r.script.closeOnce.Do(func() { close(r.script.statementRowsClosed) })
	}
	return nil
}

func (r *endingRows) Next(dest []driver.Value) error {
	r.next++
	switch {
	case r.catalog && r.script.point == endWhileReadingFacts:
		r.script.end()
		return driver.ErrBadConn
	case r.catalog && r.next == 1:
		copy(dest, []driver.Value{`"Album"`, "AlbumId", "integer", "N", int64(23), int64(0), true, false, true})
	case r.catalog && r.next == 2:
		copy(dest, []driver.Value{`"Album"`, "Title", "text", "S", int64(25), int64(0), false, false, false})
	case !r.catalog && r.next == 1:
		copy(dest, []driver.Value{int64(1), "One"})
	case !r.catalog && r.next == 2 && r.script.point == endWhileStreaming:
		r.script.end()
		return driver.ErrBadConn
	case !r.catalog && r.next == 2:
		copy(dest, []driver.Value{int64(2), "Two"})
	default:
		return io.EOF
	}
	return nil
}

func TestAReadWhoseContextEndsReturnsTheContextsError(t *testing.T) {
	points := []string{
		endBeforeTheRead, endWhileConnecting, endAfterConnecting, endDuringTheCatalog,
		endWhileReadingFacts, endBetweenStatements, endDuringTheStatement, endWhileColumnTypesAreRead, endWhileStreaming,
	}
	readers := []struct {
		name string
		// read opens the reader and reads it to its end or to the error, and returns
		// the reader's close, if it has one, and that error.
		read func(ctx context.Context, backend *database, query dal.StructuredQuery) (closeReader func(), err error)
	}{
		{"records reader", func(ctx context.Context, backend *database, query dal.StructuredQuery) (func(), error) {
			reader, err := backend.ExecuteQueryToRecordsReader(ctx, query)
			if err != nil {
				return func() {}, err
			}
			for err == nil {
				_, err = reader.Next()
			}
			return func() { _ = reader.Close() }, err
		}},
		{"recordset reader", func(ctx context.Context, backend *database, query dal.StructuredQuery) (func(), error) {
			reader, err := backend.ExecuteQueryToRecordsetReader(ctx, query)
			if err != nil {
				return func() {}, err
			}
			for err == nil {
				_, _, err = reader.Next()
			}
			return func() { _ = reader.Close() }, err
		}},
	}
	for _, point := range points {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			for _, reader := range readers {
				t.Run(fmt.Sprintf("%s/%v/%s", point, cause, reader.name), func(t *testing.T) {
					// The point at which the context ends can meet the hook that gives the
					// connection back in either order: run it a number of times.
					for range 20 {
						script := &endingScript{ctx: newEndableContext(), cause: cause, point: point, statementRowsClosed: make(chan struct{})}
						restore := endTheContextWhenColumnTypesAreRead(script)
						t.Cleanup(restore)
						db := sql.OpenDB(endingConnector{script: script})
						backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
						if point == endBeforeTheRead {
							script.end()
						}
						closeReader, err := reader.read(script.ctx, backend, typedTestFrom("Album", "").NewQuery().SelectColumns())
						closeReader()
						restore()
						if !errors.Is(err, cause) {
							t.Fatalf("error = %v, want one that matches %v", err, cause)
						}
						if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
							t.Fatalf("error = %v: a connection that is gone is not what the caller should hear about", err)
						}
						if errors.Is(err, dal.ErrNoMoreRecords) {
							t.Fatalf("error = %v: a read that was cut short is never a result that ended well", err)
						}
						if stats := db.Stats(); stats.InUse != 0 {
							t.Fatalf("%d connections are still in use: %+v", stats.InUse, stats)
						}
						_ = db.Close()
					}
				})
			}
		}
	}
}

// A read on the pool, which holds no lease, meets the same end between the lookups of the
// names and of the types of its result: the error is the context's, and the rows are closed
// so that their connection is back in the pool.
func TestAPoolReadWhoseContextEndsWhileItsColumnTypesAreReadReturnsTheContextsError(t *testing.T) {
	query := dal.NewTextQuery("SELECT * FROM Album", nil)
	readers := map[string]func(ctx context.Context, backend *database) error{
		"records reader": func(ctx context.Context, backend *database) error {
			_, err := backend.ExecuteQueryToRecordsReader(ctx, query)
			return err
		},
		"recordset reader": func(ctx context.Context, backend *database) error {
			_, err := backend.ExecuteQueryToRecordsetReader(ctx, query)
			return err
		},
	}
	for name, read := range readers {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(fmt.Sprintf("%s/%v", name, cause), func(t *testing.T) {
				for range 20 {
					script := &endingScript{ctx: newEndableContext(), cause: cause, point: endWhileColumnTypesAreRead, statementRowsClosed: make(chan struct{})}
					restore := endTheContextWhenColumnTypesAreRead(script)
					t.Cleanup(restore)
					db := sql.OpenDB(endingConnector{script: script})
					err := read(script.ctx, &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db})
					restore()
					if !errors.Is(err, cause) {
						t.Fatalf("error = %v, want one that matches %v", err, cause)
					}
					if stats := db.Stats(); stats.InUse != 0 {
						t.Fatalf("%d connections are still in use: %+v", stats.InUse, stats)
					}
					_ = db.Close()
				}
			})
		}
	}
}

// endTheContextWhenColumnTypesAreRead makes the lookup of the column types of the statement's
// rows the point at which the script ends the context, when that is its point, and returns
// the function that puts the lookup back. database/sql closes the rows from its own goroutine
// when the context ends, and which of its lookups, the names or the types, that meets first is
// the scheduler's choice, so the lookup is held here until the rows are closed: the error it
// returns is then the one a read meets when the context ends right between the two.
func endTheContextWhenColumnTypesAreRead(script *endingScript) (restore func()) {
	if script.point != endWhileColumnTypesAreRead {
		return func() {}
	}
	original := columnTypesOf
	columnTypesOf = func(rows *sql.Rows) ([]*sql.ColumnType, error) {
		script.end()
		<-script.statementRowsClosed
		return original(rows)
	}
	return func() { columnTypesOf = original }
}

// Only an error that says the connection is gone is explained by the end of the context;
// any other error is the server's, and a context that has not ended explains nothing.
func TestExplainByContext(t *testing.T) {
	ended := newEndableContext()
	ended.end(context.DeadlineExceeded)
	live := newEndableContext()
	serverError := errors.New("syntax error")
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{"no error", ended, nil, nil},
		{"a broken connection, the context ended", ended, driver.ErrBadConn, context.DeadlineExceeded},
		{"a closed connection, the context ended", ended, sql.ErrConnDone, context.DeadlineExceeded},
		{"a broken connection, wrapped", ended, fmt.Errorf("catalog facts: %w", driver.ErrBadConn), context.DeadlineExceeded},
		{"a broken connection, the context live", live, driver.ErrBadConn, driver.ErrBadConn},
		{"a closed connection, the context live", live, sql.ErrConnDone, sql.ErrConnDone},
		{"the server's error, the context ended", ended, serverError, serverError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := explainByContext(tc.ctx, tc.err); got != tc.want && !errors.Is(got, tc.want) {
				t.Errorf("explainByContext = %v, want %v", got, tc.want)
			}
		})
	}
	var noLease *connLease
	if got := noLease.explain(driver.ErrBadConn); got != driver.ErrBadConn {
		t.Errorf("a read on the pool has no lease to explain it: got %v", got)
	}
}

// Closing a *sql.Conn while a statement is being issued on it is a race inside database/sql:
// the statement can be handed a connection that is gone, and fail with a nil dereference. The
// hook that gives the leased connection back when the context ends runs in its own goroutine,
// so the lease must not close the connection while a statement is in flight. A statement that
// is held in the driver shows that the lease is locked meanwhile; no sleep is needed.
func TestALeaseDoesNotCloseTheConnectionUnderAStatementBeingIssued(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	recorder := &connectionRecorder{failStatementWith: func(string) error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	db := recorder.open(t)
	backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
	ctx := context.Background()
	execute, lease, err := backend.readExecutor(ctx, postgresConnectionQuery(), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	issued := make(chan error, 1)
	go func() {
		rows, err := execute(ctx, "SELECT 1")
		if rows != nil {
			_ = rows.Close()
		}
		issued <- err
	}()
	<-entered
	if lease.mu.TryLock() {
		lease.mu.Unlock()
		t.Error("the lease can be closed while a statement is being issued on its connection")
	}
	close(release)
	if err := <-issued; err != nil {
		t.Fatal(err)
	}
	lease.release()
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("%d connections are still in use: %+v", stats.InUse, stats)
	}
}
