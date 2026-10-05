package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// connectionRecorder is a database/sql driver that answers the PostgreSQL catalog
// query and one SELECT, and records which connection each statement ran on.
//
// database/sql may hand a statement to any connection of the pool, and each PostgreSQL
// connection has its own search_path: a catalog lookup that ran on one connection
// describes a different relation than the statement that ran on another. With
// retireAfterQuery set, a connection is not reusable once it has answered a
// statement, so a plain pool gives the next statement a new connection, which is the
// split the reader must not allow.
type connectionRecorder struct {
	mu                sync.Mutex
	connections       int
	statements        []recordedStatement
	retireAfterQuery  bool
	failConnect       error
	failStatementWith func(text string) error
	// statementScanType, when set, is the Go type the SELECT's columns report, in place
	// of the type of their first value.
	statementScanType reflect.Type
	// openRows counts the result sets handed to database/sql that nobody has closed.
	openRows int
	// transactions lets a connection begin a transaction; by default it refuses.
	transactions bool
}

type recordedStatement struct {
	connection int
	text       string
}

// openRowCount is how many result sets are open: opened by a query and not yet closed.
func (r *connectionRecorder) openRowCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.openRows
}

func (r *connectionRecorder) statementLog() []recordedStatement {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedStatement(nil), r.statements...)
}

func (r *connectionRecorder) open(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(connectionRecorderConnector{recorder: r})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type connectionRecorderConnector struct{ recorder *connectionRecorder }

func (c connectionRecorderConnector) Connect(context.Context) (driver.Conn, error) {
	c.recorder.mu.Lock()
	defer c.recorder.mu.Unlock()
	if c.recorder.failConnect != nil {
		return nil, c.recorder.failConnect
	}
	c.recorder.connections++
	return &connectionRecorderConn{recorder: c.recorder, id: c.recorder.connections}, nil
}

func (c connectionRecorderConnector) Driver() driver.Driver { return connectionRecorderDriver{} }

type connectionRecorderDriver struct{}

func (connectionRecorderDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connection recorder: open through the connector")
}

type connectionRecorderConn struct {
	recorder *connectionRecorder
	id       int
	used     bool
}

func (c *connectionRecorderConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("connection recorder: statements go through QueryContext")
}
func (c *connectionRecorderConn) Close() error { return nil }
func (c *connectionRecorderConn) Begin() (driver.Tx, error) {
	if c.recorder.transactions {
		return recordingTx{}, nil
	}
	return nil, errors.New("no transactions")
}

// IsValid is database/sql's check before a connection goes back to the pool.
func (c *connectionRecorderConn) IsValid() bool { return !c.recorder.retireAfterQuery || !c.used }

func (c *connectionRecorderConn) QueryContext(_ context.Context, text string, args []driver.NamedValue) (driver.Rows, error) {
	c.recorder.mu.Lock()
	c.recorder.statements = append(c.recorder.statements, recordedStatement{connection: c.id, text: text})
	fail := c.recorder.failStatementWith
	c.recorder.mu.Unlock()
	c.used = true
	if fail != nil {
		if err := fail(text); err != nil {
			return nil, err
		}
	}
	if strings.HasPrefix(text, "WITH RECURSIVE") {
		// The catalog knows one relation, Album(AlbumId integer NOT NULL primary key, Title text),
		// and answers nothing for any other.
		catalog := &connectionRecorderRows{recorder: c.recorder, columns: []string{"name", "attname", "data_type", "category", "type_oid", "type_elem", "attnotnull", "nondeterministic", "pk"}}
		for _, arg := range args {
			if arg.Value == `"Album"` {
				catalog.rows = [][]driver.Value{
					{`"Album"`, "AlbumId", "integer", "N", int64(23), int64(0), true, false, true},
					{`"Album"`, "Title", "text", "S", int64(25), int64(0), false, false, false},
				}
			}
		}
		return c.recorder.opened(catalog), nil
	}
	if text == postgresSuggestionQuery {
		return c.recorder.opened(&connectionRecorderRows{recorder: c.recorder, columns: []string{"nspname", "relname"}}), nil
	}
	return c.recorder.opened(&connectionRecorderRows{recorder: c.recorder, columns: []string{"AlbumId", "Title"}, rows: [][]driver.Value{{int64(1), "One"}, {int64(2), "Two"}}, scanType: c.recorder.statementScanType}), nil
}

// opened counts rows as open until they are closed.
func (r *connectionRecorder) opened(rows *connectionRecorderRows) driver.Rows {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.openRows++
	return rows
}

type connectionRecorderRows struct {
	recorder *connectionRecorder
	closed   bool
	columns  []string
	rows     [][]driver.Value
	next     int
	scanType reflect.Type // when set, the type every column reports
}

// ColumnTypeScanType tells database/sql, and the recordset reader built on it, the Go
// type of each column from the first row, as a real driver does from the server.
func (r *connectionRecorderRows) ColumnTypeScanType(index int) reflect.Type {
	if r.scanType != nil {
		return r.scanType
	}
	if len(r.rows) == 0 {
		return reflect.TypeOf("")
	}
	return reflect.TypeOf(r.rows[0][index])
}

func (r *connectionRecorderRows) Columns() []string { return r.columns }
func (r *connectionRecorderRows) Close() error {
	r.recorder.mu.Lock()
	defer r.recorder.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.recorder.openRows--
	}
	return nil
}
func (r *connectionRecorderRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

func postgresConnectionQuery() dal.StructuredQuery {
	return typedTestFrom("Album", "").NewQuery().OrderBy(dal.Ascending(typedTestField("AlbumId"))).SelectColumns(dal.AllColumnsExcept("Title"), typedTestColumn(typedTestField("Title"), "t"))
}

func postgresConnectionOptions() DbOptions { return DbOptions{StructuredQueryDialect: "postgres"} }

// The catalog query and the statement run on one connection, though the pool would
// give each a different one.
func TestPostgresReadRunsTheCatalogAndTheStatementOnOneConnection(t *testing.T) {
	ctx := context.Background()
	assertOneConnection := func(t *testing.T, recorder *connectionRecorder) {
		t.Helper()
		log := recorder.statementLog()
		if len(log) != 2 || !strings.HasPrefix(log[0].text, "WITH RECURSIVE") || !strings.HasPrefix(log[1].text, "SELECT ") {
			t.Fatalf("statements = %+v, want the catalog query and then the SELECT", log)
		}
		if log[0].connection != log[1].connection {
			t.Fatalf("the catalog query ran on connection %d and the statement on connection %d", log[0].connection, log[1].connection)
		}
	}

	t.Run("the fake does split them on a plain pool", func(t *testing.T) {
		recorder := &connectionRecorder{retireAfterQuery: true}
		db := recorder.open(t)
		reader, err := getRecordsReaderWithOptions(ctx, postgresConnectionQuery(), db.QueryContext, postgresConnectionOptions())
		if err != nil {
			t.Fatal(err)
		}
		_ = reader.Close()
		log := recorder.statementLog()
		if len(log) != 2 || log[0].connection == log[1].connection {
			t.Fatalf("statements = %+v, want two connections: the control of this test did not split the pool", log)
		}
	})
	t.Run("records reader", func(t *testing.T) {
		recorder := &connectionRecorder{retireAfterQuery: true}
		db := recorder.open(t)
		reader, err := (&database{db: db, options: postgresConnectionOptions()}).ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery())
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		assertOneConnection(t, recorder)
	})
	t.Run("recordset reader", func(t *testing.T) {
		recorder := &connectionRecorder{retireAfterQuery: true}
		db := recorder.open(t)
		backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
		reader, err := backend.ExecuteQueryToRecordsetReader(ctx, postgresConnectionQuery())
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		assertOneConnection(t, recorder)
	})
}

// The connection is the read's for as long as its reader is open, and goes back to the
// pool when the reader is done, however it is done.
func TestPostgresReadGivesTheConnectionBack(t *testing.T) {
	ctx := context.Background()
	newBackend := func(t *testing.T, recorder *connectionRecorder) (*database, *sql.DB) {
		t.Helper()
		db := recorder.open(t)
		return &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}, db
	}
	assertFree := func(t *testing.T, db *sql.DB) {
		t.Helper()
		if stats := db.Stats(); stats.InUse != 0 {
			t.Fatalf("%d connections are still in use: %+v", stats.InUse, stats)
		}
	}
	t.Run("held while the reader is open", func(t *testing.T) {
		backend, db := newBackend(t, &connectionRecorder{})
		reader, err := backend.ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery())
		if err != nil {
			t.Fatal(err)
		}
		if db.Stats().InUse != 1 {
			t.Fatalf("InUse = %d while the reader is open, want 1", db.Stats().InUse)
		}
		_ = reader.Close()
		assertFree(t, db)
	})
	t.Run("a records reader that reads to its end gives it back without Close", func(t *testing.T) {
		backend, db := newBackend(t, &connectionRecorder{})
		reader, err := backend.ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery())
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := reader.Next(); err != nil {
				if !errors.Is(err, dal.ErrNoMoreRecords) {
					t.Fatal(err)
				}
				break
			}
		}
		assertFree(t, db)
		if err := reader.Close(); err != nil { // and closing afterwards is harmless
			t.Fatal(err)
		}
		assertFree(t, db)
	})
	t.Run("a recordset reader that reads to its end gives it back without Close", func(t *testing.T) {
		backend, db := newBackend(t, &connectionRecorder{})
		reader, err := backend.ExecuteQueryToRecordsetReader(ctx, postgresConnectionQuery())
		if err != nil {
			t.Fatal(err)
		}
		for {
			if _, _, err := reader.Next(); err != nil {
				if !errors.Is(err, dal.ErrNoMoreRecords) {
					t.Fatal(err)
				}
				break
			}
		}
		assertFree(t, db)
		_ = reader.Close()
		assertFree(t, db)
	})
	t.Run("a read that fails gives it back: a table that is not there", func(t *testing.T) {
		backend, db := newBackend(t, &connectionRecorder{})
		_, err := backend.ExecuteQueryToRecordsReader(ctx, typedTestFrom("Nowhere", "").NewQuery().SelectColumns())
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("error = %v, want table not found", err)
		}
		assertFree(t, db)
	})
	t.Run("a recordset read that fails gives it back, and returns no reader", func(t *testing.T) {
		backend, db := newBackend(t, &connectionRecorder{})
		reader, err := backend.ExecuteQueryToRecordsetReader(ctx, typedTestFrom("Nowhere", "").NewQuery().SelectColumns())
		if !errors.Is(err, ErrTableNotFound) || reader != nil {
			t.Fatalf("ExecuteQueryToRecordsetReader() = %v, %v; want no reader and table not found", reader, err)
		}
		assertFree(t, db)
	})
	// The recordset reader refuses a column whose Go type it cannot hold after the
	// statement has run, with the rows open: the connection is given back only once they
	// are closed, so a read that returned its error without closing them would wait for
	// itself. The call runs in a goroutine so that a regression fails the test instead of
	// blocking it.
	for _, tc := range []struct {
		name     string
		scanType reflect.Type
		want     string
	}{
		{"a struct it does not know", reflect.TypeOf(struct{}{}), "unsupported type for column"},
		{"a pointer it does not know", reflect.TypeOf(new(int)), "unsupported pointer type for column"},
		{"a kind it does not know", reflect.TypeOf(map[string]int{}), "unsupported column type kind"},
	} {
		t.Run("a recordset read that fails on "+tc.name+" gives it back", func(t *testing.T) {
			backend, db := newBackend(t, &connectionRecorder{statementScanType: tc.scanType})
			type result struct {
				reader dal.RecordsetReader
				err    error
			}
			done := make(chan result, 1)
			go func() {
				reader, err := backend.ExecuteQueryToRecordsetReader(ctx, postgresConnectionQuery())
				done <- result{reader, err}
			}()
			select {
			case got := <-done:
				if got.err == nil || !strings.Contains(got.err.Error(), tc.want) || got.reader != nil {
					t.Fatalf("ExecuteQueryToRecordsetReader() = %v, %v; want no reader and an error that mentions %q", got.reader, got.err, tc.want)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("ExecuteQueryToRecordsetReader() did not return: the rows of the failed read were left open, and the connection waits for them")
			}
			assertFree(t, db)
		})
	}
	// On the pool, database/sql closes the rows and returns the connection when the
	// context of a read ends, whether or not the caller closes the reader. The leased
	// connection does the same. The pool holds one connection, so a second request for
	// one returns only when the read has given its own back; the request runs in a
	// goroutine so that a regression fails the test instead of blocking it.
	t.Run("a context that ends gives it back, though the reader is never closed", func(t *testing.T) {
		for _, read := range []struct {
			name string
			open func(context.Context, *database) error
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
				backend, db := newBackend(t, &connectionRecorder{})
				db.SetMaxOpenConns(1)
				readCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				if err := read.open(readCtx, backend); err != nil {
					t.Fatal(err)
				}
				cancel()
				next := make(chan error, 1)
				go func() {
					conn, err := db.Conn(ctx)
					if err == nil {
						err = conn.Close()
					}
					next <- err
				}()
				select {
				case err := <-next:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the connection of a read whose context ended was not given back")
				}
				assertFree(t, db)
			})
		}
	})
	t.Run("a statement the server rejects gives it back", func(t *testing.T) {
		boom := errors.New("syntax error")
		backend, db := newBackend(t, &connectionRecorder{failStatementWith: func(text string) error {
			if strings.HasPrefix(text, "SELECT ") && text != postgresSuggestionQuery {
				return boom
			}
			return nil
		}})
		if _, err := backend.ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery()); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want the server's", err)
		}
		assertFree(t, db)
	})
	t.Run("no connection to be had is the read's error", func(t *testing.T) {
		boom := errors.New("connection refused")
		backend, _ := newBackend(t, &connectionRecorder{failConnect: boom})
		if _, err := backend.ExecuteQueryToRecordsReader(ctx, postgresConnectionQuery()); !errors.Is(err, boom) {
			t.Fatalf("ExecuteQueryToRecordsReader() error = %v, want the connect error", err)
		}
		if _, err := backend.ExecuteQueryToRecordsetReader(ctx, postgresConnectionQuery()); !errors.Is(err, boom) {
			t.Fatalf("ExecuteQueryToRecordsetReader() error = %v, want the connect error", err)
		}
	})
	t.Run("a text query and another dialect keep the pool", func(t *testing.T) {
		recorder := &connectionRecorder{}
		db := recorder.open(t)
		backend := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: postgresConnectionOptions()}
		reader, err := backend.ExecuteQueryToRecordsReader(ctx, dal.NewTextQuery("SELECT 1", nil))
		if err != nil {
			t.Fatal(err)
		}
		_ = reader.Close()
		other := &database{recordsReaderProvider: recordsReaderProvider{executeQuery: db.QueryContext}, db: db, options: DbOptions{StructuredQueryDialect: "sqlite"}}
		if executor, lease, err := other.readExecutor(ctx, postgresConnectionQuery(), db.QueryContext); err != nil || lease != nil || executor == nil {
			t.Fatalf("readExecutor() = %v, %v, %v; want the pool and no lease", executor != nil, lease, err)
		}
		if got := recorder.statementLog(); len(got) != 1 || got[0].text != "SELECT 1" {
			t.Fatalf("statements = %+v, want only the text query", got)
		}
	})
	t.Run("releasing no lease, or one twice, is harmless", func(t *testing.T) {
		var none *connLease
		none.release()
		backend, db := newBackend(t, &connectionRecorder{})
		_, lease, err := backend.readExecutor(ctx, postgresConnectionQuery(), db.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		lease.release()
		lease.release()
		assertFree(t, db)
	})
}
