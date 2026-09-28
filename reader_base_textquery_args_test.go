package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// TestGetReaderBase_TextQuery_PositionalArgs proves a positional
// dal.QueryArg{Value: ...} (no Name) reaches database/sql as its plain
// Value, not the dal.QueryArg struct itself — against a real, in-memory
// SQLite database (modernc.org/sqlite), not a mock, so a wrong bind value
// is a genuine driver-level error, not a mock-expectation mismatch.
func TestGetReaderBase_TextQuery_PositionalArgs(t *testing.T) {
	db := openTestSQLiteDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)`)
	if _, err := db.Exec(`INSERT INTO widgets (id, name) VALUES (1, 'bolt'), (2, 'nut')`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	query := dal.NewTextQuery("SELECT name FROM widgets WHERE id = ?", nil, dal.QueryArg{Value: 2})
	rb, err := getReaderBase(context.Background(), query, db.QueryContext)
	if err != nil {
		t.Fatalf("getReaderBase: %v", err)
	}
	defer func() { _ = rb.rows.Close() }()

	if !rb.rows.Next() {
		t.Fatalf("expected one row, got none (err: %v)", rb.rows.Err())
	}
	var name string
	if err := rb.rows.Scan(&name); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name != "nut" {
		t.Errorf("name = %q, want %q", name, "nut")
	}
}

// TestGetReaderBase_TextQuery_NamedArgs proves a named
// dal.QueryArg{Name: "id", Value: ...} reaches database/sql as a proper
// sql.NamedArg (via sql.Named), so a native SQL query written with an
// @-prefixed placeholder — the shape pkg/secureread.RunNativeSQL's own
// callers use — actually binds, instead of every row silently matching
// (or the driver rejecting the query outright) because the whole
// dal.QueryArg struct was passed as the bind value.
func TestGetReaderBase_TextQuery_NamedArgs(t *testing.T) {
	db := openTestSQLiteDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)`)
	if _, err := db.Exec(`INSERT INTO widgets (id, name) VALUES (1, 'bolt'), (2, 'nut')`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	query := dal.NewTextQuery("SELECT name FROM widgets WHERE id = @id", nil, dal.QueryArg{Name: "id", Value: 2})
	rb, err := getReaderBase(context.Background(), query, db.QueryContext)
	if err != nil {
		t.Fatalf("getReaderBase: %v", err)
	}
	defer func() { _ = rb.rows.Close() }()

	if !rb.rows.Next() {
		t.Fatalf("expected one row, got none (err: %v)", rb.rows.Err())
	}
	var name string
	if err := rb.rows.Scan(&name); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name != "nut" {
		t.Errorf("name = %q, want %q", name, "nut")
	}

	if rb.rows.Next() {
		t.Errorf("expected exactly one row, got a second one — the @id filter did not apply")
	}
}

// TestGetReaderBase_TextQuery_MixedArgs proves a query mixing a named and a
// positional arg binds both correctly at once.
func TestGetReaderBase_TextQuery_MixedArgs(t *testing.T) {
	db := openTestSQLiteDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)`)
	if _, err := db.Exec(`INSERT INTO widgets (id, name, qty) VALUES (1, 'bolt', 5), (2, 'nut', 5), (3, 'nut', 9)`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	query := dal.NewTextQuery(
		"SELECT id FROM widgets WHERE name = @name AND qty = ?", nil,
		dal.QueryArg{Name: "name", Value: "nut"},
		dal.QueryArg{Value: 5},
	)
	rb, err := getReaderBase(context.Background(), query, db.QueryContext)
	if err != nil {
		t.Fatalf("getReaderBase: %v", err)
	}
	defer func() { _ = rb.rows.Close() }()

	if !rb.rows.Next() {
		t.Fatalf("expected one row, got none (err: %v)", rb.rows.Err())
	}
	var id int
	if err := rb.rows.Scan(&id); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if id != 2 {
		t.Errorf("id = %d, want 2", id)
	}
}

type failingCompiler struct{}

func (failingCompiler) CompileNativeStructuredQuery(query dal.StructuredQuery, fragments NativeJoinHintFragments) (string, []any, error) {
	return "", nil, errors.New("compiler error")
}

func TestReaderBase_AdditionalCoverage(t *testing.T) {
	ctx := context.Background()

	// 1. planWildcardProjection fails: missing source
	qBadProj := dal.From(nil).NewQuery().SelectColumns(dal.AllColumnsExcept("x"))
	_, err := getReaderBaseWithOptions(ctx, qBadProj, nil, DbOptions{})
	if err == nil {
		t.Fatal("expected error from planWildcardProjection")
	}

	// 2. NativeStructuredQueryCompiler returns error
	qValid := dal.From(dal.NewRootCollectionRef("widgets", "")).NewQuery().SelectColumns(dal.Column{Expression: dal.Field("id")})
	_, err = getReaderBaseWithOptions(ctx, qValid, nil, DbOptions{
		NativeStructuredQueryCompiler: failingCompiler{},
	})
	if err == nil {
		t.Fatal("expected error from NativeStructuredQueryCompiler")
	}

	// 3. compileStructuredSQL fails in sqlite dialect
	qBadLimit := dal.From(dal.NewRootCollectionRef("widgets", "")).NewQuery().Limit(-1).SelectIntoRecordset()
	_, err = getReaderBaseWithOptions(ctx, qBadLimit, nil, DbOptions{
		StructuredQueryDialect: "sqlite",
	})
	if err == nil {
		t.Fatal("expected error from compileStructuredSQL")
	}

	// 4. sqliteSourceColumns fails: execute returns error
	qWild := dal.From(dal.NewRootCollectionRef("widgets", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("x"))
	_, err = getReaderBaseWithOptions(ctx, qWild, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return nil, errors.New("exec error")
	}, DbOptions{
		StructuredQueryDialect: "sqlite",
	})
	if err == nil {
		t.Fatal("expected error from sqliteSourceColumns execute")
	}

	// 5. execute succeeds but rows is closed -> rb.rows.Columns() fails
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	rows := sqlmock.NewRows([]string{"id"})
	smock.ExpectQuery("SELECT id FROM widgets").WillReturnRows(rows)
	rowsClosed, _ := sdb.QueryContext(ctx, "SELECT id FROM widgets")
	_ = rowsClosed.Close()

	_, err = getReaderBaseWithOptions(ctx, dal.NewTextQuery("SELECT 1", nil), func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return rowsClosed, nil
	}, DbOptions{})
	if err == nil {
		t.Fatal("expected error from Columns() on closed rows")
	}

	// 6. visibleIndexes error: fewer columns returned than explicit projections
	qWildExplicit := dal.From(dal.NewRootCollectionRef("widgets", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("x"), dal.Column{Expression: dal.Field("a")}, dal.Column{Expression: dal.Field("b")})
	rows1Col := sqlmock.NewRows([]string{"only_one"}).AddRow(1)
	smock.ExpectQuery("SELECT \\* FROM widgets").WillReturnRows(rows1Col)

	_, err = getReaderBaseWithOptions(ctx, qWildExplicit, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return sdb.QueryContext(ctx, "SELECT * FROM widgets")
	}, DbOptions{})
	if err == nil {
		t.Fatal("expected error from visibleIndexes")
	}

	// 7. sqliteSourceColumns: scan error and rows.Err() error
	qTable := dal.From(dal.NewRootCollectionRef("widgets", "")).NewQuery().SelectIntoRecordset()
	// Scan error (string into int)
	rowsScanErr := sqlmock.NewRows([]string{"name", "hidden"}).AddRow("col", "not-an-int")
	smock.ExpectQuery("SELECT name, hidden FROM pragma_table_xinfo").WillReturnRows(rowsScanErr)
	_, err = sqliteSourceColumns(ctx, qTable, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return sdb.QueryContext(ctx, query, args...)
	})
	if err == nil {
		t.Fatal("expected scan error from sqliteSourceColumns")
	}

	// rows.Err() error
	rowsErr := sqlmock.NewRows([]string{"name", "hidden"}).AddRow("col", 0).RowError(0, errors.New("pragma row error"))
	smock.ExpectQuery("SELECT name, hidden FROM pragma_table_xinfo").WillReturnRows(rowsErr)
	_, err = sqliteSourceColumns(ctx, qTable, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return sdb.QueryContext(ctx, query, args...)
	})
	if err == nil {
		t.Fatal("expected rows error from sqliteSourceColumns")
	}
}
