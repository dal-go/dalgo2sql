package dalgo2sql

import (
	"context"
	"testing"

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
