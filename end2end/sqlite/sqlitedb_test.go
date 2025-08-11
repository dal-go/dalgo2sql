package sqlite

import (
	"database/sql"
	"errors"
	"testing"
)

// helper to get table names from sqlite_master
func getTableNames(t *testing.T, db *sql.DB) map[string]struct{} {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("query sqlite_master failed: %v", err)
	}
	defer rows.Close()
	names := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan name failed: %v", err)
		}
		names[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return names
}

func TestOpenTestDb_CreatesTables(t *testing.T) {
	db := OpenTestDb(t)
	t.Cleanup(func() { _ = db.Close() })

	if db == nil {
		t.Fatalf("expected non-nil *sql.DB")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping failed: %v", err)
	}

	names := getTableNames(t, db)

	expected := []string{
		"DalgoE2E_E2ETest1",
		"DalgoE2E_E2ETest2",
		"NonExistingKind",
	}
	for _, name := range expected {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected table %q to exist; got tables: %v", name, keys(names))
		}
	}
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func getColumns(t *testing.T, db *sql.DB, table string) map[string]string {
	t.Helper()
	// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
	rows, err := db.Query("PRAGMA table_info(\"" + table + "\")")
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s) failed: %v", table, err)
	}
	defer rows.Close()
	cols := map[string]string{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan pragma row failed: %v", err)
		}
		cols[name] = ctype
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return cols
}

func TestOpenTestDb_TableSchemas(t *testing.T) {
	db := OpenTestDb(t)
	t.Cleanup(func() { _ = db.Close() })

	// E2ETest1 has ID1 column, others have ID
	cols1 := getColumns(t, db, "DalgoE2E_E2ETest1")
	for _, want := range []string{"ID1", "StringProp", "IntegerProp"} {
		if _, ok := cols1[want]; !ok {
			t.Fatalf("table DalgoE2E_E2ETest1 missing column %q; have %v", want, cols1)
		}
	}

	cols2 := getColumns(t, db, "DalgoE2E_E2ETest2")
	for _, want := range []string{"ID", "StringProp", "IntegerProp"} {
		if _, ok := cols2[want]; !ok {
			t.Fatalf("table DalgoE2E_E2ETest2 missing column %q; have %v", want, cols2)
		}
	}

	cols3 := getColumns(t, db, "NonExistingKind")
	for _, want := range []string{"ID", "StringProp", "IntegerProp"} {
		if _, ok := cols3[want]; !ok {
			t.Fatalf("table NonExistingKind missing column %q; have %v", want, cols3)
		}
	}
}

func TestOpenTestDb_OpenError(t *testing.T) {
	// Save and restore originals
	origOpen := openSQLiteDB
	origFatal := fatalf
	defer func() { openSQLiteDB = origOpen; fatalf = origFatal }()

	// Stub to return error
	openSQLiteDB = func(dataSourceName string) (*sql.DB, error) {
		return nil, errors.New("forced open error")
	}

	// Capture fatalf without failing the test
	called := false
	var gotMsg string
	fatalf = func(t *testing.T, format string, args ...any) {
		called = true
		gotMsg = "open"
		// do not call t.Fatalf here to avoid failing the test
	}

	db := OpenTestDb(t)
	if db != nil {
		t.Fatalf("expected nil db on open error, got: %v", db)
	}
	if !called || gotMsg != "open" {
		t.Fatalf("expected fatalf to be called for open error, called=%v msg=%q", called, gotMsg)
	}
}

func TestOpenTestDb_ExecError(t *testing.T) {
	// Save and restore originals
	origOpen := openSQLiteDB
	origExec := executeSql
	origFatal := fatalf
	defer func() { openSQLiteDB = origOpen; executeSql = origExec; fatalf = origFatal }()

	// Real open to reach exec phase
	openSQLiteDB = func(dataSourceName string) (*sql.DB, error) {
		return origOpen(dataSourceName)
	}

	// Stub exec to fail on first call
	executeSql = func(db *sql.DB, query string) (sql.Result, error) {
		return nil, errors.New("forced exec error")
	}

	// Capture fatalf without failing the test
	called := false
	var gotMsg string
	fatalf = func(t *testing.T, format string, args ...any) {
		called = true
		gotMsg = "exec"
		// do not call t.Fatalf here to avoid failing the test
	}

	db := OpenTestDb(t)
	if db != nil {
		t.Fatalf("expected nil db on exec error, got: %v", db)
	}
	if !called || gotMsg != "exec" {
		t.Fatalf("expected fatalf to be called for exec error, called=%v msg=%q", called, gotMsg)
	}
}
