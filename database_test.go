package dalgo2sql

import (
	"database/sql"
	"testing"
)

// Test that NewDatabase panics when db == nil
func TestNewDatabase_PanicsOnNilDB(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when db is nil, got none")
		}
	}()
	_ = NewDatabase(nil, nil, Options{})
}

// Test that NewDatabase panics when schema == nil
func TestNewDatabase_PanicsOnNilSchema(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when schema is nil, got none")
		}
	}()
	fakeDB := &sql.DB{} // non-nil pointer is enough as NewDatabase only checks for nil
	_ = NewDatabase(fakeDB, nil, Options{})
}
