package dalgo2sql

import (
	"database/sql"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

// Test that NewDatabase panics when db == nil
func TestNewDatabase_PanicsOnNilDB(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when db is nil, got none")
		}
	}()
	schema := newSchema()
	_ = NewDatabase(nil, schema, Options{})
}

// Test that NewDatabase panics when schema == nil
func TestNewDatabase_PanicsOnNilSchema(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when schema is nil, got none")
		}
	}()
	fakeDB := &sql.DB{} // a non-nil pointer is enough as NewDatabase only checks for nil
	_ = NewDatabase(fakeDB, nil, Options{})
}

func newSchema() dal.Schema {
	keyToField := func(key *dal.Key, data any) (fields []dal.ExtraField, err error) {
		return
	}
	dataToKey := func(incompleteKey *dal.Key, data any) (key *dal.Key, err error) {
		return
	}
	return dal.NewSchema(keyToField, dataToKey)
}
