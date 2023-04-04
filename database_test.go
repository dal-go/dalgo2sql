package dalgo2sql

import (
	"github.com/dal-go/dalgo2sql/end2end/ramsqldb"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	db := ramsqldb.OpenTestDb(t)
	defer func() {
		err := db.Close()
		if err != nil {
			t.Fatalf("failed to close test DB: %v", err)
		}
	}()
	database := NewDatabase(db, Options{})
	if database == nil {
		t.Fatal("NewDatabase(db) returned nil")
	}
}
