package dalgo2sql

import (
	"fmt"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
)

// TestConformance runs the shared dalgotest suite against a real (in-memory)
// SQLite database, via the same modernc.org/sqlite driver already used by
// sql_map_get_test.go — no external database or env-gate is needed.
//
// dalgo2sql itself validates nothing (there is no Validate call anywhere in
// this adapter). Every check here passes because dal.NewDB's write pipeline
// runs BeforeSave validation and hooks before this adapter's code is ever
// entered — see database.go's NewDatabase.
func TestConformance(t *testing.T) {
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		ID   TEXT PRIMARY KEY,
		Name TEXT
	)`, dalgotest.DefaultCollection)

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			dalgotest.DefaultCollection: NewRecordset(
				dalgotest.DefaultCollection, Table, []dal.FieldRef{dal.Field("ID")},
			),
		},
		// Without this, "rejects an Insert over an existing key with
		// record.IsAlreadyExists" fails: dalgo2sql cannot recognize a
		// modernc.org/sqlite constraint violation on its own — see
		// sqlite_classifier_test.go and DbOptions.IsAlreadyExists.
		IsAlreadyExists: sqliteIsAlreadyExists,
	}

	dalgotest.RunConformance(t, func(t *testing.T) (dal.DB, func()) {
		sqlDB := openTestSQLiteDB(t, createSQL)
		return NewDatabase(sqlDB, newSchema(), opts), nil
	})
}
