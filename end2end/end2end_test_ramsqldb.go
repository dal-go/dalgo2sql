package end2end

import (
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"github.com/dal-go/dalgo2sql/end2end/ramsqldb"
	"testing"
)

// TODO: Add Close() to dal.Database
func testEndToEndRAMSQLDB(t *testing.T, options dalgo2sql.Options) {
	ramDB := ramsqldb.OpenTestDb(t)
	db := dalgo2sql.NewDatabase(ramDB, options)
	defer func() {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close database: %v", err)
		}
	}()
	end2end.TestDalgoDB(t, db, dal.ErrNotImplementedYet, false)
}
