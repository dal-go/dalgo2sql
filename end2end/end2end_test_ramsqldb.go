package end2end

import (
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"github.com/dal-go/dalgo2sql/end2end/ramsqldb"
	"testing"
)

func testEndToEndRAMSQLDB(t *testing.T, options dalgo2sql.Options) {
	db := ramsqldb.OpenTestDb(t)
	defer func() {
		_ = db.Close()
	}()
	database := dalgo2sql.NewDatabase(db, options)
	end2end.TestDalgoDB(t, database, dal.ErrNotImplementedYet, false)
}
