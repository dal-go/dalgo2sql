package end2end

import (
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"github.com/dal-go/dalgo2sql/end2end/sqlite"
	"testing"
)

func testEndToEndSQLite(t *testing.T, options dalgo2sql.Options) {
	sqliteDb := sqlite.OpenTestDb(t)
	db := dalgo2sql.NewDatabase(sqliteDb, options)
	end2end.TestDalgoDB(t, db, dal.ErrNotImplementedYet, false)
}
