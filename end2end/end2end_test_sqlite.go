package end2end

import (
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"github.com/dal-go/dalgo2sql/end2end/sqlite"
	"testing"
)

func testEndToEndSQLite(t *testing.T, options dalgo2sql.DbOptions) {
	sqliteDb := sqlite.OpenTestDb(t)
	schema := newEnd2EndSchema()
	db := dalgo2sql.NewDatabase(sqliteDb, schema, options)
	end2end.TestDalgoDB(t, db, dal.ErrNotImplementedYet, false)
}

func newEnd2EndSchema() dal.Schema {
	return dalgo2sql.NewSimpleSchema("ID")
}
