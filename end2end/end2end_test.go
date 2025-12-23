package end2end

import (
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
)

func TestEndToEnd(t *testing.T) {
	options := dalgo2sql.DbOptions{
		Recordsets: map[string]*dalgo2sql.Recordset{
			"DalgoE2E_E2ETest1": dalgo2sql.NewRecordset(
				"E2ETest1",
				dalgo2sql.Table,
				[]dal.FieldRef{dal.Field("ID1")},
			),
			"DalgoE2E_E2ETest2": dalgo2sql.NewRecordset(
				"E2ETest2",
				dalgo2sql.Table,
				[]dal.FieldRef{dal.Field("ID")},
			),
		},
	}
	t.Run("SQLITE_DB", func(t *testing.T) {
		testEndToEndSQLite(t, options)
	})
}
