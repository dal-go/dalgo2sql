package end2end

import (
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	options := dalgo2sql.Options{
		Recordsets: map[string]*dalgo2sql.Recordset{
			"DalgoE2E_E2ETest1": dalgo2sql.NewRecordset(
				"E2ETest1",
				dalgo2sql.Table,
				[]dal.FieldRef{{Name: "ID1"}},
			),
			"DalgoE2E_E2ETest2": dalgo2sql.NewRecordset(
				"E2ETest2",
				dalgo2sql.Table,
				[]dal.FieldRef{{Name: "ID"}},
			),
		},
	}
	t.Run("RAM_SQL_DB", func(t *testing.T) {
		testEndToEndRAMSQLDB(t, options)
	})
}
