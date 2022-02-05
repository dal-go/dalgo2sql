package end2end

import (
	"github.com/strongo/dalgo2sql"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	options := dalgo2sql.Options{
		Recordsets: map[string]dalgo2sql.Recordset{
			"DalgoE2E_E2ETest1": {
				Name:       "E2ETest1",
				Type:       dalgo2sql.Table,
				PrimaryKey: []dalgo2sql.Field{{Name: "ID1"}},
			},
		},
	}
	t.Run("RAMSQLDB", func(t *testing.T) {
		testEndToEndRAMSQLDB(t, options)
	})
}
