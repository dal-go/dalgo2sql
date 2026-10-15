package dalgo2sql

import (
	"github.com/DATA-DOG/go-sqlmock"
)

// sqliteKeyCatalogStatement is the statement the SQLite dialect asks its catalog for the
// primary key of a source nobody declared a key for.
const sqliteKeyCatalogStatement = `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?)`

// sqliteKeyColumn is one row of the SQLite catalog's answer about a source's columns.
type sqliteKeyColumn struct {
	name     string
	declared string
	notNull  bool
	pk       int // the 1-based position in the primary key, 0 for none
}

// expectSQLiteKeyCatalog expects the catalog statement of the SQLite dialect for table and
// answers it with columns.
func expectSQLiteKeyCatalog(mock sqlmock.Sqlmock, table string, columns ...sqliteKeyColumn) {
	rows := sqlmock.NewRows([]string{"name", "type", "notnull", "pk"})
	for _, column := range columns {
		notNull := 0
		if column.notNull {
			notNull = 1
		}
		rows.AddRow(column.name, column.declared, notNull, column.pk)
	}
	mock.ExpectQuery(sqliteKeyCatalogStatement).WithArgs(table).WillReturnRows(rows)
}
