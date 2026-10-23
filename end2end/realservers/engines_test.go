package realservers

import (
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v3"
)

// The fixture rows, the same on every engine.
var fixtureRows = []string{
	`INSERT INTO Artist VALUES (1, 'AC/DC')`,
	`INSERT INTO Artist VALUES (2, 'Accept')`,
	`INSERT INTO Artist VALUES (3, NULL)`,
	`INSERT INTO Album VALUES (1, 'For Those About to Rock', 1, 9.99)`,
	`INSERT INTO Album VALUES (2, 'Balls to the Wall', 2, NULL)`,
	`INSERT INTO Album VALUES (3, 'Restless and Wild', 2, 5.00)`,
	`INSERT INTO Album VALUES (4, 'Let There Be Rock', 1, 7.50)`,
}

// fixture writes the rows with the engine's quoting of the two table names.
func fixture(quote func(string) string) []string {
	out := make([]string, len(fixtureRows))
	for i, row := range fixtureRows {
		row = replaceTable(row, "Artist", quote)
		out[i] = replaceTable(row, "Album", quote)
	}
	return out
}

func replaceTable(row, table string, quote func(string) string) string {
	prefix := "INSERT INTO " + table + " "
	if len(row) > len(prefix) && row[:len(prefix)] == prefix {
		return "INSERT INTO " + quote(table) + " " + row[len(prefix):]
	}
	return row
}

// TestRealMySQL runs against MySQL 8.4: DALGO2SQL_MYSQL_DSN, a go-sql-driver/mysql DSN.
func TestRealMySQL(t *testing.T) {
	db := openRealServer(t, "mysql", "DALGO2SQL_MYSQL_DSN")
	quote := func(name string) string { return "`" + name + "`" }
	execAll(t, db, "DROP TABLE IF EXISTS `Album`", "DROP TABLE IF EXISTS `Artist`",
		"CREATE TABLE `Artist` (`ArtistId` INT NOT NULL PRIMARY KEY, `Name` VARCHAR(100) NULL)",
		"CREATE TABLE `Album` (`AlbumId` INT NOT NULL PRIMARY KEY, `Title` VARCHAR(100) NOT NULL, `ArtistId` INT NOT NULL, `Price` DECIMAL(10, 2) NULL)")
	execAll(t, db, fixture(quote)...)
	t.Cleanup(func() { execAll(t, db, "DROP TABLE IF EXISTS `Album`", "DROP TABLE IF EXISTS `Artist`") })
	exerciseTypedDialect(t, db, "mysql", "album")
}

// TestRealOracle runs against Oracle Database Free 23: DALGO2SQL_ORACLE_URL, a go-ora URL.
func TestRealOracle(t *testing.T) {
	db := openRealServer(t, "oracle", "DALGO2SQL_ORACLE_URL")
	quote := func(name string) string { return `"` + name + `"` }
	execAll(t, db, `DROP TABLE IF EXISTS "Album"`, `DROP TABLE IF EXISTS "Artist"`,
		`CREATE TABLE "Artist" ("ArtistId" NUMBER(10) NOT NULL PRIMARY KEY, "Name" VARCHAR2(100))`,
		`CREATE TABLE "Album" ("AlbumId" NUMBER(10) NOT NULL PRIMARY KEY, "Title" VARCHAR2(100) NOT NULL, "ArtistId" NUMBER(10) NOT NULL, "Price" NUMBER(10, 2))`)
	execAll(t, db, fixture(quote)...)
	t.Cleanup(func() { execAll(t, db, `DROP TABLE IF EXISTS "Album"`, `DROP TABLE IF EXISTS "Artist"`) })
	exerciseTypedDialect(t, db, "oracle", "ALBUM")

	// Every shape of NUMBER, read through the recordset reader (DataTug's grid) in both
	// numeric modes: values exact, never truncated, of one Go type per column.
	execAll(t, db, `DROP TABLE IF EXISTS "Numbers"`,
		`CREATE TABLE "Numbers" ("Id" NUMBER(10, 0) NOT NULL PRIMARY KEY, "Price" NUMBER(10, 2), "Star" NUMBER(*, 2), "Free" NUMBER, "Huge" NUMBER(38))`,
		`INSERT INTO "Numbers" VALUES (1, 9.99, 2.5, 2.5, 12345678901234567890123)`)
	t.Cleanup(func() { execAll(t, db, `DROP TABLE IF EXISTS "Numbers"`) })
	q := from("Numbers", "").NewQuery().GroupBy(field("Id"), field("Price"), field("Star"), field("Free"), field("Huge")).
		SelectColumns(column(field("Id"), ""), column(field("Price"), ""), column(field("Star"), ""), column(field("Free"), ""), column(field("Huge"), ""), dal.CountAs(dal.Star(), "n"))
	for _, tc := range []struct {
		exact bool
		want  map[string]any
	}{
		{false, map[string]any{"Id": int64(1), "Price": 9.99, "Star": 2.5, "Free": 2.5, "Huge": 1.2345678901234568e22, "n": int64(1)}},
		{true, map[string]any{"Id": int64(1), "Price": "9.99", "Star": "2.5", "Free": "2.5", "Huge": "12345678901234567890123", "n": int64(1)}},
	} {
		got := recordsetRow(t, dalgo2sql.NewDatabase(db, dalgo2sql.NewSimpleSchema("ID"), dalgo2sql.DbOptions{StructuredQueryDialect: "oracle", ExactNumericValues: tc.exact}), q)
		for name, want := range tc.want {
			if value, present := got[name]; !present || value != want {
				t.Errorf("recordset, exact %v: %s = %T %#v, want %T %#v", tc.exact, name, value, value, want, want)
			}
		}
	}
}

// TestRealSQLServer runs against SQL Server 2022: DALGO2SQL_SQLSERVER_URL, a go-mssqldb
// URL. The default collation ignores case, so the table spelt wrongly is a typo.
func TestRealSQLServer(t *testing.T) {
	db := openRealServer(t, "sqlserver", "DALGO2SQL_SQLSERVER_URL")
	quote := func(name string) string { return "[" + name + "]" }
	execAll(t, db, "DROP TABLE IF EXISTS [Album]", "DROP TABLE IF EXISTS [Artist]",
		"CREATE TABLE [Artist] ([ArtistId] INT NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NULL)",
		"CREATE TABLE [Album] ([AlbumId] INT NOT NULL PRIMARY KEY, [Title] NVARCHAR(100) NOT NULL, [ArtistId] INT NOT NULL, [Price] DECIMAL(10, 2) NULL)")
	execAll(t, db, fixture(quote)...)
	t.Cleanup(func() { execAll(t, db, "DROP TABLE IF EXISTS [Album]", "DROP TABLE IF EXISTS [Artist]") })
	exerciseTypedDialect(t, db, "sqlserver", "Albm")
}
