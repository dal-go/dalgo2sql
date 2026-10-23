package realservers

import (
	"testing"

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
