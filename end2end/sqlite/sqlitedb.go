package sqlite

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

var openSQLiteDB = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite3", dataSourceName)
}

var executeSql = func(db *sql.DB, query string) (sql.Result, error) {
	return db.Exec(query)
}

var fatalf = func(t *testing.T, format string, args ...any) {
	t.Fatalf(format, args...)
}

func OpenTestDb(t *testing.T) *sql.DB {
	db, err := openSQLiteDB("file::memory:?cache=shared")
	if err != nil {
		fatalf(t, "sql.Open : Error : %s\n", err)
		return nil
	}
	batch := []string{
		"CREATE TABLE DalgoE2E_E2ETest1 (ID1 VARCHAR(10) PRIMARY KEY, StringProp TEXT, IntegerProp INT);",
		"CREATE TABLE DalgoE2E_E2ETest2 (ID VARCHAR(10) PRIMARY KEY, StringProp TEXT, IntegerProp INT);",
		"CREATE TABLE NonExistingKind (ID VARCHAR(10) PRIMARY KEY, StringProp TEXT, IntegerProp INT);",
	}
	for _, b := range batch {
		_, err = executeSql(db, b)
		if err != nil {
			fatalf(t, "sql.Exec: Error: %s\n", err)
			return nil
		}
	}
	return db
}
