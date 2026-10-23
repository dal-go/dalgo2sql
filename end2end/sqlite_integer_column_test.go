package end2end

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
	_ "github.com/mattn/go-sqlite3"
)

// TestMattnSQLiteRecordsetReadsAFractionInAnIntegerColumn pins the behaviour of the
// recordset reader on mattn/go-sqlite3 (DataTug's driver) when a SQLite INTEGER column
// holds a fraction, which SQLite allows: the read succeeds, as it always has, with the
// value converted to the column's int64. The refusal of a fraction in an int64 column is
// Oracle's alone.
func TestMattnSQLiteRecordsetReadsAFractionInAnIntegerColumn(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:integer_column?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		"CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
		"INSERT INTO t VALUES (1, 2.5), (2, 3)",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	queries := map[string]dal.Query{
		"text":       dal.NewTextQuery("SELECT id, n FROM t ORDER BY id", nil),
		"structured": dal.From(dal.NewRootCollectionRef("t", "")).NewQuery().OrderBy(dal.AscendingField("id")).SelectColumns(dal.Column{Expression: dal.Field("id")}, dal.Column{Expression: dal.Field("n")}),
	}
	for _, dialect := range []string{"", "sqlite"} {
		database := dalgo2sql.NewDatabase(db, dalgo2sql.NewSimpleSchema("id"), dalgo2sql.DbOptions{StructuredQueryDialect: dialect})
		for name, query := range queries {
			reader, err := database.ExecuteQueryToRecordsetReader(context.Background(), query)
			if err != nil {
				t.Fatalf("dialect %q, %s query: %v", dialect, name, err)
			}
			var got []any
			for {
				row, rs, err := reader.Next()
				if err == dal.ErrNoMoreRecords {
					break
				}
				if err != nil {
					t.Fatalf("dialect %q, %s query: %v", dialect, name, err)
				}
				n, _ := row.GetValueByIndex(1, rs)
				got = append(got, n)
			}
			_ = reader.Close()
			if len(got) != 2 || got[0] != int64(2) || got[1] != int64(3) {
				t.Fatalf("dialect %q, %s query: n = %#v, want int64 2 (2.5 read as before) and int64 3", dialect, name, got)
			}
			t.Logf("dialect %q, %s query: n = %#v", dialect, name, got)
		}
	}
}
