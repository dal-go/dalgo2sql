package dalgo2sql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	dalrecord "github.com/dal-go/record"
	_ "modernc.org/sqlite"
)

// atomicDatabase opens a temporary SQLite file whose name column refuses NULL,
// so that a record can be made to fail on the server and not before.
func atomicDatabase(t *testing.T) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err = raw.Exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return raw
}

func countUsers(t *testing.T, raw *sql.DB) (n int) {
	t.Helper()
	if err := raw.QueryRow("SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// SetMulti on a database is one transaction: a failure on the second record
// leaves the first unwritten, whether the first is a new row or one that is
// there.
func TestDatabaseSetMulti_IsAtomic(t *testing.T) {
	c := validNames()
	for _, dialect := range []string{"", dialectSQLite} {
		for _, firstExists := range []bool{false, true} {
			name := dialect + "/first record is new"
			if firstExists {
				name = dialect + "/first record is there"
			}
			t.Run(name, func(t *testing.T) {
				raw := atomicDatabase(t)
				if _, err := raw.Exec("INSERT INTO users VALUES ('id0', 'before')"); err != nil {
					t.Fatal(err)
				}
				db := &database{db: raw, options: c.options(dialect)}
				first := "id1"
				if firstExists {
					first = "id0"
				}
				err := db.SetMulti(context.Background(), []dalrecord.Record{
					dalrecord.NewRecordWithData(c.key(first), map[string]any{"name": "written"}),
					dalrecord.NewRecordWithData(c.key("id2"), map[string]any{"name": nil}), // the server refuses NULL
				})
				if err == nil {
					t.Fatal("no error")
				}
				if n := countUsers(t, raw); n != 1 {
					t.Errorf("%d rows, want the one that was there", n)
				}
				var name string
				if err = raw.QueryRow("SELECT name FROM users WHERE id = 'id0'").Scan(&name); err != nil || name != "before" {
					t.Errorf("id0 = %q, %v; want it unchanged", name, err)
				}
			})
		}
	}
}

func TestDatabaseSetMulti_CommitsABatchThatSucceeds(t *testing.T) {
	c := validNames()
	raw := atomicDatabase(t)
	db := &database{db: raw, options: c.options("")}
	err := db.SetMulti(context.Background(), []dalrecord.Record{
		dalrecord.NewRecordWithData(c.key("id1"), map[string]any{"name": "a"}),
		dalrecord.NewRecordWithData(c.key("id2"), map[string]any{"name": "b"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := countUsers(t, raw); n != 2 {
		t.Errorf("%d rows, want 2", n)
	}
}

// A batch that cannot start a transaction is an error and writes nothing.
func TestDatabaseSetMulti_ReportsATransactionThatCannotStart(t *testing.T) {
	c := validNames()
	raw := atomicDatabase(t)
	_ = raw.Close()
	db := &database{db: raw, options: c.options("")}
	err := db.SetMulti(context.Background(), []dalrecord.Record{c.mapRecord("id1")})
	if err == nil {
		t.Fatal("no error")
	}
}
