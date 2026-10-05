package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
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
				// The error is the server's, from the second record: a batch that a check of
				// ours refused before it wrote the first would leave the rows as they were
				// too, and prove nothing about the transaction.
				if err == nil || !strings.Contains(err.Error(), "NOT NULL") ||
					errors.Is(err, ErrNoFieldsToWrite) || errors.Is(err, ErrUnsafeName) {
					t.Fatalf("error = %v, want the server's NOT NULL error", err)
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

// A batch that is refused before any of it is written opens no transaction: the check of the
// batch comes before BEGIN, so not even BEGIN and ROLLBACK reach the database. The transaction
// a caller began is theirs, and is left as it is.
func TestSetMulti_ARefusedBatchOpensNoTransaction(t *testing.T) {
	c := validNames()
	good := func(id string) dalrecord.Record { return c.mapRecord(id) }
	hostile := c
	hostile.collection = "x; DROP TABLE y"
	options := c.options("")
	options.Recordsets["lines_orders"] = nil // declared with nil: not declared
	cases := []struct {
		name   string
		record dalrecord.Record
		want   error
	}{
		{"a name that cannot be written", dalrecord.NewRecordWithData(hostile.key("id1"), map[string]any{"name": "v"}), ErrUnsafeName},
		{"a record with no field to write", dalrecord.NewRecordWithData(c.key("id1"), map[string]any{}), ErrNoFieldsToWrite},
		{"data that is not a struct or a map", dalrecord.NewRecordWithData(c.key("id1"), 5), dal.ErrNotSupported},
		{"a nested key with no declared recordset", dalrecord.NewRecordWithData(nestedNames().key("l1"), map[string]any{"name": "v"}), ErrUndeclaredNestedRecordset},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return false }) {
				err := r.api.SetMulti(context.Background(), []dalrecord.Record{good("id0"), tt.record})
				if !errors.Is(err, tt.want) {
					t.Errorf("%s: error = %v, want one wrapping %v", r.kind, err, tt.want)
				}
				if calls := r.recorder.calls(); len(calls) != 0 {
					t.Errorf("%s: a statement reached the database: %q", r.kind, calls)
				}
				// The transaction handle was begun by keyPathAPIsAnswering, once. The database
				// handle begins its own only for a batch it writes.
				wantBegins := map[string]int{"database": 0, "transaction": 1}[r.kind]
				if got := r.recorder.begun(); got != wantBegins {
					t.Errorf("%s: %d transactions begun, want %d", r.kind, got, wantBegins)
				}
			}
		})
	}
	t.Run("a batch that is written is one transaction", func(t *testing.T) {
		r := keyPathAPIsAnswering(t, options, func(string) bool { return false })[0]
		if err := r.api.SetMulti(context.Background(), []dalrecord.Record{good("id0"), good("id1")}); err != nil {
			t.Fatal(err)
		}
		if got := r.recorder.begun(); got != 1 {
			t.Errorf("%d transactions begun, want 1", got)
		}
	})
}
