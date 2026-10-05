package dalgo2sql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// The legacy emitter writes names as they are, so a primary key that is a
// reserved word makes the ORDER BY a keys-only query gets a statement the server
// rejects. The query is refused with dal.ErrNotSupported, and no statement is sent.
func TestRecordsReader_KeysOnlyQueryWithAReservedWordKeyIsRefusedByTheLegacyEmitter(t *testing.T) {
	keysOnly := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().SelectKeysOnly(reflect.String)
	for _, key := range []string{"order", "ORDER", "Select", "group", "index", "from"} {
		t.Run(key, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(t, db)
			options := DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field(key)})}}
			_, err = getRecordsReaderWithOptions(context.Background(), keysOnly, db.QueryContext, options)
			if !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("error = %v, want one wrapping dal.ErrNotSupported", err)
			}
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err) // no statement was expected
			}
		})
	}
}

// The refusal is the legacy emitter's: the SQLite compiler quotes the name, and a
// key that is not a reserved word is ordered by as before.
func TestRecordsReader_KeysOnlyQueryWithAReservedWordKeyElsewhere(t *testing.T) {
	keysOnly := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().SelectKeysOnly(reflect.String)
	options := func(dialect, key string) DbOptions {
		return DbOptions{StructuredQueryDialect: dialect, Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field(key)})}}
	}
	if got, want := executedSQL(t, keysOnly, options("sqlite", "order")), "SELECT * FROM `items` ORDER BY (`order` COLLATE BINARY)"; got != want {
		t.Errorf("sqlite: got %q, want %q", got, want)
	}
	if got, want := executedSQL(t, keysOnly, options("", "ordinal")), "SELECT * FROM items\nORDER BY ordinal"; got != want {
		t.Errorf("legacy: got %q, want %q", got, want)
	}
	// A query that selects columns is not a keys-only query: its ORDER BY is the
	// caller's and is not judged by this rule.
	byOrder := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().OrderBy(dal.AscendingField("order")).SelectColumns(dal.Column{Expression: dal.Field("a")})
	if got, want := executedSQL(t, byOrder, DbOptions{}), "SELECT a FROM items\nORDER BY order"; got != want {
		t.Errorf("columns: got %q, want %q", got, want)
	}
}

func TestIsReservedSQLWord(t *testing.T) {
	for _, word := range []string{"select", "SELECT", "Order", "group"} {
		if !isReservedSQLWord(word) {
			t.Errorf("%q is not reported as reserved", word)
		}
	}
	for _, word := range []string{"id", "ordinal", "orders", "Name", ""} {
		if isReservedSQLWord(word) {
			t.Errorf("%q is reported as reserved", word)
		}
	}
}
