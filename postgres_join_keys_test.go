package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// pgJoinKeyCatalog answers the catalog query for album and artist, which both have an id:
// album lists the columns it is given, artist is (id, name).
func pgJoinKeyCatalog(mock sqlmock.Sqlmock, album []string, quote func(string) string) {
	rows := sqlmock.NewRows(postgresCatalogColumns)
	types := map[string][]driver.Value{
		"id":        {"integer", "N", int64(23), int64(0), true, false, false},
		"title":     {"text", "S", int64(25), int64(0), false, false, false},
		"artist_id": {"integer", "N", int64(23), int64(0), false, false, false},
		"name":      {"text", "S", int64(25), int64(0), false, false, false},
	}
	for _, name := range album {
		rows.AddRow(append([]driver.Value{quote("album"), name}, types[name]...)...)
	}
	for _, name := range []string{"id", "name"} {
		rows.AddRow(append([]driver.Value{quote("artist"), name}, types[name]...)...)
	}
	mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(quote("album"), quote("artist")).WillReturnRows(rows)
}

func pgJoinKeyQuery(columns ...dal.Column) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("album", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("artist", "r"), dal.JoinInner, typedTestJoinOn("a", "artist_id", "r", "id")),
	).NewQuery().SelectColumns(columns...)
}

// A join read by the typed PostgreSQL compiler keys each record by the base row too. The
// statement carries the base's key as a column of its own, qualified, even when the
// select list names a column of that name, and the reader takes the key from that column
// by position: the select list can hold the joined table's id under the same name.
func TestPostgresJoinKeysRecordsByTheBaseRow(t *testing.T) {
	ctx := context.Background()
	qualified := typedTestQualified
	col := typedTestColumn
	const from = ` FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`
	exact := func(name string) string { return `"` + name + `"` }
	for _, tc := range []struct {
		name      string
		columns   []dal.Column
		statement string
		result    []string
		row       []driver.Value
		wantKey   any
		wantData  map[string]any
	}{
		{"the joined table's id selected under the key's name", []dal.Column{col(qualified("r", "id"), ""), col(qualified("a", "title"), "")},
			`SELECT "r"."id", "a"."title", "a"."id" AS "__dalgo_record_id"` + from, []string{"id", "title", "__dalgo_record_id"},
			[]driver.Value{int64(101), "Jazz", int64(1)}, int64(1), map[string]any{"id": int64(101), "title": "Jazz"}},
		{"the joined table's id selected last, aliased to the key's name", []dal.Column{col(qualified("a", "title"), ""), col(qualified("r", "id"), "id")},
			`SELECT "a"."title", "r"."id" AS "id", "a"."id" AS "__dalgo_record_id"` + from, []string{"title", "id", "__dalgo_record_id"},
			[]driver.Value{"Jazz", int64(101), int64(1)}, int64(1), map[string]any{"id": int64(101), "title": "Jazz"}},
		{"the base's id selected: the key still comes from the key column", []dal.Column{col(qualified("a", "id"), ""), col(qualified("a", "title"), "")},
			`SELECT "a"."id", "a"."title", "a"."id" AS "__dalgo_record_id"` + from, []string{"id", "title", "__dalgo_record_id"},
			[]driver.Value{int64(1), "Jazz", int64(1)}, int64(1), map[string]any{"id": int64(1), "title": "Jazz"}},
		{"a select list that does not name the key", []dal.Column{col(qualified("a", "title"), ""), col(qualified("r", "name"), "")},
			`SELECT "a"."title", "r"."name", "a"."id" AS "__dalgo_record_id"` + from, []string{"title", "name", "__dalgo_record_id"},
			[]driver.Value{"Jazz", "Queen", int64(1)}, int64(1), map[string]any{"title": "Jazz", "name": "Queen"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			pgJoinKeyCatalog(mock, []string{"id", "title", "artist_id"}, exact)
			mock.ExpectQuery(tc.statement).WillReturnRows(sqlmock.NewRows(tc.result).AddRow(tc.row...))
			reader, err := getRecordsReaderWithOptions(ctx, pgJoinKeyQuery(tc.columns...), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"id"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if got := record.Key().ID; got != tc.wantKey {
				t.Fatalf("record key = %v, want the base's id %v", got, tc.wantKey)
			}
			if got := record.Data(); !reflect.DeepEqual(got, tc.wantData) {
				t.Fatalf("record data = %#v, want %#v (the key column is not part of the data)", got, tc.wantData)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// With no select list the statement is a select-all over every source, the base's columns
// first, and no column can be added to it. The key is the base's column of that name, found
// from the catalog's list of the base's columns, never a column of a joined table.
func TestPostgresJoinSelectAllKeysRecordsByTheBasesColumn(t *testing.T) {
	ctx := context.Background()
	const statement = `SELECT * FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`
	exact := func(name string) string { return `"` + name + `"` }
	for _, tc := range []struct {
		name    string
		options DbOptions
		album   []string
		columns []string
		row     []driver.Value
		wantKey any
		wantID  any
	}{
		{"the base's column of that name", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"id"}},
			[]string{"id", "title", "artist_id"}, []string{"id", "title", "artist_id", "id", "name"},
			[]driver.Value{int64(1), "Jazz", int64(101), int64(101), "Queen"}, int64(1), int64(101)},
		{"the base lists it after another column: the catalog's order, not the first position", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"id"}},
			[]string{"title", "id", "artist_id"}, []string{"title", "id", "artist_id", "id", "name"},
			[]driver.Value{"Jazz", int64(1), int64(101), int64(101), "Queen"}, int64(1), int64(101)},
		{"a key the base has no column for is not taken from the joined table", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"name"}},
			[]string{"id", "title", "artist_id"}, []string{"id", "title", "artist_id", "id", "name"},
			[]driver.Value{int64(1), "Jazz", int64(101), int64(101), "Queen"}, recordIDHelperColumn, int64(101)},
		{"the key spelt in another case, on a fold-lower mount", DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower, PrimaryKey: []string{"ID"}},
			[]string{"id", "title", "artist_id"}, []string{"id", "title", "artist_id", "id", "name"},
			[]driver.Value{int64(1), "Jazz", int64(101), int64(101), "Queen"}, int64(1), int64(101)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			pgJoinKeyCatalog(mock, tc.album, exact)
			mock.ExpectQuery(statement).WillReturnRows(sqlmock.NewRows(tc.columns).AddRow(tc.row...))
			reader, err := getRecordsReaderWithOptions(ctx, pgJoinKeyQuery(), db.QueryContext, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if got := record.Key().ID; got != tc.wantKey {
				t.Fatalf("record key = %v, want %v", got, tc.wantKey)
			}
			// A select-all keeps every column in the data, the key's too, and where two
			// columns share a name the later one is the value, as in DALgo's generic join.
			if got := record.Data().(map[string]any)["id"]; got != tc.wantID {
				t.Fatalf("record data id = %v, want %v", got, tc.wantID)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
