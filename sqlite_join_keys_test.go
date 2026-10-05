package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// A join keys each record by the base row, the row of the first source of FROM. DALgo's
// generic join does (dal/join_execute.go), and the adapter's native read must give the
// same keys, whichever columns the select list names: it used to key a record by any
// column that carried the key's name, the joined table's included. album and artist both
// have an id, with values that differ, so a key from the wrong table shows.
func TestSQLiteJoinKeysRecordsByTheBaseRowLikeTheGenericEngine(t *testing.T) {
	ctx := context.Background()
	qualified := typedTestQualified
	col := typedTestColumn
	for _, tc := range []struct {
		name    string
		columns []dal.Column
		data    []map[string]any
	}{
		{"a select list that does not name the key", []dal.Column{col(qualified("a", "title"), ""), col(qualified("r", "name"), "")},
			[]map[string]any{{"title": "Jazz", "name": "Queen"}, {"title": "Arrival", "name": "Abba"}}},
		{"the joined table's key column selected first", []dal.Column{col(qualified("r", "id"), ""), col(qualified("a", "title"), "")},
			[]map[string]any{{"id": int64(101), "title": "Jazz"}, {"id": int64(102), "title": "Arrival"}}},
		{"the joined table's key column selected last, under the key's name", []dal.Column{col(qualified("a", "title"), ""), col(qualified("r", "id"), "id")},
			[]map[string]any{{"id": int64(101), "title": "Jazz"}, {"id": int64(102), "title": "Arrival"}}},
		{"the base's key column selected", []dal.Column{col(qualified("a", "id"), ""), col(qualified("a", "title"), "")},
			[]map[string]any{{"id": int64(1), "title": "Jazz"}, {"id": int64(2), "title": "Arrival"}}},
		{"both key columns, one renamed", []dal.Column{col(qualified("a", "id"), "album"), col(qualified("r", "id"), "artist")},
			[]map[string]any{{"album": int64(1), "artist": int64(101)}, {"album": int64(2), "artist": int64(102)}}},
		{"no select list: every column of both tables", nil,
			[]map[string]any{{"id": int64(101), "title": "Jazz", "artist_id": int64(101), "name": "Queen"}, {"id": int64(102), "title": "Arrival", "artist_id": int64(102), "name": "Abba"}}},
	} {
		for _, configured := range []struct {
			name    string
			options DbOptions
		}{
			{"a key for every table", DbOptions{PrimaryKey: []string{"id"}}},
			{"the base table's recordset", DbOptions{Recordsets: map[string]*Recordset{"album": NewRecordset("album", Table, []dal.FieldRef{dal.Field("id")})}}},
		} {
			t.Run(tc.name+", "+configured.name, func(t *testing.T) {
				raw := openNullTestDB(t, joinKeyScript)
				options := configured.options
				options.StructuredQueryDialect = "sqlite"
				native := NewDatabase(raw, newSchema(), options)
				options.NativeJoinEligibility = func(context.Context, dal.StructuredQuery) error { return errors.New("generic engine only") }
				generic := NewDatabase(raw, newSchema(), options)

				q := joinKeyQuery(tc.columns...)
				wantKeys, _, err := readJoinRecords(ctx, generic, q)
				if err != nil {
					t.Fatalf("the generic engine: %v", err)
				}
				if wantKeys[0] != int64(1) || wantKeys[1] != int64(2) {
					t.Fatalf("the generic engine keyed the records %v, not by the base's ids: the reference of this test is gone", wantKeys)
				}
				gotKeys, gotData, err := readJoinRecords(ctx, native, q)
				if err != nil {
					t.Fatalf("the native read: %v", err)
				}
				if !reflect.DeepEqual(gotKeys, wantKeys) {
					t.Fatalf("native keys = %v, generic keys = %v", gotKeys, wantKeys)
				}
				// The data is compared with what each column holds, not with the generic
				// engine's, which hands numbers it selects by field over as float64.
				if !reflect.DeepEqual(gotData, tc.data) {
					t.Fatalf("native data = %#v, want %#v", gotData, tc.data)
				}
			})
		}
	}
}

// With no select list the statement is a select-all over both tables, base first. A key
// that is not a column of the base is a misconfiguration that no column of another table
// may stand in for: the generic engine leaves the records with the key it gives a record
// whose key column is missing, and so does the native read.
func TestSQLiteJoinSelectAllNeverTakesAnotherTablesColumnForTheKey(t *testing.T) {
	ctx := context.Background()
	raw := openNullTestDB(t, joinKeyScript)
	options := DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"name"}} // artist's, not album's
	native := NewDatabase(raw, newSchema(), options)
	options.NativeJoinEligibility = func(context.Context, dal.StructuredQuery) error { return errors.New("generic engine only") }
	generic := NewDatabase(raw, newSchema(), options)

	wantKeys, _, err := readJoinRecords(ctx, generic, joinKeyQuery())
	if err != nil {
		t.Fatal(err)
	}
	gotKeys, _, err := readJoinRecords(ctx, native, joinKeyQuery())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("native keys = %v, generic keys = %v: a column of the joined table keyed the records", gotKeys, wantKeys)
	}
}

// The SQLite dialect reads the base's columns from its catalog, in one statement before
// the join's, only for a select-all over joins that is keyed from them. Every other read
// sends the statements it always sent.
func TestSQLiteJoinSelectAllReadsTheBaseColumnsOnlyWhenItKeysByThem(t *testing.T) {
	ctx := context.Background()
	baseColumns := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"name", "hidden"}).AddRow("id", int64(0)).AddRow("title", int64(0)).AddRow("artist_id", int64(0))
	}
	selectAll := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "title", "artist_id", "id", "name"}).AddRow(int64(1), "Jazz", int64(101), int64(101), "Queen")
	}
	named := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"title", "__dalgo_record_id"}).AddRow("Jazz", int64(1))
	}
	keyed := DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"id"}}
	unkeyed := DbOptions{StructuredQueryDialect: "sqlite"}
	selectList := joinKeyQuery(typedTestColumn(typedTestQualified("a", "title"), ""))

	for _, tc := range []struct {
		name    string
		query   dal.StructuredQuery
		options DbOptions
		results []*sqlmock.Rows
		catalog bool
	}{
		{"keyed select-all", joinKeyQuery(), keyed, []*sqlmock.Rows{baseColumns(), selectAll()}, true},
		{"select-all, no key configured", joinKeyQuery(), unkeyed, []*sqlmock.Rows{selectAll()}, false},
		{"a select list, keyed", selectList, keyed, []*sqlmock.Rows{named()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statements := executedStatements(t, tc.query, tc.options, tc.results...)
			if tc.catalog != (len(statements) == 2 && strings.Contains(statements[0], "pragma_table_xinfo")) || len(statements) != len(tc.results) {
				t.Fatalf("statements = %q, want the base's columns first: %v", statements, tc.catalog)
			}
		})
	}

	t.Run("the keyed select-all takes the key from the base's column", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, db)
		mock.ExpectQuery("pragma_table_xinfo").WillReturnRows(baseColumns())
		mock.ExpectQuery("SELECT").WillReturnRows(selectAll())
		reader, err := getRecordsReaderWithOptions(ctx, joinKeyQuery(), db.QueryContext, keyed)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		record, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := record.Key().ID; got != int64(1) {
			t.Fatalf("record key = %v, want the base's id", got)
		}
	})
	t.Run("a catalog that cannot be read is the read's error, and no statement follows", func(t *testing.T) {
		boom := errors.New("no such table")
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, db)
		mock.ExpectQuery("pragma_table_xinfo").WillReturnError(boom)
		_, err = getRecordsReaderWithOptions(ctx, joinKeyQuery(), db.QueryContext, keyed)
		if !errors.Is(err, boom) || !strings.Contains(err.Error(), "failed to inspect SQLite join source") {
			t.Fatalf("error = %v, want the catalog's error, named", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

// The base's columns come from the base's own schema. album exists in main and in an attached
// database, with its columns in another order; the join reads aux.album, so the key of each
// record is the id of that table, found at the position it has there. The lookup used to
// send the table name only, which SQLite resolves to the first table of that name, in main.
func TestSQLiteJoinSelectAllKeysRecordsByTheBasesColumnOfAnAttachedDatabase(t *testing.T) {
	raw := openNullTestDB(t, `ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.album (title TEXT, id INTEGER PRIMARY KEY, artist_id INTEGER);
CREATE TABLE album (id INTEGER PRIMARY KEY, title TEXT, artist_id INTEGER);
CREATE TABLE artist (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO artist VALUES (101, 'Queen'), (102, 'Abba');
INSERT INTO aux.album VALUES ('Jazz', 1, 101), ('Arrival', 2, 102);
INSERT INTO album VALUES (901, 'Main', 101);`)
	query := dal.From(dal.NewQualifiedRootCollectionRef("aux", "album", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("artist", "r"), dal.JoinInner, typedTestJoinOn("a", "artist_id", "r", "id")),
	).NewQuery().OrderBy(dal.Ascending(typedTestQualified("a", "id"))).SelectColumns()
	database := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"id"}})
	keys, _, err := readJoinRecords(context.Background(), database, query)
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{int64(1), int64(2)}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys = %v, want the ids of aux.album %v", keys, want)
	}
}

// A base in a schema is asked for by name and schema; one without a schema keeps the
// statement it always had.
func TestSQLiteSourceColumnsAskForTheBasesSchema(t *testing.T) {
	for _, tc := range []struct {
		name      string
		source    dal.CollectionRef
		statement string
		args      []driver.Value
	}{
		{"no schema", dal.NewRootCollectionRef("album", ""), "SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid", []driver.Value{"album"}},
		{"a schema", dal.NewQualifiedRootCollectionRef("aux", "album", ""), "SELECT name, hidden FROM pragma_table_xinfo(?, ?) ORDER BY cid", []driver.Value{"album", "aux"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(t, db)
			mock.ExpectQuery(tc.statement).WithArgs(tc.args...).WillReturnRows(sqlmock.NewRows([]string{"name", "hidden"}).AddRow("id", 0))
			names, err := sqliteSourceColumns(context.Background(), dal.From(tc.source).NewQuery().SelectColumns(), db.QueryContext)
			if err != nil || !reflect.DeepEqual(names, []string{"id"}) {
				t.Fatalf("source columns = %v, err = %v", names, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("a base that is not a table is an error", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, db)
		query := dal.From(dal.NewQuerySource(nil, "derived")).NewQuery().SelectColumns()
		if _, err := sqliteSourceColumns(context.Background(), query, db.QueryContext); err == nil || !strings.Contains(err.Error(), "has no SQLite table metadata") {
			t.Fatalf("error = %v, want the refusal of a source that is not a table", err)
		}
	})
}
