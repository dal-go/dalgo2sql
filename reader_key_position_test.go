package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// A record is keyed by the position of the column that holds the key, never by a name that
// two columns of the result share: where the reader knows the column (the catalog's key of a
// select-all, the select item that is the key's own field) it takes that position, and where it
// has only a name it takes the column of exactly that name before one that is the same name
// folded.

// A select-all returns the columns of its source in table order, and two stored names can be
// one name once folded, so only the catalog says which column is the key. The table is read
// with the key first and with the key second, as a select-all and as a select-all over a join,
// which lists the base source's columns from the same catalog.
func TestPostgresRecordsReaderKeysASelectAllByTheCatalogKeyWhenAnotherColumnFoldsToItsName(t *testing.T) {
	const join = ` FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`
	for _, tc := range []struct {
		name      string
		columns   []keyColumn // the album table, in table order
		query     dal.StructuredQuery
		statement string
		result    []string
		row       []driver.Value
		wantKey   any
	}{
		{"the key first", []keyColumn{{"id", true}, {"Id", false}, {"title", false}}, typedTestFrom("album", "").NewQuery().SelectColumns(),
			`SELECT * FROM "album"`, []string{"id", "Id", "title"}, []driver.Value{int64(1), int64(99), "Jazz"}, int64(1)},
		{"the key second", []keyColumn{{"Id", false}, {"id", true}, {"title", false}}, typedTestFrom("album", "").NewQuery().SelectColumns(),
			`SELECT * FROM "album"`, []string{"Id", "id", "title"}, []driver.Value{int64(99), int64(1), "Jazz"}, int64(1)},
		{"a join, the key first", []keyColumn{{"id", true}, {"Id", false}, {"artist_id", false}}, pgJoinKeyQuery(),
			`SELECT *` + join, []string{"id", "Id", "artist_id", "id", "name"}, []driver.Value{int64(1), int64(99), int64(101), int64(101), "Queen"}, int64(1)},
		{"a join, the key second", []keyColumn{{"Id", false}, {"id", true}, {"artist_id", false}}, pgJoinKeyQuery(),
			`SELECT *` + join, []string{"Id", "id", "artist_id", "id", "name"}, []driver.Value{int64(99), int64(1), int64(101), int64(101), "Queen"}, int64(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			if len(tc.query.From().Joins()) == 0 {
				mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(keyCatalogRows(`"album"`, tc.columns...))
			} else {
				mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"album"`, `"artist"`).WillReturnRows(keyCatalogRelations(
					keyRelation{`"album"`, tc.columns},
					keyRelation{`"artist"`, []keyColumn{{"id", true}, {"name", false}}},
				))
			}
			mock.ExpectQuery(tc.statement).WillReturnRows(sqlmock.NewRows(tc.result).AddRow(tc.row...))
			reader, err := getRecordsReaderWithOptions(context.Background(), tc.query, db.QueryContext,
				DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{tc.wantKey}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want %v: the value of the catalog's key column, not of the column that folds to its name", keys, want)
			}
		})
	}
}

// Where only a name says which column is the key, a select-all of a source whose primary key
// the caller declared, the column of exactly that name is the key, whatever order the columns
// come in, and a column that is only the same name folded is not.
func TestPostgresRecordsReaderKeysByTheColumnOfExactlyTheKeysName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  []string
		row     []driver.Value
		wantKey any
	}{
		{"the folded name first", []string{"ID", "id"}, []driver.Value{int64(99), int64(1)}, int64(1)},
		{"the folded name last", []string{"id", "ID"}, []driver.Value{int64(1), int64(99)}, int64(1)},
		{"no column of exactly the name, two that fold to it: the first", []string{"Id", "ID"}, []driver.Value{int64(5), int64(6)}, int64(5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(keyCatalogRows(`"album"`, keyColumn{"id", true}))
			mock.ExpectQuery(`SELECT * FROM "album"`).WillReturnRows(sqlmock.NewRows(tc.result).AddRow(tc.row...))
			reader, err := getRecordsReaderWithOptions(context.Background(), typedTestFrom("album", "").NewQuery().SelectColumns(), db.QueryContext,
				DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower, PrimaryKey: []string{"id"}})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{tc.wantKey}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want %v", keys, want)
			}
		})
	}
}

// A select-all over a join keyed by a name (a declared key) takes the base's column of exactly
// that name, too.
func TestPostgresRecordsReaderKeysASelectAllJoinByTheBasesColumnOfExactlyTheKeysName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []keyColumn
		result  []string
		row     []driver.Value
	}{
		{"the folded name first", []keyColumn{{"ID", false}, {"id", true}, {"artist_id", false}}, []string{"ID", "id", "artist_id", "id", "name"},
			[]driver.Value{int64(99), int64(1), int64(101), int64(101), "Queen"}},
		{"the folded name last", []keyColumn{{"id", true}, {"ID", false}, {"artist_id", false}}, []string{"id", "ID", "artist_id", "id", "name"},
			[]driver.Value{int64(1), int64(99), int64(101), int64(101), "Queen"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"album"`, `"artist"`).WillReturnRows(keyCatalogRelations(
				keyRelation{`"album"`, tc.columns},
				keyRelation{`"artist"`, []keyColumn{{"id", true}, {"name", false}}},
			))
			mock.ExpectQuery(`SELECT * FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`).
				WillReturnRows(sqlmock.NewRows(tc.result).AddRow(tc.row...))
			reader, err := getRecordsReaderWithOptions(context.Background(), pgJoinKeyQuery(), db.QueryContext,
				DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower, PrimaryKey: []string{"id"}})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{int64(1)}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want %v", keys, want)
			}
		})
	}
}

// A select list is keyed by the position of the first item that is the key's own field, not by
// the name the statement gives its columns: a compiler of the caller's writes the statement,
// and the names it gives need not be the query's.
func TestRecordsReaderKeysASelectListByTheItemThatIsTheKeysOwnField(t *testing.T) {
	title := dal.Column{Expression: dal.Field("Label")}
	key := dal.Column{Expression: dal.Field("Code")}
	for _, tc := range []struct {
		name    string
		columns []dal.Column
		wantKey any
	}{
		{"the key first", []dal.Column{key, title}, int64(7)},
		{"the key second", []dal.Column{title, key}, int64(8)},
		{"the key under its own name as an alias", []dal.Column{title, {Expression: dal.Field("Code"), Alias: "Code"}}, int64(8)},
		{"the key twice: the first", []dal.Column{title, key, key}, int64(8)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			result := make([]string, len(tc.columns))
			row := make([]driver.Value, len(tc.columns))
			for i := range result {
				result[i], row[i] = "c"+string(rune('0'+i)), int64(7+i)
			}
			mock.ExpectQuery("SELECT native").WillReturnRows(sqlmock.NewRows(result).AddRow(row...))
			compiler := nativeCompilerFunc(func(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error) {
				return "SELECT native", nil, nil
			})
			q := dal.From(dal.NewRootCollectionRef("Item", "")).NewQuery().SelectColumns(tc.columns...)
			reader, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{NativeStructuredQueryCompiler: compiler, PrimaryKey: []string{"Code"}})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{tc.wantKey}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want %v", keys, want)
			}
		})
	}
}

// One output name for two different expressions of a lone source is refused before any
// statement, whichever compiler writes it, as it is for a join: a record is read by name, one
// value per name, so it would drop one of the two columns.
func TestAReadOfALoneSourceThatGivesOneOutputNameToTwoExpressionsIsRefused(t *testing.T) {
	relabelled := dal.Column{Expression: dal.Field("Label"), Alias: "Code"}
	key := dal.Column{Expression: dal.Field("Code")}
	for _, columns := range [][]dal.Column{{key, relabelled}, {relabelled, key}, {{Expression: dal.Field("Label")}, {Expression: dal.Field("Group"), Alias: "Label"}}} {
		q := dal.From(dal.NewRootCollectionRef("Item", "")).NewQuery().SelectColumns(columns...)
		for _, tc := range []struct {
			name    string
			options DbOptions
		}{
			{"the legacy emitter", DbOptions{PrimaryKey: []string{"Code"}}},
			{"SQLite", DbOptions{StructuredQueryDialect: dialectSQLite}},
			{"PostgreSQL", DbOptions{StructuredQueryDialect: "postgres"}},
		} {
			t.Run(tc.name+"/"+q.String(), func(t *testing.T) {
				db, mock := newPostgresReadMock(t) // expects no statement, the catalog's included
				reader, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, tc.options)
				if err == nil || !strings.Contains(err.Error(), "duplicate output name") {
					t.Errorf("error = %v, want a refusal for a duplicate output name", err)
				}
				if reader != nil && reader.rows != nil {
					t.Error("a read that was refused holds rows")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

// noSourceQuery is a query that names no source.
type noSourceQuery struct{ dal.StructuredQuery }

func (noSourceQuery) From() dal.FromSource { return nil }

// What the refusal does not know it leaves to the compiler, as it was: one expression written
// twice is one column asked twice, a name a wildcard lists is not known before the statement, an
// unaliased expression is named by the server, a statement with joins is refused by its compiler
// (errJoinOutputRepeated), and a query with no source has no columns to name.
func TestRefusedOutputNamesLeavesWhatItDoesNotKnow(t *testing.T) {
	album := dal.From(dal.NewRootCollectionRef("Item", "")).NewQuery()
	count := dal.Column{Expression: dal.NewAggregate(dal.COUNT, false, dal.Field("Code"))}
	for _, tc := range []struct {
		name string
		q    dal.StructuredQuery
	}{
		{"one expression twice", album.SelectColumns(dal.Column{Expression: dal.Field("Label")}, dal.Column{Expression: dal.Field("Label")})},
		{"one expression twice under one alias", album.SelectColumns(dal.Column{Expression: dal.Field("Label"), Alias: "t"}, dal.Column{Expression: dal.Field("Label"), Alias: "t"})},
		{"a wildcard and a column of a name it may list", album.SelectColumns(dal.AllColumnsExcept("Secret"), dal.Column{Expression: dal.Field("Label"), Alias: "Code"})},
		{"expressions with no name", album.SelectColumns(count, count)},
		{"every column named apart", album.SelectColumns(dal.Column{Expression: dal.Field("Label")}, dal.Column{Expression: dal.Field("Group")})},
		{"no columns", album.SelectColumns()},
		{"a join", pgJoinKeyQuery(dal.Column{Expression: dal.NewFieldRef("a", "id")}, dal.Column{Expression: dal.NewFieldRef("r", "id")})},
		{"no source", noSourceQuery{album.SelectColumns()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := refusedOutputNames(tc.q); err != nil {
				t.Errorf("refusedOutputNames = %v, want none", err)
			}
		})
	}
}

func TestKeyColumnIndex(t *testing.T) {
	lower := nameFold(strings.ToLower)
	for _, tc := range []struct {
		name  string
		names []string
		fold  nameFold
		want  int
	}{
		{"exactly the name", []string{"title", "id"}, nil, 1},
		{"no such column", []string{"title"}, nil, -1},
		{"no columns", nil, lower, -1},
		{"only folded: the first", []string{"Id", "ID"}, lower, 0},
		{"exact wins over a folded one before it", []string{"ID", "id"}, lower, 1},
		{"exact wins over a folded one after it", []string{"id", "ID"}, lower, 0},
		{"another case is another name without a fold", []string{"ID"}, nil, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := keyColumnIndex(tc.names, "id", tc.fold); got != tc.want {
				t.Errorf("keyColumnIndex(%v) = %d, want %d", tc.names, got, tc.want)
			}
		})
	}
}

func TestKeyItemIndex(t *testing.T) {
	lower := nameFold(strings.ToLower)
	field := func(name string) dal.Column { return dal.Column{Expression: dal.Field(name)} }
	aliased := func(name, alias string) dal.Column { return dal.Column{Expression: dal.Field(name), Alias: alias} }
	for _, tc := range []struct {
		name    string
		columns []dal.Column
		fold    nameFold
		want    int
	}{
		{"the key's own field", []dal.Column{field("title"), field("id")}, nil, 1},
		{"the first of two", []dal.Column{field("id"), field("title"), field("id")}, nil, 0},
		{"under its own name as an alias", []dal.Column{field("title"), aliased("id", "id")}, nil, 1},
		{"another field under the key's name is not the key", []dal.Column{aliased("title", "id")}, nil, -1},
		{"the key under another name is not the key's own field", []dal.Column{aliased("id", "k")}, nil, -1},
		{"no key", []dal.Column{field("title")}, nil, -1},
		{"an expression is no field", []dal.Column{{Expression: dal.NewAggregate(dal.COUNT, false, dal.Field("id"))}}, nil, -1},
		{"a folded name", []dal.Column{field("ID")}, lower, 0},
		{"exact wins over a folded one before it", []dal.Column{field("ID"), field("id")}, lower, 1},
		{"a wildcard lists a number of columns the list does not say", []dal.Column{dal.AllColumnsExcept("secret"), field("id")}, nil, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := keyItemIndex(tc.columns, "id", tc.fold); got != tc.want {
				t.Errorf("keyItemIndex = %d, want %d", got, tc.want)
			}
		})
	}
}
