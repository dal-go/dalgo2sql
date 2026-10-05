package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// A record read from a source nobody declared has a real key. With the typed PostgreSQL
// dialect the catalog facts of the read say what the primary key of the source is, and the
// records are keyed by it; a source with no single-column primary key keys its rows by
// ordinal. The same catalog lookup serves the compiler: it is made once.

// keyColumn is a column of the catalog's answer for one relation.
type keyColumn struct {
	name       string
	primaryKey bool
}

// keyRelation is one relation of the catalog's answer.
type keyRelation struct {
	name    string
	columns []keyColumn
}

// keyCatalogRelations answers the catalog query for the relations given: integer for the
// columns whose name ends in "id" in any case, text for the others.
func keyCatalogRelations(relations ...keyRelation) *sqlmock.Rows {
	rows := sqlmock.NewRows(postgresCatalogColumns)
	for _, relation := range relations {
		for _, column := range relation.columns {
			if strings.HasSuffix(strings.ToLower(column.name), "id") {
				rows.AddRow(relation.name, column.name, "integer", "N", int64(23), int64(0), column.primaryKey, false, column.primaryKey)
			} else {
				rows.AddRow(relation.name, column.name, "text", "S", int64(25), int64(0), false, false, column.primaryKey)
			}
		}
	}
	return rows
}

// keyCatalogRows answers the catalog query for one relation.
func keyCatalogRows(relation string, columns ...keyColumn) *sqlmock.Rows {
	return keyCatalogRelations(keyRelation{relation, columns})
}

// readKeys reads every record of the query and returns the keys the records carry and
// their data, after checking that the database saw exactly what the test expected.
func readKeys(t *testing.T, mock sqlmock.Sqlmock, reader dal.RecordsReader) (keys []any, data []map[string]any) {
	t.Helper()
	defer func() { _ = reader.Close() }()
	for {
		record, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, record.Key().ID)
		data = append(data, record.Data().(map[string]any))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	return keys, data
}

func TestPostgresRecordsReaderKeysAnUndeclaredSourceByItsCatalogPrimaryKey(t *testing.T) {
	ctx := context.Background()
	album := []keyColumn{{"AlbumId", true}, {"Title", false}}
	lower := []keyColumn{{"albumid", true}, {"title", false}}
	for _, tc := range []struct {
		name       string
		identifier IdentifierCase
		relation   string
		catalog    []keyColumn
		query      dal.StructuredQuery
		statement  string
		columns    []string
		rows       [][]driver.Value
		wantKeys   []any
		wantData   []map[string]any
	}{
		{"a select-all", IdentifierCaseExact, `"Album"`, album, typedTestFrom("Album", "").NewQuery().SelectColumns(),
			`SELECT * FROM "Album"`, []string{"AlbumId", "Title"},
			[][]driver.Value{{int64(7), "Seven"}, {int64(8), "Eight"}}, []any{int64(7), int64(8)},
			[]map[string]any{{"AlbumId": int64(7), "Title": "Seven"}, {"AlbumId": int64(8), "Title": "Eight"}}},
		{"a select list that names the key", IdentifierCaseExact, `"Album"`, album,
			typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(typedTestField("AlbumId"), "")),
			`SELECT "Title", "AlbumId" FROM "Album"`, []string{"Title", "AlbumId"},
			[][]driver.Value{{"Seven", int64(7)}}, []any{int64(7)},
			[]map[string]any{{"Title": "Seven", "AlbumId": int64(7)}}},
		{"a select list that leaves the key out gets it as a column of its own", IdentifierCaseExact, `"Album"`, album,
			typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), "")),
			`SELECT "Title", "AlbumId" AS "__dalgo_record_id" FROM "Album"`, []string{"Title", "__dalgo_record_id"},
			[][]driver.Value{{"Seven", int64(7)}, {"Eight", int64(8)}}, []any{int64(7), int64(8)},
			[]map[string]any{{"Title": "Seven"}, {"Title": "Eight"}}},
		{"a keys-only read is ordered by the key", IdentifierCaseExact, `"Album"`, album,
			typedTestFrom("Album", "").NewQuery().SelectKeysOnly(reflect.Int64),
			`SELECT * FROM "Album" ORDER BY "AlbumId" ASC`, []string{"AlbumId", "Title"},
			[][]driver.Value{{int64(7), "Seven"}}, []any{int64(7)},
			[]map[string]any{{"AlbumId": int64(7), "Title": "Seven"}}},
		{"on a fold-lower mount, a select-all", IdentifierCaseFoldLower, `"album"`, lower, typedTestFrom("Album", "").NewQuery().SelectColumns(),
			`SELECT * FROM "album"`, []string{"albumid", "title"},
			[][]driver.Value{{int64(7), "Seven"}}, []any{int64(7)},
			[]map[string]any{{"albumid": int64(7), "title": "Seven"}}},
		{"on a fold-lower mount, a select list that leaves the key out", IdentifierCaseFoldLower, `"album"`, lower,
			typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), "")),
			`SELECT "title", "albumid" AS "__dalgo_record_id" FROM "album"`, []string{"title", "__dalgo_record_id"},
			[][]driver.Value{{"Seven", int64(7)}}, []any{int64(7)},
			[]map[string]any{{"Title": "Seven"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			// One catalog statement: the key and the compiler are answered by the same.
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(tc.relation).WillReturnRows(keyCatalogRows(tc.relation, tc.catalog...))
			result := sqlmock.NewRows(tc.columns)
			for _, row := range tc.rows {
				result.AddRow(row...)
			}
			mock.ExpectQuery(tc.statement).WillReturnRows(result)
			reader, err := getRecordsReaderWithOptions(ctx, tc.query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: tc.identifier})
			if err != nil {
				t.Fatal(err)
			}
			keys, data := readKeys(t, mock, reader)
			if !reflect.DeepEqual(keys, tc.wantKeys) {
				t.Errorf("keys = %v, want %v", keys, tc.wantKeys)
			}
			if !reflect.DeepEqual(data, tc.wantData) {
				t.Errorf("data = %#v, want %#v", data, tc.wantData)
			}
		})
	}
}

// A join is keyed by the base row, as with a key that is configured: the statement
// carries the base's key column, qualified.
func TestPostgresRecordsReaderKeysAJoinByTheCatalogPrimaryKeyOfTheBase(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	// The joined table has a primary key of its own, which is not the key of the records.
	mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"album"`, `"artist"`).WillReturnRows(keyCatalogRelations(
		keyRelation{`"album"`, []keyColumn{{"id", true}, {"title", false}, {"artist_id", false}}},
		keyRelation{`"artist"`, []keyColumn{{"id", true}, {"name", false}}},
	))
	mock.ExpectQuery(`SELECT "a"."title", "a"."id" AS "__dalgo_record_id" FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`).
		WillReturnRows(sqlmock.NewRows([]string{"title", "__dalgo_record_id"}).AddRow("Jazz", int64(1)))
	reader, err := getRecordsReaderWithOptions(context.Background(), pgJoinKeyQuery(typedTestColumn(typedTestQualified("a", "title"), "")),
		db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := readKeys(t, mock, reader)
	if !reflect.DeepEqual(keys, []any{int64(1)}) {
		t.Errorf("keys = %v, want the base's id", keys)
	}
}

// The key of a join's records is the base source's alone. When the base has no primary key the
// rows are keyed by ordinal, whatever key the joined source has: it is never taken for the
// base's, in a select list or in a select-all, and the statement is the one the read always
// sent (no key column is added).
func TestPostgresRecordsReaderKeysAJoinByOrdinalWhenOnlyTheJoinedSourceHasAPrimaryKey(t *testing.T) {
	const from = ` FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`
	for _, tc := range []struct {
		name      string
		query     dal.StructuredQuery
		statement string
		columns   []string
		rows      [][]driver.Value
	}{
		{"a select list", pgJoinKeyQuery(typedTestColumn(typedTestQualified("a", "title"), ""), typedTestColumn(typedTestQualified("r", "name"), "")),
			`SELECT "a"."title", "r"."name"` + from, []string{"title", "name"},
			[][]driver.Value{{"Jazz", "Queen"}, {"Rock", "Queen"}, {"Pop", "Abba"}}},
		{"a select-all", pgJoinKeyQuery(),
			`SELECT *` + from, []string{"id", "title", "artist_id", "id", "name"},
			[][]driver.Value{{int64(1), "Jazz", int64(101), int64(101), "Queen"}, {int64(2), "Rock", int64(101), int64(101), "Queen"}, {int64(3), "Pop", int64(102), int64(102), "Abba"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"album"`, `"artist"`).WillReturnRows(keyCatalogRelations(
				keyRelation{`"album"`, []keyColumn{{"id", false}, {"title", false}, {"artist_id", false}}},
				keyRelation{`"artist"`, []keyColumn{{"id", true}, {"name", false}}},
			))
			result := sqlmock.NewRows(tc.columns)
			for _, row := range tc.rows {
				result.AddRow(row...)
			}
			mock.ExpectQuery(tc.statement).WillReturnRows(result)
			reader, err := getRecordsReaderWithOptions(context.Background(), tc.query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{"0", "1", "2"}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want the ordinals %v: the joined source's key is not the base's", keys, want)
			}
		})
	}
}

// A select-all over a join cannot take a key column, so the record is keyed from the base's own
// column of the key's name, by its position among the columns that lead the result. Both
// sources have a column of that name here, and the base's value is the key where the data
// map, which the later column of a name wins, holds the joined source's.
func TestPostgresRecordsReaderKeysASelectAllJoinByTheCatalogKeyOfTheBaseByPosition(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"album"`, `"artist"`).WillReturnRows(keyCatalogRelations(
		keyRelation{`"album"`, []keyColumn{{"title", false}, {"id", true}, {"artist_id", false}}},
		keyRelation{`"artist"`, []keyColumn{{"id", true}, {"name", false}}},
	))
	mock.ExpectQuery(`SELECT * FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`).
		WillReturnRows(sqlmock.NewRows([]string{"title", "id", "artist_id", "id", "name"}).
			AddRow("Jazz", int64(1), int64(101), int64(101), "Queen").
			AddRow("Rock", int64(2), int64(101), int64(101), "Queen"))
	reader, err := getRecordsReaderWithOptions(context.Background(), pgJoinKeyQuery(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	keys, data := readKeys(t, mock, reader)
	if want := []any{int64(1), int64(2)}; !reflect.DeepEqual(keys, want) {
		t.Errorf("keys = %v, want the base's id %v, not the joined source's", keys, want)
	}
	if got := data[0]["id"]; got != int64(101) {
		t.Errorf("data id = %v, want the later column's value 101", got)
	}
}

// A source with no primary key, a composite one, a view (the catalog reports no primary key
// for it), and a key the mount cannot write, key their rows by ordinal: no two rows of a
// result carry the same key. The statements are the ones such a read always sent.
func TestPostgresRecordsReaderKeysByOrdinalWhereTheCatalogNamesNoKey(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		identifier IdentifierCase
		relation   string
		catalog    []keyColumn
		query      dal.StructuredQuery
		statement  string
	}{
		{"a table with no primary key", IdentifierCaseExact, `"Album"`, []keyColumn{{"AlbumId", false}, {"Title", false}},
			typedTestFrom("Album", "").NewQuery().SelectColumns(), `SELECT * FROM "Album"`},
		{"a view", IdentifierCaseExact, `"Album"`, []keyColumn{{"Title", false}},
			typedTestFrom("Album", "").NewQuery().SelectColumns(), `SELECT * FROM "Album"`},
		{"a composite primary key", IdentifierCaseExact, `"Album"`, []keyColumn{{"AlbumId", true}, {"Title", true}},
			typedTestFrom("Album", "").NewQuery().SelectColumns(), `SELECT * FROM "Album"`},
		{"a primary key the fold-lower mount cannot write", IdentifierCaseFoldLower, `"album"`, []keyColumn{{"Id", true}, {"title", false}},
			typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), "")), `SELECT "title" FROM "album"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(tc.relation).WillReturnRows(keyCatalogRows(tc.relation, tc.catalog...))
			mock.ExpectQuery(tc.statement).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow("a").AddRow("b").AddRow("c"))
			reader, err := getRecordsReaderWithOptions(ctx, tc.query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: tc.identifier})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{"0", "1", "2"}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want the ordinals %v", keys, want)
			}
		})
	}
}

// The collection of an ordinal key is the source's, as for an aggregate.
func TestPostgresRecordsReaderOrdinalKeysAreOfTheSourcesCollection(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(keyCatalogRows(`"Album"`, keyColumn{"Title", false}))
	mock.ExpectQuery(`SELECT * FROM "Album"`).WillReturnRows(sqlmock.NewRows([]string{"Title"}).AddRow("Seven"))
	reader, err := getRecordsReaderWithOptions(context.Background(), typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := record.Key().Collection(); got != "Album" {
		t.Errorf("the key's collection = %q, want Album", got)
	}
	_ = reader.Close()
}

// What names the key wins over the catalog: a recordset declared for the source, whatever
// its primary key (one column, several, none), and DbOptions.PrimaryKey. The catalog says
// AlbumId, and the statement is the one the read sent before the catalog was asked.
func TestPostgresRecordsReaderDoesNotAskTheCatalogForTheKeyOfADeclaredSource(t *testing.T) {
	ctx := context.Background()
	declared := func(fields ...string) DbOptions {
		pk := make([]dal.FieldRef, len(fields))
		for i, field := range fields {
			pk[i] = dal.Field(field)
		}
		return DbOptions{StructuredQueryDialect: "postgres", Recordsets: map[string]*Recordset{"Album": NewRecordset("Album", Table, pk)}}
	}
	for _, tc := range []struct {
		name    string
		options DbOptions
		wantKey any
	}{
		{"a recordset declared with another column", declared("Title"), "Seven"},
		{"a recordset declared with a composite key keeps the literal ID", declared("AlbumId", "Title"), recordIDHelperColumn},
		{"a recordset declared with no key keeps the literal ID", declared(), recordIDHelperColumn},
		{"DbOptions.PrimaryKey", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"Title"}}, "Seven"},
		{"a composite DbOptions.PrimaryKey keeps the literal ID", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"AlbumId", "Title"}}, recordIDHelperColumn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(keyCatalogRows(`"Album"`, keyColumn{"AlbumId", true}, keyColumn{"Title", false}))
			mock.ExpectQuery(`SELECT * FROM "Album"`).WillReturnRows(sqlmock.NewRows([]string{"AlbumId", "Title"}).AddRow(int64(7), "Seven"))
			reader, err := getRecordsReaderWithOptions(ctx, typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if !reflect.DeepEqual(keys, []any{tc.wantKey}) {
				t.Errorf("keys = %v, want %v", keys, tc.wantKey)
			}
		})
	}
}

// A read into a record keeps the key of the record the caller made, and a grouped query keys
// its rows by ordinal, as ever; neither asks the catalog for a key.
func TestPostgresRecordsReaderKeepsTheKeyOfARecordTheQueryNames(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(keyCatalogRows(`"Album"`, keyColumn{"AlbumId", true}, keyColumn{"Title", false}))
	mock.ExpectQuery(`SELECT * FROM "Album"`).WillReturnRows(sqlmock.NewRows([]string{"AlbumId", "Title"}).AddRow(int64(7), "Seven"))
	query := typedTestFrom("Album", "").NewQuery().SelectIntoRecord(func() dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Album", "made-by-the-caller"), map[string]any{})
	})
	reader, err := getRecordsReaderWithOptions(context.Background(), query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := readKeys(t, mock, reader)
	if !reflect.DeepEqual(keys, []any{"made-by-the-caller"}) {
		t.Errorf("keys = %v, want the caller's", keys)
	}
}

func TestPostgresRecordsReaderKeysAGroupedQueryByOrdinal(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(keyCatalogRows(`"Album"`, keyColumn{"AlbumId", true}, keyColumn{"Title", false}))
	mock.ExpectQuery(`SELECT "Title", COUNT(*) AS "COUNT(*)" FROM "Album" GROUP BY "Title"`).
		WillReturnRows(sqlmock.NewRows([]string{"Title", "COUNT(*)"}).AddRow("Seven", int64(2)).AddRow("Eight", int64(1)))
	query := typedTestFrom("Album", "").NewQuery().GroupBy(typedTestField("Title")).
		SelectColumns(typedTestColumn(typedTestField("Title"), ""), dal.Count())
	reader, err := getRecordsReaderWithOptions(context.Background(), query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := readKeys(t, mock, reader)
	if !reflect.DeepEqual(keys, []any{"0", "1"}) {
		t.Errorf("keys = %v, want the ordinals", keys)
	}
}

// The failures of the catalog lookup are the read's, as when the compiler made the lookup:
// an unknown table is a not-found error with a hint, and an identifier case the package does
// not define stops the read before any statement.
func TestPostgresRecordsReaderCatalogFailuresAreTheReadsOwn(t *testing.T) {
	ctx := context.Background()
	t.Run("a table that is not there", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(keyCatalogRows(`"album"`))
		mock.ExpectQuery(postgresSuggestionQuery).WithArgs("").WillReturnRows(newPostgresSuggestionRows([2]string{"public", "Album"}))
		_, err := getRecordsReaderWithOptions(ctx, typedTestFrom("album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.SuggestedName != "Album" {
			t.Fatalf("error = %v, want a not-found error that suggests Album", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("an identifier case the package does not define", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		_, err := getRecordsReaderWithOptions(ctx, typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: "upper"})
		if err == nil || !strings.HasPrefix(err.Error(), "failed to get SQL reader: unsupported identifier case") {
			t.Fatalf("error = %v, want the read's own", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a source the compiler cannot render is its refusal", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		parent := dalrecord.NewKeyWithID("Artist", "a1")
		query := dal.From(dal.NewCollectionRef("Album", "", parent)).NewQuery().SelectColumns()
		_, err := getRecordsReaderWithOptions(ctx, query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("error = %v, want ErrNotSupported", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

// A structured read that is refused is refused before any statement, the catalog lookup
// included: a query with a subquery, which DALgo's own engine runs and this adapter does not,
// and a wildcard projection that cannot be planned. The lookup is a statement, and with a table
// that is not there it would answer with the not-found error and its hint in place of the
// refusal.
func TestPostgresRecordsReaderRefusesBeforeAskingTheCatalogForAKey(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query dal.StructuredQuery
		check func(t *testing.T, err error)
	}{
		{"a query with a subquery", recursiveSQLAdapterQuery(), requireRecursiveAdapterRejection},
		{"a wildcard projection with no exclusion", typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept()), func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "wildcard projection requires at least one exclusion") {
				t.Fatalf("error = %v, want the wildcard projection's refusal", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := newPostgresReadMock(t)
			var statements []string
			execute := func(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
				statements = append(statements, statement)
				return db.QueryContext(ctx, statement, args...)
			}
			reader, err := getRecordsReaderWithOptions(context.Background(), tc.query, execute, DbOptions{StructuredQueryDialect: "postgres"})
			if len(statements) != 0 {
				t.Errorf("the read sent %d statements before it was refused: %q", len(statements), statements)
			}
			if err == nil {
				t.Fatalf("the read returned the reader %v, want a refusal", reader)
			}
			var notFound *TableNotFoundError
			if errors.As(err, &notFound) {
				t.Fatalf("error = %v: the read asked the catalog before it was refused", err)
			}
			tc.check(t, err)
		})
	}
}

// The other dialects have no catalog to ask: a source with no key configured keeps the
// literal ID, and its statement is the one it always was.
func TestRecordsReaderOfOtherDialectsKeepsTheLiteralIDForAnUndeclaredSource(t *testing.T) {
	for _, dialect := range []string{"", "sqlite"} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			statement := "SELECT * FROM Album"
			if dialect == "sqlite" {
				statement = "SELECT * FROM `Album`"
			}
			mock.ExpectQuery(statement).WillReturnRows(sqlmock.NewRows([]string{"AlbumId"}).AddRow(int64(7)).AddRow(int64(8)))
			reader, err := getRecordsReaderWithOptions(context.Background(), typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: dialect})
			if err != nil {
				t.Fatal(err)
			}
			keys, _ := readKeys(t, mock, reader)
			if want := []any{recordIDHelperColumn, recordIDHelperColumn}; !reflect.DeepEqual(keys, want) {
				t.Errorf("keys = %v, want %v", keys, want)
			}
		})
	}
}

// On a fold-lower mount the recordset lookup folds the query's spelling of the source, as the
// catalog lookup does, so a declared recordset is found whatever case the query uses. The
// name as the query spells it is tried first, and the exact mount never folds.
func TestRecordsetForQueryFoldsTheQuerysSpellingOnAFoldLowerMount(t *testing.T) {
	stored := NewRecordset("album", Table, []dal.FieldRef{dal.Field("albumid")})
	spelled := NewRecordset("Album", Table, []dal.FieldRef{dal.Field("AlbumId")})
	fold := DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower}
	exact := DbOptions{StructuredQueryDialect: "postgres"}
	with := func(options DbOptions, recordsets ...*Recordset) DbOptions {
		options.Recordsets = map[string]*Recordset{}
		for _, rs := range recordsets {
			options.Recordsets[rs.Name()] = rs
		}
		return options
	}
	query := func(name string) dal.StructuredQuery { return typedTestFrom(name, "").NewQuery().SelectColumns() }
	for _, tc := range []struct {
		name    string
		options DbOptions
		source  string
		want    *Recordset
		wantKey string
	}{
		{"declared as stored, queried as Album", with(fold, stored), "Album", stored, "albumid"},
		{"declared as stored, queried as ALBUM", with(fold, stored), "ALBUM", stored, "albumid"},
		{"declared as stored, queried as stored", with(fold, stored), "album", stored, "albumid"},
		{"the spelling of the query is tried first", with(fold, stored, spelled), "Album", spelled, "AlbumId"},
		{"declared as spelt, queried as spelt", with(fold, spelled), "Album", spelled, "AlbumId"},
		{"declared in mixed case, queried in another: not found", with(fold, spelled), "ALBUM", nil, ""},
		{"the exact mode does not fold", with(exact, stored), "Album", nil, ""},
		{"the exact mode finds the name as spelt", with(exact, stored), "album", stored, "albumid"},
		{"nothing declared", fold, "Album", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := recordsetForQuery(tc.options, query(tc.source)); got != tc.want {
				t.Errorf("recordsetForQuery = %v, want %v", got, tc.want)
			}
			if got := primaryKeyForQuery(tc.options, query(tc.source)); got != tc.wantKey {
				t.Errorf("primaryKeyForQuery = %q, want %q", got, tc.wantKey)
			}
		})
	}
}

// A recordset declared under the stored name keys the records of a query that spells the name
// in another case: the catalog's key is not used, and the statement is the one the declared
// key gives.
func TestPostgresRecordsReaderFindsADeclaredRecordsetWhateverCaseTheQueryUses(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(keyCatalogRows(`"album"`, keyColumn{"albumid", true}, keyColumn{"title", false}))
	mock.ExpectQuery(`SELECT * FROM "album"`).WillReturnRows(sqlmock.NewRows([]string{"albumid", "title"}).AddRow(int64(7), "Seven"))
	options := DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower, Recordsets: map[string]*Recordset{
		"album": NewRecordset("album", Table, []dal.FieldRef{dal.Field("title")}),
	}}
	reader, err := getRecordsReaderWithOptions(context.Background(), typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, options)
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := readKeys(t, mock, reader)
	if !reflect.DeepEqual(keys, []any{"Seven"}) {
		t.Errorf("keys = %v, want the declared key's value", keys)
	}
}

// typedSourceFacts.primaryKeyColumn names the key when it is exactly one column.
func TestTypedSourceFactsPrimaryKeyColumn(t *testing.T) {
	column := func(name string, primaryKey bool) typedColumnFact {
		return typedColumnFact{Name: name, PrimaryKey: primaryKey}
	}
	for _, tc := range []struct {
		name    string
		columns []typedColumnFact
		want    string
		ok      bool
	}{
		{"one column", []typedColumnFact{column("title", false), column("id", true)}, "id", true},
		{"none", []typedColumnFact{column("title", false)}, "", false},
		{"no columns", nil, "", false},
		{"several", []typedColumnFact{column("a", true), column("b", true)}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := typedSourceFacts{Columns: tc.columns}.primaryKeyColumn()
			if ok != tc.ok || (ok && got.Name != tc.want) {
				t.Errorf("primaryKeyColumn() = %v, %v; want %q, %v", got.Name, ok, tc.want, tc.ok)
			}
		})
	}
}
