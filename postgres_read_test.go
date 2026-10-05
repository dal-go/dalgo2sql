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
)

// postgresReadCatalogRows answers the catalog query for the sources of a read test:
// Album(AlbumId NOT NULL integer, Title text, Secret text), under the names the
// dialect's mode keeps them.
func postgresReadCatalogRows(relation string, names ...string) *sqlmock.Rows {
	rows := sqlmock.NewRows(postgresCatalogColumns)
	types := map[string][]driver.Value{
		"albumid": {"integer", "N", int64(23), int64(0), true, false},
		"title":   {"text", "S", int64(25), int64(0), false, false},
		"secret":  {"text", "S", int64(25), int64(0), false, false},
		"total":   {"numeric", "N", int64(1700), int64(0), false, false},
	}
	for _, name := range names {
		rows.AddRow(append([]driver.Value{relation, name}, types[strings.ToLower(name)]...)...)
	}
	return rows
}

func newPostgresReadMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func TestPostgresReaderReadsTheFactsThenRunsTheCompiledStatement(t *testing.T) {
	ctx := context.Background()
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).
		WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId", "Title", "Secret"))
	// The wildcard is expanded from the facts, minus the exclusion; ORDER BY a NOT NULL
	// column has no NULLS clause; the constant is bound.
	mock.ExpectQuery(`SELECT "AlbumId", "Title" FROM "Album" WHERE "AlbumId" > $1::bigint ORDER BY "AlbumId" ASC LIMIT $2`).
		WithArgs(int64(7), int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"AlbumId", "Title"}).AddRow(int64(8), "Eight"))
	q := typedTestFrom("Album", "").NewQuery().
		Where(dal.NewComparison(typedTestField("AlbumId"), dal.GreaterThen, typedTestConst(7))).
		OrderBy(dal.Ascending(typedTestField("AlbumId"))).Limit(5).
		SelectColumns(dal.AllColumnsExcept("Secret"))
	reader, err := getReaderBaseWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.rows.Close() }()
	if want := []string{"AlbumId", "Title"}; !reflect.DeepEqual(reader.colNames, want) {
		t.Fatalf("colNames = %v, want %v", reader.colNames, want)
	}
	if want := []int{0, 1}; !reflect.DeepEqual(reader.visibleIndexes, want) {
		t.Fatalf("visibleIndexes = %v, want %v (the exclusion is in the statement, not in the result)", reader.visibleIndexes, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresReaderSelectAllReturnsTheServersColumns(t *testing.T) {
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId", "Title"))
	mock.ExpectQuery(`SELECT * FROM "Album"`).WillReturnRows(sqlmock.NewRows([]string{"AlbumId", "Title"}).AddRow(int64(1), "One"))
	reader, err := getReaderBaseWithOptions(context.Background(), typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.rows.Close() }()
	if want := []string{"AlbumId", "Title"}; !reflect.DeepEqual(reader.colNames, want) {
		t.Fatalf("colNames = %v, want %v", reader.colNames, want)
	}
}

// Under FoldLower the statement names a column in lower case and the server returns
// it so; the reader gives it the name the query asked for, count(*) included, so a
// caller's map is keyed as the caller wrote.
func TestPostgresReaderMapsFoldedOutputNamesBackToTheNamesAsked(t *testing.T) {
	ctx := context.Background()
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(postgresReadCatalogRows(`"album"`, "albumid", "title", "total"))
	mock.ExpectQuery(`SELECT "title", "albumid" AS "id", COUNT(*) AS "count(*)" FROM "album" GROUP BY "title", "albumid"`).
		WillReturnRows(sqlmock.NewRows([]string{"title", "id", "count(*)"}).AddRow("One", int64(1), int64(3)))
	q := typedTestFrom("Album", "").NewQuery().GroupBy(typedTestField("Title"), typedTestField("AlbumId")).
		SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(typedTestField("AlbumId"), "Id"), dal.Count())
	reader, err := getRecordsReaderWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"Title": "One", "Id": int64(1), dal.Count().Expression.String(): int64(3)}
	if got := record.Data(); !reflect.DeepEqual(got, want) {
		t.Fatalf("record = %#v, want %#v", got, want)
	}
}

func TestPostgresRecordsetReaderNamesItsColumnsAsTheQueryAsked(t *testing.T) {
	ctx := context.Background()
	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(postgresReadCatalogRows(`"album"`, "albumid", "title"))
	mock.ExpectQuery(`SELECT "title" FROM "album"`).WillReturnRows(sqlmock.NewRows([]string{"title"}).AddRow("One"))
	q := typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), ""))
	reader, err := getRecordsetReaderWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if name := reader.Recordset().GetColumnByIndex(0).Name(); name != "Title" {
		t.Fatalf("the recordset column is named %q, want Title", name)
	}
}

func TestPostgresReaderRefusesWhatItCannotRunOnTheServer(t *testing.T) {
	ctx := context.Background()
	t.Run("a table that is not there: not found, with a hint, and no statement", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(postgresReadCatalogRows(`"album"`))
		mock.ExpectQuery(postgresSuggestionQuery).WithArgs("").WillReturnRows(newPostgresSuggestionRows([2]string{"public", "Album"}, [2]string{"public", "Artist"}))
		_, err := getReaderBaseWithOptions(ctx, typedTestFrom("album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.Suggestion.Name != "Album" {
			t.Fatalf("error = %v, want a not-found error that suggests Album", err)
		}
		if want := `table "album" not found; did you mean "Album"? Table names are case-sensitive.`; err.Error() != want {
			t.Fatalf("message = %s, want %s", err, want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a qualified x.to_json on a source the catalog does not resolve never reaches the server", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Seq"`).WillReturnRows(postgresReadCatalogRows(`"Seq"`))
		mock.ExpectQuery(postgresSuggestionQuery).WithArgs("").WillReturnRows(newPostgresSuggestionRows())
		q := typedTestFrom("Seq", "x").NewQuery().SelectColumns(typedTestColumn(typedTestQualified("x", "to_json"), "row"))
		_, err := getReaderBaseWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("error = %v, want table not found", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a query the compiler declines is ErrNotSupported and runs nothing", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId"))
		q := typedTestFrom("Album", "").NewQuery().StartFrom("cursor").SelectColumns()
		_, err := getReaderBaseWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("error = %v, want ErrNotSupported", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("an identifier case the package does not define runs nothing", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		_, err := getReaderBaseWithOptions(ctx, typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: "upper"})
		if err == nil || !strings.Contains(err.Error(), `"upper"`) {
			t.Fatalf("error = %v, want the identifier case named", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the catalog lookup failing stops the read", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		boom := errors.New("boom")
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnError(boom)
		_, err := getReaderBaseWithOptions(ctx, typedTestFrom("Album", "").NewQuery().SelectColumns(), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if !errors.Is(err, boom) || errors.Is(err, ErrTableNotFound) {
			t.Fatalf("error = %v, want the lookup's own", err)
		}
	})
	t.Run("a statement that returns another number of columns than the query asked for is an error", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId", "Title"))
		mock.ExpectQuery(`SELECT "Title" FROM "Album"`).WillReturnRows(sqlmock.NewRows([]string{"Title", "extra"}).AddRow("a", "b"))
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), ""))
		_, err := getReaderBaseWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if err == nil || !strings.Contains(err.Error(), "returned 2 columns where the query asked for 1") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("raw SQL text keeps running as written", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery(`SELECT 1`).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(int64(1)))
		reader, err := getReaderBaseWithOptions(ctx, dal.NewTextQuery("SELECT 1", nil), db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
		if err != nil {
			t.Fatal(err)
		}
		_ = reader.rows.Close()
	})
}

// A keys-only query that names no order is ordered by the primary key (the wrapper
// orderedByKey, which answers OrderBy()). The PostgreSQL path reads the order through
// that interface like any other, so the statement carries it; the key is NOT NULL, so
// the NULLS clause is left out and an index on the key can serve the order. No
// COLLATE is written: the key is unique, so the order is total whatever the
// collation, and a collation of its own would keep the planner from the key's index.
func TestPostgresReaderOrdersAKeysOnlyQueryByItsPrimaryKey(t *testing.T) {
	ctx := context.Background()
	albums := dal.NewRootCollectionRef("Album", "")
	options := DbOptions{StructuredQueryDialect: "postgres", Recordsets: map[string]*Recordset{
		"Album": NewRecordset("Album", Table, []dal.FieldRef{dal.Field("AlbumId")}),
	}}
	for _, tc := range []struct {
		name      string
		query     dal.StructuredQuery
		options   DbOptions
		statement string
	}{
		{"no order named, key known", dal.From(albums).NewQuery().SelectKeysOnly(reflect.Int), options, `SELECT * FROM "Album" ORDER BY "AlbumId" ASC`},
		{"an order that is named wins", dal.From(albums).NewQuery().OrderBy(dal.DescendingField("Title")).SelectKeysOnly(reflect.Int), options, `SELECT * FROM "Album" ORDER BY "Title" DESC NULLS LAST`},
		{"key unknown, nothing to order by", dal.From(albums).NewQuery().SelectKeysOnly(reflect.Int), DbOptions{StructuredQueryDialect: "postgres"}, `SELECT * FROM "Album"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId", "Title"))
			mock.ExpectQuery(tc.statement).WillReturnRows(sqlmock.NewRows([]string{"AlbumId", "Title"}))
			reader, err := getRecordsReaderWithOptions(ctx, tc.query, db.QueryContext, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			_ = reader.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
