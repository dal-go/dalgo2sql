package dalgo2sql

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

var postgresCatalogColumns = []string{"name", "attname", "data_type", "category", "type_oid", "attnotnull", "nondeterministic"}

// newPostgresCatalogMock returns a database that answers only what the test
// expects, matching the query text exactly.
func newPostgresCatalogMock(t *testing.T) (executeQueryFunc, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db.QueryContext, mock
}

func TestPostgresCatalogQueryGolden(t *testing.T) {
	var golden strings.Builder
	for _, n := range []int{1, 2, 3} {
		golden.WriteString("=== " + strconv.Itoa(n) + " source(s)\n" + postgresCatalogQuery(n) + "\n")
	}
	assertPostgresGolden(t, "catalog-query.golden", golden.String())
}

// TestPostgresCatalogQueryTextDependsOnTheNumberOfSourcesOnly: the bound texts
// are arguments, so no name of a source is written into the query.
func TestPostgresCatalogQueryTextDependsOnTheNumberOfSourcesOnly(t *testing.T) {
	query := postgresCatalogQuery(2)
	for _, fragment := range []string{"to_regclass(s.name)", "($1::text), ($2::text)", "pg_class", "pg_attribute", "pg_type", "pg_collation"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("catalog query does not contain %q:\n%s", fragment, query)
		}
	}
	if strings.Count(query, "$") != 2 {
		t.Fatalf("catalog query binds %d markers, want 2:\n%s", strings.Count(query, "$"), query)
	}
}

func TestPostgresCatalogFactsReadsOneQueryForEverySource(t *testing.T) {
	ctx := context.Background()
	execute, mock := newPostgresCatalogMock(t)
	mock.ExpectQuery(postgresCatalogQuery(2)).
		WithArgs(`"Album"`, `"sales"."Artist"`).
		WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
			AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), true, false).
			AddRow(`"Album"`, "Title", "text", "S", int64(25), true, false).
			AddRow(`"Album"`, "Cover", "bytea", "U", int64(17), false, false).
			AddRow(`"sales"."Artist"`, "ArtistId", "bigint", "N", int64(20), true, false).
			AddRow(`"sales"."Artist"`, "Name", "citext", "S", int64(16400), false, true))
	facts, err := newPostgresDialect(postgresExact).catalogFacts(ctx, execute, []typedSourceName{{Name: "Album"}, {Schema: "sales", Name: "Artist"}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[typedSourceName]typedSourceFacts{
		{Name: "Album"}: {Columns: []typedColumnFact{
			{Name: "AlbumId", DataType: "integer", Category: typedTypeNumber, NotNull: true},
			{Name: "Title", DataType: "text", Category: typedTypeText, NotNull: true},
			{Name: "Cover", DataType: "bytea", Category: typedTypeBinary},
		}},
		{Schema: "sales", Name: "Artist"}: {Columns: []typedColumnFact{
			{Name: "ArtistId", DataType: "bigint", Category: typedTypeNumber, NotNull: true},
			{Name: "Name", DataType: "citext", Category: typedTypeText, NonDeterministicCollation: true},
		}},
	}
	if !reflect.DeepEqual(facts.Sources, want) {
		t.Fatalf("Sources = %#v\nwant      %#v", facts.Sources, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresCatalogFactsAreKeyedByTheQuerysOwnSpelling: the compiler looks a
// source up by the schema and name the query wrote, folded, so an unqualified
// source is keyed with no schema although the server finds it through search_path,
// and in FoldLower mode both parts are folded.
func TestPostgresCatalogFactsAreKeyedByTheQuerysOwnSpelling(t *testing.T) {
	cases := []struct {
		name     string
		mode     postgresIdentifierMode
		source   typedSourceName
		wantText string
		wantKey  typedSourceName
	}{
		{"exact, unqualified", postgresExact, typedSourceName{Name: "Album"}, `"Album"`, typedSourceName{Name: "Album"}},
		{"exact, qualified", postgresExact, typedSourceName{Schema: "Sales", Name: "Album"}, `"Sales"."Album"`, typedSourceName{Schema: "Sales", Name: "Album"}},
		{"fold, unqualified", postgresFoldLower, typedSourceName{Name: "Album"}, `"album"`, typedSourceName{Name: "album"}},
		{"fold, qualified", postgresFoldLower, typedSourceName{Schema: "Sales", Name: "ALBUM"}, `"sales"."album"`, typedSourceName{Schema: "sales", Name: "album"}},
		{"a name that holds a dot and quotes", postgresExact, typedSourceName{Schema: `a.b`, Name: `c"d`}, `"a.b"."c""d"`, typedSourceName{Schema: `a.b`, Name: `c"d`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			execute, mock := newPostgresCatalogMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(c.wantText).
				WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).AddRow(c.wantText, "id", "integer", "N", int64(23), true, false))
			facts, err := newPostgresDialect(c.mode).catalogFacts(context.Background(), execute, []typedSourceName{c.source})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := facts.Sources[c.wantKey]; !ok || len(facts.Sources) != 1 {
				t.Fatalf("Sources = %v, want one entry keyed %+v", facts.Sources, c.wantKey)
			}
			// The compiler's own lookup, by the query's spelling, finds it.
			if _, ok := facts.source(c.source); !ok {
				t.Fatalf("facts.source(%+v) found nothing", c.source)
			}
		})
	}
}

func TestPostgresCatalogFactsAsksOnceForSpellingsThatWriteTheSameName(t *testing.T) {
	execute, mock := newPostgresCatalogMock(t)
	mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).
		WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).AddRow(`"album"`, "id", "integer", "N", int64(23), false, false))
	facts, err := newPostgresDialect(postgresFoldLower).catalogFacts(context.Background(), execute, []typedSourceName{{Name: "Album"}, {Name: "ALBUM"}, {Name: "album"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Sources) != 1 {
		t.Fatalf("Sources = %v, want one entry", facts.Sources)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCatalogFactsLeavesOutWhatTheServerDoesNotResolve(t *testing.T) {
	execute, mock := newPostgresCatalogMock(t)
	mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(`"Album"`, `"Nowhere"`).
		WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), true, false))
	facts, err := newPostgresDialect(postgresExact).catalogFacts(context.Background(), execute, []typedSourceName{{Name: "Album"}, {Name: "Nowhere"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, known := facts.source(typedSourceName{Name: "Nowhere"}); known || len(facts.Sources) != 1 {
		t.Fatalf("Sources = %v, want only Album", facts.Sources)
	}
}

func TestPostgresCatalogFactsNeedsNoQueryWithoutSources(t *testing.T) {
	execute, mock := newPostgresCatalogMock(t) // expects nothing: any query fails the test
	facts, err := newPostgresDialect(postgresFoldLower).catalogFacts(context.Background(), execute, nil)
	if err != nil || len(facts.Sources) != 0 || facts.Fold == nil {
		t.Fatalf("catalogFacts(no sources) = %+v, %v", facts, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCatalogFactsRefusesANameTheDialectRefusesBeforeAnyQuery(t *testing.T) {
	cases := []struct {
		name     string
		source   typedSourceName
		fragment string
	}{
		{"empty table name", typedSourceName{}, "table name"},
		{"table name over the limit", typedSourceName{Name: strings.Repeat("n", 64)}, "table name"},
		{"empty schema name is no schema, a NUL schema name is refused", typedSourceName{Schema: "s\x00", Name: "t"}, "schema name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			execute, mock := newPostgresCatalogMock(t)
			facts, err := newPostgresDialect(postgresExact).catalogFacts(context.Background(), execute, []typedSourceName{c.source})
			if err == nil || !strings.Contains(err.Error(), c.fragment) {
				t.Fatalf("catalogFacts() = %+v, %v; want an error naming the %s", facts, err, c.fragment)
			}
			if len(facts.Sources) != 0 || facts.Fold != nil {
				t.Fatalf("a failed lookup must return no facts, got %+v", facts)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresCatalogFactsReportsWhatGoesWrongWithTheQuery(t *testing.T) {
	boom := errors.New("boom")
	badRow := func() *sqlmock.Rows {
		return sqlmock.NewRows(postgresCatalogColumns).AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), "not a bool", false)
	}
	cases := []struct {
		name     string
		answer   func(*sqlmock.ExpectedQuery)
		fragment string
		wrapped  error
	}{
		{"the query fails", func(q *sqlmock.ExpectedQuery) { q.WillReturnError(boom) }, "catalog facts", boom},
		{"a row does not scan", func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(badRow()) }, "catalog facts", nil},
		{"the rows fail midway", func(q *sqlmock.ExpectedQuery) {
			q.WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
				AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), true, false).
				RowError(0, boom))
		}, "catalog facts", boom},
		{"the catalog answers for a relation nobody asked for", func(q *sqlmock.ExpectedQuery) {
			q.WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).AddRow(`"Other"`, "id", "integer", "N", int64(23), true, false))
		}, "not asked for", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			execute, mock := newPostgresCatalogMock(t)
			c.answer(mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`))
			facts, err := newPostgresDialect(postgresExact).catalogFacts(context.Background(), execute, []typedSourceName{{Name: "Album"}})
			if err == nil || !strings.Contains(err.Error(), c.fragment) {
				t.Fatalf("catalogFacts() = %+v, %v; want an error containing %q", facts, err, c.fragment)
			}
			if c.wrapped != nil && !errors.Is(err, c.wrapped) {
				t.Fatalf("error %v does not wrap %v", err, c.wrapped)
			}
			if len(facts.Sources) != 0 {
				t.Fatalf("a failed lookup must return no facts, got %+v", facts)
			}
		})
	}
}

func TestPostgresTypeCategory(t *testing.T) {
	cases := []struct {
		name     string
		category string
		oid      int64
		want     typedTypeCategory
	}{
		{"text", "S", 25, typedTypeText},
		{"integer", "N", 23, typedTypeNumber},
		{"numeric", "N", 1700, typedTypeNumber},
		{"boolean", "B", 16, typedTypeBoolean},
		{"timestamptz", "D", 1184, typedTypeTime},
		{"date", "D", 1082, typedTypeTime},
		{"bytea is category U like uuid but binary", "U", postgresOIDBytea, typedTypeBinary},
		{"uuid", "U", 2950, typedTypeOther},
		{"enum", "E", 16999, typedTypeOther},
		{"array", "A", 1007, typedTypeOther},
		{"money shares category N with numbers it does not compare to", "N", postgresOIDMoney, typedTypeOther},
		{"time shares category D with timestamps it does not compare to", "D", postgresOIDTime, typedTypeOther},
		{"timetz", "D", postgresOIDTimeTZ, typedTypeOther},
		{"interval", "T", 1186, typedTypeOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := postgresTypeCategory(c.category, c.oid); got != c.want {
				t.Fatalf("postgresTypeCategory(%q, %d) = %v, want %v", c.category, c.oid, got, c.want)
			}
		})
	}
}

// TestPostgresCatalogFactsFeedTheCompiler runs the path SQL-04 wires, without a
// server: the sources of a query, one catalog answer, then the compiler. The facts
// drive wildcard expansion, the NULLS clause and the join-key type check.
func TestPostgresCatalogFactsFeedTheCompiler(t *testing.T) {
	answer := func(mock sqlmock.Sqlmock, first, second string) {
		rows := sqlmock.NewRows(postgresCatalogColumns).
			AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), true, false).
			AddRow(`"Album"`, "Title", "text", "S", int64(25), true, false).
			AddRow(`"Album"`, "ArtistId", "integer", "N", int64(23), true, false).
			AddRow(`"Album"`, "Secret", "text", "S", int64(25), false, false).
			AddRow(`"Artist"`, "ArtistId", "integer", "N", int64(23), true, false).
			AddRow(`"Artist"`, "Name", "text", "S", int64(25), false, false)
		mock.ExpectQuery(postgresCatalogQuery(2)).WithArgs(first, second).WillReturnRows(rows)
	}
	run := func(t *testing.T, q dal.StructuredQuery) (string, []any, error) {
		t.Helper()
		execute, mock := newPostgresCatalogMock(t)
		answer(mock, `"Album"`, `"Artist"`)
		dialect := newPostgresDialect(postgresExact)
		facts, err := dialect.catalogFacts(context.Background(), execute, typedQuerySources(q.From()))
		if err != nil {
			t.Fatal(err)
		}
		return compileTypedSQL(q, dialect, facts)
	}
	join := func(left, right string) dal.IQueryBuilder {
		return typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", left, "r", right)),
		).NewQuery()
	}
	t.Run("a select-all is expanded from the facts", func(t *testing.T) {
		q := typedTestFrom("Album", "").NewQuery().OrderBy(dal.Ascending(typedTestField("AlbumId"))).SelectColumns(dal.AllColumnsExcept("Secret"))
		execute, mock := newPostgresCatalogMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
			AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), true, false).
			AddRow(`"Album"`, "Title", "text", "S", int64(25), true, false).
			AddRow(`"Album"`, "Secret", "text", "S", int64(25), false, false))
		dialect := newPostgresDialect(postgresExact)
		facts, err := dialect.catalogFacts(context.Background(), execute, typedQuerySources(q.From()))
		if err != nil {
			t.Fatal(err)
		}
		text, _, err := compileTypedSQL(q, dialect, facts)
		if want := `SELECT "AlbumId", "Title" FROM "Album" ORDER BY "AlbumId" ASC`; err != nil || text != want {
			t.Fatalf("compileTypedSQL() = %q, %v; want %q (NOT NULL key: no NULLS clause)", text, err, want)
		}
	})
	t.Run("a join of two numbers compiles", func(t *testing.T) {
		text, _, err := run(t, join("ArtistId", "ArtistId").SelectColumns(typedTestColumn(typedTestQualified("r", "Name"), "")))
		if err != nil || !strings.Contains(text, `ON ("a"."ArtistId" = "r"."ArtistId")`) {
			t.Fatalf("compileTypedSQL() = %q, %v", text, err)
		}
	})
	t.Run("a join of a number and text is refused before the server sees it", func(t *testing.T) {
		_, _, err := run(t, join("ArtistId", "Name").SelectColumns())
		if !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "key types differ") {
			t.Fatalf("compileTypedSQL() error = %v, want a join_plan refusal", err)
		}
	})
	t.Run("a field that is not a column is refused", func(t *testing.T) {
		_, _, err := run(t, join("ArtistId", "ArtistId").SelectColumns(typedTestColumn(typedTestQualified("r", "to_json"), "")))
		if err == nil || !strings.Contains(err.Error(), `field "to_json" is not a column of source "r"`) {
			t.Fatalf("compileTypedSQL() error = %v, want the not-a-column error", err)
		}
	})
}
