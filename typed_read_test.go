package dalgo2sql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestPostgresDialectForIdentifierCase(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   IdentifierCase
		want postgresIdentifierMode
	}{
		{"the zero value is exact", "", postgresExact},
		{"exact", IdentifierCaseExact, postgresExact},
		{"fold lower", IdentifierCaseFoldLower, postgresFoldLower},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialect, err := postgresDialectFor(DbOptions{IdentifierCase: tc.in})
			if err != nil || dialect.mode != tc.want {
				t.Fatalf("postgresDialectFor() = %+v, %v; want mode %v", dialect, err, tc.want)
			}
		})
	}
	t.Run("a value this package does not define is refused, not read as the default", func(t *testing.T) {
		_, err := postgresDialectFor(DbOptions{IdentifierCase: "upper"})
		if err == nil || !strings.Contains(err.Error(), `"upper"`) || !strings.Contains(err.Error(), "fold-lower") {
			t.Fatalf("postgresDialectFor() error = %v, want the value and the two valid ones named", err)
		}
	})
}

func TestTypedNearestName(t *testing.T) {
	names := []string{"Album", "Artist", "Customer", "InvoiceLine", "x"}
	for _, tc := range []struct {
		name  string
		asked string
		want  string // "" means no suggestion
	}{
		{"a name that differs only in case", "album", "Album"},
		{"the case-only match wins over a nearer-looking one", "ARTIST", "Artist"},
		{"one letter missing", "Albm", "Album"},
		{"one letter too many", "Albums", "Album"},
		{"a swapped pair", "Arsitt", "Artist"},
		{"two edits in a long name", "InvoiceLin", "InvoiceLine"},
		{"nothing near", "Employee", ""},
		{"a short name is not matched to everything", "zz", ""},
		{"the name itself is never suggested", "Album", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := typedNearestName(tc.asked, names)
			switch {
			case tc.want == "" && got != -1:
				t.Fatalf("typedNearestName(%q) = %q, want none", tc.asked, names[got])
			case tc.want != "" && (got < 0 || names[got] != tc.want):
				t.Fatalf("typedNearestName(%q) = %d, want %q", tc.asked, got, tc.want)
			}
		})
	}
	t.Run("no candidates", func(t *testing.T) {
		if got := typedNearestName("Album", nil); got != -1 {
			t.Fatalf("typedNearestName() = %d, want -1", got)
		}
	})
	t.Run("a tie goes to the first candidate, which is the catalog's own order", func(t *testing.T) {
		if got := typedNearestName("Alb", []string{"Alba", "Albe"}); got != 0 {
			t.Fatalf("typedNearestName() = %d, want 0", got)
		}
	})
	t.Run("distance counts characters, not bytes", func(t *testing.T) {
		if got := typedEditDistance("école", "ecole"); got != 1 {
			t.Fatalf("typedEditDistance() = %d, want 1", got)
		}
	})
}

func TestTableNotFoundErrorMessage(t *testing.T) {
	long := strings.Repeat("n", 200)
	for _, tc := range []struct {
		name string
		err  *TableNotFoundError
		want string
	}{
		{"a bare name", &TableNotFoundError{Name: "Albm"}, `table "Albm" not found`},
		{"with a schema", &TableNotFoundError{Schema: "sales", Name: "Albm"}, `table "sales"."Albm" not found`},
		{"with a hint", &TableNotFoundError{Name: "album", SuggestedName: "Album"}, `table "album" not found; did you mean "Album"?`},
		{"with a hint in the schema the query wrote", &TableNotFoundError{Schema: "Sales", Name: "album", SuggestedSchema: "sales", SuggestedName: "Album"},
			`table "Sales"."album" not found; did you mean "sales"."Album"?`},
		{"case-sensitive names say so", &TableNotFoundError{Name: "album", SuggestedName: "Album", CaseSensitive: true},
			`table "album" not found; did you mean "Album"? Table names are case-sensitive.`},
		{"a very long name is cut", &TableNotFoundError{Name: long}, `table "` + strings.Repeat("n", 80) + `"... not found`},
		{"a name is quoted, so a hostile one cannot forge a line", &TableNotFoundError{Name: "a\nb\"c"}, `table "a\nb\"c" not found`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Fatalf("Error() = %s, want %s", got, tc.want)
			}
		})
	}
	t.Run("a name cut inside a character keeps whole characters", func(t *testing.T) {
		// 201 bytes; byte 80 is the second byte of a character, so the cut moves back one.
		got := (&TableNotFoundError{Name: "x" + strings.Repeat("é", 100)}).Error()
		if want := `table "x` + strings.Repeat("é", 39) + `"... not found`; got != want {
			t.Fatalf("Error() = %s, want %s", got, want)
		}
	})
	t.Run("errors.Is matches the sentinel and nothing else", func(t *testing.T) {
		err := error(&TableNotFoundError{Name: "x"})
		if !errors.Is(err, ErrTableNotFound) || errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("errors.Is(%v) wrong", err)
		}
		var found *TableNotFoundError
		if wrapped := errors.Join(errors.New("outer"), err); !errors.As(wrapped, &found) || found.Name != "x" {
			t.Fatal("errors.As does not find the typed error")
		}
	})
}

func newPostgresSuggestionRows(rows ...[2]string) *sqlmock.Rows {
	result := sqlmock.NewRows([]string{"nspname", "relname"})
	for _, row := range rows {
		result.AddRow(row[0], row[1])
	}
	return result
}

func TestPostgresSuggestSource(t *testing.T) {
	ctx := context.Background()
	t.Run("the query depends on nothing the caller wrote and lists only what the session can read", func(t *testing.T) {
		for _, fragment := range []string{"pg_catalog.pg_class", "pg_catalog.pg_namespace", "pg_catalog.has_table_privilege(c.oid, 'SELECT')", "pg_catalog.pg_table_is_visible", "pg_catalog.lower", "LIMIT 5000", "$1::text"} {
			if !strings.Contains(postgresSuggestionQuery, fragment) {
				t.Errorf("suggestion query does not contain %q:\n%s", fragment, postgresSuggestionQuery)
			}
		}
		if strings.Count(postgresSuggestionQuery, "$") != 2 { // the one bound schema, written twice
			t.Errorf("suggestion query writes %d markers:\n%s", strings.Count(postgresSuggestionQuery, "$"), postgresSuggestionQuery)
		}
	})
	for _, tc := range []struct {
		name      string
		mode      postgresIdentifierMode
		source    typedSourceName
		wantArg   string
		rows      *sqlmock.Rows
		want      typedSourceName
		wantFound bool
	}{
		{"an unqualified name finds a case-only match", postgresExact, typedSourceName{Name: "album"}, "",
			newPostgresSuggestionRows([2]string{"public", "Album"}, [2]string{"public", "Artist"}), typedSourceName{Name: "Album"}, true},
		{"a qualified name keeps the catalog's schema", postgresExact, typedSourceName{Schema: "Sales", Name: "album"}, "Sales",
			newPostgresSuggestionRows([2]string{"sales", "Album"}, [2]string{"sales", "Artist"}), typedSourceName{Schema: "sales", Name: "Album"}, true},
		{"the schema is folded in FoldLower mode", postgresFoldLower, typedSourceName{Schema: "SALES", Name: "Albm"}, "sales",
			newPostgresSuggestionRows([2]string{"sales", "album"}), typedSourceName{Schema: "sales", Name: "album"}, true},
		{"nothing is near", postgresExact, typedSourceName{Name: "Employee"}, "",
			newPostgresSuggestionRows([2]string{"public", "Album"}), typedSourceName{}, false},
		{"no candidates at all", postgresExact, typedSourceName{Name: "Album"}, "", newPostgresSuggestionRows(), typedSourceName{}, false},
		// The name is right and the schema is spelt in another case: the schema is the
		// whole mistake, and the candidate's name is the one that was asked for.
		{"a schema spelt in another case is the hint, though the name is the one asked", postgresExact, typedSourceName{Schema: "Sales", Name: "Album"}, "Sales",
			newPostgresSuggestionRows([2]string{"sales", "Album"}), typedSourceName{Schema: "sales", Name: "Album"}, true},
		{"the very relation that was asked for is never the hint", postgresExact, typedSourceName{Schema: "sales", Name: "Album"}, "sales",
			newPostgresSuggestionRows([2]string{"sales", "Album"}), typedSourceName{}, false},
		{"a schema in another case wins over a name that is near", postgresExact, typedSourceName{Schema: "Sales", Name: "Album"}, "Sales",
			newPostgresSuggestionRows([2]string{"sales", "Albums"}, [2]string{"sales", "Album"}), typedSourceName{Schema: "sales", Name: "Album"}, true},
		// A mount that folds cannot write a stored name that is not in lower case, so
		// naming it would send the caller to a name every spelling folds away from.
		{"fold-lower: a stored name the mount cannot write is no hint", postgresFoldLower, typedSourceName{Name: "Album"}, "",
			newPostgresSuggestionRows([2]string{"public", "Album"}), typedSourceName{}, false},
		{"fold-lower: a name it can write is still the hint, beside one it cannot", postgresFoldLower, typedSourceName{Name: "Album"}, "",
			newPostgresSuggestionRows([2]string{"public", "Album"}, [2]string{"public", "albums"}), typedSourceName{Name: "albums"}, true},
		{"fold-lower: a stored schema the mount cannot write is no hint", postgresFoldLower, typedSourceName{Schema: "SALES", Name: "Albm"}, "sales",
			newPostgresSuggestionRows([2]string{"Sales", "album"}), typedSourceName{}, false},
		{"exact: a stored mixed-case name is a fine hint", postgresExact, typedSourceName{Name: "album"}, "",
			newPostgresSuggestionRows([2]string{"public", "Album"}), typedSourceName{Name: "Album"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execute, mock := newPostgresCatalogMock(t)
			mock.ExpectQuery(postgresSuggestionQuery).WithArgs(tc.wantArg).WillReturnRows(tc.rows)
			got, found, err := newPostgresDialect(tc.mode).suggestSource(ctx, execute, tc.source)
			if err != nil || found != tc.wantFound || got != tc.want {
				t.Fatalf("suggestSource() = %+v, %v, %v; want %+v, %v", got, found, err, tc.want, tc.wantFound)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	boom := errors.New("boom")
	for _, tc := range []struct {
		name   string
		answer func(*sqlmock.ExpectedQuery)
		wrap   error
	}{
		{"the query fails", func(q *sqlmock.ExpectedQuery) { q.WillReturnError(boom) }, boom},
		{"a row does not scan", func(q *sqlmock.ExpectedQuery) {
			q.WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("only one column"))
		}, nil},
		{"the rows fail midway", func(q *sqlmock.ExpectedQuery) {
			q.WillReturnRows(newPostgresSuggestionRows([2]string{"public", "Album"}).RowError(0, boom))
		}, boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execute, mock := newPostgresCatalogMock(t)
			tc.answer(mock.ExpectQuery(postgresSuggestionQuery).WithArgs(""))
			_, found, err := newPostgresDialect(postgresExact).suggestSource(ctx, execute, typedSourceName{Name: "Album"})
			if err == nil || found || !strings.Contains(err.Error(), "suggest a table") || (tc.wrap != nil && !errors.Is(err, tc.wrap)) {
				t.Fatalf("suggestSource() = %v, %v; want an error", found, err)
			}
		})
	}
}

// fakeSuggestingDialect is the fake dialect with a catalog and a hint of its own.
type fakeSuggestingDialect struct {
	*fakeTypedDialect
	facts      typedCatalogFacts
	factsErr   error
	suggestion typedSourceName
	suggested  bool
	suggestErr error
	asked      []typedSourceName
}

func (d *fakeSuggestingDialect) catalogFacts(_ context.Context, _ executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error) {
	d.asked = sources
	return d.facts, d.factsErr
}

func (d *fakeSuggestingDialect) suggestSource(context.Context, executeQueryFunc, typedSourceName) (typedSourceName, bool, error) {
	return d.suggestion, d.suggested, d.suggestErr
}

func TestTypedFactsForQuery(t *testing.T) {
	ctx := context.Background()
	known := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Album"}:  {Columns: []typedColumnFact{{Name: "AlbumId"}}},
		{Name: "Artist"}: {Columns: []typedColumnFact{{Name: "ArtistId"}}},
	}}
	album := typedTestFrom("Album", "")
	joined := typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")))
	t.Run("every source known: the facts come back, asked for the sources as the query spells them", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known}
		facts, err := typedFactsForQuery(ctx, dialect, nil, joined)
		if err != nil || len(facts.Sources) != 2 {
			t.Fatalf("typedFactsForQuery() = %+v, %v", facts, err)
		}
		if len(dialect.asked) != 2 || dialect.asked[0] != (typedSourceName{Name: "Album"}) || dialect.asked[1] != (typedSourceName{Name: "Artist"}) {
			t.Fatalf("catalogFacts was asked for %+v", dialect.asked)
		}
	})
	t.Run("a source the facts do not know is a table-not-found error with the hint", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known, suggestion: typedSourceName{Name: "Album"}, suggested: true}
		_, err := typedFactsForQuery(ctx, dialect, nil, typedTestFrom("album", ""))
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("error = %v, want a *TableNotFoundError", err)
		}
		if notFound.Name != "album" || notFound.Schema != "" || notFound.SuggestedName != "Album" || !notFound.CaseSensitive {
			t.Fatalf("TableNotFoundError = %+v", notFound)
		}
		if want := `table "album" not found; did you mean "Album"? Table names are case-sensitive.`; err.Error() != want {
			t.Fatalf("message = %s, want %s", err, want)
		}
	})
	t.Run("the hint carries its schema and its name as two plain fields", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known, suggestion: typedSourceName{Schema: "sales", Name: "Album"}, suggested: true}
		_, err := typedFactsForQuery(ctx, dialect, nil, typedTestFrom("album", ""))
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.SuggestedSchema != "sales" || notFound.SuggestedName != "Album" {
			t.Fatalf("error = %#v, want the suggestion sales.Album in SuggestedSchema and SuggestedName", err)
		}
	})
	t.Run("one known source does not excuse an unknown one", func(t *testing.T) {
		only := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{{Name: "Album"}: {Columns: []typedColumnFact{{Name: "AlbumId"}}}}}
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: only}
		_, err := typedFactsForQuery(ctx, dialect, nil, joined)
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.Name != "Artist" {
			t.Fatalf("error = %v, want Artist reported", err)
		}
	})
	t.Run("a source named with a schema is looked up with it", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known}
		_, err := typedFactsForQuery(ctx, dialect, nil, dal.From(dal.NewQualifiedRootCollectionRef("sales", "Album", "")))
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.Schema != "sales" || notFound.Name != "Album" {
			t.Fatalf("error = %v, want sales.Album reported", err)
		}
	})
	t.Run("a dialect that folds case says nothing about case-sensitivity", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: typedCatalogFacts{Fold: strings.ToLower}}
		_, err := typedFactsForQuery(ctx, dialect, nil, album)
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.CaseSensitive {
			t.Fatalf("error = %v, want a not-found error that is not case-sensitive", err)
		}
	})
	t.Run("a hint that cannot be had leaves the error without one", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known, suggestErr: errors.New("boom"), suggested: true, suggestion: typedSourceName{Name: "Album"}}
		_, err := typedFactsForQuery(ctx, dialect, nil, typedTestFrom("album", ""))
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.SuggestedName != "" {
			t.Fatalf("error = %v, want not-found with no hint", err)
		}
	})
	t.Run("a hint the dialect does not give leaves the error without one", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: known}
		_, err := typedFactsForQuery(ctx, dialect, nil, typedTestFrom("album", ""))
		var notFound *TableNotFoundError
		if !errors.As(err, &notFound) || notFound.SuggestedName != "" {
			t.Fatalf("error = %v, want not-found with no hint", err)
		}
	})
	t.Run("the catalog lookup failing is its own error", func(t *testing.T) {
		boom := errors.New("boom")
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), factsErr: boom}
		if _, err := typedFactsForQuery(ctx, dialect, nil, album); !errors.Is(err, boom) || errors.Is(err, ErrTableNotFound) {
			t.Fatalf("error = %v, want the lookup's own", err)
		}
	})
	t.Run("a query with no source the compiler can render needs no facts and no hint", func(t *testing.T) {
		dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect()}
		facts, err := typedFactsForQuery(ctx, dialect, nil, dal.From(dal.NewQuerySource(typedTestFrom("Album", "").NewQuery().SelectColumns(), "s")))
		if err != nil || len(facts.Sources) != 0 || len(dialect.asked) != 0 {
			t.Fatalf("typedFactsForQuery() = %+v, %v; asked %+v", facts, err, dialect.asked)
		}
	})
}

func TestCompileTypedRead(t *testing.T) {
	ctx := context.Background()
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Album"}: {Columns: []typedColumnFact{{Name: "AlbumId", NotNull: true}, {Name: "Title"}}},
	}}
	dialect := &fakeSuggestingDialect{fakeTypedDialect: newFakeTypedDialect(), facts: facts}
	t.Run("the facts reach the compiler", func(t *testing.T) {
		q := typedTestFrom("Album", "").NewQuery().OrderBy(dal.Ascending(typedTestField("AlbumId"))).SelectColumns(dal.AllColumnsExcept("Title"))
		statement, err := compileTypedRead(ctx, q, dialect, nil)
		if err != nil || statement.text != `SELECT "AlbumId" FROM "Album" ORDER BY "AlbumId" ASC` || len(statement.outputs) != 1 || statement.outputs[0] != "AlbumId" {
			t.Fatalf("compileTypedRead() = %+v, %v", statement, err)
		}
	})
	t.Run("a source that is not there is refused before any compile", func(t *testing.T) {
		_, err := compileTypedRead(ctx, typedTestFrom("Nowhere", "").NewQuery().SelectColumns(), dialect, nil)
		if !errors.Is(err, ErrTableNotFound) {
			t.Fatalf("compileTypedRead() error = %v, want table not found", err)
		}
	})
	t.Run("a qualified x.to_json on a source the facts do not know does not compile", func(t *testing.T) {
		for _, q := range []dal.StructuredQuery{
			typedTestFrom("Nowhere", "x").NewQuery().SelectColumns(typedTestColumn(typedTestQualified("x", "to_json"), "")),
			typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Nowhere", "x"), dal.JoinInner, typedTestJoinOn("a", "AlbumId", "x", "AlbumId"))).
				NewQuery().SelectColumns(typedTestColumn(typedTestQualified("x", "to_json"), "")),
		} {
			statement, err := compileTypedRead(ctx, q, dialect, nil)
			if !errors.Is(err, ErrTableNotFound) || statement.text != "" {
				t.Fatalf("compileTypedRead() = %+v, %v; want table not found and no statement", statement, err)
			}
		}
	})
}
