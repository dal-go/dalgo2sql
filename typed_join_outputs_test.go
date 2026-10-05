package dalgo2sql

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// A result is read by name: the reader keeps one value per name. Two outputs of one name
// in a join (a.id and r.id) are two columns of the statement, and the reader would keep
// one of them and drop the other without a word. DALgo's generic engine refuses the
// query with join_field: duplicate output name, and so does the compiler, with the same
// kind of error. A name repeated for the same expression is one column asked twice and
// still compiles, as it does for a lone source.
func TestCompileTypedSQLRefusesARepeatedOutputNameInAJoin(t *testing.T) {
	join := func(columns ...dal.Column) dal.StructuredQuery {
		return typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().SelectColumns(columns...)
	}
	qualified := typedTestQualified
	col := typedTestColumn
	dialects := append(foldingTypedDialects(t), foldingTypedDialect{name: "PostgreSQL Exact", dialect: newPostgresDialect(postgresExact)})
	for _, d := range dialects {
		t.Run(d.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				query dal.StructuredQuery
				path  string
			}{
				{"two sources' columns of one name", join(col(qualified("a", "ArtistId"), ""), col(qualified("r", "ArtistId"), "")), "columns[1]"},
				{"the same alias on two columns", join(col(qualified("a", "Title"), "x"), col(qualified("r", "Name"), "x")), "columns[1]"},
				{"an alias that is another column's own name", join(col(qualified("a", "Title"), ""), col(qualified("r", "Name"), "Title")), "columns[1]"},
				{"a column and an expression of one alias", join(col(qualified("a", "n"), "n"), col(dal.Binary(qualified("a", "n"), dal.Add, qualified("r", "n")), "n")), "columns[1]"},
				{"a repeat after a distinct column", join(col(qualified("a", "Title"), ""), col(qualified("a", "AlbumId"), ""), col(qualified("r", "Title"), "")), "columns[2]"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := compileTypedStatement(tc.query, d.dialect, typedCatalogFacts{})
					var joinErr *dal.JoinValidationError
					if !errors.As(err, &joinErr) {
						t.Fatalf("compileTypedStatement() error = %v, want the generic engine's kind of error (*dal.JoinValidationError)", err)
					}
					if joinErr.Category != "join_field" || joinErr.Path != tc.path || !strings.HasPrefix(joinErr.Message, "duplicate output name ") {
						t.Fatalf("error = %#v, want join_field at %s: duplicate output name", joinErr, tc.path)
					}
					if errors.Is(err, dal.ErrNotSupported) {
						t.Fatalf("error %q must not claim ErrNotSupported: the generic engine refuses the same query, so there is nothing to fall back to", err)
					}
				})
			}
			for _, tc := range []struct {
				name  string
				query dal.StructuredQuery
			}{
				{"one expression asked twice in a join", join(col(qualified("a", "ArtistId"), ""), col(qualified("a", "ArtistId"), ""))},
				{"the same alias for the same expression in a join", join(col(qualified("a", "Title"), "t"), col(qualified("a", "Title"), "t"))},
				{"distinct names in a join", join(col(qualified("a", "ArtistId"), ""), col(qualified("r", "ArtistId"), "artist"))},
				{"a lone source naming two fields alike", typedTestFrom("Album", "a").NewQuery().SelectColumns(col(qualified("a", "Title"), ""), col(typedTestField("Title"), ""))},
				{"a lone source naming one field twice", typedTestFrom("Album", "").NewQuery().SelectColumns(col(typedTestField("Title"), ""), col(typedTestField("Title"), ""))},
			} {
				t.Run(tc.name+" compiles", func(t *testing.T) {
					if _, err := compileTypedStatement(tc.query, d.dialect, typedCatalogFacts{}); err != nil {
						t.Fatalf("compileTypedStatement() error = %v", err)
					}
				})
			}
		})
	}
}

// A select-all over joins returns the base's columns first, then each joined source's.
// The statement says which are the base's, from the catalog facts, so a reader can tell
// the base's column of a name from another source's (see recordsReader.keyInBaseColumns).
func TestCompileTypedStatementListsTheBasesColumnsForASelectAllOverJoins(t *testing.T) {
	join := func(columns ...dal.Column) dal.StructuredQuery {
		return typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().SelectColumns(columns...)
	}
	number := func(name string) typedColumnFact { return typedColumnFact{Name: name, Category: typedTypeNumber} }
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Album"}:  {Columns: []typedColumnFact{number("Title"), number("AlbumId"), number("ArtistId")}},
		{Name: "Artist"}: {Columns: []typedColumnFact{number("ArtistId"), number("Name")}},
	}}
	dialect := newPostgresDialect(postgresExact)
	for _, tc := range []struct {
		name  string
		query dal.StructuredQuery
		facts typedCatalogFacts
		want  []string
	}{
		{"a select-all over a join: the base's columns, in the catalog's order", join(), facts, []string{"Title", "AlbumId", "ArtistId"}},
		{"a select-all over a join the facts do not list", join(), typedCatalogFacts{}, nil},
		{"a select list", join(typedTestColumn(typedTestQualified("a", "Title"), "")), facts, nil},
		{"a lone source", typedTestFrom("Album", "").NewQuery().SelectColumns(), facts, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := compileTypedStatement(tc.query, dialect, tc.facts)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(statement.baseColumns, tc.want) {
				t.Fatalf("baseColumns = %v, want %v", statement.baseColumns, tc.want)
			}
		})
	}
}
