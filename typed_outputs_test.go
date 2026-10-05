package dalgo2sql

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// compileTypedStatement returns, beside the text and the arguments, the name the
// query asked for each result column. A dialect that folds case writes Total as
// "total", and the server names the column total; the reader maps the result back to
// the spelling the caller used, so a caller's map is keyed as it asked.
func TestCompileTypedStatementNamesTheOutputsAsTheQueryAskedForThem(t *testing.T) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	facts := func(fold func(string) string) typedCatalogFacts {
		return typedCatalogFacts{Fold: fold, Sources: map[typedSourceName]typedSourceFacts{
			{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "email"}, {Name: "secret"}, {Name: "a"}, {Name: "b"}, {Name: "country"}}},
			{Name: "Invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "email"}, {Name: "secret"}, {Name: "a"}, {Name: "b"}, {Name: "country"}}},
		}}
	}
	cases := []struct {
		name  string
		query dal.StructuredQuery
		want  []string // nil: the statement selects *, and the columns are the catalog's own
	}{
		{"select all", invoice().SelectColumns(), nil},
		{"a field, an alias and an expression", invoice().SelectColumns(
			typedTestColumn(typedTestField("Total"), ""),
			typedTestColumn(typedTestField("Email"), "Mail"),
			typedTestColumn(dal.Binary(typedTestField("A"), dal.Add, typedTestField("B")), "")),
			[]string{"Total", "Mail", "(A + B)"}},
		{"count(*) keeps the text DALgo gives it", invoice().SelectColumns(dal.Count()), []string{dal.Count().Expression.String()}},
		{"a wildcard is expanded to the catalog's names, then the explicit columns follow", invoice().SelectColumns(
			dal.AllColumnsExcept("secret", "a", "b", "country"), typedTestColumn(typedTestField("Total"), "Again")),
			[]string{"total", "email", "Again"}},
		{"group keys stand for the columns of a query that names none", invoice().GroupBy(typedTestField("Country")).SelectColumns(), []string{"Country"}},
	}
	for _, folding := range foldingTypedDialects(t) {
		for _, tc := range cases {
			t.Run(folding.name+"/"+tc.name, func(t *testing.T) {
				statement, err := compileTypedStatement(tc.query, folding.dialect, facts(folding.fold))
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(statement.outputs, tc.want) {
					t.Fatalf("outputs = %#v, want %#v\nsql: %s", statement.outputs, tc.want, statement.text)
				}
			})
		}
	}
	t.Run("compileTypedSQL returns the same text and arguments", func(t *testing.T) {
		q := invoice().Where(typedTestEq(typedTestField("a"), 1)).SelectColumns(typedTestColumn(typedTestField("a"), "x"))
		statement, err := compileTypedStatement(q, newFakeTypedDialect(), typedCatalogFacts{})
		text, args, err2 := compileTypedSQL(q, newFakeTypedDialect(), typedCatalogFacts{})
		if err != nil || err2 != nil || text != statement.text || !reflect.DeepEqual(args, statement.args) {
			t.Fatalf("compileTypedSQL() = %q, %v, %v; compileTypedStatement() = %+v, %v", text, args, err2, statement, err)
		}
	})
	t.Run("a refusal returns no statement", func(t *testing.T) {
		statement, err := compileTypedStatement(invoice().Limit(-1).SelectColumns(), newFakeTypedDialect(), typedCatalogFacts{})
		if err == nil || statement.text != "" || statement.args != nil || statement.outputs != nil {
			t.Fatalf("compileTypedStatement() = %+v, %v; want an error and nothing else", statement, err)
		}
	})
}

// Two outputs the query names differently can be one name once the dialect writes
// them: the server then returns two columns of one name, and the reader, which
// keys a record by name, would keep one of them. The statement is refused. The same
// name asked twice is not a new problem and compiles as before.
func TestCompileTypedSQLRefusesOutputsThatFoldToTheSameName(t *testing.T) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	field := typedTestField
	col := typedTestColumn
	for _, folding := range foldingTypedDialects(t) {
		t.Run(folding.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				query dal.StructuredQuery
			}{
				{"two spellings of a field", invoice().SelectColumns(col(field("Total"), ""), col(field("TOTAL"), ""))},
				{"two aliases", invoice().SelectColumns(col(field("a"), "Mail"), col(field("b"), "MAIL"))},
				{"an alias and a field", invoice().SelectColumns(col(field("Total"), ""), col(field("b"), "total"))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expectTypedUnsupported(t, tc.query, folding.dialect, typedCatalogFacts{}, "once the dialect writes them")
				})
			}
			t.Run("a wildcard column and an explicit one in another case", func(t *testing.T) {
				facts := typedCatalogFacts{Fold: folding.fold, Sources: map[typedSourceName]typedSourceFacts{
					{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "email"}}},
				}}
				q := invoice().SelectColumns(dal.AllColumnsExcept("total"), col(field("Email"), ""))
				expectTypedUnsupported(t, q, folding.dialect, facts, "once the dialect writes them")
			})
			t.Run("the same name asked twice is left as it was", func(t *testing.T) {
				if _, err := compileTypedStatement(invoice().SelectColumns(col(field("Total"), ""), col(field("Total"), "")), folding.dialect, typedCatalogFacts{}); err != nil {
					t.Fatalf("compileTypedStatement() error = %v", err)
				}
			})
		})
	}
	t.Run("a dialect that writes names as asked keeps two spellings apart", func(t *testing.T) {
		q := invoice().SelectColumns(col(field("Total"), ""), col(field("TOTAL"), ""))
		typedGolden{query: q, wantSQL: `SELECT "Total", "TOTAL" FROM "Invoice"`}.run(t)
	})
}

// A name that only labels a result column (an alias, or the text of an unaliased
// expression) never reaches the server as anything but a label, so a name the engine
// cannot spell is not a malformed query: DALgo's generic engine labels the column
// itself. It is refused as unsupported, so the query falls back instead of failing.
func TestCompileTypedSQLDeclinesAnOutputNameOverTheEngineLimit(t *testing.T) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	long := strings.Repeat("a", 64)
	wide := strings.Repeat("Ⱥ", 22) // 44 bytes written, 66 once folded to lower case
	for _, d := range append(foldingTypedDialects(t), foldingTypedDialect{name: "PostgreSQL Exact", dialect: newPostgresDialect(postgresExact)}) {
		t.Run(d.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				query dal.StructuredQuery
			}{
				{"an alias", invoice().SelectColumns(typedTestColumn(typedTestField("a"), long))},
				{"an alias on an aggregate", invoice().SelectColumns(dal.SumAs(typedTestField("a"), long))},
				{"an unaliased expression", invoice().SelectColumns(typedTestColumn(dal.Binary(typedTestField(strings.Repeat("a", 40)), dal.Add, typedTestField(strings.Repeat("b", 40))), ""))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expectTypedUnsupported(t, tc.query, d.dialect, typedCatalogFacts{}, "output name")
					_, _, err := compileTypedSQL(tc.query, d.dialect, typedCatalogFacts{})
					if err != nil && strings.Contains(err.Error(), long) {
						t.Fatalf("error %q echoes the name", err)
					}
				})
			}
		})
	}
	t.Run("the folded length counts, as it does for any name", func(t *testing.T) {
		q := invoice().SelectColumns(typedTestColumn(typedTestField("a"), wide))
		expectTypedUnsupported(t, q, newPostgresDialect(postgresFoldLower), typedCatalogFacts{}, "output name")
		if _, _, err := compileTypedSQL(q, newPostgresDialect(postgresExact), typedCatalogFacts{}); err != nil {
			t.Fatalf("44 bytes written fit in Exact mode: %v", err)
		}
	})
	t.Run("a name that is empty, holds a NUL or is not UTF-8 is still a plain error", func(t *testing.T) {
		for _, alias := range []string{"a\x00b", "a\xffb"} {
			expectTypedInvalid(t, invoice().SelectColumns(typedTestColumn(typedTestField("a"), alias)), nil, typedCatalogFacts{}, "")
		}
	})
	t.Run("a field name over the limit is a plain error, the column cannot exist", func(t *testing.T) {
		expectTypedInvalid(t, invoice().SelectColumns(typedTestColumn(typedTestField(long), "")), nil, typedCatalogFacts{}, "limit")
	})
	t.Run("the limit error keeps its text and matches its type", func(t *testing.T) {
		err := checkTypedIdentifier(long, 63)
		var tooLong *typedIdentifierTooLongError
		if !errors.As(err, &tooLong) || err.Error() != "identifier is 64 bytes, over the engine limit of 63" {
			t.Fatalf("checkTypedIdentifier() = %v (%T)", err, err)
		}
	})
}
