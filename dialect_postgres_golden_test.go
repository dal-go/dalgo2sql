package dalgo2sql

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dtql"
)

// The PostgreSQL golden files live in testdata/postgres. Each holds, for one group
// of queries compiled in one identifier mode, the exact statement and the exact
// arguments, or the refusal. Review a changed golden as you would review code: it
// is the text sent to the server. Rewrite them with
//
//	go test -run TestPostgresGolden -update-postgres-golden
var updatePostgresGolden = flag.Bool("update-postgres-golden", false, "rewrite the golden files under testdata/postgres instead of comparing them")

// assertPostgresGolden compares got with testdata/postgres/<name>.
func assertPostgresGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "postgres", name)
	if *updatePostgresGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (create it with -update-postgres-golden)", path, err)
	}
	if string(want) != got {
		t.Fatalf("golden %s differs (rewrite with -update-postgres-golden and review the diff)\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

// postgresGoldenCase is one query compiled in both identifier modes.
type postgresGoldenCase struct {
	name  string
	query dal.StructuredQuery
	// byMode, when set, builds the query for a mode instead of query. A wildcard
	// exclusion is matched against the catalog's own names, so under FoldLower,
	// where the catalog holds lower-case names, the case spells them that way.
	byMode func(postgresIdentifierMode) dal.StructuredQuery
	// facts builds the catalog facts for a mode; nil compiles without facts.
	facts func(postgresIdentifierMode) typedCatalogFacts
}

// formatPostgresArg writes one bound argument with its Go type, so a golden shows
// what the driver receives.
func formatPostgresArg(arg any) string {
	switch v := arg.(type) {
	case nil:
		return "nil"
	case string:
		return "string " + strconv.Quote(v)
	case int64:
		return "int64 " + strconv.FormatInt(v, 10)
	case bool:
		return "bool " + strconv.FormatBool(v)
	case time.Time:
		return "time.Time " + v.Format(time.RFC3339Nano)
	case []byte:
		if len(v) == 0 {
			return "[]byte (empty)" // no trailing space in a golden line
		}
		return fmt.Sprintf("[]byte %x", v)
	}
	return fmt.Sprintf("%T %#v", arg, arg)
}

// renderPostgresGolden compiles every case in the mode and writes the statement,
// the arguments and the refusals in the golden format.
func renderPostgresGolden(cases []postgresGoldenCase, mode postgresIdentifierMode) string {
	var out strings.Builder
	dialect := newPostgresDialect(mode)
	for _, c := range cases {
		fmt.Fprintf(&out, "=== %s\n", c.name)
		var facts typedCatalogFacts
		if c.facts != nil {
			facts = c.facts(mode)
		}
		query := c.query
		if c.byMode != nil {
			query = c.byMode(mode)
		}
		text, args, err := compileTypedSQL(query, dialect, facts)
		switch {
		case errors.Is(err, dal.ErrNotSupported):
			fmt.Fprintf(&out, "refused: %v\n", err)
		case err != nil:
			fmt.Fprintf(&out, "error: %v\n", err)
		default:
			fmt.Fprintf(&out, "sql: %s\n", text)
			for i, arg := range args {
				fmt.Fprintf(&out, "$%d %s\n", i+1, formatPostgresArg(arg))
			}
		}
	}
	return out.String()
}

// assertPostgresGoldenGroup compares the group's golden of each identifier mode.
func assertPostgresGoldenGroup(t *testing.T, group string, cases []postgresGoldenCase) {
	t.Helper()
	for _, mode := range []struct {
		file string
		mode postgresIdentifierMode
	}{{"exact", postgresExact}, {"foldlower", postgresFoldLower}} {
		t.Run(mode.file, func(t *testing.T) {
			assertPostgresGolden(t, group+"-"+mode.file+".golden", renderPostgresGolden(cases, mode.mode))
		})
	}
}

// postgresChinookFacts is what the catalog says about a Chinook-like database
// created with quoted mixed-case names, the way DataTug's sample databases are:
// in postgresExact mode the names are as written here, and in postgresFoldLower
// mode, where dalgo2postgres's DDL stores every name in lower case, they are the
// lower-cased names. Each table is known unqualified (found through search_path)
// and under the schema "main" the join fixtures name.
func postgresChinookFacts(mode postgresIdentifierMode) typedCatalogFacts {
	dialect := newPostgresDialect(mode)
	facts, err := dialect.catalogFacts(context.Background(), nil, nil)
	if err != nil {
		panic(err)
	}
	col := func(name, dataType string, category typedTypeCategory, notNull bool) typedColumnFact {
		return typedColumnFact{Name: dialect.fold(name), DataType: dataType, Category: category, NotNull: notNull}
	}
	integer := func(name string, notNull bool) typedColumnFact { return col(name, "integer", typedTypeNumber, notNull) }
	text := func(name string, notNull bool) typedColumnFact { return col(name, "text", typedTypeText, notNull) }
	tables := map[string][]typedColumnFact{
		"Album":  {integer("AlbumId", true), text("Title", true), integer("ArtistId", true)},
		"Artist": {integer("ArtistId", true), text("Name", false)},
		"Invoice": {
			integer("InvoiceId", true), integer("CustomerId", true),
			col("InvoiceDate", "timestamp without time zone", typedTypeTime, true),
			text("BillingCity", false), text("BillingCountry", false),
			col("Total", "numeric", typedTypeNumber, true),
		},
		"Customer": {
			integer("CustomerId", true), text("FirstName", true), text("LastName", true),
			text("Email", false), text("Country", false), integer("SupportRepId", false),
		},
		"Employee": {integer("EmployeeId", true), text("FirstName", true)},
	}
	facts.Sources = map[typedSourceName]typedSourceFacts{}
	for name, columns := range tables {
		for _, schema := range []string{"", "main"} {
			facts.Sources[typedSourceName{Schema: dialect.fold(schema), Name: dialect.fold(name)}] = typedSourceFacts{Columns: columns}
		}
	}
	return facts
}

func postgresGoldenField(name string) dal.FieldRef { return typedTestField(name) }

// TestPostgresGoldenShapes pins the statement and arguments of every supported
// query shape.
func TestPostgresGoldenShapes(t *testing.T) {
	album := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	field := postgresGoldenField
	col := typedTestColumn
	ints := func(values ...int) dal.Array { return dal.NewArray(values) }
	bucket := dal.Binary(field("Total"), dal.Add, typedTestConst(100))
	bucketColumns := []dal.Column{col(bucket, "bucket"), dal.CountAs(field("InvoiceId"), "n")}
	distinct := func(function, name, alias string) dal.Column {
		return col(dal.NewAggregate(function, true, field(name)), alias)
	}
	withFacts := postgresChinookFacts
	// catalogSpelling writes names as the catalog holds them in the mode.
	catalogSpelling := func(mode postgresIdentifierMode, names ...string) []string {
		out := make([]string, len(names))
		for i, name := range names {
			out[i] = newPostgresDialect(mode).fold(name)
		}
		return out
	}

	cases := []postgresGoldenCase{
		// The select list.
		{name: "select all", query: album().SelectColumns()},
		{name: "columns and aliases", query: album().SelectColumns(col(field("Title"), ""), col(field("AlbumId"), "id"))},
		{name: "schema-qualified source with an alias", query: dal.From(dal.NewQualifiedRootCollectionRef("public", "Album", "a")).NewQuery().
			SelectColumns(col(typedTestQualified("a", "Title"), ""))},
		{name: "a select-all expanded from the catalog facts, minus exclusions", facts: withFacts,
			byMode: func(mode postgresIdentifierMode) dal.StructuredQuery {
				return typedTestFrom("Customer", "").NewQuery().SelectColumns(dal.AllColumnsExcept(catalogSpelling(mode, "Email", "SupportRepId")...))
			}},
		{name: "a select-all with a mask and an explicit column after it", facts: withFacts,
			byMode: func(mode postgresIdentifierMode) dal.StructuredQuery {
				return typedTestFrom("Customer", "").NewQuery().SelectColumns(dal.AllColumnsExcept(catalogSpelling(mode, "Support*", "E*")...), col(field("Email"), "mail"))
			}},
		{name: "a select-all needs the catalog facts", query: typedTestFrom("Customer", "").NewQuery().SelectColumns(dal.AllColumnsExcept("Email"))},
		{name: "an unaliased expression is named by its text", query: album().SelectColumns(col(dal.Binary(field("a"), dal.Add, field("b")), ""))},
		{name: "a constant in the select list needs an alias", query: album().SelectColumns(col(typedTestConst(5), "five"))},
		{name: "an unaliased expression carrying a constant is refused", query: album().SelectColumns(col(dal.Binary(field("a"), dal.Add, typedTestConst(1)), ""))},

		// Conditions.
		{name: "equality", query: album().Where(typedTestEq(field("AlbumId"), 7)).SelectColumns()},
		{name: "every comparison operator in AND and OR groups", query: album().Where(dal.NewGroupCondition(dal.And,
			dal.NewComparison(field("a"), dal.GreaterThen, typedTestConst(1)),
			dal.NewComparison(field("b"), dal.GreaterOrEqual, typedTestConst(2)),
			dal.NewGroupCondition(dal.Or,
				dal.NewComparison(field("c"), dal.LessThen, typedTestConst(3)),
				dal.NewComparison(field("d"), dal.LessOrEqual, typedTestConst(4)),
				typedTestEq(field("e"), "five")),
		)).SelectColumns()},
		{name: "equal to null is IS NULL and binds nothing", query: album().Where(typedTestEq(field("Title"), nil)).SelectColumns()},
		{name: "is null", query: album().Where(dal.NewIsNullCondition(field("Title"))).SelectColumns()},
		{name: "is not null", query: album().Where(dal.NewIsNotNullCondition(field("Title"))).SelectColumns()},
		{name: "ordering against null stays a bound NULL", query: album().Where(dal.NewComparison(field("AlbumId"), dal.GreaterThen, typedTestConst(nil))).SelectColumns()},
		{name: "IN list", query: album().Where(dal.NewComparison(field("AlbumId"), dal.In, dal.NewArray([]any{1, 2.5, "three"}))).SelectColumns()},
		{name: "NOT IN list", query: album().Where(dal.NewComparison(field("Title"), dal.NotIn, dal.NewArray([]string{"a", "b"}))).SelectColumns()},
		{name: "IN list with a NULL keeps three-valued logic", query: album().Where(dal.NewComparison(field("AlbumId"), dal.NotIn, dal.NewArray([]any{1, nil}))).SelectColumns()},
		{name: "empty IN is FALSE", query: album().Where(dal.NewComparison(field("AlbumId"), dal.In, ints())).SelectColumns()},
		{name: "empty NOT IN is TRUE", query: album().Where(dal.NewComparison(field("AlbumId"), dal.NotIn, ints())).SelectColumns()},

		// Arithmetic.
		{name: "add subtract multiply", query: album().SelectColumns(
			col(dal.Binary(field("a"), dal.Add, field("b")), "s"),
			col(dal.Binary(field("a"), dal.Subtract, typedTestConst(2)), "d"),
			col(dal.Binary(typedTestConst(2.5), dal.Multiply, field("b")), "m"))},
		{name: "divide is on double precision with a zero divisor read as NULL", query: album().SelectColumns(col(dal.Binary(field("a"), dal.Divide, field("b")), "q"))},
		{name: "nested division keeps argument order", query: album().SelectColumns(col(dal.Binary(
			dal.Binary(field("a"), dal.Add, typedTestConst(1)), dal.Divide, dal.Binary(field("b"), dal.Subtract, typedTestConst(2))), "q"))},
		{name: "division where both operands render the same text", query: album().SelectColumns(col(dal.Binary(
			dal.Binary(field("a"), dal.Add, typedTestConst(1)), dal.Divide, dal.Binary(field("a"), dal.Add, typedTestConst(2))), "q"))},

		// Aggregation.
		{name: "COUNT(*)", query: invoice().SelectColumns(dal.Count())},
		{name: "every aggregate", query: invoice().SelectColumns(
			dal.CountAs(field("InvoiceId"), "n"), dal.SumAs(field("Total"), "sum"), dal.AverageAs(field("Total"), "avg"),
			dal.MinAs(field("Total"), "min"), dal.MaxAs(field("Total"), "max"))},
		{name: "distinct aggregates", query: invoice().SelectColumns(
			distinct(dal.COUNT, "CustomerId", "customers"), distinct(dal.SUM, "Total", "sum"), distinct(dal.AVERAGE, "Total", "avg"))},
		{name: "GROUP BY with HAVING and ORDER BY through select aliases", query: invoice().
			GroupBy(field("BillingCountry")).
			Having(dal.NewComparison(field("revenue"), dal.GreaterThen, typedTestConst(1000))).
			OrderBy(dal.Descending(field("revenue")), dal.Ascending(field("country"))).
			Limit(10).
			SelectColumns(col(field("BillingCountry"), "country"), dal.SumAs(field("Total"), "revenue"), dal.CountAs(field("InvoiceId"), "invoices"))},
		{name: "a grouped expression carrying a constant is referenced by position", query: invoice().
			GroupBy(bucket).OrderBy(dal.Ascending(field("bucket"))).SelectColumns(bucketColumns...)},
		{name: "GROUP BY without selected columns groups by its keys", query: invoice().GroupBy(field("BillingCountry")).SelectColumns()},
		{name: "FIRST needs an input order SQL does not give", query: invoice().SelectColumns(dal.FirstAs(field("Total"), "first"))},
		{name: "an aggregate in WHERE is refused", query: invoice().Where(dal.NewComparison(dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.GreaterThen, typedTestConst(1))).SelectColumns()},

		// Ordering and paging.
		{name: "ORDER BY without facts keeps the NULLS clause", query: album().OrderBy(dal.Ascending(field("AlbumId")), dal.Descending(field("Title"))).SelectColumns()},
		{name: "ORDER BY a NOT NULL column drops the NULLS clause", facts: withFacts,
			query: album().OrderBy(dal.Ascending(field("AlbumId")), dal.Descending(field("AlbumId")), dal.Descending(field("Title"))).SelectColumns()},
		{name: "ORDER BY a nullable column keeps the NULLS clause", facts: withFacts,
			query: typedTestFrom("Artist", "").NewQuery().OrderBy(dal.Ascending(field("Name")), dal.Descending(field("Name"))).SelectColumns()},
		{name: "ORDER BY an aggregate", query: invoice().GroupBy(field("BillingCountry")).OrderBy(dal.Descending(dal.NewAggregate(dal.SUM, false, field("Total")))).
			SelectColumns(col(field("BillingCountry"), ""), dal.SumAs(field("Total"), "revenue"))},
		{name: "LIMIT", query: album().Limit(10).SelectColumns()},
		{name: "OFFSET", query: album().Offset(5).SelectColumns()},
		{name: "LIMIT and OFFSET", query: album().Limit(10).Offset(5).SelectColumns()},
		{name: "numbering follows the text across clauses", query: invoice().
			Where(typedTestEq(field("a"), 1)).GroupBy(field("g")).
			Having(dal.NewComparison(dal.NewAggregate(dal.SUM, false, field("t")), dal.GreaterThen, typedTestConst(2))).
			OrderBy(dal.Ascending(dal.Binary(field("g"), dal.Add, typedTestConst(3)))).Limit(4).Offset(5).
			SelectColumns(col(field("g"), ""), col(dal.Binary(dal.Count().Expression, dal.Add, typedTestConst(6)), "n"))},

		// Names.
		{name: "identifiers that hold quotes, question marks and spaces are quoted", query: typedTestFrom(`tab?le"x`, "").NewQuery().
			Where(typedTestEq(field(`we?ird"col`), 1)).OrderBy(dal.Ascending(field(`we?ird"col`))).SelectColumns(col(field(`we?ird"col`), `a?"b`))},
		{name: "names with non-ASCII capitals", query: typedTestFrom("Écoles", "").NewQuery().Where(typedTestEq(field("Élève"), 1)).SelectColumns(col(field("Élève"), "Prénom"))},
		{name: "a name over 63 bytes is refused, never truncated", query: typedTestFrom(strings.Repeat("N", 64), "").NewQuery().SelectColumns()},
		{name: "a name of 44 bytes that folds to 66 is refused in FoldLower only", query: typedTestFrom(strings.Repeat("Ⱥ", 22), "").NewQuery().SelectColumns()},
		{name: "a name that is a bare source identity without facts is refused", query: typedTestFrom("Invoice", "x").NewQuery().SelectColumns(col(field("x"), ""))},
		{name: "a name that is not a column of a known source is an error", facts: withFacts, query: typedTestFrom("Album", "x").NewQuery().SelectColumns(col(typedTestQualified("x", "to_json"), ""))},

		// What stays out of one statement.
		{name: "subqueries run in the generic engine", query: dal.From(dal.NewQuerySource(album().SelectColumns(), "s")).NewQuery().SelectColumns()},
		{name: "cursors are refused", query: album().StartFrom("c").SelectColumns()},
	}
	assertPostgresGoldenGroup(t, "shapes", cases)
}

// TestPostgresGoldenConstants pins the cast and the argument of each constant type.
func TestPostgresGoldenConstants(t *testing.T) {
	stampUTC := time.Date(2026, time.October, 4, 12, 30, 15, 0, time.UTC)
	stampZoned := time.Date(2026, time.October, 4, 12, 30, 15, 123456789, time.FixedZone("CEST", 2*3600))
	of := func(value any) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().Where(typedTestEq(typedTestField("Title"), value)).SelectColumns()
	}
	cases := []postgresGoldenCase{
		{name: "int", query: of(7)},
		{name: "negative int", query: of(-7)},
		{name: "zero", query: of(0)},
		{name: "int8", query: of(int8(-8))},
		{name: "int64 maximum", query: of(int64(math.MaxInt64))},
		{name: "int64 minimum", query: of(int64(math.MinInt64))},
		{name: "uint8", query: of(uint8(8))},
		{name: "uint64 above bigint", query: of(uint64(math.MaxUint64))},
		{name: "float64 fraction", query: of(0.5)},
		{name: "float64 whole", query: of(5.0)},
		{name: "float64 shortest text", query: of(0.1)},
		{name: "float64 negative zero", query: of(math.Copysign(0, -1))},
		{name: "float64 large", query: of(1e21)},
		{name: "float64 tiny", query: of(1e-7)},
		{name: "float32 keeps its own shortest text", query: of(float32(0.1))},
		{name: "NaN", query: of(math.NaN())},
		{name: "positive infinity", query: of(math.Inf(1))},
		{name: "negative infinity", query: of(math.Inf(-1))},
		{name: "string stays untyped", query: of("Ada Lovelace")},
		{name: "empty string", query: of("")},
		{name: "a string of digits is still a string", query: of("42")},
		{name: "a string that looks like a date is read by the server", query: of("2026-10-04")},
		{name: "bool true", query: of(true)},
		{name: "bool false", query: of(false)},
		{name: "time in UTC", query: of(stampUTC)},
		{name: "time keeps its instant under another zone", query: of(stampZoned)},
		{name: "bytes", query: of([]byte{0, 1, 0xfe})},
		{name: "empty bytes", query: of([]byte{})},
		{name: "nil is an untyped NULL in an ordering comparison", query: typedTestFrom("Album", "").NewQuery().
			Where(dal.NewComparison(typedTestField("AlbumId"), dal.LessThen, typedTestConst(nil))).SelectColumns()},
		{name: "a type DALgo cannot hold is refused", query: of(struct{}{})},
		{name: "mixed constants in an IN list", query: typedTestFrom("Album", "").NewQuery().
			Where(dal.NewComparison(typedTestField("AlbumId"), dal.In, dal.NewArray([]any{1, 2.5, uint(3), "four", true, stampUTC, []byte("x"), nil}))).SelectColumns()},
	}
	assertPostgresGoldenGroup(t, "constants", cases)
}

// postgresInjectionProbes are values that would matter if one ever reached the
// statement text: quotes, comments, statement separators, placeholders and
// escapes.
var postgresInjectionProbes = []string{
	`O'Brien'; DROP TABLE "Album"; --`,
	`x') OR 1=1 --`,
	`'; SELECT pg_sleep(10); --`,
	`$1`,
	`?`,
	`\`,
	`%`,
	`"; DROP TABLE x; --`,
	"line\nbreak ",
	"nul\x00byte",
	`/* c */`,
}

// postgresProbeQuery puts one probe in every place a value can go: a comparison,
// an IN list, HAVING, an arithmetic operand, and the select list through a
// grouped expression.
func postgresProbeQuery(probe string) dal.StructuredQuery {
	field := typedTestField
	shifted := dal.Binary(field("Total"), dal.Add, typedTestConst(probe))
	return typedTestFrom("Invoice", "").NewQuery().
		Where(dal.NewGroupCondition(dal.And,
			typedTestEq(field("BillingCity"), probe),
			dal.NewComparison(field("BillingCountry"), dal.In, dal.NewArray([]string{"FR", probe})),
		)).
		GroupBy(shifted).
		Having(dal.NewComparison(field("revenue"), dal.GreaterThen, typedTestConst(probe))).
		OrderBy(dal.Ascending(field("bucket"))).
		SelectColumns(typedTestColumn(shifted, "bucket"), dal.SumAs(field("Total"), "revenue"))
}

// TestPostgresGoldenInjectionProbes pins the statements built around hostile
// values, and proves the values appear only in the arguments: the statement is the
// same for every probe and for a harmless value in the same places, so nothing of
// a probe is in it.
func TestPostgresGoldenInjectionProbes(t *testing.T) {
	var cases []postgresGoldenCase
	for i, probe := range postgresInjectionProbes {
		cases = append(cases, postgresGoldenCase{name: fmt.Sprintf("probe %d in every position", i+1), query: postgresProbeQuery(probe)})
	}
	assertPostgresGoldenGroup(t, "probes", cases)

	for _, mode := range []postgresIdentifierMode{postgresExact, postgresFoldLower} {
		dialect := newPostgresDialect(mode)
		reference, _, err := compileTypedSQL(postgresProbeQuery("harmless"), dialect, typedCatalogFacts{})
		if err != nil {
			t.Fatal(err)
		}
		for i, probe := range postgresInjectionProbes {
			text, args, err := compileTypedSQL(postgresProbeQuery(probe), dialect, typedCatalogFacts{})
			if err != nil {
				t.Fatalf("probe %d: %v", i+1, err)
			}
			if text != reference {
				t.Fatalf("probe %d changed the statement:\n%s\n%s", i+1, text, reference)
			}
			found := 0
			for _, arg := range args {
				if arg == probe {
					found++
				}
			}
			if found != 4 {
				t.Fatalf("probe %q is bound %d times, want 4 (comparison, list, arithmetic, HAVING): %v", probe, found, args)
			}
		}
	}
}

// TestPostgresGoldenDatatugShippedDTQL compiles every DTQL text DataTug ships
// (dtql_datatug_inventory_test.go) with the PostgreSQL dialect, with the catalog
// facts of the sample database, as DataTug's protected read path will.
func TestPostgresGoldenDatatugShippedDTQL(t *testing.T) {
	var cases []postgresGoldenCase
	for _, tc := range datatugShippedDTQLCases {
		cases = append(cases, postgresGoldenCase{name: tc.name, query: datatugInventoryQuery(t, tc), facts: postgresChinookFacts})
	}
	for _, tc := range datatugShippedDTQLCases {
		cases = append(cases, postgresGoldenCase{name: tc.name + ", without catalog facts", query: datatugInventoryQuery(t, tc)})
	}
	assertPostgresGoldenGroup(t, "datatug-dtql", cases)
}

// postgresJoinFixture deserializes one of the vendored DTQL join fixtures.
func postgresJoinFixture(t *testing.T, name string) (dal.StructuredQuery, error) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "joins", name))
	if err != nil {
		t.Fatal(err)
	}
	return dtql.Deserialize(contents)
}

// TestPostgresGoldenJoins compiles the join fixtures of testdata/joins, and the
// join shapes the fixtures do not cover, with the catalog facts of the sample
// database.
func TestPostgresGoldenJoins(t *testing.T) {
	var cases []postgresGoldenCase
	for _, name := range []string{"chinook-nested.dtql.yaml", "chinook-hinted.dtql.yaml", "chinook-wildcard.dtql.yaml"} {
		query, err := postgresJoinFixture(t, name)
		if err != nil {
			t.Fatalf("dtql.Deserialize(%s): %v", name, err)
		}
		cases = append(cases, postgresGoldenCase{name: name, query: query, facts: postgresChinookFacts})
	}
	on := typedTestJoinOn
	field := typedTestQualified
	artist := dal.NewRootCollectionRef("Artist", "r")
	cases = append(cases,
		postgresGoldenCase{name: "inner join", facts: postgresChinookFacts, query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinInner, on("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().SelectColumns(typedTestColumn(field("a", "Title"), ""), typedTestColumn(field("r", "Name"), "artist"))},
		postgresGoldenCase{name: "left join with two key pairs, then a default join type", facts: postgresChinookFacts, query: typedTestFrom("Customer", "c").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Employee", "e"), dal.JoinLeft, on("c", "SupportRepId", "e", "EmployeeId"), on("c", "FirstName", "e", "FirstName")),
		).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Invoice", "i"), "", on("c", "CustomerId", "i", "CustomerId")),
		).NewQuery().SelectColumns(typedTestColumn(field("e", "FirstName"), "rep"))},
		postgresGoldenCase{name: "ORDER BY on the preserved and on the nullable side of a left join", facts: postgresChinookFacts, query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinLeft, on("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().OrderBy(dal.Ascending(field("a", "AlbumId")), dal.Ascending(field("r", "ArtistId"))).SelectColumns()},
		postgresGoldenCase{name: "aggregation over a join", facts: postgresChinookFacts, query: typedTestFrom("Invoice", "i").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Customer", "c"), dal.JoinInner, on("i", "CustomerId", "c", "CustomerId")),
		).NewQuery().GroupBy(field("c", "Country")).Having(dal.NewComparison(typedTestField("revenue"), dal.GreaterThen, typedTestConst(1000))).
			OrderBy(dal.Descending(typedTestField("revenue"))).Limit(10).
			SelectColumns(typedTestColumn(field("c", "Country"), "country"), dal.SumAs(field("i", "Total"), "revenue"))},
		postgresGoldenCase{name: "key types that differ are refused before the server sees them", facts: postgresChinookFacts, query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinInner, on("a", "ArtistId", "r", "Name")),
		).NewQuery().SelectColumns()},
		postgresGoldenCase{name: "a join key that is not a column is an error", facts: postgresChinookFacts, query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinInner, on("a", "ArtistId", "r", "to_json")),
		).NewQuery().SelectColumns()},
		postgresGoldenCase{name: "a join without facts is not type checked", query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinInner, on("a", "ArtistId", "r", "Name")),
		).NewQuery().SelectColumns()},
		postgresGoldenCase{name: "a right join is refused", query: typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(artist, dal.JoinRight, on("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().SelectColumns()},
		postgresGoldenCase{name: "a scan-bounded source needs the generic engine", query: dal.From(dal.NewRootCollectionRef("Album", "a").WithScan(10)).Join(
			dal.NewJoinedSource(artist, dal.JoinInner, on("a", "ArtistId", "r", "ArtistId")),
		).NewQuery().SelectColumns()},
	)
	assertPostgresGoldenGroup(t, "joins", cases)
}

// TestPostgresJoinErrorFixtures: the two vendored error fixtures are refused with
// the category and path their .error.json files name, before any dialect is asked.
func TestPostgresJoinErrorFixtures(t *testing.T) {
	for _, name := range []string{"forward-alias", "unknown-alias"} {
		t.Run(name, func(t *testing.T) {
			contents, err := os.ReadFile(filepath.Join("testdata", "joins", name+".error.json"))
			if err != nil {
				t.Fatal(err)
			}
			var want struct{ Category, Path string }
			if err := json.Unmarshal(contents, &want); err != nil {
				t.Fatal(err)
			}
			query, err := postgresJoinFixture(t, name+".dtql.yaml")
			if err == nil {
				_, _, err = compileTypedSQL(query, newPostgresDialect(postgresExact), postgresChinookFacts(postgresExact))
			}
			var joinErr *dal.JoinValidationError
			if !errors.As(err, &joinErr) || joinErr.Category != want.Category || joinErr.Path != want.Path {
				t.Fatalf("error = %v, want a join validation error %+v", err, want)
			}
		})
	}
}
