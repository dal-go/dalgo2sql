package dalgo2sql

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// The MySQL, Oracle and SQL Server golden files live in testdata/mysql, testdata/oracle
// and testdata/sqlserver. Each
// holds, for one group of queries, the exact statement and arguments, or the refusal,
// compiled with the catalog facts the PostgreSQL goldens use in their exact mode (the
// two dialects write names exactly as spelt). Review a changed golden as you would
// review code. Rewrite them with
//
//	go test -run 'TestMySQLOracle' -update-dialect-golden
var updateDialectGolden = flag.Bool("update-dialect-golden", false, "rewrite the golden files under testdata/mysql and testdata/oracle instead of comparing them")

// typedGoldenDialects are the dialects whose goldens this file pins, by directory.
var typedGoldenDialects = []struct {
	dir     string
	dialect typedDialect
}{
	{"mysql", mysqlDialect{}},
	{"oracle", oracleDialect{}},
	{"sqlserver", sqlserverDialect{}},
}

// renderTypedDialectGolden compiles every case with dialect, in the exact mode's
// spelling and facts, and writes the statement, the arguments and the refusals.
func renderTypedDialectGolden(cases []postgresGoldenCase, dialect typedDialect) string {
	var out strings.Builder
	for _, c := range cases {
		fmt.Fprintf(&out, "=== %s\n", c.name)
		var facts typedCatalogFacts
		if c.facts != nil {
			facts = c.facts(postgresExact)
		}
		query := c.query
		if c.byMode != nil {
			query = c.byMode(postgresExact)
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
				fmt.Fprintf(&out, "#%d %s\n", i+1, formatPostgresArg(arg))
			}
		}
	}
	return out.String()
}

// assertTypedDialectGoldens compares the golden of the group for each dialect.
func assertTypedDialectGoldens(t *testing.T, group string, cases []postgresGoldenCase) {
	t.Helper()
	for _, d := range typedGoldenDialects {
		t.Run(d.dir, func(t *testing.T) {
			path := filepath.Join("testdata", d.dir, group+".golden")
			got := renderTypedDialectGolden(cases, d.dialect)
			if *updateDialectGolden {
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
				t.Fatalf("read golden %s: %v (create it with -update-dialect-golden)", path, err)
			}
			if string(want) != got {
				t.Fatalf("golden %s differs (rewrite with -update-dialect-golden and review the diff)\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
			}
		})
	}
}

func TestMySQLOracleGoldenShapes(t *testing.T) {
	assertTypedDialectGoldens(t, "shapes", postgresGoldenShapeCases())
}

func TestMySQLOracleGoldenConstants(t *testing.T) {
	assertTypedDialectGoldens(t, "constants", postgresGoldenConstantCases())
}

func TestMySQLOracleGoldenJoins(t *testing.T) {
	assertTypedDialectGoldens(t, "joins", postgresGoldenJoinCases(t))
}

func TestMySQLOracleGoldenInjectionProbes(t *testing.T) {
	var cases []postgresGoldenCase
	for i, probe := range postgresInjectionProbes {
		cases = append(cases, postgresGoldenCase{name: fmt.Sprintf("probe %d in every position", i+1), query: postgresProbeQuery(probe)})
	}
	assertTypedDialectGoldens(t, "probes", cases)
}

// TestMySQLOracleGoldenDatatugShippedTugQL compiles every TugQL text DataTug ships with
// both dialects and the sample database's facts, as DataTug's read path will.
func TestMySQLOracleGoldenDatatugShippedTugQL(t *testing.T) {
	var cases []postgresGoldenCase
	for _, tc := range datatugShippedTugQLCases {
		cases = append(cases, postgresGoldenCase{name: tc.name, query: datatugInventoryQuery(t, tc), facts: postgresChinookFacts})
	}
	assertTypedDialectGoldens(t, "datatug-tugql", cases)
}

// TestMySQLOracleGoldenNames pins what each dialect does with the names it must
// refuse or can write: its own quote character, the length limits, characters past
// the Basic Multilingual Plane, a trailing space and a table alias.
func TestMySQLOracleGoldenNames(t *testing.T) {
	from := func(name string) dal.StructuredQuery { return typedTestFrom(name, "").NewQuery().SelectColumns() }
	column := func(name string) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField(name), ""))
	}
	cases := []postgresGoldenCase{
		{name: "a backtick in a name", query: column("a`b")},
		{name: "a double quote in a name", query: column(`a"b`)},
		{name: "a name of 64 characters", query: from(strings.Repeat("é", 64))},
		{name: "a name of 65 characters", query: from(strings.Repeat("é", 65))},
		{name: "a name of 128 bytes", query: from(strings.Repeat("n", 128))},
		{name: "a name of 129 bytes", query: from(strings.Repeat("n", 129))},
		{name: "a name of 64 characters outside the Basic Multilingual Plane", query: from(strings.Repeat("\U0001F600", 64))},
		{name: "a name of 65 characters outside the Basic Multilingual Plane", query: from(strings.Repeat("\U0001F600", 65))},
		{name: "a character outside the Basic Multilingual Plane", query: column("emoji\U0001F600")},
		{name: "a name ending with a space", query: column("Title ")},
		{name: "an alias of 65 characters is declined", query: typedTestFrom("Album", "").NewQuery().
			SelectColumns(typedTestColumn(typedTestField("Title"), strings.Repeat("a", 65)))},
		{name: "a table alias", query: typedTestFrom("Album", "a").NewQuery().Where(typedTestEq(typedTestQualified("a", "AlbumId"), 1)).
			SelectColumns(typedTestColumn(typedTestQualified("a", "Title"), ""))},
		{name: "an output named as another column is no capture: every field is qualified", facts: postgresChinookFacts, query: typedTestFrom("Album", "").NewQuery().
			OrderBy(dal.Ascending(typedTestField("Title")), dal.Descending(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestField("ArtistId")))).
			SelectColumns(typedTestColumn(typedTestField("AlbumId"), "Title"), typedTestColumn(typedTestField("Title"), "AlbumId"))},
		{name: "GROUP BY a column, HAVING an aggregate of a column an output is named after", facts: postgresChinookFacts, query: typedTestFrom("Invoice", "").NewQuery().
			GroupBy(typedTestField("BillingCountry")).
			Having(dal.NewComparison(dal.NewAggregate(dal.SUM, false, typedTestField("Total")), dal.GreaterThen, typedTestConst(1))).
			SelectColumns(typedTestColumn(typedTestField("BillingCountry"), "Total"), dal.SumAs(typedTestField("Total"), "BillingCountry"))},
	}
	assertTypedDialectGoldens(t, "names", cases)
}

// oracleWithPositionalGroupBy is the Oracle dialect with GROUP BY by position, which
// lets the value-independence proof, whose query groups by an expression that
// carries a constant, run over every other method of the dialect.
type oracleWithPositionalGroupBy struct{ oracleDialect }

func (oracleWithPositionalGroupBy) groupByPosition() bool { return true }

// sqlserverWithPositionalGroupBy is the SQL Server dialect with GROUP BY by position,
// for the same reason.
type sqlserverWithPositionalGroupBy struct{ sqlserverDialect }

func (sqlserverWithPositionalGroupBy) groupByPosition() bool { return true }

func TestMySQLOracleTextDoesNotDependOnValues(t *testing.T) {
	for name, dialect := range map[string]typedDialect{"mysql": mysqlDialect{}, "oracle": oracleWithPositionalGroupBy{}, "sqlserver": sqlserverWithPositionalGroupBy{}} {
		t.Run(name, func(t *testing.T) {
			assertTypedTextIgnoresValues(t, dialect)
			assertTypedTextHasTwoShapesForTwoConstants(t, dialect)
		})
	}
}
