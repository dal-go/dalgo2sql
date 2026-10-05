package dalgo2sql

import (
	"context"
	"math"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// postgresMeasureFacts is what the catalog says about Measure(Id integer, Reading real,
// Wide double precision, Amount numeric, Readings real[]) in a mode: the types a float
// constant is compared with.
func postgresMeasureFacts(mode postgresIdentifierMode) typedCatalogFacts {
	dialect := newPostgresDialect(mode)
	facts, err := dialect.catalogFacts(context.Background(), nil, nil)
	if err != nil {
		panic(err)
	}
	col := func(name, dataType string, category typedTypeCategory) typedColumnFact {
		return typedColumnFact{Name: dialect.fold(name), DataType: dataType, Category: category}
	}
	facts.Sources = map[typedSourceName]typedSourceFacts{
		{Name: dialect.fold("Measure")}: {Columns: []typedColumnFact{
			col("Id", "integer", typedTypeNumber),
			col("Reading", "real", typedTypeNumber),
			col("Wide", "double precision", typedTypeNumber),
			col("Amount", "numeric", typedTypeNumber),
			col("Readings", "real[]", typedTypeOther),
		}},
	}
	return facts
}

// TestPostgresGoldenFloatConstantsAgainstTheTypeOfTheirColumn pins the statement and the argument
// of a float compared with a column: a column the catalog types as real is compared in its own
// type, so that the value 0.1 it stores is found by the constant 0.1, and double precision and
// numeric columns are compared as they always were.
func TestPostgresGoldenFloatConstantsAgainstTheTypeOfTheirColumn(t *testing.T) {
	measure := func(alias string) dal.IQueryBuilder { return typedTestFrom("Measure", alias).NewQuery() }
	against := func(column string, value any) dal.StructuredQuery {
		return measure("").Where(typedTestEq(typedTestField(column), value)).SelectColumns()
	}
	cases := []postgresGoldenCase{
		{name: "float64 against real", facts: postgresMeasureFacts, query: against("Reading", 0.1)},
		{name: "float32 against real", facts: postgresMeasureFacts, query: against("Reading", float32(0.1))},
		{name: "a whole float against real", facts: postgresMeasureFacts, query: against("Reading", 5.0)},
		{name: "negative zero against real", facts: postgresMeasureFacts, query: against("Reading", math.Copysign(0, -1))},
		{name: "NaN against real", facts: postgresMeasureFacts, query: against("Reading", math.NaN())},
		{name: "infinity against real", facts: postgresMeasureFacts, query: against("Reading", math.Inf(-1))},
		{name: "the largest float32 against real", facts: postgresMeasureFacts, query: against("Reading", float64(math.MaxFloat32))},
		{name: "the smallest normal float32 against real", facts: postgresMeasureFacts, query: against("Reading", math.Float32frombits(0x00800000))},
		{name: "every ordering operator against real", facts: postgresMeasureFacts, query: measure("").Where(dal.NewGroupCondition(dal.And,
			dal.NewComparison(typedTestField("Reading"), dal.GreaterThen, typedTestConst(0.1)),
			dal.NewComparison(typedTestField("Reading"), dal.GreaterOrEqual, typedTestConst(0.2)),
			dal.NewComparison(typedTestField("Reading"), dal.LessThen, typedTestConst(0.3)),
			dal.NewComparison(typedTestField("Reading"), dal.LessOrEqual, typedTestConst(0.4)),
		)).SelectColumns()},
		{name: "a qualified real column in a join-shaped source", facts: postgresMeasureFacts, query: measure("m").
			Where(typedTestEq(typedTestQualified("m", "Reading"), 0.1)).SelectColumns()},
		{name: "an IN list against real", facts: postgresMeasureFacts, query: measure("").Where(dal.NewComparison(typedTestField("Reading"), dal.In, dal.NewArray([]any{0.1, float32(0.25), 3, "x", nil}))).SelectColumns()},
		{name: "a NOT IN list against real", facts: postgresMeasureFacts, query: measure("").Where(dal.NewComparison(typedTestField("Reading"), dal.NotIn, dal.NewArray([]float64{0.1, 0.2}))).SelectColumns()},

		// A float a real cannot hold keeps the binding it always had, as every other type does.
		{name: "a float past the range of real keeps its binding", facts: postgresMeasureFacts, query: against("Reading", 1e39)},
		{name: "a float below the range of real keeps its binding", facts: postgresMeasureFacts, query: against("Reading", 1e-40)},
		{name: "a negative float past the range of real keeps its binding", facts: postgresMeasureFacts, query: against("Reading", -1e300)},
		{name: "a whole number is bound as before", facts: postgresMeasureFacts, query: against("Reading", 5)},
		{name: "a string is bound as before", facts: postgresMeasureFacts, query: against("Reading", "0.1")},
		{name: "a NULL is IS NULL as before", facts: postgresMeasureFacts, query: against("Reading", nil)},

		{name: "float64 against double precision", facts: postgresMeasureFacts, query: against("Wide", 0.1)},
		{name: "float32 against double precision", facts: postgresMeasureFacts, query: against("Wide", float32(0.1))},
		{name: "float64 against numeric", facts: postgresMeasureFacts, query: against("Amount", 0.1)},
		{name: "float64 against integer", facts: postgresMeasureFacts, query: against("Id", 0.1)},
		{name: "float64 against an array of real", facts: postgresMeasureFacts, query: against("Readings", 0.1)},
		{name: "an IN list against double precision", facts: postgresMeasureFacts, query: measure("").Where(dal.NewComparison(typedTestField("Wide"), dal.In, dal.NewArray([]float64{0.1, 0.2}))).SelectColumns()},

		{name: "a float against a column its source does not have is an error as before", facts: postgresMeasureFacts, query: against("Missing", 0.1)},
		{name: "a float against a source the facts do not know", query: against("Reading", 0.1)},
		{name: "a float in arithmetic on a real column keeps its binding", facts: postgresMeasureFacts, query: measure("").
			Where(dal.NewComparison(dal.Binary(typedTestField("Reading"), dal.Add, typedTestConst(0.1)), dal.GreaterThen, typedTestConst(1))).SelectColumns()},
		{name: "a float against a real column in HAVING", facts: postgresMeasureFacts, query: measure("").GroupBy(typedTestField("Id")).
			Having(dal.NewComparison(dal.NewAggregate(dal.MAX, false, typedTestField("Reading")), dal.GreaterThen, typedTestConst(0.1))).
			SelectColumns(typedTestColumn(typedTestField("Id"), ""), dal.MaxAs(typedTestField("Reading"), "top"))},
	}
	assertPostgresGoldenGroup(t, "reals", cases)
}

// bindAgainst on its own: only a float compared with a column typed real is bound as a real.
func TestPostgresDialectBindsAFloatAgainstARealAsAReal(t *testing.T) {
	dialect := newPostgresDialect(postgresExact)
	real, double := typedColumnFact{DataType: "real"}, typedColumnFact{DataType: "double precision"}
	for _, tc := range []struct {
		name   string
		value  any
		column typedColumnFact
		arg    string // "" when the value is bound as it always was
	}{
		{"float64", 0.1, real, "0.1"},
		{"float32", float32(0.1), real, "0.1"},
		{"zero", 0.0, real, "0"},
		{"infinity", math.Inf(1), real, "Infinity"},
		{"NaN", math.NaN(), real, "NaN"},
		{"the largest real", float64(math.MaxFloat32), real, "340282346638528860000000000000000000000"},
		{"just past the largest real", math.Nextafter(math.MaxFloat32, math.Inf(1)), real, ""},
		{"the smallest normal real", math.Float64frombits(math.Float64bits(1.17549435082228750796873653722224568e-38)), real, "0.000000000000000000000000000000000000011754943508222875"},
		{"below the smallest normal real", 1e-39, real, ""},
		{"negative, past the range", -1e39, real, ""},
		{"an integer", 5, real, ""},
		{"a string", "0.1", real, ""},
		{"nil", nil, real, ""},
		{"double precision", 0.1, double, ""},
		{"no type known", 0.1, typedColumnFact{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker, arg, ok := dialect.bindAgainst(tc.value, tc.column)
			if tc.arg == "" {
				if ok {
					t.Fatalf("bindAgainst = %q, %v, true; want the value bound as it was", marker, arg)
				}
				return
			}
			if !ok || marker != "?::real" || arg != tc.arg {
				t.Fatalf("bindAgainst = %q, %v, %v; want ?::real, %q", marker, arg, ok, tc.arg)
			}
		})
	}
}

// The compiler asks only a dialect that binds against a column, and a comparison whose facts are
// known; a constant that no dialect can bind is refused as it was.
func TestTypedCompilerBindsAgainstAColumnOnlyWhereTheDialectAsks(t *testing.T) {
	q := typedTestFrom("Measure", "").NewQuery().Where(typedTestEq(typedTestField("Reading"), 0.1)).SelectColumns()
	text, args, err := compileTypedSQL(q, newPostgresDialect(postgresExact), postgresMeasureFacts(postgresExact))
	if want := `SELECT * FROM "Measure" WHERE "Reading" = $1::real`; err != nil || text != want || len(args) != 1 {
		t.Fatalf("= %q, %v, %v; want %q", text, args, err, want)
	}
	unsupported := typedTestFrom("Measure", "").NewQuery().Where(typedTestEq(typedTestField("Reading"), struct{}{})).SelectColumns()
	if _, _, err := compileTypedSQL(unsupported, newPostgresDialect(postgresExact), postgresMeasureFacts(postgresExact)); err == nil {
		t.Fatal("a constant of a type DALgo cannot hold compiled")
	}
}

// badColumnBinder is a dialect whose binding against a column breaks the contract of a marker:
// it writes two where the compiler binds one value.
type badColumnBinder struct{ typedDialect }

func (badColumnBinder) bindAgainst(any, typedColumnFact) (string, any, bool) {
	return "?, ?", "0.1", true
}

// A marker that breaks the contract is refused, as one of bind's is.
func TestTypedCompilerRefusesAColumnBindingThatBreaksTheMarkerContract(t *testing.T) {
	q := typedTestFrom("Measure", "").NewQuery().Where(typedTestEq(typedTestField("Reading"), 0.1)).SelectColumns()
	dialect := badColumnBinder{newPostgresDialect(postgresExact)}
	if _, _, err := compileTypedSQL(q, dialect, postgresMeasureFacts(postgresExact)); err == nil {
		t.Fatal("a marker that carries two placeholders was accepted")
	}
}
