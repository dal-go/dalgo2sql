package dalgo2sql

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// fakeTypedOpaqueValuer is a driver.Valuer with no exported field: every
// instance has the same text under dal.Constant.String(), whatever it holds.
type fakeTypedOpaqueValuer struct{ n int }

func (fakeTypedOpaqueValuer) Value() (driver.Value, error) { return nil, nil }

type fakeTypedNamedText string

type fakeTypedNamedBytes []byte

func TestCompileTypedSQLOrdinalMatchesOnlyTheSameConstants(t *testing.T) {
	// ORDER BY (AlbumId + <ordered>) against a selected (AlbumId + <selected>).
	// dal.Constant.String() is empty for every infinity and NaN and identical for
	// every opaque Valuer, so a match by text would sort by the selected
	// expression while the caller wrote another one.
	orderedBy := func(selected, ordered any) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().
			OrderBy(dal.Ascending(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(ordered)))).
			SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(selected)), "shifted"))
	}
	anyMarker := newFakeTypedDialect()
	anyMarker.bindMarkers = "?"
	inf, negInf := math.Inf(1), math.Inf(-1)
	runTypedGoldens(t, []typedGolden{
		{
			name:     "the same infinity is matched",
			query:    orderedBy(inf, inf),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::numeric) AS "shifted" FROM "Album" ORDER BY 2 ASC NULLS FIRST`,
			wantArgs: []any{inf},
		},
		{
			name:     "two infinities with the same text but opposite signs are not",
			query:    orderedBy(inf, negInf),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::numeric) AS "shifted" FROM "Album" ORDER BY ("AlbumId" + $2::numeric) ASC NULLS FIRST`,
			wantArgs: []any{inf, negInf},
		},
		{
			name:     "an integer of another width but the same value is matched",
			query:    orderedBy(1, int64(1)),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::bigint) AS "shifted" FROM "Album" ORDER BY 2 ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			name:     "a signed and an unsigned integer are different kinds and are not matched",
			query:    orderedBy(1, uint(1)),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::bigint) AS "shifted" FROM "Album" ORDER BY ("AlbumId" + $2::bigint) ASC NULLS FIRST`,
			wantArgs: []any{1, uint(1)},
		},
		{
			name:     "opaque valuers holding the same value are matched",
			dialect:  anyMarker,
			query:    orderedBy(fakeTypedOpaqueValuer{1}, fakeTypedOpaqueValuer{1}),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1) AS "shifted" FROM "Album" ORDER BY 2 ASC NULLS FIRST`,
			wantArgs: []any{fakeTypedOpaqueValuer{1}},
		},
		{
			name:     "opaque valuers with the same text and different contents are not",
			dialect:  anyMarker,
			query:    orderedBy(fakeTypedOpaqueValuer{1}, fakeTypedOpaqueValuer{2}),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1) AS "shifted" FROM "Album" ORDER BY ("AlbumId" + $2) ASC NULLS FIRST`,
			wantArgs: []any{fakeTypedOpaqueValuer{1}, fakeTypedOpaqueValuer{2}},
		},
	})
}

func TestTypedSameConstant(t *testing.T) {
	stamp := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	elsewhere := stamp.In(time.FixedZone("UTC+3", 3*3600))
	type shape struct{ n int }
	cases := []struct {
		name string
		a, b any
		want bool
	}{
		{"nil and nil", nil, nil, true},
		{"nil and a value", nil, 0, false},
		{"a value and nil", "", nil, false},
		{"equal integers", 5, 5, true},
		{"integers of different width", int8(5), int64(5), true},
		{"different integers", 5, 6, false},
		{"equal unsigned integers", uint8(5), uint64(5), true},
		{"different unsigned integers", uint(5), uint(6), false},
		{"signed against unsigned", 5, uint(5), false},
		{"equal floats", 1.5, 1.5, true},
		{"different floats", 1.5, 2.5, false},
		{"NaN against NaN", math.NaN(), math.NaN(), true},
		{"NaN against a number", math.NaN(), 1.0, false},
		{"a number against NaN", 1.0, math.NaN(), false},
		{"the same infinity", math.Inf(1), math.Inf(1), true},
		{"opposite infinities", math.Inf(1), math.Inf(-1), false},
		{"integer against float", 1, 1.0, false},
		{"equal strings", "a", "a", true},
		{"different strings", "a", "b", false},
		{"a named string type against string", fakeTypedNamedText("a"), "a", true},
		{"string against bytes", "a", []byte("a"), false},
		{"equal booleans", true, true, true},
		{"different booleans", true, false, false},
		{"equal bytes", []byte{1, 2}, []byte{1, 2}, true},
		{"a named byte slice against a byte slice", fakeTypedNamedBytes{1}, []byte{1}, true},
		{"different bytes", []byte{1, 2}, []byte{1, 3}, false},
		{"the same instant in two zones", stamp, elsewhere, true},
		{"different instants", stamp, stamp.Add(time.Nanosecond), false},
		{"equal valuers", fakeTypedOpaqueValuer{1}, fakeTypedOpaqueValuer{1}, true},
		{"different valuers", fakeTypedOpaqueValuer{1}, fakeTypedOpaqueValuer{2}, false},
		{"a value the compiler refuses on the left", shape{1}, 1, false},
		{"a value the compiler refuses on the right", 1, shape{1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := typedSameConstant(tc.a, tc.b); got != tc.want {
				t.Fatalf("typedSameConstant(%#v, %#v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestTypedSameExpression(t *testing.T) {
	field := func(source, name string) dal.Expression { return dal.NewFieldRef(source, name) }
	sum := func(distinct bool, args ...dal.Expression) dal.Expression {
		return dal.NewAggregate(dal.SUM, distinct, args...)
	}
	plus := func(l, r dal.Expression) dal.Expression { return dal.Binary(l, dal.Add, r) }
	cases := []struct {
		name string
		a, b dal.Expression
		want bool
	}{
		{"the same field", field("", "a"), field("", "a"), true},
		{"fields with different sources", field("x", "a"), field("", "a"), false},
		{"fields with different names", field("", "a"), field("", "b"), false},
		{"the document identity against a field of that name", dal.DocumentID(), field("", "__name__"), false},
		{"a field against a constant", field("", "a"), typedTestConst(1), false},
		{"equal constants", typedTestConst(1), typedTestConst(1), true},
		{"different constants", typedTestConst(1), typedTestConst(2), false},
		{"a constant against a field", typedTestConst(1), field("", "a"), false},
		{"equal arithmetic", plus(field("", "a"), typedTestConst(1)), plus(field("", "a"), typedTestConst(1)), true},
		{"arithmetic with another operator", plus(field("", "a"), typedTestConst(1)), dal.Binary(field("", "a"), dal.Subtract, typedTestConst(1)), false},
		{"arithmetic with another left", plus(field("", "a"), typedTestConst(1)), plus(field("", "b"), typedTestConst(1)), false},
		{"arithmetic with another right constant", plus(field("", "a"), typedTestConst(1)), plus(field("", "a"), typedTestConst(2)), false},
		{"arithmetic against a field", plus(field("", "a"), typedTestConst(1)), field("", "a"), false},
		{"equal aggregates", sum(false, field("", "a")), sum(false, field("", "a")), true},
		{"an aggregate name in another case", dal.NewAggregate("sum", false, field("", "a")), sum(false, field("", "a")), true},
		{"aggregates of different functions", sum(false, field("", "a")), dal.NewAggregate(dal.MAX, false, field("", "a")), false},
		{"aggregates differing in DISTINCT", sum(true, field("", "a")), sum(false, field("", "a")), false},
		{"aggregates with different arguments", sum(false, plus(field("", "a"), typedTestConst(1))), sum(false, plus(field("", "a"), typedTestConst(2))), false},
		{"aggregates of different arity", sum(false, field("", "a")), sum(false, field("", "a"), field("", "b")), false},
		{"an aggregate against a field", sum(false, field("", "a")), field("", "a"), false},
		{"COUNT(*) against COUNT(*)", dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.NewAggregate(dal.COUNT, false, dal.Star()), true},
		{"COUNT(*) against COUNT(a)", dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.NewAggregate(dal.COUNT, false, field("", "a")), false},
		{"COUNT(a) against COUNT(*)", dal.NewAggregate(dal.COUNT, false, field("", "a")), dal.NewAggregate(dal.COUNT, false, dal.Star()), false},
		{"a node the compiler cannot render", dal.NewParam("p"), dal.NewParam("p"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := typedSameExpression(tc.a, tc.b); got != tc.want {
				t.Fatalf("typedSameExpression(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
