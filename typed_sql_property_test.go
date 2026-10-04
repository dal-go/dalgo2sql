package dalgo2sql

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// typedDraw is one random assignment of values to every constant position of
// typedPropertyQuery. Types are fixed per position because a dialect may type a
// placeholder by the Go type of its value; only the values themselves vary.
type typedDraw struct {
	i1, i2, i3, i4 int
	f1, f2         float64
	s1, s2, s3     string
	flag           bool
	stamp          time.Time
	blob           []byte
	limit, offset  int
}

// A marker that cannot occur in the compiler's own text, plus characters that
// would matter if a value were ever spliced into SQL.
const typedHostileAlphabet = `'"?$;-\()` + "\n"

func drawTypedValues(r *rand.Rand) typedDraw {
	text := func() string {
		var b strings.Builder
		b.WriteString("§value-")
		for range r.IntN(12) {
			b.WriteByte(typedHostileAlphabet[r.IntN(len(typedHostileAlphabet))])
			b.WriteByte(byte('a' + r.IntN(26)))
		}
		return b.String()
	}
	blob := make([]byte, r.IntN(8))
	for i := range blob {
		blob[i] = byte(r.IntN(256))
	}
	return typedDraw{
		i1: r.IntN(1 << 30), i2: -r.IntN(1 << 30), i3: r.IntN(5), i4: r.IntN(1 << 20),
		f1: r.NormFloat64() * 1e6, f2: r.Float64(),
		s1: text(), s2: text(), s3: text(),
		flag:   r.IntN(2) == 0,
		stamp:  time.Unix(r.Int64N(4e9), r.Int64N(1e9)).UTC(),
		blob:   blob,
		limit:  1 + r.IntN(1000),
		offset: 1 + r.IntN(1000),
	}
}

// typedPropertyQuery touches every construct that carries a constant: select-list
// arithmetic, comparisons of every operator class, IN and NOT IN, a grouped
// expression referenced by ordinal, HAVING through an alias, ORDER BY through
// an alias, LIMIT and OFFSET.
func typedPropertyQuery(d typedDraw) dal.StructuredQuery {
	bucket := dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(d.i1))
	return typedTestFrom("Invoice", "").NewQuery().
		Where(dal.NewGroupCondition(dal.And,
			typedTestEq(typedTestField("a"), d.i2),
			dal.NewComparison(typedTestField("b"), dal.GreaterThen, typedTestConst(d.f1)),
			typedTestEq(typedTestField("c"), d.s1),
			typedTestEq(typedTestField("d"), d.flag),
			dal.NewComparison(typedTestField("e"), dal.LessOrEqual, typedTestConst(d.stamp)),
			typedTestEq(typedTestField("f"), d.blob),
			dal.NewComparison(typedTestField("g"), dal.In, dal.NewArray([]any{d.i3, d.s2, d.f2})),
			dal.NewComparison(typedTestField("h"), dal.NotIn, dal.NewArray([]string{d.s3})),
		)).
		GroupBy(bucket).
		Having(dal.NewComparison(typedTestField("total"), dal.GreaterThen, typedTestConst(d.i4))).
		OrderBy(dal.Ascending(typedTestField("bucket"))).
		Limit(d.limit).Offset(d.offset).
		SelectColumns(typedTestColumn(bucket, "bucket"), dal.SumAs(typedTestField("Total"), "total"))
}

// typedBoundArgument is the argument dialect sends beside the text for value.
func typedBoundArgument(t *testing.T, dialect typedDialect, value any) any {
	t.Helper()
	_, arg, err := dialect.bind(value)
	if err != nil {
		t.Fatalf("dialect.bind(%#v) error = %v", value, err)
	}
	return arg
}

// wantArgs lists the arguments typedPropertyQuery binds, in text order, as
// dialect spells them.
func (d typedDraw) wantArgs(t *testing.T, dialect typedDialect) []any {
	t.Helper()
	bound := func(value any) any { return typedBoundArgument(t, dialect, value) }
	_, page := dialect.limitOffset(d.limit, d.offset)
	return append([]any{
		bound(d.i1),
		bound(d.i2), bound(d.f1), bound(d.s1), bound(d.flag), bound(d.stamp), bound(d.blob), bound(d.i3), bound(d.s2), bound(d.f2), bound(d.s3),
		bound(d.i4),
	}, page...)
}

// assertTypedTextIgnoresValues is the value-independence proof every typedDialect
// must pass: 300 seeded draws of every constant, and the SQL text may not change
// by a byte. It takes the dialect, so the PostgreSQL dialect runs the same draws
// as the fake one; a dialect that typed a placeholder by the content of a value
// (as opposed to its Go type) fails here.
func assertTypedTextIgnoresValues(t *testing.T, dialect typedDialect) {
	t.Helper()
	style := dialect.placeholderStyle()
	r := rand.New(rand.NewPCG(20261004, 7))
	var reference string
	for i := range 300 {
		draw := drawTypedValues(r)
		text, args, err := compileTypedSQL(typedPropertyQuery(draw), dialect, typedCatalogFacts{})
		if err != nil {
			t.Fatalf("draw %d: compileTypedSQL() error = %v", i, err)
		}
		if i == 0 {
			reference = text
		} else if text != reference {
			t.Fatalf("draw %d changed the SQL text\n got: %s\nwant: %s", i, text, reference)
		}
		if want := draw.wantArgs(t, dialect); !reflect.DeepEqual(args, want) {
			t.Fatalf("draw %d: args = %#v, want %#v", i, args, want)
		}
		for _, value := range []string{draw.s1, draw.s2, draw.s3} {
			if strings.Contains(text, value) {
				t.Fatalf("draw %d: value %q reached the SQL text %q", i, value, text)
			}
		}
		if style.Prefix != "" {
			for n := 1; n <= len(args); n++ {
				if !strings.Contains(text, fmt.Sprintf("%s%d", style.Prefix, n)) {
					t.Fatalf("draw %d: placeholder %s%d is missing from %q", i, style.Prefix, n, text)
				}
			}
			if strings.Contains(text, fmt.Sprintf("%s%d", style.Prefix, len(args)+1)) {
				t.Fatalf("draw %d: more placeholders than the %d arguments in %q", i, len(args), text)
			}
		}
	}
}

// assertTypedTextHasTwoShapesForTwoConstants covers the one structural choice
// that depends on two values at once: a selected expression and an ORDER BY
// expression that differ only in a constant. The text is exactly one of two
// fixed strings, "ORDER BY 2" when the constants are equal and the inline bound
// expression when they are not. Each constant is drawn independently, from a
// range small enough to meet equality often.
func assertTypedTextHasTwoShapesForTwoConstants(t *testing.T, dialect typedDialect) {
	t.Helper()
	compile := func(selected, ordered int) (string, []any) {
		q := typedTestFrom("Album", "").NewQuery().
			OrderBy(dal.Ascending(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(ordered)))).
			SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(selected)), "shifted"))
		text, args, err := compileTypedSQL(q, dialect, typedCatalogFacts{})
		if err != nil {
			t.Fatalf("compileTypedSQL(%d, %d) error = %v", selected, ordered, err)
		}
		return text, args
	}
	sameText, _ := compile(1, 1)
	differentText, _ := compile(1, 2)
	if sameText == differentText {
		t.Fatalf("equal and different constants compile to the same text %q: the property would prove nothing", sameText)
	}
	r := rand.New(rand.NewPCG(20261004, 11))
	equal, different := 0, 0
	for i := range 300 {
		selected, ordered := r.IntN(4), r.IntN(4)
		text, args := compile(selected, ordered)
		want, wantArgs := differentText, []any{typedBoundArgument(t, dialect, selected), typedBoundArgument(t, dialect, ordered)}
		if selected == ordered {
			want, wantArgs = sameText, wantArgs[:1]
			equal++
		} else {
			different++
		}
		if text != want {
			t.Fatalf("draw %d (selected %d, ordered %d): text = %q, want one of the two fixed texts, here %q", i, selected, ordered, text, want)
		}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("draw %d (selected %d, ordered %d): args = %#v, want %#v", i, selected, ordered, args, wantArgs)
		}
	}
	if equal == 0 || different == 0 {
		t.Fatalf("the draws met equal constants %d times and different ones %d times; both must occur", equal, different)
	}
}

func TestCompileTypedSQLTextDoesNotDependOnValues(t *testing.T) {
	styles := map[string]typedPlaceholderStyle{
		"numbered": {Prefix: "$", IdentQuote: '"'},
		"question": {IdentQuote: '"'},
	}
	for name, style := range styles {
		t.Run(name, func(t *testing.T) {
			dialect := newFakeTypedDialect()
			dialect.style = style
			assertTypedTextIgnoresValues(t, dialect)
		})
	}
}

func TestCompileTypedSQLTwoConstantsChooseBetweenTwoFixedTexts(t *testing.T) {
	styles := map[string]typedPlaceholderStyle{
		"numbered": {Prefix: "$", IdentQuote: '"'},
		"question": {IdentQuote: '"'},
	}
	for name, style := range styles {
		t.Run(name, func(t *testing.T) {
			dialect := newFakeTypedDialect()
			dialect.style = style
			assertTypedTextHasTwoShapesForTwoConstants(t, dialect)
		})
	}
}

func TestCompileTypedSQLPropertyQueryShape(t *testing.T) {
	// One concrete rendering pins what the property test is quantifying over.
	draw := typedDraw{
		i1: 1, i2: 2, i3: 3, i4: 4, f1: 1.5, f2: 2.5, s1: "s1", s2: "s2", s3: "s3",
		flag: true, stamp: time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC), blob: []byte{9}, limit: 10, offset: 20,
	}
	typedGolden{
		query: typedPropertyQuery(draw),
		wantSQL: `SELECT ("Total" + $1::bigint) AS "bucket", CAST(SUM("Total") AS double precision) AS "total" FROM "Invoice" ` +
			`WHERE ("a" = $2::bigint AND "b" > $3::numeric AND "c" = $4 AND "d" = $5::boolean AND "e" <= $6::timestamptz AND "f" = $7::bytea ` +
			`AND "g" IN ($8::bigint, $9, $10::numeric) AND "h" NOT IN ($11)) GROUP BY 1 ` +
			`HAVING CAST(SUM("Total") AS double precision) > $12::bigint ORDER BY 1 ASC NULLS FIRST LIMIT $13 OFFSET $14`,
		wantArgs: draw.wantArgs(t, newFakeTypedDialect()),
	}.run(t)
}

func TestCompileTypedSQLNilChangesStructureNotValues(t *testing.T) {
	// `== null` is a different statement (IS NULL) rather than a bound NULL;
	// that is structure chosen by the caller, not a value leaking into text.
	withNil := typedTestFrom("Album", "").NewQuery().Where(typedTestEq(typedTestField("a"), nil)).SelectColumns()
	withValue := typedTestFrom("Album", "").NewQuery().Where(typedTestEq(typedTestField("a"), 1)).SelectColumns()
	nilText, nilArgs, err := compileTypedSQL(withNil, newFakeTypedDialect(), typedCatalogFacts{})
	if err != nil || nilText != `SELECT * FROM "Album" WHERE "a" IS NULL` || len(nilArgs) != 0 {
		t.Fatalf("== nil compiled to %q %v, %v", nilText, nilArgs, err)
	}
	valueText, _, err := compileTypedSQL(withValue, newFakeTypedDialect(), typedCatalogFacts{})
	if err != nil || valueText == nilText {
		t.Fatalf("== 1 compiled to %q, %v", valueText, err)
	}
}

func TestCompileTypedSQLListLengthAndZeroLimitChangeStructureNotValues(t *testing.T) {
	// The other two documented structural dependences: the number of markers an
	// IN list writes follows its length, and a zero limit or offset leaves its
	// clause out. Neither reads a value's content.
	inList := func(values ...int) string {
		q := typedTestFrom("Album", "").NewQuery().Where(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray(values))).SelectColumns()
		text, _, err := compileTypedSQL(q, newFakeTypedDialect(), typedCatalogFacts{})
		if err != nil {
			t.Fatal(err)
		}
		return text
	}
	if a, b := inList(1, 2, 3), inList(7, 8, 9); a != b {
		t.Fatalf("two lists of three differ: %q and %q", a, b)
	}
	if a, b := inList(1, 2, 3), inList(1, 2); a == b {
		t.Fatalf("lists of different length compiled to the same text %q", a)
	}
	page := func(limit, offset int) string {
		text, _, err := compileTypedSQL(typedTestFrom("Album", "").NewQuery().Limit(limit).Offset(offset).SelectColumns(), newFakeTypedDialect(), typedCatalogFacts{})
		if err != nil {
			t.Fatal(err)
		}
		return text
	}
	if a, b := page(5, 9), page(77, 1); a != b {
		t.Fatalf("two non-zero pages differ: %q and %q", a, b)
	}
	if a, b := page(5, 9), page(0, 9); a == b {
		t.Fatalf("a zero limit compiled to the same text %q", a)
	}
}
