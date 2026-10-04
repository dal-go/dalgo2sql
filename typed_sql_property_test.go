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

func (d typedDraw) wantArgs() []any {
	return []any{
		d.i1,
		d.i2, d.f1, d.s1, d.flag, d.stamp, d.blob, d.i3, d.s2, d.f2, d.s3,
		d.i4,
		d.limit, d.offset,
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
				if want := draw.wantArgs(); !reflect.DeepEqual(args, want) {
					t.Fatalf("draw %d: args = %#v, want %#v", i, args, want)
				}
				for _, value := range []string{draw.s1, draw.s2, draw.s3} {
					if strings.Contains(text, value) {
						t.Fatalf("draw %d: value %q reached the SQL text %q", i, value, text)
					}
				}
				if style.Prefix != "" {
					for n := 1; n <= len(args); n++ {
						if !strings.Contains(text, fmt.Sprintf("$%d", n)) {
							t.Fatalf("draw %d: placeholder $%d is missing from %q", i, n, text)
						}
					}
					if strings.Contains(text, fmt.Sprintf("$%d", len(args)+1)) {
						t.Fatalf("draw %d: more placeholders than the %d arguments in %q", i, len(args), text)
					}
				}
			}
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
		wantArgs: draw.wantArgs(),
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
