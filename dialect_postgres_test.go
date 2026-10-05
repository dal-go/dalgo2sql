package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// postgresTestModes lists the two identifier modes every dialect test runs in.
var postgresTestModes = []struct {
	name string
	mode postgresIdentifierMode
}{
	{"Exact", postgresExact},
	{"FoldLower", postgresFoldLower},
}

type postgresNamedBool bool
type postgresNamedInt int16
type postgresNamedUnsigned uint8
type postgresNamedFloat float32
type postgresNamedText string

// postgresTestValuer is a driver.Valuer the compiler accepts: its value is for
// the driver to resolve, so the dialect passes it through untyped.
type postgresTestValuer struct{}

func (postgresTestValuer) Value() (driver.Value, error) { return "from-valuer", nil }

func TestPostgresDialectPlaceholderStyle(t *testing.T) {
	for _, mode := range postgresTestModes {
		t.Run(mode.name, func(t *testing.T) {
			style := newPostgresDialect(mode.mode).placeholderStyle()
			if style.Prefix != "$" || style.IdentQuote != '"' {
				t.Fatalf("placeholderStyle() = %+v, want $n placeholders and double-quoted identifiers", style)
			}
		})
	}
}

func TestPostgresDialectQuoteIdent(t *testing.T) {
	type tc struct {
		name, in string
		exact    string // want in Exact mode, "" means refused
		folded   string // want in FoldLower mode, "" means refused
	}
	cases := []tc{
		{"plain lower case", "album", `"album"`, `"album"`},
		{"mixed case", "AlbumId", `"AlbumId"`, `"albumid"`},
		{"upper case", "ALBUM", `"ALBUM"`, `"album"`},
		{"space and punctuation", "Album Title?", `"Album Title?"`, `"album title?"`},
		{"embedded double quote is doubled", `A"B`, `"A""B"`, `"a""b"`},
		{"only quotes", `""`, `""""""`, `""""""`},
		{"non-ASCII capital", "Élan", `"Élan"`, `"élan"`},
		{"Greek capital sigma", "ΣΑΣ", `"ΣΑΣ"`, `"σασ"`},
		{"Kelvin sign folds to ASCII k", "Kelvin", "\"Kelvin\"", `"kelvin"`},
		{"exactly 63 bytes", strings.Repeat("N", 63), `"` + strings.Repeat("N", 63) + `"`, `"` + strings.Repeat("n", 63) + `"`},
		{"64 bytes is over the limit as written", strings.Repeat("N", 64), "", ""},
		// U+0130 (2 bytes) folds to "i" (1 byte): written, 64 bytes; folded, 32.
		{"64 bytes that fold to 32 are written short in FoldLower", strings.Repeat("İ", 32), "", `"` + strings.Repeat("i", 32) + `"`},
		// U+023A (2 bytes) folds to U+2C65 (3 bytes): 22 of them are 44 bytes written
		// and 66 folded, so the limit must be read on the folded name.
		{"44 bytes that fold to 66 are over the limit in FoldLower only", strings.Repeat("Ⱥ", 22), `"` + strings.Repeat("Ⱥ", 22) + `"`, ""},
		{"21 of them fold to 63 and fit", strings.Repeat("Ⱥ", 21), `"` + strings.Repeat("Ⱥ", 21) + `"`, `"` + strings.Repeat("ⱥ", 21) + `"`},
		{"empty", "", "", ""},
		{"NUL", "a\x00b", "", ""},
		{"invalid UTF-8", "a\xffb", "", ""},
	}
	for _, c := range cases {
		for i, mode := range postgresTestModes {
			want := []string{c.exact, c.folded}[i]
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				got, err := newPostgresDialect(mode.mode).quoteIdent(c.in)
				if want == "" {
					if err == nil {
						t.Fatalf("quoteIdent(%q) = %s, want an error", c.in, got)
					}
					if c.in != "" && strings.Contains(err.Error(), c.in) {
						t.Fatalf("error %q echoes the identifier", err)
					}
					return
				}
				if err != nil || got != want {
					t.Fatalf("quoteIdent(%q) = %s, %v; want %s", c.in, got, err, want)
				}
				if !isQuotedTypedIdentifier(got, '"') {
					t.Fatalf("quoteIdent(%q) = %s is not one well-formed quoted identifier", c.in, got)
				}
			})
		}
	}
	t.Run("the limit error of FoldLower says the name is read once folded", func(t *testing.T) {
		_, err := newPostgresDialect(postgresFoldLower).quoteIdent(strings.Repeat("Ⱥ", 22))
		if err == nil || !strings.Contains(err.Error(), "66 bytes") || !strings.Contains(err.Error(), "folded") {
			t.Fatalf("error = %v, want the folded length named", err)
		}
	})
}

// TestPostgresDialectBindTypesByTheGoType: one cast per Go type, never by what the
// value holds. The expected argument is what the driver receives.
func TestPostgresDialectBindTypesByTheGoType(t *testing.T) {
	stamp := time.Date(2026, time.October, 4, 12, 30, 15, 123456789, time.FixedZone("CEST", 2*3600))
	cases := []struct {
		name       string
		value      any
		wantMarker string
		wantArg    any
	}{
		{"int", 7, "?::bigint", int64(7)},
		{"negative int", -7, "?::bigint", int64(-7)},
		{"zero", 0, "?::bigint", int64(0)},
		{"int8", int8(-8), "?::bigint", int64(-8)},
		{"int16", int16(16), "?::bigint", int64(16)},
		{"int32", int32(32), "?::bigint", int64(32)},
		{"int64 at the top", int64(math.MaxInt64), "?::bigint", int64(math.MaxInt64)},
		{"int64 at the bottom", int64(math.MinInt64), "?::bigint", int64(math.MinInt64)},
		{"named int", postgresNamedInt(5), "?::bigint", int64(5)},
		// An unsigned value can exceed bigint, and the cast depends on the Go kind
		// only (the compiler treats uint8(5) and uint64(5) as one constant), so every
		// unsigned type binds as numeric from its decimal text.
		{"uint8", uint8(8), "?::numeric", "8"},
		{"uint", uint(9), "?::numeric", "9"},
		{"uint64 above bigint", uint64(math.MaxUint64), "?::numeric", "18446744073709551615"},
		{"named uint8", postgresNamedUnsigned(3), "?::numeric", "3"},
		{"float64 fraction", 0.5, "?::numeric", "0.5"},
		{"float64 whole", 5.0, "?::numeric", "5"},
		{"float64 negative", -2.25, "?::numeric", "-2.25"},
		{"float64 shortest text", 0.1, "?::numeric", "0.1"},
		{"float64 negative zero", math.Copysign(0, -1), "?::numeric", "-0"},
		{"float64 large", 1e21, "?::numeric", "1" + strings.Repeat("0", 21)},
		{"float64 tiny", 1e-7, "?::numeric", "0.0000001"},
		{"float32 keeps its own shortest text", float32(0.1), "?::numeric", "0.1"},
		{"named float32", postgresNamedFloat(1.5), "?::numeric", "1.5"},
		{"NaN", math.NaN(), "?::numeric", "NaN"},
		{"positive infinity", math.Inf(1), "?::numeric", "Infinity"},
		{"negative infinity", math.Inf(-1), "?::numeric", "-Infinity"},
		{"string stays untyped", "O'Brien'; DROP TABLE x; --", "?", "O'Brien'; DROP TABLE x; --"},
		{"empty string", "", "?", ""},
		{"digit string is still a string", "42", "?", "42"},
		{"named string", postgresNamedText("x"), "?", "x"},
		{"bool true", true, "?::boolean", true},
		{"bool false", false, "?::boolean", false},
		{"named bool", postgresNamedBool(true), "?::boolean", true},
		{"time keeps its instant and zone", stamp, "?::timestamptz", stamp},
		{"bytes", []byte{0, 1, 2}, "?::bytea", []byte{0, 1, 2}},
		{"empty bytes", []byte{}, "?::bytea", []byte{}},
		{"named bytes", typedTestNamedBytes("ab"), "?::bytea", []byte("ab")},
		{"nil is a NULL the server types from its place", nil, "?", nil},
		{"driver.Valuer is left to the driver", postgresTestValuer{}, "?", postgresTestValuer{}},
		{"sql.NullString is left to the driver", sql.NullString{String: "x", Valid: true}, "?", sql.NullString{String: "x", Valid: true}},
	}
	for _, mode := range postgresTestModes {
		dialect := newPostgresDialect(mode.mode)
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				marker, arg, err := dialect.bind(c.value)
				if err != nil || marker != c.wantMarker || !reflect.DeepEqual(arg, c.wantArg) {
					t.Fatalf("bind(%#v) = %q, %#v, %v; want %q, %#v", c.value, marker, arg, err, c.wantMarker, c.wantArg)
				}
				if got := strings.Count(marker, "?"); got != 1 {
					t.Fatalf("bind(%#v) wrote %d markers, the contract wants exactly one", c.value, got)
				}
			})
		}
	}
	t.Run("a type the compiler cannot bind is refused", func(t *testing.T) {
		_, _, err := newPostgresDialect(postgresExact).bind(struct{}{})
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("bind(struct{}) error = %v, want ErrNotSupported", err)
		}
	})
}

func TestPostgresDialectLimitOffset(t *testing.T) {
	dialect := newPostgresDialect(postgresExact)
	cases := []struct {
		name          string
		limit, offset int
		want          string
		wantArgs      []any
	}{
		{"neither", 0, 0, "", nil},
		{"limit", 10, 0, "LIMIT ?", []any{int64(10)}},
		{"offset", 0, 5, "OFFSET ?", []any{int64(5)}},
		{"both", 10, 5, "LIMIT ? OFFSET ?", []any{int64(10), int64(5)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, args := dialect.limitOffset(c.limit, c.offset)
			if got != c.want || !reflect.DeepEqual(args, c.wantArgs) {
				t.Fatalf("limitOffset(%d, %d) = %q, %#v; want %q, %#v", c.limit, c.offset, got, args, c.want, c.wantArgs)
			}
		})
	}
}

func TestPostgresDialectOrderItem(t *testing.T) {
	dialect := newPostgresDialect(postgresExact)
	cases := []struct {
		descending, notNull bool
		want                string
	}{
		{false, false, `"x" ASC NULLS FIRST`},
		{true, false, `"x" DESC NULLS LAST`},
		{false, true, `"x" ASC`},
		{true, true, `"x" DESC`},
	}
	for _, c := range cases {
		if got := dialect.orderItem(`"x"`, c.descending, c.notNull); got != c.want {
			t.Fatalf("orderItem(desc=%v, notNull=%v) = %s, want %s", c.descending, c.notNull, got, c.want)
		}
	}
}

func TestPostgresDialectArithmeticAggregatesAndEmptyIn(t *testing.T) {
	dialect := newPostgresDialect(postgresExact)
	if got, want := dialect.divide(`"a"`, `"b"`), `(("a")::double precision / NULLIF(("b")::double precision, 0))`; got != want {
		t.Fatalf("divide() = %s, want %s", got, want)
	}
	// +, - and * read both operands as double precision, so integer overflow cannot
	// differ from DALgo's generic engine (float64) and the SQLite path (REAL). The
	// fragment is self-delimiting and writes its operand once, verbatim.
	if got, want := dialect.arithmeticOperand(`"a"`), `(("a")::double precision)`; got != want {
		t.Fatalf("arithmeticOperand() = %s, want %s", got, want)
	}
	if got, want := dialect.arithmeticOperand(`("a" + ?::bigint)`), `((("a" + ?::bigint))::double precision)`; got != want {
		t.Fatalf("arithmeticOperand() = %s, want %s", got, want)
	}
	for function, want := range map[string]string{
		dal.SUM:     `((SUM("x"))::double precision)`,
		dal.AVERAGE: `((AVG("x"))::double precision)`,
		dal.COUNT:   `COUNT("x")`,
		dal.MIN:     `MIN("x")`,
		dal.MAX:     `MAX("x")`,
	} {
		if got := dialect.aggregateResult(function, function+`("x")`); got != want {
			t.Fatalf("aggregateResult(%s) = %s, want %s", function, got, want)
		}
	}
	if dialect.emptyIn(false) != "FALSE" || dialect.emptyIn(true) != "TRUE" {
		t.Fatalf("emptyIn() = %s / %s, want FALSE / TRUE", dialect.emptyIn(false), dialect.emptyIn(true))
	}
}

func TestPostgresDialectCapabilities(t *testing.T) {
	// FIRST and LAST need an input order no SQL engine guarantees, and the compiler
	// does not render a group-key order or promise a stable row order: want leaves
	// those flags false, so the comparison below pins all of them off.
	want := dal.QueryCapabilities{
		GroupBy: true, Having: true, OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count: true, CountDistinct: true,
			Sum: true, SumDistinct: true,
			Avg: true, AvgDistinct: true,
			Min: true, Max: true,
		},
	}
	for _, mode := range postgresTestModes {
		if got := newPostgresDialect(mode.mode).capabilities(); got != want {
			t.Fatalf("%s: capabilities() = %+v, want %+v", mode.name, got, want)
		}
	}
}

func TestPostgresDialectWindowIsReserved(t *testing.T) {
	_, err := newPostgresDialect(postgresExact).window("row_number", nil, nil, nil)
	if !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("window() error = %v, want ErrNotSupported", err)
	}
}

// TestPostgresDialectPassesTheContractProofs runs the value-independence proofs
// every typedDialect must pass (typed_sql_property_test.go) against the real
// dialect in both identifier modes.
func TestPostgresDialectPassesTheContractProofs(t *testing.T) {
	for _, mode := range postgresTestModes {
		t.Run(mode.name, func(t *testing.T) {
			dialect := newPostgresDialect(mode.mode)
			assertTypedTextIgnoresValues(t, dialect)
			assertTypedTextHasTwoShapesForTwoConstants(t, dialect)
		})
	}
}

// TestPostgresDialectFoldIsTheFoldQuoteIdentApplies pins the safety of the facts
// check: typedCatalogFacts.Fold must be the very case rule quoteIdent writes, over
// mixed-case and non-ASCII names, in both identifier modes. The compiler matches a
// catalog column when its own name equals Fold(query's name), and the server reads
// the text quoteIdent wrote; if the two differed for one name, a field could pass
// the facts check as one column and be written as another.
func TestPostgresDialectFoldIsTheFoldQuoteIdentApplies(t *testing.T) {
	names := []string{
		"album", "Album", "ALBUM", "AlbumId", "Album Title", "ÉLAN", "élan", "Straße", "STRASSE",
		"ΣΑΣ", "σας", "İstanbul", "Kelvin", "Ⱥa", "ǅ", "Ⅷ", "a\"B", "?Q?", "日本語", "Ünïcödé",
	}
	for _, mode := range postgresTestModes {
		t.Run(mode.name, func(t *testing.T) {
			dialect := newPostgresDialect(mode.mode)
			facts := postgresTestFolding(t, dialect)
			for _, name := range names {
				quoted, err := dialect.quoteIdent(name)
				if err != nil {
					t.Fatalf("quoteIdent(%q) error = %v", name, err)
				}
				written := unquoteTypedIdentifier(t, quoted)
				if got := facts.fold(name); got != written {
					t.Fatalf("Fold(%q) = %q but quoteIdent wrote %q", name, got, written)
				}
				// A catalog column named exactly as written is matched, whatever spelling the
				// query used; a catalog column in any other form is not.
				source := typedSourceName{Name: facts.fold("T")}
				facts.Sources = map[typedSourceName]typedSourceFacts{source: {Columns: []typedColumnFact{{Name: written}, {Name: written + "x"}}}}
				if column, ok := facts.column(typedSourceName{Name: "T"}, name); !ok || column.Name != written {
					t.Fatalf("column(%q) = %v, %v; want the catalog column %q", name, column, ok, written)
				}
				facts.Sources = map[typedSourceName]typedSourceFacts{source: {Columns: []typedColumnFact{{Name: written + "x"}}}}
				if _, ok := facts.column(typedSourceName{Name: "T"}, name); ok {
					t.Fatalf("column(%q) matched a catalog column that is not the written name", name)
				}
			}
		})
	}
	t.Run("Exact applies no fold and FoldLower is strings.ToLower", func(t *testing.T) {
		exact, lower := postgresTestFolding(t, newPostgresDialect(postgresExact)), postgresTestFolding(t, newPostgresDialect(postgresFoldLower))
		for _, name := range names {
			if exact.fold(name) != name || lower.fold(name) != strings.ToLower(name) {
				t.Fatalf("Fold(%q): Exact %q, FoldLower %q", name, exact.fold(name), lower.fold(name))
			}
		}
	})
}

// unquoteTypedIdentifier reverses quoteTypedIdentifier: it strips the outer
// quotes and un-doubles the inner ones.
func unquoteTypedIdentifier(t *testing.T, quoted string) string {
	t.Helper()
	if !isQuotedTypedIdentifier(quoted, '"') {
		t.Fatalf("%s is not a quoted identifier", quoted)
	}
	return strings.ReplaceAll(quoted[1:len(quoted)-1], `""`, `"`)
}

// postgresTestFolding returns the facts the dialect hands the compiler when the
// query reads no source: they carry the dialect's Fold and need no database.
func postgresTestFolding(t *testing.T, dialect postgresDialect) typedCatalogFacts {
	t.Helper()
	facts, err := dialect.catalogFacts(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("catalogFacts() with no sources error = %v", err)
	}
	return facts
}
