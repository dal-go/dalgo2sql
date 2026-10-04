package dalgo2sql

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestCheckTypedIdentifier(t *testing.T) {
	accepted := map[string]string{
		"plain":                "Album",
		"mixed case and space": "Album Title",
		"punctuation":          `we?ird"name`,
		"exactly the limit":    strings.Repeat("n", 63),
		"multi-byte":           strings.Repeat("é", 31), // 62 bytes
	}
	for name, identifier := range accepted {
		t.Run("accepts "+name, func(t *testing.T) {
			if err := checkTypedIdentifier(identifier, 63); err != nil {
				t.Fatalf("checkTypedIdentifier(%q) = %v", identifier, err)
			}
		})
	}
	refused := map[string]struct{ identifier, fragment string }{
		"empty":                       {"", "empty"},
		"NUL":                         {"secret\x00name", "NUL"},
		"over the limit":              {strings.Repeat("n", 64), "limit"},
		"multi-byte over the limit":   {strings.Repeat("é", 32), "limit"},
		"invalid UTF-8":               {"abc\xff", "UTF-8"},
		"truncated UTF-8 at the edge": {"abc\xc3", "UTF-8"},
	}
	for name, tc := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			err := checkTypedIdentifier(tc.identifier, 63)
			if err == nil || !strings.Contains(err.Error(), tc.fragment) {
				t.Fatalf("checkTypedIdentifier(%q) = %v, want an error mentioning %q", tc.identifier, err, tc.fragment)
			}
			if tc.identifier != "" && strings.Contains(err.Error(), tc.identifier) {
				t.Fatalf("error %q must not echo the identifier", err)
			}
		})
	}
}

func TestQuoteTypedIdentifier(t *testing.T) {
	cases := []struct {
		name, identifier string
		quote            byte
		want             string
	}{
		{"plain", "Album", '"', `"Album"`},
		{"embedded double quote is doubled", `a"b`, '"', `"a""b"`},
		{"only quotes", `""`, '"', `""""""`},
		{"placeholder character is untouched", `a?b`, '"', `"a?b"`},
		{"backtick style", "a`b", '`', "`a``b`"},
		{"other quote character is untouched", `a"b`, '`', "`a\"b`"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := quoteTypedIdentifier(tc.identifier, tc.quote); got != tc.want {
				t.Fatalf("quoteTypedIdentifier(%q) = %s, want %s", tc.identifier, got, tc.want)
			}
		})
	}
}

type typedTestNamedString string
type typedTestNamedBytes []byte

func TestTypedKindOf(t *testing.T) {
	var nilPointer *int
	accepted := []struct {
		name  string
		value any
		want  typedValueKind
	}{
		{"nil", nil, typedValueNull},
		{"driver.Valuer", sql.NullString{String: "x", Valid: true}, typedValueValuer},
		{"time", time.Now(), typedValueTime},
		{"bool", true, typedValueBool},
		{"int", 1, typedValueInteger},
		{"int8", int8(1), typedValueInteger},
		{"int16", int16(1), typedValueInteger},
		{"int32", int32(1), typedValueInteger},
		{"int64", int64(1), typedValueInteger},
		{"uint", uint(1), typedValueUnsigned},
		{"uint8", uint8(1), typedValueUnsigned},
		{"uint16", uint16(1), typedValueUnsigned},
		{"uint32", uint32(1), typedValueUnsigned},
		{"uint64", uint64(1), typedValueUnsigned},
		{"float32", float32(1), typedValueFloat},
		{"float64", 1.5, typedValueFloat},
		{"string", "x", typedValueText},
		{"named string", typedTestNamedString("x"), typedValueText},
		{"bytes", []byte("x"), typedValueBytes},
		{"named bytes", typedTestNamedBytes("x"), typedValueBytes},
	}
	for _, tc := range accepted {
		t.Run("accepts "+tc.name, func(t *testing.T) {
			got, err := typedKindOf(tc.value)
			if err != nil || got != tc.want {
				t.Fatalf("typedKindOf(%T) = %v, %v; want %v", tc.value, got, err, tc.want)
			}
		})
	}
	refused := []struct {
		name  string
		value any
	}{
		{"struct", struct{}{}},
		{"map", map[string]int{}},
		{"int slice", []int{1}},
		{"pointer", nilPointer},
		{"channel", make(chan int)},
		{"function", func() {}},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			if _, err := typedKindOf(tc.value); err == nil {
				t.Fatalf("typedKindOf(%T) must fail", tc.value)
			}
		})
	}
}

func TestTypedCatalogFactsLookups(t *testing.T) {
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Schema: "public", Name: "album"}: {Columns: []typedColumnFact{{Name: "albumid", NotNull: true}, {Name: "title"}}},
		{Name: "artist"}:                  {Columns: []typedColumnFact{{Name: "name", NonDeterministicCollation: true}}},
	}}
	t.Run("exact spelling", func(t *testing.T) {
		source, ok := facts.source(typedSourceName{Schema: "public", Name: "album"})
		if !ok || len(source.Columns) != 2 {
			t.Fatalf("source() = %v, %v", source, ok)
		}
		if _, ok := facts.source(typedSourceName{Schema: "public", Name: "Album"}); ok {
			t.Fatal("exact lookup must be case-sensitive")
		}
		if column, ok := facts.column(typedSourceName{Name: "artist"}, "name"); !ok || column.Name != "name" {
			t.Fatalf("column() = %v, %v", column, ok)
		}
		if _, ok := facts.column(typedSourceName{Name: "artist"}, "Name"); ok {
			t.Fatal("exact lookup must be case-sensitive")
		}
		if _, ok := facts.column(typedSourceName{Name: "artist"}, "missing"); ok {
			t.Fatal("a missing column must not be found")
		}
		if _, ok := facts.column(typedSourceName{Name: "missing"}, "name"); ok {
			t.Fatal("a missing source must not be found")
		}
	})
	t.Run("folded spelling", func(t *testing.T) {
		folded := facts
		folded.Fold = strings.ToLower
		if _, ok := folded.source(typedSourceName{Schema: "PUBLIC", Name: "Album"}); !ok {
			t.Fatal("a folding lookup must find the source")
		}
		if column, ok := folded.column(typedSourceName{Schema: "Public", Name: "ALBUM"}, "AlbumId"); !ok || !column.NotNull {
			t.Fatalf("column() = %v, %v", column, ok)
		}
	})
	// A folding dialect writes the folded name, so a column matches only when the
	// catalog's own name is that name; the first column that folds to the same key
	// is not enough.
	t.Run("folded lookup matches the catalog's own name only", func(t *testing.T) {
		album := typedSourceName{Name: "album"}
		colliding := typedCatalogFacts{Fold: strings.ToLower, Sources: map[typedSourceName]typedSourceFacts{
			album: {Columns: []typedColumnFact{{Name: "Title", NotNull: true}, {Name: "title"}}},
		}}
		if column, ok := colliding.column(album, "TITLE"); !ok || column.NotNull || column.Name != "title" {
			t.Fatalf("column(TITLE) = %v, %v; want the column named title, not the first one that folds to it", column, ok)
		}
		mixedOnly := typedCatalogFacts{Fold: strings.ToLower, Sources: map[typedSourceName]typedSourceFacts{
			album: {Columns: []typedColumnFact{{Name: "Title"}}},
		}}
		for _, spelling := range []string{"Title", "title", "TITLE"} {
			if column, ok := mixedOnly.column(album, spelling); ok {
				t.Fatalf("column(%s) = %v; a catalog name the dialect cannot write must never match", spelling, column)
			}
		}
		if !colliding.addressable("title") || colliding.addressable("Title") {
			t.Fatal("addressable() must hold exactly for a name that is its own folded form")
		}
		var exact typedCatalogFacts
		if !exact.addressable("Title") {
			t.Fatal("without a fold every name is addressable")
		}
	})
	t.Run("zero value has no facts", func(t *testing.T) {
		var none typedCatalogFacts
		if _, ok := none.source(typedSourceName{Name: "album"}); ok {
			t.Fatal("zero facts must know nothing")
		}
		if _, ok := none.column(typedSourceName{Name: "album"}, "title"); ok {
			t.Fatal("zero facts must know nothing")
		}
	})
}

func TestTypedJoinKeysComparable(t *testing.T) {
	col := func(dataType string, category typedTypeCategory) typedColumnFact {
		return typedColumnFact{DataType: dataType, Category: category}
	}
	cases := []struct {
		name string
		a, b typedColumnFact
		want bool
	}{
		{"same scalar category", col("int4", typedTypeNumber), col("numeric", typedTypeNumber), true},
		{"text and text", col("text", typedTypeText), col("varchar", typedTypeText), true},
		{"different categories", col("int4", typedTypeNumber), col("text", typedTypeText), false},
		{"same other type", col("uuid", typedTypeOther), col("uuid", typedTypeOther), true},
		{"different other types", col("uuid", typedTypeOther), col("json", typedTypeOther), false},
		{"same unknown type name", col("mystery", typedTypeUnknown), col("mystery", typedTypeUnknown), true},
		{"unknown without names", col("", typedTypeUnknown), col("", typedTypeUnknown), false},
		{"unknown against known", col("mystery", typedTypeUnknown), col("text", typedTypeText), false},
		{"same name different category", col("x", typedTypeText), col("x", typedTypeNumber), true},
		{"boolean and boolean", col("bool", typedTypeBoolean), col("boolean", typedTypeBoolean), true},
		{"time and time", col("timestamptz", typedTypeTime), col("date", typedTypeTime), true},
		{"binary and binary", col("bytea", typedTypeBinary), col("blob", typedTypeBinary), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := typedJoinKeysComparable(tc.a, tc.b); got != tc.want {
				t.Fatalf("typedJoinKeysComparable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsQuotedTypedIdentifier(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		quote byte
		want  bool
	}{
		{"plain", `"Album"`, '"', true},
		{"doubled inner quotes", `"a""b"`, '"', true},
		{"only a doubled quote", `""""`, '"', true},
		{"placeholder character inside", `"a?b"`, '"', true},
		{"backtick", "`a``b`", '`', true},
		{"empty identifier", `""`, '"', false},
		{"empty text", ``, '"', false},
		{"lone quote", `"`, '"', false},
		{"not wrapped", `Album`, '"', false},
		{"no closing quote", `"Album`, '"', false},
		{"no opening quote", `Album"`, '"', false},
		{"undoubled inner quote", `"a"b"`, '"', false},
		{"odd run of inner quotes", `"a"""b"`, '"', false},
		{"escaped quote swallows the closing quote", `"a""`, '"', false},
		{"escaped quote at the end is fine", `"a"""`, '"', true},
		{"NUL inside", "\"a\x00b\"", '"', false},
		{"wrong quote byte", `"a"`, '`', false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQuotedTypedIdentifier(tc.text, tc.quote); got != tc.want {
				t.Fatalf("isQuotedTypedIdentifier(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
	// Whatever the name, quoteTypedIdentifier produces a well-formed identifier.
	for _, name := range []string{`a`, `"`, `""`, `a"b"c`, `?`, "multi\nline", `é`} {
		if quoted := quoteTypedIdentifier(name, '"'); !isQuotedTypedIdentifier(quoted, '"') {
			t.Fatalf("quoteTypedIdentifier(%q) = %s is not well formed", name, quoted)
		}
	}
}
