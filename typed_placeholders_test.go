package dalgo2sql

import (
	"strings"
	"testing"
)

func TestNumberTypedPlaceholders(t *testing.T) {
	dollar := typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'}
	question := typedPlaceholderStyle{IdentQuote: '"'}
	cases := []struct {
		name  string
		text  string
		style typedPlaceholderStyle
		args  int
		want  string
	}{
		{"no markers", `SELECT * FROM "t"`, dollar, 0, `SELECT * FROM "t"`},
		{"numbered in order", `a = ? AND b = ? AND c = ?`, dollar, 3, `a = $1 AND b = $2 AND c = $3`},
		{"typed markers keep their suffix", `a = ?::bigint, b = ?`, dollar, 2, `a = $1::bigint, b = $2`},
		{"markers inside a quoted identifier are skipped", `"a?b" = ? AND "?" = ?`, dollar, 2, `"a?b" = $1 AND "?" = $2`},
		{"a doubled quote stays inside the identifier", `"a""?""b" = ?`, dollar, 1, `"a""?""b" = $1`},
		{"identifier made only of quotes", `"""" = ? AND ? = 1`, dollar, 2, `"""" = $1 AND $2 = 1`},
		{"two digit numbers", strings.Repeat("?,", 12), dollar, 12, "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,"},
		{"question style keeps the text", `"a?b" = ? AND c = ?`, question, 2, `"a?b" = ? AND c = ?`},
		{"other prefix", `a = ?`, typedPlaceholderStyle{Prefix: "@p", IdentQuote: '"'}, 1, `a = @p1`},
		{"backtick identifiers", "`a?` = ?", typedPlaceholderStyle{Prefix: "$", IdentQuote: '`'}, 1, "`a?` = $1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := numberTypedPlaceholders(tc.text, tc.style, tc.args)
			if err != nil || got != tc.want {
				t.Fatalf("numberTypedPlaceholders() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestNumberTypedPlaceholdersRejectsInconsistentText(t *testing.T) {
	dollar := typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'}
	cases := []struct {
		name     string
		text     string
		args     int
		fragment string
	}{
		{"more markers than arguments", `a = ? AND b = ?`, 1, "2 placeholders for 1 arguments"},
		{"fewer markers than arguments", `a = ?`, 2, "1 placeholders for 2 arguments"},
		{"an identifier that never closes", `"a = ?`, 1, "quoted identifier"},
		{"an identifier that never closes hides its marker", `x = ? AND "a = ?`, 2, "quoted identifier"},
		{"a string literal", `a = 'x' AND b = ?`, 1, "string literal"},
		{"a string literal hiding a marker", `a = '?' AND b = ?`, 2, "string literal"},
		{"a line comment", `a = ? -- x`, 1, "comment"},
		{"a block comment", `a = ? /* x */`, 1, "comment"},
		{"a block comment that opens the text", `/* x */ a = ?`, 1, "comment"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := numberTypedPlaceholders(tc.text, dollar, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.fragment) || got != "" {
				t.Fatalf("numberTypedPlaceholders() = %q, %v; want an error mentioning %q", got, err, tc.fragment)
			}
		})
	}
}

func TestNumberTypedPlaceholdersAcceptsLookalikesInsideIdentifiersAndOperators(t *testing.T) {
	dollar := typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'}
	cases := []struct {
		name, text string
		args       int
		want       string
	}{
		{"quote, dashes and comment openers inside an identifier", `"it's" = ? AND "a--b" = ? AND "c/*d" = ?`, 3, `"it's" = $1 AND "a--b" = $2 AND "c/*d" = $3`},
		{"a lone minus and a lone slash", `(a - ?) / (b / ?)`, 2, `(a - $1) / (b / $2)`},
		{"a star after a parenthesis", `COUNT(*) - ?`, 1, `COUNT(*) - $1`},
		{"a dash at the very end", `a -`, 0, `a -`},
		{"a slash at the very end", `a /`, 0, `a /`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := numberTypedPlaceholders(tc.text, dollar, tc.args)
			if err != nil || got != tc.want {
				t.Fatalf("numberTypedPlaceholders() = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestCountTypedMarkers(t *testing.T) {
	cases := []struct {
		name, text string
		want       int
	}{
		{"none", `"a" + "b"`, 0},
		{"two", `? + ?::bigint`, 2},
		{"inside an identifier is not a marker", `"a?b" = ?`, 1},
		{"a doubled quote keeps the identifier open", `"a""?" = ? AND ? = 1`, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := countTypedMarkers(tc.text, '"'); got != tc.want {
				t.Fatalf("countTypedMarkers(%q) = %d, want %d", tc.text, got, tc.want)
			}
		})
	}
}

func TestCheckTypedFragment(t *testing.T) {
	left, right := `("a" + ?::bigint)`, `("b" - ?::bigint)`
	valueFree, other := `"a"`, `"b"`
	cases := []struct {
		name      string
		fragment  string
		own       int
		operands  []string
		wantError string
	}{
		{"operands in order", "(" + left + " / " + right + ")", 0, []string{left, right}, ""},
		{"operands wrapped in order", "NULLIF(" + left + ", 0) / CAST(" + right + " AS double precision)", 0, []string{left, right}, ""},
		{"operands in the wrong order", "(" + right + " / " + left + ")", 0, []string{left, right}, "verbatim and in argument order"},
		{"an operand renders twice", "(" + left + " / " + left + ")", 0, []string{left}, "2 placeholders where 1 belong"},
		{"an operand is dropped", "(" + left + ")", 0, []string{left, right}, "1 placeholders where 2 belong"},
		{"an operand is rewritten", "(" + strings.ToUpper(left) + " / " + right + ")", 0, []string{left, right}, "verbatim and in argument order"},
		{"the first operand is missing but the count is made up", "(" + right + " / " + right + ")", 0, []string{left, right}, "verbatim and in argument order"},
		{"value-free operands may repeat and reorder", valueFree + " / " + other + " / " + valueFree, 0, []string{valueFree, other}, ""},
		{"the dialect's own marker is counted", "?::bigint", 1, nil, ""},
		{"two own markers where one belongs", "? + ?", 1, nil, "2 placeholders where 1 belong"},
		{"no marker where the dialect owes one", "TRUE", 1, nil, "0 placeholders where 1 belong"},
		{"an empty fragment owes nothing", "", 0, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTypedFragment(tc.fragment, '"', tc.own, tc.operands...)
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("checkTypedFragment() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("checkTypedFragment() = %v, want an error mentioning %q", err, tc.wantError)
			}
		})
	}
}
