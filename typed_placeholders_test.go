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
