package dalgo2sql

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/tugql"
)

// commentsVectorsSHA256 pins testdata/tugql/v1/comments.json to the copy shipped in
// github.com/dal-go/dalgo v0.95.2 (tugql/testdata/tugql/v1/comments.json).
const commentsVectorsSHA256 = "867afde01bdfdf87e6ca489d2620ba75a2c87f0f53636ed8411b3cc4183417df"

type commentsVectors struct {
	Schema    []tugql.TugQLAuthorizedSchema `json:"schema"`
	TextCases []struct {
		Name     string `json:"name"`
		Text     string `json:"text"`
		Expected struct {
			Diagnostics   []tugql.TugQLDiagnostic `json:"diagnostics"`
			CommentTokens []struct {
				Line   int `json:"line"`
				Column int `json:"column"`
			} `json:"commentTokens"`
			SQL *struct {
				Forbid                    string   `json:"forbid"`
				StringLiterals            []string `json:"stringLiterals"`
				QuotedIdentifiers         []string `json:"quotedIdentifiers"`
				IgnoringStringLiterals    bool     `json:"ignoringStringLiterals"`
				IgnoringQuotedIdentifiers bool     `json:"ignoringQuotedIdentifiers"`
			} `json:"sql"`
		} `json:"expected"`
	} `json:"textCases"`
}

func loadCommentsVectors(t *testing.T) commentsVectors {
	t.Helper()
	contents, err := os.ReadFile("testdata/tugql/v1/comments.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(contents)); got != commentsVectorsSHA256 {
		t.Fatalf("comments.json SHA-256 = %s, want %s (the dalgo v0.95.2 copy)", got, commentsVectorsSHA256)
	}
	var vectors commentsVectors
	if err := json.Unmarshal(contents, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

// stripComments removes, from text, every comment whose marker starts at one of the
// 1-based (line, column) positions; the comment runs to the end of its line.
func stripComments(text string, starts map[int]int) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		column, ok := starts[i+1]
		if !ok {
			continue
		}
		runes := []rune(line)
		tail := ""
		if strings.HasSuffix(line, "\r") {
			tail = "\r"
		}
		lines[i] = string(runes[:column-1]) + tail
	}
	return strings.Join(lines, "\n")
}

// withoutAllowed removes the string literals and quoted identifiers in which the
// vectors allow the forbidden text to appear.
func withoutAllowed(sql string, allowed ...[]string) string {
	for _, group := range allowed {
		for _, literal := range group {
			sql = strings.ReplaceAll(sql, literal, "")
		}
	}
	return sql
}

// tugqlResolvedQuery parses and resolves text, failing the test on any diagnostic.
func tugqlResolvedQuery(t *testing.T, text string, schema []tugql.TugQLAuthorizedSchema) dal.StructuredQuery {
	t.Helper()
	doc, diagnostics := tugql.ParseTugQL(text)
	if len(diagnostics) != 0 {
		t.Fatalf("parse diagnostics: %+v", diagnostics)
	}
	resolved, diagnostics := tugql.ResolveTugQL(doc, tugql.TugQLResolveContext{AuthorizedSchemas: schema})
	if len(diagnostics) != 0 {
		t.Fatalf("resolve diagnostics: %+v", diagnostics)
	}
	return resolved.Query
}

// TestTugQLCommentsNeverReachSQL feeds every comments.json vector through the
// parse, resolve and SQL emission of each dialect this package emits (the generic
// ?-placeholder emitter used for SQLite and MySQL, and PostgreSQL in both
// identifier modes). The SQL must contain no `//` outside string literals and
// quoted identifiers, and must be byte-identical to the SQL of the same query with
// its comments removed, so no comment text travels into it either.
func TestTugQLCommentsNeverReachSQL(t *testing.T) {
	vectors := loadCommentsVectors(t)
	dialects := []struct {
		name    string
		compile func(dal.StructuredQuery) (string, []any, error)
	}{
		{"generic", compileStructuredSQL},
		{"postgres-exact", func(q dal.StructuredQuery) (string, []any, error) {
			return compileTypedSQL(q, newPostgresDialect(postgresExact), typedCatalogFacts{})
		}},
		{"postgres-foldlower", func(q dal.StructuredQuery) (string, []any, error) {
			return compileTypedSQL(q, newPostgresDialect(postgresFoldLower), typedCatalogFacts{})
		}},
	}
	covered := 0
	for _, c := range vectors.TextCases {
		if c.Expected.SQL == nil || len(c.Expected.Diagnostics) != 0 {
			continue
		}
		covered++
		starts := map[int]int{}
		for _, token := range c.Expected.CommentTokens {
			starts[token.Line] = token.Column
		}
		stripped := stripComments(c.Text, starts)
		for _, dialect := range dialects {
			t.Run(c.Name+"/"+dialect.name, func(t *testing.T) {
				sql, args, err := dialect.compile(tugqlResolvedQuery(t, c.Text, vectors.Schema))
				if err != nil {
					// A shape this emitter refuses (a CTE needs the generic engine)
					// is refused the same way with and without comments, and the
					// refusal carries no comment.
					_, _, strippedErr := dialect.compile(tugqlResolvedQuery(t, stripped, vectors.Schema))
					if strippedErr == nil || strippedErr.Error() != err.Error() || strings.Contains(err.Error(), "//") {
						t.Fatalf("emit: %v (without comments: %v)", err, strippedErr)
					}
					return
				}
				if got := withoutAllowed(sql, c.Expected.SQL.StringLiterals, c.Expected.SQL.QuotedIdentifiers); strings.Contains(got, c.Expected.SQL.Forbid) {
					t.Fatalf("SQL contains %q outside literals:\n%s", c.Expected.SQL.Forbid, sql)
				}
				for _, arg := range args {
					if text, ok := arg.(string); ok && strings.Contains(text, "//") && !contains(c.Expected.SQL.StringLiterals, text) {
						t.Fatalf("argument %q carries a comment", text)
					}
				}
				wantSQL, wantArgs, err := dialect.compile(tugqlResolvedQuery(t, stripped, vectors.Schema))
				if err != nil {
					t.Fatalf("emit without comments: %v", err)
				}
				if sql != wantSQL || fmt.Sprint(args) != fmt.Sprint(wantArgs) {
					t.Fatalf("comments changed the emitted SQL:\n%s %v\nvs\n%s %v", sql, args, wantSQL, wantArgs)
				}
			})
		}
	}
	if covered == 0 {
		t.Fatal("no comments.json vector carries an sql expectation")
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// TestTugQLCommentsVectorsGuards covers the helpers' edge cases.
func TestTugQLCommentsVectorsGuards(t *testing.T) {
	if got := stripComments("a // b\r\nc\nd", map[int]int{1: 3}); got != "a \nc\nd" && got != "a \r\nc\nd" {
		t.Fatalf("stripComments = %q", got)
	}
	if got := stripComments("a // b\r\nc", map[int]int{1: 3}); got != "a \r\nc" {
		t.Fatalf("stripComments CRLF = %q", got)
	}
	if got := withoutAllowed("x//y", []string{"//"}); got != "xy" {
		t.Fatalf("withoutAllowed = %q", got)
	}
}
