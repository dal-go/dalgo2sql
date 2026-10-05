package dalgo2sql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The README and the comments state rules the code keeps. These tests pin what
// each of them says, so a rule cannot change without its text.

func readSource(t *testing.T, path string) string {
	t.Helper()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

// squash makes text comparable across the line breaks of a comment or a paragraph.
func squash(text string) string { return strings.Join(strings.Fields(text), " ") }

func requireContains(t *testing.T, where, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(squash(text), squash(want)) {
			t.Errorf("%s does not say %q", where, want)
		}
	}
}

func requireAbsent(t *testing.T, where, text string, unwanted ...string) {
	t.Helper()
	for _, bad := range unwanted {
		if strings.Contains(squash(text), squash(bad)) {
			t.Errorf("%s still says %q", where, bad)
		}
	}
}

// parsed returns the syntax tree of a source file of this package, comments kept.
func parsed(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// The exported godoc of DbOptions.Recordsets says under which name a nested key's
// recordset is declared, and that a parent's ID is in no statement.
func TestDbOptionsRecordsetsDocumentsNestedKeys(t *testing.T) {
	var doc string
	ast.Inspect(parsed(t, "database_options.go"), func(n ast.Node) bool {
		if spec, ok := n.(*ast.TypeSpec); ok && spec.Name.Name == "DbOptions" {
			for _, field := range spec.Type.(*ast.StructType).Fields.List {
				if len(field.Names) == 1 && field.Names[0].Name == "Recordsets" {
					doc = field.Doc.Text()
				}
			}
		}
		return true
	})
	requireContains(t, "the godoc of DbOptions.Recordsets", doc,
		"joined with \"_\"", "the key's own collection first", `"lines_orders"`,
		"ErrUndeclaredNestedRecordset", "A parent's ID is in no statement",
		"cannot tell the rows of different parents apart")
}

// The comment of getRecordsetName says where the primary key is looked up.
func TestGetRecordsetNameCommentSaysWhereThePrimaryKeyIsLookedUp(t *testing.T) {
	var doc string
	for _, decl := range parsed(t, "recordsets.go").Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "getRecordsetName" {
			doc = fn.Doc.Text()
		}
	}
	requireContains(t, "the comment of getRecordsetName", doc,
		"The primary key is looked up under the same name", "DbOptions.PrimaryKeyFieldNames",
		"for GetMulti of several records, directly in", "DbOptions.Recordsets")
	requireAbsent(t, "the comment of getRecordsetName", doc, "DbOptions.PrimaryKeyFieldNames (the primary key)")
}

// deleteByKeys takes the keys of one recordset and nothing else.
func TestDeleteByKeysHasNoDeadParameter(t *testing.T) {
	found := false
	ast.Inspect(parsed(t, "deleter.go"), func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 {
			return true
		}
		if name, ok := assign.Lhs[0].(*ast.Ident); ok && name.Name == "deleteByKeys" {
			found = true
			if params := assign.Rhs[0].(*ast.FuncLit).Type.Params.List; len(params) != 1 {
				t.Errorf("deleteByKeys has %d parameters, want 1", len(params))
			}
		}
		return true
	})
	if !found {
		t.Error("deleteByKeys was not found")
	}
}

func TestReadmeStatesTheLegacyPathAndTheNumericRules(t *testing.T) {
	readme := readSource(t, "README.md")
	legacy := readme[strings.Index(readme, "### Legacy text path"):strings.Index(readme, "## NUMERIC result values")]
	requireContains(t, "the README's legacy text path", legacy,
		"What it refuses, by where it is found",
		"A string inside a slice passed as one constant",
		"A plain value and an `IN` list may hold these characters",
		"A source leaves this path by setting a dialect",
		"`postgres` for PostgreSQL, which is selectable",
		"MySQL has no dialect")
	requireAbsent(t, "the README's legacy text path", legacy, "mounts today")
	numeric := readme[strings.Index(readme, "## NUMERIC result values"):strings.Index(readme, "## End2end")]
	if !strings.HasPrefix(numeric, "## NUMERIC result values\n\nSince dalgo2sql v0.21.0 the readers turn the text") {
		t.Errorf("the NUMERIC section does not lead with the rule and its baseline version:\n%.200s", numeric)
	}
	requireContains(t, "the README's NUMERIC section", numeric, "Compared with dalgo2sql v0.20")
}

func TestReadmeStatesTheStructRules(t *testing.T) {
	readme := readSource(t, "README.md")
	requireContains(t, "the README", readme,
		"An exact spelling beats depth",
		"the shallower one wins",
		`scany's `+"`db:\"\"`"+` on a named struct field`,
		"is not supported",
		"`NULL` is `nil` in a pointer,",
		"outside the field's range")
	requireAbsent(t, "the README", readme, "`NULL` stores the zero value")
}
