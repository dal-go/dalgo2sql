package dalgo2sql

import (
	"context"
	"flag"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

// updateKeyStatementsGolden rewrites testdata/key_statements.golden from the
// code under test. The file was recorded from main before the nested-key rule,
// the empty-update refusals and the batch changes were written. Its one
// deliberate difference from that recording is the statements of DeleteMulti: main
// sent a DELETE per key and then the IN statement for a recordset with several
// keys, and the file has the IN statement only. Do not regenerate it to follow any
// other change of the statements of a key without a parent.
var updateKeyStatementsGolden = flag.Bool("update-key-statements-golden", false, "rewrite testdata/key_statements.golden")

// goldenCase is one call whose statements are recorded: a key without a parent
// is addressed, so what is sent is the text this package has always sent.
type goldenCase struct {
	name string
	// sorted is set where the call visits its recordsets in map order.
	sorted bool
	// rowExists is what the driver answers to a query.
	rowExists bool
	run       func(context.Context, keyPathAPI) error
}

func goldenCases() []goldenCase {
	c := validNames()
	structRecord := func(id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(c.key(id), &struct{ Name string }{})
	}
	other := func(id string) *dalrecord.Key { return dalrecord.NewKeyWithID("accounts", id) }
	twoUpdates := []update.Update{rawUpdate{name: "name", value: "v"}, rawUpdate{name: "age", value: 3}}
	cases := make([]goldenCase, 0, 40)
	for _, op := range keyPathOps {
		for _, exists := range []bool{false, true} {
			cases = append(cases, goldenCase{name: op.name, rowExists: exists, run: func(ctx context.Context, api keyPathAPI) error {
				return op.run(ctx, api, c)
			}})
		}
	}
	return append(cases,
		goldenCase{name: "get-struct-record", run: func(ctx context.Context, api keyPathAPI) error {
			return api.Get(ctx, structRecord("id1"))
		}},
		goldenCase{name: "update-two-fields", run: func(ctx context.Context, api keyPathAPI) error {
			return api.Update(ctx, c.key("id1"), twoUpdates)
		}},
		goldenCase{name: "update-multi-two-fields", run: func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, c.keys(), twoUpdates)
		}},
		goldenCase{name: "update-multi-two-recordsets", run: func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{c.key("id1"), other("id2")}, c.updates())
		}},
		goldenCase{name: "delete-multi-one-key", run: func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{c.key("id1")})
		}},
		goldenCase{name: "delete-multi-alternating-recordsets", run: func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{c.key("id1"), other("id2"), c.key("id3")})
		}},
		goldenCase{name: "delete-multi-consecutive-recordsets", run: func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{c.key("id1"), c.key("id2"), other("id3"), other("id4")})
		}},
		goldenCase{name: "delete-multi-no-keys", run: func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, nil)
		}},
		goldenCase{name: "get-multi-two-recordsets", sorted: true, run: func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(c.key("id1"), map[string]any{}),
				dalrecord.NewRecordWithData(c.key("id2"), map[string]any{}),
				dalrecord.NewRecordWithData(other("id3"), map[string]any{}),
			})
		}},
		goldenCase{name: "get-multi-struct-one-record", run: func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{structRecord("id1")})
		}},
		goldenCase{name: "set-multi-two-recordsets", run: func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{
				c.mapRecord("id1"),
				dalrecord.NewRecordWithData(other("id2"), map[string]any{"name": "v"}),
			})
		}},
		goldenCase{name: "set-struct", run: func(ctx context.Context, api keyPathAPI) error {
			return api.Set(ctx, structRecord("id1"))
		}},
		goldenCase{name: "insert-struct", run: func(ctx context.Context, api keyPathAPI) error {
			return api.Insert(ctx, structRecord("id1"))
		}},
		goldenCase{name: "insert-adapter-generated-id", run: func(ctx context.Context, api keyPathAPI) error {
			return api.Insert(ctx, dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{"name": "v"}), dal.WithAdapterGeneratedID())
		}},
		goldenCase{name: "insert-multi", run: func(ctx context.Context, api keyPathAPI) error {
			batch, ok := api.(insertMultiAPI) // a database handle has none
			if !ok {
				return nil
			}
			return batch.InsertMulti(ctx, []dalrecord.Record{c.mapRecord("id1"), c.mapRecord("id2")})
		}},
		goldenCase{name: "insert-multi-generated-ids", run: func(ctx context.Context, api keyPathAPI) error {
			batch, ok := api.(insertMultiAPI)
			if !ok {
				return nil
			}
			return batch.InsertMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{"name": "v"}),
				dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{"name": "v"}),
			}, dal.WithRandomStringKey(8, 3))
		}},
	)
}

// goldenOptions declares the users recordset with the primary key id, and
// leaves accounts undeclared, so that its keys fall back to the defaults.
func goldenOptions(dialect string, placeholder PlaceholderDialect) DbOptions {
	options := validNames().options(dialect)
	options.Placeholder = placeholder
	delete(options.Recordsets, "accounts")
	return options
}

func recordKeyStatements(t *testing.T) string {
	t.Helper()
	var out strings.Builder
	for _, dialect := range []string{"", dialectSQLite} {
		for _, placeholder := range []PlaceholderDialect{PlaceholderQuestion, PlaceholderDollar} {
			for _, tt := range goldenCases() {
				for _, r := range keyPathAPIsAnswering(t, goldenOptions(dialect, placeholder), func(string) bool { return tt.rowExists }) {
					_ = tt.run(context.Background(), r.api)
					calls := r.recorder.calls()
					if tt.sorted {
						slices.Sort(calls)
					}
					out.WriteString("## dialect=" + dialect + " placeholder=" + map[PlaceholderDialect]string{
						PlaceholderQuestion: "question", PlaceholderDollar: "dollar"}[placeholder] +
						" " + r.kind + " " + tt.name + " rowExists=" + map[bool]string{false: "no", true: "yes"}[tt.rowExists] + "\n")
					for _, call := range calls {
						out.WriteString(strings.ReplaceAll(call, "\n", `\n`) + "\n")
					}
				}
			}
		}
	}
	return out.String()
}

// The statements of a key without a parent are the ones main sent: every
// operation, on a database handle and on a transaction, for a declared and an
// undeclared recordset, with each placeholder style and each name mode.
func TestKeyStatements_WithoutAParentAreTheOnesMainSent(t *testing.T) {
	const path = "testdata/key_statements.golden"
	got := recordKeyStatements(t)
	if *updateKeyStatementsGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		gotLines, wantLines := strings.Split(got, "\n"), strings.Split(string(want), "\n")
		for i := 0; i < len(gotLines) && i < len(wantLines); i++ {
			if gotLines[i] != wantLines[i] {
				t.Fatalf("statements differ from %s at line %d:\n got  %q\n want %q", path, i+1, gotLines[i], wantLines[i])
			}
		}
		t.Fatalf("statements differ from %s: %d lines, want %d", path, len(gotLines), len(wantLines))
	}
}
