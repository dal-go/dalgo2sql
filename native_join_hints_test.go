package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	_ "modernc.org/sqlite"
)

type nativeJoinHintTranslatorTestDouble struct {
	fragments NativeJoinHintFragments
	err       error
	calls     int
	from      dal.FromSource
}

func (d *nativeJoinHintTranslatorTestDouble) TranslateNativeJoinHints(from dal.FromSource) (NativeJoinHintFragments, error) {
	d.calls++
	d.from = from
	if d.err != nil {
		return NativeJoinHintFragments{}, d.err
	}
	return d.fragments, nil
}

type nativeStructuredCompilerTestDouble struct {
	calls     int
	query     dal.StructuredQuery
	fragments NativeJoinHintFragments
}

func (d *nativeStructuredCompilerTestDouble) CompileNativeStructuredQuery(query dal.StructuredQuery, fragments NativeJoinHintFragments) (string, []any, error) {
	d.calls++
	d.query = query
	d.fragments = fragments
	// A concrete adapter owns this text. This test compiler deliberately emits
	// SQLite-compatible SQL only to prove the DALgo transaction entrypoint
	// forwards its already-translated fragments; it does not claim another
	// dialect's SQL syntax or semantics.
	return "SELECT 7 AS result", nil, nil
}

func nativeHintTree() dal.StructuredQuery {
	c := dal.From(dal.NewRootCollectionRef("C", "c"))
	b := dal.From(dal.NewRootCollectionRef("B", "b")).Join(
		dal.NewJoinedFrom(c, dal.JoinLeft, sqlJoin("b", "id", "c", "b_id")).WithAlgorithms(dal.JoinAlgorithmMerge),
	)
	return dal.From(dal.NewRootCollectionRef("A", "a")).
		Join(dal.NewJoinedFrom(b, dal.JoinInner, sqlJoin("a", "id", "b", "a_id")).WithAlgorithms(dal.JoinAlgorithmHash, dal.JoinAlgorithmNestedLoop)).
		Join(dal.NewJoinedSource(dal.NewRootCollectionRef("D", "d"), dal.JoinLeft, sqlJoin("a", "id", "d", "a_id")).WithAlgorithms(dal.JoinAlgorithmLookup)).
		NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "id")})
}

func TestNativeJoinHintsReachOptInCompilerThroughDALgoTransaction(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	translator := &nativeJoinHintTranslatorTestDouble{fragments: NativeJoinHintFragments{
		AfterSelect: "/*+ USE_HASH(b) */",
		JoinOperators: map[string]string{
			"from.joins[0]":               "HASH",
			"from.joins[0].from.joins[0]": "MERGE",
			"from.joins[1]":               "LOOP",
		},
		AfterQuery:   "OPTION (RECOMPILE)",
		HandledPaths: []string{"from.joins[0]", "from.joins[0].from.joins[0]", "from.joins[1]"},
	}}
	compiler := &nativeStructuredCompilerTestDouble{}
	eligibleCalls := 0
	query := nativeHintTree()
	db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{
		StructuredQueryDialect:        "example-native",
		NativeJoinHintTranslator:      translator,
		NativeStructuredQueryCompiler: compiler,
		NativeJoinEligibility: func(_ context.Context, candidate dal.StructuredQuery) error {
			eligibleCalls++
			if !reflect.DeepEqual(candidate, query) {
				return fmt.Errorf("unexpected query")
			}
			return nil
		},
	})
	var got any
	if err := db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, query)
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		record, err := reader.Next()
		if err != nil {
			return err
		}
		got = record.Data().(map[string]any)["result"]
		if _, err := reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
			return fmt.Errorf("second reader.Next() = %v, want EOF", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != int64(7) {
		t.Fatalf("result = %#v, want 7", got)
	}
	if eligibleCalls != 1 || translator.calls != 1 || compiler.calls != 1 {
		t.Fatalf("eligibility/translator/compiler calls = %d/%d/%d, want 1/1/1", eligibleCalls, translator.calls, compiler.calls)
	}
	if !reflect.DeepEqual(compiler.query, query) || !reflect.DeepEqual(translator.from, query.From()) {
		t.Fatal("native adapter did not receive the complete query tree")
	}
	joins := translator.from.Joins()
	if len(joins) != 2 || !reflect.DeepEqual(joins[0].Algorithms(), []dal.JoinAlgorithm{dal.JoinAlgorithmHash, dal.JoinAlgorithmNestedLoop}) || !reflect.DeepEqual(joins[0].From().Joins()[0].Algorithms(), []dal.JoinAlgorithm{dal.JoinAlgorithmMerge}) || !reflect.DeepEqual(joins[1].Algorithms(), []dal.JoinAlgorithm{dal.JoinAlgorithmLookup}) {
		t.Fatalf("translator JOIN algorithms = %#v", joins)
	}
	if !reflect.DeepEqual(compiler.fragments, translator.fragments) {
		t.Fatalf("compiler fragments = %#v, want %#v", compiler.fragments, translator.fragments)
	}
}

func TestNativeJoinHintTranslatorRejectsUnacknowledgedEdgeBeforeCompiler(t *testing.T) {
	query := nativeHintTree()
	translator := &nativeJoinHintTranslatorTestDouble{fragments: NativeJoinHintFragments{HandledPaths: []string{"from.joins[0]"}}}
	compiler := &nativeStructuredCompilerTestDouble{}
	_, err := getReaderBaseWithOptions(context.Background(), query, func(context.Context, string, ...any) (*sql.Rows, error) {
		t.Fatal("SQL execution must not begin after join hint translation fails")
		return nil, nil
	}, DbOptions{NativeJoinHintTranslator: translator, NativeStructuredQueryCompiler: compiler})
	var diagnostic *dal.JoinValidationError
	if !errors.As(err, &diagnostic) || diagnostic.Category != "join_plan" || diagnostic.Path != "from.joins[0].from.joins[0]" || !containsAll(err.Error(), "did not acknowledge") {
		t.Fatalf("translator error = %v", err)
	}
	if compiler.calls != 0 {
		t.Fatalf("compiler calls = %d, want 0", compiler.calls)
	}
}

func TestNativeJoinHintTranslatorRejectsInvalidAcknowledgments(t *testing.T) {
	from := nativeHintTree().From()
	for _, tc := range []struct {
		name      string
		fragments NativeJoinHintFragments
		err       error
		want      string
		path      string
	}{
		{name: "translator error", err: errors.New("unsupported preference"), want: "unsupported preference", path: "from"},
		{name: "unknown acknowledgment", fragments: NativeJoinHintFragments{HandledPaths: []string{"from.joins[9]"}}, want: "unknown path", path: "from.joins[9]"},
		{name: "duplicate acknowledgment", fragments: NativeJoinHintFragments{HandledPaths: []string{"from.joins[0]", "from.joins[0]"}}, want: "more than once", path: "from.joins[0]"},
		{name: "unknown operator", fragments: NativeJoinHintFragments{HandledPaths: []string{"from.joins[0]", "from.joins[0].from.joins[0]", "from.joins[1]"}, JoinOperators: map[string]string{"from.joins[9]": "HASH"}}, want: "operator for an unknown path", path: "from.joins[9]"},
		{name: "empty operator", fragments: NativeJoinHintFragments{HandledPaths: []string{"from.joins[0]", "from.joins[0].from.joins[0]", "from.joins[1]"}, JoinOperators: map[string]string{"from.joins[0]": ""}}, want: "empty operator", path: "from.joins[0]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := translateNativeJoinHints(from, &nativeJoinHintTranslatorTestDouble{fragments: tc.fragments, err: tc.err})
			var diagnostic *dal.JoinValidationError
			if !errors.As(err, &diagnostic) || diagnostic.Category != "join_plan" || diagnostic.Path != tc.path || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("translator error = %v, want %q", err, tc.want)
			}
		})
	}
	precise := &dal.JoinValidationError{Category: "join_plan", Path: "from.joins[1]", Message: "lookup is disabled"}
	_, err := translateNativeJoinHints(from, &nativeJoinHintTranslatorTestDouble{err: precise})
	var diagnostic *dal.JoinValidationError
	if !errors.As(err, &diagnostic) || diagnostic != precise {
		t.Fatalf("precise translator diagnostic = %#v, want %#v", diagnostic, precise)
	}

	translator := &nativeJoinHintTranslatorTestDouble{err: errors.New("must not be called")}
	fragments, err := translateNativeJoinHints(dal.From(dal.NewRootCollectionRef("A", "a")), translator)
	if err != nil || !reflect.DeepEqual(fragments, NativeJoinHintFragments{}) || translator.calls != 0 {
		t.Fatalf("unhinted translation = %#v, %v, calls=%d", fragments, err, translator.calls)
	}
}

func TestSQLiteNativeJoinHintsRemainIgnoredWithoutOptInCompiler(t *testing.T) {
	query := nativeHintTree()
	translator := &nativeJoinHintTranslatorTestDouble{err: errors.New("must not be called")}
	want, _, err := compileStructuredSQL(query)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE A (id INTEGER); CREATE TABLE B (id INTEGER, a_id INTEGER); CREATE TABLE C (id INTEGER, b_id INTEGER); CREATE TABLE D (a_id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	var got string
	reader, err := getReaderBaseWithOptions(context.Background(), query, func(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
		got = text
		return raw.QueryContext(ctx, text, args...)
	}, DbOptions{StructuredQueryDialect: "sqlite", NativeJoinHintTranslator: translator})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.rows.Close(); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("SQLite SQL changed without an opt-in compiler:\n%s\nwant:\n%s", got, want)
	}
	if translator.calls != 0 {
		t.Fatalf("SQLite translator calls = %d, want 0", translator.calls)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
