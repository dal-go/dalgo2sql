package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func budgetArithmetic(depth int, right any) dal.Expression {
	var expression dal.Expression = dal.Field("value")
	for range depth {
		expression = dal.BinaryExpression{Left: expression, Operator: dal.Add, Right: dal.Constant{Value: right}}
	}
	return expression
}

func budgetQuery(expressions ...dal.Expression) dal.StructuredQuery {
	columns := make([]dal.Column, len(expressions))
	for i, expression := range expressions {
		columns[i] = dal.Column{Expression: expression}
	}
	return dal.From(dal.NewRootCollectionRef("Numbers", "")).NewQuery().SelectColumns(columns...)
}

func TestSQLiteCompileBudgetRefusesOriginalExpansionBeforeAllocation(t *testing.T) {
	for depth := 1; depth <= 3; depth++ {
		expression := budgetArithmetic(depth, int64(1))
		legacy, _, err := compileSQLExpressionWithSources(expression, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		emitter := newSQLiteEmitter(true)
		text, args, err := emitter.compileSQLExpressionWithSources(expression, nil, false)
		runtime.ReadMemStats(&after)
		if depth < 3 {
			if err != nil || len(text) == 0 || len(args) == 0 {
				t.Fatalf("bounded arithmetic lost semantics: depth%d,%v", depth, err)
			}
		} else {
			if !errors.Is(err, errSQLiteCompileBudget) || text != "" || args != nil {
				t.Fatalf("amplifying expression escaped: bytes%d,args%d,%v", len(text), len(args), err)
			}
			// This conservative whole-test allocation ceiling discriminates the
			// old 3.8MiB final string plus its intermediates. It is not a claimed
			// total process/native heap ceiling. No deeper/OOM probe is run.
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 3<<20 {
				t.Fatalf("refusal allocated expanded SQL first: %d bytes", allocated)
			}
		}
		if emitter.budget.bytes > sqliteCompileBytes || emitter.budget.slots > sqliteCompileSlots || emitter.budget.values > sqliteCompileBytes {
			t.Fatal("construction counters escaped their ceilings")
		}
		t.Logf("depth%d legacy%d strict%d refused%v", depth, len(legacy), len(text), err != nil)
	}
}

func TestSQLiteCompileBudgetIsSharedAcrossQueryAndNativePlanning(t *testing.T) {
	medium := budgetArithmetic(2, int64(1))
	if text, _, err := newSQLiteEmitter(true).compileStructuredSQL(budgetQuery(medium)); err != nil || text == "" {
		t.Fatalf("one bounded expression refused: %v", err)
	}
	columns := make([]dal.Expression, 16)
	for i := range columns {
		columns[i] = medium
	}
	query := budgetQuery(columns...)
	if text, args, err := newSQLiteEmitter(true).compileStructuredSQL(query); !errors.Is(err, errSQLiteCompileBudget) || text != "" || args != nil {
		t.Fatalf("projection fanout received fresh budgets: bytes%d,args%d,%v", len(text), len(args), err)
	}
	calls := 0
	execute := func(context.Context, string, ...any) (*sql.Rows, error) {
		calls++
		return nil, errors.New("unexpected native statement")
	}
	if reader, err := getRecordsetReaderWithOptions(context.Background(), query, execute, DbOptions{StructuredQueryDialect: "sqlite", PreserveNativeValues: true, SQLiteStrictNumbers: true}); err == nil || reader != nil || calls != 0 {
		t.Fatalf("budget refusal reached native read: calls%d,%v", calls, err)
	}
	if err := canExecuteSQLiteJoinProfile(context.Background(), query, "sqlite", execute, true); !errors.Is(err, errSQLiteCompileBudget) || calls != 0 {
		t.Fatalf("native planning used unbounded compiler: calls%d,%v", calls, err)
	}
	// Shared construction accounting includes WHERE, GROUP, HAVING and ORDER,
	// rather than resetting the emitter for any later clause or rewritten query.
	emitter := newSQLiteEmitter(true)
	if _, _, err := emitter.compileStructuredSQL(budgetQuery(medium)); err != nil {
		t.Fatal(err)
	}
	for range 16 {
		_, _, err := emitter.compileSQLConditionWithSources(dal.NewComparison(dal.Field("value"), dal.Equal, medium), nil, false)
		if err != nil {
			break
		}
	}
	if !errors.Is(emitter.failure(), errSQLiteCompileBudget) {
		t.Fatal("later clauses reset shared construction accounting")
	}
}

type budgetOpaqueValuer struct{ calls *atomic.Int32 }

func (v budgetOpaqueValuer) Value() (driver.Value, error) {
	v.calls.Add(1)
	return strings.Repeat("x", 2<<20), nil
}

func TestSQLiteCompileBudgetBoundsArgumentsBeforeCopyOrValuer(t *testing.T) {
	var invoked atomic.Int32
	for _, value := range []any{budgetOpaqueValuer{calls: &invoked}, strings.Repeat("x", sqliteCompileBytes+1), make([]byte, sqliteCompileBytes+1)} {
		if text, args, err := newSQLiteEmitter(true).compileStructuredSQL(budgetQuery(dal.Constant{Value: value})); !errors.Is(err, errSQLiteCompileBudget) || text != "" || args != nil {
			t.Fatalf("unsized/excessive binding admitted=%d,%d,%v", len(text), len(args), err)
		}
	}
	if invoked.Load() != 0 {
		t.Fatal("unsized custom Valuer was invoked")
	}
	for _, value := range []any{strings.Repeat("x", 64<<10), make([]byte, 64<<10)} {
		emitter := newSQLiteEmitter(true)
		if values := emitter.repeatArgs([]any{value}, 17); values != nil || !errors.Is(emitter.failure(), errSQLiteCompileBudget) {
			t.Fatal("large binding payload copied before size check")
		}
	}
	emitter := newSQLiteEmitter(true)
	if values := emitter.repeatArgs([]any{nil}, sqliteCompileSlots+1); values != nil || emitter.failure() == nil {
		t.Fatal("excessive argument slots copied")
	}
	emitter = newSQLiteEmitter(true)
	for range 16 {
		if emitter.appendArgs(nil, strings.Repeat("x", 64<<10)) == nil {
			t.Fatal("in-budget binding refused")
		}
	}
	if values := emitter.appendArgs(nil, "x"); values != nil || emitter.failure() == nil {
		t.Fatal("argument byte pool reset between appends")
	}
	if values := newSQLiteEmitter(true).repeatArgs([]any{1}, -1); values != nil {
		t.Fatal("negative repetition admitted")
	}
}

func TestSQLiteCompileBudgetKnownValuesAndSharedStringBoundaries(t *testing.T) {
	for _, value := range []any{nil, false, int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1), "text", []byte{1}} {
		if _, known := sqliteArgumentBytes(value); !known {
			t.Fatalf("known builtin refused: %T", value)
		}
	}
	for _, value := range []any{float32(math.Inf(1)), math.Inf(-1), math.NaN(), struct{}{}} {
		if _, known := sqliteArgumentBytes(value); known {
			t.Fatalf("unsized/nonfinite value accepted: %T", value)
		}
	}
	emitter := newSQLiteEmitter(true)
	first := strings.Repeat("x", sqliteCompileBytes-1)
	if got := emitter.concat(first); len(got) != len(first) {
		t.Fatal("bounded string lost")
	}
	if got := emitter.join([]string{"", ""}, "xx"); got != "" || emitter.failure() == nil {
		t.Fatal("separator expansion escaped shared budget")
	}
	emitter = newSQLiteEmitter(true)
	if got := emitter.concat(strings.Repeat("x", sqliteCompileBytes+1)); got != "" || emitter.failure() == nil {
		t.Fatal("oversize string allocated")
	}
	if _, err := emitter.stringParts(1); err == nil {
		t.Fatal("failed budget produced staging slice")
	}
	emitter = newSQLiteEmitter(true)
	emitter.budget.slots = sqliteCompileSlots
	if _, err := emitter.stringParts(1); err == nil {
		t.Fatal("staging slice allocated before slot check")
	}
	if _, err := emitter.copySources(map[string]struct{}{"n": {}}); err == nil {
		t.Fatal("source map copied before shared check")
	}
	emitter = newSQLiteEmitter(true)
	emitter.budget.bytes = sqliteCompileBytes
	if emitter.quoteIdentifier("n") != "" {
		t.Fatal("identifier quoted before shared check")
	}
	if _, err := emitter.quoteCheckedIdentifier(positionField, "n"); err == nil {
		t.Fatal("checked identifier allocated after failure")
	}
	var used int
	b := &sqliteCompileBudget{}
	if b.take(&used, -1, 8) || b.err == nil {
		t.Fatal("negative construction budget admitted")
	}
	builder := sqliteSQLBuilder{emitter: newSQLiteEmitter(true)}
	builder.addString("ok")
	builder.addByte('!')
	if builder.String() != "ok!" || builder.emitter.failure() != nil {
		t.Fatal("bounded builder failed")
	}
	builder.emitter.budget.bytes = sqliteCompileBytes
	builder.addString("refused")
	builder.addByte('!')
	if builder.emitter.failure() == nil || builder.String() != "ok!" {
		t.Fatal("builder ignored shared budget")
	}
}

func TestSQLiteCompileBudgetPreflightBoundsEveryClause(t *testing.T) {
	root := dal.NewRootCollectionRef("Numbers", "n")
	other := dal.NewRootCollectionRef("Other", "x")
	query := func() dal.IQueryBuilder { return dal.From(root).NewQuery() }
	large := strings.Repeat("x", sqliteCompileBytes+1)
	wild := func(exclude []string) dal.StructuredQuery {
		return query().SelectColumns(dal.Column{Wildcard: &dal.WildcardProjection{Exclude: exclude}})
	}
	join := func(source dal.RecordsetSource, on dal.Condition) dal.FromSource {
		return dal.From(root).Join(dal.NewJoinedSource(source, dal.JoinInner, on))
	}
	equal := dal.NewComparison(dal.NewFieldRef("n", "value"), dal.Equal, dal.NewFieldRef("x", "value"))
	bad := dal.Constant{Value: struct{}{}}
	for _, node := range []any{
		wild(make([]string, sqliteCompileNodes+1)), wild([]string{large}),
		query().GroupBy(dal.Field(large)).SelectIntoRecordset(), query().OrderBy(nil).SelectIntoRecordset(),
		join(dal.NewRootCollectionRef(large, "x"), equal), join(other, dal.NewComparison(dal.Field(large), dal.Equal, dal.Constant{Value: 1})),
		dal.From(root).Join(dal.NewJoinedSource(other, dal.JoinType(large), equal)), dal.NewAggregate(large, false, dal.Field("value")),
		dal.From(dal.NewRootCollectionRef(large, "")), dal.Array{Value: make([]int, sqliteCompileNodes+1)}, dal.Array{Value: []any{struct{}{}}},
		dal.NewAggregate(dal.SUM, false, bad), dal.NewOrderedAggregate(dal.FIRST, []dal.OrderExpression{nil}, dal.Field("value")),
		dal.NewGroupCondition(dal.And, dal.FieldName("opaque")), dal.QuerySource{}, struct{}{}, (*dal.CollectionRef)(nil),
	} {
		if emitter := newSQLiteEmitter(true); emitter.inspect(node, 0) || emitter.failure() == nil {
			t.Fatalf("unsafe structural input accepted: %T", node)
		}
	}
	for _, node := range []any{wild([]string{"private"}), &root, dal.NewOrderedAggregate(dal.FIRST, []dal.OrderExpression{dal.AscendingField("value")}, dal.Field("value")), dal.NewIsNullCondition(dal.Field("value")), dal.NewGroupCondition(dal.And, equal), dal.Array{Value: []any{nil, "known", 1}}} {
		if emitter := newSQLiteEmitter(true); !emitter.inspect(node, 0) {
			t.Fatalf("supported structure lost during preflight: %T,%v", node, emitter.failure())
		}
	}
	if emitter := newSQLiteEmitter(true); emitter.inspect(dal.Field("value"), 65) {
		t.Fatal("structural depth cap was ignored")
	}
	for _, node := range []any{query().GroupBy(dal.Field("value")).SelectIntoRecordset(), query().OrderBy(dal.AscendingField("value")).SelectIntoRecordset()} {
		emitter := newSQLiteEmitter(true)
		emitter.budget.nodes = sqliteCompileNodes - 5
		if emitter.inspect(node, 0) {
			t.Fatal("later query clause received a fresh work budget")
		}
	}
	for _, node := range []any{join(other, equal), dal.NewOrderedAggregate(dal.FIRST, []dal.OrderExpression{dal.AscendingField(large)}, dal.Field("value"))} {
		emitter := newSQLiteEmitter(true)
		emitter.budget.inputs = sqliteCompileBytes
		if emitter.inspect(node, 0) {
			t.Fatal("later relation/order received a fresh input budget")
		}
	}
}

func TestSQLiteCompileBudgetRefusesExhaustedStagesWithoutPartialSQL(t *testing.T) {
	root := dal.NewRootCollectionRef("Numbers", "n")
	other := dal.NewRootCollectionRef("Other", "x")
	equal := dal.NewComparison(dal.NewFieldRef("n", "value"), dal.Equal, dal.NewFieldRef("x", "value"))
	if _, err := (sqliteEmitter{strictNumbers: true}).compileSQLJoinOn([]dal.Condition{equal}, map[string]struct{}{"n": {}, "x": {}}, "from"); err != nil {
		t.Fatal(err)
	}
	if _, err := (sqliteEmitter{strictNumbers: true}).compileSQLJoinOn([]dal.Condition{dal.FieldName("opaque")}, nil, "from"); err == nil {
		t.Fatal("cold ON bypassed shape check")
	}
	from := dal.From(root).Join(dal.NewJoinedSource(other, dal.JoinInner, equal))
	if text, _, err := (sqliteEmitter{strictNumbers: true}).compileStructuredSQL(budgetQuery(dal.Field("value"))); err != nil || text == "" {
		t.Fatal(err)
	}
	if _, _, _, err := (sqliteEmitter{strictNumbers: true}).compileSQLRelationWithin(dal.From(root), "from", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := (sqliteEmitter{strictNumbers: true}).compileSQLRelationWithin(dal.From(dal.NewRootCollectionRef(strings.Repeat("x", sqliteCompileBytes+1), "")), "from", nil); err == nil {
		t.Fatal("cold relation bypassed budget")
	}
	if _, _, err := (sqliteEmitter{strictNumbers: true}).compileSQLConditionWithSources(dal.WhereField("value", dal.Equal, 1), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (sqliteEmitter{strictNumbers: true}).compileSQLConditionWithSources(dal.FieldName("opaque"), nil, false); err == nil {
		t.Fatal("cold condition bypassed shape check")
	}
	if _, _, err := (sqliteEmitter{strictNumbers: true}).compileSQLExpressionWithSources(dal.FieldName("opaque"), nil, false); err == nil {
		t.Fatal("cold expression bypassed shape check")
	}
	for _, outer := range []map[string]struct{}{nil, {"outer": {}}} {
		emitter := newSQLiteEmitter(true)
		emitter.budget.slots = sqliteCompileSlots
		if text, sources, _, err := emitter.compileSQLRelationWithin(from, "from", outer); !errors.Is(err, errSQLiteCompileBudget) || text != "" || sources != nil {
			t.Fatal("exhausted relation leaked partial SQL")
		}
	}
	emitter := newSQLiteEmitter(true)
	emitter.budget.slots = sqliteCompileSlots
	if text, err := emitter.compileSQLJoinOn([]dal.Condition{equal}, map[string]struct{}{"n": {}, "x": {}}, "from"); !errors.Is(err, errSQLiteCompileBudget) || text != "" {
		t.Fatal("exhausted ON staging leaked SQL")
	}
	for _, condition := range []dal.Condition{dal.NewComparison(dal.Field("value"), dal.In, dal.Array{Value: []any{1}}), dal.NewGroupCondition(dal.And, dal.WhereField("value", dal.Equal, 1))} {
		emitter := newSQLiteEmitter(true)
		emitter.budget.slots = sqliteCompileSlots
		if _, isArray := condition.(dal.Comparison); isArray {
			emitter.budget.slots--
		}
		if text, args, err := emitter.compileSQLConditionWithSources(condition, nil, false); !errors.Is(err, errSQLiteCompileBudget) || text != "" || args != nil {
			t.Fatal("exhausted predicate staging leaked SQL")
		}
	}
	emitter = newSQLiteEmitter(true)
	emitter.budget.nodes = sqliteCompileNodes
	if text, _, _, err := emitter.compileSQLRelationWithin(from, "from", nil); text != "" || err == nil {
		t.Fatal("exhausted relation work budget ignored")
	}
	if text, err := emitter.compileSQLJoinOn([]dal.Condition{equal}, nil, "from"); text != "" || err == nil {
		t.Fatal("exhausted ON work budget ignored")
	}
	if text, args, err := emitter.compileSQLConditionWithSources(equal, nil, false); text != "" || args != nil || err == nil {
		t.Fatal("exhausted WHERE work budget ignored")
	}
	if text, args, err := emitter.compileSQLExpressionWithSources(dal.Field("value"), nil, false); text != "" || args != nil || err == nil {
		t.Fatal("exhausted expression work budget ignored")
	}
	for _, quoted := range []bool{false, true} {
		emitter := newSQLiteEmitter(true)
		emitter.budget.bytes = sqliteCompileBytes
		if quoted {
			if _, err := emitter.quoteCheckedIdentifier(positionField, "a`b"); err == nil {
				t.Fatal("quoted identifier copy escaped budget")
			}
		} else if emitter.quoteIdentifier("a`b") != "" {
			t.Fatal("identifier copy escaped budget")
		}
	}
	for _, value := range []any{[]any{struct{}{}}, []any{strings.Repeat("x", sqliteCompileBytes+1)}} {
		if _, err := newSQLiteEmitter(true).arrayValues(value); err == nil {
			t.Fatal("array copied before payload sizing")
		}
	}
	emitter = newSQLiteEmitter(true)
	emitter.budget.slots = sqliteCompileSlots
	if _, err := emitter.arrayValues([]any{1}); err == nil {
		t.Fatal("array copied before slot sizing")
	}
	if newSQLiteEmitter(true).repeatArgs([]any{struct{}{}}, 1) != nil {
		t.Fatal("opaque argument copied")
	}
	calls := 0
	execute := func(context.Context, string, ...any) (*sql.Rows, error) {
		calls++
		return nil, errors.New("unexpected")
	}
	if reader, err := getRecordsetReaderWithOptions(context.Background(), budgetQuery(dal.Constant{Value: strings.Repeat("x", sqliteCompileBytes+1)}), execute, DbOptions{StructuredQueryDialect: "sqlite", SQLiteStrictNumbers: true}); reader != nil || err == nil || calls != 0 {
		t.Fatal("preflight failure reached native execution")
	}
}
