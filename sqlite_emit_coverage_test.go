package dalgo2sql

import (
	"math"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

type dummyCustomCondition struct{}

func (dummyCustomCondition) String() string { return "custom" }

func TestSQLiteEmit_ClausesCoverage(t *testing.T) {
	t.Run("groupBy_multiple_and_error", func(t *testing.T) {
		// Multiple GroupBy
		q := dal.From(dal.NewRootCollectionRef("t", "")).NewQuery().
			GroupBy(dal.NewFieldRef("", "a"), dal.NewFieldRef("", "b")).
			SelectColumns()
		sql, _, err := compileStructuredSQL(q)
		if err != nil || !strings.Contains(sql, "GROUP BY") || !strings.Contains(sql, ", ") {
			t.Fatalf("unexpected sql: %s, err: %v", sql, err)
		}

		// GroupBy expression error (unqualified in join)
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id")),
		)
		qErr := from.NewQuery().
			GroupBy(dal.NewFieldRef("", "unqualified")).
			SelectColumns(dal.Column{Expression: dal.NewAggregate(dal.COUNT, false, dal.NewFieldRef("a", "id"))})
		_, _, err = compileStructuredSQL(qErr)
		if err == nil || !strings.Contains(err.Error(), "groupBy 0") {
			t.Fatalf("expected groupBy error, got %v", err)
		}
	})

	t.Run("having_error", func(t *testing.T) {
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id")),
		)
		// Aggregate with unknown source inside join
		agg := dal.NewAggregate(dal.COUNT, false, dal.NewFieldRef("missing_alias", "col"))
		q := from.NewQuery().Having(dal.NewComparison(agg, dal.Equal, dal.NewConstant(1))).SelectColumns()
		_, _, err := compileStructuredSQL(q)
		if err == nil || !strings.Contains(err.Error(), "having:") {
			t.Fatalf("expected having error, got %v", err)
		}
	})

	t.Run("orderBy_multiple_error_and_values", func(t *testing.T) {
		// Multiple OrderBy
		q := dal.From(dal.NewRootCollectionRef("t", "")).NewQuery().
			OrderBy(dal.Ascending(dal.NewFieldRef("", "a")), dal.Descending(dal.NewFieldRef("", "b"))).
			SelectColumns()
		sql, _, err := compileStructuredSQL(q)
		if err != nil || !strings.Contains(sql, "ORDER BY (`a` COLLATE BINARY), (`b` COLLATE BINARY) DESC") {
			t.Fatalf("unexpected sql: %s, err: %v", sql, err)
		}

		// OrderBy error in join
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id")),
		)
		qErr := from.NewQuery().OrderBy(dal.Ascending(dal.NewFieldRef("", "unqualified"))).SelectColumns()
		_, _, err = compileStructuredSQL(qErr)
		if err == nil || !strings.Contains(err.Error(), "orderBy 0") {
			t.Fatalf("expected orderBy error, got %v", err)
		}

		// OrderBy values not supported (constant without aggregation)
		qVal := dal.From(dal.NewRootCollectionRef("t", "")).NewQuery().
			OrderBy(dal.Ascending(dal.NewConstant("val"))).
			SelectColumns()
		_, _, err = compileStructuredSQL(qVal)
		if err == nil || !strings.Contains(err.Error(), "values are not supported") {
			t.Fatalf("expected values not supported error, got %v", err)
		}
	})
}

func TestSQLiteEmit_RelationCoverage(t *testing.T) {
	t.Run("compileSQLRelation_nil_from", func(t *testing.T) {
		_, _, _, err := compileSQLRelation(nil, "from")
		if err == nil || !strings.Contains(err.Error(), "relation requires a source") {
			t.Fatalf("expected nil relation error, got %v", err)
		}
	})

	t.Run("join_unsupported_type", func(t *testing.T) {
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "b"), dal.JoinRight, sqlJoin("a", "id", "b", "id")),
		)
		_, _, _, err := compileSQLRelation(from, "from")
		if err == nil || !strings.Contains(err.Error(), "SQLite supports INNER and LEFT joins") {
			t.Fatalf("expected unsupported join type error, got %v", err)
		}
	})

	t.Run("join_nil_source", func(t *testing.T) {
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(nil, dal.JoinInner, sqlJoin("a", "id", "b", "id")),
		)
		_, _, _, err := compileSQLRelation(from, "from")
		if err == nil || !strings.Contains(err.Error(), "relation requires a source") {
			t.Fatalf("expected nil source error, got %v", err)
		}
	})

	t.Run("join_duplicate_alias", func(t *testing.T) {
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "a"), dal.JoinInner, sqlJoin("a", "id", "a", "id")),
		)
		_, _, _, err := compileSQLRelation(from, "from")
		if err == nil || !strings.Contains(err.Error(), "duplicate source alias") {
			t.Fatalf("expected duplicate alias error, got %v", err)
		}
	})

	t.Run("join_on_error", func(t *testing.T) {
		// Empty ON clause
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "b"), dal.JoinInner),
		)
		_, _, _, err := compileSQLRelation(from, "from")
		if err == nil || !strings.Contains(err.Error(), "ON must not be empty") {
			t.Fatalf("expected empty ON error, got %v", err)
		}
	})

	t.Run("rejectSQLCorrelatedJoinOn_non_comparison", func(t *testing.T) {
		err := rejectSQLCorrelatedJoinOn([]dal.Condition{dummyCustomCondition{}}, nil, "from.joins[0].on")
		if err != nil {
			t.Fatalf("expected nil error for non-comparison condition, got %v", err)
		}
	})
}

func TestSQLiteEmit_JoinOnAndConditionCoverage(t *testing.T) {
	t.Run("compileSQLJoinOn_right_error_and_args", func(t *testing.T) {
		sources := map[string]struct{}{"a": {}, "b": {}}

		// Right operand error (unknown source)
		condErr := dal.NewComparison(dal.NewFieldRef("a", "id"), dal.Equal, dal.NewFieldRef("unknown", "id"))
		_, err := compileSQLJoinOn([]dal.Condition{condErr}, sources, "join.on")
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("expected right operand error, got %v", err)
		}

		// Operand contains argument (e.g. constant)
		condArg := dal.NewComparison(dal.NewFieldRef("a", "id"), dal.Equal, dal.NewConstant(1))
		_, err = compileSQLJoinOn([]dal.Condition{condArg}, sources, "join.on")
		if err == nil || !strings.Contains(err.Error(), "must be qualified fields") {
			t.Fatalf("expected qualified fields error, got %v", err)
		}
	})

	t.Run("compileSQLCondition_with_alias", func(t *testing.T) {
		cond := dal.NewComparison(dal.NewFieldRef("t", "id"), dal.Equal, dal.NewConstant(1))
		sql, args, err := compileSQLCondition(cond, "t")
		if err != nil || len(args) != 1 || !strings.Contains(sql, "`t`.`id`") {
			t.Fatalf("unexpected result: sql=%s, args=%v, err=%v", sql, args, err)
		}
	})

	t.Run("comparison_in_errors", func(t *testing.T) {
		sources := map[string]struct{}{"t": {}}

		// IN left operand contains parameters
		leftParam := dal.BinaryExpression{
			Left:     dal.NewAggregate(dal.COUNT, false, dal.NewFieldRef("t", "id")),
			Operator: dal.Add,
			Right:    dal.NewConstant(1),
		}
		condParam := dal.NewComparison(leftParam, dal.In, dal.Array{Value: []any{1}})
		_, _, err := compileSQLConditionWithSources(condParam, sources, false)
		if err == nil || !strings.Contains(err.Error(), "left operand must not contain parameters") {
			t.Fatalf("expected left param error, got %v", err)
		}

		// IN right operand not slice
		condNotSlice := dal.NewComparison(dal.NewFieldRef("t", "id"), dal.In, dal.Array{Value: "not-a-slice"})
		_, _, err = compileSQLConditionWithSources(condNotSlice, sources, false)
		if err == nil || !strings.Contains(err.Error(), "IN array has type") {
			t.Fatalf("expected slice error, got %v", err)
		}

		// IN right operand invalid number
		condNaN := dal.NewComparison(dal.NewFieldRef("t", "id"), dal.In, dal.Array{Value: []any{math.NaN()}})
		_, _, err = compileSQLConditionWithSources(condNaN, sources, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported portable number") {
			t.Fatalf("expected NaN error, got %v", err)
		}

		// Regular comparison with invalid number
		condCmpNaN := dal.NewComparison(dal.NewFieldRef("t", "id"), dal.GreaterThen, dal.NewConstant(math.NaN()))
		_, _, err = compileSQLConditionWithSources(condCmpNaN, sources, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported portable number") {
			t.Fatalf("expected NaN comparison error, got %v", err)
		}
	})

	t.Run("sqlExpressionContainsAggregate_binary", func(t *testing.T) {
		bin := dal.BinaryExpression{
			Left:     dal.NewAggregate(dal.COUNT, false, dal.NewFieldRef("", "id")),
			Operator: dal.Add,
			Right:    dal.NewFieldRef("", "x"),
		}
		if !sqlExpressionContainsAggregate(bin) {
			t.Fatal("expected contains aggregate to be true")
		}
	})

	t.Run("normalizedNumber_string_error", func(t *testing.T) {
		_, err := normalizedNumber("not-a-number")
		if err == nil || !strings.Contains(err.Error(), "unsupported portable number") {
			t.Fatalf("expected unmarshal error, got %v", err)
		}
	})

	t.Run("compileSQLExpression_with_alias", func(t *testing.T) {
		sql, args, err := compileSQLExpression(dal.NewFieldRef("t", "id"), "t")
		if err != nil || len(args) != 0 || sql != "`t`.`id`" {
			t.Fatalf("unexpected: sql=%s, args=%v, err=%v", sql, args, err)
		}
	})
}

func TestSQLiteEmit_ExpressionsCoverage(t *testing.T) {
	sources := map[string]struct{}{"t": {}}

	t.Run("aggregate_arity_and_error", func(t *testing.T) {
		// 0 args
		zeroArgs := dal.NewAggregate(dal.COUNT, false)
		_, _, err := compileSQLExpressionWithSources(zeroArgs, sources, false)
		if err == nil || !strings.Contains(err.Error(), "requires exactly one argument") {
			t.Fatalf("expected arg count error, got %v", err)
		}

		// Arg error in aggregate
		argErr := dal.NewAggregate(dal.COUNT, false, dal.NewFieldRef("unknown", "id"))
		_, _, err = compileSQLExpressionWithSources(argErr, sources, true)
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("expected arg error, got %v", err)
		}

		// Unknown aggregate name
		unknownAgg := dal.NewAggregate("MEDIAN", false, dal.NewFieldRef("t", "id"))
		_, _, err = compileSQLExpressionWithSources(unknownAgg, sources, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported aggregate") {
			t.Fatalf("expected unsupported aggregate error, got %v", err)
		}
	})

	t.Run("binary_expression_errors", func(t *testing.T) {
		// Left operand error
		binLeftErr := dal.BinaryExpression{Left: dal.NewFieldRef("unknown", "id"), Operator: dal.Add, Right: dal.NewFieldRef("t", "id")}
		_, _, err := compileSQLExpressionWithSources(binLeftErr, sources, true)
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("expected left operand error, got %v", err)
		}

		// Right operand error
		binRightErr := dal.BinaryExpression{Left: dal.NewFieldRef("t", "id"), Operator: dal.Add, Right: dal.NewFieldRef("unknown", "id")}
		_, _, err = compileSQLExpressionWithSources(binRightErr, sources, true)
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("expected right operand error, got %v", err)
		}

		// Unsupported operator
		binOpErr := dal.BinaryExpression{Left: dal.NewFieldRef("t", "id"), Operator: "%", Right: dal.NewFieldRef("t", "id")}
		_, _, err = compileSQLExpressionWithSources(binOpErr, sources, false)
		if err == nil || !strings.Contains(err.Error(), "unsupported arithmetic operator") {
			t.Fatalf("expected unsupported operator error, got %v", err)
		}
	})

	t.Run("rewriteSQLConditionAliases_default", func(t *testing.T) {
		cond := dummyCustomCondition{}
		res := rewriteSQLConditionAliases(cond, nil)
		if res != cond {
			t.Fatalf("expected unchanged condition, got %v", res)
		}
	})
}
