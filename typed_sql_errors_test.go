package dalgo2sql

import (
	"errors"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type fakeTypedCondition struct{}

func (fakeTypedCondition) String() string { return "fake" }

func TestCompileTypedSQLRefusesUnsupportedNodesWithErrNotSupported(t *testing.T) {
	album := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	whereBy := func(c dal.Condition) dal.StructuredQuery { return album().Where(c).SelectColumns() }
	selectOf := func(e dal.Expression) dal.StructuredQuery {
		return album().SelectColumns(typedTestColumn(e, "x"))
	}
	stableFirstLast := newFakeTypedDialect()
	stableFirstLast.caps.StableRowOrder = true
	stableFirstLast.caps.Aggregate.First = true
	stableFirstLast.caps.Aggregate.Last = true
	noMin := newFakeTypedDialect()
	noMin.caps.Aggregate.Min = false
	noGroupBy := newFakeTypedDialect()
	noGroupBy.caps.GroupBy = false
	parent := record.NewKeyWithID("Parent", "p1")
	bucket := dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1))
	bucketColumns := []dal.Column{typedTestColumn(bucket, "bucket"), typedTestColumn(dal.NewAggregate(dal.COUNT, false, dal.Star()), "n")}
	scanned := dal.NewRootCollectionRef("Invoice", "i").WithScan(10, dal.Descending(typedTestField("Total")))
	scanLimitOnly := dal.NewRootCollectionRef("Invoice", "i").WithScan(10)
	scanOrderOnly := dal.NewRootCollectionRef("Customer", "c").WithScan(0, dal.Ascending(typedTestField("Name")))
	plainCustomer := dal.NewRootCollectionRef("Customer", "c")
	customerJoin := typedTestJoinOn("i", "CustomerId", "c", "CustomerId")
	joinedTo := func(base dal.RecordsetSource, joined dal.RecordsetSource) dal.StructuredQuery {
		return dal.From(base).Join(dal.NewJoinedSource(joined, dal.JoinInner, customerJoin)).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("c", "Name"), ""))
	}

	cases := []struct {
		name     string
		query    dal.StructuredQuery
		dialect  typedDialect
		fragment string
	}{
		{"start from cursor", album().StartFrom("c").SelectColumns(), nil, "cursors"},
		{"start after cursor", album().StartAfter("c").SelectColumns(), nil, "cursors"},
		{"subquery source", dal.From(dal.NewQuerySource(album().SelectColumns(), "s")).NewQuery().SelectColumns(), nil, "subqueries"},
		{"FIRST without a stable order", album().SelectColumns(dal.FirstAs(typedTestField("a"), "f")), nil, "stable"},
		{"LAST without a stable order", album().SelectColumns(dal.LastAs(typedTestField("a"), "l")), nil, "stable"},
		{"FIRST even when the dialect claims support", album().SelectColumns(dal.FirstAs(typedTestField("a"), "f")), stableFirstLast, "FIRST"},
		{"LAST even when the dialect claims support", album().SelectColumns(dal.LastAs(typedTestField("a"), "l")), stableFirstLast, "LAST"},
		{"aggregate the dialect cannot run natively", album().SelectColumns(dal.MinAs(typedTestField("a"), "m")), noMin, "natively"},
		{"grouping the dialect cannot run natively", album().GroupBy(typedTestField("a")).SelectColumns(), noGroupBy, "natively"},
		{"right join", typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinRight, typedTestJoinOn("a", "x", "r", "y"))).NewQuery().SelectColumns(), nil, "join_type"},
		{"non-equality join", typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, dal.NewComparison(typedTestQualified("a", "x"), dal.GreaterThen, typedTestQualified("r", "y")))).NewQuery().SelectColumns(), nil, "join_operator"},
		{"collection group source", dal.From(dal.NewCollectionGroupRef("Album", "")).NewQuery().SelectColumns(), nil, "source"},
		{"parented collection", dal.From(dal.NewCollectionRef("Child", "", parent)).NewQuery().SelectColumns(), nil, "parented"},
		{"nil collection pointer", dal.From((*dal.CollectionRef)(nil)).NewQuery().SelectColumns(), nil, "source"},
		{"unqualified field in a join", typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "x", "r", "y"))).NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), "")), nil, "must name a source"},
		{"param expression", whereBy(dal.NewComparison(typedTestField("a"), dal.Equal, dal.NewParam("p"))), nil, "expression"},
		{"array outside IN", whereBy(dal.NewComparison(typedTestField("a"), dal.Equal, dal.NewArray([]int{1}))), nil, "expression"},
		{"nil select expression", album().SelectColumns(dal.Column{}), nil, "expression"},
		{"unsupported arithmetic operator", selectOf(dal.Binary(typedTestField("a"), dal.ArithmeticOperator("%"), typedTestField("b"))), nil, "arithmetic operator"},
		{"unsupported value type", whereBy(typedTestEq(typedTestField("a"), struct{}{})), nil, "SQL value type"},
		{"unsupported value type in a list", whereBy(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray([]any{1, map[string]int{}}))), nil, "SQL value type"},
		{"unaliased constant carrying expression", album().SelectColumns(typedTestColumn(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)), "")), nil, "needs an alias"},
		{"comparison with a constant on the left", whereBy(dal.NewComparison(typedTestConst(1), dal.Equal, typedTestField("a"))), nil, "on the left"},
		{"array membership of a stored array", album().WhereInArrayField("tags", "x").SelectColumns(), nil, "on the left"},
		{"unsupported comparison operator", whereBy(dal.NewComparison(typedTestField("a"), dal.Operator("like"), typedTestConst("x"))), nil, "comparison operator"},
		{"IN needs an array", whereBy(dal.NewComparison(typedTestField("a"), dal.In, typedTestConst(1))), nil, "array"},
		{"NOT IN needs an array", whereBy(dal.NewComparison(typedTestField("a"), dal.NotIn, typedTestField("b"))), nil, "array"},
		{"IN over a non-slice array", whereBy(dal.NewComparison(typedTestField("a"), dal.In, dal.Array{Value: 5})), nil, "array has type"},
		{"IN over a nil array", whereBy(dal.NewComparison(typedTestField("a"), dal.In, dal.Array{})), nil, "array has type"},
		{"IN over too many values", whereBy(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray(make([]int, typedMaxArguments+1)))), nil, "too many"},
		{"unsupported group operator", whereBy(dal.NewGroupCondition(dal.Operator("XOR"), typedTestEq(typedTestField("a"), 1))), nil, "group operator"},
		{"unsupported condition type", whereBy(fakeTypedCondition{}), nil, "condition"},
		{"nil child condition", whereBy(dal.NewGroupCondition(dal.And, nil)), nil, "condition"},
		{"IS NULL without an operand", whereBy(dal.NewIsNullCondition(nil)), nil, "expression"},
		{"IS NULL of a constant", whereBy(dal.NewIsNullCondition(typedTestConst(1))), nil, "constant"},
		{"constant-only GROUP BY", album().GroupBy(typedTestConst(1)).SelectColumns(dal.Count()), nil, "constant"},
		{"constant-only ORDER BY", album().OrderBy(dal.Ascending(typedTestConst(1))).SelectColumns(), nil, "constant"},
		{
			"HAVING over a grouped alias that carries a constant",
			album().GroupBy(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1))).
				Having(dal.NewComparison(typedTestField("bucket"), dal.GreaterThen, typedTestConst(2))).
				SelectColumns(typedTestColumn(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)), "bucket"), dal.CountAs(typedTestField("a"), "n")),
			nil, "grouped expression",
		},
		{
			"ORDER BY over a grouped alias nested in a larger expression",
			album().GroupBy(bucket).
				OrderBy(dal.Ascending(dal.Binary(typedTestField("bucket"), dal.Multiply, typedTestConst(2)))).
				SelectColumns(bucketColumns...),
			nil, "ORDER BY refers to \"bucket\"",
		},
		{
			"ORDER BY over a grouped alias added to an aggregate",
			album().GroupBy(bucket).
				OrderBy(dal.Ascending(dal.Binary(typedTestField("bucket"), dal.Add, typedTestField("n")))).
				SelectColumns(bucketColumns...),
			nil, "ORDER BY refers to \"bucket\"",
		},
		{
			"ORDER BY nesting a grouped alias after an item that is fine",
			album().GroupBy(bucket).
				OrderBy(dal.Ascending(typedTestField("bucket")), dal.Ascending(dal.Binary(typedTestField("bucket"), dal.Multiply, typedTestConst(2)))).
				SelectColumns(bucketColumns...),
			nil, "orderBy 1",
		},
		{"scan-bounded base source in a join", joinedTo(scanned, plainCustomer), nil, "from: join_plan: a scan-bounded source"},
		{"scan limit alone on the base source of a join", joinedTo(scanLimitOnly, plainCustomer), nil, "scan-bounded"},
		{"scan order alone on a joined source", joinedTo(dal.NewRootCollectionRef("Invoice", "i"), scanOrderOnly), nil, "from.joins[0].from: join_plan: a scan-bounded source"},
		{"scan-bounded pointer source in a join", joinedTo(typedTestPtr(scanned), plainCustomer), nil, "scan-bounded"},
		{
			"scan-bounded source inside a nested join subtree",
			dal.From(dal.NewRootCollectionRef("Invoice", "i")).Join(
				dal.NewJoinedFrom(dal.From(dal.NewRootCollectionRef("Customer", "c")).Join(
					dal.NewJoinedSource(dal.NewRootCollectionRef("Employee", "e").WithScan(5), dal.JoinLeft, typedTestJoinOn("c", "SupportRepId", "e", "EmployeeId")),
				), dal.JoinInner, customerJoin),
			).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("c", "Name"), "")),
			nil, "from.joins[0].from.joins[0].from: join_plan: a scan-bounded source",
		},
		{"named-database base source in a join", joinedTo(dal.NewDatabaseCollectionRef("db1", "", "Invoice", "i"), plainCustomer), nil, "from: join_plan: a source in a named database"},
		{"named-database joined source", joinedTo(dal.NewRootCollectionRef("Invoice", "i"), dal.NewDatabaseCollectionRef("db2", "", "Customer", "c")), nil, "from.joins[0].from: join_plan: a source in a named database"},
		{"the document identity has no column to read", album().SelectColumns(typedTestColumn(dal.DocumentID(), "")), nil, "dal.DocumentID()"},
		{"the document identity in WHERE", album().Where(dal.NewComparison(dal.DocumentID(), dal.Equal, typedTestConst("x"))).SelectColumns(), nil, "dal.DocumentID()"},
		{"the document identity in ORDER BY", album().OrderBy(dal.Ascending(dal.DocumentID())).SelectColumns(), nil, "dal.DocumentID()"},
		{
			"two IN lists that together bind too many values",
			album().Where(dal.NewGroupCondition(dal.And,
				dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray(make([]int, 40000))),
				dal.NewComparison(typedTestField("b"), dal.In, dal.NewArray(make([]int, 40000))),
			)).SelectColumns(),
			nil, "binds 80000 values",
		},
		{
			"an IN list at the limit plus one more constant",
			album().Where(dal.NewGroupCondition(dal.And,
				dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray(make([]int, typedMaxArguments))),
				typedTestEq(typedTestField("b"), 1),
			)).SelectColumns(),
			nil, "binds 65536 values",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectTypedUnsupported(t, tc.query, tc.dialect, typedCatalogFacts{}, tc.fragment)
		})
	}
}

func TestCompileTypedSQLRejectsInvalidQueriesWithoutClaimingUnsupported(t *testing.T) {
	album := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	longName := strings.Repeat("n", 64)
	cases := []struct {
		name     string
		query    dal.StructuredQuery
		dialect  typedDialect
		fragment string
	}{
		{"no source", dal.NewQueryBuilder(nil).SelectColumns(), nil, "requires a source"},
		{"nil query", nil, nil, "requires a source"},
		{"negative limit", album().Limit(-1).SelectColumns(), nil, "non-negative"},
		{"negative offset", album().Offset(-1).SelectColumns(), nil, "non-negative"},
		{"ungrouped column", album().GroupBy(typedTestField("a")).SelectColumns(typedTestColumn(typedTestField("b"), "")), nil, "invalid aggregation"},
		{"empty join ON", typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner)).NewQuery().SelectColumns(), nil, "join_shape"},
		{"unknown source", album().SelectColumns(typedTestColumn(typedTestQualified("zzz", "a"), "")), nil, "unknown source"},
		{"empty field name", album().SelectColumns(typedTestColumn(typedTestField(""), "")), nil, "empty"},
		{"NUL in a field name", album().SelectColumns(typedTestColumn(typedTestField("a\x00b"), "")), nil, "NUL"},
		{"invalid UTF-8 in an alias", album().SelectColumns(typedTestColumn(typedTestField("a"), "a\xffb")), nil, "UTF-8"},
		{"table name over the limit", typedTestFrom(longName, "").NewQuery().SelectColumns(), nil, "limit"},
		{"schema name over the limit", dal.From(dal.NewQualifiedRootCollectionRef(longName, "t", "")).NewQuery().SelectColumns(), nil, "limit"},
		{"source alias over the limit", typedTestFrom("t", longName).NewQuery().SelectColumns(), nil, "limit"},
		{"unaliased expression whose text is too long", album().SelectColumns(typedTestColumn(dal.Binary(typedTestField(strings.Repeat("a", 40)), dal.Add, typedTestField(strings.Repeat("b", 40))), "")), nil, "limit"},
		{"aggregate in WHERE", album().Where(dal.NewComparison(dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.GreaterThen, typedTestConst(1))).SelectColumns(), nil, "aggregate"},
		{"empty condition group", album().Where(dal.NewGroupCondition(dal.And)).SelectColumns(), nil, "empty condition group"},
		{"dialect refuses a value", album().Where(typedTestEq(typedTestField("a"), fakeTypedFailingValuer{})).SelectColumns(), nil, "refuses"},
		{"dialect refuses a listed value", album().Where(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray([]any{fakeTypedFailingValuer{}}))).SelectColumns(), nil, "refuses"},
		{"bad field name inside GROUP BY", album().GroupBy(typedTestField("")).SelectColumns(dal.Count()), nil, "groupBy 0"},
		{"bad field name inside ORDER BY", album().OrderBy(dal.Ascending(typedTestField(""))).SelectColumns(), nil, "orderBy 0"},
		{"bad field name inside HAVING", album().Having(dal.NewComparison(dal.NewAggregate(dal.MIN, false, typedTestField("")), dal.GreaterThen, typedTestConst(1))).SelectColumns(typedTestColumn(dal.NewAggregate(dal.COUNT, false, dal.Star()), "n")), nil, "having"},
		{"bad field name inside WHERE", album().Where(typedTestEq(typedTestField(""), 1)).SelectColumns(), nil, "where"},
		{"bad column", album().SelectColumns(typedTestColumn(typedTestField(""), "x")), nil, "column 0"},
		{"bad nested join", typedTestFrom("Album", "a").Join(dal.NewJoinedFrom(dal.From(dal.NewRootCollectionRef(longName, "r")), dal.JoinInner, typedTestJoinOn("a", "x", "r", "y"))).NewQuery().SelectColumns(), nil, "joins[0].from"},
		{"bad IS NULL operand", album().Where(dal.NewIsNullCondition(typedTestField(""))).SelectColumns(), nil, "where"},
		{"bad aggregate argument", album().SelectColumns(typedTestColumn(dal.NewAggregate(dal.SUM, false, typedTestField("")), "s")), nil, "column 0"},
		{"bad arithmetic operand", album().SelectColumns(typedTestColumn(dal.Binary(typedTestField("a"), dal.Add, typedTestField("")), "s")), nil, "column 0"},
		{"bad left arithmetic operand", album().SelectColumns(typedTestColumn(dal.Binary(typedTestField(""), dal.Add, typedTestField("a")), "s")), nil, "column 0"},
		{"bad group condition child", album().Where(dal.NewGroupCondition(dal.And, typedTestEq(typedTestField("a"), 1), typedTestEq(typedTestField(""), 1))).SelectColumns(), nil, "where"},
		{"bad comparison right operand", album().Where(dal.NewComparison(typedTestField("a"), dal.Equal, typedTestField(""))).SelectColumns(), nil, "where"},
		{"bad IN left operand", album().Where(dal.NewComparison(typedTestField(""), dal.In, dal.NewArray([]int{1}))).SelectColumns(), nil, "where"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectTypedInvalid(t, tc.query, tc.dialect, typedCatalogFacts{}, tc.fragment)
		})
	}
}

func TestCompileTypedSQLWithoutDialect(t *testing.T) {
	text, args, err := compileTypedSQL(typedTestFrom("Album", "").NewQuery().SelectColumns(), nil, typedCatalogFacts{})
	if err == nil || !strings.Contains(err.Error(), "requires a dialect") || text != "" || args != nil {
		t.Fatalf("compileTypedSQL(nil dialect) = %q, %v, %v", text, args, err)
	}
}

func TestCompileTypedSQLKeepsTheDialectErrorInTheChain(t *testing.T) {
	q := typedTestFrom("Album", "").NewQuery().Where(typedTestEq(typedTestField("a"), fakeTypedFailingValuer{})).SelectColumns()
	_, _, err := compileTypedSQL(q, newFakeTypedDialect(), typedCatalogFacts{})
	if !errors.Is(err, errFakeTypedBind) {
		t.Fatalf("error %v does not wrap the dialect error", err)
	}
}

func TestCompileTypedSQLCatchesADialectThatBreaksTheMarkerContract(t *testing.T) {
	q := typedTestFrom("Album", "").NewQuery().Where(typedTestEq(typedTestField("a"), 1)).SelectColumns()
	t.Run("two markers for one value", func(t *testing.T) {
		dialect := newFakeTypedDialect()
		dialect.bindMarkers = "? + ?"
		_, _, err := compileTypedSQL(q, dialect, typedCatalogFacts{})
		if err == nil || !strings.Contains(err.Error(), "placeholder") {
			t.Fatalf("error = %v, want a placeholder count error", err)
		}
	})
	// The compiler verifies every identifier a dialect hands back, so no faulty
	// dialect can let a name escape its quotes into the statement.
	malformed := map[string]string{
		"an opening quote only":      `"Album`,
		"an undoubled inner quote":   `"Al"bum"`,
		"a NUL byte":                 "\"Al\x00bum\"",
		"nothing between the quotes": `""`,
		"no quotes at all":           `Album`,
		"a lone quote":               `"`,
		"an escaped closing quote":   `"Album""`,
	}
	for name, quoted := range malformed {
		t.Run("an identifier with "+name, func(t *testing.T) {
			dialect := newFakeTypedDialect()
			dialect.quoteOverride = func(string) (string, error) { return quoted, nil }
			text, args, err := compileTypedSQL(typedTestFrom("Album", "").NewQuery().SelectColumns(), dialect, typedCatalogFacts{})
			if err == nil || !strings.Contains(err.Error(), "malformed quoted identifier") || text != "" || args != nil {
				t.Fatalf("compileTypedSQL() = %q, %v, %v; want a malformed quoted identifier error", text, args, err)
			}
		})
	}
	t.Run("a dialect error from quoteIdent is passed through", func(t *testing.T) {
		boom := errors.New("quote failed")
		dialect := newFakeTypedDialect()
		dialect.quoteOverride = func(string) (string, error) { return "", boom }
		if _, _, err := compileTypedSQL(typedTestFrom("Album", "").NewQuery().SelectColumns(), dialect, typedCatalogFacts{}); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want the dialect's error", err)
		}
	})
}

// TestCompileTypedSQLRefusesADialectThatBreaksTheOperandRules proves the two
// rules numbered in the typedDialect contract. Both leave the marker count right,
// so the final count check alone passes them and binds values to wrong places.
func TestCompileTypedSQLRefusesADialectThatBreaksTheOperandRules(t *testing.T) {
	album := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	// (a + 1) / (b - 2): both operands carry a value.
	dividedValues := album().SelectColumns(typedTestColumn(dal.Binary(
		dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)), dal.Divide,
		dal.Binary(typedTestField("b"), dal.Subtract, typedTestConst(2))), "r"))
	// a / b: neither operand carries a value.
	dividedFields := album().SelectColumns(typedTestColumn(dal.Binary(typedTestField("a"), dal.Divide, typedTestField("b")), "r"))
	sumOfProduct := album().SelectColumns(dal.SumAs(dal.Binary(typedTestField("a"), dal.Multiply, typedTestConst(2)), "s"))
	orderedByValue := album().OrderBy(dal.Ascending(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)))).SelectColumns()
	inEmpty := album().Where(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray([]int{}))).SelectColumns()
	limited := album().Limit(5).SelectColumns()

	cases := []struct {
		name     string
		break_   func(*fakeTypedDialect)
		query    dal.StructuredQuery
		fragment string
	}{
		{
			"divide renders the right operand before the left",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + right + " / " + left + ")" }
			},
			dividedValues, "verbatim and in argument order",
		},
		{
			"divide renders the left operand twice",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + " / " + right + " + " + left + ")" }
			},
			dividedValues, "the dialect wrote",
		},
		{
			"divide drops the right operand",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + ")" }
			},
			dividedValues, "the dialect wrote",
		},
		{
			"divide rewrites an operand that carries a value",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string {
					return "(" + strings.Replace(left, "?::bigint", "?", 1) + " / " + right + ")"
				}
			},
			dividedValues, "verbatim and in argument order",
		},
		{
			"aggregateResult renders the aggregate twice",
			func(d *fakeTypedDialect) {
				d.aggregateOverride = func(function, aggregate string) string { return "(" + aggregate + " + " + aggregate + ")" }
			},
			sumOfProduct, "the dialect wrote",
		},
		{
			"aggregateResult rewrites the aggregate",
			func(d *fakeTypedDialect) {
				d.aggregateOverride = func(function, aggregate string) string { return strings.ToLower(aggregate) }
			},
			sumOfProduct, "verbatim and in argument order",
		},
		{
			"orderItem renders the expression twice",
			func(d *fakeTypedDialect) {
				d.orderItemOverride = func(expression string, descending, notNull bool) string { return expression + ", " + expression }
			},
			orderedByValue, "the dialect wrote",
		},
		{
			"emptyIn writes a marker",
			func(d *fakeTypedDialect) { d.emptyInOverride = func(bool) string { return "(? IS NULL)" } },
			inEmpty, "the dialect wrote",
		},
		{
			"limitOffset writes fewer markers than arguments",
			func(d *fakeTypedDialect) {
				d.limitOffsetOverride = func(limit, offset int) (string, []any) { return "LIMIT ?", []any{limit, offset} }
			},
			limited, "the dialect wrote",
		},
		{
			"orderItem rewrites the expression it was given",
			func(d *fakeTypedDialect) {
				d.orderItemOverride = func(expression string, descending, notNull bool) string {
					return strings.Replace(expression, "?::bigint", "?", 1) + " ASC"
				}
			},
			orderedByValue, "verbatim and in argument order",
		},
		{
			"bind writes two markers for one value",
			func(d *fakeTypedDialect) { d.bindMarkers = "? + ?" },
			orderedByValue, "the dialect wrote 2 placeholders where 1 belong",
		},
		{
			"divide adds a string literal that holds a marker",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + " / " + right + " + '?')" }
			},
			dividedFields, "the dialect wrote",
		},
		{
			"divide adds a string literal",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + " / " + right + " + 'x')" }
			},
			dividedFields, "string literal",
		},
		{
			"divide adds a line comment",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + " / " + right + ") -- x" }
			},
			dividedFields, "comment",
		},
		{
			"divide adds a block comment",
			func(d *fakeTypedDialect) {
				d.divideOverride = func(left, right string) string { return "(" + left + " / " + right + ") /* x */" }
			},
			dividedFields, "comment",
		},
		{
			"orderItem adds a string literal",
			func(d *fakeTypedDialect) {
				d.orderItemOverride = func(expression string, descending, notNull bool) string { return expression + " ASC, 'x'" }
			},
			orderedByValue, "string literal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialect := newFakeTypedDialect()
			tc.break_(dialect)
			text, args, err := compileTypedSQL(tc.query, dialect, typedCatalogFacts{})
			if err == nil {
				t.Fatalf("compileTypedSQL() = %q, %v; want the dialect refused", text, args)
			}
			if errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("error %q is a dialect fault, not a query the engine declines", err)
			}
			if text != "" || args != nil {
				t.Fatalf("a refused query must return no SQL, got %q %v", text, args)
			}
			if !strings.Contains(err.Error(), tc.fragment) {
				t.Fatalf("error %q does not mention %q", err, tc.fragment)
			}
		})
	}

	// Rule 1 binds only operands that carry a value; one that does not may be
	// repeated, which a CASE form of divide does.
	t.Run("a dialect may repeat or move an operand that carries no value", func(t *testing.T) {
		dialect := newFakeTypedDialect()
		dialect.divideOverride = func(left, right string) string {
			return "(CASE WHEN " + right + " = 0 THEN NULL ELSE " + left + " / " + right + " END)"
		}
		typedGolden{
			dialect: dialect,
			query:   dividedFields,
			wantSQL: `SELECT (CASE WHEN "b" = 0 THEN NULL ELSE "a" / "b" END) AS "r" FROM "Album"`,
		}.run(t)
	})
	t.Run("a dialect that wraps operands without changing them is accepted", func(t *testing.T) {
		typedGolden{
			query:    dividedValues,
			wantSQL:  `SELECT (CAST(("a" + $1::bigint) AS double precision) / NULLIF(CAST(("b" - $2::bigint) AS double precision), 0)) AS "r" FROM "Album"`,
			wantArgs: []any{1, 2},
		}.run(t)
	})
}

// TestCompileTypedSQLCatchesFaultsWhoseMarkerCountsCancel shows why each fragment
// is checked where it is written and not only by the final count: emptyIn gains a
// marker and limitOffset loses one, so the totals match the arguments and the
// numbering pass alone would accept a statement that binds the LIMIT value to
// the marker emptyIn invented.
func TestCompileTypedSQLCatchesFaultsWhoseMarkerCountsCancel(t *testing.T) {
	q := typedTestFrom("Album", "").NewQuery().
		Where(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray([]int{}))).
		Limit(5).SelectColumns()
	dialect := newFakeTypedDialect()
	dialect.emptyInOverride = func(bool) string { return "(? IS NULL)" }
	dialect.limitOffsetOverride = func(limit, offset int) (string, []any) { return "LIMIT 5", []any{limit} }

	// Without the per-fragment checks this text passes the final count.
	text := `SELECT * FROM "Album" WHERE (? IS NULL) LIMIT 5`
	if _, err := numberTypedPlaceholders(text, dialect.placeholderStyle(), 1); err != nil {
		t.Fatalf("the cancelling faults were meant to pass the final count, got %v", err)
	}
	gotText, gotArgs, err := compileTypedSQL(q, dialect, typedCatalogFacts{})
	if err == nil || !strings.Contains(err.Error(), "the dialect wrote") || gotText != "" || gotArgs != nil {
		t.Fatalf("compileTypedSQL() = %q, %v, %v; want a refusal naming the faulty fragment", gotText, gotArgs, err)
	}
}

func TestCompileTypedSQLCheckedJoinKeyTypes(t *testing.T) {
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Album"}: {Columns: []typedColumnFact{
			{Name: "ArtistId", DataType: "int4", Category: typedTypeNumber},
			{Name: "Guid", DataType: "uuid", Category: typedTypeOther},
			{Name: "Mystery", DataType: "mystery", Category: typedTypeUnknown},
			{Name: "Blank", Category: typedTypeUnknown},
		}},
		{Name: "Artist"}: {Columns: []typedColumnFact{
			{Name: "ArtistId", DataType: "int8", Category: typedTypeNumber},
			{Name: "Name", DataType: "text", Category: typedTypeText},
			{Name: "Guid", DataType: "uuid", Category: typedTypeOther},
			{Name: "Doc", DataType: "json", Category: typedTypeOther},
			{Name: "Mystery", DataType: "mystery", Category: typedTypeUnknown},
		}},
	}}
	join := func(albumField, artistField string) dal.StructuredQuery {
		return typedTestFrom("Album", "a").Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", albumField, "r", artistField)),
		).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("r", "Name"), ""))
	}
	accepted := []struct{ name, album, artist string }{
		{"same category different widths", "ArtistId", "ArtistId"},
		{"same non-scalar type", "Guid", "Guid"},
		{"same unknown type name", "Mystery", "Mystery"},
		{"column missing from the facts is left to the server", "Missing", "ArtistId"},
		{"other side missing from the facts", "ArtistId", "Missing"},
	}
	for _, tc := range accepted {
		t.Run("accepts "+tc.name, func(t *testing.T) {
			if _, _, err := compileTypedSQL(join(tc.album, tc.artist), newFakeTypedDialect(), facts); err != nil {
				t.Fatalf("compileTypedSQL() error = %v", err)
			}
		})
	}
	refused := []struct{ name, album, artist string }{
		{"number against text", "ArtistId", "Name"},
		{"two different non-scalar types", "Guid", "Doc"},
		{"unknown against known", "Mystery", "Name"},
		{"unknown without a type name", "Blank", "Mystery"},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			expectTypedUnsupported(t, join(tc.album, tc.artist), nil, facts, "join_plan")
		})
	}
	t.Run("without facts nothing is checked", func(t *testing.T) {
		if _, _, err := compileTypedSQL(join("ArtistId", "Name"), newFakeTypedDialect(), typedCatalogFacts{}); err != nil {
			t.Fatalf("compileTypedSQL() error = %v", err)
		}
	})
}

func typedTestAlbumArtistSources() map[string]typedSource {
	return map[string]typedSource{
		"a": {quoted: `"a"`, table: typedSourceName{Name: "Album"}},
		"r": {quoted: `"r"`, table: typedSourceName{Name: "Artist"}},
	}
}

func TestTypedAliasRewriterLeavesUnknownConditionsAlone(t *testing.T) {
	rewriter := typedAliasRewriter{aliases: map[string]dal.Expression{"x": typedTestField("y")}}
	if got := rewriter.condition(fakeTypedCondition{}); got != (fakeTypedCondition{}) {
		t.Fatalf("condition() = %v, want the condition unchanged", got)
	}
}

func TestTypedCompilerDirectGuards(t *testing.T) {
	newCompiler := func() *typedCompiler {
		return &typedCompiler{dialect: newFakeTypedDialect(), sources: typedTestAlbumArtistSources()}
	}
	t.Run("an aggregate name outside the whitelist is never emitted", func(t *testing.T) {
		_, _, err := newCompiler().aggregate(dal.NewAggregate("MEDIAN; DROP TABLE x", false, typedTestField("a")))
		if !errors.Is(err, dal.ErrNotSupported) || strings.Contains(err.Error(), "DROP") {
			t.Fatalf("error = %v, want ErrNotSupported without echoing the name", err)
		}
	})
	t.Run("an aggregate needs exactly one argument", func(t *testing.T) {
		_, _, err := newCompiler().aggregate(dal.NewAggregate(dal.SUM, false))
		if err == nil || errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("error = %v, want a plain arity error", err)
		}
	})
	joinOnCases := []struct {
		name       string
		conditions []dal.Condition
	}{
		{"empty", nil},
		{"not a comparison", []dal.Condition{fakeTypedCondition{}}},
		{"not an equality", []dal.Condition{dal.NewComparison(typedTestQualified("a", "x"), dal.LessThen, typedTestQualified("r", "y"))}},
		{"left operand not a field", []dal.Condition{dal.NewComparison(typedTestConst(1), dal.Equal, typedTestQualified("r", "y"))}},
		{"right operand not a field", []dal.Condition{dal.NewComparison(typedTestQualified("a", "x"), dal.Equal, typedTestConst(1))}},
	}
	for _, tc := range joinOnCases {
		t.Run("ON "+tc.name, func(t *testing.T) {
			_, err := newCompiler().joinOn(tc.conditions, typedTestAlbumArtistSources(), nil, "from.joins[0].on")
			if !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("error = %v, want ErrNotSupported", err)
			}
		})
	}
	emptyFieldCases := map[string]dal.Condition{
		"left":  typedTestJoinOn("a", "", "r", "y"),
		"right": typedTestJoinOn("a", "x", "r", ""),
	}
	for side, condition := range emptyFieldCases {
		t.Run("ON "+side+" field with an empty name", func(t *testing.T) {
			_, err := newCompiler().joinOn([]dal.Condition{condition}, typedTestAlbumArtistSources(), nil, "from.joins[0].on")
			if err == nil || errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "from.joins[0].on[0]."+side) {
				t.Fatalf("error = %v, want a plain path-qualified error", err)
			}
		})
	}
	t.Run("ON field outside the visible scope", func(t *testing.T) {
		_, err := newCompiler().joinOn([]dal.Condition{typedTestJoinOn("a", "x", "zzz", "y")}, map[string]typedSource{"a": {quoted: `"a"`, table: typedSourceName{Name: "Album"}}}, nil, "from.joins[0].on")
		if err == nil || !strings.Contains(err.Error(), "from.joins[0].on[0].right") {
			t.Fatalf("error = %v, want a path-qualified scope error", err)
		}
	})
}

func TestTypedCompilerRelationRefusesAJoinTypeItCannotRender(t *testing.T) {
	// compileTypedSQL stops these earlier through dal.ValidateJoinTree; the
	// renderer keeps its own guard so it never emits INNER for a RIGHT join.
	from := typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinRight, typedTestJoinOn("a", "x", "r", "y")))
	c := &typedCompiler{dialect: newFakeTypedDialect()}
	if _, err := c.relation(from, "from", nil); !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "from.joins[0].type") {
		t.Fatalf("relation() error = %v, want an unsupported join_type error", err)
	}
}

func TestTypedQuerySources(t *testing.T) {
	artists := dal.From(dal.NewQualifiedRootCollectionRef("s", "Artist", "r")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Album", "b"), dal.JoinInner, typedTestJoinOn("r", "x", "b", "y")),
	)
	from := typedTestFrom("Album", "a").
		Join(dal.NewJoinedFrom(artists, dal.JoinLeft, typedTestJoinOn("a", "x", "r", "y"))).
		Join(dal.NewJoinedSource(dal.NewQuerySource(typedTestFrom("Q", "").NewQuery().SelectColumns(), "q"), dal.JoinInner, typedTestJoinOn("a", "x", "q", "y"))).
		Join(dal.NewJoinedSource(dal.NewCollectionGroupRef("G", ""), dal.JoinInner, typedTestJoinOn("a", "x", "g", "y")))
	want := []typedSourceName{{Name: "Album"}, {Schema: "s", Name: "Artist"}}
	got := typedQuerySources(from)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("typedQuerySources() = %v, want %v (duplicates removed, unrenderable sources skipped)", got, want)
	}
	if got := typedQuerySources(nil); len(got) != 0 {
		t.Fatalf("typedQuerySources(nil) = %v, want none", got)
	}
}
