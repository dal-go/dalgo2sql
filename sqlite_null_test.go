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

func openNullTestDB(t *testing.T, script string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(script); err != nil {
		t.Fatal(err)
	}
	return raw
}

// queryNullIDs compiles q natively, executes it and returns the first column
// of every row as text, so NULL, numbers and strings compare uniformly.
func queryNullIDs(t *testing.T, raw *sql.DB, q dal.StructuredQuery) []string {
	t.Helper()
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if placeholders := strings.Count(text, "?"); placeholders != len(args) {
		t.Fatalf("SQL has %d placeholders and %d args:\n%s\n%#v", placeholders, len(args), text, args)
	}
	rows, err := raw.Query(text, args...)
	if err != nil {
		t.Fatalf("execute: %v\n%s", err, text)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		if value := values[0]; value.Valid {
			got = append(got, value.String)
		} else {
			got = append(got, "NULL")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCompileSQLConditionIsNull(t *testing.T) {
	sources := map[string]struct{}{"a": {}}
	for _, test := range []struct {
		name          string
		condition     dal.Condition
		requireSource bool
		wantSQL       string
		wantArgs      []any
	}{
		{
			name:      "is null on a field",
			condition: dal.NewIsNullCondition(dal.Field("x")),
			wantSQL:   "(+`x` COLLATE BINARY) IS NULL",
		},
		{
			name:      "is not null on a field",
			condition: dal.NewIsNotNullCondition(dal.Field("x")),
			wantSQL:   "(+`x` COLLATE BINARY) IS NOT NULL",
		},
		{
			name:          "qualified field in a join",
			condition:     dal.NewIsNullCondition(dal.NewFieldRef("a", "x")),
			requireSource: true,
			wantSQL:       "(+`a`.`x` COLLATE BINARY) IS NULL",
		},
		{
			name:      "field helper methods",
			condition: dal.NewGroupCondition(dal.And, dal.Field("x").IsNull(), dal.Field("y").IsNotNull()),
			wantSQL:   "((+`x` COLLATE BINARY) IS NULL AND (+`y` COLLATE BINARY) IS NOT NULL)",
		},
		{
			name: "inside AND and OR",
			condition: dal.NewGroupCondition(dal.Or,
				dal.NewGroupCondition(dal.And, dal.Field("x").IsNull(), dal.Field("y").IsNotNull()),
				dal.NewIsNullCondition(dal.Field("z")),
			),
			wantSQL: "(((+`x` COLLATE BINARY) IS NULL AND (+`y` COLLATE BINARY) IS NOT NULL) OR (+`z` COLLATE BINARY) IS NULL)",
		},
		{
			name:      "arithmetic operand keeps every bound argument",
			condition: dal.NewIsNullCondition(dal.Binary(dal.Field("x"), dal.Add, dal.NewConstant(2))),
			wantSQL:   "(+(CASE WHEN typeof((CASE WHEN typeof(`x`) IN ('integer','real') THEN CAST(`x` AS REAL) ELSE `x` END)) IN ('integer','real') AND typeof((CASE WHEN typeof(?) IN ('integer','real') THEN CAST(? AS REAL) ELSE ? END)) IN ('integer','real') THEN (CASE WHEN typeof(`x`) IN ('integer','real') THEN CAST(`x` AS REAL) ELSE `x` END) + (CASE WHEN typeof(?) IN ('integer','real') THEN CAST(? AS REAL) ELSE ? END) ELSE NULL END) COLLATE BINARY) IS NULL",
			wantArgs:  []any{2, 2, 2, 2, 2, 2},
		},
		{
			name:      "constant operand is a bound argument",
			condition: dal.NewIsNotNullCondition(dal.NewConstant(nil)),
			wantSQL:   "(+? COLLATE BINARY) IS NOT NULL",
			wantArgs:  []any{nil},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			text, args, err := compileSQLConditionWithSources(test.condition, sources, test.requireSource)
			if err != nil {
				t.Fatal(err)
			}
			if text != test.wantSQL {
				t.Fatalf("SQL = %s, want %s", text, test.wantSQL)
			}
			if !reflect.DeepEqual(args, test.wantArgs) {
				t.Fatalf("args = %#v, want %#v", args, test.wantArgs)
			}
		})
	}
}

func TestCompileSQLConditionIsNullRejectsBadOperands(t *testing.T) {
	sources := map[string]struct{}{"a": {}}
	for _, test := range []struct {
		name          string
		condition     dal.Condition
		requireSource bool
		want          string
	}{
		{"missing operand", dal.NewIsNullCondition(nil), false, "unsupported expression"},
		{"unknown source", dal.NewIsNullCondition(dal.NewFieldRef("zzz", "x")), false, `unknown source "zzz"`},
		{"unqualified field in a join", dal.NewIsNotNullCondition(dal.Field("x")), true, "must name a source"},
		{"unsupported value", dal.NewIsNullCondition(dal.Constant{Value: struct{}{}}), false, "unsupported SQL value type"},
		{"star operand", dal.NewIsNullCondition(dal.Star()), false, "null test over *"},
		{"star operand, negated", dal.NewIsNotNullCondition(dal.Star()), false, "null test over *"},
		{"star on the left of arithmetic", dal.NewIsNullCondition(dal.Binary(dal.Star(), dal.Add, dal.NewConstant(1))), false, "null test over *"},
		{"star on the right of arithmetic", dal.NewIsNotNullCondition(dal.Binary(dal.NewConstant(1), dal.Multiply, dal.Star())), false, "null test over *"},
		{"star deep in arithmetic", dal.NewIsNullCondition(dal.Binary(dal.Binary(dal.Field("x"), dal.Add, dal.Binary(dal.Field("y"), dal.Subtract, dal.Star())), dal.Multiply, dal.NewConstant(2))), false, "null test over *"},
		{"star in arithmetic, inside a group", dal.NewGroupCondition(dal.And, dal.Field("x").IsNull(), dal.NewIsNullCondition(dal.Binary(dal.Star(), dal.Add, dal.Field("x")))), false, "null test over *"},
		{"parameter operand", dal.NewIsNullCondition(dal.NewParam("p")), false, "unsupported expression"},
		{"array operand", dal.NewIsNullCondition(dal.NewArray([]any{1, 2})), false, "unsupported expression"},
		{"error inside a group", dal.NewGroupCondition(dal.And, dal.Field("x").IsNull(), dal.NewIsNullCondition(nil)), false, "unsupported expression"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := compileSQLConditionWithSources(test.condition, sources, test.requireSource)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestRewriteSQLConditionAliasesRewritesIsNullOperand(t *testing.T) {
	total := dal.NewAggregate(dal.SUM, false, dal.Field("amount"))
	aliases := map[string]dal.Expression{"total": total}

	rewritten, ok := rewriteSQLConditionAliases(dal.NewIsNullCondition(dal.Field("total")), aliases).(dal.IsNullCondition)
	if !ok || rewritten.Negated() || !reflect.DeepEqual(rewritten.Operand(), dal.Expression(total)) {
		t.Fatalf("IS NULL rewrite = %#v", rewritten)
	}
	rewritten, ok = rewriteSQLConditionAliases(dal.NewIsNotNullCondition(dal.Binary(dal.Field("total"), dal.Add, dal.NewConstant(1))), aliases).(dal.IsNullCondition)
	if !ok || !rewritten.Negated() {
		t.Fatalf("IS NOT NULL rewrite lost its negation: %#v", rewritten)
	}
	if binary, isBinary := rewritten.Operand().(dal.BinaryExpression); !isBinary || !reflect.DeepEqual(binary.Left, dal.Expression(total)) {
		t.Fatalf("IS NOT NULL operand = %#v, want the alias replaced inside the arithmetic", rewritten.Operand())
	}
	// A qualified field is never an alias.
	kept, ok := rewriteSQLConditionAliases(dal.NewIsNullCondition(dal.NewFieldRef("t", "total")), aliases).(dal.IsNullCondition)
	if !ok || !reflect.DeepEqual(kept.Operand(), dal.Expression(dal.NewFieldRef("t", "total"))) {
		t.Fatalf("qualified operand was rewritten: %#v", kept)
	}
	// A nil operand stays nil so the compiler reports it.
	nilOperand, ok := rewriteSQLConditionAliases(dal.NewIsNullCondition(nil), aliases).(dal.IsNullCondition)
	if !ok || nilOperand.Operand() != nil {
		t.Fatalf("nil operand rewrite = %#v", nilOperand)
	}
	// Nested inside a group.
	group, ok := rewriteSQLConditionAliases(dal.NewGroupCondition(dal.Or, dal.NewIsNullCondition(dal.Field("total")), dal.Field("x").IsNotNull()), aliases).(dal.GroupCondition)
	if !ok || len(group.Conditions()) != 2 {
		t.Fatalf("group rewrite = %#v", group)
	}
	if first, ok := group.Conditions()[0].(dal.IsNullCondition); !ok || !reflect.DeepEqual(first.Operand(), dal.Expression(total)) {
		t.Fatalf("group child was not rewritten: %#v", group.Conditions()[0])
	}
}

func TestSQLiteNullTestsInWhere(t *testing.T) {
	// Row 3 holds an empty string and row 4 a zero: values, not NULL.
	raw := openNullTestDB(t, "CREATE TABLE items (id INTEGER, name TEXT, qty INTEGER);"+
		"INSERT INTO items VALUES (1, 'a', 5), (2, NULL, 7), (3, '', NULL), (4, 'd', 0), (5, NULL, NULL);")
	items := func() dal.IQueryBuilder {
		return dal.From(dal.NewRootCollectionRef("items", "")).NewQuery()
	}
	selectID := dal.Column{Expression: dal.Field("id")}
	for _, test := range []struct {
		name  string
		where dal.Condition
		want  []string
	}{
		{"is null", dal.Field("name").IsNull(), []string{"2", "5"}},
		{"is not null", dal.Field("name").IsNotNull(), []string{"1", "3", "4"}},
		{"zero and empty text are not null", dal.Field("qty").IsNotNull(), []string{"1", "2", "4"}},
		{"AND", dal.NewGroupCondition(dal.And, dal.Field("name").IsNull(), dal.Field("qty").IsNotNull()), []string{"2"}},
		{"OR", dal.NewGroupCondition(dal.Or, dal.Field("name").IsNull(), dal.Field("qty").IsNull()), []string{"2", "3", "5"}},
		{"OR with an equality to nil", dal.NewGroupCondition(dal.Or,
			dal.NewComparison(dal.Field("name"), dal.Equal, dal.NewConstant(nil)),
			dal.Field("qty").IsNull(),
		), []string{"2", "3", "5"}},
		{"nested", dal.NewGroupCondition(dal.And,
			dal.NewGroupCondition(dal.Or, dal.Field("name").IsNull(), dal.Field("qty").IsNull()),
			dal.Field("id").IsNotNull(),
			dal.NewComparison(dal.Field("id"), dal.GreaterThen, dal.NewConstant(2)),
		), []string{"3", "5"}},
		{"arithmetic operand is null when an input is null", dal.NewIsNullCondition(dal.Binary(dal.Field("qty"), dal.Add, dal.NewConstant(1))), []string{"3", "5"}},
		{"division by zero is null", dal.NewIsNullCondition(dal.Binary(dal.NewConstant(10), dal.Divide, dal.Field("qty"))), []string{"3", "4", "5"}},
		{"constant null", dal.NewIsNullCondition(dal.NewConstant(nil)), []string{"1", "2", "3", "4", "5"}},
		{"constant value", dal.NewIsNullCondition(dal.NewConstant("x")), []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := queryNullIDs(t, raw, items().Where(test.where).OrderBy(dal.Ascending(dal.Field("id"))).SelectColumns(selectID))
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ids = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSQLiteNullTestsInHaving(t *testing.T) {
	// Group "x" sums to 3, group "y" has only NULL amounts so SUM is NULL,
	// group "z" sums to 0 and the NULL-group has an amount of 9.
	raw := openNullTestDB(t, "CREATE TABLE orders (grp TEXT, amount INTEGER);"+
		"INSERT INTO orders VALUES ('x', 1), ('x', 2), ('y', NULL), ('y', NULL), ('z', 0), (NULL, 9);")
	sum := dal.NewAggregate(dal.SUM, false, dal.Field("amount"))
	query := func(having dal.Condition) dal.StructuredQuery {
		return dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().
			GroupBy(dal.Field("grp")).
			Having(having).
			OrderBy(dal.Ascending(dal.Field("grp"))).
			SelectColumns(
				dal.Column{Expression: dal.Field("grp")},
				dal.Column{Expression: sum, Alias: "total"},
			)
	}
	for _, test := range []struct {
		name   string
		having dal.Condition
		want   []string
	}{
		{"aggregate is null", dal.NewIsNullCondition(sum), []string{"y"}},
		{"aggregate is not null", dal.NewIsNotNullCondition(sum), []string{"NULL", "x", "z"}},
		{"alias is null", dal.NewIsNullCondition(dal.Field("total")), []string{"y"}},
		{"alias is not null", dal.NewIsNotNullCondition(dal.Field("total")), []string{"NULL", "x", "z"}},
		{"alias inside arithmetic", dal.NewIsNullCondition(dal.Binary(dal.Field("total"), dal.Add, dal.NewConstant(1))), []string{"y"}},
		{"group key is null", dal.NewIsNullCondition(dal.Field("grp")), []string{"NULL"}},
		{"AND", dal.NewGroupCondition(dal.And, dal.NewIsNotNullCondition(dal.Field("total")), dal.NewIsNotNullCondition(dal.Field("grp"))), []string{"x", "z"}},
		{"OR with a comparison", dal.NewGroupCondition(dal.Or,
			dal.NewIsNullCondition(dal.Field("total")),
			dal.NewComparison(dal.Field("total"), dal.GreaterThen, dal.NewConstant(2)),
		), []string{"NULL", "x", "y"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := queryNullIDs(t, raw, query(test.having))
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("groups = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSQLiteNullTestsWithAWhereAndAHavingTogether(t *testing.T) {
	raw := openNullTestDB(t, "CREATE TABLE orders (grp TEXT, amount INTEGER, note TEXT);"+
		"INSERT INTO orders VALUES ('x', 1, NULL), ('x', 2, 'n'), ('y', NULL, NULL), ('z', 5, NULL);")
	sum := dal.NewAggregate(dal.SUM, false, dal.Field("amount"))
	q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().
		Where(dal.Field("note").IsNull()).
		GroupBy(dal.Field("grp")).
		Having(dal.NewIsNotNullCondition(dal.Field("total"))).
		OrderBy(dal.Ascending(dal.Field("grp"))).
		SelectColumns(dal.Column{Expression: dal.Field("grp")}, dal.Column{Expression: sum, Alias: "total"})
	if got, want := queryNullIDs(t, raw, q), []string{"x", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
}

const nullJoinSchema = `CREATE TABLE customers (id INTEGER, name TEXT);
	CREATE TABLE orders (id INTEGER, customer_id INTEGER, note TEXT);
	INSERT INTO customers VALUES (1, 'ann'), (2, 'bob'), (3, 'cy');
	INSERT INTO orders VALUES (10, 1, NULL), (11, 1, 'gift'), (12, 3, 'rush');`

func nullJoinQuery(where dal.Condition) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("customers", "c")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("orders", "o"), dal.JoinLeft, sqlJoin("c", "id", "o", "customer_id")),
	).NewQuery().
		Where(where).
		OrderBy(dal.Ascending(dal.NewFieldRef("c", "id")), dal.Ascending(dal.NewFieldRef("o", "id"))).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("c", "name"), Alias: "customer"},
			dal.Column{Expression: dal.NewFieldRef("o", "id"), Alias: "order_id"},
		)
}

func TestSQLiteJoinedQueryWithNullTestIsNativeAndReturnsRows(t *testing.T) {
	raw := openNullTestDB(t, nullJoinSchema)
	ctx := context.Background()
	for _, test := range []struct {
		name  string
		where dal.Condition
		want  []string
	}{
		// Customer "bob" has no order, so the LEFT JOIN yields an absent o.id.
		{"customer without an order", dal.NewIsNullCondition(dal.NewFieldRef("o", "id")), []string{"bob"}},
		{"customer with an order", dal.NewIsNotNullCondition(dal.NewFieldRef("o", "id")), []string{"ann", "ann", "cy"}},
		{"null column on the joined side", dal.NewIsNullCondition(dal.NewFieldRef("o", "note")), []string{"ann", "bob"}},
		{"combined", dal.NewGroupCondition(dal.And,
			dal.NewIsNotNullCondition(dal.NewFieldRef("o", "id")),
			dal.NewIsNotNullCondition(dal.NewFieldRef("o", "note")),
		), []string{"ann", "cy"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := nullJoinQuery(test.where)

			tx, err := raw.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if err := canExecuteSQLiteJoin(ctx, q, "sqlite", tx.QueryContext); err != nil {
				t.Fatalf("CanExecuteJoin = %v, want a joined null test accepted for native execution", err)
			}
			plan, err := dal.PlanJoin(ctx, q, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
			if err != nil {
				t.Fatal(err)
			}
			if plan.Strategy != dal.JoinNative {
				t.Fatalf("plan = %#v, want native", plan)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}

			var got []string
			db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
			err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.ExecuteQueryToRecordsReader(ctx, q)
				if err != nil {
					return err
				}
				defer func() { _ = reader.Close() }()
				for {
					record, err := reader.Next()
					if errors.Is(err, dal.ErrNoMoreRecords) {
						return nil
					}
					if err != nil {
						return err
					}
					data, ok := record.Data().(map[string]any)
					if !ok {
						return fmt.Errorf("record data = %T, want map[string]any", record.Data())
					}
					got = append(got, fmt.Sprint(data["customer"]))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("customers = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSQLiteJoinedQueryRejectsInvalidNullOperandBeforeNativePlanning(t *testing.T) {
	raw := openNullTestDB(t, nullJoinSchema)
	ctx := context.Background()
	tx, err := raw.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// An unqualified field is ambiguous in a join and must decline native
	// planning rather than fail after it was chosen.
	err = canExecuteSQLiteJoin(ctx, nullJoinQuery(dal.NewIsNullCondition(dal.Field("note"))), "sqlite", tx.QueryContext)
	if err == nil || !strings.Contains(err.Error(), "join_plan") || !strings.Contains(err.Error(), "must name a source") {
		t.Fatalf("CanExecuteJoin = %v, want a join_plan decline naming the unqualified field", err)
	}
}

// A null test over an aggregate is a HAVING condition. In a WHERE it names an
// aggregate before any group exists, and SQLite rejects the statement, so the
// compiler refuses it rather than sending it.
func TestCompileStructuredSQLRefusesANullTestOverAnAggregateInWhere(t *testing.T) {
	sum := dal.NewAggregate(dal.SUM, false, dal.Field("amount"))
	for name, where := range map[string]dal.Condition{
		"aggregate":           dal.NewIsNullCondition(sum),
		"negated":             dal.NewIsNotNullCondition(sum),
		"inside arithmetic":   dal.NewIsNullCondition(dal.Binary(sum, dal.Add, dal.NewConstant(1))),
		"inside a group":      dal.NewGroupCondition(dal.And, dal.Field("x").IsNull(), dal.NewIsNullCondition(sum)),
		"inside nested group": dal.NewGroupCondition(dal.Or, dal.NewGroupCondition(dal.And, dal.NewIsNullCondition(sum))),
	} {
		t.Run(name, func(t *testing.T) {
			q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().Where(where).SelectColumns(dal.Column{Expression: dal.Field("id")})
			text, _, err := compileStructuredSQL(q)
			if text != "" || err == nil || !strings.Contains(err.Error(), "null test over an aggregate") {
				t.Fatalf("compileStructuredSQL = %q, %v; want a refusal", text, err)
			}
		})
	}
}
