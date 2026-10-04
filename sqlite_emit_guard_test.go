package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

const (
	hostileName  = "x) OR 1=1 --"
	plainName    = "ok_name1"
	hostileValue = `a\' OR 1=1`
)

func guardQuery(from dal.FromSource, build func(b dal.IQueryBuilder) dal.IQueryBuilder) dal.StructuredQuery {
	b := dal.IQueryBuilder(dal.NewQueryBuilder(from))
	if build != nil {
		b = build(b)
	}
	return b.SelectIntoRecordset()
}

func baseFrom() dal.FromSource { return dal.From(dal.NewRootCollectionRef("customers", "c")) }

// TestEmitSQLAcceptanceProbes is the acceptance line of SQL-01: with no
// dialect set, a hostile field name or a backslash value is refused with an
// error wrapping dal.ErrNotSupported and no SQL reaches the database.
func TestEmitSQLAcceptanceProbes(t *testing.T) {
	probes := map[string]dal.StructuredQuery{
		"field name":   guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Where(dal.Field(hostileName).EqualTo(1)) }),
		"string value": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereField("a", dal.Equal, hostileValue) }),
	}
	for name, q := range probes {
		t.Run(name, func(t *testing.T) {
			executed := 0
			execute := func(context.Context, string, ...any) (*sql.Rows, error) {
				executed++
				return nil, errors.New("must not execute")
			}
			_, err := getReaderBase(context.Background(), q, execute)
			if !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("error = %v, want one wrapping dal.ErrNotSupported", err)
			}
			if executed != 0 {
				t.Fatalf("execute called %d times, want 0", executed)
			}
			if text, err := emitSQL(q); text != "" || !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("emitSQL() = %q, %v; want empty text and ErrNotSupported", text, err)
			}
		})
	}
}

func TestEmitSQLAcceptsOrdinaryQuery(t *testing.T) {
	q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
		return b.WhereField("name", dal.Equal, "O'Brien; DROP TABLE x --").
			WhereField("age", dal.GreaterThen, 3).
			WhereField("ratio", dal.LessOrEqual, 0.5).
			WhereField("at", dal.GreaterOrEqual, time.Unix(0, 0).UTC()).
			WhereField("ok", dal.Equal, true).
			WhereField("gone", dal.Equal, nil).
			WhereField("id", dal.In, []string{"a", "b"}).
			WhereField("n", dal.NotIn, []any{1, "x", nil}).
			GroupBy(dal.Field("name")).
			Having(dal.NewComparison(dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.GreaterThen, dal.Constant{Value: 1})).
			OrderBy(dal.DescendingField("name"), dal.Ascending(dal.Binary(dal.Field("a"), dal.Add, dal.Constant{Value: 2})))
	})
	text, err := emitSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "'O''Brien; DROP TABLE x --'") {
		t.Fatalf("apostrophe must be doubled, got %q", text)
	}
}

// TestEmitSQLGuardTable lists values and identifiers that must be refused and
// the ordinary ones that must be accepted, position by position. Every refused
// row has an accepted twin so the position, not something else in the query,
// is what the guard reacts to.
func TestEmitSQLGuardTable(t *testing.T) {
	type position struct {
		name  string
		build func(value string) dal.StructuredQuery
	}
	where := func(c dal.Condition) func(b dal.IQueryBuilder) dal.IQueryBuilder {
		return func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Where(c) }
	}
	identifierPositions := []position{
		{"collection name", func(v string) dal.StructuredQuery {
			return guardQuery(dal.From(dal.NewRootCollectionRef(v, "")), nil)
		}},
		{"collection alias", func(v string) dal.StructuredQuery {
			return guardQuery(dal.From(dal.NewRootCollectionRef("customers", v)), nil)
		}},
		{"collection schema", func(v string) dal.StructuredQuery {
			return guardQuery(dal.From(dal.NewQualifiedRootCollectionRef(v, "customers", "")), nil)
		}},
		{"collection pointer name", func(v string) dal.StructuredQuery {
			ref := dal.NewRootCollectionRef(v, "")
			return guardQuery(dal.From(&ref), nil)
		}},
		{"collection group name", func(v string) dal.StructuredQuery {
			return guardQuery(dal.From(dal.NewCollectionGroupRef(v, "")), nil)
		}},
		{"collection group alias", func(v string) dal.StructuredQuery {
			return guardQuery(dal.From(dal.NewCollectionGroupRef("customers", v)), nil)
		}},
		{"collection group pointer name", func(v string) dal.StructuredQuery {
			ref := dal.NewCollectionGroupRef(v, "")
			return guardQuery(dal.From(&ref), nil)
		}},
		{"join collection name", func(v string) dal.StructuredQuery {
			from := baseFrom().Join(dal.NewJoinedSource(dal.NewRootCollectionRef(v, "o"), dal.JoinInner, dal.NewComparison(dal.NewFieldRef("c", "id"), dal.Equal, dal.NewFieldRef("o", "cid"))))
			return guardQuery(from, nil)
		}},
		{"join on field", func(v string) dal.StructuredQuery {
			from := baseFrom().Join(dal.NewJoinedSource(dal.NewRootCollectionRef("orders", "o"), dal.JoinInner, dal.NewComparison(dal.NewFieldRef("c", "id"), dal.Equal, dal.NewFieldRef("o", v))))
			return guardQuery(from, nil)
		}},
		{"nested join collection alias", func(v string) dal.StructuredQuery {
			nested := dal.From(dal.NewRootCollectionRef("orders", v))
			from := baseFrom().Join(dal.NewJoinedFrom(nested, dal.JoinLeft, dal.NewComparison(dal.NewFieldRef("c", "id"), dal.Equal, dal.NewFieldRef("o", "cid"))))
			return guardQuery(from, nil)
		}},
		{"column alias", func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.Column{Alias: v, Expression: dal.Field("a")})
		}},
		{"column field name", func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.Field(v)})
		}},
		{"column field source", func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef(v, "a")})
		}},
		{"wildcard source", func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.AllColumnsExceptFrom(v, "a"))
		}},
		{"wildcard exclusion", func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.AllColumnsExcept(v))
		}},
		{"where field", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.Field(v).EqualTo(1)))
		}},
		{"where right field", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.Field(v))))
		}},
		{"where is null", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.Field(v).IsNull()))
		}},
		{"where is not null", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.Field(v).IsNotNull()))
		}},
		{"where nested group", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewGroupCondition(dal.And, dal.Field("a").EqualTo(1), dal.NewGroupCondition(dal.Or, dal.Field(v).EqualTo(2)))))
		}},
		{"where binary operand", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.Binary(dal.Field("a"), dal.Add, dal.Field(v)), dal.Equal, dal.Constant{Value: 1})))
		}},
		{"where binary left operand", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.Binary(dal.Field(v), dal.Add, dal.Field("a")), dal.Equal, dal.Constant{Value: 1})))
		}},
		{"where aggregate argument", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.NewAggregate(dal.SUM, true, dal.Field(v)), dal.GreaterThen, dal.Constant{Value: 1})))
		}},
		{"where aggregate name", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.NewAggregate(v, false, dal.Field("a")), dal.GreaterThen, dal.Constant{Value: 1})))
		}},
		{"group by", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.GroupBy(dal.Field(v)) })
		}},
		{"having", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Having(dal.Field(v).EqualTo(1)) })
		}},
		{"order by", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.OrderBy(dal.AscendingField(v)) })
		}},
		{"order by descending", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.OrderBy(dal.DescendingField(v)) })
		}},
		{"parameter name", func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.Param{Name: v})))
		}},
	}
	hostileIdentifiers := []string{
		hostileName, "has space", "has-dash", "a]b", "[a]", "a`b", `a"b`, "a'b", `a\b`, "1starts_with_digit",
		"a;b", "a\nb", "a\x00b", "naïve",
	}
	for _, p := range identifierPositions {
		t.Run(p.name, func(t *testing.T) {
			if _, err := emitSQL(p.build(plainName)); err != nil {
				t.Fatalf("plain identifier refused: %v", err)
			}
			for _, bad := range hostileIdentifiers {
				text, err := emitSQL(p.build(bad))
				if text != "" || !errors.Is(err, dal.ErrNotSupported) {
					t.Fatalf("%q accepted: text=%q err=%v", bad, text, err)
				}
			}
		})
	}
}

// An empty name is "not set" for optional positions (alias, schema, source) and
// a refusal for a field, which always needs a name.
func TestEmitSQLGuardEmptyNames(t *testing.T) {
	optional := []dal.StructuredQuery{
		guardQuery(dal.From(dal.NewRootCollectionRef("customers", "")), nil),
		guardQuery(dal.From(dal.NewCollectionGroupRef("customers", "")), nil),
		baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.Field("a")}),
		baseFrom().NewQuery().SelectColumns(dal.AllColumnsExcept("a")),
	}
	for i, q := range optional {
		if _, err := emitSQL(q); err != nil {
			t.Errorf("optional case %d refused: %v", i, err)
		}
	}
	required := []dal.StructuredQuery{
		baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.Field("")}),
		baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.Field("a"), Alias: " "}),
		baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.FieldName("a")}),
		guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.Param{}))
		}),
	}
	for i, q := range required {
		if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("required case %d accepted: %v", i, err)
		}
	}
}

func TestEmitSQLGuardValues(t *testing.T) {
	refused := []string{`a\b`, `a\' OR 1=1`, "a[b", "a]b", "[x]", "a\nb", "a\tb", "a\rb", "a\x00b", "a\x7fb", "a\u0085b"}
	accepted := []string{"", "plain", "O'Brien", "''", "a'; DROP TABLE x; --", "100%", "naïve", "日本", "a b", `a"b`}
	shapes := map[string]func(v string) dal.StructuredQuery{
		"comparison constant": func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereField("a", dal.Equal, v) })
		},
		"constant pointer": func(v string) dal.StructuredQuery {
			c := dal.Constant{Value: v}
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
				return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, &c))
			})
		},
		"in string slice": func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereField("a", dal.In, []string{"x", v}) })
		},
		"in any slice": func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereField("a", dal.NotIn, []any{1, v}) })
		},
		"array in constant position": func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereInArrayField("a", []string{v}) })
		},
		"select constant": func(v string) dal.StructuredQuery {
			return baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.Constant{Value: v}})
		},
		"having constant": func(v string) dal.StructuredQuery {
			return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Having(dal.Field("a").EqualTo(v)) })
		},
		"join on constant": func(v string) dal.StructuredQuery {
			from := baseFrom().Join(dal.NewJoinedSource(dal.NewRootCollectionRef("orders", "o"), dal.JoinInner, dal.NewComparison(dal.NewFieldRef("o", "k"), dal.Equal, dal.Constant{Value: v})))
			return guardQuery(from, nil)
		},
	}
	for shape, build := range shapes {
		t.Run(shape, func(t *testing.T) {
			for _, v := range accepted {
				if shape == "array in constant position" && strings.Contains(v, `"`) {
					continue // JSON-rendered; covered by TestEmitSQLRefusesDoubleQuoteInJSONRenderedValues
				}
				if _, err := emitSQL(build(v)); err != nil {
					t.Errorf("value %q refused: %v", v, err)
				}
			}
			for _, v := range refused {
				if text, err := emitSQL(build(v)); text != "" || !errors.Is(err, dal.ErrNotSupported) {
					t.Errorf("value %q accepted: text=%q err=%v", v, text, err)
				}
			}
		})
	}
}

// A Constant holding a slice is rendered through encoding/json, where a double
// quote inside a string becomes backslash-quote and would close an SQL
// identifier token early.
func TestEmitSQLRefusesDoubleQuoteInJSONRenderedValues(t *testing.T) {
	q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
		return b.WhereInArrayField("a", []string{`x" OR 1=1 --`})
	})
	if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error = %v, want ErrNotSupported", err)
	}
}

type strangeValue struct{ Text string }

func (s strangeValue) MarshalJSON() ([]byte, error) { return []byte(s.Text), nil }

type namedString string

func TestEmitSQLGuardValueTypes(t *testing.T) {
	scalars := []any{nil, true, 1, int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1.5), 2.5, time.Unix(1, 0).UTC(), "s"}
	for _, v := range scalars {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.Constant{Value: v}))
		})
		if _, err := emitSQL(q); err != nil {
			t.Errorf("constant %T refused: %v", v, err)
		}
	}
	ints := []any{[]int{1, 2}, []float64{1.5}, []byte("abc"), []any{nil, true, "s"}}
	for _, v := range ints {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.WhereInArrayField("a", v) })
		if _, err := emitSQL(q); err != nil {
			t.Errorf("constant slice %T refused: %v", v, err)
		}
	}
	unsupported := []any{
		strangeValue{Text: "1 OR 1=1"}, &strangeValue{}, namedString("x"), map[string]any{"a": 1}, struct{}{}, new(string),
		[]any{strangeValue{Text: "1"}}, []strangeValue{{Text: "1"}}, [][]int{{1}},
	}
	for _, v := range unsupported {
		constant := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.Constant{Value: v}))
		})
		if _, err := emitSQL(constant); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("constant %T accepted: %v", v, err)
		}
		array := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.In, dal.Array{Value: v}))
		})
		if _, err := emitSQL(array); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("array %T accepted: %v", v, err)
		}
	}
	// Array.Value forms that String() renders safely.
	for _, v := range []any{nil, []string{}, []int{1}, []any{nil, 1, "x"}} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.In, dal.Array{Value: v}))
		})
		if _, err := emitSQL(q); err != nil {
			t.Errorf("array %T refused: %v", v, err)
		}
	}
}

func TestEmitSQLGuardOperators(t *testing.T) {
	for _, op := range []dal.Operator{dal.Equal, dal.In, dal.NotIn, dal.GreaterThen, dal.GreaterOrEqual, dal.LessThen, dal.LessOrEqual} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), op, dal.Constant{Value: 1}))
		})
		if _, err := emitSQL(q); err != nil {
			t.Errorf("operator %q refused: %v", op, err)
		}
	}
	for _, op := range []dal.Operator{"", "= 1 OR 1=1 --", "!=", "LIKE", "AND"} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), op, dal.Constant{Value: 1}))
		})
		if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("operator %q accepted: %v", op, err)
		}
	}
	for _, op := range []dal.Operator{dal.And, dal.Or} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewGroupCondition(op, dal.Field("a").EqualTo(1)))
		})
		if _, err := emitSQL(q); err != nil {
			t.Errorf("group operator %q refused: %v", op, err)
		}
	}
	for _, op := range []dal.Operator{"", "OR 1=1 --", dal.Equal} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewGroupCondition(op, dal.Field("a").EqualTo(1), dal.Field("b").EqualTo(2)))
		})
		if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("group operator %q accepted: %v", op, err)
		}
	}
	for _, op := range []dal.ArithmeticOperator{dal.Add, dal.Subtract, dal.Multiply, dal.Divide} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.OrderBy(dal.Ascending(dal.Binary(dal.Field("a"), op, dal.Constant{Value: 1})))
		})
		if _, err := emitSQL(q); err != nil {
			t.Errorf("arithmetic %q refused: %v", op, err)
		}
	}
	for _, op := range []dal.ArithmeticOperator{"", "+ 1) OR (1=1", "%"} {
		q := guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.OrderBy(dal.Ascending(dal.Binary(dal.Field("a"), op, dal.Constant{Value: 1})))
		})
		if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("arithmetic %q accepted: %v", op, err)
		}
	}
}

// Types the guard cannot prove safe are refused: their String() output is
// arbitrary text that would be pasted into the statement.

type opaqueExpression struct{ text string }

func (e opaqueExpression) String() string { return e.text }

type opaqueCondition struct{ text string }

func (c opaqueCondition) String() string { return c.text }

type opaqueSource struct{ dal.CollectionRef }

type opaqueOrder struct {
	dal.OrderExpression
	text string
}

func (o opaqueOrder) String() string { return o.text }

type opaqueAggregate struct {
	text string
	name string
	args []dal.Expression
	dist bool
}

func (a opaqueAggregate) String() string             { return a.text }
func (a opaqueAggregate) FuncName() string           { return a.name }
func (a opaqueAggregate) FuncArgs() []dal.Expression { return a.args }
func (a opaqueAggregate) IsDistinct() bool           { return a.dist }

type plainAggregate struct {
	text string
	name string
}

func (a plainAggregate) String() string             { return a.text }
func (a plainAggregate) FuncName() string           { return a.name }
func (a plainAggregate) FuncArgs() []dal.Expression { return nil }

type opaqueStar struct {
	text string
	star bool
}

func (s opaqueStar) String() string { return s.text }
func (s opaqueStar) IsStar() bool   { return s.star }

func TestEmitSQLGuardRefusesWhatItCannotProve(t *testing.T) {
	sub := guardQuery(dal.From(dal.NewRootCollectionRef("t", "")), nil)
	nilConstant := (*dal.Constant)(nil)
	cyclic := baseFrom()
	cyclic.Join(dal.NewJoinedFrom(cyclic, dal.JoinInner, dal.NewComparison(dal.NewFieldRef("c", "a"), dal.Equal, dal.NewFieldRef("c", "b"))))
	parentKey := record.NewKeyWithID("parent", "1")
	nilRef := (*dal.CollectionRef)(nil)
	nilGroup := (*dal.CollectionGroupRef)(nil)
	cases := map[string]dal.StructuredQuery{
		"nil query":                   nil,
		"opaque expression in column": baseFrom().NewQuery().SelectColumns(dal.Column{Expression: opaqueExpression{"1) OR (1=1"}}),
		"opaque expression in where": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, opaqueExpression{"1 OR 1=1"}))
		}),
		"nil expression operand": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(nil, dal.Equal, dal.Constant{Value: 1}))
		}),
		"opaque condition": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Where(opaqueCondition{"1=1 OR 1=1"}) }),
		"pointer comparison": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			c := dal.NewComparison(dal.Field("a"), dal.Equal, dal.Constant{Value: 1})
			return b.Where(&c)
		}),
		"opaque condition in group": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewGroupCondition(dal.And, opaqueCondition{"x"}))
		}),
		"subquery expression": baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.NewQueryExpression(sub, "s")}),
		"exists condition": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewExistsCondition(sub))
		}),
		"is null without operand": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewIsNullCondition(nil))
		}),
		"nil constant pointer": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, nilConstant))
		}),
		"query source base":      guardQuery(dal.From(dal.NewQuerySource(sub, "s")), nil),
		"opaque source base":     guardQuery(dal.From(opaqueSource{dal.NewRootCollectionRef("t", "")}), nil),
		"nil source base":        guardQuery(dal.From(nil), nil),
		"nil collection pointer": guardQuery(dal.From(nilRef), nil),
		"nil group pointer":      guardQuery(dal.From(nilGroup), nil),
		"parented collection":    guardQuery(dal.From(dal.NewCollectionRef("child", "", parentKey)), nil),
		"cyclic join tree":       guardQuery(cyclic, nil),
		"opaque join source":     guardQuery(baseFrom().Join(dal.NewJoinedSource(opaqueSource{dal.NewRootCollectionRef("t", "")}, dal.JoinInner)), nil),
		"opaque order text": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.OrderBy(opaqueOrder{dal.AscendingField("a"), "a; DROP TABLE x"})
		}),
		"opaque aggregate text":   baseFrom().NewQuery().SelectColumns(dal.Column{Expression: opaqueAggregate{text: "SUM(a); DROP TABLE x", name: "SUM", args: []dal.Expression{dal.Field("a")}}}),
		"distinct aggregate text": baseFrom().NewQuery().SelectColumns(dal.Column{Expression: opaqueAggregate{text: "SUM(a)", name: "SUM", args: []dal.Expression{dal.Field("a")}, dist: true}}),
		"plain aggregate text":    baseFrom().NewQuery().SelectColumns(dal.Column{Expression: plainAggregate{text: "COUNT() --", name: "COUNT"}}),
		"aggregate nil argument":  baseFrom().NewQuery().SelectColumns(dal.Column{Expression: opaqueAggregate{text: "SUM(<nil>)", name: "SUM", args: []dal.Expression{nil}}}),
		"opaque star":             baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.NewAggregate(dal.COUNT, false, opaqueStar{"*) --", true})}),
		"not a star":              baseFrom().NewQuery().SelectColumns(dal.Column{Expression: dal.NewAggregate(dal.COUNT, false, opaqueStar{"*", false})}),
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			text, err := emitSQL(q)
			if text != "" || !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("emitSQL() = %q, %v; want empty text and ErrNotSupported", text, err)
			}
		})
	}
}

func TestEmitSQLGuardAcceptsWhatItCanProve(t *testing.T) {
	plainAgg := opaqueAggregate{text: "SUM(a)", name: "SUM", args: []dal.Expression{dal.Field("a")}}
	distinctAgg := opaqueAggregate{text: "SUM(DISTINCT a)", name: "SUM", args: []dal.Expression{dal.Field("a")}, dist: true}
	cases := map[string]dal.StructuredQuery{
		"no from":    dal.NewQueryBuilder(nil).SelectIntoRecordset(),
		"nil column": baseFrom().NewQuery().SelectColumns(dal.Column{}),
		"param": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, dal.NewParam("p.q")))
		}),
		"count star":       baseFrom().NewQuery().SelectColumns(dal.Count()),
		"custom aggregate": baseFrom().NewQuery().SelectColumns(dal.Column{Expression: plainAgg}),
		"custom distinct":  baseFrom().NewQuery().SelectColumns(dal.Column{Expression: distinctAgg}),
		"constant pointer": guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder {
			c := dal.Constant{Value: 1}
			return b.Where(dal.NewComparison(dal.Field("a"), dal.Equal, &c))
		}),
		"joined from nested": guardQuery(baseFrom().Join(dal.NewJoinedFrom(dal.From(dal.NewRootCollectionRef("o", "o")), dal.JoinInner, dal.NewComparison(dal.NewFieldRef("c", "a"), dal.Equal, dal.NewFieldRef("o", "b")))), nil),
		"wildcard masks":     baseFrom().NewQuery().SelectColumns(dal.AllColumnsExceptFrom("c", "Billing*", "*_hash")),
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := emitSQL(q); err != nil {
				t.Fatalf("refused: %v", err)
			}
		})
	}
}

func TestEmitSQLGuardRefusesBadWildcardMasks(t *testing.T) {
	for _, mask := range []string{"a b", "a)--", "1abc", "", "a]"} {
		q := baseFrom().NewQuery().SelectColumns(dal.AllColumnsExcept(mask))
		if _, err := emitSQL(q); !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("mask %q accepted: %v", mask, err)
		}
	}
}
