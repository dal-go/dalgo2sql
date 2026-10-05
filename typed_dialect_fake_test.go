package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

// fakeTypedDialect is a PostgreSQL-shaped dialect used only to prove the typed
// compiler. It needs no database: golden tests compare SQL text and arguments.
type fakeTypedDialect struct {
	style      typedPlaceholderStyle
	caps       dal.QueryCapabilities
	identLimit int
	// quoteOverride, bindMarkers and the *Override hooks let a test make the
	// dialect misbehave, to prove the compiler refuses a dialect that breaks the
	// contract in typed_dialect.go.
	quoteOverride       func(string) (string, error)
	bindMarkers         string
	bindOverride        func(value any) (marker string, arg any, err error)
	divideOverride      func(left, right string) string
	arithmeticOverride  func(operand string) string
	aggregateOverride   func(function, aggregate string) string
	orderItemOverride   func(expression string, descending, notNull bool) string
	emptyInOverride     func(negated bool) string
	limitOffsetOverride func(limit, offset int) (string, []any)
}

func newFakeTypedDialect() *fakeTypedDialect {
	return &fakeTypedDialect{
		style:      typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'},
		caps:       fullFakeTypedCapabilities(),
		identLimit: 63,
	}
}

// newFoldingFakeTypedDialect is the fake dialect with a quoteIdent that folds
// case inside the quotes, as dalgo2postgres's quoteIdent does
// (sql_gen.go: `"` + strings.ToLower(name) + `"`) and as the PostgreSQL
// dialect's FoldLower mode will. Two spellings that differ only in case are then
// one identifier to the server, which a check that compares the query's
// spelling cannot see.
func newFoldingFakeTypedDialect() *fakeTypedDialect {
	d := newFakeTypedDialect()
	d.quoteOverride = func(name string) (string, error) {
		if err := checkTypedIdentifier(name, d.identLimit); err != nil {
			return "", err
		}
		return quoteTypedIdentifier(strings.ToLower(name), '"'), nil
	}
	return d
}

// foldingTypedDialect is a dialect whose quoteIdent folds case, with what a test
// needs to build facts and expectations for it.
type foldingTypedDialect struct {
	name    string
	dialect typedDialect
	// fold is the Fold the dialect's catalog facts carry: the rule its quoteIdent
	// applies.
	fold func(string) string
	// arg is the argument the dialect sends beside the marker of a Go constant.
	arg func(value any) any
	// operand is how the dialect writes one operand of +, - and *.
	operand func(operand string) string
}

// foldingTypedDialects lists every dialect whose quoteIdent folds case: the fake
// one, and the real PostgreSQL dialect in FoldLower mode. The compiler's rules for
// such a dialect (the name-resolution section of typedDialect) are proved against
// both, so they hold for the dialect that ships and not only for a model of it.
func foldingTypedDialects(t *testing.T) []foldingTypedDialect {
	t.Helper()
	postgres := newPostgresDialect(postgresFoldLower)
	return []foldingTypedDialect{
		{name: "fake", dialect: newFoldingFakeTypedDialect(), fold: strings.ToLower, arg: func(value any) any { return value }, operand: func(operand string) string { return operand }},
		{name: "PostgreSQL FoldLower", dialect: postgres, fold: postgresTestFolding(t, postgres).Fold, arg: func(value any) any { return typedBoundArgument(t, postgres, value) }, operand: postgres.arithmeticOperand},
	}
}

func fullFakeTypedCapabilities() dal.QueryCapabilities {
	return dal.QueryCapabilities{
		GroupBy: true,
		Having:  true,
		OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count: true, CountDistinct: true,
			Sum: true, SumDistinct: true,
			Avg: true, AvgDistinct: true,
			Min: true, Max: true,
			OrderBy: true,
		},
	}
}

func (d *fakeTypedDialect) placeholderStyle() typedPlaceholderStyle { return d.style }

func (d *fakeTypedDialect) quoteIdent(name string) (string, error) {
	if d.quoteOverride != nil {
		return d.quoteOverride(name)
	}
	if err := checkTypedIdentifier(name, d.identLimit); err != nil {
		return "", err
	}
	return quoteTypedIdentifier(name, '"'), nil
}

var errFakeTypedBind = errors.New("fake dialect refuses this value")

// fakeTypedFailingValuer is accepted by the compiler (it is a driver.Valuer) and
// refused by the fake dialect, which exercises the dialect error path.
type fakeTypedFailingValuer struct{}

func (fakeTypedFailingValuer) Value() (driver.Value, error) { return nil, nil }

func (d *fakeTypedDialect) bind(value any) (string, any, error) {
	if d.bindOverride != nil {
		return d.bindOverride(value)
	}
	kind, err := typedKindOf(value)
	if err != nil {
		return "", nil, err
	}
	if d.bindMarkers != "" {
		return d.bindMarkers, value, nil
	}
	switch kind {
	case typedValueValuer:
		return "", nil, errFakeTypedBind
	case typedValueTime:
		return "?::timestamptz", value, nil
	case typedValueBool:
		return "?::boolean", value, nil
	case typedValueInteger, typedValueUnsigned:
		return "?::bigint", value, nil
	case typedValueFloat:
		return "?::numeric", value, nil
	case typedValueBytes:
		return "?::bytea", value, nil
	default: // null and text stay untyped so the server infers the type
		return "?", value, nil
	}
}

func (d *fakeTypedDialect) limitOffset(limit, offset int) (string, []any) {
	if d.limitOffsetOverride != nil {
		return d.limitOffsetOverride(limit, offset)
	}
	switch {
	case limit > 0 && offset > 0:
		return "LIMIT ? OFFSET ?", []any{limit, offset}
	case limit > 0:
		return "LIMIT ?", []any{limit}
	case offset > 0:
		return "OFFSET ?", []any{offset}
	}
	return "", nil
}

func (d *fakeTypedDialect) orderItem(expression string, descending, notNull bool) string {
	if d.orderItemOverride != nil {
		return d.orderItemOverride(expression, descending, notNull)
	}
	switch {
	case notNull && descending:
		return expression + " DESC"
	case notNull:
		return expression + " ASC"
	case descending:
		return expression + " DESC NULLS LAST"
	}
	return expression + " ASC NULLS FIRST"
}

func (d *fakeTypedDialect) divide(left, right string) string {
	if d.divideOverride != nil {
		return d.divideOverride(left, right)
	}
	return "(CAST(" + left + " AS double precision) / NULLIF(CAST(" + right + " AS double precision), 0))"
}

// arithmeticOperand is the identity: the fake adds no cast to the operands of +, -
// and *, so the many goldens of the compiler's own tests keep reading as plain
// arithmetic. A test that needs the cast overrides it (see arithmeticOverride).
func (d *fakeTypedDialect) arithmeticOperand(operand string) string {
	if d.arithmeticOverride != nil {
		return d.arithmeticOverride(operand)
	}
	return operand
}

func (d *fakeTypedDialect) aggregateResult(function, aggregate string) string {
	if d.aggregateOverride != nil {
		return d.aggregateOverride(function, aggregate)
	}
	if function == dal.SUM || function == dal.AVERAGE {
		return "CAST(" + aggregate + " AS double precision)"
	}
	return aggregate
}

func (d *fakeTypedDialect) emptyIn(negated bool) string {
	if d.emptyInOverride != nil {
		return d.emptyInOverride(negated)
	}
	if negated {
		return "TRUE"
	}
	return "FALSE"
}

func (d *fakeTypedDialect) capabilities() dal.QueryCapabilities { return d.caps }

func (d *fakeTypedDialect) catalogFacts(context.Context, executeQueryFunc, []typedSourceName) (typedCatalogFacts, error) {
	return typedCatalogFacts{}, nil
}

func (d *fakeTypedDialect) suggestSource(context.Context, executeQueryFunc, typedSourceName) (typedSourceName, bool, error) {
	return typedSourceName{}, false, nil
}

func (d *fakeTypedDialect) window(string, []string, []string, []string) (string, error) {
	return "", fmt.Errorf("window functions are reserved: %w", dal.ErrNotSupported)
}

var _ typedDialect = (*fakeTypedDialect)(nil)

// typedGolden is one clause-level golden case.
type typedGolden struct {
	name     string
	query    dal.StructuredQuery
	facts    typedCatalogFacts
	dialect  typedDialect
	wantSQL  string
	wantArgs []any
}

func (g typedGolden) run(t *testing.T) {
	t.Helper()
	dialect := g.dialect
	if dialect == nil {
		dialect = newFakeTypedDialect()
	}
	gotSQL, gotArgs, err := compileTypedSQL(g.query, dialect, g.facts)
	if err != nil {
		t.Fatalf("compileTypedSQL() error = %v", err)
	}
	if gotSQL != g.wantSQL {
		t.Fatalf("SQL mismatch\n got: %s\nwant: %s", gotSQL, g.wantSQL)
	}
	if len(gotArgs) == 0 && len(g.wantArgs) == 0 {
		return
	}
	if !reflect.DeepEqual(gotArgs, g.wantArgs) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", gotArgs, g.wantArgs)
	}
}

func runTypedGoldens(t *testing.T, cases []typedGolden) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

// expectTypedUnsupported compiles q and requires an error matching
// dal.ErrNotSupported whose text contains fragment.
func expectTypedUnsupported(t *testing.T, q dal.StructuredQuery, dialect typedDialect, facts typedCatalogFacts, fragment string) {
	t.Helper()
	if dialect == nil {
		dialect = newFakeTypedDialect()
	}
	text, args, err := compileTypedSQL(q, dialect, facts)
	if err == nil {
		t.Fatalf("compileTypedSQL() = %q, %v; want an unsupported error", text, args)
	}
	if !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error %q does not match dal.ErrNotSupported", err)
	}
	if text != "" || args != nil {
		t.Fatalf("a refused query must return no SQL, got %q %v", text, args)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error %q does not mention %q", err, fragment)
	}
}

// expectTypedInvalid compiles q and requires a non-nil error that is NOT a
// capability refusal: the query itself is malformed.
func expectTypedInvalid(t *testing.T, q dal.StructuredQuery, dialect typedDialect, facts typedCatalogFacts, fragment string) {
	t.Helper()
	if dialect == nil {
		dialect = newFakeTypedDialect()
	}
	text, args, err := compileTypedSQL(q, dialect, facts)
	if err == nil {
		t.Fatalf("compileTypedSQL() = %q, %v; want an error", text, args)
	}
	if errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error %q must not claim ErrNotSupported", err)
	}
	if text != "" || args != nil {
		t.Fatalf("a refused query must return no SQL, got %q %v", text, args)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error %q does not mention %q", err, fragment)
	}
}

func typedTestField(name string) dal.FieldRef { return dal.NewFieldRef("", name) }

func typedTestQualified(source, name string) dal.FieldRef { return dal.NewFieldRef(source, name) }

// typedTestConst builds a Constant directly: dal.NewConstant panics on types the
// compiler is expected to refuse.
func typedTestConst(value any) dal.Constant { return dal.Constant{Value: value} }

func typedTestEq(left dal.Expression, right any) dal.Comparison {
	return dal.NewComparison(left, dal.Equal, typedTestConst(right))
}

func typedTestFrom(name, alias string) dal.FromSource {
	return dal.From(dal.NewRootCollectionRef(name, alias))
}

func typedTestColumn(expression dal.Expression, alias string) dal.Column {
	return dal.Column{Expression: expression, Alias: alias}
}

func typedTestJoinOn(leftSource, leftField, rightSource, rightField string) dal.Condition {
	return dal.NewComparison(typedTestQualified(leftSource, leftField), dal.Equal, typedTestQualified(rightSource, rightField))
}
