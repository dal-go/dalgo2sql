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
	// quoteOverride and bindMarkers let a test make the dialect misbehave.
	quoteOverride func(string) (string, error)
	bindMarkers   string
}

func newFakeTypedDialect() *fakeTypedDialect {
	return &fakeTypedDialect{
		style:      typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'},
		caps:       fullFakeTypedCapabilities(),
		identLimit: 63,
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
	return "(CAST(" + left + " AS double precision) / NULLIF(CAST(" + right + " AS double precision), 0))"
}

func (d *fakeTypedDialect) aggregateResult(function, aggregate string) string {
	if function == dal.SUM || function == dal.AVERAGE {
		return "CAST(" + aggregate + " AS double precision)"
	}
	return aggregate
}

func (d *fakeTypedDialect) emptyIn(negated bool) string {
	if negated {
		return "TRUE"
	}
	return "FALSE"
}

func (d *fakeTypedDialect) capabilities() dal.QueryCapabilities { return d.caps }

func (d *fakeTypedDialect) catalogFacts(context.Context, executeQueryFunc, []typedSourceName) (typedCatalogFacts, error) {
	return typedCatalogFacts{}, nil
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
	dialect  *fakeTypedDialect
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
