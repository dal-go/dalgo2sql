package dalgo2sql

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
)

// typedMaxArguments bounds the values one statement may bind, counted over the
// whole statement. PostgreSQL's wire protocol counts parameters in 16 bits; no
// other engine allows more.
const typedMaxArguments = 65535

// typedUnsupported builds an error matching dal.ErrNotSupported. Callers use it
// for every query shape the typed compiler declines, so a caller can tell "this
// engine cannot run it in one statement" from "the query is malformed". The
// message never includes caller values.
func typedUnsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", dal.ErrNotSupported, fmt.Sprintf(format, args...))
}

// compileTypedSQL renders one structured query as a single SELECT for a
// statically typed SQL engine described by dialect. It is a pure function: it
// reads no database, and facts carries everything it knows about the catalog.
//
// Every value is a bound argument and every identifier goes through
// dialect.quoteIdent, so no byte of a value reaches the text. The text still
// depends on the shape of the query, and on exactly these facts about its
// values, each of which picks between fixed fragments and writes no content:
//
//   - a nil constant on the right of == renders IS NULL instead of a marker;
//   - the length of an IN or NOT IN list sets how many markers it writes, and an
//     empty list renders the dialect's emptyIn constant;
//   - a zero limit or a zero offset leaves its clause out;
//   - whether a GROUP BY or ORDER BY expression that carries a constant is the
//     same expression as a selected one (same shape, and constants of the same
//     kind and content, see typedSameConstant) picks the select-list position or
//     the inline bound expression, so two constants that are equal or not choose
//     between two fixed fragments;
//   - the Go type of a constant picks the cast the dialect writes beside its
//     marker (dialect.bind), never its content.
//
// Catalog facts, not values, decide wildcard expansion and the NULLS clause.
// assertTypedTextIgnoresValues and assertTypedTextHasTwoShapesForTwoConstants
// (typed_sql_property_test.go) check this for any dialect.
//
// Error messages follow the same rule: they quote identifiers and name what was
// refused, never a constant. DALgo's aggregation validation words its message
// with the query's own expressions, so it is replaced by a fixed message that
// points at dal.ValidateAggregation (errTypedInvalidAggregation); an error a
// dialect returns is the dialect's to keep clean (see typedDialect).
//
// Anything the engine cannot run faithfully returns an error matching
// dal.ErrNotSupported rather than an approximation: an unrecognised node, a
// query option the statement cannot honour (the DTQL money option, a source's
// scan or database: see validateTypedJoinSources and validateTypedScanRestated),
// a reference the server could not match (typedCheckOrderByName), and a bare name
// the server would read as the whole row (typedCompiler.checkNamesAColumn).
// DALgo's generic engine is the place for those queries only where DALgo has a
// fallback: a join it asked the adapter about first (CanExecuteJoin compiles the whole
// query and declines on a refusal), or a query with a subquery, which never reaches the
// compiler. For a query over one source the refusal is the read's error. A field that
// the catalog facts of a known source do not list is a plain error, not a refusal: the
// table has no such column.
func compileTypedSQL(q dal.StructuredQuery, dialect typedDialect, facts typedCatalogFacts) (string, []any, error) {
	statement, err := compileTypedStatement(q, dialect, facts)
	if err != nil {
		return "", nil, err
	}
	return statement.text, statement.args, nil
}

// typedStatement is a compiled query.
type typedStatement struct {
	text string
	args []any
	// outputs names each column of the result, in order, as the query asked for it:
	// the alias, else the field's own name, else the expression's text, and for a
	// column a wildcard expanded, the catalog's name. The server names a column as
	// the dialect wrote it, and a dialect that folds case writes Total as "total", so
	// the reader keys each record by outputs, not by the name the server returns. It
	// is nil when the statement selects *, whose columns are the catalog's own.
	outputs []string
	// baseColumns, for a select-all over joins, lists the base source's columns in the
	// order the server returns them: they come first in the result, before those of the
	// joined sources, which the reader needs to tell the base's column of a name from
	// another source's. It is nil for every other statement, and when the facts do not
	// list the base.
	baseColumns []string
}

// compileTypedStatement is compileTypedSQL with the names the query asked for each
// result column. It is what a reader calls.
func compileTypedStatement(q dal.StructuredQuery, dialect typedDialect, facts typedCatalogFacts) (typedStatement, error) {
	if dialect == nil {
		return typedStatement{}, errors.New("typed SQL compiler requires a dialect")
	}
	if q == nil || q.From() == nil || q.From().Base() == nil {
		return typedStatement{}, errors.New("typed SQL query requires a source")
	}
	if q.StartFrom() != "" || q.StartAfter() != "" {
		return typedStatement{}, typedUnsupported("cursors (startFrom, startAfter)")
	}
	if dal.HasSubquery(q) {
		return typedStatement{}, typedUnsupported("subqueries run in the generic engine, not in one statement")
	}
	if err := typedRefuseMoney(q); err != nil {
		return typedStatement{}, err
	}
	if q.Limit() < 0 || q.Offset() < 0 {
		return typedStatement{}, errors.New("limit and offset must be non-negative")
	}
	if err := validateTypedAggregation(q, dialect); err != nil {
		return typedStatement{}, err
	}
	if len(q.From().Joins()) != 0 {
		if err := validateTypedJoins(q.From()); err != nil {
			return typedStatement{}, err
		}
		if err := validateTypedJoinSources(q.From(), "from"); err != nil {
			return typedStatement{}, err
		}
	} else if err := validateTypedScanRestated(q); err != nil {
		return typedStatement{}, err
	}
	c := &typedCompiler{dialect: dialect, facts: facts}
	statement, err := c.query(q)
	if err != nil {
		return typedStatement{}, err
	}
	// The bound is on the statement, not on one IN list: lists and constants add
	// up, and the driver would fail the whole statement past the limit.
	if len(statement.args) > typedMaxArguments {
		return typedStatement{}, typedUnsupported("the statement binds %d values, over the %d one statement may bind", len(statement.args), typedMaxArguments)
	}
	statement.text, err = numberTypedPlaceholders(statement.text, dialect.placeholderStyle(), len(statement.args))
	if err != nil {
		return typedStatement{}, err
	}
	return statement, nil
}

// typedRefuseMoney refuses a query that carries the DTQL money option, which asks for
// exact decimal results (dtql/query.go, Money()). DALgo computes those in its federated
// engine, in decimal text with half-even rounding; a statement would compute SUM, AVG and
// division in double precision, so honouring the option here would be approximating it.
//
// The option is read from the query it is handed. A caller that wraps the query before
// it reaches the compiler (dal.WithColumns and the key order of the records reader do)
// hides it, so such a caller reads it from the query it was given first.
func typedRefuseMoney(q dal.StructuredQuery) error {
	if m, ok := q.(interface{ Money() *dal.MoneyConfig }); ok && m.Money() != nil {
		return typedUnsupported("the money option (exact decimal results) is computed by DALgo's federated engine, not in one statement")
	}
	return nil
}

// errTypedInvalidAggregation is the whole of what the compiler says when
// dal.ValidateAggregation rejects a query. That validation words its message with
// the query's own expressions, constants included (selected expression "(a +
// 'secret')" is neither aggregated nor present in GROUP BY), and a server logs
// compile errors, so the message is dropped. It rejects for more reasons than
// grouping (SUM(*), COUNT(DISTINCT *), DISTINCT on MIN or MAX, a wildcard in an
// aggregate select, an unsupported aggregate or condition, a nil expression), so
// the replacement does not say which rule failed; it points at the validator,
// which the caller can run on the query to be told.
var errTypedInvalidAggregation = errors.New("invalid aggregation: the query breaks DALgo's aggregation rules (grouping of selected, HAVING and ORDER BY expressions, alias uniqueness, or an aggregate form DALgo does not accept); " +
	"dal.ValidateAggregation(q) names the rule, and its text is left out here because it can quote the query's constants")

func validateTypedAggregation(q dal.StructuredQuery, dialect typedDialect) error {
	if dal.ValidateAggregation(q) != nil {
		return errTypedInvalidAggregation
	}
	plan, err := dal.PlanAggregation(q, dialect.capabilities())
	if err != nil {
		// The aggregation is valid, so the one refusal left is FIRST or LAST,
		// which need an input order no SQL engine guarantees.
		return fmt.Errorf("%w: %v", dal.ErrNotSupported, err)
	}
	if plan.Strategy != dal.AggregationNative {
		return typedUnsupported("the dialect does not run this aggregation natively: %s", plan.Reason)
	}
	return nil
}

// validateTypedJoins applies DALgo's structural join validation. A join type or
// operator the model reserves for later is a missing feature, not a malformed
// query, so those two categories also match dal.ErrNotSupported.
func validateTypedJoins(from dal.FromSource) error {
	err := dal.ValidateJoinTree(from)
	var joinErr *dal.JoinValidationError
	if errors.As(err, &joinErr) && (joinErr.Category == "join_type" || joinErr.Category == "join_operator") {
		return fmt.Errorf("%w: %w", dal.ErrNotSupported, err)
	}
	return err
}

// validateTypedJoinSources refuses, in a statement with joins, the source
// options a single SQL join cannot honour. DALgo's generic join applies a
// source's scan (CollectionRef.WithScan: an order and a row limit taken before
// the relation joins) and reads a source in a named database from that database;
// rendering either as the whole local table would join rows the generic join
// never sees. Returning ErrNotSupported sends the query to the generic join.
//
// A lone source is different, and validateTypedScanRestated decides what a
// scan-bounded one compiles to.
//
// Join algorithm hints (JoinedSource.Algorithms) are not read: they are
// preferences, DALgo's native join contract lets the engine choose its own plan
// (dal.NativeJoinProvider), and the server picks the algorithm.
func validateTypedJoinSources(from dal.FromSource, path string) error {
	if collection, err := typedCollection(from.Base()); err == nil {
		if collection.ScanLimit() > 0 || len(collection.ScanOrders()) != 0 {
			return typedUnsupported("%s: join_plan: a scan-bounded source needs the generic engine", path)
		}
		if collection.Database() != "" {
			return typedUnsupported("%s: join_plan: a source in a named database needs the generic engine", path)
		}
	}
	for i, join := range from.Joins() {
		child := join.From()
		if child == nil {
			child = dal.From(join.RecordsetSource)
		}
		if err := validateTypedJoinSources(child, fmt.Sprintf("%s.joins[%d].from", path, i)); err != nil {
			return err
		}
	}
	return nil
}

// validateTypedScanRestated decides what a statement over one scan-bounded
// source (CollectionRef.WithScan: an order and a row limit) compiles to. A scan
// means: take the ordered, bounded rows first, then apply the rest of the
// statement to them. A SELECT over the table can say that only when the
// statement is the bounded read itself, which is the leaf query DALgo builds for
// a bounded relation (dal/join_execute.go, dal/federated_query.go,
// dal/federated_stream.go): the scan restated as ORDER BY and LIMIT, with no
// filter, grouping or offset. Anything else would be rendered over the whole
// table (a WHERE would filter before the bound, not after it), so it returns
// ErrNotSupported. What happens next depends on who asked: a plain read goes
// straight to the adapter (dal/join_execute.go), so the refusal is the read's
// error and nothing falls back to DALgo's generic engine; only the federated
// executor runs such a query in the generic engine, and it decides that itself,
// before it asks the adapter (dal/federated_query.go).
//
// The statement restates the scan when ORDER BY equals the scan's orders item
// by item (after select aliases are resolved, as orderBy resolves them), in the
// same direction, and, if the scan has a limit, the statement's LIMIT is set and
// no larger. The projection is free: a projection after a limit reads the same
// rows. A statement limit below the scan's is the federated executor's own
// tightening and is accepted.
//
// What is compared is the expression orderBy writes, which the server reads as
// that expression only if no select-list output shares a bare written name with
// it. This check cannot see that, because it runs before the select list is
// rendered; typedCheckOrderByName runs on the same item when the statement is
// compiled and refuses such a statement, so a statement that passes both sorts by
// the scan's own column. The comparison here is on the query's spelling, so under
// a dialect that folds case the scan order Total and an ORDER BY TOTAL are
// different and the statement is refused: it fails closed, and never accepts a
// pair that is not the same expression.
//
// Database() is not checked. A lone source in a named database is right only
// because the federated executor already routed the leaf to that database's
// connection, which the compiler cannot verify: it renders the table by its own
// name. The compiler would need the connection's own database name to compare, and
// neither the readers nor DbOptions have one (DbOptions.ID names the adapter, not a
// database), so the check is not made.
func validateTypedScanRestated(q dal.StructuredQuery) error {
	collection, err := typedCollection(q.From().Base())
	if err != nil || (collection.ScanLimit() == 0 && len(collection.ScanOrders()) == 0) {
		return nil // no scan; typedCompiler.table refuses a source it cannot render
	}
	refuse := func(why string) error {
		return typedUnsupported("from: a scan-bounded source compiles only when the statement restates its scan: %s", why)
	}
	switch {
	case q.Where() != nil:
		return refuse("WHERE filters after the scan, not before it")
	case dal.HasAggregation(q):
		return refuse("aggregation runs over the scanned rows, not the table")
	case q.Offset() != 0:
		return refuse("OFFSET skips rows of the scan")
	case collection.ScanLimit() > 0 && (q.Limit() == 0 || q.Limit() > collection.ScanLimit()):
		return refuse("LIMIT is missing or larger than the scan limit")
	}
	scanOrders, orders := collection.ScanOrders(), q.OrderBy()
	if len(orders) != len(scanOrders) {
		return refuse("ORDER BY differs from the scan order")
	}
	aliases := typedAliases(q.Columns())
	for i, order := range orders {
		rewriter := typedAliasRewriter{aliases: aliases}
		resolved := rewriter.expression(order.Expression())
		if order.Descending() != scanOrders[i].Descending() || !typedSameExpression(resolved, scanOrders[i].Expression()) {
			return refuse("ORDER BY differs from the scan order")
		}
	}
	return nil
}

// typedAliases maps each select alias to the expression it names.
func typedAliases(columns []dal.Column) map[string]dal.Expression {
	aliases := make(map[string]dal.Expression, len(columns))
	for _, column := range columns {
		if column.Alias != "" {
			aliases[column.Alias] = column.Expression
		}
	}
	return aliases
}

// typedCompiler carries the state of one compileTypedSQL call.
type typedCompiler struct {
	dialect typedDialect
	facts   typedCatalogFacts

	// sources maps every source identity visible to the statement (the alias,
	// else the table name) to the table it names.
	sources map[string]typedSource
	// nullable holds identities on the nullable side of an outer join: a column
	// that is NOT NULL in the catalog can still be NULL there.
	nullable map[string]bool
	// base is the identity of the first FROM source.
	base     string
	hasJoins bool
	// noAggregates is set while compiling WHERE, where aggregates are illegal.
	noAggregates bool
}

// typedSource is one source visible to a statement.
type typedSource struct {
	// quoted is the source's identity, quoted once when its FROM entry was
	// rendered, ready to qualify a field.
	quoted string
	table  typedSourceName
}

type typedSelectItem struct {
	// expression is nil for the bare "*" of a query with no columns.
	expression dal.Expression
	sql        string
	args       []any
	// output is the quoted name the server gives the item's output column, as the
	// dialect wrote it: the alias, else the column's own name, else the text the
	// compiler wrote after AS. It is empty for the bare "*", whose outputs are the
	// input columns themselves. It is the quoted text, not the query's spelling,
	// because the server compares what is written: a dialect that folds case in
	// quoteIdent writes Total and TOTAL as one identifier.
	output string
	// asked is the name the query asked for this output: the alias, else the field's
	// own name, else the expression's text; for a column a wildcard expanded, the
	// catalog's name. Two items can write one output name while asking two different
	// ones when the dialect folds case. Empty for the bare "*".
	asked string
	// column is, when expression is a field, that field's quoted name on its own
	// (no source qualifier, no alias), as the dialect wrote it. Empty otherwise.
	// An item whose output equals the bare quoted name of a column and whose
	// column equals it too is that column itself.
	column string
}

// query assembles the statement. FROM is rendered first because it fixes which
// sources are visible, but it binds no value, so the arguments still follow the
// text order: select list, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT and OFFSET.
func (c *typedCompiler) query(q dal.StructuredQuery) (typedStatement, error) {
	relation, err := c.relation(q.From(), "from", nil)
	if err != nil {
		return typedStatement{}, err
	}
	c.sources, c.nullable = relation.sources, relation.nullable
	c.base = typedIdentity(q.From().Base())
	c.hasJoins = len(q.From().Joins()) != 0

	columns := q.Columns()
	if dal.HasAggregation(q) {
		columns = dal.EffectiveAggregationColumns(q)
	}
	items, err := c.selectItems(q, columns)
	if err != nil {
		return typedStatement{}, err
	}
	aliases := typedAliases(columns)

	var text strings.Builder
	var args []any
	text.WriteString("SELECT ")
	for i, item := range items {
		if i > 0 {
			text.WriteString(", ")
		}
		text.WriteString(item.sql)
		args = append(args, item.args...)
	}
	text.WriteString(" FROM ")
	text.WriteString(relation.sql)

	// Arguments are collected in the order the clauses appear in the text.
	for _, clause := range []func() (string, []any, error){
		func() (string, []any, error) { return c.where(q.Where()) },
		func() (string, []any, error) { return c.groupBy(q.GroupBy(), items) },
		func() (string, []any, error) { return c.having(q.Having(), aliases) },
		func() (string, []any, error) { return c.orderBy(q.OrderBy(), items, aliases, len(q.GroupBy()) != 0) },
		func() (string, []any, error) {
			clause, limitArgs := c.dialect.limitOffset(q.Limit(), q.Offset())
			if err := c.checkFragment(clause, len(limitArgs)); err != nil {
				return "", nil, err
			}
			if clause == "" {
				return "", nil, nil
			}
			return " " + clause, limitArgs, nil
		},
	} {
		clauseText, clauseArgs, err := clause()
		if err != nil {
			return typedStatement{}, err
		}
		text.WriteString(clauseText)
		args = append(args, clauseArgs...)
	}
	statement := typedStatement{text: text.String(), args: args}
	for _, item := range items {
		if item.asked == "" { // the bare *: its columns are the catalog's own
			statement.outputs = nil
			break
		}
		statement.outputs = append(statement.outputs, item.asked)
	}
	if c.hasJoins && len(items) == 1 && items[0].expression == nil {
		if base, known := c.facts.source(c.sources[c.base].table); known {
			for _, column := range base.Columns {
				statement.baseColumns = append(statement.baseColumns, column.Name)
			}
		}
	}
	return statement, nil
}

// selectItems renders the SELECT list. Aggregate queries pass their effective
// columns (the group keys when none were named).
func (c *typedCompiler) selectItems(q dal.StructuredQuery, columns []dal.Column) ([]typedSelectItem, error) {
	if len(columns) == 0 {
		return []typedSelectItem{{sql: "*"}}, nil
	}
	wildcard, err := planWildcardProjection(q)
	if err != nil {
		return nil, typedUnsupported("%v", err)
	}
	var items []typedSelectItem
	joinOutputs := map[string]dal.Expression{}
	for i, column := range columns {
		if column.Wildcard != nil {
			expanded, err := c.expandWildcard(wildcard)
			if err != nil {
				return nil, fmt.Errorf("column %d: %w", i, err)
			}
			items = append(items, expanded...)
			continue
		}
		item, err := c.selectItem(column)
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", i, err)
		}
		// A wildcard is refused in a statement with joins (planWildcardProjection), so
		// only the columns named one by one can write a name twice there.
		if err := c.checkJoinOutputIsNew(joinOutputs, i, item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, typedUnsupported("the wildcard projection excludes every column")
	}
	return items, c.checkOutputsStayApart(items)
}

// checkJoinOutputIsNew refuses, in a statement with joins, an output name that an
// earlier column already writes for another expression. A result is read by name, one
// value per name, so a.id and r.id in one select list are two columns of the statement
// of which the reader keeps one and drops the other without a word. DALgo's generic
// engine refuses the same query with join_field at columns[n]: duplicate output name,
// and so does this, with the same kind of error: it is no refusal to fall back on, as
// the generic engine would refuse too. A name repeated for the same expression is one
// column asked twice, which compiles as it does for a lone source, where a repeated name
// is left as it was. seen holds, by written output name, the expression that first wrote
// it; column is the position of the select-list column that item comes from, as DALgo's
// error names it.
func (c *typedCompiler) checkJoinOutputIsNew(seen map[string]dal.Expression, column int, item typedSelectItem) error {
	if !c.hasJoins {
		return nil
	}
	if first, repeated := seen[item.output]; repeated && !typedSameExpression(first, item.expression) {
		return errJoinOutputRepeated(column, item.asked)
	}
	seen[item.output] = item.expression
	return nil
}

// errJoinOutputRepeated is the refusal of a select list that gives two expressions of a
// join one output name, in the words and with the category DALgo's generic engine uses
// for the same query. The name is quoted: an alias is the caller's.
func errJoinOutputRepeated(column int, name string) error {
	return &dal.JoinValidationError{Category: "join_field", Path: fmt.Sprintf("columns[%d]", column), Message: "duplicate output name " + strconv.Quote(name)}
}

// checkOutputsStayApart refuses a select list in which two outputs the query names
// differently are one name once the dialect writes them: a dialect that folds case
// writes Total and TOTAL as "total". The server then returns two columns of one name
// and the reader, which keys a record by name, keeps one of them. Naming the same
// output twice is not new (the query asked for one name), and with a dialect that
// writes names as asked two names are never one, so neither is refused.
func (c *typedCompiler) checkOutputsStayApart(items []typedSelectItem) error {
	asked := make(map[string]string, len(items))
	for _, item := range items {
		if previous, seen := asked[item.output]; seen && previous != item.asked {
			return typedUnsupported("the select list names %q and %q, which are one name once the dialect writes them (%s)", previous, item.asked, item.output)
		}
		asked[item.output] = item.asked
	}
	return nil
}

// expandWildcard replaces `*` with the source's columns from the catalog
// facts, minus the exclusions. No value is read to do it.
func (c *typedCompiler) expandWildcard(wildcard *wildcardProjectionPlan) ([]typedSelectItem, error) {
	source, ok := c.facts.source(c.sources[c.base].table)
	if !ok {
		return nil, typedUnsupported("a wildcard projection needs catalog facts for its source")
	}
	// The statement writes the dialect's folded names, so an exclusion is matched
	// against them folded the same way. Matched as the query spells it, an exclusion
	// Email would leave catalog column email in the list of a dialect that folds case
	// (dal.WildcardProjection.Excludes is exact for a plain exclusion), and the select-all
	// would return a column its caller excluded. A mask is case-insensitive already.
	projection := *wildcard.projection
	if c.facts.Fold != nil {
		projection.Exclude = make([]string, len(wildcard.projection.Exclude))
		for i, exclusion := range wildcard.projection.Exclude {
			projection.Exclude[i] = c.facts.fold(exclusion)
		}
	}
	items := make([]typedSelectItem, 0, len(source.Columns))
	for _, column := range source.Columns {
		// The statement writes the dialect's folded name, so a column whose own name
		// is not that name cannot be listed, and writing the folded one would return
		// another column, or the same one twice. The check is on every column of the
		// source, before the exclusions are applied: a select-all over a source that
		// lists such a column is refused even when it excludes that column, which
		// keeps the rule simple and never writes a statement over a catalog the
		// dialect cannot spell.
		if !c.facts.addressable(column.Name) {
			return nil, typedUnsupported("a wildcard projection cannot list catalog column %q: the dialect writes its name in another form", column.Name)
		}
		if projection.Excludes(column.Name) {
			continue
		}
		quoted, err := c.quote(column.Name)
		if err != nil {
			return nil, err
		}
		items = append(items, typedSelectItem{expression: dal.NewFieldRef("", column.Name), sql: quoted, output: quoted, column: quoted, asked: column.Name})
	}
	return items, nil
}

// selectItem renders one column. A result column keeps the name DALgo's
// generic engine gives it: the alias, else the field name, else the
// expression's own text. That last name must not contain a value, so an
// unaliased expression that carries a constant is refused.
func (c *typedCompiler) selectItem(column dal.Column) (typedSelectItem, error) {
	var item typedSelectItem
	field, isField := column.Expression.(dal.FieldRef)
	if isField {
		// The field's own written name, apart from any source qualifier or alias.
		quoted, err := c.quote(field.Name())
		if err != nil {
			return typedSelectItem{}, err
		}
		item.column = quoted
	}
	sql, args, err := c.expr(column.Expression)
	if err != nil {
		return typedSelectItem{}, err
	}
	item.expression, item.sql, item.args = column.Expression, sql, args
	name := column.Alias
	if name == "" {
		if isField {
			item.output, item.asked = item.column, field.Name()
			return item, nil
		}
		if len(args) != 0 {
			return typedSelectItem{}, typedUnsupported("an expression carrying a constant needs an alias")
		}
		name = column.Expression.String()
	}
	quoted, err := c.quoteOutputName(name)
	if err != nil {
		return typedSelectItem{}, err
	}
	item.sql += " AS " + quoted
	item.output, item.asked = quoted, name
	return item, nil
}

// quoteOutputName quotes the name of a result column: an alias, or the text of an
// unaliased expression. Such a name only labels the column. A name the engine
// cannot spell because it is too long is therefore no malformed query, and DALgo's
// generic engine, which writes no name into a statement, could label the column
// itself: the name is refused as unsupported (dal.ErrNotSupported).
//
// What that refusal does depends on the query, because DALgo falls back only where it
// has a fallback. A join is asked of the adapter first (CanExecuteJoin compiles the
// whole query), and DALgo runs the generic join when the adapter declines it, so a long
// alias in a join is served by the generic engine. A query over one source goes to the
// adapter with no such step, so a long alias there fails the read with this error and
// the caller shortens the alias.
//
// Any other fault (empty, a NUL byte, not UTF-8) stays a plain error, as for every
// other name. The message carries the length and the limit, never the name.
func (c *typedCompiler) quoteOutputName(name string) (string, error) {
	quoted, err := c.quote(name)
	var tooLong *typedIdentifierTooLongError
	if errors.As(err, &tooLong) {
		return "", typedUnsupported("an output name is too long for the engine, which cannot spell it as a column label: %v", err)
	}
	return quoted, err
}

func (c *typedCompiler) where(condition dal.Condition) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	c.noAggregates = true
	defer func() { c.noAggregates = false }()
	sql, args, err := c.condition(condition)
	if err != nil {
		return "", nil, fmt.Errorf("where: %w", err)
	}
	return " WHERE " + sql, args, nil
}

func (c *typedCompiler) groupBy(groups []dal.Expression, items []typedSelectItem) (string, []any, error) {
	if len(groups) == 0 {
		return "", nil, nil
	}
	parts := make([]string, len(groups))
	var args []any
	for i, group := range groups {
		sql, values, err := c.byExpression(group, items)
		if err != nil {
			return "", nil, fmt.Errorf("groupBy %d: %w", i, err)
		}
		parts[i] = sql
		args = append(args, values...)
	}
	return " GROUP BY " + strings.Join(parts, ", "), args, nil
}

// having rewrites select aliases to their expressions, as PostgreSQL does not
// let HAVING see output names. A grouped expression that carries a constant
// cannot be referenced that way (see byExpression), and HAVING has no ordinal
// form, so it is refused.
func (c *typedCompiler) having(condition dal.Condition, aliases map[string]dal.Expression) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	rewriter := typedAliasRewriter{aliases: aliases}
	rewritten := rewriter.condition(condition)
	if rewriter.groupedConstant != "" {
		return "", nil, typedUnsupported("HAVING refers to %q, a grouped expression carrying a constant that the server cannot match to GROUP BY", rewriter.groupedConstant)
	}
	sql, args, err := c.condition(rewritten)
	if err != nil {
		return "", nil, fmt.Errorf("having: %w", err)
	}
	return " HAVING " + sql, args, nil
}

// orderBy renders ORDER BY, rewriting select aliases to their expressions as
// having does. grouped says the statement has a GROUP BY.
//
// An item that is a whole alias of a grouped expression carrying a constant is
// referenced by position (see byExpression). The same alias nested in a larger
// expression has no position to refer to, and bound inline it would be a fresh
// parameter that the server cannot match to GROUP BY, so it is refused as HAVING
// refuses it. Without a GROUP BY the inline form is valid and stays.
//
// An item that rewrites to a bare column name is checked by
// typedCheckOrderByName: the server reads a bare ORDER BY name as a select-list
// output first, so the written name must not be one. The check compares the
// quoted names the dialect wrote, so a dialect that folds case is covered.
func (c *typedCompiler) orderBy(orders []dal.OrderExpression, items []typedSelectItem, aliases map[string]dal.Expression, grouped bool) (string, []any, error) {
	if len(orders) == 0 {
		return "", nil, nil
	}
	parts := make([]string, len(orders))
	var args []any
	for i, order := range orders {
		// One rewriter per item: what an earlier item resolved must not make a
		// later, unrelated item look like it refers to a grouped constant.
		rewriter := typedAliasRewriter{aliases: aliases}
		expression := rewriter.expression(order.Expression())
		sql, values, err := c.byExpression(expression, items)
		if err != nil {
			return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
		}
		if err := typedCheckOrderByName(expression, sql, items); err != nil {
			return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
		}
		if grouped && rewriter.groupedConstant != "" && len(values) != 0 {
			return "", nil, fmt.Errorf("orderBy %d: %w", i, typedUnsupported("ORDER BY refers to %q, a grouped expression carrying a constant that the server cannot match to GROUP BY", rewriter.groupedConstant))
		}
		item := c.dialect.orderItem(sql, order.Descending(), c.notNull(expression))
		if err := c.checkFragment(item, 0, sql); err != nil {
			return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
		}
		parts[i] = item
		args = append(args, values...)
	}
	return " ORDER BY " + strings.Join(parts, ", "), args, nil
}

// typedCheckOrderByName refuses an ORDER BY item whose written form is a bare
// column name that the server would read as a select-list output of that name
// when that output is another expression. PostgreSQL matches a name that stands
// alone in ORDER BY against output columns first and against input columns only
// when none has the name; a qualified name, and a name inside an expression, are
// always input columns. Select aliases are rewritten to the aliased expression
// (typedAliasRewriter), so `SELECT Total AS a, Name AS Total ORDER BY a` would be
// written ORDER BY "Total" and sort by Name, and, in a grouped query,
// `SELECT a AS x, COUNT(b) AS a GROUP BY a ORDER BY x` would sort by the count
// where DALgo sorts by the group key. Every output of that name must be the
// column itself, so any other one, not just the first, refuses the item.
//
// The comparison is on what was written, never on the query's spelling: written
// is the quoted name byExpression put in the text, item.output is the quoted name
// the dialect gave the output, and the item is the column itself when item.column,
// the quoted name of its own field, equals it. A dialect whose quoteIdent folds
// case writes Total and total as one identifier, and the server compares the
// written one, so `SELECT Total AS a, Name AS total ORDER BY a` is the capture
// above even though the query spells the column and the alias differently. For a
// dialect that quotes names as written, equal text and equal spelling coincide.
//
// An unaliased expression is named by its text (typedSelectItem.output holds the
// quoted text the compiler wrote after AS), so it counts too. A qualified field
// cannot be captured, and a join statement writes no bare name (c.expr refuses it
// first), so a source on the item or on the output needs no comparison here.
//
// The refusal is an error the caller of a one-source read sees: DALgo has no generic
// engine to send such a query to. Writing the column qualified by the base source would
// keep it native; refusing is the narrower change, and the shape is rare.
func typedCheckOrderByName(expression dal.Expression, written string, items []typedSelectItem) error {
	field, ok := expression.(dal.FieldRef)
	if !ok || field.Source() != "" {
		return nil
	}
	for _, item := range items {
		if item.output == written && item.column != written {
			return typedUnsupported("ORDER BY resolves to column %q (written %s), which the server reads as the select-list output of that name, a different expression", field.Name(), written)
		}
	}
	return nil
}

// byExpression compiles a GROUP BY or ORDER BY expression.
//
// A constant-only expression neither groups nor orders anything, and the server
// cannot infer the type of a bound parameter standing alone there, so it is
// refused. An expression that carries constants and is also selected is
// referenced by its position in the select list: the server matches GROUP BY
// and select expressions structurally, and two bound parameters never compare
// equal however equal their values.
func (c *typedCompiler) byExpression(expression dal.Expression, items []typedSelectItem) (string, []any, error) {
	if typedConstantOnly(expression) {
		return "", nil, typedUnsupported("a constant-only expression has no effect here")
	}
	sql, args, err := c.expr(expression)
	if err != nil {
		return "", nil, err
	}
	if len(args) != 0 {
		if ordinal, ok := typedOrdinal(items, expression); ok {
			return strconv.Itoa(ordinal), nil, nil
		}
	}
	return sql, args, nil
}

// typedOrdinal returns the 1-based position of the selected expression that is
// target. Sameness is structural (typedSameExpression), not by text: DALgo's
// aggregation validation compares expressions by String(), which is empty for
// every infinity and NaN and identical for every driver.Valuer without exported
// fields, so two different constants can share a text, and a match by text
// would order or group by the selected expression instead of the written one.
func typedOrdinal(items []typedSelectItem, target dal.Expression) (int, bool) {
	for i, item := range items {
		if item.expression != nil && typedSameExpression(item.expression, target) {
			return i + 1, true
		}
	}
	return 0, false
}

// typedSameExpression walks two expressions in parallel and reports whether they
// are the same: the same fields, operators and aggregate calls, and constants
// that are the same by typedSameConstant. A node the compiler cannot render is
// never the same as anything.
func typedSameExpression(a, b dal.Expression) bool {
	switch x := a.(type) {
	case dal.FieldRef:
		y, ok := b.(dal.FieldRef)
		return ok && x.Equal(y)
	case dal.Constant:
		y, ok := b.(dal.Constant)
		return ok && typedSameConstant(x.Value, y.Value)
	case dal.BinaryExpression:
		y, ok := b.(dal.BinaryExpression)
		return ok && x.Operator == y.Operator && typedSameExpression(x.Left, y.Left) && typedSameExpression(x.Right, y.Right)
	case dal.AggregateFunc:
		y, ok := b.(dal.AggregateFunc)
		if !ok || !strings.EqualFold(x.FuncName(), y.FuncName()) || typedIsDistinct(x) != typedIsDistinct(y) || len(x.FuncArgs()) != len(y.FuncArgs()) {
			return false
		}
		for i, argument := range x.FuncArgs() {
			if !typedSameExpression(argument, y.FuncArgs()[i]) {
				return false
			}
		}
		return true
	case dal.StarExpression:
		y, ok := b.(dal.StarExpression)
		return ok && x.IsStar() == y.IsStar()
	}
	return false
}

func typedIsDistinct(function dal.AggregateFunc) bool {
	d, ok := function.(dal.DistinctAggregateFunc)
	return ok && d.IsDistinct()
}

// typedSameConstant reports whether two constants bind the same value: the same
// kind (typedKindOf) and the same content. The Go type inside a kind does not
// count, because a dialect types a placeholder by kind only, so int8(5) and
// int64(5) are one constant; NaN equals NaN, because both bind NaN; and two
// instants are the same whatever their zone. A value the compiler refuses is
// never the same as anything.
func typedSameConstant(a, b any) bool {
	kindA, errA := typedKindOf(a)
	kindB, errB := typedKindOf(b)
	if errA != nil || errB != nil || kindA != kindB {
		return false
	}
	valueA, valueB := reflect.ValueOf(a), reflect.ValueOf(b)
	switch kindA {
	case typedValueNull:
		return true
	case typedValueInteger:
		return valueA.Int() == valueB.Int()
	case typedValueUnsigned:
		return valueA.Uint() == valueB.Uint()
	case typedValueFloat:
		floatA, floatB := valueA.Float(), valueB.Float()
		return floatA == floatB || (math.IsNaN(floatA) && math.IsNaN(floatB))
	case typedValueText:
		return valueA.String() == valueB.String()
	case typedValueBool:
		return valueA.Bool() == valueB.Bool()
	case typedValueBytes:
		return bytes.Equal(valueA.Bytes(), valueB.Bytes())
	case typedValueTime:
		return a.(time.Time).Equal(b.(time.Time))
	}
	return reflect.DeepEqual(a, b) // a driver.Valuer: its fields are all there is to compare
}

// notNull reports whether expression is a column the catalog says is NOT NULL
// and that no outer join can null out.
func (c *typedCompiler) notNull(expression dal.Expression) bool {
	field, ok := expression.(dal.FieldRef)
	if !ok {
		return false
	}
	column, identity, known := c.columnFact(field, c.sources)
	return known && column.NotNull && !c.nullable[identity]
}

// columnFact finds the catalog facts of a field, returning the identity of the
// source it belongs to. Every field reaching it was already rendered, so its
// source is visible; a name outside sources would only miss in the facts.
func (c *typedCompiler) columnFact(field dal.FieldRef, sources map[string]typedSource) (typedColumnFact, string, bool) {
	identity := field.Source()
	if identity == "" {
		identity = c.base
	}
	column, ok := c.facts.column(sources[identity].table, field.Name())
	return column, identity, ok
}

// typedRelation is a compiled FROM tree.
type typedRelation struct {
	sql      string
	sources  map[string]typedSource
	nullable map[string]bool
}

// relation renders a FROM tree. A nested right subtree is parenthesised so the
// engine keeps its LEFT/INNER grouping. The scope rules are those of the SQLite
// compiler: a subtree starts with an empty visible scope, and a nested ON that
// names an alias outside its subtree is refused because a parenthesised JOIN
// cannot represent it. outer lists the identities visible around this subtree.
// Join algorithm hints are not read (see validateTypedJoinSources).
func (c *typedCompiler) relation(from dal.FromSource, path string, outer map[string]struct{}) (typedRelation, error) {
	text, source, err := c.table(from.Base())
	if err != nil {
		return typedRelation{}, fmt.Errorf("%s: %w", path, err)
	}
	relation := typedRelation{
		sql:      text,
		sources:  map[string]typedSource{typedIdentity(from.Base()): source},
		nullable: map[string]bool{},
	}
	for i, join := range from.Joins() {
		joinPath := fmt.Sprintf("%s.joins[%d]", path, i)
		var keyword string
		switch join.JoinType() {
		case dal.JoinInner:
			keyword = " INNER JOIN "
		case dal.JoinLeft:
			keyword = " LEFT JOIN "
		default:
			return typedRelation{}, typedUnsupported("%s.type: join_type: only INNER and LEFT joins are supported", joinPath)
		}
		right := join.From()
		if right == nil {
			right = dal.From(join.RecordsetSource)
		}
		scope := make(map[string]struct{}, len(outer)+len(relation.sources))
		for identity := range outer {
			scope[identity] = struct{}{}
		}
		for identity := range relation.sources {
			scope[identity] = struct{}{}
		}
		child, err := c.relation(right, joinPath+".from", scope)
		if err != nil {
			return typedRelation{}, err
		}
		visible := maps.Clone(relation.sources)
		maps.Copy(visible, child.sources)
		on, err := c.joinOn(join.On(), visible, outer, joinPath+".on")
		if err != nil {
			return typedRelation{}, err
		}
		rightSQL := child.sql
		if len(right.Joins()) != 0 {
			rightSQL = "(" + rightSQL + ")"
		}
		relation.sql += keyword + rightSQL + " ON " + on
		relation.sources = visible
		maps.Copy(relation.nullable, child.nullable)
		if join.JoinType() == dal.JoinLeft {
			for identity := range child.sources {
				relation.nullable[identity] = true
			}
		}
	}
	return relation, nil
}

// joinOn renders a join's ON list: equalities between qualified fields. When
// the catalog knows both columns it also refuses a pair whose types the server
// would not compare, so the failure is a refusal here and not a server error.
func (c *typedCompiler) joinOn(conditions []dal.Condition, visible map[string]typedSource, outer map[string]struct{}, path string) (string, error) {
	if len(conditions) == 0 {
		return "", typedUnsupported("%s: join_shape: ON must not be empty", path)
	}
	parts := make([]string, len(conditions))
	for i, condition := range conditions {
		pair := fmt.Sprintf("%s[%d]", path, i)
		comparison, isComparison := condition.(dal.Comparison)
		left, leftIsField := comparison.Left.(dal.FieldRef)
		right, rightIsField := comparison.Right.(dal.FieldRef)
		if !isComparison || comparison.Operator != dal.Equal || !leftIsField || !rightIsField {
			return "", typedUnsupported("%s: join_operator: a JOIN needs an equality between two fields", pair)
		}
		leftSQL, err := c.onField(left, visible, outer, pair+".left")
		if err != nil {
			return "", err
		}
		rightSQL, err := c.onField(right, visible, outer, pair+".right")
		if err != nil {
			return "", err
		}
		leftColumn, _, leftKnown := c.columnFact(left, visible)
		rightColumn, _, rightKnown := c.columnFact(right, visible)
		if leftKnown && rightKnown && !typedJoinKeysComparable(leftColumn, rightColumn) {
			if leftColumn.NoJoinKey || rightColumn.NoJoinKey {
				return "", typedUnsupported("%s: join_plan: key types %q and %q cannot be compared in a JOIN", pair, leftColumn.DataType, rightColumn.DataType)
			}
			if typedCollationsConflict(leftColumn, rightColumn) {
				return "", typedUnsupported("%s: join_plan: the keys have different collations of their own, so the server cannot compare them", pair)
			}
			return "", typedUnsupported("%s: join_plan: key types differ (%q against %q)", pair, leftColumn.DataType, rightColumn.DataType)
		}
		parts[i] = leftSQL + " = " + rightSQL
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (c *typedCompiler) onField(field dal.FieldRef, visible map[string]typedSource, outer map[string]struct{}, path string) (string, error) {
	if _, correlated := outer[field.Source()]; correlated {
		return "", typedUnsupported("%s: join_plan: a nested ON refers to %q outside its subtree", path, field.Source())
	}
	sql, err := c.field(field, visible, true)
	if err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	return sql, nil
}

// table renders one FROM entry and describes the source it introduces.
func (c *typedCompiler) table(source dal.RecordsetSource) (string, typedSource, error) {
	collection, err := typedCollection(source)
	if err != nil {
		return "", typedSource{}, err
	}
	name, err := c.quote(collection.Name())
	if err != nil {
		return "", typedSource{}, fmt.Errorf("table name: %w", err)
	}
	text, identity := name, name
	if schema := collection.Schema(); schema != "" {
		quoted, err := c.quote(schema)
		if err != nil {
			return "", typedSource{}, fmt.Errorf("schema name: %w", err)
		}
		text = quoted + "." + text
	}
	if alias := collection.Alias(); alias != "" {
		identity, err = c.quote(alias)
		if err != nil {
			return "", typedSource{}, fmt.Errorf("source alias: %w", err)
		}
		text += " AS " + identity
	}
	return text, typedSource{quoted: identity, table: typedSourceName{Schema: collection.Schema(), Name: collection.Name()}}, nil
}

// typedCollection accepts the one source kind this compiler can render: a root
// collection (a table) in the connected database.
func typedCollection(source dal.RecordsetSource) (dal.CollectionRef, error) {
	var collection dal.CollectionRef
	switch typed := source.(type) {
	case dal.CollectionRef:
		collection = typed
	case *dal.CollectionRef:
		if typed == nil {
			return dal.CollectionRef{}, typedUnsupported("source %T is nil", source)
		}
		collection = *typed
	default:
		return dal.CollectionRef{}, typedUnsupported("source %T", source)
	}
	if collection.Parent() != nil {
		return dal.CollectionRef{}, typedUnsupported("parented collection sources")
	}
	return collection, nil
}

// typedIdentity is the name a source goes by inside the statement.
func typedIdentity(source dal.RecordsetSource) string {
	if alias := source.Alias(); alias != "" {
		return alias
	}
	return source.Name()
}

// typedQuerySources lists, without duplicates and in FROM order, the tables a
// query reads. The reader passes them to typedDialect.catalogFacts. A source
// the compiler would refuse is skipped; compileTypedSQL reports it.
func typedQuerySources(from dal.FromSource) []typedSourceName {
	var names []typedSourceName
	seen := map[typedSourceName]bool{}
	var walk func(dal.FromSource)
	walk = func(node dal.FromSource) {
		if node == nil {
			return
		}
		if collection, err := typedCollection(node.Base()); err == nil {
			name := typedSourceName{Schema: collection.Schema(), Name: collection.Name()}
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
		for _, join := range node.Joins() {
			child := join.From()
			if child == nil {
				child = dal.From(join.RecordsetSource)
			}
			walk(child)
		}
	}
	walk(from)
	return names
}

// field renders a field reference. Inside a JOIN every field must name its
// source; elsewhere an unqualified field belongs to the single FROM source.
func (c *typedCompiler) field(field dal.FieldRef, sources map[string]typedSource, requireQualified bool) (string, error) {
	// dal.DocumentID() names the record's identity, which adapters map to their
	// own identity field. The compiler has no mapping yet, and rendering it as a
	// column called __name__ would read a column that does not exist. dal.ID(name,
	// value) is a different field: it names a real column and is rendered as one.
	if field.IsID() && field.Name() == dal.DocumentID().Name() {
		return "", typedUnsupported("dal.DocumentID() has no column; mapping it to the primary key is not implemented")
	}
	name, err := c.quote(field.Name())
	if err != nil {
		return "", err
	}
	if field.Source() == "" {
		if requireQualified {
			return "", typedUnsupported("field %q must name a source in a JOIN query", field.Name())
		}
		// Without a qualifier the field belongs to the single FROM source.
		if err := c.checkNamesAColumn(field, name, sources[c.base]); err != nil {
			return "", err
		}
		return name, nil
	}
	source, ok := sources[field.Source()]
	if !ok {
		return "", fmt.Errorf("field %q references unknown source %q", field.Name(), field.Source())
	}
	if err := c.checkNamesAColumn(field, name, source); err != nil {
		return "", err
	}
	return source.quoted + "." + name, nil
}

// checkNamesAColumn refuses a field the server could read as something other than
// a column of its source. PostgreSQL reads a name that matches no column as the
// whole row of a source when it is the source's identity (a bare name: the alias,
// else the table name), and as a function of the row when it is qualified (x.f is
// f(x), documented as the table.func notation: to_json, row_to_json, to_jsonb,
// concat, count, quote_literal read alike). Either hands back every column of the
// row inside one value, past a field policy that checks names only, and the
// caller chooses both the alias and the field.
//
// Two cases, decided by what the catalog facts say about the source:
//
//   - The facts know the source: the field must be one of its columns (the lookup
//     matches the name the dialect writes, see typedCatalogFacts.column). Anything
//     else is a plain error naming the column, as an unknown source is. It holds
//     for a bare and for a qualified field, in every clause and in a join's ON.
//   - The facts do not know the source: a bare field whose written name is the
//     written identity of its source is refused, because without the column list
//     the compiler cannot tell the column of that name from the whole row. The
//     comparison is on the quoted texts, as the ORDER BY guard's is, so a dialect
//     that folds case in quoteIdent is covered. A qualified field cannot be
//     checked without facts, which is why the caller must pass facts for every
//     source it compiles (the typedDialect contract).
//
// An aliased source hides its table's own name, so only the alias is an identity
// here: a bare name equal to the table name is then no whole-row reference.
func (c *typedCompiler) checkNamesAColumn(field dal.FieldRef, written string, source typedSource) error {
	if _, known := c.facts.source(source.table); known {
		if _, ok := c.facts.column(source.table, field.Name()); !ok {
			// The message says what is true of the table. Why the compiler refuses
			// instead of leaving it to the server (which would read the name as the
			// row or as a function of it, not report a missing column) is the reason
			// given above, and it belongs in this comment, not in an error a caller
			// sees.
			return fmt.Errorf("field %q is not a column of source %s", field.Name(), source.quoted)
		}
		return nil
	}
	if field.Source() == "" && written == source.quoted {
		return typedUnsupported("field %q is written as the identity of its source %s, which the server reads as the whole row unless a column has that name, and no catalog facts say whether one does", field.Name(), source.quoted)
	}
	return nil
}

// quote is the only way an identifier reaches the SQL text. The dialect
// validates and quotes; the compiler then checks the result is one well-formed
// quoted identifier, so a faulty dialect cannot let a name escape its quotes.
func (c *typedCompiler) quote(name string) (string, error) {
	quoted, err := c.dialect.quoteIdent(name)
	if err != nil {
		return "", err
	}
	if !isQuotedTypedIdentifier(quoted, c.dialect.placeholderStyle().IdentQuote) {
		return "", errors.New("typed SQL: the dialect returned a malformed quoted identifier")
	}
	return quoted, nil
}

// bind turns one constant into its marker and argument.
func (c *typedCompiler) bind(value any) (string, []any, error) {
	if _, err := typedKindOf(value); err != nil {
		return "", nil, err
	}
	marker, arg, err := c.dialect.bind(value)
	if err != nil {
		return "", nil, err
	}
	if err := c.checkFragment(marker, 1); err != nil {
		return "", nil, err
	}
	return marker, []any{arg}, nil
}

// checkFragment applies operand rule 1 of the typedDialect contract (see
// checkTypedFragment) to one fragment the dialect wrote.
func (c *typedCompiler) checkFragment(fragment string, own int, operands ...string) error {
	return checkTypedFragment(fragment, c.dialect.placeholderStyle().IdentQuote, own, operands...)
}

func (c *typedCompiler) expr(expression dal.Expression) (string, []any, error) {
	switch e := expression.(type) {
	case dal.FieldRef:
		sql, err := c.field(e, c.sources, c.hasJoins)
		return sql, nil, err
	case dal.Constant:
		return c.bind(e.Value)
	case dal.AggregateFunc:
		return c.aggregate(e)
	case dal.BinaryExpression:
		return c.binary(e)
	default:
		return "", nil, typedUnsupported("expression %T", expression)
	}
}

func (c *typedCompiler) aggregate(function dal.AggregateFunc) (string, []any, error) {
	if c.noAggregates {
		return "", nil, errors.New("aggregate functions are not allowed in WHERE")
	}
	// The function name is emitted from the whitelist below, never from the
	// caller's spelling.
	name := strings.ToUpper(function.FuncName())
	switch name {
	case dal.COUNT, dal.SUM, dal.AVERAGE, dal.MIN, dal.MAX:
	case dal.FIRST, dal.LAST:
		return "", nil, typedUnsupported("%s needs a deterministic row order that SQL aggregation does not provide", name)
	default:
		return "", nil, typedUnsupported("an aggregate function DALgo does not define")
	}
	arguments := function.FuncArgs()
	if len(arguments) != 1 {
		return "", nil, fmt.Errorf("%s requires exactly one argument", name)
	}
	argument, args := "*", []any(nil)
	if star, ok := arguments[0].(dal.StarExpression); !ok || !star.IsStar() {
		var err error
		if argument, args, err = c.expr(arguments[0]); err != nil {
			return "", nil, err
		}
	}
	distinct := ""
	if d, ok := function.(dal.DistinctAggregateFunc); ok && d.IsDistinct() {
		distinct = "DISTINCT "
	}
	call := name + "(" + distinct + argument + ")"
	result := c.dialect.aggregateResult(name, call)
	if err := c.checkFragment(result, 0, call); err != nil {
		return "", nil, err
	}
	return result, args, nil
}

func (c *typedCompiler) binary(expression dal.BinaryExpression) (string, []any, error) {
	switch expression.Operator {
	case dal.Add, dal.Subtract, dal.Multiply, dal.Divide:
	default:
		return "", nil, typedUnsupported("arithmetic operator %q", expression.Operator)
	}
	left, leftArgs, err := c.expr(expression.Left)
	if err != nil {
		return "", nil, err
	}
	right, rightArgs, err := c.expr(expression.Right)
	if err != nil {
		return "", nil, err
	}
	args := slices.Concat(leftArgs, rightArgs)
	if expression.Operator == dal.Divide {
		if err := c.checkDivideOrder(left, right); err != nil {
			return "", nil, err
		}
		quotient := c.dialect.divide(left, right)
		if err := c.checkFragment(quotient, 0, left, right); err != nil {
			return "", nil, err
		}
		return quotient, args, nil
	}
	left, err = c.arithmeticOperand(left)
	if err != nil {
		return "", nil, err
	}
	right, err = c.arithmeticOperand(right)
	if err != nil {
		return "", nil, err
	}
	return "(" + left + " " + string(expression.Operator) + " " + right + ")", args, nil
}

// arithmeticOperand asks the dialect to render one operand of +, - or *, and
// applies operand rule 1 to what it wrote.
func (c *typedCompiler) arithmeticOperand(operand string) (string, error) {
	wrapped := c.dialect.arithmeticOperand(operand)
	if err := c.checkFragment(wrapped, 0, operand); err != nil {
		return "", err
	}
	return wrapped, nil
}

// typedProbeLeft and typedProbeRight are two operands that differ in text, carry
// one marker each, and neither holds the other. They never reach a statement;
// checkDivideOrder hands them to the dialect.
const (
	typedProbeLeft  = "(? + 0)"
	typedProbeRight = "(0 + ?)"
)

// checkDivideOrder closes the one gap in operand rule 1. checkTypedFragment sees
// text, so when both operands of a division carry a marker and render as the same
// text, as in (a + 1) / (a + 2), it cannot tell which operand is which. A dialect
// that places the right operand before the left, in a form whose value depends on
// the position such as (1.0 / right * left), would pass: the fragment is
// byte-identical to the same form with the operands in the order they were passed,
// (1.0 / left * right), so the text check cannot object, the arguments stay in
// order, the first marker takes the left operand's value, and the statement
// computes the inverse quotient. (A plain (right / left) writes the same bytes
// as (left / right) for equal texts and does no harm there, but the probe
// refuses it too, because divide must be a function of its operand texts
// alone.) For this case the
// compiler asks the dialect to divide two probe operands that differ and checks
// the order on them. An operand without a marker carries no argument, so a
// division with one is not probed and the dialect keeps its freedom to repeat or
// move it.
func (c *typedCompiler) checkDivideOrder(left, right string) error {
	quote := c.dialect.placeholderStyle().IdentQuote
	if countTypedMarkers(left, quote) == 0 || countTypedMarkers(right, quote) == 0 {
		return nil
	}
	if err := c.checkFragment(c.dialect.divide(typedProbeLeft, typedProbeRight), 0, typedProbeLeft, typedProbeRight); err != nil {
		return fmt.Errorf("divide, asked to divide two different operands that carry a value: %w", err)
	}
	return nil
}

func (c *typedCompiler) condition(condition dal.Condition) (string, []any, error) {
	switch cond := condition.(type) {
	case dal.Comparison:
		return c.comparison(cond)
	case dal.IsNullCondition:
		return c.isNull(cond)
	case dal.GroupCondition:
		return c.group(cond)
	default:
		return "", nil, typedUnsupported("condition %T", condition)
	}
}

var typedComparisonOperators = map[dal.Operator]string{
	dal.Equal:          "=",
	dal.GreaterThen:    ">",
	dal.GreaterOrEqual: ">=",
	dal.LessThen:       "<",
	dal.LessOrEqual:    "<=",
}

// comparison renders one comparison. `== null` is IS NULL, as in DALgo's other
// adapters; every other comparison against a bound NULL stays UNKNOWN, which is
// what SQL three-valued logic gives and what DALgo's generic engine does for
// WHERE.
func (c *typedCompiler) comparison(comparison dal.Comparison) (string, []any, error) {
	if typedConstantOnly(comparison.Left) {
		return "", nil, typedUnsupported("a comparison needs a field or aggregate on the left")
	}
	left, args, err := c.expr(comparison.Left)
	if err != nil {
		return "", nil, err
	}
	if dal.IsGroupOperator(comparison.Operator) {
		return c.membership(left, args, comparison)
	}
	operator, ok := typedComparisonOperators[comparison.Operator]
	if !ok {
		return "", nil, typedUnsupported("comparison operator %q", comparison.Operator)
	}
	if constant, isConstant := comparison.Right.(dal.Constant); isConstant && constant.Value == nil && comparison.Operator == dal.Equal {
		return left + " IS NULL", args, nil
	}
	right, rightArgs, err := c.expr(comparison.Right)
	if err != nil {
		return "", nil, err
	}
	return left + " " + operator + " " + right, slices.Concat(args, rightArgs), nil
}

// membership renders IN and NOT IN. The engine's own semantics are the right
// ones: a NULL in the list makes a non-match UNKNOWN, so NULL IN (NULL) is not
// true and its negation is not true either.
func (c *typedCompiler) membership(left string, leftArgs []any, comparison dal.Comparison) (string, []any, error) {
	array, ok := comparison.Right.(dal.Array)
	if !ok {
		return "", nil, typedUnsupported("%s needs an array on the right", comparison.Operator)
	}
	values, err := typedArrayValues(array.Value)
	if err != nil {
		return "", nil, err
	}
	negated := comparison.Operator == dal.NotIn
	if len(values) == 0 {
		constant := c.dialect.emptyIn(negated)
		if err := c.checkFragment(constant, 0); err != nil {
			return "", nil, err
		}
		return constant, nil, nil
	}
	if len(values) > typedMaxArguments {
		return "", nil, typedUnsupported("an IN list of %d values is too many to bind", len(values))
	}
	markers := make([]string, len(values))
	args := slices.Clone(leftArgs)
	for i, value := range values {
		marker, valueArgs, err := c.bind(value)
		if err != nil {
			return "", nil, fmt.Errorf("IN value %d: %w", i, err)
		}
		markers[i] = marker
		args = append(args, valueArgs...)
	}
	keyword := " IN ("
	if negated {
		keyword = " NOT IN ("
	}
	return left + keyword + strings.Join(markers, ", ") + ")", args, nil
}

func typedArrayValues(value any) ([]any, error) {
	array := reflect.ValueOf(value)
	if !array.IsValid() || (array.Kind() != reflect.Slice && array.Kind() != reflect.Array) {
		return nil, typedUnsupported("IN array has type %T", value)
	}
	values := make([]any, array.Len())
	for i := range values {
		values[i] = array.Index(i).Interface()
	}
	return values, nil
}

func (c *typedCompiler) isNull(condition dal.IsNullCondition) (string, []any, error) {
	if typedConstantOnly(condition.Operand()) {
		return "", nil, typedUnsupported("IS NULL needs a field or aggregate, not a constant")
	}
	operand, args, err := c.expr(condition.Operand())
	if err != nil {
		return "", nil, err
	}
	if condition.Negated() {
		return operand + " IS NOT NULL", args, nil
	}
	return operand + " IS NULL", args, nil
}

func (c *typedCompiler) group(group dal.GroupCondition) (string, []any, error) {
	var operator string
	switch group.Operator() {
	case dal.And:
		operator = "AND"
	case dal.Or:
		operator = "OR"
	default:
		return "", nil, typedUnsupported("group operator %q", group.Operator())
	}
	children := group.Conditions()
	if len(children) == 0 {
		return "", nil, errors.New("empty condition group")
	}
	parts := make([]string, len(children))
	var args []any
	for i, child := range children {
		part, childArgs, err := c.condition(child)
		if err != nil {
			return "", nil, err
		}
		parts[i] = part
		args = append(args, childArgs...)
	}
	return "(" + strings.Join(parts, " "+operator+" ") + ")", args, nil
}

// typedConstantOnly reports whether an expression is built from constants
// alone, with no field and no aggregate.
func typedConstantOnly(expression dal.Expression) bool {
	switch e := expression.(type) {
	case dal.Constant:
		return true
	case dal.BinaryExpression:
		return typedConstantOnly(e.Left) && typedConstantOnly(e.Right)
	}
	return false
}

func typedContainsAggregate(expression dal.Expression) bool {
	switch e := expression.(type) {
	case dal.AggregateFunc:
		return true
	case dal.BinaryExpression:
		return typedContainsAggregate(e.Left) || typedContainsAggregate(e.Right)
	}
	return false
}

// typedContainsConstant reports a constant outside any aggregate argument.
func typedContainsConstant(expression dal.Expression) bool {
	switch e := expression.(type) {
	case dal.Constant:
		return true
	case dal.BinaryExpression:
		return typedContainsConstant(e.Left) || typedContainsConstant(e.Right)
	}
	return false
}

// typedAliasRewriter replaces unqualified references to select aliases with the
// aliased expression. The replacement is not rewritten again, so an alias that
// shares a name with a column it selects cannot loop.
type typedAliasRewriter struct {
	aliases map[string]dal.Expression
	// groupedConstant is the first alias that stood for a non-aggregate
	// expression carrying a constant, which can only be a grouped expression.
	groupedConstant string
}

func (r *typedAliasRewriter) expression(expression dal.Expression) dal.Expression {
	switch e := expression.(type) {
	case dal.FieldRef:
		if replacement, ok := r.aliases[e.Name()]; ok && e.Source() == "" {
			if r.groupedConstant == "" && !typedContainsAggregate(replacement) && typedContainsConstant(replacement) {
				r.groupedConstant = e.Name()
			}
			return replacement
		}
	case dal.BinaryExpression:
		e.Left = r.expression(e.Left)
		e.Right = r.expression(e.Right)
		return e
	}
	return expression
}

func (r *typedAliasRewriter) condition(condition dal.Condition) dal.Condition {
	switch c := condition.(type) {
	case dal.Comparison:
		c.Left = r.expression(c.Left)
		c.Right = r.expression(c.Right)
		return c
	case dal.IsNullCondition:
		operand := r.expression(c.Operand())
		if c.Negated() {
			return dal.NewIsNotNullCondition(operand)
		}
		return dal.NewIsNullCondition(operand)
	case dal.GroupCondition:
		children := make([]dal.Condition, len(c.Conditions()))
		for i, child := range c.Conditions() {
			children[i] = r.condition(child)
		}
		return dal.NewGroupCondition(c.Operator(), children...)
	}
	return condition
}
