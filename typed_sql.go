package dalgo2sql

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// typedMaxArguments bounds the values one statement may bind. PostgreSQL's wire
// protocol counts parameters in 16 bits; no other engine allows more.
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
// dialect.quoteIdent, so the returned text depends on the shape of the query
// and never on the values in it. Anything the engine cannot run faithfully
// returns an error matching dal.ErrNotSupported rather than an approximation;
// DALgo's generic engine remains the place for those queries.
func compileTypedSQL(q dal.StructuredQuery, dialect typedDialect, facts typedCatalogFacts) (string, []any, error) {
	if dialect == nil {
		return "", nil, errors.New("typed SQL compiler requires a dialect")
	}
	if q == nil || q.From() == nil || q.From().Base() == nil {
		return "", nil, errors.New("typed SQL query requires a source")
	}
	if q.StartFrom() != "" || q.StartAfter() != "" {
		return "", nil, typedUnsupported("cursors (startFrom, startAfter)")
	}
	if dal.HasSubquery(q) {
		return "", nil, typedUnsupported("subqueries run in the generic engine, not in one statement")
	}
	if q.Limit() < 0 || q.Offset() < 0 {
		return "", nil, errors.New("limit and offset must be non-negative")
	}
	if err := validateTypedAggregation(q, dialect); err != nil {
		return "", nil, err
	}
	if len(q.From().Joins()) != 0 {
		if err := validateTypedJoins(q.From()); err != nil {
			return "", nil, err
		}
	}
	c := &typedCompiler{dialect: dialect, facts: facts}
	text, args, err := c.query(q)
	if err != nil {
		return "", nil, err
	}
	text, err = numberTypedPlaceholders(text, dialect.placeholderStyle(), len(args))
	if err != nil {
		return "", nil, err
	}
	return text, args, nil
}

func validateTypedAggregation(q dal.StructuredQuery, dialect typedDialect) error {
	if err := dal.ValidateAggregation(q); err != nil {
		return fmt.Errorf("invalid aggregation: %w", err)
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
}

// query assembles the statement. FROM is rendered first because it fixes which
// sources are visible, but it binds no value, so the arguments still follow the
// text order: select list, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT and OFFSET.
func (c *typedCompiler) query(q dal.StructuredQuery) (string, []any, error) {
	relation, err := c.relation(q.From(), "from", nil)
	if err != nil {
		return "", nil, err
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
		return "", nil, err
	}
	aliases := make(map[string]dal.Expression, len(columns))
	for _, column := range columns {
		if column.Alias != "" {
			aliases[column.Alias] = column.Expression
		}
	}

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
		func() (string, []any, error) { return c.orderBy(q.OrderBy(), items, aliases) },
		func() (string, []any, error) {
			clause, limitArgs := c.dialect.limitOffset(q.Limit(), q.Offset())
			if clause == "" {
				return "", nil, nil
			}
			return " " + clause, limitArgs, nil
		},
	} {
		clauseText, clauseArgs, err := clause()
		if err != nil {
			return "", nil, err
		}
		text.WriteString(clauseText)
		args = append(args, clauseArgs...)
	}
	return text.String(), args, nil
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
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, typedUnsupported("the wildcard projection excludes every column")
	}
	return items, nil
}

// expandWildcard replaces `*` with the source's columns from the catalog
// facts, minus the exclusions. No value is read to do it.
func (c *typedCompiler) expandWildcard(wildcard *wildcardProjectionPlan) ([]typedSelectItem, error) {
	source, ok := c.facts.source(c.sources[c.base].table)
	if !ok {
		return nil, typedUnsupported("a wildcard projection needs catalog facts for its source")
	}
	items := make([]typedSelectItem, 0, len(source.Columns))
	for _, column := range source.Columns {
		if wildcard.projection.Excludes(column.Name) {
			continue
		}
		quoted, err := c.quote(column.Name)
		if err != nil {
			return nil, err
		}
		items = append(items, typedSelectItem{expression: dal.NewFieldRef("", column.Name), sql: quoted})
	}
	return items, nil
}

// selectItem renders one column. A result column keeps the name DALgo's
// generic engine gives it: the alias, else the field name, else the
// expression's own text. That last name must not contain a value, so an
// unaliased expression that carries a constant is refused.
func (c *typedCompiler) selectItem(column dal.Column) (typedSelectItem, error) {
	sql, args, err := c.expr(column.Expression)
	if err != nil {
		return typedSelectItem{}, err
	}
	item := typedSelectItem{expression: column.Expression, sql: sql, args: args}
	name := column.Alias
	if name == "" {
		if _, isField := column.Expression.(dal.FieldRef); isField {
			return item, nil
		}
		if len(args) != 0 {
			return typedSelectItem{}, typedUnsupported("an expression carrying a constant needs an alias")
		}
		name = column.Expression.String()
	}
	quoted, err := c.quote(name)
	if err != nil {
		return typedSelectItem{}, err
	}
	item.sql += " AS " + quoted
	return item, nil
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

func (c *typedCompiler) orderBy(orders []dal.OrderExpression, items []typedSelectItem, aliases map[string]dal.Expression) (string, []any, error) {
	if len(orders) == 0 {
		return "", nil, nil
	}
	rewriter := typedAliasRewriter{aliases: aliases}
	parts := make([]string, len(orders))
	var args []any
	for i, order := range orders {
		expression := rewriter.expression(order.Expression())
		sql, values, err := c.byExpression(expression, items)
		if err != nil {
			return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
		}
		parts[i] = c.dialect.orderItem(sql, order.Descending(), c.notNull(expression))
		args = append(args, values...)
	}
	return " ORDER BY " + strings.Join(parts, ", "), args, nil
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

// typedOrdinal returns the 1-based position of the selected expression whose
// text equals target's, the same equality DALgo's aggregation validation uses.
func typedOrdinal(items []typedSelectItem, target dal.Expression) (int, bool) {
	key := target.String()
	for i, item := range items {
		if item.expression != nil && item.expression.String() == key {
			return i + 1, true
		}
	}
	return 0, false
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
	name, err := c.quote(field.Name())
	if err != nil {
		return "", err
	}
	if field.Source() == "" {
		if requireQualified {
			return "", typedUnsupported("field %q must name a source in a JOIN query", field.Name())
		}
		return name, nil
	}
	source, ok := sources[field.Source()]
	if !ok {
		return "", fmt.Errorf("field %q references unknown source %q", field.Name(), field.Source())
	}
	return source.quoted + "." + name, nil
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
	return marker, []any{arg}, nil
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
	return c.dialect.aggregateResult(name, name+"("+distinct+argument+")"), args, nil
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
		return c.dialect.divide(left, right), args, nil
	}
	return "(" + left + " " + string(expression.Operator) + " " + right + ")", args, nil
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
		return c.dialect.emptyIn(negated), nil, nil
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
