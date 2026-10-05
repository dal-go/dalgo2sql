package dalgo2sql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

// compileStructuredSQL renders the deliberately small structured-query subset
// supported by SQL readers. Identifiers are quoted and values are parameters;
// unsupported nodes fail instead of falling back to Query.String().
func compileStructuredSQL(q dal.StructuredQuery) (string, []any, error) {
	if q == nil || q.From() == nil || q.From().Base() == nil {
		return "", nil, fmt.Errorf("structured SQL query requires a source")
	}
	if q.StartFrom() != "" || q.StartAfter() != "" {
		return "", nil, fmt.Errorf("structured SQL query uses an unsupported cursor")
	}
	if err := dal.ValidateAggregation(q); err != nil {
		return "", nil, fmt.Errorf("invalid aggregation: %w", err)
	}
	if len(q.From().Joins()) != 0 {
		if err := dal.ValidateJoinTree(q.From()); err != nil {
			return "", nil, err
		}
	}
	fromSQL, sourceAliases, hasJoins, err := compileSQLRelation(q.From(), "from")
	if err != nil {
		return "", nil, err
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	var args []any
	wildcard, err := planWildcardProjection(q)
	if err != nil {
		return "", nil, err
	}
	columns := q.Columns()
	if dal.HasAggregation(q) {
		columns = dal.EffectiveAggregationColumns(q)
	}
	aliases := make(map[string]dal.Expression, len(columns))
	groupExpressions := make(map[string]bool, len(q.GroupBy()))
	for _, group := range q.GroupBy() {
		groupExpressions[group.String()] = true
	}
	for _, column := range columns {
		if column.Alias != "" {
			aliases[column.Alias] = column.Expression
		}
	}
	if len(columns) == 0 {
		b.WriteByte('*')
	} else {
		for i, column := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			if column.Wildcard != nil {
				b.WriteString(wildcard.sqlExpression(quoteSQLIdentifier))
				continue
			}
			expr, values, err := compileSQLExpressionWithSources(column.Expression, sourceAliases, hasJoins)
			if err != nil {
				return "", nil, fmt.Errorf("column %d: %w", i, err)
			}
			normalizedGroupOutput := dal.HasAggregation(q) && groupExpressions[column.Expression.String()]
			if normalizedGroupOutput {
				expr = normalizeSQLNumber(expr)
				values = repeatSQLArgs(values, 3)
			}
			b.WriteString(expr)
			args = append(args, values...)
			if column.Alias != "" {
				alias, err := quoteCheckedSQLIdentifier(positionAlias, column.Alias)
				if err != nil {
					return "", nil, fmt.Errorf("column %d: %w", i, err)
				}
				b.WriteString(" AS ")
				b.WriteString(alias)
			} else if normalizedGroupOutput {
				name := column.Expression.String()
				if field, ok := column.Expression.(dal.FieldRef); ok {
					name = field.Name()
				}
				alias, err := quoteCheckedSQLIdentifier(positionAlias, name)
				if err != nil {
					return "", nil, fmt.Errorf("column %d: %w", i, err)
				}
				b.WriteString(" AS ")
				b.WriteString(alias)
			}
		}
	}
	b.WriteString(" FROM ")
	b.WriteString(fromSQL)
	if q.Where() != nil {
		if sqlConditionTestsAggregateForNull(q.Where()) {
			return "", nil, fmt.Errorf("where: unsupported null test over an aggregate")
		}
		condition, values, err := compileSQLConditionWithSources(q.Where(), sourceAliases, hasJoins)
		if err != nil {
			return "", nil, fmt.Errorf("where: %w", err)
		}
		b.WriteString(" WHERE ")
		b.WriteString(condition)
		args = append(args, values...)
	}
	if groups := q.GroupBy(); len(groups) > 0 {
		b.WriteString(" GROUP BY ")
		for i, group := range groups {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, values, err := compileSQLExpressionWithSources(group, sourceAliases, hasJoins)
			if err != nil {
				return "", nil, fmt.Errorf("groupBy %d: %w", i, err)
			}
			if dal.HasAggregation(q) {
				expr = normalizeSQLNumber(expr)
				values = repeatSQLArgs(values, 3)
			}
			b.WriteString("(" + expr + " COLLATE BINARY)")
			args = append(args, values...)
		}
	}
	if q.Having() != nil {
		condition, values, err := compileSQLConditionWithSources(rewriteSQLConditionAliases(q.Having(), aliases), sourceAliases, hasJoins)
		if err != nil {
			return "", nil, fmt.Errorf("having: %w", err)
		}
		b.WriteString(" HAVING ")
		b.WriteString(condition)
		args = append(args, values...)
	}
	if orders := q.OrderBy(); len(orders) > 0 {
		b.WriteString(" ORDER BY ")
		for i, order := range orders {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, values, err := compileSQLExpressionWithSources(rewriteSQLExpressionAlias(order.Expression(), aliases), sourceAliases, hasJoins)
			if err != nil {
				return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
			}
			if len(values) != 0 && !dal.HasAggregation(q) {
				return "", nil, fmt.Errorf("orderBy %d: values are not supported", i)
			}
			if dal.HasAggregation(q) {
				expr = normalizeSQLNumber(expr)
				values = repeatSQLArgs(values, 3)
			}
			b.WriteString("(" + expr + " COLLATE BINARY)")
			args = append(args, values...)
			if order.Descending() {
				b.WriteString(" DESC")
			}
		}
	}
	if q.Limit() < 0 || q.Offset() < 0 {
		return "", nil, fmt.Errorf("limit and offset must be non-negative")
	}
	if q.Limit() > 0 {
		b.WriteString(" LIMIT ?")
		args = append(args, q.Limit())
	} else if q.Offset() > 0 {
		b.WriteString(" LIMIT -1")
	}
	if q.Offset() > 0 {
		b.WriteString(" OFFSET ?")
		args = append(args, q.Offset())
	}
	return b.String(), args, nil
}

func quoteSQLIdentifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// quoteCheckedSQLIdentifier is quoteSQLIdentifier for a name the caller of a read
// chose: it applies quotableNameProblem, the one rule for the names a SQLite
// statement writes (it is the rule of the key reads and writes, and of the
// protected path), and returns an error wrapping ErrUnsafeName for a name that is
// empty, too long, invalid UTF-8 or holds a control character.
func quoteCheckedSQLIdentifier(position, name string) (string, error) {
	if problem := quotableNameProblem(name); problem != "" {
		return "", newUnsafeNameError(position, name, problem)
	}
	return quoteSQLIdentifier(name), nil
}

// compileSQLRelation emits an ordinary same-database relation tree. A nested
// right subtree is parenthesized so SQLite preserves LEFT/INNER grouping. The
// recursive call intentionally starts with an empty visible scope: SQLite's
// parenthesized JOIN grammar cannot safely represent a nested ON correlated to
// an alias outside that subtree.
func compileSQLRelation(from dal.FromSource, path string) (string, map[string]struct{}, bool, error) {
	return compileSQLRelationWithin(from, path, nil)
}

func compileSQLRelationWithin(from dal.FromSource, path string, outerSources map[string]struct{}) (string, map[string]struct{}, bool, error) {
	if from == nil || from.Base() == nil {
		return "", nil, false, fmt.Errorf("%s: join_shape: relation requires a source", path)
	}
	base, err := compileSQLTableSource(from.Base())
	if err != nil {
		return "", nil, false, fmt.Errorf("%s: %w", path, err)
	}
	aliases := map[string]struct{}{sourceSQLIdentity(from.Base()): {}}
	text := base
	joins := from.Joins()
	for i, join := range joins {
		joinPath := fmt.Sprintf("%s.joins[%d]", path, i)
		if join.JoinType() != dal.JoinInner && join.JoinType() != dal.JoinLeft {
			return "", nil, false, fmt.Errorf("%s.type: join_type: SQLite supports INNER and LEFT joins", joinPath)
		}
		right := join.From()
		if right == nil {
			if join.RecordsetSource == nil {
				return "", nil, false, fmt.Errorf("%s.from: join_shape: relation requires a source", joinPath)
			}
			right = dal.From(join.RecordsetSource)
		}
		correlationScope := copySQLSourceSet(outerSources)
		for alias := range aliases {
			correlationScope[alias] = struct{}{}
		}
		rightSQL, rightAliases, rightHasJoins, err := compileSQLRelationWithin(right, joinPath+".from", correlationScope)
		if err != nil {
			return "", nil, false, err
		}
		visible := copySQLSourceSet(aliases)
		for alias := range rightAliases {
			if _, exists := visible[alias]; exists {
				return "", nil, false, fmt.Errorf("%s.from: join_scope: duplicate source alias %q", joinPath, alias)
			}
			visible[alias] = struct{}{}
		}
		if err := rejectSQLCorrelatedJoinOn(join.On(), outerSources, joinPath+".on"); err != nil {
			return "", nil, false, err
		}
		on, err := compileSQLJoinOn(join.On(), visible, joinPath+".on")
		if err != nil {
			return "", nil, false, err
		}
		text += " " + string(join.JoinType()) + " JOIN "
		if rightHasJoins {
			text += "(" + rightSQL + ")"
		} else {
			text += rightSQL
		}
		text += " ON " + on
		aliases = visible
	}
	return text, aliases, len(joins) != 0, nil
}

func rejectSQLCorrelatedJoinOn(conditions []dal.Condition, outerSources map[string]struct{}, path string) error {
	for i, condition := range conditions {
		comparison, ok := condition.(dal.Comparison)
		if !ok {
			continue
		}
		for _, operand := range []dal.Expression{comparison.Left, comparison.Right} {
			field, ok := operand.(dal.FieldRef)
			if ok && hasSQLSource(outerSources, field.Source()) {
				return fmt.Errorf("%s[%d]: join_plan: correlated nested ON reference to %q is not representable by parenthesized SQLite JOIN", path, i, field.Source())
			}
		}
	}
	return nil
}

func compileSQLTableSource(source dal.RecordsetSource) (string, error) {
	var collection dal.CollectionRef
	switch source := source.(type) {
	case dal.CollectionRef:
		collection = source
	case *dal.CollectionRef:
		if source == nil {
			return "", fmt.Errorf("unsupported structured SQL source %T", source)
		}
		collection = *source
	default:
		return "", fmt.Errorf("unsupported structured SQL source %T", source)
	}
	if collection.Parent() != nil {
		return "", fmt.Errorf("parented collection sources are not supported")
	}
	if alias := collection.Alias(); alias != "" && !isPlainSQLIdentifier(alias) {
		return "", fmt.Errorf("structured SQL query source alias %q is not a plain identifier", alias)
	}
	name, err := quoteCheckedSQLIdentifier(positionCollection, collection.Name())
	if err != nil {
		return "", err
	}
	if schema := collection.Schema(); schema != "" {
		quotedSchema, err := quoteCheckedSQLIdentifier(positionSchema, schema)
		if err != nil {
			return "", err
		}
		name = quotedSchema + "." + name
	}
	if alias := collection.Alias(); alias != "" {
		name += " AS " + quoteSQLIdentifier(alias)
	}
	return name, nil
}

func sourceSQLIdentity(source dal.RecordsetSource) string {
	if alias := source.Alias(); alias != "" {
		return alias
	}
	return source.Name()
}

func copySQLSourceSet(sources map[string]struct{}) map[string]struct{} {
	copy := make(map[string]struct{}, len(sources))
	for source := range sources {
		copy[source] = struct{}{}
	}
	return copy
}

func hasSQLSource(sources map[string]struct{}, source string) bool {
	_, ok := sources[source]
	return ok
}

func compileSQLJoinOn(conditions []dal.Condition, sources map[string]struct{}, path string) (string, error) {
	if len(conditions) == 0 {
		return "", fmt.Errorf("%s: join_shape: ON must not be empty", path)
	}
	parts := make([]string, len(conditions))
	for i, condition := range conditions {
		comparison, ok := condition.(dal.Comparison)
		if !ok || comparison.Operator != dal.Equal {
			return "", fmt.Errorf("%s[%d]: join_operator: SQLite JOIN requires equality comparisons", path, i)
		}
		left, leftArgs, err := compileSQLExpressionWithSources(comparison.Left, sources, true)
		if err != nil {
			return "", fmt.Errorf("%s[%d].left: %w", path, i, err)
		}
		right, rightArgs, err := compileSQLExpressionWithSources(comparison.Right, sources, true)
		if err != nil {
			return "", fmt.Errorf("%s[%d].right: %w", path, i, err)
		}
		if len(leftArgs) != 0 || len(rightArgs) != 0 {
			return "", fmt.Errorf("%s[%d]: join_shape: ON operands must be qualified fields", path, i)
		}
		parts[i] = compileSQLTypedJoinEquality(left, right)
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

// SQLite considers INTEGER and REAL comparable numbers, but it also applies
// column affinity to plain equality. Remove affinity and guard the runtime
// types so numeric values compare across integer/real representations while
// text and booleans cannot accidentally match numeric keys. NULL never matches
// because the equality expression evaluates to NULL.
func compileSQLTypedJoinEquality(left, right string) string {
	left = "(+" + left + " COLLATE BINARY)"
	right = "(+" + right + " COLLATE BINARY)"
	numeric := "(typeof(%s) IN ('integer','real') AND typeof(%s) IN ('integer','real'))"
	types := "(typeof(" + left + ") = typeof(" + right + ") OR " + fmt.Sprintf(numeric, left, right) + ")"
	return "(" + types + " AND " + normalizeSQLNumber(left) + " = " + normalizeSQLNumber(right) + ")"
}

// rePlainSQLIdentifier matches a bare identifier: a letter or underscore
// followed by letters, digits or underscores. A source alias must satisfy
// this before it is trusted to appear (quoted) in emitted SQL, and a
// qualified field reference (FieldRef.Source()) is only honoured when it
// equals the query's declared alias exactly.
var rePlainSQLIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func isPlainSQLIdentifier(name string) bool { return rePlainSQLIdentifier.MatchString(name) }

func compileSQLCondition(condition dal.Condition, alias string) (string, []any, error) {
	sources := map[string]struct{}{}
	if alias != "" {
		sources[alias] = struct{}{}
	}
	return compileSQLConditionWithSources(condition, sources, false)
}

func compileSQLConditionWithSources(condition dal.Condition, sources map[string]struct{}, requireQualified bool) (string, []any, error) {
	switch c := condition.(type) {
	case dal.Comparison:
		left, leftArgs, err := compileSQLExpressionWithSources(c.Left, sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		if len(leftArgs) != 0 && !sqlExpressionContainsAggregate(c.Left) {
			return "", nil, fmt.Errorf("comparison left operand must be a field or aggregate")
		}
		// Unary plus removes declared affinity without coercing the stored
		// value; explicit BINARY prevents schema collations widening ACLs.
		left = "(+" + left + " COLLATE BINARY)"
		if c.Operator == dal.In || c.Operator == dal.NotIn {
			operator := "IN"
			if c.Operator == dal.NotIn {
				operator = "NOT IN"
			}
			if len(leftArgs) != 0 {
				return "", nil, fmt.Errorf("%s left operand must not contain parameters", operator)
			}
			array, ok := c.Right.(dal.Array)
			if !ok {
				return "", nil, fmt.Errorf("%s requires an array right operand", operator)
			}
			values, err := arrayValues(array.Value)
			if err != nil {
				return "", nil, err
			}
			if len(values) == 0 {
				if c.Operator == dal.NotIn {
					return "1 = 1", nil, nil
				}
				return "0 = 1", nil, nil
			}
			parts := make([]string, len(values))
			for i, value := range values {
				if _, ok := value.(bool); ok {
					return "", nil, fmt.Errorf("portable SQLite boolean predicates require schema typing")
				}
				operand := left
				right := "?"
				if isSQLNumber(value) {
					values[i], err = normalizedNumber(value)
					if err != nil {
						return "", nil, err
					}
					operand = normalizeSQLNumber(left)
					right = "(+CAST(? AS REAL))"
				}
				// Equality against NULL must remain UNKNOWN. IS NULL would
				// incorrectly make NULL IN [NULL] true and its negation false.
				parts[i] = operand + " = " + right
			}
			membership := "(" + strings.Join(parts, " OR ") + ")"
			if c.Operator == dal.NotIn {
				return "NOT " + membership, values, nil
			}
			return membership, values, nil
		}
		op := string(c.Operator)
		if c.Operator == dal.Equal {
			op = "="
		}
		switch c.Operator {
		case dal.Equal, dal.GreaterThen, dal.GreaterOrEqual, dal.LessThen, dal.LessOrEqual:
		default:
			return "", nil, fmt.Errorf("unsupported comparison operator %q", c.Operator)
		}
		right, rightArgs, err := compileSQLExpressionWithSources(c.Right, sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		_, rightIsConstant := c.Right.(dal.Constant)
		if !rightIsConstant {
			right = "(+" + right + " COLLATE BINARY)"
			left, right = normalizeSQLNumber(left), normalizeSQLNumber(right)
			leftArgs = repeatSQLArgs(leftArgs, 3)
			rightArgs = repeatSQLArgs(rightArgs, 3)
		}
		if rightIsConstant && len(rightArgs) == 1 {
			if _, ok := rightArgs[0].(bool); ok {
				return "", nil, fmt.Errorf("portable SQLite boolean predicates require schema typing")
			}
			if rightArgs[0] == nil {
				if c.Operator == dal.Equal {
					return left + " IS NULL", leftArgs, nil
				}
				return "0 = 1", nil, nil
			}
			if isSQLNumber(rightArgs[0]) {
				rightArgs[0], err = normalizedNumber(rightArgs[0])
				if err != nil {
					return "", nil, err
				}
				left = normalizeSQLNumber(left)
				leftArgs = repeatSQLArgs(leftArgs, 3)
				right = "(+CAST(? AS REAL))"
			}
		}
		comparison := left + " " + op + " " + right
		if c.Operator == dal.Equal {
			return comparison, append(append([]any(nil), leftArgs...), rightArgs...), nil
		}
		{
			guard := ""
			if rightIsConstant {
				if _, ok := rightArgs[0].(string); ok {
					guard = "typeof(" + left + ") = 'text'"
				} else {
					guard = "typeof(" + left + ") IN ('integer','real')"
				}
			} else {
				guard = "(typeof(" + left + ") = typeof(" + right + ") OR (typeof(" + left + ") IN ('integer','real') AND typeof(" + right + ") IN ('integer','real')))"
			}
			comparison = "(" + guard + " AND " + comparison + ")"
		}
		if !rightIsConstant {
			// The type guard contains each operand twice and the comparison
			// contains each once more. Preserve placeholder order as L,R,L,R,L,R.
			values := make([]any, 0, 3*(len(leftArgs)+len(rightArgs)))
			for range 3 {
				values = append(values, leftArgs...)
				values = append(values, rightArgs...)
			}
			return comparison, values, nil
		}
		// A constant comparison guard references only the left expression;
		// the comparison then references left followed by right.
		values := append([]any(nil), leftArgs...)
		values = append(values, leftArgs...)
		values = append(values, rightArgs...)
		return comparison, values, nil
	case dal.GroupCondition:
		if c.Operator() != dal.And && c.Operator() != dal.Or {
			return "", nil, fmt.Errorf("unsupported group operator %q", c.Operator())
		}
		children := c.Conditions()
		if len(children) == 0 {
			return "", nil, fmt.Errorf("empty condition group")
		}
		parts := make([]string, len(children))
		var args []any
		for i, child := range children {
			part, values, err := compileSQLConditionWithSources(child, sources, requireQualified)
			if err != nil {
				return "", nil, err
			}
			parts[i] = part
			args = append(args, values...)
		}
		return "(" + strings.Join(parts, " "+string(c.Operator())+" ") + ")", args, nil
	case dal.IsNullCondition:
		if _, star := c.Operand().(dal.StarExpression); star {
			return "", nil, fmt.Errorf("unsupported null test over *")
		}
		operand, args, err := compileSQLExpressionWithSources(c.Operand(), sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		// Unary plus drops declared affinity, as for comparisons; it never
		// turns NULL into a value or a value into NULL. IS [NOT] NULL is
		// TRUE or FALSE for every input, so it composes with AND and OR.
		test := "(+" + operand + " COLLATE BINARY) IS NULL"
		if c.Negated() {
			test = "(+" + operand + " COLLATE BINARY) IS NOT NULL"
		}
		return test, args, nil
	default:
		return "", nil, fmt.Errorf("unsupported condition %T", condition)
	}
}

// sqlConditionTestsAggregateForNull reports whether condition holds a null test
// whose operand holds an aggregate. It is a HAVING condition: in a WHERE it names
// an aggregate before any group exists.
func sqlConditionTestsAggregateForNull(condition dal.Condition) bool {
	switch c := condition.(type) {
	case dal.IsNullCondition:
		return sqlExpressionContainsAggregate(c.Operand())
	case dal.GroupCondition:
		for _, child := range c.Conditions() {
			if sqlConditionTestsAggregateForNull(child) {
				return true
			}
		}
	}
	return false
}

func sqlExpressionContainsAggregate(expression dal.Expression) bool {
	switch expression := expression.(type) {
	case dal.AggregateFunc:
		return true
	case dal.BinaryExpression:
		return sqlExpressionContainsAggregate(expression.Left) || sqlExpressionContainsAggregate(expression.Right)
	default:
		return false
	}
}

// DALgo compares JSON-normalized numbers as float64, including INTEGER values
// outside the exact binary64 range. Normalize stored numeric values as well as
// parameters before filtering; casting only the parameter still permits SQLite
// to compare its original integer exactly. Preserve text/null types so numeric
// normalization cannot turn a text value into an authorized number.
func normalizeSQLNumber(expression string) string {
	return "(CASE WHEN typeof(" + expression + ") IN ('integer','real') THEN CAST(" + expression + " AS REAL) ELSE " + expression + " END)"
}

func repeatSQLArgs(values []any, count int) []any {
	if len(values) == 0 || count <= 0 {
		return nil
	}
	result := make([]any, 0, len(values)*count)
	for range count {
		result = append(result, values...)
	}
	return result
}

func isSQLNumber(value any) bool {
	if value == nil {
		return false
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func normalizedNumber(value any) (float64, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("unsupported portable number: %w", err)
	}
	var number float64
	if err := json.Unmarshal(encoded, &number); err != nil {
		return 0, fmt.Errorf("unsupported portable number: %w", err)
	}
	return number, nil
}

// compileSQLExpression renders expression as SQL text plus bound args.
// alias is the query's declared FROM-source alias (empty when the source
// has none); a FieldRef.Source() must be empty (unqualified, the common
// case) or exactly equal to alias to compile — anything else names a
// source this single-table dialect cannot resolve and is rejected.
func compileSQLExpression(expression dal.Expression, alias string) (string, []any, error) {
	sources := map[string]struct{}{}
	if alias != "" {
		sources[alias] = struct{}{}
	}
	return compileSQLExpressionWithSources(expression, sources, false)
}

func compileSQLExpressionWithSources(expression dal.Expression, sources map[string]struct{}, requireQualified bool) (string, []any, error) {
	switch e := expression.(type) {
	case dal.FieldRef:
		switch {
		case e.Source() == "":
			if requireQualified {
				return "", nil, fmt.Errorf("field %q must name a source in a JOIN query", e.Name())
			}
			field, err := quoteCheckedSQLIdentifier(positionField, e.Name())
			return field, nil, err
		case hasSQLSource(sources, e.Source()):
			// The source is the name or alias of a source that was compiled, so its
			// own name has been checked.
			field, err := quoteCheckedSQLIdentifier(positionField, e.Name())
			if err != nil {
				return "", nil, err
			}
			return quoteSQLIdentifier(e.Source()) + "." + field, nil, nil
		default:
			return "", nil, fmt.Errorf("field %q references unknown source %q", e.Name(), e.Source())
		}
	case dal.Constant:
		if err := validateSQLValue(e.Value); err != nil {
			return "", nil, err
		}
		return "?", []any{e.Value}, nil
	case dal.StarExpression:
		return "*", nil, nil
	case dal.AggregateFunc:
		name := strings.ToUpper(e.FuncName())
		if name == dal.FIRST || name == dal.LAST {
			return "", nil, fmt.Errorf("%s requires deterministic aggregate ordering and is not native in SQLite", name)
		}
		args := e.FuncArgs()
		if len(args) != 1 {
			return "", nil, fmt.Errorf("%s requires exactly one argument", name)
		}
		arg, values, err := compileSQLExpressionWithSources(args[0], sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		distinctAggregate := false
		if d, ok := e.(dal.DistinctAggregateFunc); ok {
			distinctAggregate = d.IsDistinct()
		}
		distinct := ""
		if distinctAggregate {
			distinct = "DISTINCT "
		}
		switch name {
		case dal.COUNT, dal.SUM, dal.AVERAGE, dal.MIN, dal.MAX:
		default:
			return "", nil, fmt.Errorf("unsupported aggregate %q", name)
		}
		portableArg := arg
		if name == dal.COUNT && distinctAggregate {
			if _, star := args[0].(dal.StarExpression); !star {
				portableArg = "(" + normalizeSQLNumber(arg) + " COLLATE BINARY)"
				values = repeatSQLArgs(values, 3)
			}
		}
		if name == dal.MIN || name == dal.MAX {
			portableArg = "(" + normalizeSQLNumber(arg) + " COLLATE BINARY)"
			values = repeatSQLArgs(values, 3)
		}
		expression := name + "(" + distinct + portableArg + ")"
		if name == dal.SUM || name == dal.AVERAGE {
			numericArg := "CASE WHEN typeof(" + arg + ") IN ('integer','real') THEN CAST(" + arg + " AS REAL) ELSE NULL END"
			expression = name + "(" + distinct + numericArg + ")"
			values = repeatSQLArgs(values, 2)
		}
		if name == dal.SUM {
			expression = "CAST(" + expression + " AS REAL)"
		}
		return expression, values, nil
	case dal.BinaryExpression:
		left, leftArgs, err := compileSQLExpressionWithSources(e.Left, sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		right, rightArgs, err := compileSQLExpressionWithSources(e.Right, sources, requireQualified)
		if err != nil {
			return "", nil, err
		}
		switch e.Operator {
		case dal.Add, dal.Subtract, dal.Multiply, dal.Divide:
		default:
			return "", nil, fmt.Errorf("unsupported arithmetic operator %q", e.Operator)
		}
		left = normalizeSQLNumber(left)
		right = normalizeSQLNumber(right)
		leftArgs = repeatSQLArgs(leftArgs, 3)
		rightArgs = repeatSQLArgs(rightArgs, 3)
		guard := "typeof(" + left + ") IN ('integer','real') AND typeof(" + right + ") IN ('integer','real')"
		values := append(append([]any(nil), leftArgs...), rightArgs...)
		if e.Operator == dal.Divide {
			guard += " AND " + right + " != 0"
			values = append(values, rightArgs...)
		}
		expression := "(CASE WHEN " + guard + " THEN " + left + " " + string(e.Operator) + " " + right + " ELSE NULL END)"
		values = append(values, leftArgs...)
		values = append(values, rightArgs...)
		return expression, values, nil
	default:
		return "", nil, fmt.Errorf("unsupported expression %T", expression)
	}
}

func rewriteSQLExpressionAlias(expression dal.Expression, aliases map[string]dal.Expression) dal.Expression {
	if field, ok := expression.(dal.FieldRef); ok && field.Source() == "" {
		if replacement, exists := aliases[field.Name()]; exists {
			return replacement
		}
	}
	if binary, ok := expression.(dal.BinaryExpression); ok {
		binary.Left = rewriteSQLExpressionAlias(binary.Left, aliases)
		binary.Right = rewriteSQLExpressionAlias(binary.Right, aliases)
		return binary
	}
	return expression
}

func rewriteSQLConditionAliases(condition dal.Condition, aliases map[string]dal.Expression) dal.Condition {
	switch c := condition.(type) {
	case dal.Comparison:
		c.Left = rewriteSQLExpressionAlias(c.Left, aliases)
		c.Right = rewriteSQLExpressionAlias(c.Right, aliases)
		return c
	case dal.GroupCondition:
		children := make([]dal.Condition, len(c.Conditions()))
		for i, child := range c.Conditions() {
			children[i] = rewriteSQLConditionAliases(child, aliases)
		}
		return dal.NewGroupCondition(c.Operator(), children...)
	case dal.IsNullCondition:
		operand := c.Operand()
		if operand != nil {
			operand = rewriteSQLExpressionAlias(operand, aliases)
		}
		if c.Negated() {
			return dal.NewIsNotNullCondition(operand)
		}
		return dal.NewIsNullCondition(operand)
	default:
		return condition
	}
}

func arrayValues(value any) ([]any, error) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil, fmt.Errorf("IN array has type %T", value)
	}
	values := make([]any, rv.Len())
	for i := range values {
		values[i] = rv.Index(i).Interface()
		if err := validateSQLValue(values[i]); err != nil {
			return nil, fmt.Errorf("IN value %d: %w", i, err)
		}
	}
	return values, nil
}

func validateSQLValue(value any) error {
	if value == nil {
		return nil
	}
	if _, ok := value.(driver.Valuer); ok {
		return nil
	}
	if _, ok := value.(time.Time); ok {
		return nil
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Slice:
		if reflect.TypeOf(value).Elem().Kind() == reflect.Uint8 {
			return nil
		}
	}
	return fmt.Errorf("unsupported SQL value type %T", value)
}

// emitSQL preserves the historical string-rewrite helper for compatibility.
// Structured query execution uses compileStructuredSQL instead.
//
// The text it builds comes from dal.QueryString(q), which pastes names and
// values straight into the statement and then strips every bracket pair. It is
// therefore only safe for plain names and values, so emitSQL first runs
// guardLegacyEmit and refuses anything it cannot prove plain, with an error
// wrapping dal.ErrNotSupported. The caller must not execute SQL on error.
func emitSQL(q dal.StructuredQuery) (string, error) {
	if err := guardLegacyEmit(q); err != nil {
		return "", err
	}
	// dal.QueryString reads every part through the accessors guardLegacyEmit
	// just checked; q.String() may be a wrapper's own text.
	text := dal.QueryString(q)
	if wildcard, err := planWildcardProjection(q); err == nil && wildcard != nil {
		for _, column := range q.Columns() {
			if column.Wildcard != nil {
				// QueryString's legacy FROM rendering does not emit source aliases.
				// This path supports only a single source, so an unqualified wildcard
				// is equivalent and avoids producing an alias that is absent from FROM.
				text = strings.Replace(text, column.String(), "*", 1)
				break
			}
		}
	}
	text = stripBracketIdents(text)
	if limit := q.Limit(); limit > 0 {
		top := fmt.Sprintf("SELECT TOP %d", limit)
		if strings.HasPrefix(text, top) {
			text = "SELECT" + text[len(top):] + fmt.Sprintf("\nLIMIT %d", limit)
		}
	}
	return text, nil
}

func stripBracketIdents(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		if sql[i] == '[' {
			if end := strings.IndexByte(sql[i+1:], ']'); end >= 0 {
				b.WriteString(sql[i+1 : i+1+end])
				i += end + 2
				continue
			}
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

// maxLegacyGuardDepth bounds the walk over nested join sources so a cyclic
// FromSource cannot overflow the stack.
const maxLegacyGuardDepth = 64

// reLegacyWildcardMask matches a wildcard exclusion: a plain identifier in
// which '*' may stand for any run of characters.
var reLegacyWildcardMask = regexp.MustCompile(`^[A-Za-z_*][A-Za-z0-9_*]*$`)

// legacyRefusal builds the error guardLegacyEmit returns. It names the
// position and the reason only: the error reaches logs and clients, so a
// caller's name, operator or value is never part of the text.
func legacyRefusal(path, format string, args ...any) error {
	return fmt.Errorf("%w: legacy SQL emission refused at %s: %s", dal.ErrNotSupported, path, fmt.Sprintf(format, args...))
}

// guardLegacyEmit walks every name, operator and constant that emitSQL's text
// would contain and fails closed: any collection, schema, alias, field or
// column-alias name that is not a plain identifier, any string constant with a
// backslash, bracket or control character, and any node whose rendering is not
// known to be safe is refused.
func guardLegacyEmit(q dal.StructuredQuery) error {
	if q == nil {
		return legacyRefusal("query", "no query")
	}
	if from := q.From(); from != nil {
		if err := guardLegacyFrom(from, "from", 0); err != nil {
			return err
		}
	}
	for i, column := range q.Columns() {
		if err := guardLegacyColumn(column, fmt.Sprintf("columns[%d]", i)); err != nil {
			return err
		}
	}
	if where := q.Where(); where != nil {
		if err := guardLegacyCondition(where, "where"); err != nil {
			return err
		}
	}
	for i, expression := range q.GroupBy() {
		if err := guardLegacyExpression(expression, fmt.Sprintf("groupBy[%d]", i)); err != nil {
			return err
		}
	}
	if having := q.Having(); having != nil {
		if err := guardLegacyCondition(having, "having"); err != nil {
			return err
		}
	}
	for i, order := range q.OrderBy() {
		path := fmt.Sprintf("orderBy[%d]", i)
		if err := guardLegacyOrder(order, path); err != nil {
			return err
		}
		// A keys-only query is ordered by its primary key, which the reader adds and
		// the legacy text writes unquoted: a reserved word there is a statement the
		// server rejects.
		if field, ok := order.Expression().(dal.FieldRef); ok && isKeysOnlyQuery(q) && isReservedSQLWord(field.Name()) {
			return legacyRefusal(path, "a keys-only query cannot be ordered by a field that is a reserved word")
		}
	}
	return nil
}

func guardLegacyIdentifier(path, kind, name string) error {
	if !isPlainSQLIdentifier(name) {
		return legacyRefusal(path, "%s is not a plain identifier", kind)
	}
	return nil
}

// guardLegacyOptionalIdentifier accepts "" as "not set".
func guardLegacyOptionalIdentifier(path, kind, name string) error {
	if name == "" {
		return nil
	}
	return guardLegacyIdentifier(path, kind, name)
}

func guardLegacyFrom(from dal.FromSource, path string, depth int) error {
	if depth > maxLegacyGuardDepth {
		return legacyRefusal(path, "join tree is nested too deeply")
	}
	if err := guardLegacySource(from.Base(), path+".base"); err != nil {
		return err
	}
	for i, join := range from.Joins() {
		joinPath := fmt.Sprintf("%s.joins[%d]", path, i)
		if nested := join.From(); nested != nil {
			if err := guardLegacyFrom(nested, joinPath+".from", depth+1); err != nil {
				return err
			}
		} else if err := guardLegacySource(join.RecordsetSource, joinPath); err != nil {
			return err
		}
		for j, condition := range join.On() {
			if err := guardLegacyCondition(condition, fmt.Sprintf("%s.on[%d]", joinPath, j)); err != nil {
				return err
			}
		}
	}
	return nil
}

func guardLegacySource(source dal.RecordsetSource, path string) error {
	switch s := source.(type) {
	case dal.CollectionRef:
		return guardLegacyCollection(s, path)
	case *dal.CollectionRef:
		if s == nil {
			return legacyRefusal(path, "nil collection")
		}
		return guardLegacyCollection(*s, path)
	case dal.CollectionGroupRef:
		return guardLegacyCollectionGroup(s, path)
	case *dal.CollectionGroupRef:
		if s == nil {
			return legacyRefusal(path, "nil collection group")
		}
		return guardLegacyCollectionGroup(*s, path)
	default:
		return legacyRefusal(path, "unsupported source %T", source)
	}
}

func guardLegacyCollection(collection dal.CollectionRef, path string) error {
	if collection.Parent() != nil {
		return legacyRefusal(path, "parented collection is not supported")
	}
	if err := guardLegacyIdentifier(path, "collection name", collection.Name()); err != nil {
		return err
	}
	if err := guardLegacyOptionalIdentifier(path, "schema", collection.Schema()); err != nil {
		return err
	}
	return guardLegacyOptionalIdentifier(path, "alias", collection.Alias())
}

func guardLegacyCollectionGroup(group dal.CollectionGroupRef, path string) error {
	if err := guardLegacyIdentifier(path, "collection group name", group.Name()); err != nil {
		return err
	}
	return guardLegacyOptionalIdentifier(path, "alias", group.Alias())
}

func guardLegacyColumn(column dal.Column, path string) error {
	if err := guardLegacyOptionalIdentifier(path, "column alias", column.Alias); err != nil {
		return err
	}
	if wildcard := column.Wildcard; wildcard != nil {
		if err := guardLegacyOptionalIdentifier(path, "wildcard source", wildcard.Source); err != nil {
			return err
		}
		for _, name := range wildcard.Exclude {
			if !reLegacyWildcardMask.MatchString(name) {
				return legacyRefusal(path, "wildcard exclusion is not a plain identifier or mask")
			}
		}
	}
	if column.Expression == nil {
		return nil // rendered as the literal NULL
	}
	return guardLegacyExpression(column.Expression, path)
}

func guardLegacyOrder(order dal.OrderExpression, path string) error {
	if err := guardLegacyExpression(order.Expression(), path); err != nil {
		return err
	}
	want := order.Expression().String()
	if order.Descending() {
		want += " DESC"
	}
	if order.String() != want {
		return legacyRefusal(path, "order expression renders unexpected text")
	}
	return nil
}

var legacyComparisonOperators = map[dal.Operator]bool{
	dal.Equal: true, dal.In: true, dal.NotIn: true,
	dal.GreaterThen: true, dal.GreaterOrEqual: true, dal.LessThen: true, dal.LessOrEqual: true,
}

func guardLegacyCondition(condition dal.Condition, path string) error {
	switch c := condition.(type) {
	case dal.Comparison:
		if !legacyComparisonOperators[c.Operator] {
			return legacyRefusal(path, "comparison operator is not supported")
		}
		if err := guardLegacyExpression(c.Left, path+".left"); err != nil {
			return err
		}
		return guardLegacyExpression(c.Right, path+".right")
	case dal.GroupCondition:
		if c.Operator() != dal.And && c.Operator() != dal.Or {
			return legacyRefusal(path, "group operator is not supported")
		}
		for i, nested := range c.Conditions() {
			if err := guardLegacyCondition(nested, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case dal.IsNullCondition:
		return guardLegacyExpression(c.Operand(), path)
	default:
		return legacyRefusal(path, "unsupported condition %T", condition)
	}
}

func guardLegacyExpression(expression dal.Expression, path string) error {
	switch e := expression.(type) {
	case dal.FieldRef:
		if err := guardLegacyOptionalIdentifier(path, "field source", e.Source()); err != nil {
			return err
		}
		return guardLegacyIdentifier(path, "field name", e.Name())
	case dal.Constant:
		return guardLegacyConstantValue(path, e.Value)
	case *dal.Constant:
		if e == nil {
			return legacyRefusal(path, "nil constant")
		}
		return guardLegacyConstantValue(path, e.Value)
	case dal.Array:
		return guardLegacyArray(path, e.Value)
	case dal.BinaryExpression:
		switch e.Operator {
		case dal.Add, dal.Subtract, dal.Multiply, dal.Divide:
		default:
			return legacyRefusal(path, "arithmetic operator is not supported")
		}
		if err := guardLegacyExpression(e.Left, path+".left"); err != nil {
			return err
		}
		return guardLegacyExpression(e.Right, path+".right")
	case dal.Param:
		if !dal.ValidParamName(e.Name) {
			return legacyRefusal(path, "parameter name is not valid")
		}
		return nil
	case dal.AggregateFunc:
		return guardLegacyAggregate(e, path)
	case dal.StarExpression:
		if !e.IsStar() || expression.String() != "*" {
			return legacyRefusal(path, "unsupported star expression %T", expression)
		}
		return nil
	default:
		return legacyRefusal(path, "unsupported expression %T", expression)
	}
}

// guardLegacyAggregate validates the parts of an aggregate call and checks that
// the call renders exactly as those parts say it does.
func guardLegacyAggregate(aggregate dal.AggregateFunc, path string) error {
	if err := guardLegacyIdentifier(path, "function name", aggregate.FuncName()); err != nil {
		return err
	}
	// The name is written into the statement as NAME(args), so it is limited
	// to the aggregates compileSQLExpressionWithSources accepts. The core
	// aggregation check does not look at a WHERE clause, so this is the only
	// place that stops a caller choosing the function there.
	switch strings.ToUpper(aggregate.FuncName()) {
	case dal.COUNT, dal.SUM, dal.AVERAGE, dal.MIN, dal.MAX:
	default:
		return legacyRefusal(path, "aggregate function is not supported")
	}
	args := make([]string, 0, len(aggregate.FuncArgs()))
	for i, arg := range aggregate.FuncArgs() {
		if err := guardLegacyExpression(arg, fmt.Sprintf("%s.args[%d]", path, i)); err != nil {
			return err
		}
		args = append(args, arg.String())
	}
	prefix := ""
	if distinct, ok := aggregate.(dal.DistinctAggregateFunc); ok && distinct.IsDistinct() {
		prefix = "DISTINCT "
	}
	if aggregate.String() != aggregate.FuncName()+"("+prefix+strings.Join(args, ", ")+")" {
		return legacyRefusal(path, "aggregate function renders unexpected text")
	}
	return nil
}

// guardLegacyConstantValue checks a Constant. Constant.String() renders a
// scalar through strconv or encoding/json, a string through quote doubling, and
// a slice through encoding/json, so a slice must hold only checked scalars and,
// because json turns a double quote into backslash-quote, no double quote. Only
// an unnamed slice type is accepted: a named one may carry its own marshaller,
// and then the text is not what was checked. A slice of bytes is refused too:
// json renders it as one base64 string, not as the numbers the guard checked.
func guardLegacyConstantValue(path string, value any) error {
	if value != nil && reflect.TypeOf(value).Kind() == reflect.Slice {
		if !isUnnamedSlice(value) {
			return legacyRefusal(path, "constant of named slice type %T is not supported", value)
		}
		if reflect.TypeOf(value).Elem().Kind() == reflect.Uint8 {
			return legacyRefusal(path, "constant of byte slice type %T is not supported", value)
		}
		return guardLegacySequence(path, value, legacyJSONElement)
	}
	return guardLegacyScalar(path, value, legacyConstantScalar)
}

// isUnnamedSlice reports whether value's type is a slice type literal such as
// []string, not a defined type like json.RawMessage.
func isUnnamedSlice(value any) bool {
	t := reflect.TypeOf(value)
	return t.Kind() == reflect.Slice && t.Name() == ""
}

func guardLegacyArray(path string, value any) error {
	if value == nil {
		return nil
	}
	if reflect.TypeOf(value).Kind() != reflect.Slice {
		return legacyRefusal(path, "array value of type %T is not a slice", value)
	}
	if !isUnnamedSlice(value) {
		return legacyRefusal(path, "array of named slice type %T is not supported", value)
	}
	return guardLegacySequence(path, value, legacyListElement)
}

// legacyRendering says which String() renders a scalar, because each prints
// the same value differently.
type legacyRendering int

const (
	// legacyConstantScalar: Constant.String() on a scalar. A string is quote
	// doubled, anything else goes through encoding/json or strconv.
	legacyConstantScalar legacyRendering = iota
	// legacyJSONElement: an element of a slice in constant position, rendered
	// by encoding/json as a whole.
	legacyJSONElement
	// legacyListElement: an element of Array.String(), printed by fmt.
	legacyListElement
)

func guardLegacySequence(path string, value any, rendering legacyRendering) error {
	slice := reflect.ValueOf(value)
	for i := 0; i < slice.Len(); i++ {
		if err := guardLegacyScalar(fmt.Sprintf("%s[%d]", path, i), slice.Index(i).Interface(), rendering); err != nil {
			return err
		}
	}
	return nil
}

func guardLegacyScalar(path string, value any, rendering legacyRendering) error {
	switch v := value.(type) {
	case nil, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return nil
	case float32:
		return guardLegacyFloat(path, float64(v))
	case float64:
		return guardLegacyFloat(path, v)
	case time.Time:
		return guardLegacyTime(path, v, rendering)
	case string:
		return guardLegacyString(path, v, rendering == legacyJSONElement)
	default:
		return legacyRefusal(path, "constant of type %T is not supported", value)
	}
}

// guardLegacyFloat refuses NaN and the infinities: encoding/json fails on them
// (the text would be empty) and fmt prints them as bare words.
func guardLegacyFloat(path string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return legacyRefusal(path, "number is not finite")
	}
	return nil
}

// guardLegacyTime accepts a time only where encoding/json renders it, and only
// when json can render it: that text is digits and punctuation. Inside an
// Array the element is printed by fmt, which includes the zone name, a text the
// caller chooses.
func guardLegacyTime(path string, value time.Time, rendering legacyRendering) error {
	if rendering == legacyListElement {
		return legacyRefusal(path, "time in a list is not supported")
	}
	if _, err := value.MarshalJSON(); err != nil {
		return legacyRefusal(path, "time is outside the supported range")
	}
	return nil
}

// guardLegacyString refuses text the legacy emitter cannot carry safely: a
// backslash (MySQL-style escapes), a bracket (stripBracketIdents removes it
// from the whole statement), a control character, and, when the text is
// rendered by encoding/json, a double quote, every character json writes as a
// backslash-u escape instead of as itself (<, >, &, U+2028, U+2029) and invalid
// UTF-8, which json replaces with U+FFFD, so the text sent is not the text checked.
// The error does not repeat the text.
func guardLegacyString(path, value string, jsonRendered bool) error {
	if jsonRendered && !utf8.ValidString(value) {
		return legacyRefusal(path, "string constant contains a character that is not supported")
	}
	for _, r := range value {
		if r == '\\' || r == '[' || r == ']' || unicode.IsControl(r) {
			return legacyRefusal(path, "string constant contains a character that is not supported")
		}
		if jsonRendered && (r == '"' || r == '<' || r == '>' || r == '&' || r == '\u2028' || r == '\u2029') {
			return legacyRefusal(path, "string constant contains a character that is not supported")
		}
	}
	return nil
}
