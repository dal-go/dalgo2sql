package dalgo2sql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

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
				b.WriteString(" AS ")
				b.WriteString(quoteSQLIdentifier(column.Alias))
			} else if normalizedGroupOutput {
				name := column.Expression.String()
				if field, ok := column.Expression.(dal.FieldRef); ok {
					name = field.Name()
				}
				b.WriteString(" AS ")
				b.WriteString(quoteSQLIdentifier(name))
			}
		}
	}
	b.WriteString(" FROM ")
	b.WriteString(fromSQL)
	if q.Where() != nil {
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
	name := quoteSQLIdentifier(collection.Name())
	if schema := collection.Schema(); schema != "" {
		name = quoteSQLIdentifier(schema) + "." + name
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
		if c.Operator == dal.In {
			if len(leftArgs) != 0 {
				return "", nil, fmt.Errorf("IN left operand must not contain parameters")
			}
			array, ok := c.Right.(dal.Array)
			if !ok {
				return "", nil, fmt.Errorf("IN requires an array right operand")
			}
			values, err := arrayValues(array.Value)
			if err != nil {
				return "", nil, err
			}
			if len(values) == 0 {
				return "0 = 1", nil, nil
			}
			parts := make([]string, len(values))
			for i, value := range values {
				if _, ok := value.(bool); ok {
					return "", nil, fmt.Errorf("portable SQLite boolean predicates require schema typing")
				}
				op := "="
				if value == nil {
					op = "IS"
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
				parts[i] = operand + " " + op + " " + right
			}
			return "(" + strings.Join(parts, " OR ") + ")", values, nil
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
	default:
		return "", nil, fmt.Errorf("unsupported condition %T", condition)
	}
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
			return quoteSQLIdentifier(e.Name()), nil, nil
		case hasSQLSource(sources, e.Source()):
			return quoteSQLIdentifier(e.Source()) + "." + quoteSQLIdentifier(e.Name()), nil, nil
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
func emitSQL(q dal.StructuredQuery) string {
	text := q.String()
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
	return text
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
