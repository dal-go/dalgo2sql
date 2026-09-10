package dalgo2sql

import (
	"database/sql/driver"
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
	if len(q.From().Joins()) != 0 || len(q.GroupBy()) != 0 || q.Having() != nil || q.StartFrom() != "" || q.StartAfter() != "" {
		return "", nil, fmt.Errorf("structured SQL query uses an unsupported join, grouping, having, or cursor")
	}
	source := q.From().Base()
	switch source := source.(type) {
	case dal.CollectionRef:
		if source.Parent() != nil {
			return "", nil, fmt.Errorf("parented collection sources are not supported")
		}
	case *dal.CollectionRef:
		if source == nil || source.Parent() != nil {
			return "", nil, fmt.Errorf("parented collection sources are not supported")
		}
	default:
		return "", nil, fmt.Errorf("unsupported structured SQL source %T", source)
	}
	sourceAlias := source.Alias()
	if sourceAlias != "" && !isPlainSQLIdentifier(sourceAlias) {
		return "", nil, fmt.Errorf("structured SQL query source alias %q is not a plain identifier", sourceAlias)
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	var args []any
	if columns := q.Columns(); len(columns) == 0 {
		b.WriteByte('*')
	} else {
		for i, column := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, values, err := compileSQLExpression(column.Expression, sourceAlias)
			if err != nil {
				return "", nil, fmt.Errorf("column %d: %w", i, err)
			}
			b.WriteString(expr)
			args = append(args, values...)
			if column.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(quoteSQLIdentifier(column.Alias))
			}
		}
	}
	b.WriteString(" FROM ")
	b.WriteString(quoteSQLIdentifier(source.Name()))
	if sourceAlias != "" {
		b.WriteString(" AS ")
		b.WriteString(quoteSQLIdentifier(sourceAlias))
	}
	if q.Where() != nil {
		condition, values, err := compileSQLCondition(q.Where(), sourceAlias)
		if err != nil {
			return "", nil, fmt.Errorf("where: %w", err)
		}
		b.WriteString(" WHERE ")
		b.WriteString(condition)
		args = append(args, values...)
	}
	if orders := q.OrderBy(); len(orders) > 0 {
		b.WriteString(" ORDER BY ")
		for i, order := range orders {
			if i > 0 {
				b.WriteString(", ")
			}
			expr, values, err := compileSQLExpression(order.Expression(), sourceAlias)
			if err != nil {
				return "", nil, fmt.Errorf("orderBy %d: %w", i, err)
			}
			if len(values) != 0 {
				return "", nil, fmt.Errorf("orderBy %d: values are not supported", i)
			}
			b.WriteString(expr)
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

// rePlainSQLIdentifier matches a bare identifier: a letter or underscore
// followed by letters, digits or underscores. A source alias must satisfy
// this before it is trusted to appear (quoted) in emitted SQL, and a
// qualified field reference (FieldRef.Source()) is only honoured when it
// equals the query's declared alias exactly.
var rePlainSQLIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func isPlainSQLIdentifier(name string) bool { return rePlainSQLIdentifier.MatchString(name) }

func compileSQLCondition(condition dal.Condition, alias string) (string, []any, error) {
	switch c := condition.(type) {
	case dal.Comparison:
		left, leftArgs, err := compileSQLExpression(c.Left, alias)
		if err != nil {
			return "", nil, err
		}
		if len(leftArgs) != 0 {
			return "", nil, fmt.Errorf("comparison left operand must be a field")
		}
		if c.Operator == dal.In {
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
			return left + " IN (" + strings.TrimSuffix(strings.Repeat("?, ", len(values)), ", ") + ")", values, nil
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
		right, rightArgs, err := compileSQLExpression(c.Right, alias)
		if err != nil {
			return "", nil, err
		}
		return left + " " + op + " " + right, rightArgs, nil
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
			part, values, err := compileSQLCondition(child, alias)
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

// compileSQLExpression renders expression as SQL text plus bound args.
// alias is the query's declared FROM-source alias (empty when the source
// has none); a FieldRef.Source() must be empty (unqualified, the common
// case) or exactly equal to alias to compile — anything else names a
// source this single-table dialect cannot resolve and is rejected.
func compileSQLExpression(expression dal.Expression, alias string) (string, []any, error) {
	switch e := expression.(type) {
	case dal.FieldRef:
		switch {
		case e.Source() == "":
			return quoteSQLIdentifier(e.Name()), nil, nil
		case alias != "" && e.Source() == alias:
			return quoteSQLIdentifier(alias) + "." + quoteSQLIdentifier(e.Name()), nil, nil
		default:
			return "", nil, fmt.Errorf("field %q references unknown source %q", e.Name(), e.Source())
		}
	case dal.Constant:
		if err := validateSQLValue(e.Value); err != nil {
			return "", nil, err
		}
		return "?", []any{e.Value}, nil
	default:
		return "", nil, fmt.Errorf("unsupported expression %T", expression)
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
	text := stripBracketIdents(q.String())
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
