package dalgo2sql

import (
	"errors"
	"math"
	"reflect"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// These are per-compilation construction budgets, not total heap limits. The
// same budget follows every clause and intermediate of one strict emitter.
const (
	sqliteCompileBytes = 1 << 20
	sqliteCompileSlots = 32766
	sqliteCompileNodes = 4096
)

var errSQLiteCompileBudget = errors.New("strict SQLite query exceeds its construction budget")

type sqliteCompileBudget struct {
	bytes, slots, values, nodes, inputs int
	err                                 error
}

func newSQLiteEmitter(strict bool) sqliteEmitter {
	emitter := sqliteEmitter{strictNumbers: strict}
	if strict {
		emitter.budget = &sqliteCompileBudget{}
	}
	return emitter
}

func (b *sqliteCompileBudget) take(used *int, count, ceiling int) bool {
	if b.err != nil {
		return false
	}
	if count < 0 || count > ceiling-*used {
		b.err = errSQLiteCompileBudget
		return false
	}
	*used += count
	return true
}

func (emitter sqliteEmitter) concat(parts ...string) string {
	return emitter.join(parts, "")
}

func (emitter sqliteEmitter) join(parts []string, separator string) string {
	if emitter.budget != nil {
		for i, part := range parts {
			if i != 0 && !emitter.budget.take(&emitter.budget.bytes, len(separator), sqliteCompileBytes) {
				return ""
			}
			if !emitter.budget.take(&emitter.budget.bytes, len(part), sqliteCompileBytes) {
				return ""
			}
		}
	}
	return strings.Join(parts, separator)
}

func sqliteArgumentBytes(value any) (int, bool) {
	switch value := value.(type) {
	case nil:
		return 0, true
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 8, true
	case float32:
		return 8, !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0)
	case float64:
		return 8, !math.IsNaN(value) && !math.IsInf(value, 0)
	case string:
		return len(value), true
	case []byte:
		return len(value), true
	default:
		// Never invoke a custom Valuer to guess the size of its future result.
		return 0, false
	}
}

func (emitter sqliteEmitter) argumentSize(values []any) (int, bool) {
	bytes := 0
	for _, value := range values {
		size, known := sqliteArgumentBytes(value)
		if !known || size > sqliteCompileBytes-bytes {
			return 0, false
		}
		bytes += size
	}
	return bytes, true
}

func (emitter sqliteEmitter) claimArguments(values []any, count int) bool {
	if emitter.budget == nil {
		return true
	}
	bytes, known := emitter.argumentSize(values)
	if !known || count < 0 || count != 0 && (len(values) > sqliteCompileSlots/count || bytes > sqliteCompileBytes/count) {
		emitter.budget.err = errSQLiteCompileBudget
		return false
	}
	return emitter.budget.take(&emitter.budget.slots, len(values)*count, sqliteCompileSlots) && emitter.budget.take(&emitter.budget.values, bytes*count, sqliteCompileBytes)
}

func (emitter sqliteEmitter) repeatArgs(values []any, count int) []any {
	if !emitter.claimArguments(values, count) {
		return nil
	}
	return repeatSQLArgs(values, count)
}

func (emitter sqliteEmitter) appendArgs(target []any, values ...any) []any {
	if !emitter.claimArguments(target, 1) || !emitter.claimArguments(values, 1) {
		return nil
	}
	return append(target, values...)
}

func (emitter sqliteEmitter) stringParts(count int) ([]string, error) {
	if emitter.budget != nil && !emitter.budget.take(&emitter.budget.slots, count, sqliteCompileSlots) {
		return nil, emitter.budget.err
	}
	return make([]string, count), nil
}

func (emitter sqliteEmitter) failure() error {
	if emitter.budget != nil {
		return emitter.budget.err
	}
	return nil
}

func (emitter sqliteEmitter) step() error {
	if emitter.budget != nil {
		emitter.budget.take(&emitter.budget.nodes, 1, sqliteCompileNodes)
	}
	return emitter.failure()
}

func (emitter sqliteEmitter) finish(text *string, args *[]any, err *error) {
	if failure := emitter.failure(); failure != nil {
		*text, *args, *err = "", nil, failure
	}
}

func (emitter sqliteEmitter) quoteIdentifier(name string) string {
	if emitter.budget != nil {
		if count := strings.Count(name, "`"); count != 0 && !emitter.budget.take(&emitter.budget.bytes, len(name)+count, sqliteCompileBytes) {
			return ""
		}
		if !emitter.budget.take(&emitter.budget.bytes, len(name), sqliteCompileBytes) ||
			!emitter.budget.take(&emitter.budget.bytes, 2+strings.Count(name, "`"), sqliteCompileBytes) {
			return ""
		}
	}
	return quoteSQLIdentifier(name)
}

func (emitter sqliteEmitter) quoteCheckedIdentifier(position, name string) (string, error) {
	if emitter.budget == nil {
		return quoteCheckedSQLIdentifier(position, name)
	}
	// Quoting and an error's quoted name both have a bounded original input.
	if count := strings.Count(name, "`"); count != 0 && !emitter.budget.take(&emitter.budget.bytes, len(name)+count, sqliteCompileBytes) {
		return "", emitter.budget.err
	}
	if !emitter.budget.take(&emitter.budget.bytes, len(name), sqliteCompileBytes) ||
		!emitter.budget.take(&emitter.budget.bytes, 2+strings.Count(name, "`"), sqliteCompileBytes) {
		return "", emitter.budget.err
	}
	return quoteCheckedSQLIdentifier(position, name)
}

func (emitter sqliteEmitter) arrayValues(value any) ([]any, error) {
	if emitter.budget != nil {
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			if !emitter.budget.take(&emitter.budget.slots, rv.Len(), sqliteCompileSlots) {
				return nil, emitter.budget.err
			}
			for i := 0; i < rv.Len(); i++ {
				size, known := sqliteArgumentBytes(rv.Index(i).Interface())
				if !known || !emitter.budget.take(&emitter.budget.values, size, sqliteCompileBytes) {
					emitter.budget.err = errSQLiteCompileBudget
					return nil, emitter.budget.err
				}
			}
		}
	}
	return arrayValues(value)
}

func (emitter sqliteEmitter) copySources(sources map[string]struct{}) (map[string]struct{}, error) {
	if emitter.budget != nil && !emitter.budget.take(&emitter.budget.slots, len(sources), sqliteCompileSlots) {
		return nil, emitter.budget.err
	}
	return copySQLSourceSet(sources), nil
}

func (emitter sqliteEmitter) expandWildcard(plan wildcardProjectionPlan, query dal.StructuredQuery, names []string) (dal.StructuredQuery, bool, error) {
	if b := emitter.budget; b != nil {
		// Check metadata-derived map/column staging and case-folding work before
		// the ordinary expansion allocates. Two ToLower calls can each expand an
		// invalid UTF-8 byte to a three-byte replacement rune; six times the input
		// bytes is therefore a conservative bound, including malformed names.
		if plan.explicitCount < 0 || len(names) > sqliteCompileSlots || plan.explicitCount > sqliteCompileSlots-len(names) ||
			!b.take(&b.nodes, len(names), sqliteCompileNodes) ||
			!b.take(&b.slots, len(names), sqliteCompileSlots) ||
			!b.take(&b.slots, len(names)+plan.explicitCount, sqliteCompileSlots) {
			b.err = errSQLiteCompileBudget
			return nil, false, b.err
		}
		for _, name := range names {
			if len(name) > sqliteCompileBytes/6 || !b.take(&b.bytes, len(name)*6, sqliteCompileBytes) {
				b.err = errSQLiteCompileBudget
				return nil, false, b.err
			}
		}
	}
	expanded, ok := plan.expandSQLiteWildcard(query, names)
	return expanded, ok, nil
}

type sqliteSQLBuilder struct {
	strings.Builder
	emitter sqliteEmitter
}

func (b *sqliteSQLBuilder) addString(value string) {
	if b.emitter.budget == nil || b.emitter.budget.take(&b.emitter.budget.bytes, len(value), sqliteCompileBytes) {
		b.WriteString(value)
	}
}
func (b *sqliteSQLBuilder) addByte(value byte) {
	if b.emitter.budget == nil || b.emitter.budget.take(&b.emitter.budget.bytes, 1, sqliteCompileBytes) {
		b.WriteByte(value)
	}
}

// inspect counts known structural work before the compiler's validation/maps.
// It does no resource authorization and never calls an argument's Valuer.
func (emitter sqliteEmitter) inspect(node any, depth int) bool {
	b := emitter.budget
	if b == nil {
		return true
	}
	if depth > 64 || !b.take(&b.nodes, 1, sqliteCompileNodes) {
		b.err = errSQLiteCompileBudget
		return false
	}
	if node != nil {
		value := reflect.ValueOf(node)
		if value.Kind() == reflect.Pointer && value.IsNil() {
			b.err = errSQLiteCompileBudget
			return false
		}
	}
	visit := func(child any) bool { return emitter.inspect(child, depth+1) }
	text := func(value string) bool { return b.take(&b.inputs, len(value), sqliteCompileBytes) }
	switch n := node.(type) {
	case dal.StructuredQuery:
		if !visit(n.From()) || !visit(n.Where()) || !visit(n.Having()) {
			return false
		}
		for _, column := range n.Columns() {
			if !text(column.Alias) || !visit(column.Expression) {
				return false
			}
			if column.Wildcard != nil {
				if !b.take(&b.nodes, len(column.Wildcard.Exclude), sqliteCompileNodes) || !text(column.Wildcard.Source) {
					return false
				}
				for _, name := range column.Wildcard.Exclude {
					if !text(name) {
						return false
					}
				}
			}
		}
		for _, group := range n.GroupBy() {
			if !visit(group) {
				return false
			}
		}
		for _, order := range n.OrderBy() {
			if order == nil || !visit(order.Expression()) {
				b.err = errSQLiteCompileBudget
				return false
			}
		}
	case dal.FromSource:
		if !visit(n.Base()) {
			return false
		}
		for _, join := range n.Joins() {
			if !text(string(join.JoinType())) {
				return false
			}
			child := any(join.From())
			if join.From() == nil {
				child = join.RecordsetSource
			}
			if !visit(child) {
				return false
			}
			for _, condition := range join.On() {
				if !visit(condition) {
					return false
				}
			}
		}
	case dal.CollectionRef:
		return text(n.Name()) && text(n.Schema()) && text(n.Database()) && text(n.Alias())
	case *dal.CollectionRef:
		if n != nil {
			return visit(*n)
		}
	case dal.FieldRef:
		return text(n.Name()) && text(n.Source())
	case dal.Constant:
		size, known := sqliteArgumentBytes(n.Value)
		if !known || !b.take(&b.inputs, size, sqliteCompileBytes) {
			b.err = errSQLiteCompileBudget
			return false
		}
	case dal.Array:
		value := reflect.ValueOf(n.Value)
		if value.IsValid() && (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) && !b.take(&b.nodes, value.Len(), sqliteCompileNodes) {
			return false
		}
		if value.IsValid() && (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) {
			for i := 0; i < value.Len(); i++ {
				size, known := sqliteArgumentBytes(value.Index(i).Interface())
				if !known || !b.take(&b.inputs, size, sqliteCompileBytes) {
					b.err = errSQLiteCompileBudget
					return false
				}
			}
		}
	case dal.AggregateFunc:
		if !text(n.FuncName()) {
			return false
		}
		for _, arg := range n.FuncArgs() {
			if !visit(arg) {
				return false
			}
		}
		if ordered, ok := n.(dal.OrderedAggregateFunc); ok {
			for _, order := range ordered.AggregateOrder() {
				if order == nil || !visit(order.Expression()) {
					b.err = errSQLiteCompileBudget
					return false
				}
			}
		}
	case dal.BinaryExpression:
		return text(string(n.Operator)) && visit(n.Left) && visit(n.Right)
	case dal.Comparison:
		return text(string(n.Operator)) && visit(n.Left) && visit(n.Right)
	case dal.GroupCondition:
		for _, child := range n.Conditions() {
			if !visit(child) {
				return false
			}
		}
	case dal.IsNullCondition:
		return visit(n.Operand())
	case dal.QuerySource, *dal.QuerySource, dal.QueryExpression, *dal.QueryExpression:
		b.err = errSQLiteCompileBudget
		return false
	case nil, dal.StarExpression:
	default:
		b.err = errSQLiteCompileBudget
		return false
	}
	return true
}
