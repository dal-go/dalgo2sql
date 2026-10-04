package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

// typedDialect is the spelling guide for one statically typed SQL engine
// (PostgreSQL first; MySQL, SQL Server and Oracle later). compileTypedSQL owns
// the walk over the query; the dialect owns every engine-specific word.
//
// The contract that keeps the compiler safe. The compiler checks every rule
// marked (checked) on each fragment a dialect returns and refuses the statement
// when one is broken, so a faulty dialect fails loudly instead of binding a
// value to the wrong place.
//
//   - Nothing a dialect returns may depend on the content of a value. Values
//     travel only as bound arguments, so the SQL text is the same whatever the
//     caller's constants are. bind may type its marker by the Go type of the
//     value (typedKindOf), never by what the value holds: not "bigint for a
//     whole number and numeric for a fraction" read from the number, but one cast
//     per type. assertTypedTextIgnoresValues (typed_sql_property_test.go) takes
//     a dialect and is the test every dialect must pass.
//   - Every placeholder a dialect writes is the neutral marker "?". The
//     compiler numbers the markers once, at the end, with placeholderStyle.
//     bind and limitOffset write exactly one marker per argument they return;
//     every other method writes none of its own (checked).
//   - Operand rule 1, count and order (checked): divide, aggregateResult and
//     orderItem receive operands already rendered as SQL text. The compiler
//     appends the operands' arguments in the order it passes them, and the
//     numbering pass gives the nth marker of the final text the nth argument.
//     So an operand that carries a marker must be written verbatim, exactly
//     once, and the operands in the order they were passed. A dialect that
//     writes divide's right operand before its left, or repeats one, keeps the
//     marker count right and binds values to the wrong positions. An operand
//     without a marker carries no argument and may be repeated, moved or
//     wrapped freely. The check reads text, and two operands with the same text
//     cannot be told apart by it, as in (a + 1) / (a + 2). So whenever both
//     operands of a division carry a marker, the compiler also calls divide
//     once with two different probe operands that carry one marker each and
//     applies the check to that fragment. divide must therefore be a function
//     of its two operand texts alone; it is called more than once per
//     statement. aggregateResult and orderItem take one operand, so they have
//     no order to get wrong.
//   - Operand rule 2, no literal and no comment (checked): a fragment is
//     quoted identifiers, keywords, punctuation and the marker, never a string
//     literal and never a comment. The numbering pass understands quoted
//     identifiers only, so a "?" inside a literal or a comment would be
//     numbered as a parameter. Write constants with bind.
//   - Fragments that stand for an expression (divide, aggregateResult) must be
//     self-delimiting, that is wrapped in parentheses or a function call, so
//     the compiler can place them next to any operator.
//   - An error a dialect returns must not quote a value: the compiler passes it
//     on, and a server logs it. Name the type or the rule, never the content.
//
// The interface stays unexported until three dialects exist.
type typedDialect interface {
	// placeholderStyle tells the final numbering pass how to spell the nth
	// placeholder and which byte delimits a quoted identifier.
	placeholderStyle() typedPlaceholderStyle

	// quoteIdent validates one identifier segment (see checkTypedIdentifier:
	// non-empty, no NUL, valid UTF-8, within the engine's length limit) and
	// returns it quoted. It is the only way an identifier reaches SQL text.
	quoteIdent(name string) (string, error)

	// bind returns the SQL that stands for one constant and the argument to
	// send beside the text. The SQL contains exactly one marker, for example
	// "?::bigint" for a whole number and "?" for a string whose type the server
	// should infer. The value has already passed typedKindOf.
	bind(value any) (marker string, arg any, err error)

	// limitOffset returns the row-limiting clause (without a leading space) and
	// its arguments, or "" when limit and offset are both zero. Both numbers are
	// non-negative; a zero limit means unlimited.
	limitOffset(limit, offset int) (clause string, args []any)

	// orderItem renders one ORDER BY item. DALgo sorts NULL first ascending and
	// last descending; notNull says the expression can never be NULL, so a
	// dialect may drop its NULLS clause and let an index serve the order.
	orderItem(expression string, descending, notNull bool) string

	// divide renders left / right with the engine's numeric semantics: DALgo
	// divides as floating point and a zero divisor yields NULL.
	divide(left, right string) string

	// aggregateResult wraps one rendered aggregate (function is dal.SUM,
	// dal.COUNT, ...) so its result type matches DALgo's, for example a cast of
	// SUM and AVG to double precision.
	aggregateResult(function, aggregate string) string

	// emptyIn renders IN () (or NOT IN () when negated) over an empty list,
	// which SQL has no syntax for: false, or true when negated, even for NULL.
	emptyIn(negated bool) string

	// capabilities is what the engine runs natively. The compiler refuses any
	// aggregation these flags do not cover.
	capabilities() dal.QueryCapabilities

	// catalogFacts reads, with one catalog lookup, the facts compileTypedSQL
	// needs about the sources of a query. The reader calls it before compiling.
	catalogFacts(ctx context.Context, execute executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error)

	// window is reserved for window functions. DTQL has none today and the
	// compiler never calls it; the seam exists so a dialect can add them later
	// without changing this interface.
	window(function string, args, partitionBy, orderBy []string) (string, error)
}

// typedSourceName names a table as a query spells it.
type typedSourceName struct {
	Schema string
	Name   string
}

// typedTypeCategory groups catalog types by whether two columns can be
// compared. Number covers every integer, float and decimal type.
type typedTypeCategory int

const (
	typedTypeUnknown typedTypeCategory = iota
	typedTypeText
	typedTypeNumber
	typedTypeBoolean
	typedTypeTime
	typedTypeBinary
	// typedTypeOther is for types such as uuid, json or an enum: two of them
	// compare only when they are the same type.
	typedTypeOther
)

// typedColumnFact is what the catalog says about one column.
type typedColumnFact struct {
	Name string
	// DataType is the engine's own type name, used to accept a join between two
	// columns of the same type that have no common category.
	DataType string
	Category typedTypeCategory
	NotNull  bool
	// NonDeterministicCollation marks columns whose equality is not byte
	// equality (citext, ICU nondeterministic collations). The compiler carries
	// it for dialect helpers; it does not act on it.
	NonDeterministicCollation bool
}

// typedSourceFacts holds a source's columns in table order.
type typedSourceFacts struct {
	Columns []typedColumnFact
}

// typedCatalogFacts is the compiler's whole knowledge of the database. The
// zero value knows nothing, and the compiler then emits correct but less
// optimised SQL (no wildcard expansion, no NOT NULL shortcuts, no join key
// type check).
type typedCatalogFacts struct {
	// Fold maps a name to the key it is stored under, for engines or modes
	// that fold identifier case. Nil means names are matched exactly.
	Fold    func(string) string
	Sources map[typedSourceName]typedSourceFacts
}

func (f typedCatalogFacts) fold(name string) string {
	if f.Fold == nil {
		return name
	}
	return f.Fold(name)
}

func (f typedCatalogFacts) source(name typedSourceName) (typedSourceFacts, bool) {
	source, ok := f.Sources[typedSourceName{Schema: f.fold(name.Schema), Name: f.fold(name.Name)}]
	return source, ok
}

func (f typedCatalogFacts) column(name typedSourceName, column string) (typedColumnFact, bool) {
	source, ok := f.source(name)
	if !ok {
		return typedColumnFact{}, false
	}
	key := f.fold(column)
	for _, candidate := range source.Columns {
		if f.fold(candidate.Name) == key {
			return candidate, true
		}
	}
	return typedColumnFact{}, false
}

// typedJoinKeysComparable reports whether a join may equate two columns on a
// statically typed engine: they share a scalar category, or they are the same
// named type. A column of unknown type is comparable only to the same type.
func typedJoinKeysComparable(a, b typedColumnFact) bool {
	if a.Category == b.Category && a.Category != typedTypeUnknown && a.Category != typedTypeOther {
		return true
	}
	return a.DataType != "" && a.DataType == b.DataType
}

// checkTypedIdentifier is the validation every dialect's quoteIdent starts
// with. It never echoes the identifier, which may be hostile or very long.
func checkTypedIdentifier(name string, maxBytes int) error {
	switch {
	case name == "":
		return fmt.Errorf("identifier is empty")
	case strings.IndexByte(name, 0) >= 0:
		return fmt.Errorf("identifier contains a NUL byte")
	case !utf8.ValidString(name):
		return fmt.Errorf("identifier is not valid UTF-8")
	case len(name) > maxBytes:
		return fmt.Errorf("identifier is %d bytes, over the engine limit of %d", len(name), maxBytes)
	}
	return nil
}

// quoteTypedIdentifier wraps name in quote and doubles any quote inside it.
// The name must already have passed checkTypedIdentifier.
func quoteTypedIdentifier(name string, quote byte) string {
	q := string(quote)
	return q + strings.ReplaceAll(name, q, q+q) + q
}

// isQuotedTypedIdentifier reports whether text is exactly one quoted
// identifier: wrapped in quote, never empty, with every inner quote doubled and
// no NUL byte.
func isQuotedTypedIdentifier(text string, quote byte) bool {
	if len(text) < 3 || text[0] != quote || text[len(text)-1] != quote {
		return false
	}
	inner := text[1 : len(text)-1]
	for i := 0; i < len(inner); i++ {
		switch inner[i] {
		case 0:
			return false
		case quote:
			if i+1 >= len(inner) || inner[i+1] != quote {
				return false
			}
			i++
		}
	}
	return true
}

// typedValueKind classifies the Go type of a constant so a dialect can type
// its placeholder by the type, never by the value.
type typedValueKind int

const (
	typedValueNull typedValueKind = iota
	typedValueValuer
	typedValueTime
	typedValueBool
	typedValueInteger
	typedValueUnsigned
	typedValueFloat
	typedValueText
	typedValueBytes
)

// typedKindOf accepts the constant types DALgo can hold and refuses the rest
// with an error matching dal.ErrNotSupported.
func typedKindOf(value any) (typedValueKind, error) {
	if value == nil {
		return typedValueNull, nil
	}
	if _, ok := value.(driver.Valuer); ok {
		return typedValueValuer, nil
	}
	if _, ok := value.(time.Time); ok {
		return typedValueTime, nil
	}
	valueType := reflect.TypeOf(value)
	switch valueType.Kind() {
	case reflect.Bool:
		return typedValueBool, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return typedValueInteger, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return typedValueUnsigned, nil
	case reflect.Float32, reflect.Float64:
		return typedValueFloat, nil
	case reflect.String:
		return typedValueText, nil
	case reflect.Slice:
		if valueType.Elem().Kind() == reflect.Uint8 {
			return typedValueBytes, nil
		}
	}
	return typedValueNull, typedUnsupported("SQL value type %T", value)
}
