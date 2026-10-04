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
//   - Name resolution (an assumption of the compiler, not checked): a statement
//     over one source writes bare column names, and the compiler relies on the
//     PostgreSQL reading of them, and on the server taking two quoted names for
//     one identifier exactly when the texts quoteIdent returned are equal. What
//     quoteIdent does to a name is the dialect's own business, and it may fold
//     case: PostgreSQL's FoldLower mode writes Total and total as "total".
//     A bare name that stands alone in ORDER BY is a select-list output first,
//     so the compiler refuses an ORDER BY item whose written name is the written
//     name of another output (typedCheckOrderByName). That check compares the
//     quoted names quoteIdent returned, never the query's spelling, so it holds
//     for a folding dialect too. Three other kinds of lookup do not compare
//     written names. The first looks a name up by the query's spelling, because it
//     decides what DALgo means and not what the server reads: the lookup of a
//     select alias (typedAliasRewriter), source qualifiers, and the comparison of
//     ORDER BY with a source's scan (validateTypedScanRestated). Under a folding
//     dialect those treat Total and TOTAL as different names, which makes them
//     refuse or leave a name as the column, never pick another expression, and the
//     ORDER BY check above catches the written names that then meet. The second
//     matches a wildcard exclusion against the catalog's names exactly, as
//     dal.WildcardProjection.Excludes defines. The third is the catalog facts
//     lookup, which folds the query's name with typedCatalogFacts.Fold and matches
//     a column only when the catalog's own name equals the result: Fold must be
//     the case rule quoteIdent applies, a catalog column whose name is not its own
//     folded form is never matched, and a select-all over a source that lists one is
//     refused, even when its exclusions leave that column out, because the check
//     runs on every catalog column before the exclusions apply and a statement
//     could not name that column. In GROUP BY a
//     bare name is an input column before it is an output, and HAVING cannot see
//     outputs at all, so both write the input column as it is, under any case
//     rule. A dialect for an engine that reads a select alias before an input
//     column in GROUP BY or HAVING needs that check extended before it is added.
//   - A name that matches no column is not an error to the server, it is read as
//     something else: a bare name equal to a source's identity (its alias, else its
//     table name) as the whole row, one composite value that holds every column,
//     and a qualified name x.f that is no column of x as the function f applied to
//     the row of x (x.to_json is to_json(x); row_to_json, to_jsonb, concat, count
//     and quote_literal read alike). Either returns the row inside one value, past
//     any policy that checks field names. The compiler closes both with the
//     catalog facts (typedCompiler.checkNamesAColumn): a field of a source whose
//     facts are known must be one of its columns, else the statement fails with a
//     plain error naming the column; and when the facts do not know the source, an
//     unqualified field written as the base source's identity is refused with
//     dal.ErrNotSupported. A qualified name without facts cannot be checked, so
//     the caller (SQL-04) must pass catalog facts for every source it compiles,
//     and each source's Columns must list every column a query may name (a system
//     column such as ctid is not one unless the dialect lists it).
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
	// needs about the sources of a query. The reader calls it before compiling,
	// with typedQuerySources(q.From()): the sources as the query spells them.
	// The facts it returns are keyed by that spelling, folded with the facts'
	// Fold and schema included, because that is the key the compiler looks a
	// source up by: an unqualified source has an empty Schema whatever schema the
	// server resolves it to, and a source the server does not resolve is absent.
	// The lookup must describe the relation the statement will read, so it
	// resolves the very text the statement writes for the source, on the same
	// connection or transaction. Sources lists every column a statement may read
	// (see the name-resolution rules above). The PostgreSQL dialect
	// (dialect_postgres.go) documents how it does this.
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
// type check) and cannot check that a field is a column: it refuses a bare name
// written as the base source's identity, and leaves a qualified name to the
// server, which may read it as a function of the row. Pass facts for every source
// of a query (see the name-resolution rules in typedDialect).
type typedCatalogFacts struct {
	// Fold maps a name to the key it is stored under, for engines or modes
	// that fold identifier case. Nil means names are matched exactly. It must be
	// the case rule the dialect's quoteIdent applies, and Sources holds the folded
	// names: a column is matched when its name is the folded name of the query's.
	Fold func(string) string
	// Sources lists the columns of every source the query reads, all of them (see
	// the name-resolution rules in typedDialect): a field the facts of a known
	// source do not list is refused.
	Sources map[typedSourceName]typedSourceFacts
}

func (f typedCatalogFacts) fold(name string) string {
	if f.Fold == nil {
		return name
	}
	return f.Fold(name)
}

// addressable reports whether a statement can write a catalog name: the name is
// its own folded form (every name is, when nothing folds). A column that is not
// can never be matched by column, and a select-all cannot list it.
func (f typedCatalogFacts) addressable(name string) bool { return f.fold(name) == name }

func (f typedCatalogFacts) source(name typedSourceName) (typedSourceFacts, bool) {
	source, ok := f.Sources[typedSourceName{Schema: f.fold(name.Schema), Name: f.fold(name.Name)}]
	return source, ok
}

// column finds the column a statement means when it writes the name column. The
// statement writes the folded name, so a catalog column matches only when its own
// name is that folded name: comparing both sides through Fold would let a column
// the dialect cannot write (Total, where the statement says "total") stand in for
// another, or take the first of several that fold together.
func (f typedCatalogFacts) column(name typedSourceName, column string) (typedColumnFact, bool) {
	source, ok := f.source(name)
	if !ok {
		return typedColumnFact{}, false
	}
	key := f.fold(column)
	for _, candidate := range source.Columns {
		if candidate.Name == key {
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
