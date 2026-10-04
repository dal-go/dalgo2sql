package dalgo2sql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// postgresIdentifierMode says how the PostgreSQL dialect writes a name.
type postgresIdentifierMode int

const (
	// postgresExact writes every name as the query spells it, inside double
	// quotes, so Album and album are two tables. It is the mode for databases
	// whose objects were created with quoted mixed-case names (DataTug).
	postgresExact postgresIdentifierMode = iota
	// postgresFoldLower writes strings.ToLower(name) inside double quotes. It is the
	// rule of dalgo2postgres's DDL (sql_gen.go quoteIdent), which stores every name
	// in lower case, so a database it created is read back with any spelling
	// (OVDB).
	postgresFoldLower
)

// postgresMaxIdentifierBytes is NAMEDATALEN-1. PostgreSQL truncates a longer
// identifier silently, so two long names that share their first 63 bytes would be
// one name to the server; the dialect refuses them instead.
const postgresMaxIdentifierBytes = 63

// Builtin pg_type OIDs (src/include/catalog/pg_type.dat) whose catalog category
// says less than the type's comparability: bytea is category U like uuid and
// json, and money, time and timetz share a category with types they do not
// compare to.
const (
	postgresOIDBytea  = 17
	postgresOIDMoney  = 790
	postgresOIDTime   = 1083
	postgresOIDTimeTZ = 1266
)

// postgresDialect is the typedDialect for PostgreSQL 12 and later. It writes SQL
// for compileTypedSQL; nothing here reads a value into the text.
//
// Identifiers. Every name is double-quoted with inner quotes doubled (quoteIdent).
// In postgresFoldLower mode the name is lower-cased first. The 63-byte limit is
// read on the name as written, that is after folding, which can be longer or
// shorter than the query's spelling (U+023A is 2 bytes and folds to 3).
//
// Constants. A constant is typed like a literal typed in psql, by its Go type and
// never by what it holds (the contract in typed_dialect.go): a signed integer is
// ?::bigint, an unsigned integer and a float are ?::numeric bound from decimal
// text (an unsigned value can exceed bigint, and a float is sent as its shortest
// decimal text, which is the number the caller wrote), a bool is ?::boolean, a
// time.Time is ?::timestamptz, bytes are ?::bytea, and a string is untyped, so the
// server reads it as a date, a UUID or an enum where the column says so. A nil is
// an untyped NULL; `== nil` never reaches here, the compiler writes IS NULL for it.
// A mismatch between a constant and a column (a number against text) is a server
// error, not an empty result. An untyped string with nothing to infer its type from
// (a lone constant in the select list) is a server error too.
//
// Statements. LIMIT and OFFSET are bound. NULLs sort first ascending and last
// descending, as in DALgo; the NULLS clause is dropped for a NOT NULL column so an
// index can serve the order. Division is on double precision with a zero divisor
// read as NULL. SUM and AVG are cast to double precision, as DALgo's generic
// engine returns float64; COUNT, MIN and MAX are native.
//
// Catalog facts. catalogFacts reads one catalog query (see postgresCatalogQuery)
// and returns facts keyed by the query's own spelling of each source, folded:
//
//	sources := typedQuerySources(q.From())            // as the query spells them
//	facts, err := dialect.catalogFacts(ctx, execute, sources)
//
// Sources holds an entry per source the server resolves, under
// typedSourceName{Schema: fold(schema as written, empty when none), Name:
// fold(name as written)}: an unqualified source is keyed with an empty Schema
// although the server finds it through search_path, because that is the key the
// compiler looks it up by. The relation asked of the server is the very text the
// statement will write for the source, so the catalog query and the statement
// resolve it the same way; run both on the same connection or transaction so
// search_path agrees. A source the server does not resolve, one that is not a
// table, view, materialized view, foreign table or partitioned table, and one with
// no readable column are absent. A source's Columns list every column a statement
// may read, in table order, for a view as for a table; system columns and dropped
// columns are not listed. Fold is the function quoteIdent applies
// (TestPostgresDialectFoldIsTheFoldQuoteIdentApplies pins it).
type postgresDialect struct {
	mode postgresIdentifierMode
}

var _ typedDialect = postgresDialect{}

func newPostgresDialect(mode postgresIdentifierMode) postgresDialect {
	return postgresDialect{mode: mode}
}

// fold is the one case rule of the dialect: quoteIdent writes its result and the
// catalog facts match columns by it.
func (d postgresDialect) fold(name string) string {
	if d.mode == postgresFoldLower {
		return strings.ToLower(name)
	}
	return name
}

func (postgresDialect) placeholderStyle() typedPlaceholderStyle {
	return typedPlaceholderStyle{Prefix: "$", IdentQuote: '"'}
}

func (d postgresDialect) quoteIdent(name string) (string, error) {
	// Validity is judged on the name as given: folding replaces invalid UTF-8 with
	// U+FFFD, which would hide it.
	if err := checkTypedIdentifier(name, math.MaxInt); err != nil {
		return "", err
	}
	folded := d.fold(name)
	if err := checkTypedIdentifier(folded, postgresMaxIdentifierBytes); err != nil {
		if d.mode == postgresFoldLower {
			err = fmt.Errorf("%w once folded to lower case", err)
		}
		return "", err
	}
	return quoteTypedIdentifier(folded, '"'), nil
}

func (postgresDialect) bind(value any) (string, any, error) {
	kind, err := typedKindOf(value)
	if err != nil {
		return "", nil, err
	}
	v := reflect.ValueOf(value)
	switch kind {
	case typedValueInteger:
		return "?::bigint", v.Int(), nil
	case typedValueUnsigned:
		return "?::numeric", strconv.FormatUint(v.Uint(), 10), nil
	case typedValueFloat:
		return "?::numeric", postgresNumericText(v.Float(), v.Type().Bits()), nil
	case typedValueBool:
		return "?::boolean", v.Bool(), nil
	case typedValueTime:
		return "?::timestamptz", value, nil
	case typedValueBytes:
		return "?::bytea", v.Bytes(), nil
	case typedValueText:
		return "?", v.String(), nil
	}
	// A NULL, and a driver.Valuer whose value the driver resolves: the server types
	// the marker from the place it stands in.
	return "?", value, nil
}

// postgresNumericText is the shortest decimal text that reads back as the same
// float of the given size (32 or 64 bits), in the plain form numeric accepts.
// numeric spells its three special values NaN, Infinity and -Infinity.
func postgresNumericText(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

func (postgresDialect) limitOffset(limit, offset int) (string, []any) {
	switch {
	case limit > 0 && offset > 0:
		return "LIMIT ? OFFSET ?", []any{int64(limit), int64(offset)}
	case limit > 0:
		return "LIMIT ?", []any{int64(limit)}
	case offset > 0:
		return "OFFSET ?", []any{int64(offset)}
	}
	return "", nil
}

func (postgresDialect) orderItem(expression string, descending, notNull bool) string {
	direction, nulls := " ASC", " NULLS FIRST"
	if descending {
		direction, nulls = " DESC", " NULLS LAST"
	}
	if notNull {
		return expression + direction
	}
	return expression + direction + nulls
}

// divide reads both operands as double precision: integer / integer would
// truncate (7 / 2 is 3), and a zero divisor would be an error where DALgo's result
// is NULL.
func (postgresDialect) divide(left, right string) string {
	return "((" + left + ")::double precision / NULLIF((" + right + ")::double precision, 0))"
}

// aggregateResult casts SUM and AVG, which PostgreSQL returns as bigint or numeric
// for integer input, to the float64 DALgo's generic engine returns. The cast is
// wrapped so the result stands next to any operator, as the contract asks.
func (postgresDialect) aggregateResult(function, aggregate string) string {
	if function == dal.SUM || function == dal.AVERAGE {
		return "((" + aggregate + ")::double precision)"
	}
	return aggregate
}

func (postgresDialect) emptyIn(negated bool) string {
	if negated {
		return "TRUE"
	}
	return "FALSE"
}

func (postgresDialect) capabilities() dal.QueryCapabilities {
	return dal.QueryCapabilities{
		GroupBy: true,
		Having:  true,
		OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count: true, CountDistinct: true,
			Sum: true, SumDistinct: true,
			Avg: true, AvgDistinct: true,
			Min: true, Max: true,
		},
	}
}

func (postgresDialect) window(string, []string, []string, []string) (string, error) {
	return "", typedUnsupported("window functions are reserved and not implemented")
}

// postgresCatalogHead and postgresCatalogTail surround the VALUES list of
// postgresCatalogQuery.
//
// to_regclass resolves the bound text exactly as the statement's FROM clause will,
// search_path included, and gives NULL for a name that is not a relation; the
// join then drops that source. pg_class limits the answer to what a SELECT can
// read. pg_attribute lists every column of the relation, a view's as a table's
// (attnum above zero skips system columns, attisdropped the dropped ones). pg_type
// gives the category of the column's type, taken from the base type for a domain.
// pg_collation says whether the column compares by bytes (collisdeterministic,
// PostgreSQL 12 and later; NULL for a type with no collation). A citext column is
// flagged like a non-deterministic collation: its equality ignores case.
const (
	postgresCatalogHead = `SELECT s.name, a.attname::text, format_type(a.atttypid, NULL), bt.typcategory::text, bt.oid::bigint, a.attnotnull, ` +
		`(NOT COALESCE(co.collisdeterministic, TRUE) OR bt.typname = 'citext') ` +
		`FROM (VALUES `
	postgresCatalogTail = `) AS s(name) ` +
		`JOIN pg_class c ON c.oid = to_regclass(s.name) AND c.relkind IN ('r', 'p', 'v', 'm', 'f') ` +
		`JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped ` +
		`JOIN pg_type t ON t.oid = a.atttypid ` +
		`JOIN pg_type bt ON bt.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END ` +
		`LEFT JOIN pg_collation co ON co.oid = a.attcollation ` +
		`ORDER BY s.name, a.attnum`
)

// postgresCatalogQuery is the one query catalogFacts sends, for n sources whose
// written names are bound as $1 to $n, each as text. Its text depends on n only.
func postgresCatalogQuery(n int) string {
	var query strings.Builder
	query.WriteString(postgresCatalogHead)
	for i := 1; i <= n; i++ {
		if i > 1 {
			query.WriteString(", ")
		}
		query.WriteString("($" + strconv.Itoa(i) + "::text)")
	}
	query.WriteString(postgresCatalogTail)
	return query.String()
}

// postgresTypeCategory maps a pg_type category letter to the compiler's coarse
// category. Only the four scalar families compare across types; anything else
// (uuid, json, enums, arrays, ranges, inet) compares to its own type only.
func postgresTypeCategory(category string, typeOID int64) typedTypeCategory {
	switch typeOID {
	case postgresOIDBytea:
		return typedTypeBinary
	case postgresOIDMoney, postgresOIDTime, postgresOIDTimeTZ:
		return typedTypeOther
	}
	switch category {
	case "S":
		return typedTypeText
	case "N":
		return typedTypeNumber
	case "B":
		return typedTypeBoolean
	case "D":
		return typedTypeTime
	}
	return typedTypeOther
}

// relation is the text the statement writes for a source: its quoted schema, when
// the query names one, and its quoted name.
func (d postgresDialect) relation(source typedSourceName) (string, error) {
	name, err := d.quoteIdent(source.Name)
	if err != nil {
		return "", fmt.Errorf("table name: %w", err)
	}
	if source.Schema == "" {
		return name, nil
	}
	schema, err := d.quoteIdent(source.Schema)
	if err != nil {
		return "", fmt.Errorf("schema name: %w", err)
	}
	return schema + "." + name, nil
}

func (d postgresDialect) catalogFacts(ctx context.Context, execute executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error) {
	facts := typedCatalogFacts{Fold: d.fold}
	if len(sources) == 0 {
		return facts, nil
	}
	keys := make(map[string]typedSourceName, len(sources))
	names := make([]any, 0, len(sources))
	for _, source := range sources {
		text, err := d.relation(source)
		if err != nil {
			return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
		}
		if _, asked := keys[text]; asked {
			continue
		}
		keys[text] = typedSourceName{Schema: d.fold(source.Schema), Name: d.fold(source.Name)}
		names = append(names, text)
	}
	rows, err := execute(ctx, postgresCatalogQuery(len(names)), names...)
	if err != nil {
		return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	facts.Sources = make(map[typedSourceName]typedSourceFacts, len(names))
	for rows.Next() {
		var relation, column, dataType, category string
		var typeOID int64
		var notNull, nonDeterministic bool
		if err := rows.Scan(&relation, &column, &dataType, &category, &typeOID, &notNull, &nonDeterministic); err != nil {
			return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
		}
		key, asked := keys[relation]
		if !asked {
			return typedCatalogFacts{}, errors.New("catalog facts: the catalog answered for a relation that was not asked for")
		}
		source := facts.Sources[key]
		source.Columns = append(source.Columns, typedColumnFact{
			Name:                      column,
			DataType:                  dataType,
			Category:                  postgresTypeCategory(category, typeOID),
			NotNull:                   notNull,
			NonDeterministicCollation: nonDeterministic,
		})
		facts.Sources[key] = source
	}
	if err := rows.Err(); err != nil {
		return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
	}
	return facts, nil
}
