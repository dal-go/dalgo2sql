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
	// (OVDB). A field mask or access check applied on such a mount must fold the
	// names it compares first, or Total passes a mask that names total.
	postgresFoldLower
)

// postgresMaxIdentifierBytes is NAMEDATALEN-1. PostgreSQL truncates a longer
// identifier silently, so two long names that share their first 63 bytes would be
// one name to the server; the dialect refuses them instead. The limit is counted in
// bytes of the server's encoding, and the dialect counts the UTF-8 bytes it writes:
// the two agree only when the server encoding is UTF8, which is the encoding the
// dialect assumes. On a server in another encoding a name of 63 UTF-8 bytes can
// still be longer than the server's limit and be truncated.
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

// Builtin pg_type OIDs of the types that cannot key a join (see postgresNoJoinKey).
// The geometric ones are listed too, for the array types whose element is one; a
// scalar of category G is caught by its category.
var postgresNoJoinKeyOIDs = map[int64]bool{
	114:  true, // json: no equality operator
	142:  true, // xml: no equality operator
	4072: true, // jsonpath: no equality operator
	2970: true, // txid_snapshot: no equality operator
	5038: true, // pg_snapshot: no equality operator
	600:  true, // point: no = (only the "same as" operator ~=)
	601:  true, // lseg
	602:  true, // path
	603:  true, // box: = compares area
	604:  true, // polygon
	628:  true, // line
	718:  true, // circle: = compares area
	26:   true, // oid
	24:   true, // regproc
	2202: true, // regprocedure
	2203: true, // regoper
	2204: true, // regoperator
	2205: true, // regclass
	2206: true, // regtype
	3734: true, // regconfig
	3769: true, // regdictionary
	4089: true, // regnamespace
	4096: true, // regrole
	4191: true, // regcollation
}

// postgresDialect is the typedDialect for PostgreSQL 12 and later, with one
// exception: NUMERIC accepts Infinity and -Infinity from PostgreSQL 14, so a float
// infinity constant is a server error on 12 and 13. It writes SQL for
// compileTypedSQL; nothing here reads a value into the text.
//
// Identifiers. Every name is double-quoted with inner quotes doubled (quoteIdent).
// In postgresFoldLower mode the name is lower-cased first. The 63-byte limit is
// read on the name as written, that is after folding, which can be longer or
// shorter than the query's spelling (U+023A is 2 bytes and folds to 3). It counts
// UTF-8 bytes, which is the server's count when its encoding is UTF8
// (postgresMaxIdentifierBytes).
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
// error, not an empty result. An untyped string is typed by the place it stands
// in: the column it is compared to, or text in the select list (PostgreSQL 10 and
// later resolve an unknown-typed output column to text; not verified against a
// server here). Where nothing types it, as an aggregate's argument, the server
// refuses the statement (error 42P18).
//
// Pass Go integers for whole numbers. A whole number that arrives as a float64, as
// every number in a JSON-decoded value does, is bound ?::numeric like any float,
// not ?::bigint: the comparison is then numeric, correct but unable to use an index
// on an integer column. The dialect does not turn a float64 into an integer, which
// would make the cast depend on the value (the contract in typed_dialect.go).
//
// Statements. LIMIT and OFFSET are bound. NULLs sort first ascending and last
// descending, as in DALgo; the NULLS clause is dropped for a NOT NULL column so an
// index can serve the order. Arithmetic is on double precision: division reads a
// zero divisor as NULL, and +, - and * cast both operands, so an integer
// overflow cannot differ from DALgo's generic engine and the SQLite path (arithmetic
// is float64 there). SUM and AVG are cast to double precision, as DALgo's generic
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
// (TestPostgresDialectFoldIsTheFoldQuoteIdentApplies pins it). A column of the
// relation's PRIMARY KEY constraint is marked PrimaryKey: a view, a materialized view and
// a foreign table have none, and the records reader keys a record read from a source
// nobody declared by the key when it is one column (typedSourceFacts.primaryKeyPosition).
type postgresDialect struct {
	mode postgresIdentifierMode
}

var (
	_ typedDialect      = postgresDialect{}
	_ typedColumnBinder = postgresDialect{}
)

func newPostgresDialect(mode postgresIdentifierMode) postgresDialect {
	return postgresDialect{mode: mode}
}

// postgresDialectFor builds the dialect DbOptions asks for. An IdentifierCase this
// package does not define is refused rather than read as the default: a mount that
// meant to fold names must not run in the exact mode.
func postgresDialectFor(options DbOptions) (postgresDialect, error) {
	switch options.IdentifierCase {
	case "", IdentifierCaseExact:
		return newPostgresDialect(postgresExact), nil
	case IdentifierCaseFoldLower:
		return newPostgresDialect(postgresFoldLower), nil
	}
	return postgresDialect{}, fmt.Errorf("unsupported identifier case %q: use %q or %q", options.IdentifierCase, IdentifierCaseExact, IdentifierCaseFoldLower)
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

// bindAgainst binds a float compared with a column that is a real (float4) as ?::real, from its
// decimal text, so that the comparison runs in the column's own type: a real stores 0.1 as the
// nearest float4, and compared as a numeric it is not the number 0.1, so the equality finds
// nothing. A column of any other type, and a constant that is not a float, are bound as bind
// binds them, and so is a finite float that a real cannot hold (past its range, or too small for
// a normal float4), which the server would refuse as out of range where a numeric holds it.
func (postgresDialect) bindAgainst(value any, column typedColumnFact) (string, any, bool) {
	if kind, _ := typedKindOf(value); kind != typedValueFloat || column.DataType != "real" {
		return "", nil, false
	}
	v := reflect.ValueOf(value)
	if magnitude := math.Abs(v.Float()); !math.IsInf(magnitude, 0) && !math.IsNaN(magnitude) && magnitude != 0 &&
		(magnitude > math.MaxFloat32 || magnitude < postgresSmallestNormalReal) {
		return "", nil, false
	}
	return "?::real", postgresNumericText(v.Float(), v.Type().Bits()), true
}

// postgresSmallestNormalReal is the smallest positive normal float4, 2^-126.
const postgresSmallestNormalReal = 1.17549435082228750796873653722224568e-38

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

// arithmeticOperand reads one operand of +, - and * as double precision, as divide
// does: integer * integer would fail with error 22003 past the range of the type
// where DALgo's generic engine and the SQLite path return a float. The cast is
// wrapped so the operand stands next to any operator, as the contract asks.
func (postgresDialect) arithmeticOperand(operand string) string {
	return "((" + operand + ")::double precision)"
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
// Every system object is spelt pg_catalog.x: an unqualified name is looked up through
// search_path, where a temporary relation of the same name, or a schema listed before
// pg_catalog, would answer with another column list.
//
// CTE s holds the bound relation texts. CTE c is the columns: pg_catalog.to_regclass
// resolves the bound text exactly as the statement's FROM clause will, search_path
// included, and gives NULL for a name that is not a relation, so that source has no
// row; relkind limits the answer to what a SELECT can read; pg_attribute lists every
// column of the relation, a view's as a table's (attnum above zero skips system
// columns, attisdropped the dropped ones). Recursive CTE d walks each column's type
// down the chain of domains to the base type that is not a domain, however deep the
// chain (typbasetype of a domain names its parent), and the final select reads the
// category, the OID and the element type of that base type. The not-null fact (the
// column that is named attnotnull) is true only when the column is not null and no
// not-null constraint that is not validated covers it: since PostgreSQL 18 a constraint
// added NOT VALID (pg_constraint.contype 'n', convalidated false) sets
// pg_attribute.attnotnull while rows that were there may hold NULL, and the order of
// NULLs is then the one DALgo's rule writes a NULLS clause for. PostgreSQL 17 and
// earlier have no contype 'n' row, so there the fact is attnotnull. collisdeterministic
// (PostgreSQL 12 and later; NULL for a type with no collation) says whether the
// column compares by bytes. A citext column is flagged like a non-deterministic
// collation: its equality ignores case. The last column, pk, says whether the column
// is one of those the relation's PRIMARY KEY constraint constrains (pg_constraint with
// contype 'p', whose conkey lists the key columns only, which a view, a materialized view
// and a foreign table never have, and which a unique index is not): a record read from
// a source nobody declared is keyed by it. The very last column, collation, is the OID of the
// column's collation when it is not the database's default (OID 100) and 0 otherwise, which a
// join needs: the server refuses to compare two columns whose collations are different and
// both not the default (SQLSTATE 42P22). The index behind the key is not asked:
// pg_index.indkey lists the INCLUDE columns of the index as well, since PostgreSQL 11, so
// a key declared PRIMARY KEY (id) INCLUDE (payload) would show two columns.
//
// The query was written by reading the PostgreSQL catalog documentation; no server
// has run it. SQL-08 pins it against PostgreSQL 12, 17 and 18.
const (
	postgresCatalogHead = `WITH RECURSIVE s(name) AS (VALUES `
	postgresCatalogTail = `), ` +
		`c AS (SELECT s.name, a.attnum, a.attname, a.atttypid, ` +
		`(a.attnotnull AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint n WHERE n.conrelid = r.oid AND n.contype = 'n' AND a.attnum = ANY (n.conkey) AND NOT n.convalidated)) AS attnotnull, ` +
		`a.attcollation, ` +
		`EXISTS (SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.conrelid = r.oid AND k.contype = 'p' AND a.attnum = ANY (k.conkey)) AS pk ` +
		`FROM s ` +
		`JOIN pg_catalog.pg_class r ON r.oid = pg_catalog.to_regclass(s.name) AND r.relkind IN ('r', 'p', 'v', 'm', 'f') ` +
		`JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped), ` +
		`d(start, base, typtype, typbasetype) AS (` +
		`SELECT t.oid, t.oid, t.typtype, t.typbasetype FROM pg_catalog.pg_type t WHERE t.oid IN (SELECT c.atttypid FROM c) ` +
		`UNION ALL ` +
		`SELECT d.start, p.oid, p.typtype, p.typbasetype FROM d JOIN pg_catalog.pg_type p ON d.typtype = 'd' AND p.oid = d.typbasetype) ` +
		`SELECT c.name, c.attname::text, pg_catalog.format_type(c.atttypid, NULL), bt.typcategory::text, bt.oid::bigint, bt.typelem::bigint, c.attnotnull, ` +
		`(NOT COALESCE(co.collisdeterministic, TRUE) OR bt.typname = 'citext'), c.pk, ` +
		`CASE WHEN c.attcollation = 100 THEN 0 ELSE c.attcollation::bigint END ` +
		`FROM c ` +
		`JOIN d ON d.start = c.atttypid AND d.typtype <> 'd' ` +
		`JOIN pg_catalog.pg_type bt ON bt.oid = d.base ` +
		`LEFT JOIN pg_catalog.pg_collation co ON co.oid = c.attcollation ` +
		`ORDER BY c.name, c.attnum`
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

// postgresSuggestionQuery lists the names suggestSource chooses from, as (schema,
// table) pairs of what the session can SELECT. $1 is the schema the query wrote,
// folded, or empty when it wrote none. With no schema the candidates are the
// relations search_path makes visible, minus the system schemas; with one, the
// relations of the schema that has that name in any case. The query depends on
// nothing the caller wrote: the schema travels as an argument.
const postgresSuggestionQuery = `SELECT n.nspname::text, c.relname::text ` +
	`FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
	`WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND pg_catalog.has_table_privilege(c.oid, 'SELECT') ` +
	`AND CASE WHEN $1::text = '' THEN pg_catalog.pg_table_is_visible(c.oid) AND n.nspname NOT IN ('pg_catalog', 'information_schema') ` +
	`ELSE pg_catalog.lower(n.nspname::text) = pg_catalog.lower($1::text) END ` +
	`ORDER BY n.nspname, c.relname LIMIT 5000`

// suggestSource names the relation the caller most likely meant. On a mount that folds
// case, only a name the mount can write is a candidate: a stored Album, which no
// spelling reaches, would be a hint to a name that folds back to the one that was not
// found. A candidate in the schema the query wrote, spelt in another case, comes
// first when its name is the one asked: the schema is then the whole mistake. The
// relation that was asked for, schema and name, is never the hint.
func (d postgresDialect) suggestSource(ctx context.Context, execute executeQueryFunc, source typedSourceName) (typedSourceName, bool, error) {
	rows, err := execute(ctx, postgresSuggestionQuery, d.fold(source.Schema))
	if err != nil {
		return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []typedSourceName
	var names []string
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
		}
		if source.Schema == "" {
			schema = ""
		}
		if d.fold(schema) != schema || d.fold(name) != name {
			continue // a mount that folds writes it in another form than the stored one
		}
		found = append(found, typedSourceName{Schema: schema, Name: name})
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
	}
	asked := d.fold(source.Name)
	for _, candidate := range found {
		if candidate.Name == asked && candidate.Schema != d.fold(source.Schema) {
			return candidate, true, nil
		}
	}
	if nearest := typedNearestName(asked, names); nearest >= 0 {
		return found[nearest], true, nil
	}
	return typedSourceName{}, false, nil
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

// postgresNoJoinKey reports whether a column of a base type with this category,
// OID and element type must not key a join: its type has no equality operator, or
// compares in a way DALgo does not mean (postgresNoJoinKeyOIDs). An array is as good
// as its element, which the catalog gives as the element type; any other type with
// an element type (name, point) is judged by itself.
func postgresNoJoinKey(category string, typeOID, elementOID int64) bool {
	if category == "A" {
		typeOID = elementOID
	}
	return category == "G" || postgresNoJoinKeyOIDs[typeOID]
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
	// Fold is the rule quoteIdent applies, and nil when it applies none: in the exact
	// mode names are matched as written, which is what nil says (and what tells a
	// reader that table names are case-sensitive).
	facts := typedCatalogFacts{}
	if d.mode == postgresFoldLower {
		facts.Fold = d.fold
	}
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
		var typeOID, elementOID, collation int64
		var notNull, nonDeterministic, primaryKey bool
		if err := rows.Scan(&relation, &column, &dataType, &category, &typeOID, &elementOID, &notNull, &nonDeterministic, &primaryKey, &collation); err != nil {
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
			NoJoinKey:                 postgresNoJoinKey(category, typeOID, elementOID),
			PrimaryKey:                primaryKey,
			Collation:                 collation,
		})
		facts.Sources[key] = source
	}
	if err := rows.Err(); err != nil {
		return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
	}
	return facts, nil
}
