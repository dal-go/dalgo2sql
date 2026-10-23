package dalgo2sql

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

// mysqlMaxIdentifierChars is the length limit MySQL puts on a database, table,
// column or alias name, in characters (an alias may be longer, 256 characters; the
// dialect holds every name to the shorter limit, so a long alias is declined as
// unsupported rather than written).
const mysqlMaxIdentifierChars = 64

// mysqlNoOffsetLimit is the LIMIT MySQL documents for "all the rows from an offset
// on": the statement has no OFFSET without a LIMIT.
const mysqlNoOffsetLimit = "18446744073709551615"

// mysqlDialect is the typedDialect for MySQL 8.0.17 and later (CAST AS DOUBLE),
// verified against MySQL 8.4. It writes SQL for compileTypedSQL; nothing here reads
// a value into the text.
//
// Identifiers. Every name is written exactly as the query spells it, inside
// backticks, with an inner backtick doubled. MySQL limits a name to 64 characters
// of the Basic Multilingual Plane, and a database, table or column name may not end
// with a space; quoteIdent refuses anything else, so no name is cut or altered by
// the server. Table names are case-sensitive or not by the server's
// lower_case_table_names; column names and aliases never are, and the server also
// ignores accents when it compares them, and it reads a name inside an ORDER BY
// expression as a select-list output first. So every field is written qualified
// with its source (typedEngineRules.qualifiesBareFields), which no output name can
// capture. The catalog facts match a column by its exact name: a query that
// spells a column in another case than the catalog's is refused with a plain error
// although the server would accept it.
//
// Constants. Every constant is one ? marker, typed by the driver from its Go type:
// a signed integer as int64, an unsigned one as uint64, a float as float64 (a
// float32 as the float64 nearest its shortest decimal text, so float32(0.1) is
// 0.1; NaN and the infinities are refused), a bool as bool, a time.Time, []byte and string as they are. A nil is a NULL.
//
// Statements. LIMIT and OFFSET are bound; an OFFSET without a limit is written with
// MySQL's largest LIMIT. MySQL sorts NULL first ascending and last descending, as
// DALgo does, so ORDER BY writes no NULLS clause. Arithmetic is on DOUBLE: +, - and *
// cast both operands, division casts both and reads a zero divisor as NULL (as MySQL
// does itself), and SUM and AVG cast their argument to DOUBLE (AVG of an exact value
// would otherwise be a DECIMAL rounded to 4 more places). Text follows the column's
// collation: with MySQL's default, utf8mb4_0900_ai_ci, 'abc' = 'ABC' is true, and
// ORDER BY, GROUP BY, DISTINCT, MIN and MAX treat 'a' and 'A' alike, where DALgo's
// generic engine compares bytes.
//
// Catalog facts come from information_schema.COLUMNS, one query for every source,
// for the schema the query names or, for an unqualified source, DATABASE(): the
// schema the session reads an unqualified name from. Run it on the connection the
// statement runs on (the database pins one: readExecutor). A TEMPORARY table is not
// listed by information_schema. Alone, it is not found; but when a base table of the
// same name exists, the statement reads the TEMPORARY table while the facts describe
// the base table, so the select-all expansion, the NOT NULL facts and the key the
// records are read by describe another relation. Do not read through a connection that
// holds a TEMPORARY table shadowing a base table.
type mysqlDialect struct{}

var (
	_ typedDialect     = mysqlDialect{}
	_ typedEngineRules = mysqlDialect{}
)

func (mysqlDialect) placeholderStyle() typedPlaceholderStyle {
	return typedPlaceholderStyle{IdentQuote: '`'}
}

func (mysqlDialect) quoteIdent(name string) (string, error) {
	if err := checkTypedIdentifier(name, math.MaxInt); err != nil {
		return "", err
	}
	if length := utf8.RuneCountInString(name); length > mysqlMaxIdentifierChars {
		return "", &typedIdentifierTooLongError{length: length, limit: mysqlMaxIdentifierChars, characters: true}
	}
	for _, r := range name {
		if r > 0xFFFF {
			return "", errors.New("identifier holds a character outside the Basic Multilingual Plane, which MySQL does not accept in a name")
		}
	}
	if strings.HasSuffix(name, " ") {
		return "", errors.New("identifier ends with a space, which MySQL does not accept in a name")
	}
	return quoteTypedIdentifier(name, '`'), nil
}

// typedFiniteFloat64 is a float constant as a float64: a float32 is read from its
// shortest decimal text, so the constant float32(0.1) is the number 0.1 and not
// 0.100000001490116. NaN and the infinities are refused (dal.ErrNotSupported): MySQL's
// DOUBLE, Oracle's NUMBER (as go-ora sends a float) and SQL Server's FLOAT hold none of
// them, and the driver or the server would fail the statement, or worse, read it as
// another number. The refusal names no value.
func typedFiniteFloat64(v reflect.Value) (float64, error) {
	f := v.Float()
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, typedUnsupported("a NaN or infinite float constant: the engine has no such number")
	}
	if v.Type().Bits() == 32 {
		f, _ = strconv.ParseFloat(strconv.FormatFloat(f, 'g', -1, 32), 64)
	}
	return f, nil
}

func (mysqlDialect) bind(value any) (string, any, error) {
	kind, err := typedKindOf(value)
	if err != nil {
		return "", nil, err
	}
	v := reflect.ValueOf(value)
	switch kind {
	case typedValueInteger:
		return "?", v.Int(), nil
	case typedValueUnsigned:
		return "?", v.Uint(), nil
	case typedValueFloat:
		f, err := typedFiniteFloat64(v)
		return "?", f, err
	case typedValueBool:
		return "?", v.Bool(), nil
	case typedValueBytes:
		return "?", v.Bytes(), nil
	case typedValueText:
		return "?", v.String(), nil
	}
	// A NULL, a time.Time and a driver.Valuer go to the driver as they are.
	return "?", value, nil
}

func (mysqlDialect) limitOffset(limit, offset int) (string, []any) {
	switch {
	case limit > 0 && offset > 0:
		return "LIMIT ? OFFSET ?", []any{int64(limit), int64(offset)}
	case limit > 0:
		return "LIMIT ?", []any{int64(limit)}
	case offset > 0:
		return "LIMIT " + mysqlNoOffsetLimit + " OFFSET ?", []any{int64(offset)}
	}
	return "", nil
}

// orderItem writes no NULLS clause: MySQL has none, and its order of NULLs is DALgo's.
func (mysqlDialect) orderItem(expression string, descending, _ bool) string {
	if descending {
		return expression + " DESC"
	}
	return expression + " ASC"
}

func (mysqlDialect) divide(left, right string) string {
	return "(CAST((" + left + ") AS DOUBLE) / NULLIF(CAST((" + right + ") AS DOUBLE), 0))"
}

func (mysqlDialect) arithmeticOperand(operand string) string {
	return "(CAST((" + operand + ") AS DOUBLE))"
}

// aggregateResult leaves the call as it is: SUM and AVG already run on DOUBLE, as
// aggregateArgument casts their argument.
func (mysqlDialect) aggregateResult(_, aggregate string) string { return aggregate }

func (mysqlDialect) emptyIn(negated bool) string {
	if negated {
		return "TRUE"
	}
	return "FALSE"
}

// capabilities: what PostgreSQL runs, MySQL runs too.
func (mysqlDialect) capabilities() dal.QueryCapabilities {
	return postgresDialect{}.capabilities()
}

func (mysqlDialect) window(string, []string, []string, []string) (string, error) {
	return "", typedUnsupported("window functions are reserved and not implemented")
}

func (mysqlDialect) sourceAliasKeyword() string { return " AS " }
func (mysqlDialect) groupByPosition() bool      { return true }
func (mysqlDialect) qualifiesBareFields() bool  { return true }
func (mysqlDialect) pagingOrderBy() string      { return "" }

// aggregateArgument casts the argument of SUM and AVG to DOUBLE. MySQL computes AVG of
// an exact argument (INT, DECIMAL) as a DECIMAL rounded to the argument's scale plus
// div_precision_increment (4), so the average of 1, 1 and 2 is 1.3333, where DALgo's
// generic engine computes 1.3333333333333333 in float64; a cast of the result comes
// too late.
func (mysqlDialect) aggregateArgument(function, argument string) string {
	return sqlserverCastArgument(function, argument, "DOUBLE")
}

// mysqlCatalogQuery is the one query catalogFacts sends, for n sources. A derived
// table of the source numbers 1 to n (integer literals, so no collation meets the
// catalog's) is joined to the catalog, source k on its own (schema, name), bound
// once each: two spellings of one relation (Album and dalgo.Album, or Album and album
// under lower_case_table_names=1) each get every column. The schema is NULL for an
// unqualified source, which COALESCE reads as DATABASE(). The name is compared with information_schema's own
// rule, which is the server's rule for table names (lower_case_table_names). The
// columns are: the source number, the column name, its DATA_TYPE, whether it is NOT
// NULL, whether it is a column of the PRIMARY KEY (KEY_COLUMN_USAGE of the constraint
// named PRIMARY, not COLUMN_KEY, which shows PRI for a unique key when there is no
// primary key), its collation's ID (0 for none) and whether that collation compares
// bytes (a _bin collation or none). Its text depends on n only.
func mysqlCatalogQuery(n int) string {
	var numbers, on strings.Builder
	for i := 1; i <= n; i++ {
		k := strconv.Itoa(i)
		if i > 1 {
			numbers.WriteString(" UNION ALL ")
			on.WriteString(" OR ")
		}
		numbers.WriteString("SELECT " + k + " AS k")
		on.WriteString("(q.k = " + k + " AND c.TABLE_SCHEMA = COALESCE(?, DATABASE()) AND c.TABLE_NAME = ?)")
	}
	return "SELECT q.k, " +
		"c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE = 'NO', " +
		"EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE u WHERE u.TABLE_SCHEMA = c.TABLE_SCHEMA AND u.TABLE_NAME = c.TABLE_NAME " +
		"AND u.COLUMN_NAME = c.COLUMN_NAME AND u.CONSTRAINT_NAME = 'PRIMARY'), " +
		"COALESCE((SELECT co.ID FROM information_schema.COLLATIONS co WHERE co.COLLATION_NAME = c.COLLATION_NAME), 0), " +
		"(c.COLLATION_NAME IS NULL OR RIGHT(c.COLLATION_NAME, 4) = '_bin') " +
		"FROM (" + numbers.String() + ") q JOIN information_schema.COLUMNS c ON " + on.String() + " " +
		"ORDER BY q.k, c.ORDINAL_POSITION"
}

// mysqlSuggestionQuery lists the names suggestSource chooses from: the tables and
// views of DATABASE() when the query named no schema (? is NULL), else of the schema
// that has the name the query wrote in any case.
const mysqlSuggestionQuery = `SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES ` +
	`WHERE (? IS NULL AND TABLE_SCHEMA = DATABASE()) OR LOWER(TABLE_SCHEMA) = LOWER(?) ` +
	`ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT 5000`

// mysqlSchemaArg is the schema argument of the catalog queries: NULL for none.
func mysqlSchemaArg(schema string) any {
	if schema == "" {
		return nil
	}
	return schema
}

func (d mysqlDialect) catalogFacts(ctx context.Context, execute executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error) {
	return typedCatalogFactsByCase(ctx, execute, sources, d.quoteIdent, mysqlCatalogQuery, typedSchemaNameArgs(mysqlSchemaArg), mysqlColumnFact)
}

// mysqlColumnFact builds a column's facts from one row of mysqlCatalogQuery.
func mysqlColumnFact(row typedCatalogRow) typedColumnFact {
	category, noJoinKey := mysqlTypeCategory(row.dataType)
	return typedColumnFact{
		Name:                      row.column,
		DataType:                  row.dataType,
		Category:                  category,
		NotNull:                   row.notNull,
		PrimaryKey:                row.primaryKey,
		Collation:                 row.collation,
		NonDeterministicCollation: !row.bytewise,
		NoJoinKey:                 noJoinKey,
	}
}

// mysqlTypeCategory maps an information_schema DATA_TYPE to the compiler's category,
// and says whether the type must not key a join (JSON compares by its own rules, the
// spatial types by geometry).
func mysqlTypeCategory(dataType string) (typedTypeCategory, bool) {
	switch strings.ToLower(dataType) {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return typedTypeText, false
	case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double", "year":
		return typedTypeNumber, false
	case "date", "datetime", "timestamp":
		return typedTypeTime, false
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return typedTypeBinary, false
	case "json", "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geomcollection", "geometrycollection":
		return typedTypeOther, true
	}
	return typedTypeOther, false // time, bit, enum, set: each compares to its own type
}

func (d mysqlDialect) suggestSource(ctx context.Context, execute executeQueryFunc, source typedSourceName) (typedSourceName, bool, error) {
	schema := mysqlSchemaArg(source.Schema)
	return typedSuggestSource(ctx, execute, source, mysqlSuggestionQuery, schema, schema)
}
