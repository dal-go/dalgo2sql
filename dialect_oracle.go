package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// oracleMaxIdentifierBytes is the length limit of an Oracle name from 12.2 on, in
// bytes of the database character set; the dialect counts UTF-8 bytes, which is the
// server's count in an AL32UTF8 database, the default since 12.2.
const oracleMaxIdentifierBytes = 128

// oracleDialect is the typedDialect for Oracle Database 23ai (23.4 and later), verified
// against Oracle Database Free 23. It writes SQL for compileTypedSQL; nothing here
// reads a value into the text.
//
// Identifiers. Every name is written exactly as the query spells it, inside double
// quotes, so Album and ALBUM are two tables: a table created without quotes is stored
// in upper case, and the query must spell it so (a not-found error suggests the name
// that differs only in case). Oracle has no escape for a double quote inside a quoted
// name, so quoteIdent refuses a name that holds one, as it refuses a name over 128
// bytes. Every field is written qualified with its source
// (typedEngineRules.qualifiesBareFields): Oracle 23ai reads a select alias in GROUP
// BY and HAVING, and a qualified name is always the column. A table alias is written
// without AS, which Oracle refuses there.
//
// Constants. Every constant is one ? marker, numbered :1, :2, ... and typed by the
// driver (go-ora) from its Go type: a signed integer as int64, an unsigned one as
// uint64, a float as float64 (a float32 as the float64 nearest its shortest decimal
// text), which go-ora sends as a NUMBER, so NaN and the infinities are refused
// (dal.ErrNotSupported); a bool as bool (a BOOLEAN, which Oracle has from 23ai); a
// time.Time, []byte and string as they are. Oracle reads the empty string (and empty
// RAW) as NULL, so a comparison with "" is never true, nor its negation, where DALgo's
// generic engine finds the empty strings: an empty string or byte constant is refused.
// A NUMBER result, which go-ora delivers as text, is read back as an int64 when its
// scale is 0 or unconstrained and the value is whole, else as a float64
// (oracleNumberValue).
//
// Statements. OFFSET n ROWS and FETCH NEXT m ROWS ONLY are bound. Oracle sorts NULL
// last ascending, so ORDER BY writes NULLS FIRST ascending and NULLS LAST descending,
// as DALgo sorts, unless the catalog says the column is NOT NULL. Arithmetic is on
// BINARY_DOUBLE: +, - and * cast both operands, division casts both and reads a zero
// divisor as NULL, and SUM and AVG are cast to BINARY_DOUBLE. An empty IN list is
// (1 = 0), an empty NOT IN (1 = 1). GROUP BY has no select-list position in Oracle
// (unless group_by_position_enabled), so a grouped expression that carries a constant
// is refused (typedEngineRules.groupByPosition).
//
// Catalog facts come from ALL_TAB_COLUMNS, which lists the columns of the tables and
// views the session can read, one query for every source, for the schema the query
// names or, for an unqualified source, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'): the
// schema the session reads an unqualified name from. A synonym is not resolved, so a
// source that is one is not found. NOT NULL is the column's NULLABLE, unless a check
// constraint on the column is not validated (a NOT NULL constraint added NOVALIDATE
// leaves NULLs in place); a PRIMARY KEY column is one of the columns of the table's
// constraint of type P. Run it on the connection the statement runs on (the database
// pins one: readExecutor).
type oracleDialect struct{}

var (
	_ typedDialect     = oracleDialect{}
	_ typedEngineRules = oracleDialect{}
)

func (oracleDialect) placeholderStyle() typedPlaceholderStyle {
	return typedPlaceholderStyle{Prefix: ":", IdentQuote: '"'}
}

func (oracleDialect) quoteIdent(name string) (string, error) {
	if err := checkTypedIdentifier(name, oracleMaxIdentifierBytes); err != nil {
		return "", err
	}
	if strings.IndexByte(name, '"') >= 0 {
		return "", errors.New("identifier contains a double quote, which an Oracle name cannot hold")
	}
	return quoteTypedIdentifier(name, '"'), nil
}

func (oracleDialect) bind(value any) (string, any, error) {
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
	case typedValueBytes, typedValueText:
		// Oracle stores an empty VARCHAR2 and an empty RAW as NULL and compares the
		// empty string as NULL, so a comparison with "" is never true, nor is its
		// negation: Title != "" and Title NOT IN ("") would find no row where DALgo's
		// generic engine finds every title that is not empty. The constant is refused.
		if v.Len() == 0 {
			return "", nil, typedUnsupported("an empty string or byte constant, which Oracle reads as NULL")
		}
		if kind == typedValueText {
			return "?", v.String(), nil
		}
		return "?", v.Bytes(), nil
	}
	// A NULL, a time.Time and a driver.Valuer go to the driver as they are.
	return "?", value, nil
}

func (oracleDialect) limitOffset(limit, offset int) (string, []any) {
	switch {
	case limit > 0 && offset > 0:
		return "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", []any{int64(offset), int64(limit)}
	case limit > 0:
		return "FETCH FIRST ? ROWS ONLY", []any{int64(limit)}
	case offset > 0:
		return "OFFSET ? ROWS", []any{int64(offset)}
	}
	return "", nil
}

func (oracleDialect) orderItem(expression string, descending, notNull bool) string {
	return postgresDialect{}.orderItem(expression, descending, notNull)
}

func (oracleDialect) divide(left, right string) string {
	return "(CAST((" + left + ") AS BINARY_DOUBLE) / NULLIF(CAST((" + right + ") AS BINARY_DOUBLE), 0))"
}

func (oracleDialect) arithmeticOperand(operand string) string {
	return "(CAST((" + operand + ") AS BINARY_DOUBLE))"
}

// aggregateResult casts SUM and AVG, which Oracle returns as NUMBER, to the float64
// DALgo's generic engine returns.
func (oracleDialect) aggregateResult(function, aggregate string) string {
	if function == dal.SUM || function == dal.AVERAGE {
		return "(CAST(" + aggregate + " AS BINARY_DOUBLE))"
	}
	return aggregate
}

func (oracleDialect) emptyIn(negated bool) string {
	if negated {
		return "(1 = 1)"
	}
	return "(1 = 0)"
}

// capabilities: what PostgreSQL runs, Oracle runs too; a grouping it cannot write is
// refused by the compiler (groupByPosition).
func (oracleDialect) capabilities() dal.QueryCapabilities {
	return postgresDialect{}.capabilities()
}

func (oracleDialect) window(string, []string, []string, []string) (string, error) {
	return "", typedUnsupported("window functions are reserved and not implemented")
}

func (oracleDialect) sourceAliasKeyword() string { return " " }
func (oracleDialect) groupByPosition() bool      { return false }
func (oracleDialect) qualifiesBareFields() bool  { return true }
func (oracleDialect) pagingOrderBy() string      { return "" }

func (oracleDialect) aggregateArgument(_, argument string) string { return argument }

// oracleCurrentSchema is the schema an unqualified name is read from.
const oracleCurrentSchema = `SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')`

// oracleCatalogQuery is the one query catalogFacts sends, for n sources. A derived
// table of the source numbers 1 to n is joined to the catalog, source k on its own
// (schema, name), bound once each as :2k-1 and :2k, so two spellings of one relation
// (ALBUM and DALGO.ALBUM) each get every column. The schema is the empty string for an unqualified source, which
// Oracle reads as NULL and COALESCE as the current schema. The columns are those
// typedCatalogFactsByCase reads, each truth value a 0 or a 1 (no boolean expression
// stands in the select list). Its text depends on n only.
func oracleCatalogQuery(n int) string {
	var numbers, on strings.Builder
	for i := 1; i <= n; i++ {
		k := strconv.Itoa(i)
		if i > 1 {
			numbers.WriteString(" UNION ALL ")
			on.WriteString(" OR ")
		}
		numbers.WriteString("SELECT " + k + " AS k FROM DUAL")
		on.WriteString("(q.k = " + k + " AND c.OWNER = COALESCE(:" + strconv.Itoa(2*i-1) + ", " + oracleCurrentSchema + ") AND c.TABLE_NAME = :" + strconv.Itoa(2*i) + ")")
	}
	return "SELECT q.k, " +
		"c.COLUMN_NAME, c.DATA_TYPE, " +
		"CASE WHEN c.NULLABLE = 'N' AND NOT EXISTS (SELECT 1 FROM ALL_CONSTRAINTS n JOIN ALL_CONS_COLUMNS nc " +
		"ON nc.OWNER = n.OWNER AND nc.CONSTRAINT_NAME = n.CONSTRAINT_NAME " +
		"WHERE n.OWNER = c.OWNER AND n.TABLE_NAME = c.TABLE_NAME AND n.CONSTRAINT_TYPE = 'C' AND n.VALIDATED <> 'VALIDATED' " +
		"AND nc.TABLE_NAME = c.TABLE_NAME AND nc.COLUMN_NAME = c.COLUMN_NAME) THEN 1 ELSE 0 END, " +
		"CASE WHEN EXISTS (SELECT 1 FROM ALL_CONSTRAINTS p JOIN ALL_CONS_COLUMNS pc " +
		"ON pc.OWNER = p.OWNER AND pc.CONSTRAINT_NAME = p.CONSTRAINT_NAME " +
		"WHERE p.OWNER = c.OWNER AND p.TABLE_NAME = c.TABLE_NAME AND p.CONSTRAINT_TYPE = 'P' " +
		"AND pc.TABLE_NAME = c.TABLE_NAME AND pc.COLUMN_NAME = c.COLUMN_NAME) THEN 1 ELSE 0 END, " +
		"0, " +
		"CASE WHEN c.COLLATION IS NULL OR c.COLLATION IN ('BINARY', 'USING_NLS_COMP') THEN 1 ELSE 0 END " +
		"FROM (" + numbers.String() + ") q JOIN ALL_TAB_COLUMNS c ON " + on.String() + " " +
		"ORDER BY q.k, c.COLUMN_ID"
}

// oracleSuggestionQuery lists the names suggestSource chooses from: the tables and
// views of the current schema when the query named no schema (:1 is NULL), else of
// the schema that has the name the query wrote in any case (:2).
const oracleSuggestionQuery = `SELECT o.OWNER, o.OBJECT_NAME FROM ALL_OBJECTS o ` +
	`WHERE o.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW') ` +
	`AND ((:1 IS NULL AND o.OWNER = ` + oracleCurrentSchema + `) OR UPPER(o.OWNER) = UPPER(:2)) ` +
	`ORDER BY o.OWNER, o.OBJECT_NAME FETCH FIRST 5000 ROWS ONLY`

// oracleSchemaArg is the schema argument of the catalog queries: the empty string,
// which Oracle reads as NULL, for none.
func oracleSchemaArg(schema string) any { return schema }

func (d oracleDialect) catalogFacts(ctx context.Context, execute executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error) {
	return typedCatalogFactsByCase(ctx, execute, sources, d.quoteIdent, oracleCatalogQuery, typedSchemaNameArgs(oracleSchemaArg), oracleColumnFact)
}

// oracleColumnFact builds a column's facts from one row of oracleCatalogQuery.
func oracleColumnFact(row typedCatalogRow) typedColumnFact {
	category, noJoinKey := oracleTypeCategory(row.dataType)
	return typedColumnFact{
		Name:                      row.column,
		DataType:                  row.dataType,
		Category:                  category,
		NotNull:                   row.notNull,
		PrimaryKey:                row.primaryKey,
		NonDeterministicCollation: !row.bytewise,
		NoJoinKey:                 noJoinKey,
	}
}

// oracleTypeCategory maps an ALL_TAB_COLUMNS DATA_TYPE to the compiler's category,
// and says whether the type must not key a join: a LOB, a LONG, JSON, XMLType and a
// vector cannot be compared with =.
func oracleTypeCategory(dataType string) (typedTypeCategory, bool) {
	upper := strings.ToUpper(dataType)
	switch {
	case upper == "VARCHAR2" || upper == "NVARCHAR2" || upper == "CHAR" || upper == "NCHAR":
		return typedTypeText, false
	case upper == "NUMBER" || upper == "FLOAT" || upper == "BINARY_FLOAT" || upper == "BINARY_DOUBLE" || upper == "INTEGER":
		return typedTypeNumber, false
	case upper == "DATE" || (strings.HasPrefix(upper, "TIMESTAMP") && !strings.Contains(upper, "LOCAL")):
		return typedTypeTime, false
	case upper == "RAW":
		return typedTypeBinary, false
	case upper == "BOOLEAN":
		return typedTypeBoolean, false
	case upper == "CLOB" || upper == "NCLOB" || upper == "BLOB" || upper == "BFILE" || upper == "LONG" || upper == "LONG RAW" ||
		upper == "JSON" || upper == "XMLTYPE" || upper == "VECTOR":
		return typedTypeOther, true
	}
	return typedTypeOther, false // intervals, ROWID, LOCAL TIME ZONE timestamps, object types
}

func (d oracleDialect) suggestSource(ctx context.Context, execute executeQueryFunc, source typedSourceName) (typedSourceName, bool, error) {
	return typedSuggestSource(ctx, execute, source, oracleSuggestionQuery, source.Schema, source.Schema)
}

// oracleNumberValue turns the decimal text go-ora delivers for a NUMBER (it reads every
// NUMBER as text, and reports no database type name) into the Go number DALgo's generic
// engine would hold: an int64 when the column's scale is 0 or unconstrained (COUNT,
// an integer column, an expression over integers) and the text is a whole number that
// fits, else a float64, as PostgreSQL's NUMERIC is read. With exact (ExactNumericValues)
// a value that is not a whole number keeps its decimal text. A column is a NUMBER when
// the driver reports a precision and scale for it, which go-ora does for NUMBER only;
// every other value is returned as it is.
func oracleNumberValue(columnType *sql.ColumnType, value any, exact bool) any {
	if columnType == nil {
		return value
	}
	_, scale, isNumber := columnType.DecimalSize()
	if !isNumber {
		return value
	}
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return value
	}
	if scale == 0 || scale > 127 { // go-ora reports an unconstrained scale as 255
		if whole, err := strconv.ParseInt(text, 10, 64); err == nil {
			return whole
		}
	}
	if exact {
		return text
	}
	if number, err := strconv.ParseFloat(text, 64); err == nil {
		return number
	}
	return text
}
