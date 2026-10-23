package dalgo2sql

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/dal-go/dalgo/dal"
)

// sqlserverMaxIdentifierUnits is the length limit of a SQL Server name (sysname,
// nvarchar(128)), in UTF-16 code units: a character outside the Basic Multilingual
// Plane counts two.
const sqlserverMaxIdentifierUnits = 128

// sqlserverDialect is the typedDialect for SQL Server 2022, verified against SQL
// Server 2022 (Developer edition) through github.com/microsoft/go-mssqldb. It writes
// SQL for compileTypedSQL; nothing here reads a value into the text.
//
// Identifiers. Every name is written exactly as the query spells it, inside double
// quotes with an inner quote doubled, which SQL Server reads as a delimited identifier
// while QUOTED_IDENTIFIER is ON: go-mssqldb logs in with the ODBC flag, which turns it
// on for the session. A name over 128 UTF-16 code units is refused. Whether Album and
// album are one name is the database collation's business (the default,
// SQL_Latin1_General_CP1_CI_AS, ignores case); every field is written qualified with
// its source (typedEngineRules.qualifiesBareFields), so no select alias can capture a
// name whatever the collation. The catalog facts match a column by its exact name.
//
// Constants. Every constant is one ? marker, numbered @p1, @p2, ... and typed by the
// driver from its Go type: a signed integer as int64, a float as float64 (a float32 as
// the float64 nearest its shortest decimal text), a bool as bool (a bit), a time.Time,
// []byte and string as they are (go-mssqldb sends a string as nvarchar). An unsigned
// integer is sent as its decimal text and cast to DECIMAL(20, 0), because the driver
// refuses a uint64 past the range of bigint. SQL Server's float has no NaN or
// infinity, so those are refused. A statement binds at most 2100 values; past that
// the server refuses it.
//
// Statements. OFFSET n ROWS and FETCH NEXT m ROWS ONLY are bound; a limit alone is
// OFFSET 0 ROWS, and a statement that limits or skips rows without an ORDER BY writes
// ORDER BY (SELECT NULL), which SQL Server requires. SQL Server sorts NULL first
// ascending and last descending, as DALgo does, so ORDER BY writes no NULLS clause.
// Arithmetic is on FLOAT: +, - and * cast both operands, division casts both and reads
// a zero divisor as NULL, and SUM and AVG cast their argument (AVG of an int is an int
// in SQL Server). An empty IN list is (1 = 0), an empty NOT IN (1 = 1). GROUP BY has no
// select-list position in SQL Server, so a grouped expression that carries a constant
// is refused.
//
// Catalog facts come from sys.columns for the relation OBJECT_ID resolves from the
// very name the statement writes (in brackets), so an unqualified name is looked up
// as the statement's is: the user's default schema, then dbo. A table or a view is
// listed; a synonym and a temporary table are not, so a source that is one is not
// found. A column of the primary key is one of the key columns of the index that is
// the primary key. Run it on the connection the statement runs on (the database pins
// one: readExecutor).
type sqlserverDialect struct{}

var (
	_ typedDialect     = sqlserverDialect{}
	_ typedEngineRules = sqlserverDialect{}
)

func (sqlserverDialect) placeholderStyle() typedPlaceholderStyle {
	return typedPlaceholderStyle{Prefix: "@p", IdentQuote: '"'}
}

func (sqlserverDialect) quoteIdent(name string) (string, error) {
	if err := checkTypedIdentifier(name, len(name)); err != nil {
		return "", err
	}
	if units := len(utf16.Encode([]rune(name))); units > sqlserverMaxIdentifierUnits {
		return "", &typedIdentifierTooLongError{length: units, limit: sqlserverMaxIdentifierUnits, characters: true}
	}
	return quoteTypedIdentifier(name, '"'), nil
}

func (sqlserverDialect) bind(value any) (string, any, error) {
	kind, err := typedKindOf(value)
	if err != nil {
		return "", nil, err
	}
	v := reflect.ValueOf(value)
	switch kind {
	case typedValueInteger:
		return "?", v.Int(), nil
	case typedValueUnsigned:
		return "CAST(? AS DECIMAL(20, 0))", strconv.FormatUint(v.Uint(), 10), nil
	case typedValueFloat:
		return "?", typedFloat64(v), nil
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

func (sqlserverDialect) limitOffset(limit, offset int) (string, []any) {
	switch {
	case limit > 0:
		return "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", []any{int64(offset), int64(limit)}
	case offset > 0:
		return "OFFSET ? ROWS", []any{int64(offset)}
	}
	return "", nil
}

// orderItem writes no NULLS clause: SQL Server has none, and its order of NULLs is
// DALgo's.
func (sqlserverDialect) orderItem(expression string, descending, notNull bool) string {
	return mysqlDialect{}.orderItem(expression, descending, notNull)
}

func (sqlserverDialect) divide(left, right string) string {
	return "(CAST((" + left + ") AS FLOAT) / NULLIF(CAST((" + right + ") AS FLOAT), 0))"
}

func (sqlserverDialect) arithmeticOperand(operand string) string {
	return "(CAST((" + operand + ") AS FLOAT))"
}

// aggregateResult leaves the call as it is: SUM and AVG already run on FLOAT, as
// aggregateArgument casts their argument.
func (sqlserverDialect) aggregateResult(_, aggregate string) string { return aggregate }

func (sqlserverDialect) emptyIn(negated bool) string {
	return oracleDialect{}.emptyIn(negated)
}

// capabilities: what PostgreSQL runs, SQL Server runs too; a grouping it cannot write
// is refused by the compiler (groupByPosition).
func (sqlserverDialect) capabilities() dal.QueryCapabilities {
	return postgresDialect{}.capabilities()
}

func (sqlserverDialect) window(string, []string, []string, []string) (string, error) {
	return "", typedUnsupported("window functions are reserved and not implemented")
}

func (sqlserverDialect) sourceAliasKeyword() string { return " AS " }
func (sqlserverDialect) groupByPosition() bool      { return false }
func (sqlserverDialect) qualifiesBareFields() bool  { return true }
func (sqlserverDialect) pagingOrderBy() string      { return "(SELECT NULL)" }

// aggregateArgument casts the argument of SUM and AVG to FLOAT: SQL Server's AVG of an
// int is an int (the average of 1 and 2 is 1) and its SUM of an int fails past the int
// range, where DALgo's generic engine computes in float64.
func (sqlserverDialect) aggregateArgument(function, argument string) string {
	if function == dal.SUM || function == dal.AVERAGE {
		return "CAST((" + argument + ") AS FLOAT)"
	}
	return argument
}

// sqlserverCatalogQuery is the one query catalogFacts sends, for n sources whose
// names, in brackets as OBJECT_ID reads them, are bound as @p1 to @pn. The columns
// are those typedCatalogFactsByCase reads: the source number, the column name, the
// base type of the column, whether it is NOT NULL, whether it is a key column of the
// primary key, the CollationId of its collation (0 for none) and whether that
// collation compares bytes (a BIN or BIN2 collation, or none). Its text depends on n
// only.
func sqlserverCatalogQuery(n int) string {
	var values strings.Builder
	for i := 1; i <= n; i++ {
		if i > 1 {
			values.WriteString(", ")
		}
		values.WriteString("(" + strconv.Itoa(i) + ", @p" + strconv.Itoa(i) + ")")
	}
	return "SELECT s.k, c.name, TYPE_NAME(c.system_type_id), " +
		"CASE WHEN c.is_nullable = 0 THEN 1 ELSE 0 END, " +
		"CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
		"WHERE i.object_id = c.object_id AND i.is_primary_key = 1 AND ic.column_id = c.column_id AND ic.is_included_column = 0) THEN 1 ELSE 0 END, " +
		"COALESCE(CONVERT(BIGINT, COLLATIONPROPERTY(c.collation_name, 'CollationId')), 0), " +
		"CASE WHEN c.collation_name IS NULL OR c.collation_name LIKE '%[_]BIN' OR c.collation_name LIKE '%[_]BIN2' THEN 1 ELSE 0 END " +
		"FROM (VALUES " + values.String() + ") AS s(k, name) " +
		"JOIN sys.objects o ON o.object_id = OBJECT_ID(s.name) AND o.type IN ('U', 'V') " +
		"JOIN sys.columns c ON c.object_id = o.object_id " +
		"ORDER BY s.k, c.column_id"
}

// sqlserverBracketed is a name as OBJECT_ID reads it: in brackets, with an inner ]
// doubled.
func sqlserverBracketed(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// sqlserverRelationArgs binds each source as the bracketed text OBJECT_ID resolves:
// [schema].[name], or [name] for an unqualified source.
func sqlserverRelationArgs(sources []typedSourceName) []any {
	args := make([]any, len(sources))
	for i, source := range sources {
		text := sqlserverBracketed(source.Name)
		if source.Schema != "" {
			text = sqlserverBracketed(source.Schema) + "." + text
		}
		args[i] = text
	}
	return args
}

// sqlserverSuggestionQuery lists the names suggestSource chooses from: the tables and
// views of the user's default schema and of dbo when the query named no schema (@p1
// is empty), else of the schema that has the name the query wrote in any case (@p2).
const sqlserverSuggestionQuery = `SELECT TOP (5000) s.name, o.name FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id ` +
	`WHERE o.type IN ('U', 'V') AND ((@p1 = '' AND s.name IN (SCHEMA_NAME(), 'dbo')) OR LOWER(s.name) = LOWER(@p2)) ` +
	`ORDER BY s.name, o.name`

func (d sqlserverDialect) catalogFacts(ctx context.Context, execute executeQueryFunc, sources []typedSourceName) (typedCatalogFacts, error) {
	return typedCatalogFactsByCase(ctx, execute, sources, d.quoteIdent, sqlserverCatalogQuery, sqlserverRelationArgs, sqlserverColumnFact)
}

// sqlserverColumnFact builds a column's facts from one row of sqlserverCatalogQuery.
func sqlserverColumnFact(row typedCatalogRow) typedColumnFact {
	category, noJoinKey := sqlserverTypeCategory(row.dataType)
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

// sqlserverTypeCategory maps a system type name to the compiler's category, and says
// whether the type must not key a join: text, ntext, image, xml and the spatial types
// cannot be compared with =.
func sqlserverTypeCategory(dataType string) (typedTypeCategory, bool) {
	switch strings.ToLower(dataType) {
	case "char", "varchar", "nchar", "nvarchar":
		return typedTypeText, false
	case "tinyint", "smallint", "int", "bigint", "decimal", "numeric", "float", "real", "money", "smallmoney":
		return typedTypeNumber, false
	case "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset":
		return typedTypeTime, false
	case "binary", "varbinary":
		return typedTypeBinary, false
	case "bit":
		return typedTypeBoolean, false
	case "text", "ntext", "image", "xml", "geometry", "geography", "json", "vector":
		return typedTypeOther, true
	}
	return typedTypeOther, false // time, uniqueidentifier, sql_variant, hierarchyid, rowversion
}

func (d sqlserverDialect) suggestSource(ctx context.Context, execute executeQueryFunc, source typedSourceName) (typedSourceName, bool, error) {
	return typedSuggestSource(ctx, execute, source, sqlserverSuggestionQuery, source.Schema, source.Schema)
}
