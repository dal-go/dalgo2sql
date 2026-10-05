package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

var _ dal.RecordsReader = (*recordsReader)(nil)

func getRecordsReader(ctx context.Context, query dal.Query, execute executeQueryFunc) (rr *recordsReader, err error) {
	return getRecordsReaderWithOptions(ctx, query, execute, DbOptions{})
}

const recordIDHelperColumn = "__dalgo_record_id"

// getRecordsReaderWithOptions runs the query and returns a reader over its records. How
// each record is keyed:
//
//   - By the primary key of the recordset declared for the source in DbOptions.Recordsets,
//     or, when it declares none, DbOptions.PrimaryKey: the value of that column.
//   - With the typed PostgreSQL dialect and a source nobody declared, by the primary key the
//     catalog reports for it, when that is exactly one column. The catalog lookup is the
//     one the read makes anyway for its compiler.
//   - By ordinal, the position of the row in the result counted from 0 and written as
//     decimal text, for the rows of a grouped or aggregated query (which have no source
//     row) and, with the typed PostgreSQL dialect and a source nobody declared, for those of
//     a source with no primary key, a composite one, or a view, a materialized view or
//     a foreign table, which have none.
//   - In every other case (the SQLite dialect, the legacy emitter or a native compiler of
//     the caller's, with no key configured, and a recordset declared with no single-column
//     primary key) by the literal ID "__dalgo_record_id", the same for every record: those
//     paths have no catalog to ask. A read into a record the query names
//     (dal.StructuredQuery.IntoRecord) keeps that record's key unless a key is found by one of
//     the first two rules.
func getRecordsReaderWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions) (rr *recordsReader, err error) {
	rr = &recordsReader{
		fold:                recordNameFold(options),
		identityColumnIndex: -1,
		newRecord: func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Unknown", ""), make(map[string]any))
		},
	}
	// facts, when the read asked the catalog before compiling, are handed to the compiler
	// so that it does not ask again.
	var facts *typedCatalogFacts
	if q, ok := query.(dal.StructuredQuery); ok {
		if readsWithTypedPostgres(options) {
			// The typed compiler refuses what it cannot compute as asked, and reads that
			// from the query it is handed, which below can be a wrapper of this reader's
			// own that does not carry it. So the caller's query is asked first.
			if err = typedRefuseMoney(q); err != nil {
				err = fmt.Errorf("failed to get SQL reader: %w", err)
				return
			}
		}
		rr.validateFinite = dal.HasAggregation(q)
		if rec := q.IntoRecord(); rec != nil {
			rr.newRecord = func() dalrecord.Record { return q.IntoRecord() }
		} else if from := q.From(); from != nil && from.Base() != nil {
			collection := from.Base().Name()
			if rr.validateFinite {
				// Aggregate result rows do not have a source record identity. Give
				// each result row a deterministic synthetic key without exposing a
				// helper column in its data.
				rr.newRecord = ordinalRecords(collection)
			} else {
				rr.newRecord = func() dalrecord.Record {
					return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, recordIDHelperColumn), make(map[string]any))
				}
			}
		}
		primaryKey := primaryKeyForQuery(options, query)
		if primaryKey == "" && keyedByCatalog(options, q) {
			// Nothing names the key of this source, so the catalog does: the primary key,
			// when it is one column, else the rows are keyed by ordinal.
			var column string
			if facts, column, err = catalogPrimaryKey(ctx, options, q, execute); err != nil {
				err = fmt.Errorf("failed to get SQL reader: %w", err)
				return
			}
			if primaryKey = column; primaryKey == "" {
				rr.newRecord = ordinalRecords(q.From().Base().Name())
			}
		}
		if primaryKey != "" && !rr.validateFinite {
			if isKeysOnlyQuery(q) && len(q.OrderBy()) == 0 && len(q.From().Joins()) == 0 && canOrderByKey(options, primaryKey) {
				// A keys-only query names no order, and SQL returns rows in no
				// defined order without one, so callers would see a different
				// order from one run or database to the next. Order by the key.
				// A JOIN query keeps its statement: the key is unqualified, which
				// the SQLite compiler refuses for a JOIN and which is ambiguous
				// when both tables have a column of that name.
				q = orderedByKey{StructuredQuery: q, key: primaryKey}
				query = q
			}
			rr.identityColumn = primaryKey
			// A join read by a compiler of this package keys each record by the base
			// row, as DALgo's generic join does. Matching a result column by the key's
			// name cannot: any source can have a column of that name, and the select
			// list can name it. So the statement always carries the base's key column,
			// qualified, and the record is keyed from it by position. A select-all, which
			// no column can be added to, is keyed from the base's own column of that name
			// (see baseColumnIndex).
			keyedByBase := len(q.From().Joins()) != 0 && readsWithOwnCompiler(options)
			if selected := q.Columns(); len(selected) > 0 && (keyedByBase || !selectsIdentityField(selected, primaryKey, rr.fold)) {
				columns := append([]dal.Column(nil), selected...)
				helper := unusedHelperColumn(selected)
				columns = append(columns, dal.Column{Expression: recordIdentityField(options, q, primaryKey), Alias: helper})
				query = dal.WithColumns(q, columns)
				rr.identityColumn = helper
				rr.hideIdentityColumn = true
				rr.identityByPosition = true
			} else if keyedByBase && len(selected) == 0 {
				rr.identityByPosition = true
				rr.keyInBaseColumns = true
			}
		}
	}

	if rr.readerBase, err = getReaderBaseFor(ctx, query, execute, options, rr.keyInBaseColumns, facts); err != nil {
		err = fmt.Errorf("failed to get SQL reader: %w", err)
		return
	}
	if rr.hideIdentityColumn {
		rr.identityColumnIndex = len(rr.colNames) - 1
	} else if rr.keyInBaseColumns {
		rr.identityColumnIndex = baseColumnIndex(rr.baseColumns, rr.identityColumn, rr.fold)
	}

	return
}

// ordinalRecords makes the records of rows that have no key of their own, one per row,
// keyed by the position of the row in the result, from 0, written as decimal text.
func ordinalRecords(collection string) func() dalrecord.Record {
	ordinal := 0
	return func() dalrecord.Record {
		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, strconv.Itoa(ordinal)), make(map[string]any))
		ordinal++
		return record
	}
}

// keyedByCatalog reports whether the key of the records of q is for the catalog to say:
// the typed PostgreSQL compiler runs the read, which asks the catalog anyway, and nothing
// else names the key (no recordset declared for the source, and no DbOptions.PrimaryKey).
// A grouped or aggregated query has no source row to key, a read into a record keeps that
// record's own key, and a query with no base source has no source to ask about.
func keyedByCatalog(options DbOptions, q dal.StructuredQuery) bool {
	return readsWithTypedPostgres(options) &&
		!dal.HasAggregation(q) && q.IntoRecord() == nil &&
		q.From() != nil && q.From().Base() != nil &&
		len(options.PrimaryKey) == 0 && recordsetForQuery(options, q) == nil
}

// catalogPrimaryKey reads the catalog facts of the sources of q, which the compiler needs
// next and is handed back to reuse, and names the column that is the whole of the primary
// key of the base source, as the catalog stores it. The column is "" when the source has
// none or several, and when the mount cannot write its name (a column stored as Id where the
// fold-lower mount writes id: the statement could not select it).
func catalogPrimaryKey(ctx context.Context, options DbOptions, q dal.StructuredQuery, execute executeQueryFunc) (*typedCatalogFacts, string, error) {
	dialect, err := postgresDialectFor(options)
	if err != nil {
		return nil, "", err
	}
	facts, err := typedFactsForQuery(ctx, dialect, execute, q.From())
	if err != nil {
		return nil, "", err
	}
	base, err := typedCollection(q.From().Base())
	if err != nil {
		return &facts, "", nil // a source the compiler refuses: it reports it
	}
	source, _ := facts.source(typedSourceName{Schema: base.Schema(), Name: base.Name()})
	if column, ok := source.primaryKeyColumn(); ok && facts.addressable(column.Name) {
		return &facts, column.Name, nil
	}
	return &facts, "", nil
}

// baseColumnIndex is the position, in a select-all over joins, of the base source's
// column called name (under the fold), or -1 when the base has none. The base's columns
// lead the result, so a position among them is a position in the result; a column of a
// joined source that carries the name is never taken for it.
func baseColumnIndex(baseColumns []string, name string, fold nameFold) int {
	for i, column := range baseColumns {
		if fold.of(column) == fold.of(name) {
			return i
		}
	}
	return -1
}

// readsWithOwnCompiler reports whether a structured read is compiled by one of this
// package's compilers, the typed PostgreSQL one or the SQLite one, rather than by a native
// compiler of the caller's or the legacy emitter: those take the query as it always did.
func readsWithOwnCompiler(options DbOptions) bool {
	return options.NativeStructuredQueryCompiler == nil && (options.StructuredQueryDialect == "postgres" || options.StructuredQueryDialect == "sqlite")
}

// readsWithTypedPostgres reports whether a structured read is compiled by the typed
// PostgreSQL compiler: a native compiler of the caller's takes precedence over the
// dialect (see getReaderBaseWithOptions).
func readsWithTypedPostgres(options DbOptions) bool {
	return options.NativeStructuredQueryCompiler == nil && options.StructuredQueryDialect == "postgres"
}

// recordIdentityField is the field the reader adds to the select list to key each
// record by the primary key. It is unqualified, as it always was, except for a JOIN
// read by a compiler of this package, the typed PostgreSQL one or the SQLite one: both
// refuse an unqualified field in a JOIN, so the field names the base source, the table
// the key belongs to. Without that, a join CanExecuteJoin accepted would fail once DALgo
// has chosen the native plan, which it does not retry. A native compiler of the
// caller's keeps the unqualified field it was given.
func recordIdentityField(options DbOptions, q dal.StructuredQuery, primaryKey string) dal.FieldRef {
	if readsWithOwnCompiler(options) && len(q.From().Joins()) != 0 {
		return dal.NewFieldRef(typedIdentity(q.From().Base()), primaryKey)
	}
	return dal.Field(primaryKey)
}

// nameFold is the rule a read applies to the names it compares, which is the rule its
// statement was written with: the fold of the PostgreSQL dialect on a fold-lower
// mount. The zero value compares names as they are.
type nameFold func(string) string

// of folds name.
func (f nameFold) of(name string) string {
	if f == nil {
		return name
	}
	return f(name)
}

// recordNameFold is the fold of the read's statement: the typed PostgreSQL compiler's,
// when it folds names, else none. A configured primary key is spelt as the caller
// chose, and the statement returns the column under the name it wrote, so the two are
// compared folded or the key is never found.
func recordNameFold(options DbOptions) nameFold {
	if !readsWithTypedPostgres(options) {
		return nil
	}
	// An IdentifierCase the package does not define fails the read itself, before any
	// name is compared.
	dialect, _ := postgresDialectFor(options)
	if dialect.mode != postgresFoldLower {
		return nil
	}
	return dialect.fold
}

// isKeysOnlyQuery reports whether q was built with SelectKeysOnly: it selects
// no columns, reads into no record and names a key kind.
func isKeysOnlyQuery(q dal.StructuredQuery) bool {
	return len(q.Columns()) == 0 && q.IDKind() != reflect.Invalid && q.IntoRecord() == nil
}

// canOrderByKey reports whether the statement for the configured emitter can
// carry an ORDER BY on the key. The legacy text emitter refuses names that are
// not plain identifiers, so for those keys it keeps the unordered statement it
// always produced; the SQLite compiler and native compilers quote the name.
func canOrderByKey(options DbOptions, primaryKey string) bool {
	legacy := options.NativeStructuredQueryCompiler == nil && options.StructuredQueryDialect == ""
	return !legacy || isPlainSQLIdentifier(primaryKey)
}

// orderedByKey is a query that sorts ascending by one field, the primary key.
type orderedByKey struct {
	dal.StructuredQuery
	key string
}

func (o orderedByKey) OrderBy() []dal.OrderExpression {
	return []dal.OrderExpression{dal.AscendingField(o.key)}
}

// String renders the wrapper itself, order included, like dalgo's own query
// wrappers; the embedded query would render without the order.
func (o orderedByKey) String() string { return dal.QueryString(o) }

type recordsReader struct {
	readerBase
	newRecord           func() dalrecord.Record
	identityColumn      string
	identityColumnIndex int
	hideIdentityColumn  bool
	// identityByPosition says the record's key is read from the column at
	// identityColumnIndex, not from the column called identityColumn: set for the helper
	// column the reader appends, and for a select-all over joins.
	identityByPosition bool
	// keyInBaseColumns says the statement is a select-all over joins, keyed from the base
	// source's own column of the key's name (baseColumnIndex).
	keyInBaseColumns bool
	validateFinite   bool
	// fold is applied to a column name and to the key's name before they are compared.
	fold nameFold
}

// decimalText matches the text PostgreSQL prints for a finite NUMERIC: an
// optional sign and decimal digits with an optional fraction, no exponent.
var decimalText = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$`)

// normalizeValueByDatabaseType turns the text a PostgreSQL driver delivers for
// a NUMERIC column into a float64. pgx reports float64 as the scan type of
// NUMERIC but delivers a string.
//
//   - NUMERIC delivered as string or []byte becomes float64 when the text is
//     decimal text, or exactly NaN, Infinity or -Infinity (PostgreSQL's
//     spellings). Any other text, and decimal text outside the float64 range,
//     is kept as a string.
//   - Every other type name (DECIMAL, which is MySQL's, included), every other
//     value and nil are returned unchanged. JSON and JSONB need no handling here:
//     the records reader already stores []byte as string, and a string-typed
//     recordset column converts []byte.
//
// Callers choose where this applies: the recordset reader calls it only for a
// float64-typed column; text that is not a number comes back as a string and
// the reader returns an error for it. With lib/pq the recordset reader keeps
// NUMERIC as []byte (the column is typed []byte from the interface scan type),
// so only the records reader converts.
//
// A float64 holds about 15 significant digits exactly. Longer NUMERIC values,
// such as a NUMERIC(20,0) key, are rounded, so distinct values can compare
// equal. A NUMERIC NaN or infinity in a plain SELECT becomes a non-finite
// float64, which encoding/json refuses to marshal; aggregate queries reject it.
func normalizeValueByDatabaseType(databaseTypeName string, value any) any {
	if !strings.EqualFold(databaseTypeName, "NUMERIC") {
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
	switch text {
	case "NaN", "Infinity", "-Infinity":
	default:
		if !decimalText.MatchString(text) {
			return text
		}
	}
	if number, err := strconv.ParseFloat(text, 64); err == nil {
		return number
	}
	return text
}

// selectsIdentityField reports whether the select list returns the column name, under
// the names fold gives (nil: as they are). A wildcard returns it unless an exclusion
// names it, and the compiler folds the exclusions it applies, so they are folded here.
func selectsIdentityField(columns []dal.Column, name string, fold nameFold) bool {
	name = fold.of(name)
	for _, column := range columns {
		if column.Wildcard != nil {
			projection := *column.Wildcard
			projection.Exclude = make([]string, len(column.Wildcard.Exclude))
			for i, exclusion := range column.Wildcard.Exclude {
				projection.Exclude[i] = fold.of(exclusion)
			}
			if !projection.Excludes(name) {
				return true
			}
		}
		if field, ok := column.Expression.(dal.FieldRef); ok && fold.of(field.Name()) == name && (column.Alias == "" || fold.of(column.Alias) == name) {
			return true
		}
	}
	return false
}

func unusedHelperColumn(columns []dal.Column) string {
	for suffix := 0; ; suffix++ {
		name := recordIDHelperColumn
		if suffix > 0 {
			name = fmt.Sprintf("%s_%d", name, suffix)
		}
		used := false
		for _, column := range columns {
			resultName := column.Alias
			if resultName == "" {
				if field, ok := column.Expression.(dal.FieldRef); ok {
					resultName = field.Name()
				}
			}
			if resultName == name {
				used = true
				break
			}
		}
		if !used {
			return name
		}
	}
}

func (r recordsReader) Next() (record dalrecord.Record, err error) {
	if !r.rows.Next() {
		r.lease.release() // the rows closed themselves at the end: the connection goes back
		if err := r.rows.Err(); err != nil {
			return nil, streamError(r.lease, err)
		}
		return nil, dal.ErrNoMoreRecords
	}
	record = r.newRecord()
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		data = make(map[string]any)
		record = dalrecord.NewRecordWithData(record.Key(), data)
	}
	var set func(column string, raw, normalized any) error
	if d, isMap := data.(map[string]any); isMap {
		set = func(column string, _, normalized any) error {
			// database/sql returns []byte for TEXT/VARCHAR columns with some
			// drivers (notably go-sql-driver/mysql); store as string so the
			// map is usable and JSON-serializes as text, not base64. Matches
			// scanRowIntoMap on the Get path.
			d[column] = textValue(normalized)
			return nil
		}
	} else if structSet, isStruct := structSetter(data); isStruct {
		// A struct target takes the same normalised values as a map, and the
		// driver's own value where the field type needs it (see
		// assignColumnValue).
		set = structSet
	} else {
		// TODO: implement Scan into `[]any`
		return nil, fmt.Errorf("unsupported data type %T", data)
	}
	var values []any
	if values, err = r.scanValues(); err != nil {
		return nil, err
	}
	for i, n := range r.colNames {
		normalized := normalizeValueByDatabaseType(r.colTypes[i].DatabaseTypeName(), values[i])
		if r.validateFinite {
			if number, ok := normalized.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0)) {
				return nil, fmt.Errorf("non-finite aggregate result in column %q", n)
			}
		}
		v := textValue(normalized)
		identityValue := r.fold.of(n) == r.fold.of(r.identityColumn)
		if r.identityByPosition {
			identityValue = i == r.identityColumnIndex
		}
		if identityValue {
			record.Key().ID = v
			if v != nil {
				record.Key().IDKind = reflect.TypeOf(v).Kind()
			}
		}
		if !r.hideIdentityColumn || i != r.identityColumnIndex {
			if err = set(n, values[i], normalized); err != nil {
				return nil, err
			}
		}
	}
	return
}

func (r recordsReader) Cursor() (string, error) {
	return "", dal.ErrNotSupported
}

func (r recordsReader) Close() error {
	err := r.rows.Close()
	r.lease.release()
	return err
}

// recordsReaderProvider is embedded into database and transaction
type recordsReaderProvider struct {
	executeQuery func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (rrp recordsReaderProvider) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return getRecordsReader(ctx, query, rrp.executeQuery)
}

//func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]record.Record, error) {
//	r, err := rrp.ExecuteQueryToRecordsReader(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//	return dal.ReadAllToRecords(ctx, r, options...)
//}
