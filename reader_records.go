package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

var _ dal.RecordsReader = (*recordsReader)(nil)

func getRecordsReader(ctx context.Context, query dal.Query, execute executeQueryFunc) (rr *recordsReader, err error) {
	return getRecordsReaderWithOptions(ctx, query, execute, DbOptions{})
}

const recordIDHelperColumn = "__dalgo_record_id"

// errColumnsChanged is the error of a read whose result does not hold the key's column where the
// catalog, asked in the statement before it, said the source has it: the columns of the source
// changed between the two. The catalog's answer is not used to key records then.
var errColumnsChanged = errors.New("the columns of the source changed during the read")

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
//     the caller's with no key configured, and a recordset declared with no single-column
//     primary key) by the literal ID "__dalgo_record_id", the same for every record: those
//     paths have no catalog to ask.
//
// A read into a record the query names (dal.StructuredQuery.IntoRecord) keeps that record's
// own key, except that a key configured by the first rule is written into it; the catalog is
// not asked for such a read's key.
func getRecordsReaderWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions) (rr *recordsReader, err error) {
	rr = &recordsReader{
		identityColumnIndex:  -1,
		dialect:              options.StructuredQueryDialect,
		exactNumericValues:   options.ExactNumericValues,
		preserveBinaryValues: options.PreserveBinaryValues,
		newRecord: func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Unknown", ""), make(map[string]any))
		},
	}
	fold := recordNameFold(options)
	// knownKeyIndex is the position of the column that keys the records, in the result, when
	// the read knows it before the statement runs: the catalog's key of a select-all, and the
	// first select item that is the key's own field. It is -1 when only a name says which column
	// is the key (see keyColumnIndex).
	knownKeyIndex := -1
	// keyFromCatalog says knownKeyIndex is the catalog's, which the result is checked against.
	keyFromCatalog := false
	// catalogKeyIndex is the position of the catalog's key among the columns of its source in
	// table order, which is the order a select-all returns them in; -1 when the key is not the
	// catalog's.
	catalogKeyIndex := -1
	// facts, when the read asked the catalog before compiling, are handed to the compiler
	// so that it does not ask again.
	var facts *typedCatalogFacts
	if q, ok := query.(dal.StructuredQuery); ok {
		if options.ExactNumericValues {
			if rec := q.IntoRecord(); rec != nil && !isMapData(rec.Data()) {
				return nil, errors.New("ExactNumericValues requires map record data so NUMERIC values can remain decimal text")
			}
		}
		if options.PreserveBinaryValues {
			if rec := q.IntoRecord(); rec != nil && !isMapData(rec.Data()) {
				return nil, errors.New("PreserveBinaryValues requires map record data so PostgreSQL BYTEA values can remain byte slices")
			}
		}
		if readsWithTypedPostgres(options) {
			// The typed compiler refuses what it cannot compute as asked, and reads that
			// from the query it is handed, which below can be a wrapper of this reader's
			// own that does not carry it. So the caller's query is asked first.
			if err = typedRefuseMoney(q); err != nil {
				err = fmt.Errorf("failed to get SQL reader: %w", err)
				return
			}
		}
		// A record is read by name, one value per name, so a select list that gives one name to
		// two expressions would lose a column. It is refused before any statement, which includes
		// the catalog's.
		if err = refusedOutputNames(q); err != nil {
			err = fmt.Errorf("failed to get SQL reader: %w", err)
			return
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
			// when it is one column, else the rows are keyed by ordinal. The lookup is a
			// statement, and a query that is refused is refused before any statement, so
			// the refusals getReaderBaseFor would make come first.
			if err = refusedBeforeAnyStatement(q); err != nil {
				err = fmt.Errorf("failed to get SQL reader: %w", err)
				return
			}
			var column string
			if facts, column, catalogKeyIndex, err = catalogPrimaryKey(ctx, options, q, execute); err != nil {
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
			// no column can be added to, is keyed from the base's own column of the key
			// (see keyColumnIndex).
			keyedByBase := len(q.From().Joins()) != 0 && readsWithOwnCompiler(options)
			selected := q.Columns()
			base := typedIdentity(q.From().Base())
			if len(selected) > 0 && (keyedByBase || !selectsIdentityField(selected, primaryKey, base, fold)) {
				columns := append([]dal.Column(nil), selected...)
				helper := unusedHelperColumn(selected)
				columns = append(columns, dal.Column{Expression: recordIdentityField(options, q, primaryKey), Alias: helper})
				query = dal.WithColumns(q, columns)
				rr.identityColumn = helper
				rr.hideIdentityColumn = true
			} else if len(selected) == 0 {
				rr.keyInBaseColumns = keyedByBase
				knownKeyIndex = catalogKeyIndex
				keyFromCatalog = catalogKeyIndex >= 0
			} else {
				knownKeyIndex = keyItemIndex(selected, primaryKey, base, fold)
			}
		}
	}

	if rr.readerBase, err = getReaderBaseFor(ctx, query, execute, options, rr.keyInBaseColumns, facts); err != nil {
		err = fmt.Errorf("failed to get SQL reader: %w", err)
		return
	}
	// Every record is keyed by the position of the column that holds the key, never by a name
	// that two columns of the result can share.
	switch {
	case rr.hideIdentityColumn:
		rr.identityColumnIndex = len(rr.colNames) - 1
	case knownKeyIndex >= 0:
		// The catalog and the statement are two statements, which share a connection and not a
		// snapshot: a bare * returns the stored names, and the catalog's key is a stored name, so
		// the column at its position must be that one.
		if keyFromCatalog && (knownKeyIndex >= len(rr.colNames) || rr.colNames[knownKeyIndex] != rr.identityColumn) {
			_ = rr.rows.Close()
			err = fmt.Errorf("failed to get SQL reader: %w", errColumnsChanged)
			return
		}
		rr.identityColumnIndex = knownKeyIndex
	case rr.identityColumn != "":
		names := rr.colNames
		if rr.keyInBaseColumns {
			names = rr.baseColumns // they lead the result
		}
		rr.identityColumnIndex = keyColumnIndex(names, rr.identityColumn, fold)
	}

	return
}

// refusedOutputNames refuses the select list of a query over one source that gives one output
// name to two different expressions (see repeatedOutputName), as DALgo's generic engine and
// the compilers of this package refuse it for a statement with joins (errJoinOutputRepeated). A
// record holds one value per name, so one of the two columns would be dropped without a word,
// and the key of a record could be the value of the wrong one. The names a wildcard lists are
// not known before the statement, and a statement with joins is left to its compiler.
func refusedOutputNames(q dal.StructuredQuery) error {
	if from := q.From(); from == nil || len(from.Joins()) != 0 {
		return nil
	}
	seen := map[string]dal.Expression{}
	for i, column := range q.Columns() {
		if name, repeated := repeatedOutputName(seen, column); repeated {
			return errOutputRepeated(i, name)
		}
	}
	return nil
}

// errOutputRepeated is the refusal of a select list over one source that gives two
// expressions one output name. The name is quoted: an alias is the caller's.
func errOutputRepeated(column int, name string) error {
	return fmt.Errorf("columns[%d]: duplicate output name %s", column, strconv.Quote(name))
}

// refusedBeforeAnyStatement is the refusals getReaderBaseFor makes of a structured query
// before it sends a statement: a query DALgo's own engine runs, and a wildcard projection
// that cannot be planned.
func refusedBeforeAnyStatement(q dal.StructuredQuery) error {
	if err := rejectRawRecursiveStructuredQuery(q); err != nil {
		return err
	}
	_, err := planWildcardProjection(q)
	return err
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
// key of the base source, as the catalog stores it, and its position among the columns of
// the source in table order. The column is "" and the position -1 when the source has none
// or several, and when the mount cannot write its name (a column stored as Id where the
// fold-lower mount writes id: the statement could not select it).
func catalogPrimaryKey(ctx context.Context, options DbOptions, q dal.StructuredQuery, execute executeQueryFunc) (*typedCatalogFacts, string, int, error) {
	dialect, err := postgresDialectFor(options)
	if err != nil {
		return nil, "", -1, err
	}
	facts, err := typedFactsForQuery(ctx, dialect, execute, q.From())
	if err != nil {
		return nil, "", -1, err
	}
	base, err := typedCollection(q.From().Base())
	if err != nil {
		return &facts, "", -1, nil // a source the compiler refuses: it reports it
	}
	source, _ := facts.source(typedSourceName{Schema: base.Schema(), Name: base.Name()})
	if position, ok := source.primaryKeyPosition(); ok && facts.addressable(source.Columns[position].Name) {
		return &facts, source.Columns[position].Name, position, nil
	}
	return &facts, "", -1, nil
}

// keyColumnIndex is the position of the column that carries the key's name, among the
// columns names lists, or -1 when none does. The statement writes the key's name as the mount
// folds it (under fold, nil: as it is), so a column stored under that name is the key. Failing
// that, a column that spells the name as the caller declared it is taken, and failing that the
// first column that is the name folded, so the choice does not depend on how many columns share
// the name. In a select-all over joins the base's columns lead the result, so a position among
// them is a position in the result; a column of a joined source that carries the name is never
// taken for it.
func keyColumnIndex(names []string, name string, fold nameFold) int {
	for _, written := range []string{fold.of(name), name} {
		for i, column := range names {
			if column == written {
				return i
			}
		}
	}
	for i, column := range names {
		if fold.of(column) == fold.of(name) {
			return i
		}
	}
	return -1
}

// keyItemIndex is the position of the first item of a select list that is the key's own field
// (see keyItemIsField), or -1 when none is and when the list holds a wildcard, which lists as
// many columns as its source has and puts the rest of the list at a position the list does not
// say. An item that spells the key's name as it is wins over one that is the name folded. base is
// the name the base source goes by in the statement.
func keyItemIndex(columns []dal.Column, name, base string, fold nameFold) int {
	for _, column := range columns {
		if column.Wildcard != nil {
			return -1
		}
	}
	for _, f := range []nameFold{nil, fold} {
		for i, column := range columns {
			if keyItemIsField(column, name, base, f) {
				return i
			}
		}
	}
	return -1
}

// keyItemIsField reports whether a select item is the field called name of the base source,
// under fold, under no name of its own or under that name: the column the key is read from. A
// field belongs to the base when it names no source, or the name the base goes by in the
// statement (base: its alias, else its name). A field of another source, which a join can give
// the same name, is not the key's.
func keyItemIsField(column dal.Column, name, base string, fold nameFold) bool {
	field, ok := column.Expression.(dal.FieldRef)
	if !ok || fold.of(field.Name()) != fold.of(name) {
		return false
	}
	if source := field.Source(); source != "" && fold.of(source) != fold.of(base) {
		return false
	}
	return column.Alias == "" || fold.of(column.Alias) == fold.of(name)
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
	newRecord func() dalrecord.Record
	// identityColumn is the name of the column the key is read from, and identityColumnIndex
	// its position in the result, from 0; -1 when the result has none, and the key is then the
	// ID the record was made with. A record is keyed by that position, never by a name.
	identityColumn      string
	identityColumnIndex int
	hideIdentityColumn  bool
	// keyInBaseColumns says the statement is a select-all over joins, keyed from the base
	// source's own column of the key's name (keyColumnIndex).
	keyInBaseColumns     bool
	validateFinite       bool
	dialect              string
	exactNumericValues   bool
	preserveBinaryValues bool
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

// exactNumericValue returns NUMERIC as decimal text without a float conversion.
// pgx returns NUMERIC this way; other drivers that provide a float64 are refused
// because the source decimal cannot be reconstructed from its rounded value.
func exactNumericValue(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return exactNumericText(v)
	case []byte:
		if !utf8.Valid(v) {
			return nil, fmt.Errorf("driver returned non-UTF-8 bytes; exact decimal text is required")
		}
		return exactNumericText(string(v))
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	default:
		return nil, fmt.Errorf("driver returned %T; exact decimal text is required", value)
	}
}

func exactNumericText(text string) (string, error) {
	if text == "NaN" || text == "Infinity" || text == "-Infinity" || decimalText.MatchString(text) {
		return text, nil
	}
	return "", fmt.Errorf("driver returned unsupported NUMERIC text; expected decimal text or PostgreSQL NaN/Infinity")
}

// selectsIdentityField reports whether the select list returns the column name of the base
// source (see keyItemIsField), under the names fold gives (nil: as they are). A wildcard
// returns it unless an exclusion names it, and the compiler folds the exclusions it applies,
// so they are folded here.
func selectsIdentityField(columns []dal.Column, name, base string, fold nameFold) bool {
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
		if keyItemIsField(column, name, base, fold) {
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
	mapColumnIndex := 0
	if d, isMap := data.(map[string]any); isMap {
		set = func(column string, raw, normalized any) error {
			// database/sql returns []byte for TEXT/VARCHAR columns with some
			// drivers (notably go-sql-driver/mysql); store as string so the
			// map is usable and JSON-serializes as text, not base64. Matches
			// scanRowIntoMapWithOptions on the Get path.
			value := textValue(normalized)
			columnType := columnTypeAt(r.colTypes, mapColumnIndex)
			if r.preserveBinaryValues && r.dialect == dialectPostgres && columnType != nil && strings.EqualFold(columnType.DatabaseTypeName(), "BYTEA") {
				if bytes, ok := normalized.([]byte); ok {
					value = bytes
				}
			}
			if r.dialect == "sqlite" && columnType != nil && strings.Contains(strings.ToUpper(columnType.DatabaseTypeName()), "BLOB") {
				value = normalizeReadMapValue(raw, columnType, r.dialect)
			}
			d[column] = value
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
		if _, isMap := data.(map[string]any); isMap {
			mapColumnIndex = i
		}
		var normalized any
		columnType := columnTypeAt(r.colTypes, i)
		if r.preserveBinaryValues && r.dialect == dialectPostgres && columnType != nil && strings.EqualFold(columnType.DatabaseTypeName(), "BYTEA") {
			normalized, err = preservePostgresBytea(values[i])
			if err != nil {
				return nil, fmt.Errorf("PostgreSQL BYTEA value in column %q: %w", n, err)
			}
		} else if r.exactNumericValues && strings.EqualFold(r.colTypes[i].DatabaseTypeName(), "NUMERIC") {
			normalized, err = exactNumericValue(values[i])
			if err != nil {
				return nil, fmt.Errorf("exact NUMERIC value in column %q: %w", n, err)
			}
		} else {
			normalized = normalizeValueByDatabaseType(r.colTypes[i].DatabaseTypeName(), values[i])
		}
		if r.validateFinite {
			if number, ok := normalized.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0)) {
				return nil, fmt.Errorf("non-finite aggregate result in column %q", n)
			}
		}
		v := textValue(normalized)
		if i == r.identityColumnIndex {
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
	var err error
	if r.rows != nil { // a reader the read did not get as far as opening rows for
		err = r.rows.Close()
	}
	r.lease.release()
	return err
}

// recordsReaderProvider is embedded into database and transaction
type recordsReaderProvider struct {
	executeQuery func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (rrp recordsReaderProvider) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	reader, err := getRecordsReader(ctx, query, rrp.executeQuery)
	if err != nil {
		return nil, err // a literal nil: see transaction.ExecuteQueryToRecordsReader
	}
	return reader, nil
}

//func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]record.Record, error) {
//	r, err := rrp.ExecuteQueryToRecordsReader(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//	return dal.ReadAllToRecords(ctx, r, options...)
//}
