package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/georgysavva/scany/v2/sqlscan"
)

type queryExecutor = func(query string, args ...interface{}) (*sql.Rows, error)

func (dtb *database) Exists(ctx context.Context, key *dalrecord.Key) (exists bool, err error) {
	return executeExists(ctx, dtb.options, key, dtb.db.Query)
}

func (t transaction) Exists(ctx context.Context, key *dalrecord.Key) (exists bool, err error) {
	return executeExists(ctx, t.sqlOptions, key, t.tx.Query)
}

func (dtb *database) Get(ctx context.Context, record dalrecord.Record) error {
	return getSingle(ctx, dtb.options, record, dtb.db.Query)
}

func (t transaction) Get(ctx context.Context, record dalrecord.Record) error {
	return getSingle(ctx, t.sqlOptions, record, t.tx.Query)
}

func (dtb *database) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	return getMulti(ctx, dtb.options, records, dtb.db.Query)
}

func (t transaction) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	return getMulti(ctx, t.sqlOptions, records, t.tx.Query)
}

func executeExists(_ context.Context, options DbOptions, key *dalrecord.Key, exec queryExecutor) (exists bool, err error) {
	table, err := options.recordsetIdentifier(key)
	if err != nil {
		return false, err
	}
	rsName := getRecordsetName(key)
	queryText := fmt.Sprintf("SELECT 1 FROM %s WHERE ", table)

	pk := options.PrimaryKeyFieldNames(key)
	if len(pk) == 0 {
		err = fmt.Errorf("%w: primary key is not defined for recorset %s", dalrecord.ErrRecordNotFound, rsName)
		return
	} else if len(pk) > 1 {
		err = fmt.Errorf("%w: select by composite primary key is not supported yet", dal.ErrNotImplementedYet)
		return
	}
	pkName, err := options.sqlIdentifier(positionPrimaryKey, pk[0])
	if err != nil {
		return false, err
	}
	queryText += pkName + " = " + options.Placeholder.placeholder(1)

	var rows *sql.Rows
	if rows, err = exec(queryText, key.ID); err != nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()
	if !rows.Next() {
		return false, nil
	}
	return true, nil
}

// singleGetNames are the names a read of one record writes into SQL text.
type singleGetNames struct {
	table  string // the recordset
	fields string // the SELECT list, "1" for a record with no field
	pk     string // the primary-key column
}

// renderSingleGet returns the names a read of record writes into SQL text, or the
// error that stops the read. onRecord says whether the error is recorded on the
// record: a name that cannot be written is, a primary key that cannot be used is
// only returned.
func renderSingleGet(options DbOptions, record dalrecord.Record) (names singleGetNames, onRecord bool, err error) {
	key := record.Key()
	if names.table, err = options.recordsetIdentifier(key); err != nil {
		return singleGetNames{}, true, err
	}
	fields, _ := dataFieldNames(record)
	if names.fields, err = selectList(options, fields, false); err != nil {
		return singleGetNames{}, true, err
	}
	if names.fields == "" {
		names.fields = "1"
	}
	pk := options.PrimaryKeyFieldNames(key)
	if len(pk) == 0 {
		return singleGetNames{}, false, fmt.Errorf("%w: primary key is not defined for recorset %s", dalrecord.ErrRecordNotFound, getRecordsetName(key))
	} else if len(pk) > 1 {
		return singleGetNames{}, false, fmt.Errorf("%w: select by composite primary key is not supported yet", dal.ErrNotImplementedYet)
	}
	if names.pk, err = options.sqlIdentifier(positionPrimaryKey, pk[0]); err != nil {
		return singleGetNames{}, true, err
	}
	return names, false, nil
}

func getSingle(_ context.Context, options DbOptions, record dalrecord.Record, exec queryExecutor) error {
	key := record.Key()
	if err := checkReadTarget(record); err != nil {
		record.SetError(err)
		return err
	}
	names, onRecord, err := renderSingleGet(options, record)
	if err != nil {
		if onRecord {
			record.SetError(err)
		}
		return err
	}
	queryText := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s", names.fields, names.table, names.pk, options.Placeholder.placeholder(1))

	rows, err := exec(queryText, key.ID)
	if err != nil {
		record.SetError(err)
		return err
	}
	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		notFound := dal.NewErrNotFoundByKey(key, nil)
		record.SetError(notFound)
		return notFound
	}
	if err = rowIntoRecord(rows, record, false); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New("expected to get single row but got multiple")
	}
	return nil
}

func getMulti(ctx context.Context, options DbOptions, records []dalrecord.Record, exec queryExecutor) error {
	// The whole batch is checked before its first read: the recordsets below are
	// read one after the other in map order, so a refusal found half way would
	// leave it to chance which of the valid ones had been sent. Every name a read
	// writes (the collection path of every key, the primary-key column, the fields)
	// is checked here, for every recordset.
	for _, r := range records {
		if _, err := options.recordsetIdentifier(r.Key()); err != nil {
			return refuseRecords(records, err)
		}
		if err := checkReadTarget(r); err != nil {
			return refuseRecords(records, err)
		}
	}
	// Records are read together when their keys address the same recordset.
	byRecordset := make(map[string][]dalrecord.Record)
	for _, r := range records {
		name := getRecordsetName(r.Key())
		byRecordset[name] = append(byRecordset[name], r)
	}
	for _, recs := range byRecordset {
		if err := checkGetNames(options, recs); err != nil {
			return refuseRecords(records, err)
		}
	}
	for _, recs := range byRecordset {
		if len(recs) == 1 {
			if err := getSingle(ctx, options, recs[0], exec); err != nil {
				recs[0].SetError(err)
				if errors.Is(err, ErrUnsafeName) {
					return err
				}
			}
		} else if err := getMultiFromSingleTable(ctx, options, recs, exec); err != nil {
			return err
		}
	}
	return nil
}

// checkGetNames returns the error that refuses a batch because a name the read of
// records, which share a recordset, would write cannot be written; it is nil for a
// batch that is read, and also for one whose records only fail to be found.
func checkGetNames(options DbOptions, records []dalrecord.Record) error {
	if len(records) == 1 {
		// A primary key that cannot be used is reported on the record, as it is
		// when the record is read.
		if _, _, err := renderSingleGet(options, records[0]); errors.Is(err, ErrUnsafeName) {
			return err
		}
		return nil
	}
	_, _, err := renderMultiGet(options, records)
	return err
}

// refuseRecords records err on every record of a refused batch and returns it.
func refuseRecords(records []dalrecord.Record, err error) error {
	for _, record := range records {
		record.SetError(err)
	}
	return err
}

// multiGetNames are the names a read of several records of one recordset writes
// into SQL text.
type multiGetNames struct {
	table      string   // the recordset
	primaryKey []string // the primary-key fields, as declared
	pkColumns  []string // the primary-key columns, as written
	fields     []string // the fields to select, "*" for map data
	columns    string   // the SELECT list
	dataIsMap  bool
}

// renderMultiGet returns the names a read of records, which share a recordset,
// writes into SQL text. noPrimaryKey is the error of a recordset with no primary
// key, which is recorded on the records and does not stop the batch; err is any
// other reason the read cannot be built.
func renderMultiGet(options DbOptions, records []dalrecord.Record) (names multiGetNames, noPrimaryKey, err error) {
	// The records share the recordset their keys address, which is the table and
	// the one the primary key is looked up in.
	recordset := getRecordsetName(records[0].Key())
	if names.table, err = options.recordsetIdentifier(records[0].Key()); err != nil {
		return multiGetNames{}, nil, err
	}

	rs, hasRecordsetDefinition := options.Recordsets[recordset]
	if hasRecordsetDefinition && len(rs.PrimaryKey()) > 0 {
		for _, pk := range rs.PrimaryKey() {
			names.primaryKey = append(names.primaryKey, pk.Name())
		}
	} else if len(options.PrimaryKey) > 0 {
		names.primaryKey = options.PrimaryKey
	} else {
		return multiGetNames{}, fmt.Errorf("%w: no primary key defined for: '%s'", dalrecord.ErrRecordNotFound, recordset), nil
	}

	// One IN statement compares one column; the records of a composite key have no
	// such statement yet. This is checked with the names, before any statement of the
	// batch, so that no recordset of it has been read when the refusal is returned.
	if len(names.primaryKey) > 1 && len(records) > 1 {
		return multiGetNames{}, nil, fmt.Errorf("%w: reading several records of recordset %s by its composite primary key is not supported yet",
			dal.ErrNotImplementedYet, recordset)
	}

	names.pkColumns = make([]string, len(names.primaryKey))
	for i, pkName := range names.primaryKey {
		if names.pkColumns[i], err = options.sqlIdentifier(positionPrimaryKey, pkName); err != nil {
			return multiGetNames{}, nil, err
		}
	}

	// Call SetError(nil) on all records so that Data() is accessible below.
	for _, r := range records {
		r.SetError(nil)
	}

	// For map data we use SELECT * and identify the PK column after reading columns.
	// For struct data we enumerate fields explicitly.
	names.dataIsMap = isMapData(records[0].Data())
	for _, r := range records[1:] {
		// One statement reads the records, and each is filled from its row as the first is.
		if isMapData(r.Data()) != names.dataIsMap {
			return multiGetNames{}, nil, fmt.Errorf("%w: the records of recordset %s read together mix map and struct data",
				dal.ErrNotSupported, recordset)
		}
	}
	if names.dataIsMap {
		names.fields = []string{"*"}
	} else if names.fields, err = getSelectFields(true, options, records...); err != nil {
		return multiGetNames{}, nil, err
	}
	if names.columns, err = selectList(options, names.fields, true); err != nil {
		return multiGetNames{}, nil, err
	}
	return names, nil, nil
}

func getMultiFromSingleTable(_ context.Context, options DbOptions, records []dalrecord.Record, exec queryExecutor) error {
	if len(records) == 0 {
		return nil
	}
	records = append(make([]dalrecord.Record, 0, len(records)), records...)
	names, noPrimaryKey, err := renderMultiGet(options, records)
	if noPrimaryKey != nil {
		for _, record := range records {
			record.SetError(noPrimaryKey)
		}
		return nil
	}
	if err != nil {
		return refuseRecords(records, err)
	}
	table, primaryKey, pkColumns, fields, columns, dataIsMap := names.table, names.primaryKey, names.pkColumns, names.fields, names.columns, names.dataIsMap
	queryText := fmt.Sprintf("SELECT %v FROM %v WHERE ", columns, table)
	args := make([]interface{}, len(records))
	if len(records) == 1 /*len(records) == 1*/ {
		args = []any{}
		var pkConditions []string
		n := 1
		if err = processPrimaryKey(primaryKey, records[0].Key(), func(i int, _ string, v any) {
			pkConditions = append(pkConditions, pkColumns[i]+" = "+options.Placeholder.placeholder(n))
			n++
		}); err != nil {
			return refuseRecords(records, err)
		}
		queryText += " " + strings.Join(pkConditions, " AND ")
	} else {
		// Several records: renderMultiGet has refused a composite primary key, so the
		// key has one column and the ID of each record is its value.
		queryText += fmt.Sprintf("%s IN (", pkColumns[0]) // TODO(help-wanted): support composite primary keys
		var argPlaceholders []string
		for i, record := range records {
			argPlaceholders = append(argPlaceholders, options.Placeholder.placeholder(i+1))
			args[i] = record.Key().ID
		}
		queryText += strings.Join(argPlaceholders, ", ") + ")"
	}

	// EXECUTE QUERY
	rows, err := exec(queryText, args...)
	if err != nil {
		return err
	}

	if dataIsMap {
		// For map data: scan each row generically, then match by PK value.
		pkCol := primaryKey[0]
		for rows.Next() {
			cols, _ := rows.Columns()
			cells := make([]interface{}, len(cols))
			cellPtrs := make([]interface{}, len(cols))
			for i := range cells {
				cellPtrs[i] = &cells[i]
			}
			_ = rows.Scan(cellPtrs...)
			// Find PK column value.
			var rowIDVal interface{}
			for i, col := range cols {
				if col == pkCol {
					rowIDVal = cells[i]
					if b, ok := rowIDVal.([]byte); ok {
						rowIDVal = string(b)
					}
					break
				}
			}
			rowID := fmt.Sprintf("%v", rowIDVal)
			for i, record := range records {
				if fmt.Sprintf("%v", record.Key().ID) == rowID {
					records = append(records[:i], records[i+1:]...)
					// Fill the target map.
					m := record.Data()
					mv := reflect.ValueOf(m)
					if mv.Kind() == reflect.Pointer || mv.Kind() == reflect.Interface {
						mv = mv.Elem()
					}
					for ci, col := range cols {
						val := cells[ci]
						if b, ok := val.([]byte); ok {
							val = string(b)
						}
						setMapColumn(mv, col, val)
					}
					record.SetError(dalrecord.ErrNoError)
					break
				}
			}
		}
	} else {
		// Struct data path: use the struct-field-aware scan.
		val := reflect.ValueOf(records[0].Data()).Elem()
		valType := val.Type()
		for rows.Next() {
			var id string
			cells := make([]interface{}, len(fields))
			cells[0] = &id

			for i := 0; i < valType.NumField(); i++ {
				switch valType.Field(i).Type {
				case reflect.ValueOf("").Type():
					v := ""
					cells[i+1] = &v
				case reflect.ValueOf(1).Type():
					v := 0
					cells[i+1] = &v
				}
			}

			if err = rows.Scan(cells...); err != nil {
				return err
			}
			for i, record := range records {
				if record.Key().ID == id {
					records = append(records[:i], records[i+1:]...)
					if err = rowIntoRecord(rows, record, true); err != nil {
						return err
					}
					break
				}
			}
		}
	}
	if err = rows.Err(); errors.Is(err, sql.ErrNoRows) {
		err = nil
	} else if err != nil {
		return err
	}
	for _, record := range records {
		record.SetError(dal.NewErrNotFoundByKey(record.Key(), nil))
	}
	return err
}

func rowIntoRecord(rows *sql.Rows, record dalrecord.Record, pkIncluded bool) error {
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		panic("getting records by key requires a record with data")
	}
	if err := scanIntoData(rows, data, pkIncluded); err != nil {
		record.SetError(err)
		return err
	}
	record.SetError(dalrecord.ErrNoError)
	return nil
	//return delayedScanWithDataTo(rows, record)
}

//func delayedScanWithDataTo(rows *sql.Rows, record record.Record) error {
//	row, err := scanIntoMap(rows)
//	if err != nil {
//		record.SetError(err)
//		return err
//	}
//	record.SetDataTo(func(target interface{}) error {
//		t := reflect.ValueOf(target)
//		val := t.Elem()
//		valType := val.Type()
//		for i := 0; i < val.NumField(); i++ {
//			if val.Field(i).CanSet() {
//				fieldName := valType.Field(i).Name
//				if v, hasValue := row[fieldName]; hasValue {
//					val.Set(reflect.ValueOf(v))
//				}
//			}
//		}
//		return nil
//	})
//	return nil
//}

func scanIntoData(rows *sql.Rows, data interface{}, pkIncluded bool) error {
	if isMapData(data) {
		return scanRowIntoMap(rows, data, pkIncluded)
	}
	if pkIncluded {
		return scanIntoDataWithPrimaryKeyIncluded(rows, data)
	}
	if fields, isStruct := newStructFields(data); isStruct {
		return scanRowIntoStruct(rows, data, fields)
	}
	return sqlscan.ScanRow(data, rows)
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// scanRowIntoStruct scans the current row into the struct fields describes.
// Only the column matcher differs from scany: each column is resolved to a
// field without regard to case or underscores (see structColumns.lookup) and
// the row is scanned into the field addresses, so database/sql converts every
// value as it did for scany.
//
// scany stays in charge where it knew more than the matcher: a Scanner target
// with one column is scanned as a whole, and a row with a dotted column name
// that no field matches is left to scany, which accepts dotted names for nested
// struct fields. Any other column without a field is an error naming it.
func scanRowIntoStruct(rows *sql.Rows, data any, fields *structFields) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cols) == 1 && reflect.TypeOf(data).Implements(scannerType) {
		return sqlscan.ScanRow(data, rows)
	}
	dest := make([]any, len(cols))
	for i, col := range cols {
		field, found, err := fields.field(col)
		if err != nil {
			return err
		}
		if !found {
			if strings.Contains(col, ".") {
				return sqlscan.ScanRow(data, rows)
			}
			return fmt.Errorf("column %q: no corresponding field in %s", col, fields.elem.Type())
		}
		dest[i] = field.Addr().Interface()
	}
	return rows.Scan(dest...)
}

// isMapData reports whether data is a map[string]any or *map[string]any.
func isMapData(data interface{}) bool {
	if data == nil {
		return false
	}
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String
}

// setMapColumn stores value in the map m under the name of its column. A map with string
// keys of a named type, which a write takes as it takes any map with string keys, is
// keyed by the name converted to that type: reflect refuses a key of another type.
func setMapColumn(m reflect.Value, column string, value any) {
	key := reflect.ValueOf(column)
	if keyType := m.Type().Key(); keyType.Kind() == reflect.String && keyType != key.Type() {
		key = key.Convert(keyType)
	}
	m.SetMapIndex(key, reflect.ValueOf(value))
}

// scanRowIntoMap scans the current sql.Rows row into a map[string]any (or
// *map[string]any). PK columns are skipped if pkIncluded is false; when
// pkIncluded is true they are also included in the result map.
func scanRowIntoMap(rows *sql.Rows, data interface{}, pkIncluded bool) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Build generic scan targets.
	cells := make([]interface{}, len(cols))
	cellPtrs := make([]interface{}, len(cols))
	for i := range cells {
		cellPtrs[i] = &cells[i]
	}
	_ = rows.Scan(cellPtrs...)

	// Resolve the target map (handle *map[string]any or map[string]any).
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		// A pointer to a nil map has its map already: checkReadTarget made it.
		v = v.Elem()
	}

	for i, col := range cols {
		// Include all columns in the map (including PK column).
		// Callers that do not want the PK in the map can delete it afterward.
		_ = pkIncluded
		val := cells[i]
		// database/sql returns []byte for TEXT columns; convert to string for usability.
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		if val != nil {
			setMapColumn(v, col, val)
		}
	}
	return nil
}

func scanIntoDataWithPrimaryKeyIncluded(rows *sql.Rows, data interface{}) error {
	var id []byte
	val := reflect.ValueOf(data).Elem()
	valType := val.Type()
	cells := make([]interface{}, valType.NumField()+1)
	cells[0] = &id
	for i := 1; i < len(cells); i++ {
		cells[i] = val.Field(i - 1).Addr().Interface()
	}
	return rows.Scan(cells...)
}

//func scanIntoMap(rows *sql.Rows) (row map[string]interface{}, err error) {
//
//	cols, err := rows.Columns()
//
//	// Create a slice of interface{}'s to represent each cell,
//	// and a second slice to contain pointers to each item in the cells slice.
//	cells := make([]interface{}, len(cols))
//	cellPointers := make([]interface{}, len(cols))
//	for i := range cells {
//		cellPointers[i] = &cells[i]
//	}
//
//	// Scan the row into the cell pointers...
//	if err := rows.Scan(cellPointers...); err != nil {
//		return nil, err
//	}
//
//	// Create our map, and retrieve the value for each column from the pointers slice,
//	// storing it in the map with the name of the column as the key.
//	m := make(map[string]interface{}, len(cols))
//	for i, colName := range cols {
//		val := cellPointers[i].(*interface{})
//		m[colName] = *val
//	}
//	return m, nil
//}

// checkReadTarget refuses, before any statement, a record whose data a read cannot fill: the
// errors wrap dal.ErrNotSupported, name the kind of data and never a value. A read fills a
// pointer to a struct with exported fields, a map with string keys whose elements hold any
// value (a map by value must be made: the read stores into it), or a pointer to either, and
// whatever else the scan of one record accepts, such as a pointer to a sql.Scanner. The data
// of a record that has none, a struct given by value (its fields cannot be set), a nil
// pointer, a struct with a field that is not exported (reflect cannot set it, and it cannot be
// told from a column), a nil map by value, and a map with keys that are not strings or elements
// that cannot hold every column value can never be filled, and storing into them panics.
//
// A pointer to a nil map is a target: its map is made here, as the scan of one record always
// made it, so that a read of several records can store into it.
func checkReadTarget(record dalrecord.Record) error {
	// Data() panics while the record's error is unset or set to a failure, and SetError(nil)
	// clears both.
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		return fmt.Errorf("%w: the record has no data to read into, want a pointer to a struct or a map with string keys", dal.ErrNotSupported)
	}
	target := reflect.ValueOf(data)
	switch target.Kind() {
	case reflect.Struct:
		return fmt.Errorf("%w: the data is a %s value, which a read cannot fill, want a pointer to it", dal.ErrNotSupported, target.Type())
	case reflect.Map:
		if target.IsNil() {
			return fmt.Errorf("%w: the data is a nil %s, which a read cannot store into, want a made map or a pointer to it", dal.ErrNotSupported, target.Type())
		}
		return checkMapTarget(target.Type())
	case reflect.Pointer:
		if target.IsNil() {
			return fmt.Errorf("%w: the data is a nil %s, which a read cannot fill", dal.ErrNotSupported, target.Type())
		}
		switch elem := target.Elem(); elem.Kind() {
		case reflect.Struct:
			return checkStructTarget(elem.Type())
		case reflect.Map:
			if err := checkMapTarget(elem.Type()); err != nil {
				return err
			}
			if elem.IsNil() {
				elem.Set(reflect.MakeMap(elem.Type()))
			}
		}
	}
	return nil
}

// checkStructTarget refuses a struct type that has a field that is not exported.
func checkStructTarget(t reflect.Type) error {
	for i := 0; i < t.NumField(); i++ {
		if field := t.Field(i); !field.IsExported() {
			return fmt.Errorf("%w: the field %s of the %s data is not exported, so a read cannot set it", dal.ErrNotSupported, field.Name, t)
		}
	}
	return nil
}

// checkMapTarget refuses a map type whose keys are not strings or whose elements cannot hold
// every value a column can have, which is any value.
func checkMapTarget(t reflect.Type) error {
	if kind := t.Key().Kind(); kind != reflect.String {
		return fmt.Errorf("%w: the keys of the %s data are %s, not strings", dal.ErrNotSupported, t, kind)
	}
	if elem := t.Elem(); elem.Kind() != reflect.Interface || elem.NumMethod() != 0 {
		return fmt.Errorf("%w: the elements of the %s data are %s, which cannot hold every column value, want any", dal.ErrNotSupported, t, elem)
	}
	return nil
}

// dataFieldNames lists the fields a read of record selects: the names of the
// struct fields of its data, or the one wildcard for map data, whose columns
// cannot be enumerated ahead of time (the scan path, scanRowIntoMap, handles the
// result columns generically).
func dataFieldNames(record dalrecord.Record) (fields []string, isMap bool) {
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		panic(fmt.Sprintf("getting by ID requires a record with data, key: %v", record.Key()))
	}
	val := reflect.ValueOf(data)
	kind := val.Kind()
	if kind == reflect.Pointer || kind == reflect.Interface {
		val = val.Elem()
	} // TODO: throw panic
	if val.Kind() == reflect.Map {
		return []string{"*"}, true
	}
	if val.Kind() != reflect.Struct {
		return nil, false
	}
	fields = make([]string, val.NumField())
	for i := range fields {
		fields[i] = val.Type().Field(i).Name
	}
	return fields, false
}

// getSelectFields lists the fields of a read of records, which share a recordset,
// with the primary key first when includePK is set. A declared recordset with no
// primary key is an error when the primary key is wanted.
func getSelectFields(includePK bool, options DbOptions, records ...dalrecord.Record) (fields []string, err error) {
	record := records[0] // TODO: support union of fields from multiple records?
	fields, isMap := dataFieldNames(record)
	if !includePK || isMap {
		return fields, nil
	}
	key := record.Key()
	if key == nil {
		return nil, fmt.Errorf("%w: the primary key cannot be determined, as the record has no key", dal.ErrNotSupported)
	}
	if strings.TrimSpace(key.Collection()) == "" {
		return nil, fmt.Errorf("%w: the primary key cannot be determined, as the key of the record names no collection", dal.ErrNotSupported)
	}
	primaryKey := "ID"
	if rs, hasOptions := options.Recordsets[getRecordsetName(key)]; hasOptions {
		declared := rs.PrimaryKey()
		if len(declared) == 0 {
			return nil, fmt.Errorf("primary key is not defined for recordset %s", getRecordsetName(key))
		}
		primaryKey = declared[0].Name()
	}
	return append([]string{primaryKey}, fields...), nil
}

// selectList renders the names getSelectFields returned as a SELECT list. The
// wildcard it returns for map data is not a name and is kept as is. When
// primaryKeyFirst is set the first name is the primary key.
func selectList(options DbOptions, fields []string, primaryKeyFirst bool) (string, error) {
	if len(fields) == 1 && fields[0] == "*" {
		return "*", nil
	}
	rendered := make([]string, len(fields))
	for i, name := range fields {
		position := positionField
		if primaryKeyFirst && i == 0 {
			position = positionPrimaryKey
		}
		var err error
		if rendered[i], err = options.sqlIdentifier(position, name); err != nil {
			return "", err
		}
	}
	return strings.Join(rendered, ", "), nil
}
