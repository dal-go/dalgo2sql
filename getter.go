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
	rsName := getRecordsetName(key)
	table, err := options.recordsetIdentifier(key)
	if err != nil {
		return false, err
	}
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

func getSingle(_ context.Context, options DbOptions, record dalrecord.Record, exec queryExecutor) error {
	key := record.Key()
	rsName := getRecordsetName(key)
	table, err := options.recordsetIdentifier(key)
	if err != nil {
		record.SetError(err)
		return err
	}
	fields := getSelectFields(false, options, record)
	fieldsStr, err := selectList(options, fields, false)
	if err != nil {
		record.SetError(err)
		return err
	}
	if fieldsStr == "" {
		fieldsStr = "1"
	}
	queryText := fmt.Sprintf("SELECT %s FROM %s WHERE ", fieldsStr, table)

	pk := options.PrimaryKeyFieldNames(key)
	if len(pk) == 0 {
		return fmt.Errorf("%w: primary key is not defined for recorset %s", dalrecord.ErrRecordNotFound, rsName)
	} else if len(pk) > 1 {
		return fmt.Errorf("%w: select by composite primary key is not supported yet", dal.ErrNotImplementedYet)
	}
	pkName, err := options.sqlIdentifier(positionPrimaryKey, pk[0])
	if err != nil {
		record.SetError(err)
		return err
	}
	queryText += pkName + " = " + options.Placeholder.placeholder(1)

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
	byCollection := make(map[string][]dalrecord.Record)
	for _, r := range records {
		id := r.Key().Collection()
		recs := byCollection[id]
		byCollection[id] = append(recs, r)
	}
	for _, recs := range byCollection {
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

// refuseRecords records err on every record of a refused batch and returns it.
func refuseRecords(records []dalrecord.Record, err error) error {
	for _, record := range records {
		record.SetError(err)
	}
	return err
}

func getMultiFromSingleTable(_ context.Context, options DbOptions, records []dalrecord.Record, exec queryExecutor) error {
	if len(records) == 0 {
		return nil
	}
	records = append(make([]dalrecord.Record, 0, len(records)), records...)
	collection := records[0].Key().Collection()
	table, err := options.sqlIdentifier(positionCollection, collection)
	if err != nil {
		return refuseRecords(records, err)
	}

	rs, hasRecordsetDefinition := options.Recordsets[collection]
	var primaryKey []string
	if hasRecordsetDefinition && len(rs.PrimaryKey()) > 0 {
		for _, pk := range rs.PrimaryKey() {
			primaryKey = append(primaryKey, pk.Name())
		}
	} else if len(options.PrimaryKey) > 0 {
		primaryKey = options.PrimaryKey
	} else {
		err := fmt.Errorf("%w: no primary key defined for: '%s'", dalrecord.ErrRecordNotFound, collection)
		for _, record := range records {
			record.SetError(err)
		}
		return nil
	}

	pkColumns := make([]string, len(primaryKey))
	for i, pkName := range primaryKey {
		if pkColumns[i], err = options.sqlIdentifier(positionPrimaryKey, pkName); err != nil {
			return refuseRecords(records, err)
		}
	}

	// Call SetError(nil) on all records so that Data() is accessible below.
	for _, r := range records {
		r.SetError(nil)
	}

	// For map data we use SELECT * and identify the PK column after reading columns.
	// For struct data we enumerate fields explicitly.
	dataIsMap := isMapData(records[0].Data())

	var fields []string
	if dataIsMap {
		fields = []string{"*"}
	} else {
		fields = getSelectFields(true, options, records...)
	}

	columns, err := selectList(options, fields, true)
	if err != nil {
		return refuseRecords(records, err)
	}
	queryText := fmt.Sprintf("SELECT %v FROM %v WHERE ", columns, table)
	args := make([]interface{}, len(records))
	if len(records) == 1 /*len(records) == 1*/ {
		args = []any{}
		var pkConditions []string
		n := 1
		processPrimaryKey(primaryKey, records[0].Key(), func(i int, _ string, v any) {
			pkConditions = append(pkConditions, pkColumns[i]+" = "+options.Placeholder.placeholder(n))
			n++
		})
		queryText += " " + strings.Join(pkConditions, " AND ")
	} else {
		if len(primaryKey) > 1 {
			panic("not yet supported to query multiple records by key from recordsets with composite primary key")
		}
		queryText += fmt.Sprintf("%s IN (", pkColumns[0]) // TODO(help-wanted): support composite primary keys
		var argPlaceholders []string
		for i, record := range records {
			n := i + 1
			processPrimaryKey(primaryKey, record.Key(), func(_ int, name string, v any) {
				argPlaceholders = append(argPlaceholders, options.Placeholder.placeholder(n))
				args[i] = v
			})
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
						mv.SetMapIndex(reflect.ValueOf(col), reflect.ValueOf(val))
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
	return sqlscan.ScanRow(data, rows)
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
		// If it's a pointer to a nil map, initialize the map first.
		if v.Elem().Kind() == reflect.Map && v.Elem().IsNil() {
			v.Elem().Set(reflect.MakeMap(v.Elem().Type()))
		}
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
			v.SetMapIndex(reflect.ValueOf(col), reflect.ValueOf(val))
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

func getSelectFields(includePK bool, options DbOptions, records ...dalrecord.Record) (fields []string) {
	record := records[0] // TODO: support union of fields from multiple records?
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

	// For map data we cannot enumerate columns ahead of time, so use SELECT *.
	// The scan path (scanRowIntoMap) handles the result columns generically.
	if val.Kind() == reflect.Map {
		return []string{"*"}
	}

	valType := val.Type()
	var numberOfFields int
	if val.Kind() == reflect.Struct {
		numberOfFields = valType.NumField()
	}
	if includePK {
		key := record.Key()
		if key == nil {
			panic("not able to determine key field(s) as a record does not reference a key")
		}
		collection := record.Key().Collection()
		if strings.TrimSpace(collection) == "" {
			panic("record key reference an empty collection name")
		}
		fields = make([]string, 1, numberOfFields+1)
		if rs, hasOptions := options.Recordsets[collection]; hasOptions {
			fields[0] = rs.PrimaryKey()[0].Name()
		} else {
			fields[0] = "ID"
		}
	} else {
		fields = make([]string, 0, numberOfFields)
	}
	for i := 0; i < numberOfFields; i++ {
		fields = append(fields, valType.Field(i).Name)
	}
	return fields
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
