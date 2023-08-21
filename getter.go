package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/georgysavva/scany/v2/sqlscan"
	"reflect"
	"strings"
)

type queryExecutor = func(query string, args ...interface{}) (*sql.Rows, error)

func (dtb *database) Get(ctx context.Context, record dal.Record) error {
	return getSingle(ctx, dtb.options, record, dtb.db.Query)
}

func (t transaction) Get(ctx context.Context, record dal.Record) error {
	return getSingle(ctx, t.sqlOptions, record, t.tx.Query)
}

func (dtb *database) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMulti(ctx, dtb.options, records, dtb.db.Query)
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMulti(ctx, t.sqlOptions, records, t.tx.Query)
}

func getSingle(_ context.Context, options Options, record dal.Record, exec queryExecutor) error {
	fields := getSelectFields(record, false, options)
	queryText := fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), record.Key().Collection())
	rows, err := exec(queryText)
	if err != nil {
		record.SetError(err)
		if dal.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !rows.Next() {
		record.SetError(dal.ErrRecordNotFound)
		return nil
	}
	if err = rowIntoRecord(rows, record, false); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New("expected to get single row but got multiple")
	}
	return nil
}

func getMulti(ctx context.Context, options Options, records []dal.Record, exec queryExecutor) error {
	byCollection := make(map[string][]dal.Record)
	for _, r := range records {
		id := r.Key().Collection()
		recs := byCollection[id]
		byCollection[id] = append(recs, r)
	}
	for _, recs := range byCollection {
		if err := getMultiFromSingleTable(ctx, options, recs, exec); err != nil {
			return err
		}
	}
	return nil
}

func getMultiFromSingleTable(_ context.Context, options Options, records []dal.Record, exec queryExecutor) error {
	if len(records) == 0 {
		return nil
	}
	records = append(make([]dal.Record, 0, len(records)), records...)
	collection := records[0].Key().Collection()
	records[0].SetError(nil)
	val := reflect.ValueOf(records[0].Data()).Elem()
	valType := val.Type()
	fields := getSelectFields(records[0], true, options)
	idCol := "ID"
	if rs, hasOptions := options.Recordsets[collection]; hasOptions && len(rs.PrimaryKey) == 1 {
		idCol = rs.PrimaryKey[0].Name
	}
	queryText := fmt.Sprintf("SELECT %v FROM %v WHERE %v",
		strings.Join(fields, ", "),
		records[0].Key().Collection(),
		idCol,
	)
	q := make([]string, len(records))
	args := make([]interface{}, len(records))
	for i, record := range records {
		args[i] = record.Key().ID
		q[i] = "?"
	}
	if len(records) == 1 {
		queryText += " = ?"
	} else {
		queryText += " IN (" + strings.Join(q, ", ")
	}
	queryText += ")"
	rows, err := exec(queryText, args...)
	if err != nil {
		return err
	}

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
	if err = rows.Err(); err == sql.ErrNoRows {
		err = nil
	} else if err != nil {
		return err
	}
	for _, record := range records {
		record.SetError(dal.NewErrNotFoundByKey(record.Key(), nil))
	}
	return err
}

func rowIntoRecord(rows *sql.Rows, record dal.Record, pkIncluded bool) error {
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		panic("getting records by key requires a record with data")
	}
	if err := scanIntoData(rows, data, pkIncluded); err != nil {
		record.SetError(err)
		return err
	}
	record.SetError(dal.NoError)
	return nil
	//return delayedScanWithDataTo(rows, record)
}

//func delayedScanWithDataTo(rows *sql.Rows, record dal.Record) error {
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
	if pkIncluded {
		return scanIntoDataWithPrimaryKeyIncluded(rows, data)
	}
	return sqlscan.ScanRow(data, rows)
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

func getSelectFields(record dal.Record, includePK bool, options Options) (fields []string) {
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		panic(fmt.Sprintf("getting by ID requires a record with data, key: %v", record.Key()))
	}
	val := reflect.ValueOf(data)
	kind := val.Kind()
	if kind == reflect.Ptr || kind == reflect.Interface {
		val = val.Elem()
	} // TODO: throw panic

	valType := val.Type()
	numberOfFields := valType.NumField()
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
			fields[0] = rs.PrimaryKey[0].Name
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
