package dalgo2sql

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

var _ dal.RecordsetReader = (*recordsetReader)(nil)

func getRecordsetReader(ctx context.Context, query dal.Query, execute executeQueryFunc) (rr *recordsetReader, err error) {
	rr = &recordsetReader{}
	if rr.readerBase, err = getReaderBase(ctx, query, execute); err != nil {
		return nil, err
	}

	var cols []recordset.Column[any]
	for _, col := range rr.colTypes {
		name := col.Name()
		var c recordset.Column[any]
		scanType := col.ScanType()
		dbTypeName := col.DatabaseTypeName()
		dbType := recordset.ColDbType(dbTypeName)

		if scanType == nil {
			// This happens for some views in SQLite
			c = recordset.NewColumn[string](name, "")
		} else {
			kind := scanType.Kind()
			switch kind {
			case reflect.String:
				c = recordset.UntypedCol(recordset.NewTypedColumn[string](name, "", dbType))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				c = recordset.UntypedCol(recordset.NewTypedColumn[int64](name, 0, dbType))
			case reflect.Float32, reflect.Float64:
				c = recordset.UntypedCol(recordset.NewTypedColumn[float64](name, 0, dbType))
			case reflect.Bool:
				c = recordset.UntypedCol(recordset.NewBoolColumn(name))
			case reflect.Struct:
				switch scanType.String() {
				case "time.Time":
					c = recordset.UntypedCol(recordset.NewTypedColumn[time.Time](name, time.Time{}, dbType))
				case "sql.NullString":
					c = recordset.UntypedCol(recordset.NewTypedColumn[string](name, "", dbType))
				case "sql.NullInt8":
					c = recordset.UntypedCol(recordset.NewTypedColumn[int8](name, 0, dbType))
				case "sql.NullInt16":
					c = recordset.UntypedCol(recordset.NewTypedColumn[int16](name, 0, dbType))
				case "sql.NullInt32":
					c = recordset.UntypedCol(recordset.NewTypedColumn[int32](name, 0, dbType))
				case "sql.NullInt64":
					c = recordset.UntypedCol(recordset.NewTypedColumn[int64](name, 0, dbType))
				case "sql.NullFloat64":
					c = recordset.UntypedCol(recordset.NewTypedColumn[float64](name, 0, dbType))
				case "sql.NullBool":
					c = recordset.UntypedCol(recordset.NewBoolColumn(name))
				case "sql.NullTime":
					c = recordset.UntypedCol(recordset.NewTypedColumn[time.Time](name, time.Time{}, dbType))
				default:
					err = fmt.Errorf("unsupported type for column %s: %s", name, scanType.String())
					return
				}
			case reflect.Slice, reflect.Array:
				if scanType.Elem().Kind() == reflect.Uint8 {
					c = recordset.UntypedCol(recordset.NewTypedColumn[[]byte](name, nil, dbType))
				} else {
					// For now, let's assume it's a string if it's a slice (common for SQL)
					c = recordset.UntypedCol(recordset.NewTypedColumn[string](name, "", dbType))
				}
			case reflect.Interface:
				// Assume it's a nullable []byte/blob if it's an interface (common for some drivers/sqlmock)
				c = recordset.UntypedCol(recordset.NewTypedColumn[[]byte](name, nil, dbType))
			case reflect.Ptr:
				elem := scanType.Elem()
				switch elem.Kind() {
				case reflect.Uint8:
					c = recordset.UntypedCol(recordset.NewTypedColumn[[]byte](name, nil, dbType))
				case reflect.Interface:
					// SQLite might return *interface{} for some columns
					c = recordset.NewColumn[string](name, "")
				default:
					err = fmt.Errorf("unsupported pointer type for column %s: %s", name, scanType.String())
					return
				}
			default:
				err = fmt.Errorf("unsupported column type kind %v for column %s", kind, name)
				return
			}
		}
		if c == nil {
			err = fmt.Errorf("column %s has nil recordset column after type mapping", name)
			return
		}
		cols = append(cols, c)
	}
	rr.rs = recordset.NewColumnarRecordset(cols...)
	return rr, nil
}

type recordsetReader struct {
	readerBase
	rs recordset.Recordset
}

func (r *recordsetReader) Recordset() recordset.Recordset {
	return r.rs
}

func (r *recordsetReader) Cursor() (string, error) {
	return "", dal.ErrNotImplementedYet
}

func (r *recordsetReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

func (r *recordsetReader) Next() (row recordset.Row, rs recordset.Recordset, err error) {
	if !r.rows.Next() {
		err = dal.ErrNoMoreRecords
		return
	}
	var values []any
	if values, err = r.scanValues(); err != nil {
		err = fmt.Errorf("failed to get row values from SQL reader: %w", err)
		return
	}

	row = r.rs.NewRow()
	for i := range r.colNames {
		value := values[i]
		col := r.rs.GetColumnByIndex(i)
		vt := col.ValueType()
		if value == nil {
			if vt == reflect.TypeOf([]byte(nil)) {
				value = []byte(nil)
			} else if vt.Kind() != reflect.Interface {
				// dalgo/recordset.UntypedColWrapper.SetValue(row int, value any) performs value.(T)
				// which panics if value is nil and T is not an interface.
				value = col.DefaultValue()
			}
		} else {
			// SQLite might return int64 for a column mapped to float64 if the value is an integer.
			// dalgo/recordset is strict about types in SetValue.
			if vt.Kind() == reflect.Float64 {
				switch v := value.(type) {
				case int64:
					value = float64(v)
				case int:
					value = float64(v)
				case float32:
					value = float64(v)
				}
			} else if vt.Kind() == reflect.Int64 {
				switch v := value.(type) {
				case int:
					value = int64(v)
				case int32:
					value = int64(v)
				case float64:
					value = int64(v)
				}
			} else if vt.Kind() == reflect.String {
				switch v := value.(type) {
				case []byte:
					value = string(v)
				case fmt.Stringer:
					value = v.String()
				default:
					value = fmt.Sprint(v)
				}
			}
		}
		if err = row.SetValueByIndex(i, value, r.rs); err != nil {
			err = fmt.Errorf("failed to set value for column %s: %w", r.colNames[i], err)
			return
		}
	}
	return row, r.rs, nil
}
