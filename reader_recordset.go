package dalgo2sql

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

var _ dal.RecordsetReader = (*recordsetReader)(nil)

func getRecordsetReader(ctx context.Context, query dal.Query, execute executeQueryFunc, options ...recordset.Option) (rr *recordsetReader, err error) {
	return getRecordsetReaderWithOptions(ctx, query, execute, DbOptions{}, options...)
}

func getRecordsetReaderWithDialect(ctx context.Context, query dal.Query, execute executeQueryFunc, dialect string, options ...recordset.Option) (rr *recordsetReader, err error) {
	return getRecordsetReaderWithOptions(ctx, query, execute, DbOptions{StructuredQueryDialect: dialect}, options...)
}

// getRecordsetReaderWithOptions runs the query and returns a reader over its rows. A read
// that fails returns no reader and holds no rows: one that fails after its statement ran
// (a column the recordset cannot hold) closes them here, on every path, because a caller
// who is handed an error does not close a reader it was not given.
func getRecordsetReaderWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, sqlOptions DbOptions, options ...recordset.Option) (rr *recordsetReader, err error) {
	rr = &recordsetReader{}
	if q, ok := query.(dal.StructuredQuery); ok {
		rr.validateFinite = dal.HasAggregation(q)
	}
	if rr.readerBase, err = getReaderBaseWithOptions(ctx, query, execute, sqlOptions); err != nil {
		return nil, err
	}

	// From here the statement has run and its rows are open.
	defer func() {
		if err != nil {
			_ = rr.rows.Close()
			rr = nil
		}
	}()

	rsOptions := recordset.NewOptions(options...)

	var cols []recordset.Column[any]
	for i, col := range rr.colTypes {
		// The name the reader gives the column: the server's, or, for a statement
		// compiled with a folding dialect, the name the query asked for.
		name := rr.colNames[i]
		var c recordset.Column[any]
		scanType := col.ScanType()
		dbTypeName := col.DatabaseTypeName()

		if scanType == nil {
			// This happens for some views in SQLite
			c = newNullableColumn(name, "", "")
		} else {
			kind := scanType.Kind()
			switch kind {
			case reflect.String:
				c = newNullableColumn(name, "", dbTypeName)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				c = newNullableColumn(name, int64(0), dbTypeName)
			case reflect.Float32, reflect.Float64:
				c = newNullableColumn(name, float64(0), dbTypeName)
			case reflect.Bool:
				c = newNullableColumn(name, false, "")
			case reflect.Struct:
				switch scanType.String() {
				case "time.Time":
					c = newNullableColumn(name, time.Time{}, dbTypeName)
				case "sql.NullString":
					c = newNullableColumn(name, "", dbTypeName)
				case "sql.NullByte":
					c = newNullableColumn(name, int64(0), dbTypeName)
				case "sql.NullInt16":
					c = newNullableColumn(name, int16(0), dbTypeName)
				case "sql.NullInt32":
					c = newNullableColumn(name, int32(0), dbTypeName)
				case "sql.NullInt64":
					c = newNullableColumn(name, int64(0), dbTypeName)
				case "sql.NullFloat64":
					c = newNullableColumn(name, float64(0), dbTypeName)
				case "sql.NullBool":
					c = newNullableColumn(name, false, "")
				case "sql.NullTime":
					c = newNullableColumn(name, time.Time{}, dbTypeName)
				default:
					err = fmt.Errorf("unsupported type for column %s: %s", name, scanType.String())
					return
				}
			case reflect.Slice, reflect.Array:
				if scanType.Elem().Kind() == reflect.Uint8 {
					c = newNullableColumn(name, []byte(nil), dbTypeName)
				} else {
					// For now, let's assume it's a string if it's a slice (common for SQL)
					c = newNullableColumn(name, "", dbTypeName)
				}
			case reflect.Interface:
				// Assume it's a nullable []byte/blob if it's an interface (common for some drivers/sqlmock)
				c = newNullableColumn(name, []byte(nil), dbTypeName)
			case reflect.Pointer:
				elem := scanType.Elem()
				switch elem.Kind() {
				case reflect.Uint8:
					c = newNullableColumn(name, []byte(nil), dbTypeName)
				case reflect.Interface:
					// SQLite might return *interface{} for some columns
					c = newNullableColumn(name, "", "")
				default:
					err = fmt.Errorf("unsupported pointer type for column %s: %s", name, scanType.String())
					return
				}
			default:
				err = fmt.Errorf("unsupported column type kind %v for column %s", kind, name)
				return
			}
		}
		cols = append(cols, c)
	}
	rr.rs = recordset.NewColumnarRecordset(rsOptions.Name(), cols...)
	return rr, nil
}

type recordsetReader struct {
	readerBase
	rs             recordset.Recordset
	validateFinite bool
}

func (r *recordsetReader) Recordset() recordset.Recordset {
	return r.rs
}

func (r *recordsetReader) Cursor() (string, error) {
	return "", dal.ErrNotImplementedYet
}

func (r *recordsetReader) Close() error {
	var err error
	if r.rows != nil {
		err = r.rows.Close()
	}
	r.lease.release()
	return err
}

func (r *recordsetReader) Next() (row recordset.Row, rs recordset.Recordset, err error) {
	if !r.rows.Next() {
		r.lease.release() // the rows closed themselves at the end: the connection goes back
		// Next is false at the end of the result and also when the stream broke (a
		// timeout, a server error at one row): only Err tells them apart, and a broken
		// stream is never a shorter result that ended well.
		if err = r.rows.Err(); err == nil {
			err = dal.ErrNoMoreRecords
		} else {
			err = streamError(r.lease, err)
		}
		return
	}
	var values []any
	if values, err = r.scanValues(); err != nil {
		err = fmt.Errorf("failed to get row values from SQL reader: %w", err)
		return
	}

	row = r.rs.NewRow()
	for i := range r.colNames {
		col := r.rs.GetColumnByIndex(i)
		vt := col.ValueType()
		value := values[i]
		if vt.Kind() == reflect.Float64 {
			// Only a float64 column can hold the converted value; any other column
			// keeps what the driver delivered.
			value = normalizeValueByDatabaseType(r.colTypes[i].DatabaseTypeName(), value)
		}
		if r.validateFinite {
			if number, ok := value.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0)) {
				err = fmt.Errorf("non-finite aggregate result in column %q", r.colNames[i])
				return
			}
		}
		// A NULL stays nil: the column holds it as a NULL (nullableColumn), so no value
		// stands in for it. Any other value is made the Go type of its column, which holds
		// that type only.
		if value != nil {
			switch vt.Kind() {
			case reflect.Float64:
				// SQLite might return int64 for a column mapped to float64 if the value is
				// an integer.
				switch v := value.(type) {
				case int64:
					value = float64(v)
				case string:
					// A string cannot live in a float64 column (a NUMERIC that is
					// not a number, or text stored in a SQLite REAL column); fail
					// with the column name and Go type, never the cell.
					err = fmt.Errorf("failed to set value for column %s: unexpected %T value in a float64 column", r.colNames[i], v)
					return
				}
			case reflect.Int64:
				if v, ok := value.(float64); ok {
					value = int64(v)
				}
			case reflect.Int16, reflect.Int32:
				// A driver delivers every integer as an int64. One that does not fit the
				// column is left as it is, and the column refuses it.
				value = narrowedInteger(vt, value)
			case reflect.Bool:
				// SQLite delivers the 0 and 1 of a BOOLEAN column as integers.
				if v, ok := value.(int64); ok {
					value = v != 0
				}
			case reflect.String:
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

// narrowedInteger returns value as a number of the integer type t, when value is an int64
// that t holds, and value as it was otherwise.
func narrowedInteger(t reflect.Type, value any) any {
	v, ok := value.(int64)
	if !ok {
		return value
	}
	if narrowed := reflect.ValueOf(v).Convert(t); narrowed.Int() == v {
		return narrowed.Interface()
	}
	return value
}
