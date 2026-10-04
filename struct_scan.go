package dalgo2sql

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// structColumns maps a normalised column key to the index path of the struct
// field that receives it. See columnKey for the normalisation.
type structColumns map[string][]int

var structColumnsCache sync.Map // reflect.Type -> structColumns

// columnKey makes column and field names comparable regardless of case and
// underscores, so a PostgreSQL column "areasqkm", a SQLite column "AreaSqKm"
// and a snake_case column "area_sq_km" all reach the field AreaSqKm.
func columnKey(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

// structColumnsOf lists the exported fields of t, promoted fields of embedded
// structs included. A field is known by the name in its `db` tag, or by its Go
// name without one; the tag "-" hides it. When two fields share a key the first
// wins.
func structColumnsOf(t reflect.Type) structColumns {
	if cached, ok := structColumnsCache.Load(t); ok {
		return cached.(structColumns)
	}
	columns := structColumns{}
	for _, f := range reflect.VisibleFields(t) {
		if !f.IsExported() || (f.Anonymous && f.Type.Kind() == reflect.Struct) {
			continue
		}
		name := f.Name
		if tag, _, _ := strings.Cut(f.Tag.Get("db"), ","); tag == "-" {
			continue
		} else if tag != "" {
			name = tag
		}
		if _, taken := columns[columnKey(name)]; !taken {
			columns[columnKey(name)] = f.Index
		}
	}
	structColumnsCache.Store(t, columns)
	return columns
}

// structSetter returns a function that stores one column value into the struct
// that target points to. A column without a matching field is an error, as it
// is with scany, unless ignoreUnmatched is set: the records reader needs that
// because the identity column of a record is not a field of its data. ok is
// false when target is not a non-nil pointer to a struct.
func structSetter(target any, ignoreUnmatched bool) (set func(column string, value any) error, ok bool) {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return nil, false
	}
	elem := rv.Elem()
	columns := structColumnsOf(elem.Type())
	return func(column string, value any) error {
		index, found := columns[columnKey(column)]
		if !found {
			if ignoreUnmatched {
				return nil
			}
			return fmt.Errorf("column %q: no corresponding field found in %s", column, elem.Type())
		}
		field, err := elem.FieldByIndexErr(index)
		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		if err = assignColumnValue(field, value); err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		return nil
	}, true
}

// assignColumnValue stores a database/sql value into a struct field. It
// handles what drivers deliver for the scalar field kinds: integers, floats,
// booleans (SQLite stores them as integers) and strings, plus time.Time and
// any type that implements sql.Scanner, directly or through a pointer. nil
// stores the zero value.
func assignColumnValue(field reflect.Value, value any) error {
	if value == nil {
		field.SetZero()
		return nil
	}
	source := reflect.ValueOf(value)
	if source.Type().AssignableTo(field.Type()) {
		field.Set(source)
		return nil
	}
	if scanner, ok := field.Addr().Interface().(sql.Scanner); ok {
		return scanner.Scan(value)
	}
	switch field.Kind() {
	case reflect.Pointer:
		pointee := reflect.New(field.Type().Elem())
		if err := assignColumnValue(pointee.Elem(), value); err != nil {
			return err
		}
		field.Set(pointee)
		return nil
	case reflect.Bool:
		b, err := toBool(source)
		if err != nil {
			return err
		}
		field.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := toInt64(source)
		if err != nil {
			return err
		}
		if field.OverflowInt(n) {
			return fmt.Errorf("value %d overflows %s", n, field.Type())
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := toUint64(source)
		if err != nil {
			return err
		}
		if field.OverflowUint(n) {
			return fmt.Errorf("value %d overflows %s", n, field.Type())
		}
		field.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := toFloat64(source)
		if err != nil {
			return err
		}
		if field.OverflowFloat(f) {
			return fmt.Errorf("value %v overflows %s", f, field.Type())
		}
		field.SetFloat(f)
	case reflect.String:
		if source.Kind() != reflect.String {
			return fmt.Errorf("cannot assign %T to %s", value, field.Type())
		}
		field.SetString(source.String())
	default:
		return fmt.Errorf("cannot assign %T to %s", value, field.Type())
	}
	return nil
}

func toBool(v reflect.Value) (bool, error) {
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return strconv.ParseBool(v.String())
	}
	n, err := toInt64(v)
	if err != nil || (n != 0 && n != 1) {
		return false, fmt.Errorf("cannot assign %v to a bool", v.Interface())
	}
	return n == 1, nil
}

func toInt64(v reflect.Value) (int64, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows int64", v.Uint())
		}
		return int64(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if f != math.Trunc(f) || f < -(1<<63) || f >= 1<<63 {
			return 0, fmt.Errorf("value %v is not an int64", f)
		}
		return int64(f), nil
	case reflect.Bool:
		if v.Bool() {
			return 1, nil
		}
		return 0, nil
	case reflect.String:
		return strconv.ParseInt(v.String(), 10, 64)
	}
	return 0, fmt.Errorf("cannot assign %s to an integer", v.Type())
}

func toUint64(v reflect.Value) (uint64, error) {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if f != math.Trunc(f) || f < 0 || f >= 1<<64 {
			return 0, fmt.Errorf("value %v is not a uint64", f)
		}
		return uint64(f), nil
	case reflect.String:
		return strconv.ParseUint(v.String(), 10, 64)
	}
	n, err := toInt64(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("cannot assign %v to an unsigned integer", v.Interface())
	}
	return uint64(n), nil
}

func toFloat64(v reflect.Value) (float64, error) {
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), nil
	case reflect.String:
		return strconv.ParseFloat(v.String(), 64)
	}
	return 0, fmt.Errorf("cannot assign %s to a float", v.Type())
}
