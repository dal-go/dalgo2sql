package dalgo2sql

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// structField is one settable field of a struct target.
type structField struct {
	index []int // path for reflect.Value.FieldByIndex, through embedded structs
	id    int   // position among reflect.VisibleFields, tells fields apart
}

// structColumns maps column names to the fields that receive them. A column is
// looked up by the three spellings in turn, so a closer spelling always wins
// over a looser one; see lookup.
type structColumns struct {
	exact  map[string]structField // the name as the db tag or Go field spells it
	folded map[string]structField // the same name lower-cased
	loose  map[string]structField // see columnKey
}

var structColumnsCache sync.Map // reflect.Type -> structColumns

// columnKey makes column and field names comparable regardless of case and
// underscores, so a PostgreSQL column "areasqkm", a SQLite column "AreaSqKm"
// and a snake_case column "area_sq_km" all reach the field AreaSqKm.
func columnKey(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

// lookup finds the field for a column: by exact name first, then without
// regard to case, then without regard to case and underscores.
func (c structColumns) lookup(column string) (structField, bool) {
	if field, ok := c.exact[column]; ok {
		return field, true
	}
	if field, ok := c.folded[strings.ToLower(column)]; ok {
		return field, true
	}
	field, ok := c.loose[columnKey(column)]
	return field, ok
}

// structColumnsOf lists the exported fields of t, promoted fields of embedded
// structs included. A field is known by the name in its `db` tag, or by its Go
// name without one; the tag "-" hides it. Within each spelling, when two fields
// share a name the first in declaration order wins.
func structColumnsOf(t reflect.Type) structColumns {
	if cached, ok := structColumnsCache.Load(t); ok {
		return cached.(structColumns)
	}
	columns := structColumns{
		exact:  map[string]structField{},
		folded: map[string]structField{},
		loose:  map[string]structField{},
	}
	add := func(into map[string]structField, name string, field structField) {
		if _, taken := into[name]; !taken {
			into[name] = field
		}
	}
	for id, f := range reflect.VisibleFields(t) {
		if !f.IsExported() || (f.Anonymous && isStructOrPointerToStruct(f.Type)) {
			continue
		}
		name := f.Name
		if tag, _, _ := strings.Cut(f.Tag.Get("db"), ","); tag == "-" {
			continue
		} else if tag != "" {
			name = tag
		}
		field := structField{index: f.Index, id: id}
		add(columns.exact, name, field)
		add(columns.folded, strings.ToLower(name), field)
		add(columns.loose, columnKey(name), field)
	}
	structColumnsCache.Store(t, columns)
	return columns
}

func isStructOrPointerToStruct(t reflect.Type) bool {
	return t.Kind() == reflect.Struct || (t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct)
}

// structFields resolves the columns of one row to the fields of a struct.
type structFields struct {
	elem    reflect.Value
	columns structColumns
	claimed map[int]string // field id -> the column of this row that reached it
}

// newStructFields returns nil, false when target is not a non-nil pointer to a
// struct.
func newStructFields(target any) (*structFields, bool) {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return nil, false
	}
	elem := rv.Elem()
	return &structFields{elem: elem, columns: structColumnsOf(elem.Type()), claimed: map[int]string{}}, true
}

// field returns the field that receives column, allocating nil embedded struct
// pointers on the way as scany and encoding/json do. found is false when the
// column has no field. Two columns of one row that reach the same field are an
// error, since one value would silently replace the other.
func (s *structFields) field(column string) (field reflect.Value, found bool, err error) {
	target, found := s.columns.lookup(column)
	if !found {
		return reflect.Value{}, false, nil
	}
	if other, taken := s.claimed[target.id]; taken {
		return reflect.Value{}, true, fmt.Errorf("columns %q and %q reach the same field of %s", other, column, s.elem.Type())
	}
	s.claimed[target.id] = column
	field = s.elem
	for i, x := range target.index {
		if i > 0 && field.Kind() == reflect.Pointer {
			if field.IsNil() {
				if !field.CanSet() {
					return reflect.Value{}, true, fmt.Errorf("column %q: cannot allocate embedded %s", column, field.Type())
				}
				field.Set(reflect.New(field.Type().Elem()))
			}
			field = field.Elem()
		}
		field = field.Field(x)
	}
	return field, true, nil
}

// structSetter returns a function that stores one column of a row into the
// struct that target points to. raw is the value the driver delivered and
// normalized is the same value after normalizeValueByDatabaseType. A column
// without a matching field is skipped, because the identity column of a record
// is not a field of its data. ok is false when target is not a non-nil pointer
// to a struct. A setter serves one row.
func structSetter(target any) (set func(column string, raw, normalized any) error, ok bool) {
	fields, ok := newStructFields(target)
	if !ok {
		return nil, false
	}
	return func(column string, raw, normalized any) error {
		field, found, err := fields.field(column)
		if err == nil && found {
			err = assignColumnValue(field, raw, normalized)
		}
		if err != nil {
			return fmt.Errorf("column %q: %w", column, err)
		}
		return nil
	}, true
}

// assignColumnValue stores a database/sql value into a struct field of the
// records reader. raw is what the driver delivered and normalized is raw after
// NUMERIC normalisation.
//
//   - A sql.Scanner receives raw, as database/sql would pass it, so a decimal
//     type sees the exact NUMERIC text and a JSON type sees []byte.
//   - A string field takes raw text; a []byte field (json.RawMessage included)
//     takes a copy of raw bytes.
//   - An integer field takes the integer the driver's text spells, so a NUMERIC
//     such as 9007199254740993 keeps every digit; text that is not a plain
//     integer (12.00) goes the way of the next line.
//   - Every other field takes normalized, with []byte turned into string as a
//     map target stores it: floats, booleans (SQLite stores them as integers),
//     time.Time and interface fields, and integers as just said.
//   - nil stores the zero value.
func assignColumnValue(field reflect.Value, raw, normalized any) error {
	if scanner, ok := field.Addr().Interface().(sql.Scanner); ok {
		return scanner.Scan(raw)
	}
	if raw == nil {
		field.SetZero()
		return nil
	}
	if field.Kind() == reflect.Pointer {
		pointee := reflect.New(field.Type().Elem())
		if err := assignColumnValue(pointee.Elem(), raw, normalized); err != nil {
			return err
		}
		field.Set(pointee)
		return nil
	}
	if b, ok := raw.([]byte); ok && field.Kind() == reflect.Slice {
		if clone := reflect.ValueOf(bytes.Clone(b)); clone.Type().AssignableTo(field.Type()) {
			field.Set(clone)
			return nil
		}
	}
	value := textValue(normalized)
	if field.Kind() == reflect.String {
		if text, ok := textValue(raw).(string); ok {
			value = text
		}
	}
	source := reflect.ValueOf(value)
	if source.Type().AssignableTo(field.Type()) {
		field.Set(source)
		return nil
	}
	switch field.Kind() {
	case reflect.Bool:
		b, err := toBool(source)
		if err != nil {
			return err
		}
		field.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := exactInt64(raw, source)
		if err != nil {
			return err
		}
		if field.OverflowInt(n) {
			return fmt.Errorf("value %d overflows %s", n, field.Type())
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := exactUint64(raw, source)
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

// exactInt64 reads an integer field from the driver's text when that text is an
// integer, because normalized is a float64 for NUMERIC and float64 holds only
// 53 bits. Any other text, and every value that is not text, is converted from
// normalized as before.
func exactInt64(raw any, normalized reflect.Value) (int64, error) {
	if text, ok := textValue(raw).(string); ok {
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return n, nil
		}
	}
	return toInt64(normalized)
}

// exactUint64 is exactInt64 for unsigned fields.
func exactUint64(raw any, normalized reflect.Value) (uint64, error) {
	if text, ok := textValue(raw).(string); ok {
		if n, err := strconv.ParseUint(text, 10, 64); err == nil {
			return n, nil
		}
	}
	return toUint64(normalized)
}

// textValue turns []byte into string and returns every other value unchanged:
// database/sql returns []byte for TEXT columns with some drivers, and the
// records reader stores text as string.
func textValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
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
