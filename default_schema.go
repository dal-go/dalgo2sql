package dalgo2sql

import (
	"fmt"
	"reflect"

	"github.com/dal-go/dalgo/dal"
)

func NewSimpleSchema(idFieldName string) dal.Schema {
	return dal.NewSchema(
		simpleKeyToFields(idFieldName),
		simpleFieldsToKey(idFieldName),
	)
}

// simpleKeyToFields Check if the `data` argument has `idFieldName` field or `SetID(id any)` method sets the ID on data.
// Otherwise, adds `dal.NewExtraField("ID", key.ID)` to `fields`
// If `idFieldName` is an empty string, it is set to `idFieldName = fmt.Sprintf("%s_%v", key.Collection(), "_", key.ID)
// If `key` has parents, each parent is mapped to a field with name like `{parent.Collection()}_{parent.ID}`
func simpleKeyToFields(idFieldName string) dal.KeyToFieldsFunc {
	keyToField := func(key *dal.Key, data any) (fields []dal.ExtraField, err error) {

		if idFieldName == "" {
			idFieldName = fmt.Sprintf("%s_%v", key.Collection(), key.ID)
		}

		// Collect parent fields first (always include parents if present)
		var parentFields []dal.ExtraField
		for p := key.Parent(); p != nil; p = p.Parent() {
			parentFields = append(parentFields, dal.NewExtraField(
				fmt.Sprintf("%s_%v", p.Collection(), p.ID), p.ID,
			))
		}

		// If data is nil, it cannot carry ID, add an extra field for ID in addition to parents
		if data == nil {
			return append(parentFields, dal.NewExtraField(idFieldName, key.ID)), nil
		}

		// Helper to check for SetID method on a type
		hasSetIDMethod := func(v reflect.Value) bool {
			if !v.IsValid() {
				return false
			}
			// Check method on the value
			if m := v.MethodByName("SetID"); m.IsValid() {
				// Ensure it has exactly one input parameter besides receiver
				t := m.Type()
				if t.NumIn() == 1 { // for reflect.Value.Method, receiver is bound; NumIn counts only method parameters
					return true
				}
			}
			// If value is not a pointer, also check a pointer to it (for pointer receiver methods)
			if v.Kind() != reflect.Ptr {
				pv := reflect.New(v.Type())
				if m := pv.MethodByName("SetID"); m.IsValid() {
					t := m.Type()
					if t.NumIn() == 1 {
						return true
					}
				}
			} else if v.Kind() == reflect.Ptr {
				// Additionally check the element (for value receiver methods)
				e := v.Elem()
				if e.IsValid() {
					if m := e.MethodByName("SetID"); m.IsValid() {
						t := m.Type()
						if t.NumIn() == 1 {
							return true
						}
					}
				}
			}
			return false
		}

		v := reflect.ValueOf(data)
		if hasSetIDMethod(v) {
			return parentFields, nil
		}

		// Check for exported field named `idFieldName` on struct or pointer to struct
		t := v.Type()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct {
			if f, ok := t.FieldByName(idFieldName); ok {
				// Exported fields have empty PkgPath
				if f.PkgPath == "" {
					return parentFields, nil
				}
			}
		}

		// Neither a SetID method nor an exported ID field exists; add extra field alongside parents
		return append(parentFields, dal.NewExtraField(idFieldName, key.ID)), nil
	}
	return keyToField
}

// simpleFieldsToKey creates a key from `incompleteKey` and takes ID for the returned `key`
// either returned by `data.GetID() any` if has such a function on `data` struct
// or from the data field name defined in `idFieldName` argument.
func simpleFieldsToKey(idFieldName string) dal.DataToKeyFunc {
	dataToKey := func(incompleteKey *dal.Key, data any) (key *dal.Key, err error) {
		if data == nil {
			return nil, fmt.Errorf("data is nil: cannot derive ID for key in collection %s", incompleteKey.Collection())
		}

		// Prefer GetID() any if present
		v := reflect.ValueOf(data)

		getID := func(v reflect.Value) (any, bool) {
			if !v.IsValid() {
				return nil, false
			}
			if m := v.MethodByName("GetID"); m.IsValid() {
				mt := m.Type()
				if mt.NumIn() == 0 && mt.NumOut() == 1 { // method bound to receiver
					res := m.Call(nil)
					return res[0].Interface(), true
				}
			}
			// If pointer, also check value receiver; if value, also check pointer receiver
			if v.Kind() == reflect.Ptr {
				e := v.Elem()
				if e.IsValid() {
					if m := e.MethodByName("GetID"); m.IsValid() {
						mt := m.Type()
						if mt.NumIn() == 0 && mt.NumOut() == 1 {
							res := m.Call(nil)
							return res[0].Interface(), true
						}
					}
				}
			} else {
				pv := reflect.New(v.Type())
				if m := pv.MethodByName("GetID"); m.IsValid() {
					mt := m.Type()
					if mt.NumIn() == 0 && mt.NumOut() == 1 {
						res := m.Call(nil)
						return res[0].Interface(), true
					}
				}
			}
			return nil, false
		}

		var id any
		if got, ok := getID(v); ok {
			id = got
		} else {
			// Fallback to field by name
			name := idFieldName
			if name == "" {
				name = "ID"
			}
			// Dereference pointer if needed
			vt := v.Type()
			if vt.Kind() == reflect.Ptr {
				v = v.Elem()
				vt = v.Type()
			}
			if vt.Kind() != reflect.Struct {
				return nil, fmt.Errorf("data must be a struct or pointer to struct to read field %q", name)
			}
			if f, ok := vt.FieldByName(name); ok {
				if f.PkgPath != "" { // unexported
					return nil, fmt.Errorf("field %q is not exported", name)
				}
				fv := v.FieldByName(name)
				if !fv.IsValid() {
					return nil, fmt.Errorf("field %q value is invalid", name)
				}
				id = fv.Interface()
			} else {
				return nil, fmt.Errorf("field %q not found on data", name)
			}
		}

		// Build a new key preserving collection and parent, converting id to the expected kind
		parent := incompleteKey.Parent()
		collection := incompleteKey.Collection()
		// Helper to convert numeric kinds where possible
		convert := func(val any, kind reflect.Kind) (any, error) {
			if val == nil {
				return nil, fmt.Errorf("nil id")
			}
			rv := reflect.ValueOf(val)
			// direct kind match
			if rv.Kind() == kind {
				return val, nil
			}
			// attempt conversion when types are convertible
			target := reflect.New(reflect.TypeOf(0)).Elem() // placeholder int
			switch kind {
			case reflect.String:
				return fmt.Sprintf("%v", val), nil
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Bool, reflect.Float32, reflect.Float64:
				// try Convert via reflect when possible
				kt := kind
				var t reflect.Type
				switch kt {
				case reflect.Int:
					t = reflect.TypeOf(int(0))
				case reflect.Int8:
					t = reflect.TypeOf(int8(0))
				case reflect.Int16:
					t = reflect.TypeOf(int16(0))
				case reflect.Int32:
					t = reflect.TypeOf(int32(0))
				case reflect.Int64:
					t = reflect.TypeOf(int64(0))
				case reflect.Uint:
					t = reflect.TypeOf(uint(0))
				case reflect.Uint8:
					t = reflect.TypeOf(uint8(0))
				case reflect.Uint16:
					t = reflect.TypeOf(uint16(0))
				case reflect.Uint32:
					t = reflect.TypeOf(uint32(0))
				case reflect.Uint64:
					t = reflect.TypeOf(uint64(0))
				case reflect.Bool:
					t = reflect.TypeOf(false)
				case reflect.Float32:
					t = reflect.TypeOf(float32(0))
				case reflect.Float64:
					t = reflect.TypeOf(float64(0))
				}
				if rv.Type().ConvertibleTo(t) {
					cv := rv.Convert(t)
					return cv.Interface(), nil
				}
			}
			_ = target
			return nil, fmt.Errorf("cannot convert id of type %T to kind %v", val, kind)
		}

		idVal, err := convert(id, incompleteKey.IDKind)
		if err != nil {
			return nil, err
		}

		switch incompleteKey.IDKind {
		case reflect.String:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(string)), nil
		case reflect.Int:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(int)), nil
		case reflect.Int8:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(int8)), nil
		case reflect.Int16:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(int16)), nil
		case reflect.Int32:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(int32)), nil
		case reflect.Int64:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(int64)), nil
		case reflect.Uint:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(uint)), nil
		case reflect.Uint8:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(uint8)), nil
		case reflect.Uint16:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(uint16)), nil
		case reflect.Uint32:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(uint32)), nil
		case reflect.Uint64:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(uint64)), nil
		case reflect.Bool:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(bool)), nil
		case reflect.Float32:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(float32)), nil
		case reflect.Float64:
			return dal.NewKeyWithParentAndID(parent, collection, idVal.(float64)), nil
		default:
			// As a fallback, try string representation
			return dal.NewKeyWithParentAndID(parent, collection, fmt.Sprintf("%v", idVal)), nil
		}
	}
	return dataToKey
}
