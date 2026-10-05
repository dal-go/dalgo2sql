package dalgo2sql

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type operation = int

const (
	insertOperation operation = iota
	updateOperation
)

// ErrNoFieldsToWrite is wrapped by the error a key write returns when there is
// nothing to write, before any statement is sent: an Update or UpdateMulti with no
// updates; a Set or SetMulti of a record with no field that is not a column of the
// primary key; an Insert or InsertMulti of a record with neither a key ID nor a
// field. Test for it with errors.Is.
var ErrNoFieldsToWrite = errors.New("no fields to write")

type query struct {
	text string
	args []interface{}
}

// anyValues returns the values of a slice as the []any that processPrimaryKey walks.
func anyValues[T any](values []T) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

// processPrimaryKey calls f once for each column of primaryKey, with the part of the
// key's ID that is written to, or compared with, that column. A key of one column
// takes the ID itself. A key of several takes an ID that is a slice with one value
// for each column: a []string, []int, []int8, []int16, []int32, []int64 or
// []time.Time. Any other ID, and a slice of another length, is an error wrapping
// dal.ErrNotSupported, and f is not called.
func processPrimaryKey(primaryKey []string, key *dalrecord.Key, f func(i int, name string, v any)) error {
	if len(primaryKey) == 1 {
		f(0, primaryKey[0], key.ID)
		return nil
	}
	var values []any
	switch id := key.ID.(type) { // TODO(ask-stackoverflow): how to avoid this switch?
	case []string:
		values = anyValues(id)
	case []int:
		values = anyValues(id)
	case []int8:
		values = anyValues(id)
	case []int16:
		values = anyValues(id)
	case []int32:
		values = anyValues(id)
	case []int64:
		values = anyValues(id)
	case []time.Time:
		values = anyValues(id)
	default:
		return fmt.Errorf("%w: the ID of a key of recordset %s is a %T, and the primary key of the recordset has %d columns: "+
			"the ID of such a key is a slice with one value for each column ([]string, []int, []int8, []int16, []int32, []int64 or []time.Time)",
			dal.ErrNotSupported, getRecordsetName(key), key.ID, len(primaryKey))
	}
	if len(values) != len(primaryKey) {
		return fmt.Errorf("%w: the ID of a key of recordset %s has %d values, and the primary key of the recordset has %d columns",
			dal.ErrNotSupported, getRecordsetName(key), len(values), len(primaryKey))
	}
	for i, name := range primaryKey {
		f(i, name, values[i])
	}
	return nil
}

// recordField is one field of the data of a write.
type recordField struct {
	name  string
	value any
}

// recordDataFields returns the fields a write of data writes: the fields of a
// struct, in the order of their declaration, or the entries of a map with string
// keys (of any string type), sorted by name; data may also be a pointer to either.
// Any other data, and a struct with a field that is not exported (reflect cannot
// read its value, and the field cannot be told from a column), is an error
// wrapping dal.ErrNotSupported. The error names the recordset, never a value.
func recordDataFields(collection string, data any) ([]recordField, error) {
	val := reflect.ValueOf(data)
	if kind := val.Kind(); kind == reflect.Interface || kind == reflect.Pointer {
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Struct:
		valType := val.Type()
		fields := make([]recordField, 0, val.NumField())
		for i := 0; i < val.NumField(); i++ {
			field := valType.Field(i)
			if !field.IsExported() {
				return nil, fmt.Errorf("%w: the field %s of the %s data of recordset %s is not exported, so its value cannot be written",
					dal.ErrNotSupported, field.Name, valType, collection)
			}
			fields = append(fields, recordField{name: field.Name, value: val.Field(i).Interface()})
		}
		return fields, nil
	case reflect.Map:
		if kind := val.Type().Key().Kind(); kind != reflect.String {
			return nil, fmt.Errorf("%w: the keys of the map data of recordset %s are %s, not strings",
				dal.ErrNotSupported, collection, kind)
		}
		fields := make([]recordField, 0, val.Len())
		for entries := val.MapRange(); entries.Next(); {
			fields = append(fields, recordField{name: entries.Key().String(), value: entries.Value().Interface()})
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
		return fields, nil
	case reflect.Invalid:
		return nil, fmt.Errorf("%w: the data of recordset %s is nil, want a struct or a map with string keys",
			dal.ErrNotSupported, collection)
	}
	return nil, fmt.Errorf("%w: the data of recordset %s is a %s, want a struct or a map with string keys",
		dal.ErrNotSupported, collection, val.Type())
}

// buildSingleRecordQuery builds the INSERT or UPDATE statement that writes
// record. Every collection, field and primary-key name goes into the text
// through DbOptions.sqlIdentifier; one that is refused returns an error
// wrapping ErrUnsafeName and no statement.
func buildSingleRecordQuery(o operation, options DbOptions, record dalrecord.Record) (q query, err error) {
	key := record.Key()
	collection := getRecordsetName(key) // for messages; the text carries table
	table, err := options.recordsetIdentifier(key)
	if err != nil {
		return query{}, err
	}
	pk := options.PrimaryKeyFieldNames(key)
	switch o {
	case insertOperation:
		q.text = "INSERT INTO " + table
	case updateOperation:
		q.text = fmt.Sprintf("UPDATE %v SET ", table)
	}
	// ident renders a name for the text. A refused name is kept in nameErr and
	// checked before the statement is used, as the callbacks cannot return it.
	var nameErr error
	ident := func(position, name string) string {
		rendered, err := options.sqlIdentifier(position, name)
		if err != nil && nameErr == nil {
			nameErr = err
		}
		return rendered
	}
	var cols []string
	var argPlaceholders []string
	record.SetError(nil)
	data := record.Data()

	if key.ID != nil && o == insertOperation {
		if len(pk) == 0 {
			return query{}, fmt.Errorf("primary key is not defined for recordset %s: the ID of the record's key has no column to be written to", collection)
		}
		if err = processPrimaryKey(pk, key, func(i int, pkName string, v any) {
			cols = append(cols, ident(positionPrimaryKey, pkName))
			q.args = append(q.args, v)
			argPlaceholders = append(argPlaceholders, options.Placeholder.placeholder(len(q.args)))
		}); err != nil {
			return query{}, err
		}
		if nameErr != nil {
			return query{}, nameErr
		}
	}

	setColsCount := 0

	addField := func(fieldName string, value any) {
		if slices.Contains(pk, fieldName) {
			return
		}
		col := ident(positionField, fieldName)
		cols = append(cols, col)
		q.args = append(q.args, value)
		placeholder := options.Placeholder.placeholder(len(q.args))
		switch o {
		case insertOperation:
			argPlaceholders = append(argPlaceholders, placeholder)
		case updateOperation:
			argPlaceholders = append(argPlaceholders, col+" = "+placeholder)
			setColsCount++
		}
	}

	fields, err := recordDataFields(collection, data)
	if err != nil {
		return query{}, err
	}
	for _, field := range fields {
		addField(field.name, field.value)
	}
	if nameErr != nil {
		return query{}, nameErr
	}

	switch o {
	case insertOperation:
		if len(cols) == 0 {
			return query{}, fmt.Errorf("%w: the record of recordset %s has neither a key ID nor a field to insert", ErrNoFieldsToWrite, collection)
		}
		q.text += fmt.Sprintf("(%v) VALUES (%v)",
			strings.Join(cols, ", "),
			strings.Join(argPlaceholders, ", "),
		)
	case updateOperation:
		if setColsCount == 0 {
			return query{}, fmt.Errorf("%w: the record of recordset %s has no field that is not a column of its primary key to update", ErrNoFieldsToWrite, collection)
		}
		var pkConditions []string
		if err = processPrimaryKey(pk, key, func(i int, pkName string, v any) {
			q.args = append(q.args, v)
			pkConditions = append(pkConditions, ident(positionPrimaryKey, pkName)+" = "+options.Placeholder.placeholder(len(q.args)))
		}); err != nil {
			return query{}, err
		}
		if nameErr != nil {
			return query{}, nameErr
		}
		q.text += " " + strings.Join(argPlaceholders, ", ") +
			fmt.Sprintf(" WHERE %v", strings.Join(pkConditions, " AND "))
	}
	return q, nil
}
