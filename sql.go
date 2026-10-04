package dalgo2sql

import (
	"fmt"
	dalrecord "github.com/dal-go/record"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

type operation = int

const (
	insertOperation operation = iota
	updateOperation
)

type query struct {
	text string
	args []interface{}
}

func processPrimaryKey(primaryKey []string, key *dalrecord.Key, f func(i int, name string, v any)) {
	if len(primaryKey) == 1 {
		f(0, primaryKey[0], key.ID)
		return
	}
	id := reflect.ValueOf(key.ID).Interface()
	for i, pk := range primaryKey {
		var v any
		switch id := id.(type) { // TODO(ask-stackoverflow): how to avoid this switch?
		case []string:
			v = id[i]
		case []int:
			v = id[i]
		case []int8:
			v = id[i]
		case []int16:
			v = id[i]
		case []int32:
			v = id[i]
		case []int64:
			v = id[i]
		case []time.Time:
			v = id[i]
		default:
			panic(fmt.Sprintf("unsupported type for primary key value %T", id))
		}
		f(i, pk, v)
	}
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
	val := reflect.ValueOf(data)
	if kind := val.Kind(); kind == reflect.Interface || kind == reflect.Pointer {
		val = val.Elem()
	}

	if key.ID != nil && o == insertOperation {
		if len(pk) == 0 {
			panic(fmt.Sprintf("record key has value but no primary key defined for: '%s'", collection))
		}
		processPrimaryKey(pk, key, func(i int, pkName string, v any) {
			cols = append(cols, ident(positionPrimaryKey, pkName))
			q.args = append(q.args, v)
			argPlaceholders = append(argPlaceholders, options.Placeholder.placeholder(len(q.args)))
		})
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

	switch val.Kind() {
	case reflect.Struct:
		valType := val.Type()
		for i := 0; i < val.NumField(); i++ {
			addField(valType.Field(i).Name, val.Field(i).Interface())
		}
	case reflect.Map:
		if val.Type().Key().Kind() != reflect.String {
			panic(fmt.Sprintf("record data is a map but its keys are not strings: key kind=%s for collection '%s'", val.Type().Key().Kind(), collection))
		}
		mapKeys := val.MapKeys()
		names := make([]string, len(mapKeys))
		for i, k := range mapKeys {
			names[i] = k.String()
		}
		sort.Strings(names)
		for _, field := range names {
			v := val.MapIndex(reflect.ValueOf(field))
			addField(field, v.Interface())
		}
	default:
		panic(fmt.Sprintf("unsupported record data kind %s for collection '%s': expected struct or map[string]any", val.Kind(), collection))
	}
	if nameErr != nil {
		return query{}, nameErr
	}

	switch o {
	case insertOperation:
		q.text += fmt.Sprintf("(%v) VALUES (%v)",
			strings.Join(cols, ", "),
			strings.Join(argPlaceholders, ", "),
		)
	case updateOperation:
		if setColsCount == 0 {
			panic(fmt.Sprintf("no fields to updateOperation for: '%s'", collection))
		}
		var pkConditions []string
		processPrimaryKey(pk, key, func(i int, pkName string, v any) {
			q.args = append(q.args, v)
			pkConditions = append(pkConditions, ident(positionPrimaryKey, pkName)+" = "+options.Placeholder.placeholder(len(q.args)))
		})
		if nameErr != nil {
			return query{}, nameErr
		}
		q.text += " " + strings.Join(argPlaceholders, ", ") +
			fmt.Sprintf(" WHERE %v", strings.Join(pkConditions, " AND "))
	}
	return q, nil
}
