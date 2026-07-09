package dalgo2sql

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
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

func processPrimaryKey(primaryKey []string, key *dal.Key, f func(i int, name string, v any)) {
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

func buildSingleRecordQuery(o operation, options DbOptions, record dal.Record) (query query) {
	key := record.Key()
	collection := getRecordsetName(key)
	pk := options.PrimaryKeyFieldNames(key)
	switch o {
	case insertOperation:
		query.text = "INSERT INTO " + collection
	case updateOperation:
		query.text = fmt.Sprintf("UPDATE %v SET ", collection)
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
		processPrimaryKey(pk, key, func(i int, name string, v any) {
			cols = append(cols, name)
			query.args = append(query.args, v)
			argPlaceholders = append(argPlaceholders, "?")
		})
	}

	setColsCount := 0

	addField := func(name string, value any) {
		if slices.Contains(pk, name) {
			return
		}
		cols = append(cols, name)
		query.args = append(query.args, value)
		switch o {
		case insertOperation:
			argPlaceholders = append(argPlaceholders, "?")
		case updateOperation:
			argPlaceholders = append(argPlaceholders, name+" = ?")
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
		for _, name := range names {
			v := val.MapIndex(reflect.ValueOf(name))
			addField(name, v.Interface())
		}
	default:
		panic(fmt.Sprintf("unsupported record data kind %s for collection '%s': expected struct or map[string]any", val.Kind(), collection))
	}

	switch o {
	case insertOperation:
		query.text += fmt.Sprintf("(%v) VALUES (%v)",
			strings.Join(cols, ", "),
			strings.Join(argPlaceholders, ", "),
		)
	case updateOperation:
		if setColsCount == 0 {
			panic(fmt.Sprintf("no fields to updateOperation for: '%s'", collection))
		}
		var pkConditions []string
		processPrimaryKey(pk, key, func(i int, name string, v any) {
			pkConditions = append(pkConditions, name+" = ?")
			query.args = append(query.args, v)
		})
		query.text += " " + strings.Join(argPlaceholders, ", ") +
			fmt.Sprintf(" WHERE %v", strings.Join(pkConditions, " AND "))
	}
	// Rewrite "?" placeholders to the dialect-specific form (e.g. "$1" for Postgres).
	query.text = options.Placeholder.rewritePlaceholders(query.text)
	return query
}
