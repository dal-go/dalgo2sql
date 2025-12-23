package dalgo2sql

import (
	"fmt"
	"reflect"
	"slices"
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
		query.text = fmt.Sprintf("UPDATE %v SET", collection)
	}
	var cols []string
	var argPlaceholders []string
	record.SetError(nil)
	data := record.Data()
	val := reflect.ValueOf(data)
	if kind := val.Kind(); kind == reflect.Interface || kind == reflect.Ptr {
		val = val.Elem()
	}
	valType := val.Type()

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

	for i := 0; i < val.NumField(); i++ {
		name := valType.Field(i).Name
		if slices.Contains(pk, name) {
			continue
		}
		cols = append(cols, name)
		query.args = append(query.args, val.Field(i).Interface())
		switch o {
		case insertOperation:
			argPlaceholders = append(argPlaceholders, "?")
		case updateOperation:
			argPlaceholders = append(argPlaceholders, valType.Field(i).Name+" = ?")
			setColsCount++
		}
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
		query.text += strings.Join(argPlaceholders, ",\n") +
			fmt.Sprintf(" WHERE %v = ?", strings.Join(pkConditions, " AND "))
	}
	return query
}
