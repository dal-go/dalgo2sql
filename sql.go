package dalgo2sql

import (
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"reflect"
	"strings"
)

type operation = int

const (
	insert operation = iota
	update
)

type query struct {
	text string
	args []interface{}
}

func buildSingleRecordQuery(o operation, options Options, record dal.Record) (query query) {
	key := record.Key()
	switch o {
	case insert:
		query.text = fmt.Sprintf("INSERT INTO %v ", key.Collection())
	case update:
		query.text = fmt.Sprintf("UPDATE %v ", key.Collection())
	}
	var cols []string
	var q []string
	data := record.Data()
	val := reflect.ValueOf(data)
	if kind := val.Kind(); kind == reflect.Interface || kind == reflect.Ptr {
		val = val.Elem()
	}
	valType := val.Type()

	if key.ID != nil && o == insert {
		query.args = append(query.args, key.ID)
		if rs, hasOptions := options.Recordsets[key.Collection()]; hasOptions && len(rs.PrimaryKey) == 1 {
			cols = append(cols, rs.PrimaryKey[0].Name)
		} else {
			cols = append(cols, "ID")
		}
		q = append(q, "?")
	}

	for i := 0; i < val.NumField(); i++ {
		cols = append(cols, valType.Field(i).Name)
		query.args = append(query.args, val.Field(i).Interface())
		switch o {
		case insert:
			q = append(q, "?")
		case update:
			q = append(q, valType.Field(i).Name+" = ?")
		}
	}

	switch o {
	case insert:
		query.text = fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)",
			key.Collection(),
			strings.Join(cols, ", "),
			strings.Join(q, ", "),
		)
	case update:
		collection := key.Collection()
		where := "WHERE ID = ?"
		if rs, hasOptions := options.Recordsets[collection]; hasOptions {
			if len(rs.PrimaryKey) == 1 {
				where = fmt.Sprintf("WHERE %v = ?", rs.PrimaryKey[0].Name)
			}
		}
		query.text = fmt.Sprintf("UPDATE %v SET\n", collection) +
			strings.Join(q, ",\n") +
			where
		query.args = append(query.args, key.ID)
	}
	return query
}
