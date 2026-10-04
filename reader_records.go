package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

var _ dal.RecordsReader = (*recordsReader)(nil)

func getRecordsReader(ctx context.Context, query dal.Query, execute executeQueryFunc) (rr *recordsReader, err error) {
	return getRecordsReaderWithOptions(ctx, query, execute, DbOptions{})
}

const recordIDHelperColumn = "__dalgo_record_id"

func getRecordsReaderWithOptions(ctx context.Context, query dal.Query, execute executeQueryFunc, options DbOptions) (rr *recordsReader, err error) {
	rr = &recordsReader{
		identityColumnIndex: -1,
		newRecord: func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Unknown", ""), make(map[string]any))
		},
	}
	if q, ok := query.(dal.StructuredQuery); ok {
		rr.validateFinite = dal.HasAggregation(q)
		if rec := q.IntoRecord(); rec != nil {
			rr.newRecord = func() dalrecord.Record { return q.IntoRecord() }
		} else if from := q.From(); from != nil && from.Base() != nil {
			collection := from.Base().Name()
			if rr.validateFinite {
				// Aggregate result rows do not have a source record identity. Give
				// each result row a deterministic synthetic key without exposing a
				// helper column in its data.
				ordinal := 0
				rr.newRecord = func() dalrecord.Record {
					record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, strconv.Itoa(ordinal)), make(map[string]any))
					ordinal++
					return record
				}
			} else {
				rr.newRecord = func() dalrecord.Record {
					return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, recordIDHelperColumn), make(map[string]any))
				}
			}
		}
		if primaryKey := primaryKeyForQuery(options, query); primaryKey != "" && !dal.HasAggregation(q) {
			rr.identityColumn = primaryKey
			if selected := q.Columns(); len(selected) > 0 && !selectsIdentityField(selected, primaryKey) {
				columns := append([]dal.Column(nil), selected...)
				helper := unusedHelperColumn(selected)
				columns = append(columns, dal.Column{Expression: dal.Field(primaryKey), Alias: helper})
				query = dal.WithColumns(q, columns)
				rr.identityColumn = helper
				rr.hideIdentityColumn = true
			}
		}
	}

	if rr.readerBase, err = getReaderBaseWithOptions(ctx, query, execute, options); err != nil {
		err = fmt.Errorf("failed to get SQL reader: %w", err)
		return
	}
	if rr.hideIdentityColumn {
		rr.identityColumnIndex = len(rr.colNames) - 1
	}

	return
}

type recordsReader struct {
	readerBase
	newRecord           func() dalrecord.Record
	identityColumn      string
	identityColumnIndex int
	hideIdentityColumn  bool
	validateFinite      bool
}

// decimalText matches the text PostgreSQL prints for a finite NUMERIC: an
// optional sign and decimal digits with an optional fraction, no exponent.
var decimalText = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$`)

// normalizeValueByDatabaseType turns the text a PostgreSQL driver delivers for
// a NUMERIC column into a float64. pgx reports float64 as the scan type of
// NUMERIC but delivers a string.
//
//   - NUMERIC delivered as string or []byte becomes float64 when the text is
//     decimal text, or exactly NaN, Infinity or -Infinity (PostgreSQL's
//     spellings). Any other text, and decimal text outside the float64 range,
//     is kept as a string.
//   - Every other type name (DECIMAL, which is MySQL's, included), every other
//     value and nil are returned unchanged. JSON and JSONB need no handling here:
//     the records reader already stores []byte as string, and a string-typed
//     recordset column converts []byte.
//
// Callers choose where this applies: the recordset reader calls it only for a
// float64-typed column, so a value is never turned into a Go type the column
// cannot hold.
//
// A float64 holds about 15 significant digits exactly. Longer NUMERIC values,
// such as a NUMERIC(20,0) key, are rounded, so distinct values can compare
// equal. A NUMERIC NaN or infinity in a plain SELECT becomes a non-finite
// float64, which encoding/json refuses to marshal; aggregate queries reject it.
func normalizeValueByDatabaseType(databaseTypeName string, value any) any {
	if !strings.EqualFold(databaseTypeName, "NUMERIC") {
		return value
	}
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return value
	}
	switch text {
	case "NaN", "Infinity", "-Infinity":
	default:
		if !decimalText.MatchString(text) {
			return text
		}
	}
	if number, err := strconv.ParseFloat(text, 64); err == nil {
		return number
	}
	return text
}

func selectsIdentityField(columns []dal.Column, name string) bool {
	for _, column := range columns {
		if column.Wildcard != nil {
			if !column.Wildcard.Excludes(name) {
				return true
			}
		}
		if field, ok := column.Expression.(dal.FieldRef); ok && field.Name() == name && (column.Alias == "" || column.Alias == name) {
			return true
		}
	}
	return false
}

func unusedHelperColumn(columns []dal.Column) string {
	for suffix := 0; ; suffix++ {
		name := recordIDHelperColumn
		if suffix > 0 {
			name = fmt.Sprintf("%s_%d", name, suffix)
		}
		used := false
		for _, column := range columns {
			resultName := column.Alias
			if resultName == "" {
				if field, ok := column.Expression.(dal.FieldRef); ok {
					resultName = field.Name()
				}
			}
			if resultName == name {
				used = true
				break
			}
		}
		if !used {
			return name
		}
	}
}

func (r recordsReader) Next() (record dalrecord.Record, err error) {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return nil, err
		}
		return nil, dal.ErrNoMoreRecords
	}
	record = r.newRecord()
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		data = make(map[string]any)
		record = dalrecord.NewRecordWithData(record.Key(), data)
	}
	switch d := data.(type) {
	case map[string]any:
		var values []any
		if values, err = r.scanValues(); err != nil {
			return nil, err
		}
		for i, n := range r.colNames {
			v := normalizeValueByDatabaseType(r.colTypes[i].DatabaseTypeName(), values[i])
			if r.validateFinite {
				if number, ok := v.(float64); ok && (math.IsNaN(number) || math.IsInf(number, 0)) {
					return nil, fmt.Errorf("non-finite aggregate result in column %q", n)
				}
			}
			// database/sql returns []byte for TEXT/VARCHAR columns with some
			// drivers (notably go-sql-driver/mysql); store as string so the
			// map is usable and JSON-serializes as text, not base64. Matches
			// scanRowIntoMap on the Get path.
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			identityValue := n == r.identityColumn
			if r.hideIdentityColumn {
				identityValue = i == r.identityColumnIndex
			}
			if identityValue {
				record.Key().ID = v
				if v != nil {
					record.Key().IDKind = reflect.TypeOf(v).Kind()
				}
			}
			if !r.hideIdentityColumn || i != r.identityColumnIndex {
				d[n] = v
			}
		}
	default:
		// TODO: implement Scan into `*struct` and into `[]any`

		err = fmt.Errorf("unsupported data type %T", data)
		return nil, err
	}
	return
}

func (r recordsReader) Cursor() (string, error) {
	return "", dal.ErrNotSupported
}

func (r recordsReader) Close() error {
	return r.rows.Close()
}

// recordsReaderProvider is embedded into database and transaction
type recordsReaderProvider struct {
	executeQuery func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (rrp recordsReaderProvider) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return getRecordsReader(ctx, query, rrp.executeQuery)
}

//func (rrp recordsReaderProvider) ReadAllRecords(ctx context.Context, query dal.Query, options ...dal.ReaderOption) ([]record.Record, error) {
//	r, err := rrp.ExecuteQueryToRecordsReader(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//	return dal.ReadAllToRecords(ctx, r, options...)
//}
