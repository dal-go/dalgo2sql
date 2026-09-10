package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

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
		newRecord: func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Unknown", ""), make(map[string]any))
		},
	}
	if q, ok := query.(dal.StructuredQuery); ok {
		if rec := q.IntoRecord(); rec != nil {
			rr.newRecord = func() dalrecord.Record { return q.IntoRecord() }
		} else if from := q.From(); from != nil && from.Base() != nil {
			collection := from.Base().Name()
			rr.newRecord = func() dalrecord.Record {
				return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, recordIDHelperColumn), make(map[string]any))
			}
		}
		if primaryKey := primaryKeyForQuery(options, query); primaryKey != "" {
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

	if rr.readerBase, err = getReaderBaseWithDialect(ctx, query, execute, options.StructuredQueryDialect); err != nil {
		err = fmt.Errorf("failed to get SQL reader: %w", err)
		return
	}

	return
}

type recordsReader struct {
	readerBase
	newRecord          func() dalrecord.Record
	identityColumn     string
	hideIdentityColumn bool
}

func selectsIdentityField(columns []dal.Column, name string) bool {
	for _, column := range columns {
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
			v := values[i]
			// database/sql returns []byte for TEXT/VARCHAR columns with some
			// drivers (notably go-sql-driver/mysql); store as string so the
			// map is usable and JSON-serializes as text, not base64. Matches
			// scanRowIntoMap on the Get path.
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			if n == r.identityColumn {
				record.Key().ID = v
				if v != nil {
					record.Key().IDKind = reflect.TypeOf(v).Kind()
				}
			}
			if !r.hideIdentityColumn || n != r.identityColumn {
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
