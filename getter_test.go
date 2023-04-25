package dalgo2sql

import (
	"github.com/dal-go/dalgo/dal"
	"reflect"
	"testing"
)

func Test_getSelectFields(t *testing.T) {
	type args struct {
		record    dal.Record
		includePK bool
		options   Options
	}

	tests := []struct {
		name       string
		args       args
		wantFields []string
	}{
		{
			name: "simple_fields_exclude_primary_key",
			args: args{
				record: dal.NewRecordWithoutKey("SomeCollection", reflect.String, struct {
					StrField string
					IntField int
				}{}),
				includePK: false,
			},
			wantFields: []string{"StrField", "IntField"},
		},
		{
			name: "simple_fields_include_primary_key",
			args: args{
				record: dal.NewRecordWithData(
					dal.NewKeyWithID("TestTable", "r1"),
					struct {
						StrField string
						IntField int
					}{}),
				includePK: true,
			},
			wantFields: []string{"ID", "StrField", "IntField"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFields := getSelectFields(tt.args.record, tt.args.includePK, tt.args.options); !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("getSelectFields() = %v, want %v", gotFields, tt.wantFields)
			}
		})
	}
}
