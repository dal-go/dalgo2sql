package dalgo2sql

import (
	"context"
	"errors"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// GetMulti checks every name it will write, for every key, before its first
// statement: a batch with one name that cannot be written sends nothing, whichever
// order the recordsets are read in.
func TestGetMulti_ChecksEveryNameBeforeTheFirstStatement(t *testing.T) {
	type nonASCII struct{ Имя string }
	plain := func(id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", id), map[string]any{})
	}
	account := func(id string, data any) dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(hostilePrimaryKeyCollection, id), data)
	}
	orders := func(id string, data any) dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("orders", id), data)
	}
	cases := []struct {
		name     string
		dialect  string
		position string
		records  func() []dalrecord.Record
	}{
		{"primary key of a recordset read on its own", "", "primary key", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), account("id2", map[string]any{})}
		}},
		{"primary key of a recordset read with several records", "", "primary key", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), account("id2", map[string]any{}), account("id3", map[string]any{})}
		}},
		{"primary key, struct data", "", "primary key", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), account("id2", &struct{ Name string }{}), account("id3", &struct{ Name string }{})}
		}},
		{"struct field of a recordset read on its own", "", "field", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), orders("id2", &nonASCII{})}
		}},
		{"struct field of a recordset read with several records", "", "field", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), orders("id2", &nonASCII{}), orders("id3", &nonASCII{})}
		}},
		{"primary key, sqlite", dialectSQLite, "primary key", func() []dalrecord.Record {
			return []dalrecord.Record{plain("id1"), account("id2", map[string]any{})}
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			options := validNames().options(tt.dialect)
			options.Recordsets["orders"] = NewRecordset("orders", Table, []dal.FieldRef{dal.Field("id")})
			hostile := "id; --"
			if tt.dialect == dialectSQLite {
				hostile = "nul\x00name"
			}
			options.Recordsets[hostilePrimaryKeyCollection] = NewRecordset(hostilePrimaryKeyCollection, Table, []dal.FieldRef{dal.Field(hostile)})
			for _, r := range keyPathAPIs(t, options) {
				t.Run(r.kind, func(t *testing.T) {
					records := tt.records()
					var err error
					for range 25 { // recordsets are read in map order
						err = r.api.GetMulti(context.Background(), records)
					}
					requireRefused(t, r.recorder, err, tt.position)
					for _, record := range records {
						if !errors.Is(record.Error(), ErrUnsafeName) {
							t.Errorf("record error = %v, want one wrapping ErrUnsafeName", record.Error())
						}
					}
				})
			}
		})
	}
}

// The names are checked again where the read is built: a record changed after the
// check cannot carry a name the check never saw, and nothing is sent for it.
func TestGetMulti_ChecksTheNamesAgainWhereTheReadIsBuilt(t *testing.T) {
	record := &shiftingRecord{
		key:   dalrecord.NewKeyWithID("users", "id1"),
		datas: []any{&struct{ Name string }{}, &struct{ Имя string }{}},
	}
	for _, r := range keyPathAPIs(t, validNames().options("")) {
		t.Run(r.kind, func(t *testing.T) {
			record.reads = 0
			err := r.api.GetMulti(context.Background(), []dalrecord.Record{record})
			requireRefused(t, r.recorder, err, "field")
		})
	}
}

// A recordset with no primary key is not a name that cannot be written: it is
// reported on its records as not found and the other recordsets are read.
func TestGetMulti_ARecordsetWithNoPrimaryKeyDoesNotRefuseTheBatch(t *testing.T) {
	options := validNames().options("")
	options.PrimaryKey = nil
	things := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("things", "t1"), map[string]any{}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("things", "t2"), map[string]any{}),
	}
	users := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), map[string]any{})
	for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return false }) {
		t.Run(r.kind, func(t *testing.T) {
			if err := r.api.GetMulti(context.Background(), append([]dalrecord.Record{users}, things...)); err != nil {
				t.Fatal(err)
			}
			if got := r.recorder.calls(); len(got) != 1 || got[0] != "SELECT * FROM users WHERE id = ?" {
				t.Errorf("statements = %q", got)
			}
			for _, record := range things {
				if record.Exists() {
					t.Errorf("record %v exists", record.Key())
				}
			}
		})
	}
}
