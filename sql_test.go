package dalgo2sql

import (
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

func TestProcessPrimaryKey(t *testing.T) {
	t.Run("single_key", func(t *testing.T) {
		key := dal.NewKeyWithID("users", "u1")
		processPrimaryKey([]string{"ID"}, key, func(i int, name string, v any) {
			if i != 0 || name != "ID" || v != "u1" {
				t.Errorf("unexpected values: i=%d, name=%s, v=%v", i, name, v)
			}
		})
	})

	t.Run("composite_string_key", func(t *testing.T) {
		id := []string{"u1", "p1"}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"ID", "ParentID"}, key, func(i int, name string, v any) {
			if i == 0 {
				if name != "ID" || v != "u1" {
					t.Errorf("unexpected values at 0: name=%s, v=%v", name, v)
				}
			} else if i == 1 {
				if name != "ParentID" || v != "p1" {
					t.Errorf("unexpected values at 1: name=%s, v=%v", name, v)
				}
			}
		})
	})

	t.Run("composite_int_key", func(t *testing.T) {
		id := []int{1, 2}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != i+1 {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int8_key", func(t *testing.T) {
		id := []int8{1, 2}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int8(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int16_key", func(t *testing.T) {
		id := []int16{1, 2}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int16(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int32_key", func(t *testing.T) {
		id := []int32{1, 2}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int32(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int64_key", func(t *testing.T) {
		id := []int64{1, 2}
		key := &dal.Key{ID: id}
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int64(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_time_key", func(t *testing.T) {
		now := time.Now()
		key := &dal.Key{ID: []time.Time{now, now}}
		processPrimaryKey([]string{"T1", "T2"}, key, func(i int, name string, v any) {
			if v != now {
				t.Errorf("expected %v, got %v", now, v)
			}
		})
	})

	t.Run("unsupported_type", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		key := dal.NewKeyWithID("users", 1.23)
		processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {})
	})
}

func TestBuildSingleRecordQuery_Panics(t *testing.T) {
	t.Run("insert_no_pk_defined", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
		buildSingleRecordQuery(insertOperation, DbOptions{}, record)
	})

	t.Run("update_no_fields", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		// If we mark "Name" as part of PK, there will be no fields to update
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
		buildSingleRecordQuery(updateOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID"), dal.Field("Name")}),
			},
		}, record)
	})
}
