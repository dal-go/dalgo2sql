package dalgo2sql

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// mustProcessPrimaryKey is processPrimaryKey for an ID that fits the key.
func mustProcessPrimaryKey(t *testing.T, primaryKey []string, key *dalrecord.Key, f func(i int, name string, v any)) {
	t.Helper()
	if err := processPrimaryKey(primaryKey, key, f); err != nil {
		t.Fatal(err)
	}
}

func TestProcessPrimaryKey(t *testing.T) {
	t.Run("single_key", func(t *testing.T) {
		key := dalrecord.NewKeyWithID("users", "u1")
		mustProcessPrimaryKey(t, []string{"ID"}, key, func(i int, name string, v any) {
			if i != 0 || name != "ID" || v != "u1" {
				t.Errorf("unexpected values: i=%d, name=%s, v=%v", i, name, v)
			}
		})
	})

	t.Run("composite_string_key", func(t *testing.T) {
		id := []string{"u1", "p1"}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"ID", "ParentID"}, key, func(i int, name string, v any) {
			switch i {
			case 0:
				if name != "ID" || v != "u1" {
					t.Errorf("unexpected values at 0: name=%s, v=%v", name, v)
				}
			case 1:
				if name != "ParentID" || v != "p1" {
					t.Errorf("unexpected values at 1: name=%s, v=%v", name, v)
				}
			}
		})
	})

	t.Run("composite_int_key", func(t *testing.T) {
		id := []int{1, 2}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != i+1 {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int8_key", func(t *testing.T) {
		id := []int8{1, 2}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int8(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int16_key", func(t *testing.T) {
		id := []int16{1, 2}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int16(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int32_key", func(t *testing.T) {
		id := []int32{1, 2}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int32(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_int64_key", func(t *testing.T) {
		id := []int64{1, 2}
		key := &dalrecord.Key{ID: id}
		mustProcessPrimaryKey(t, []string{"K1", "K2"}, key, func(i int, name string, v any) {
			if v != int64(i+1) {
				t.Errorf("expected %d, got %v", i+1, v)
			}
		})
	})

	t.Run("composite_time_key", func(t *testing.T) {
		now := time.Now()
		key := &dalrecord.Key{ID: []time.Time{now, now}}
		mustProcessPrimaryKey(t, []string{"T1", "T2"}, key, func(i int, name string, v any) {
			if v != now {
				t.Errorf("expected %v, got %v", now, v)
			}
		})
	})

	t.Run("unsupported_type", func(t *testing.T) {
		key := dalrecord.NewKeyWithID("users", 1.23)
		err := processPrimaryKey([]string{"K1", "K2"}, key, func(i int, name string, v any) {
			t.Errorf("called for column %d", i)
		})
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("error = %v, want one wrapping dal.ErrNotSupported", err)
		}
	})
}

// An insert with a key ID has to write the ID to a column, so a recordset with
// no primary key configured is an error, and no statement is built.
func TestBuildSingleRecordQuery_InsertWithAKeyIDAndNoPrimaryKeyIsAnError(t *testing.T) {
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &user2{Name: "John"})
	q, err := buildSingleRecordQuery(insertOperation, DbOptions{}, record)
	if err == nil || !strings.Contains(err.Error(), "primary key is not defined for recordset users") {
		t.Errorf("error = %v, want one saying the primary key of recordset users is not defined", err)
	}
	if q.text != "" || q.args != nil {
		t.Errorf("an error left a statement behind: %+v", q)
	}
}
