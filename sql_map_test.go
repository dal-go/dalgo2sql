package dalgo2sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestBuildSingleRecordQuery_Map(t *testing.T) {
	t.Run("insert_map_sorted_keys", func(t *testing.T) {
		// Use an incomplete key (no ID) so PK-injection branch is skipped,
		// allowing assertion of pure sorted-key ordering from the map.
		data := map[string]any{"col_b": 42, "col_a": "x"}
		record := dal.NewRecordWithData(dal.NewIncompleteKey("users", reflect.String, nil), data)
		q := buildSingleRecordQuery(insertOperation, DbOptions{}, record)
		const want = "INSERT INTO users(col_a, col_b) VALUES (?, ?)"
		if q.text != want {
			t.Errorf("unexpected SQL:\n got: %q\nwant: %q", q.text, want)
		}
		if len(q.args) != 2 || q.args[0] != "x" || q.args[1] != 42 {
			t.Errorf("unexpected args: %v", q.args)
		}
	})

	t.Run("insert_map_with_pk", func(t *testing.T) {
		data := map[string]any{"Name": "John", "Age": 30}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), data)
		q := buildSingleRecordQuery(insertOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}, record)
		const want = "INSERT INTO users(ID, Age, Name) VALUES (?, ?, ?)"
		if q.text != want {
			t.Errorf("unexpected SQL:\n got: %q\nwant: %q", q.text, want)
		}
		if len(q.args) != 3 || q.args[0] != "id1" || q.args[1] != 30 || q.args[2] != "John" {
			t.Errorf("unexpected args: %v", q.args)
		}
	})

	t.Run("insert_map_skips_pk_key_in_data", func(t *testing.T) {
		// "ID" appears both as PK and as a data key; the data entry must be skipped.
		data := map[string]any{"ID": "should-be-ignored", "Name": "John"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), data)
		q := buildSingleRecordQuery(insertOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}, record)
		const want = "INSERT INTO users(ID, Name) VALUES (?, ?)"
		if q.text != want {
			t.Errorf("unexpected SQL:\n got: %q\nwant: %q", q.text, want)
		}
		if len(q.args) != 2 || q.args[0] != "id1" || q.args[1] != "John" {
			t.Errorf("unexpected args: %v", q.args)
		}
	})

	t.Run("update_map_sorted_set", func(t *testing.T) {
		data := map[string]any{"col_b": 42, "col_a": "x"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), data)
		q := buildSingleRecordQuery(updateOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}, record)
		// Note: existing struct path also produces a double space after "SET ".
		const want = "UPDATE users SET  col_a = ?, col_b = ? WHERE ID = ?"
		if q.text != want {
			t.Errorf("unexpected SQL:\n got: %q\nwant: %q", q.text, want)
		}
		if len(q.args) != 3 || q.args[0] != "x" || q.args[1] != 42 || q.args[2] != "id1" {
			t.Errorf("unexpected args: %v", q.args)
		}
	})

	t.Run("non_string_key_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for non-string map keys")
			}
		}()
		data := map[int]any{1: "x"}
		record := dal.NewRecordWithData(dal.NewIncompleteKey("users", reflect.String, nil), data)
		buildSingleRecordQuery(insertOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}, record)
	})

	t.Run("unsupported_kind_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for unsupported data kind")
			}
		}()
		data := 42
		record := dal.NewRecordWithData(dal.NewIncompleteKey("users", reflect.String, nil), &data)
		buildSingleRecordQuery(insertOperation, DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}, record)
	})
}

func TestInserter_MapData(t *testing.T) {
	ctx := context.Background()

	t.Run("Insert_map", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		data := map[string]any{"col_b": 42, "col_a": "x"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), data)

		mock.ExpectExec("INSERT INTO users(ID, col_a, col_b) VALUES (?, ?, ?)").
			WithArgs("id1", "x", 42).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := db.Insert(ctx, record); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations: %v", err)
		}
	})
}
