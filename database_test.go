package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func newSchema() dal.Schema {
	keyToField := func(key *dal.Key, data any) (fields []dal.ExtraField, err error) {
		return
	}
	dataToKey := func(incompleteKey *dal.Key, data any) (key *dal.Key, err error) {
		return
	}
	return dal.NewSchema(keyToField, dataToKey)
}

func TestField_String(t *testing.T) {
	f := Field{Name: "test"}
	if f.String() != "test" {
		t.Errorf("expected test, got %s", f.String())
	}
}

func TestRecordset(t *testing.T) {
	pkFields := []dal.FieldRef{dal.Field("id")}
	rs := NewRecordset("test_table", Table, pkFields)

	if rs.Name() != "test_table" {
		t.Errorf("expected test_table, got %s", rs.Name())
	}
	if rs.Type() != Table {
		t.Errorf("expected Table, got %v", rs.Type())
	}
	pk := rs.PrimaryKey()
	if len(pk) != 1 || pk[0].Name() != "id" {
		t.Errorf("unexpected primary key: %v", pk)
	}
	pkNames := rs.PrimaryKeyFieldNames()
	if len(pkNames) != 1 || pkNames[0] != "id" {
		t.Errorf("unexpected primary key names: %v", pkNames)
	}

	var nilRS *Recordset
	if nilRS.PrimaryKey() != nil {
		t.Errorf("expected nil for nil recordset PrimaryKey")
	}
}

func TestDatabase_Basics(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	schema := newSchema()
	d := NewDatabase(db, schema, DbOptions{ID: "test-db"})

	if d.ID() != "test-db" {
		t.Errorf("expected test-db, got %s", d.ID())
	}
	if d.Adapter().Name() != "dalgo2sql" {
		t.Errorf("expected dalgo2sql, got %s", d.Adapter().Name())
	}
	if d.Schema() != schema {
		t.Errorf("expected schema to match")
	}
}

func TestOptions_PrimaryKeyFieldNames(t *testing.T) {
	rs := NewRecordset("table1", Table, []dal.FieldRef{dal.Field("pk1"), dal.Field("pk2")})
	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"table1": rs,
		},
	}

	key := dal.NewKeyWithID("table1", "1")
	pkNames := opts.PrimaryKeyFieldNames(key)
	if len(pkNames) != 2 || pkNames[0] != "pk1" || pkNames[1] != "pk2" {
		t.Errorf("unexpected pk names: %v", pkNames)
	}

	key2 := dal.NewKeyWithID("unknown", "1")
	if opts.PrimaryKeyFieldNames(key2) != nil {
		t.Errorf("expected nil for unknown recordset")
	}
}

func newDatabase() (sqlDB *sql.DB, mock sqlmock.Sqlmock, db *database, closer func(), err error) {
	sqlDB, mock, err = sqlmock.New()
	db = NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
	closer = func() {
		_ = sqlDB.Close()
	}
	return
}

func TestDatabase_RunReadonlyTransaction(t *testing.T) {
	_, mock, db, closer, err := newDatabase()
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectCommit().WillReturnError(nil)
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(errors.New("begin error"))
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return nil
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("readonly_driver_unsupported", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(errors.New("sql: driver does not support read-only transactions"))
		mock.ExpectBegin().WillReturnError(nil) // second attempt without readonly
		mock.ExpectCommit().WillReturnError(nil)
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !db.onlyReadWriteTx {
			t.Errorf("expected onlyReadWriteTx to be true")
		}
	})

	t.Run("worker_error_rollback_success", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectRollback().WillReturnError(nil)
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return errors.New("worker error")
		})
		if err == nil || err.Error() != "worker error" {
			t.Errorf("expected worker error, got %v", err)
		}
	})

	t.Run("worker_error_rollback_error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectRollback().WillReturnError(errors.New("rollback error"))
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return errors.New("worker error")
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectCommit().WillReturnError(errors.New("commit error"))
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return nil
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("missing_readonly_option", func(t *testing.T) {
		// RunReadonlyTransaction forces dal.TxWithReadonly()
		// so dalgoTxOptions.IsReadonly() will always be true.
		// To test the "else" branch, we'db need to bypass RunReadonlyTransaction
		// or change its implementation.
	})
}

func TestDatabase_RunReadwriteTransaction(t *testing.T) {
	_, mock, d, closer, err := newDatabase()
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectCommit().WillReturnError(nil)
		err := d.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("readonly_option_error", func(t *testing.T) {
		err := d.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return nil
		}, dal.TxWithReadonly())
		if err == nil {
			t.Errorf("expected error when passing readonly=true to RunReadwriteTransaction")
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(errors.New("begin error"))
		err := d.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return nil
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("worker_error_rollback_error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(nil)
		mock.ExpectRollback().WillReturnError(errors.New("rollback error"))
		err := d.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return errors.New("worker error")
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

func TestDatabase_GetReader(t *testing.T) {
	db, mock, _ := sqlmock.New()
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		q := dal.NewTextQuery("SELECT id FROM users", nil)
		reader, err := getReader(ctx, q, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return db.QueryContext(ctx, query, args...)
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		reader.newRecord = func() dal.Record {
			return dal.NewRecordWithData(dal.NewKeyWithID("users", 1), make(map[string]any))
		}
		record, err := reader.Next()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if record.Data().(map[string]any)["id"] != int64(1) {
			t.Errorf("expected 1, got %v", record.Data().(map[string]any)["id"])
		}
	})
}

func Test_simpleKeyToFields_Additional(t *testing.T) {
	t.Run("invalid_reflect_value", func(t *testing.T) {
		f := simpleKeyToFields("ID")
		key := dal.NewKeyWithID("users", 123)
		// We need to pass something that reflect.ValueOf(data) returns an invalid value.
		// Actually reflect.ValueOf(nil) is invalid, but the code checks data == nil before that.
		// Wait, if I pass a nil pointer?
		var p *int
		fields, err := f(key, p)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fields) != 1 || fields[0].Name() != "ID" {
			t.Errorf("expected 1 field ID, got %v", fields)
		}
	})

	t.Run("SetID_pointer_receiver_on_pointer", func(t *testing.T) {
		f := simpleKeyToFields("ID")
		key := dal.NewKeyWithID("users", 123)
		data := &withSetIDPtr{}
		fields, err := f(key, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fields) != 0 {
			t.Errorf("expected 0 fields because SetID exists, got %d", len(fields))
		}
	})

	t.Run("SetID_value_receiver_on_pointer", func(t *testing.T) {
		f := simpleKeyToFields("ID")
		key := dal.NewKeyWithID("users", 123)
		data := &withSetIDValue{}
		fields, err := f(key, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fields) != 0 {
			t.Errorf("expected 0 fields because SetID exists, got %d", len(fields))
		}
	})

	t.Run("struct_field_unexported", func(t *testing.T) {
		f := simpleKeyToFields("id")
		key := dal.NewKeyWithID("users", 123)
		data := struct{ id int }{id: 1}
		fields, err := f(key, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fields) != 1 || fields[0].Name() != "id" {
			t.Errorf("expected 1 field id (because struct field is unexported), got %v", fields)
		}
	})
}

func Test_simpleFieldsToKey_Additional(t *testing.T) {
	t.Run("GetID_value_receiver_on_pointer", func(t *testing.T) {
		schema := NewSimpleSchema("ID")
		incomplete := dal.NewIncompleteKey("users", reflect.Int64, nil)
		data := &getIDVal{}
		key, err := schema.DataToKey(incomplete, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key.ID != int64(101) {
			t.Errorf("expected 101, got %v", key.ID)
		}
	})

	t.Run("GetID_pointer_receiver_on_value", func(t *testing.T) {
		schema := NewSimpleSchema("ID")
		incomplete := dal.NewIncompleteKey("users", reflect.String, nil)
		// getIDPtr has pointer receiver. Passing a value.
		// reflect.ValueOf(getIDPtr{}).MethodByName("GetID") should be invalid.
		// But if it's addressable, it might work?
		// The code checks if it can create a pointer to it.
		data := getIDPtr{}
		key, err := schema.DataToKey(incomplete, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key.ID != "u-abc" {
			t.Errorf("expected u-abc, got %v", key.ID)
		}
	})
}
