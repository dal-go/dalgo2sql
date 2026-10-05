package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type structWithIntAndString struct {
	ID   string
	Age  int
	Name string
}

type mockRecord struct {
	dalrecord.Record
	key  *dalrecord.Key
	data any
}

func (m mockRecord) Key() *dalrecord.Key             { return m.key }
func (m mockRecord) Data() any                       { return m.data }
func (m mockRecord) SetError(error) dalrecord.Record { return m }

func TestGetter_GetSelectFields_Panics(t *testing.T) {
	t.Run("nil_data", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil data")
			}
		}()
		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), nil)
		_, _ = getSelectFields(false, DbOptions{}, rec)
	})

	t.Run("nil_key_with_include_pk", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on nil key")
			}
		}()
		rec := mockRecord{key: nil, data: &struct{ Name string }{}}
		_, _ = getSelectFields(true, DbOptions{}, rec)
	})

	t.Run("empty_collection_with_include_pk", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on empty collection")
			}
		}()
		rec := mockRecord{key: &dalrecord.Key{}, data: &struct{ Name string }{}}
		_, _ = getSelectFields(true, DbOptions{}, rec)
	})
}

func TestGetter_GetSingle_Additional(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	t.Run("fieldsStr_empty_becomes_1", func(t *testing.T) {
		var val int
		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &val)
		smock.ExpectQuery("SELECT 1 FROM users WHERE ID = \\?").WithArgs("u1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		opts := DbOptions{Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")})}}
		err := getSingle(ctx, opts, rec, sdb.Query)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if val != 1 {
			t.Fatalf("expected val 1, got %d", val)
		}
	})

	t.Run("pk_len_0", func(t *testing.T) {
		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		opts := DbOptions{} // PrimaryKeyFieldNames returns nil
		err := getSingle(ctx, opts, rec, sdb.Query)
		if err == nil {
			t.Fatal("expected error for empty pk")
		}
	})

	t.Run("pk_len_greater_than_1", func(t *testing.T) {
		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		opts := DbOptions{Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id1"), dal.Field("id2")})}}
		err := getSingle(ctx, opts, rec, sdb.Query)
		if err == nil {
			t.Fatal("expected error for composite pk")
		}
	})

	t.Run("rowIntoRecord_error", func(t *testing.T) {
		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Age int }{})
		opts := DbOptions{Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")})}}
		// Return a string where int is expected for scanning into Age
		smock.ExpectQuery("SELECT Age FROM users WHERE ID = \\?").WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"Age"}).AddRow("not-an-int"))
		err := getSingle(ctx, opts, rec, sdb.Query)
		if err == nil {
			t.Fatal("expected rowIntoRecord error")
		}
	})
}

func TestGetter_GetMulti_Additional(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	t.Run("getMulti_error_from_table", func(t *testing.T) {
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &struct{ Name string }{})
		smock.ExpectQuery("SELECT ID, Name FROM users WHERE ID IN").WillReturnError(errors.New("query error"))
		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMulti(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err == nil {
			t.Fatal("expected error from getMulti")
		}
	})

	t.Run("getMultiFromSingleTable_empty", func(t *testing.T) {
		err := getMultiFromSingleTable(ctx, DbOptions{}, nil, sdb.Query)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("getMultiFromSingleTable_len_1", func(t *testing.T) {
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		smock.ExpectQuery("SELECT ID, Name FROM users WHERE ID = \\?").WithArgs().WillReturnRows(sqlmock.NewRows([]string{"ID", "Name"}).AddRow("u1", "Alice"))
		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1}, sdb.Query)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("getMultiFromSingleTable_composite_pk_is_an_error", func(t *testing.T) {
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &struct{ Name string }{})
		opts := DbOptions{PrimaryKey: []string{"p1", "p2"}}
		// No statement is expected: the mock fails the test of any it did not expect.
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if !errors.Is(err, dal.ErrNotImplementedYet) {
			t.Fatalf("error = %v, want one wrapping dal.ErrNotImplementedYet", err)
		}
	})

	t.Run("getMultiFromSingleTable_len_1_with_an_ID_that_does_not_fit_a_composite_pk", func(t *testing.T) {
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		opts := DbOptions{PrimaryKey: []string{"p1", "p2"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1}, sdb.Query)
		if !errors.Is(err, dal.ErrNotSupported) || !errors.Is(r1.Error(), dal.ErrNotSupported) {
			t.Fatalf("error = %v, record error = %v, want both to wrap dal.ErrNotSupported", err, r1.Error())
		}
	})

	t.Run("getMultiFromSingleTable_map_bytes_and_pointer_to_map", func(t *testing.T) {
		m1 := map[string]any{}
		m2 := map[string]any{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &m1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &m2)
		// return []byte for ID, and []byte for data col
		rows := sqlmock.NewRows([]string{"ID", "data"}).
			AddRow([]byte("u1"), []byte("payload1")).
			AddRow([]byte("u2"), "payload2")
		smock.ExpectQuery("SELECT \\* FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if m1["data"] != "payload1" {
			t.Fatalf("expected payload1, got %v", m1["data"])
		}
	})

	t.Run("getMultiFromSingleTable_struct_with_int_scan", func(t *testing.T) {
		s1 := structWithIntAndString{}
		s2 := structWithIntAndString{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &s1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &s2)

		rows := sqlmock.NewRows([]string{"ID", "ID", "Age", "Name"}).
			AddRow("u1", "u1", 25, "Alice").
			AddRow("u2", "u2", 30, "Bob")
		smock.ExpectQuery("SELECT ID, ID, Age, Name FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if s1.Age != 25 || s1.Name != "Alice" {
			t.Fatalf("unexpected struct s1: %+v", s1)
		}
	})

	t.Run("getMultiFromSingleTable_struct_scan_error", func(t *testing.T) {
		s1 := structWithIntAndString{}
		s2 := structWithIntAndString{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &s1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &s2)

		rows := sqlmock.NewRows([]string{"ID", "ID", "Age", "Name"}).
			AddRow("u1", "u1", "not-an-int", "Alice")
		smock.ExpectQuery("SELECT ID, ID, Age, Name FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err == nil {
			t.Fatal("expected scan error")
		}
	})

	t.Run("getMultiFromSingleTable_map_scan_error", func(t *testing.T) {
		// Cause columns or scan error on map path
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), make(map[string]any))
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), make(map[string]any))

		rows := sqlmock.NewRows([]string{"ID", "Name"}).AddRow("u1", "Alice")
		rows.RowError(0, errors.New("scan failure"))
		smock.ExpectQuery("SELECT \\* FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		_ = getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
	})

	t.Run("getMultiFromSingleTable_rows_Err_sqlErrNoRows", func(t *testing.T) {
		s1 := structWithIntAndString{}
		s2 := structWithIntAndString{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &s1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &s2)

		rows := sqlmock.NewRows([]string{"ID", "ID", "Age", "Name"}).
			AddRow("u1", "u1", 25, "Alice").
			RowError(0, sql.ErrNoRows)
		smock.ExpectQuery("SELECT ID, ID, Age, Name FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err != nil {
			t.Fatalf("expected nil error for ErrNoRows, got %v", err)
		}
	})

	t.Run("getMultiFromSingleTable_rows_Err_other", func(t *testing.T) {
		s1 := structWithIntAndString{}
		s2 := structWithIntAndString{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &s1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &s2)

		rows := sqlmock.NewRows([]string{"ID", "ID", "Age", "Name"}).
			AddRow("u1", "u1", 25, "Alice").
			RowError(0, errors.New("stream broken"))
		smock.ExpectQuery("SELECT ID, ID, Age, Name FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err == nil {
			t.Fatal("expected error from rows.Err()")
		}
	})

	t.Run("getMultiFromSingleTable_rowIntoRecord_error", func(t *testing.T) {
		s1 := structWithIntAndString{}
		s2 := struct {
			Age bool
		}{}
		r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &s1)
		r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &s2)

		rows := sqlmock.NewRows([]string{"ID", "ID", "Age", "Name"}).
			AddRow("u1", "u1", 25, "Alice").
			AddRow("u2", "u2", 30, "Bob")
		smock.ExpectQuery("SELECT ID, ID, Age, Name FROM users WHERE ID IN").WillReturnRows(rows)

		opts := DbOptions{PrimaryKey: []string{"ID"}}
		err := getMultiFromSingleTable(ctx, opts, []dalrecord.Record{r1, r2}, sdb.Query)
		if err == nil {
			t.Fatal("expected rowIntoRecord error for r2")
		}
	})
}

func TestGetter_ScanRowIntoMap_Errors(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	t.Run("columns_error_on_closed_rows", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id"})
		smock.ExpectQuery("SELECT 1").WillReturnRows(rows)
		r, _ := sdb.QueryContext(ctx, "SELECT 1")
		_ = r.Close()

		m := make(map[string]any)
		err := scanRowIntoMap(r, m, false)
		if err == nil {
			t.Fatal("expected error on closed rows")
		}
	})

	t.Run("scanIntoData_error", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id"}).AddRow("u1")
		smock.ExpectQuery("SELECT 2").WillReturnRows(rows)
		r, _ := sdb.QueryContext(ctx, "SELECT 2")
		defer func() { _ = r.Close() }()
		if !r.Next() {
			t.Fatal("expected row")
		}

		rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &struct{ Age int }{})
		err := rowIntoRecord(r, rec, false)
		if err == nil {
			t.Fatal("expected scan into data error")
		}
	})
}
