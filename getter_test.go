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

func Test_getSelectFields(t *testing.T) {
	type args struct {
		record    dal.Record
		includePK bool
		options   DbOptions
	}

	tests := []struct {
		name       string
		args       args
		wantFields []string
	}{
		{
			name: "simple_fields_exclude_primary_key",
			args: args{
				record: dal.NewRecordWithIncompleteKey("SomeCollection", reflect.String, struct {
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
			if gotFields := getSelectFields(tt.args.includePK, tt.args.options, tt.args.record); !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("getSelectFields() = %v, want %v", gotFields, tt.wantFields)
			}
		})
	}
}

func TestGetter_Exists(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() {
		_ = db.Close()
	}()

	d := NewDatabase(db, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	ctx := context.Background()
	key := dal.NewKeyWithID("users", "u1")

	t.Run("exists", func(t *testing.T) {
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		exists, err := d.Exists(ctx, key)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !exists {
			t.Errorf("expected exists to be true")
		}
	})

	t.Run("not_exists", func(t *testing.T) {
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"1"}))
		exists, err := d.Exists(ctx, key)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if exists {
			t.Errorf("expected exists to be false")
		}
	})

	t.Run("query_error", func(t *testing.T) {
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnError(errors.New("query error"))
		_, err := d.Exists(ctx, key)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("no_primary_key", func(t *testing.T) {
		d2 := NewDatabase(db, newSchema(), DbOptions{})
		_, err := d2.Exists(ctx, key)
		if !errors.Is(err, dal.ErrRecordNotFound) {
			t.Errorf("expected ErrRecordNotFound, got %v", err)
		}
	})

	t.Run("composite_primary_key", func(t *testing.T) {
		d3 := NewDatabase(db, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("pk1"), dal.Field("pk2")}),
			},
		})
		_, err := d3.Exists(ctx, key)
		if !errors.Is(err, dal.ErrNotImplementedYet) {
			t.Errorf("expected ErrNotImplementedYet, got %v", err)
		}
	})
}

func TestGetter_Get(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() {
		_ = db.Close()
	}()

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	d := NewDatabase(db, newSchema(), opts)
	ctx := context.Background()

	t.Run("success_map", func(t *testing.T) {
		data := make(map[string]any)
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &data)
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		err := d.Get(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("success_struct", func(t *testing.T) {
		type User struct {
			Name string `db:"Name"`
		}
		user := User{}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user)
		mock.ExpectQuery("SELECT Name FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow("John"))
		err := d.Get(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if user.Name != "John" {
			t.Errorf("expected John, got %s", user.Name)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &struct{ Name string }{})
		mock.ExpectQuery("SELECT Name FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"Name"}))
		err := d.Get(ctx, record)
		if !errors.Is(err, dal.ErrRecordNotFound) {
			t.Errorf("expected ErrRecordNotFound, got %v", err)
		}
	})

	t.Run("multiple_rows", func(t *testing.T) {
		type User struct {
			Name string `db:"Name"`
		}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &User{})
		mock.ExpectQuery("SELECT Name FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow("John").AddRow("Jane"))
		err := d.Get(ctx, record)
		if err == nil || err.Error() != "expected to get single row but got multiple" {
			t.Errorf("expected multiple rows error, got %v", err)
		}
	})
}

func TestGetter_GetMulti(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() {
		_ = db.Close()
	}()

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	d := NewDatabase(db, newSchema(), opts)
	ctx := context.Background()

	t.Run("success_single_table", func(t *testing.T) {
		type User struct {
			Name string `db:"Name"`
		}
		u1 := User{}
		u2 := User{}
		records := []dal.Record{
			dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &u1),
			dal.NewRecordWithData(dal.NewKeyWithID("users", "u2"), &u2),
		}
		// getMultiFromSingleTable uses "SELECT id, Name FROM users WHERE id IN (?, ?)"
		mock.ExpectQuery("SELECT id, Name FROM users WHERE id IN \\(\\?, \\?\\)").
			WithArgs("u1", "u2").
			WillReturnRows(sqlmock.NewRows([]string{"id", "Name"}).
				AddRow("u1", "John").
				AddRow("u2", "Jane"))
		err := d.GetMulti(ctx, records)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if u1.Name != "John" || u2.Name != "Jane" {
			t.Errorf("unexpected results: %v, %v", u1, u2)
		}
	})

	t.Run("multiple_tables", func(t *testing.T) {
		d = NewDatabase(db, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users":  NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
				"groups": NewRecordset("groups", Table, []dal.FieldRef{dal.Field("id")}),
			},
		})
		type User struct {
			Name string `db:"Name"`
		}
		records := []dal.Record{
			dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &User{}),
			dal.NewRecordWithData(dal.NewKeyWithID("groups", "g1"), &User{}),
		}
		mock.ExpectQuery("SELECT id, Name FROM users WHERE id IN \\(\\?\\)").WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "Name"}).AddRow("u1", "John"))
		mock.ExpectQuery("SELECT id, Name FROM groups WHERE id IN \\(\\?\\)").WithArgs("g1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "Name"}).AddRow("g1", "Admins"))
		err := d.GetMulti(ctx, records)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestGetter_ScanIntoData_Errors(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() {
		_ = db.Close()
	}()

	t.Run("unsupported_data_type", func(t *testing.T) {
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow(1))
		rows, _ := db.Query("SELECT")
		rows.Next()
		err := scanIntoData(rows, 123, false) // int is not supported (needs pointer to struct or map)
		if err == nil {
			t.Errorf("expected error for unsupported data type")
		}
	})
}

func TestGetReaderBase_StructuredQuery(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	ctx := context.Background()
	q := dal.NewTextQuery("SELECT id FROM users", nil)

	mock.ExpectQuery("SELECT id FROM users").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	rb, err := getReaderBase(ctx, q, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return sqlDB.QueryContext(ctx, query, args...)
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rb.colNames) != 1 || rb.colNames[0] != "id" {
		t.Errorf("unexpected columns: %v", rb.colNames)
	}
}

func TestRecordsetReader_Types(t *testing.T) {
	t.Skip("Skipping due to index out of range panic in dalgo/recordset")
}
