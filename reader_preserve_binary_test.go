package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func TestRecordsReaderPreservePostgresByteaIsOptIn(t *testing.T) {
	want := []byte{0x00, 0xff, 0x10}
	for _, tc := range []struct {
		name       string
		options    DbOptions
		want       any
		columnType string
	}{
		{name: "legacy default", want: string(want)},
		{name: "preserved", options: DbOptions{StructuredQueryDialect: dialectPostgres, PreserveBinaryValues: true}, want: want},
		{name: "opt-in does not change text", options: DbOptions{StructuredQueryDialect: dialectPostgres, PreserveBinaryValues: true}, want: string(want), columnType: "TEXT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(t, db)
			columnType := tc.columnType
			if columnType == "" {
				columnType = "BYTEA"
			}
			column := sqlmock.NewColumn("value").OfType(columnType, []byte(nil))
			rowValue := any(want)
			if columnType == "BYTEA" {
				rowValue = []byte{0x00, 0xff, 0x10}
			}
			mock.ExpectQuery("SELECT value FROM values_table").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(rowValue))
			reader, err := getRecordsReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT value FROM values_table", nil), db.QueryContext, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Errorf("close reader: %v", err)
				}
			}()
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			got := record.Data().(map[string]any)["value"]
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("value = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecordsetReaderPreservesPostgresByteaWithOption(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("value").OfType("BYTEA", []byte(nil))
	mock.ExpectQuery("SELECT value FROM values_table").WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(column).AddRow([]byte{0x00, 0xff, 0x10}).AddRow([]byte{}).AddRow(nil),
	)
	reader, err := getRecordsetReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT value FROM values_table", nil), db.QueryContext, DbOptions{StructuredQueryDialect: dialectPostgres, PreserveBinaryValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()
	want := []any{[]byte{0x00, 0xff, 0x10}, []byte{}, nil}
	for i, expected := range want {
		row, rs, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		got, err := row.GetValueByIndex(0, rs)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("row %d value = %#v (%T), want %#v", i, got, got, expected)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPreservePostgresByteaMakesAnOwnedCopy(t *testing.T) {
	source := []byte{0x00, 0xff, 0x10}
	got, err := preservePostgresBytea(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 0x7f
	if !reflect.DeepEqual(got, []byte{0x00, 0xff, 0x10}) {
		t.Fatalf("preserved value = %#v, want an owned byte copy", got)
	}
	if got, err := preservePostgresBytea("already-decoded text"); err == nil || got != nil {
		t.Fatalf("non-byte driver value = %#v, %v; want refusal", got, err)
	}
}

func TestPostgresGetAndGetMultiPreserveByteaNullAndEmpty(t *testing.T) {
	options := DbOptions{
		StructuredQueryDialect: dialectPostgres,
		PreserveBinaryValues:   true,
		Placeholder:            PlaceholderDollar,
		Recordsets: map[string]*Recordset{
			"files": NewRecordset("files", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	t.Run("Get distinguishes binary, empty, and NULL", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, db)
		blob := sqlmock.NewColumn("blob").OfType("BYTEA", []byte(nil))
		for _, tc := range []struct {
			id   string
			in   any
			want any
		}{
			{id: "binary", in: []byte{0, 0xff, 0x10}, want: []byte{0, 0xff, 0x10}},
			{id: "empty", in: []byte{}, want: []byte{}},
			{id: "null", in: nil, want: nil},
		} {
			mock.ExpectQuery(`SELECT \* FROM files WHERE id = \$1`).WithArgs(tc.id).
				WillReturnRows(sqlmock.NewRowsWithColumnDefinition(blob).AddRow(tc.in))
			rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", tc.id), map[string]any{"blob": []byte{0xaa}})
			if err := getSingle(context.Background(), options, rec, db.QueryContext); err != nil {
				t.Fatalf("Get(%s): %v", tc.id, err)
			}
			got, exists := rec.Data().(map[string]any)["blob"]
			if tc.id == "null" {
				if exists {
					t.Fatalf("Get(null) left stale value %#v", got)
				}
				continue
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Get(%s) blob = %#v (%T), want %#v", tc.id, got, got, tc.want)
			}
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("GetMulti maps rows and preserves byte slices", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, db)
		id := sqlmock.NewColumn("id").OfType("TEXT", "")
		blob := sqlmock.NewColumn("blob").OfType("BYTEA", []byte(nil))
		mock.ExpectQuery(`SELECT \* FROM files WHERE id IN \(\$1, \$2\)`).WithArgs("one", "two").
			WillReturnRows(sqlmock.NewRowsWithColumnDefinition(id, blob).AddRow("two", []byte{0, 0xff, 0x10}).AddRow("one", []byte{}))
		records := []dalrecord.Record{
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", "one"), map[string]any{"blob": nil}),
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", "two"), map[string]any{"blob": nil}),
		}
		if err := getMulti(context.Background(), options, records, db.QueryContext); err != nil {
			t.Fatalf("GetMulti(): %v", err)
		}
		want := [][]byte{{}, {0, 0xff, 0x10}}
		for i, rec := range records {
			if got := rec.Data().(map[string]any)["blob"]; !reflect.DeepEqual(got, want[i]) {
				t.Fatalf("record %s blob = %#v (%T), want %#v", rec.Key().ID, got, got, want[i])
			}
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRecordsReaderPreservePostgresByteaRejectsNonByteDriverValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("value").OfType("BYTEA", []byte(nil))
	mock.ExpectQuery("SELECT value FROM values_table").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow("lossy"))
	reader, err := getRecordsReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT value FROM values_table", nil), db.QueryContext, DbOptions{StructuredQueryDialect: dialectPostgres, PreserveBinaryValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := reader.Next(); err == nil {
		t.Fatal("Next() accepted a non-byte value for BYTEA")
	}
}

func TestRecordsReaderPreservesByteaAlongsideExactNumericValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	binaryColumn := sqlmock.NewColumn("blob").OfType("BYTEA", []byte(nil))
	numericColumn := sqlmock.NewColumn("amount").OfType("NUMERIC", "")
	mock.ExpectQuery("SELECT blob, amount FROM values_table").WillReturnRows(
		sqlmock.NewRowsWithColumnDefinition(binaryColumn, numericColumn).AddRow([]byte{0, 0xff}, "12345678901234567890.1234"),
	)
	options := DbOptions{StructuredQueryDialect: dialectPostgres, PreserveBinaryValues: true, ExactNumericValues: true}
	reader, err := getRecordsReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT blob, amount FROM values_table", nil), db.QueryContext, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	values := record.Data().(map[string]any)
	if !reflect.DeepEqual(values["blob"], []byte{0, 0xff}) || values["amount"] != "12345678901234567890.1234" {
		t.Fatalf("values = %#v, want exact bytea and numeric", values)
	}
}

func TestPreservePostgresByteaRequiresMapBackedRecords(t *testing.T) {
	options := DbOptions{
		StructuredQueryDialect: dialectPostgres,
		PreserveBinaryValues:   true,
		Placeholder:            PlaceholderDollar,
		Recordsets: map[string]*Recordset{
			"files": NewRecordset("files", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	newStructRecord := func(id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", id), &struct{ Blob []byte }{})
	}
	t.Run("Get refuses before SQL", func(t *testing.T) {
		reached := false
		err := getSingle(context.Background(), options, newStructRecord("one"), func(context.Context, string, ...any) (*sql.Rows, error) {
			reached = true
			return nil, errors.New("unexpected SQL")
		})
		if err == nil || !strings.Contains(err.Error(), "map record data") || reached {
			t.Fatalf("getSingle() = %v, SQL reached=%v; want map-only refusal before SQL", err, reached)
		}
	})
	t.Run("GetMulti refuses before SQL", func(t *testing.T) {
		reached := false
		err := getMulti(context.Background(), options, []dalrecord.Record{newStructRecord("one"), newStructRecord("two")}, func(context.Context, string, ...any) (*sql.Rows, error) {
			reached = true
			return nil, errors.New("unexpected SQL")
		})
		if err == nil || !strings.Contains(err.Error(), "map record data") || reached {
			t.Fatalf("getMulti() = %v, SQL reached=%v; want map-only refusal before SQL", err, reached)
		}
	})
	t.Run("structured query into struct refuses before SQL", func(t *testing.T) {
		query := dal.From(dal.NewRootCollectionRef("files", "")).NewQuery().SelectIntoRecord(func() dalrecord.Record {
			return newStructRecord("one")
		})
		reached := false
		_, err := getRecordsReaderWithOptions(context.Background(), query, func(context.Context, string, ...any) (*sql.Rows, error) {
			reached = true
			return nil, errors.New("unexpected SQL")
		}, options)
		if err == nil || !strings.Contains(err.Error(), "map record data") || reached {
			t.Fatalf("getRecordsReaderWithOptions() = %v, SQL reached=%v; want map-only refusal before SQL", err, reached)
		}
	})
}
