package dalgo2sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
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
