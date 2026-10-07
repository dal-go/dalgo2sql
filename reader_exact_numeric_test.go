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

func TestRecordsReaderExactNumericValuesPreservesTextAndLegacyDefault(t *testing.T) {
	const source = "12345678901234567890.1234500"
	for _, tc := range []struct {
		name    string
		options DbOptions
		want    any
	}{
		{name: "legacy", want: float64(12345678901234567000)},
		{name: "exact", options: DbOptions{ExactNumericValues: true}, want: source},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(t, db)
			column := sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0))
			mock.ExpectQuery("SELECT amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(source))
			reader, err := getRecordsReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT amount FROM invoices", nil), db.QueryContext, tc.options)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if got := record.Data().(map[string]any)["amount"]; got != tc.want {
				t.Fatalf("amount = %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecordsReaderExactNumericValuesRejectsFloatDriver(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0))
	mock.ExpectQuery("SELECT amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(float64(0.1)))
	reader, err := getRecordsReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT amount FROM invoices", nil), db.QueryContext, DbOptions{ExactNumericValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err == nil || !strings.Contains(err.Error(), "exact decimal text is required") {
		t.Fatalf("Next() error = %v, want refusal of lossy float driver value", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExactNumericValuePreservesSupportedDriverValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want any
	}{
		{name: "null", in: nil, want: nil},
		{name: "string", in: "NaN", want: "NaN"},
		{name: "bytes", in: []byte("-Infinity"), want: "-Infinity"},
		{name: "decimal", in: "12345678901234567890.1234500", want: "12345678901234567890.1234500"},
		{name: "int", in: int(1), want: "1"},
		{name: "int8", in: int8(2), want: "2"},
		{name: "int16", in: int16(3), want: "3"},
		{name: "int32", in: int32(4), want: "4"},
		{name: "int64", in: int64(5), want: "5"},
		{name: "uint", in: uint(6), want: "6"},
		{name: "uint8", in: uint8(7), want: "7"},
		{name: "uint16", in: uint16(8), want: "8"},
		{name: "uint32", in: uint32(9), want: "9"},
		{name: "uint64", in: uint64(10), want: "10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := exactNumericValue(tc.in)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("exactNumericValue(%T) = %#v, %v; want %#v", tc.in, got, err, tc.want)
			}
		})
	}
	for _, value := range []any{"not a number", []byte{0xff, 0xfe}} {
		if got, err := exactNumericValue(value); err == nil {
			t.Errorf("exactNumericValue(%#v) = %#v, want unsupported input error", value, got)
		}
	}
}

func TestRecordsetReaderExactNumericValuesUsesTextColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0))
	mock.ExpectQuery("SELECT amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow("12345678901234567890.1234500"))
	reader, err := getRecordsetReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT amount FROM invoices", nil), db.QueryContext, DbOptions{ExactNumericValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	row, rs, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	got, err := row.GetValueByIndex(0, rs)
	if err != nil {
		t.Fatal(err)
	}
	if got != "12345678901234567890.1234500" {
		t.Fatalf("recordset amount = %T(%v), want exact decimal text", got, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordsetReaderExactNumericValuesRejectsFloatDriver(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0))
	mock.ExpectQuery("SELECT amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(float64(0.1)))
	reader, err := getRecordsetReaderWithOptions(context.Background(), dal.NewTextQuery("SELECT amount FROM invoices", nil), db.QueryContext, DbOptions{ExactNumericValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, _, err := reader.Next(); err == nil || !strings.Contains(err.Error(), "exact decimal text is required") {
		t.Fatalf("Next() error = %v, want refusal of lossy float driver value", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExactNumericMapKeyReadPreservesText(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	const amount = "12345678901234567890.1234500"
	columns := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0)),
	).AddRow(int64(1), amount)
	mock.ExpectQuery("SELECT id, amount FROM invoices").WillReturnRows(columns)
	rows, err := db.QueryContext(context.Background(), "SELECT id, amount FROM invoices")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected row: %v", rows.Err())
	}
	data := map[string]any{}
	if err := scanRowIntoMapWithOptions(rows, data, true, DbOptions{ExactNumericValues: true}); err != nil {
		t.Fatal(err)
	}
	if got := data["amount"]; got != amount {
		t.Fatalf("amount = %T(%v), want exact decimal text", got, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExactNumericKeyReadRefusesStructTargetsBeforeQuery(t *testing.T) {
	type invoice struct{ Amount float64 }
	value := invoice{}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("invoices", int64(1)), &value)
	err := getSingle(context.Background(), DbOptions{ExactNumericValues: true, PrimaryKey: []string{"id"}}, rec, func(context.Context, string, ...any) (*sql.Rows, error) {
		t.Fatal("exact numeric struct key read must be refused before querying")
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "requires map record data") {
		t.Fatalf("Get error = %v, want map-target refusal", err)
	}
}

func TestExactNumericGetMultiRefusesStructTargetsBeforeQuery(t *testing.T) {
	type invoice struct{ Amount float64 }
	value := invoice{}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("invoices", int64(1)), &value)
	err := getMulti(context.Background(), DbOptions{ExactNumericValues: true, PrimaryKey: []string{"id"}}, []dalrecord.Record{rec}, func(context.Context, string, ...any) (*sql.Rows, error) {
		t.Fatal("exact numeric GetMulti must refuse struct targets before querying")
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "requires map record data") {
		t.Fatalf("GetMulti error = %v, want map-target refusal", err)
	}
}

func TestExactNumericStructuredQueryRefusesIntoStructBeforeQuery(t *testing.T) {
	queried := false
	_, err := getRecordsReaderWithOptions(context.Background(), structTargetQuery(), func(context.Context, string, ...any) (*sql.Rows, error) {
		queried = true
		return nil, errors.New("query should not be executed")
	}, DbOptions{ExactNumericValues: true})
	if err == nil || !strings.Contains(err.Error(), "requires map record data") {
		t.Fatalf("reader creation error = %v, want map-target refusal", err)
	}
	if queried {
		t.Fatal("struct target was refused only after query execution")
	}
}

func TestExactNumericMapScannerRejectsFloatDriver(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	column := sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0))
	mock.ExpectQuery("SELECT amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(float64(0.1)))
	rows, err := db.QueryContext(context.Background(), "SELECT amount FROM invoices")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected row: %v", rows.Err())
	}
	err = scanRowIntoMapWithOptions(rows, map[string]any{}, false, DbOptions{ExactNumericValues: true})
	if err == nil || !strings.Contains(err.Error(), "column \"amount\"") {
		t.Fatalf("scan error = %v, want exact NUMERIC column error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExactNumericFillMapRecordsRejectsFloatPrimaryKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	mock.ExpectQuery("SELECT id FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("NUMERIC", float64(0))).AddRow(float64(1)))
	rows, err := db.QueryContext(context.Background(), "SELECT id FROM invoices")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	_, err = fillMapRecords(rows, columns, types, nil, "id", DbOptions{ExactNumericValues: true})
	if err == nil || !strings.Contains(err.Error(), `column "id"`) {
		t.Fatalf("fillMapRecords error = %v, want exact primary key refusal", err)
	}
}

func TestExactNumericFillMapRecordsRejectsFloatValue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	mock.ExpectQuery("SELECT id, amount FROM invoices").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("amount").OfType("NUMERIC", float64(0)),
	).AddRow(int64(1), float64(0.1)))
	rows, err := db.QueryContext(context.Background(), "SELECT id, amount FROM invoices")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("invoices", int64(1)), map[string]any{})
	_, err = fillMapRecords(rows, columns, types, []dalrecord.Record{rec}, "id", DbOptions{ExactNumericValues: true})
	if err == nil || !strings.Contains(err.Error(), `column "amount"`) {
		t.Fatalf("fillMapRecords error = %v, want exact amount refusal", err)
	}
}
