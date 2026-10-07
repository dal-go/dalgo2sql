package dalgo2sql

import (
	"context"
	"database/sql"
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
