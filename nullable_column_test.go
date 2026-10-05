package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestNullableColumn_HoldsNullsAndValues(t *testing.T) {
	column := newNullableColumn("n", int64(0), "BIGINT")
	if column.Name() != "n" || column.DbType() != "BIGINT" || column.IsBitmap() || column.ValueType() != reflect.TypeOf(int64(0)) {
		t.Errorf("column = %q, %q, bitmap %v, %v", column.Name(), column.DbType(), column.IsBitmap(), column.ValueType())
	}
	if got := column.DefaultValue(); got != int64(0) {
		t.Errorf("DefaultValue = %#v, want int64(0)", got)
	}
	for _, value := range []any{int64(5), nil, int64(0)} {
		if err := column.Add(value); err != nil {
			t.Fatalf("Add(%v): %v", value, err)
		}
	}
	want := []any{int64(5), nil, int64(0)}
	if got := column.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Values = %#v, want %#v", got, want)
	}
	// A cell is set again to a value, to NULL and back.
	for _, step := range []struct {
		row   int
		value any
	}{{0, nil}, {1, int64(9)}, {2, nil}, {0, int64(1)}} {
		if err := column.SetValue(step.row, step.value); err != nil {
			t.Fatalf("SetValue(%d, %v): %v", step.row, step.value, err)
		}
	}
	want = []any{int64(1), int64(9), nil}
	if got := column.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Values = %#v, want %#v", got, want)
	}
	// The slice Values returns is a copy.
	column.Values()[0] = "changed"
	if got, _ := column.GetValue(0); got != int64(1) {
		t.Errorf("GetValue(0) = %#v after the copy was changed, want 1", got)
	}
}

func TestNullableColumn_RefusesWhatItCannotHold(t *testing.T) {
	column := newNullableColumn("n", int64(0), "")
	if err := column.Add(int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := column.Add("secret cell"); err == nil || !strings.Contains(err.Error(), "string") || strings.Contains(err.Error(), "secret cell") {
		t.Errorf("Add of a string = %v, want an error that names the type and not the value", err)
	}
	if got := column.Values(); len(got) != 1 {
		t.Errorf("a refused Add left %d cells, want 1", len(got))
	}
	if err := column.SetValue(0, "secret cell"); err == nil || strings.Contains(err.Error(), "secret cell") {
		t.Errorf("SetValue of a string = %v, want an error that does not carry the value", err)
	}
	for _, row := range []int{-1, 1} {
		if err := column.SetValue(row, int64(2)); err == nil {
			t.Errorf("SetValue(%d) = nil, want an error: the column has one row", row)
		}
		if got, err := column.GetValue(row); err == nil || got != nil {
			t.Errorf("GetValue(%d) = %v, %v, want nil and an error: the column has one row", row, got, err)
		}
	}
	if got, err := column.GetValue(0); err != nil || got != int64(1) {
		t.Errorf("GetValue(0) = %v, %v after refused calls, want 1", got, err)
	}
}

// A driver hands the reader an integer as an int64, and the reader narrows it to the column
// of a smaller integer type when the column holds it; one it does not hold is an error that
// is returned from Next, where the recordset's own column used to panic.
func TestRecordsetReader_AValueThatDoesNotFitItsColumnIsAnError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	mock.ExpectQuery("SELECT narrow").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("a").OfType("SMALLINT", sql.NullInt16{}),
		sqlmock.NewColumn("b").OfType("INT", sql.NullInt32{}),
	).AddRow(int64(32767), int64(-2147483648)).AddRow(int64(32768), int64(1)).AddRow("text", int64(2)))
	reader, err := getRecordsetReader(context.Background(), dal.NewTextQuery("SELECT narrow", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	row, rs, err := reader.Next()
	if err != nil {
		t.Fatalf("a number the column holds: %v", err)
	}
	if got, _ := row.GetValueByName("a", rs); got != int16(32767) {
		t.Errorf("a = %#v, want int16(32767)", got)
	}
	if got, _ := row.GetValueByName("b", rs); got != int32(-2147483648) {
		t.Errorf("b = %#v, want int32(-2147483648)", got)
	}
	err = noPanic(t, func() error { _, _, err := reader.Next(); return err })
	if err == nil || !strings.Contains(err.Error(), "column a") || strings.Contains(err.Error(), "32768") {
		t.Errorf("a number that does not fit = %v, want an error naming the column and not the number", err)
	}
	// A value of another type is refused as well, whatever its text.
	err = noPanic(t, func() error { _, _, err := reader.Next(); return err })
	if err == nil || !strings.Contains(err.Error(), "column a") || strings.Contains(err.Error(), "text") {
		t.Errorf("a string in an int16 column = %v, want an error naming the column and not the text", err)
	}
}
