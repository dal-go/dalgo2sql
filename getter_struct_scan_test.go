package dalgo2sql

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type scanGetCity struct {
	Name       string
	Population int
	AreaSqKm   int
	Tagged     string `db:"label"`
}

func TestScanIntoData_StructColumnNamesIgnoreCase(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	query := func(rows *sqlmock.Rows) {
		mock.ExpectQuery("SELECT").WillReturnRows(rows)
	}
	t.Run("matches by case, snake_case and db tag", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name", "population", "area_sq_km", "label"}).AddRow([]byte("Tokyo"), int64(1), int64(2), "l"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		var got scanGetCity
		if err := scanIntoData(rows, &got, false); err != nil {
			t.Fatal(err)
		}
		if want := (scanGetCity{Name: "Tokyo", Population: 1, AreaSqKm: 2, Tagged: "l"}); got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("rejects a column without a field", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name", "unknown"}).AddRow("Tokyo", 1))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil || !strings.Contains(err.Error(), "no corresponding field") {
			t.Errorf("got %v", err)
		}
	})
	t.Run("scan error without a current row", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil {
			t.Error("want error")
		}
	})
	t.Run("columns error on closed rows", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		_ = rows.Close()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil {
			t.Error("want error")
		}
	})
	t.Run("non-struct targets still use scany", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		var name string
		if err := scanIntoData(rows, &name, false); err != nil || name != "Tokyo" {
			t.Errorf("got %q, %v", name, err)
		}
	})
}
