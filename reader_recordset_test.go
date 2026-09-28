package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

func TestGetRecordsetReader_Binary(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer closeDatabase(t, db)

	ctx := context.Background()
	query := dal.NewTextQuery("SELECT blob_col FROM test_table", nil)

	t.Run("non-nullable-blob", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"blob_col"}).
			AddRow([]byte("hello"))

		mock.ExpectQuery(query.Text()).WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, query, db.QueryContext)
		if err != nil {
			t.Fatalf("failed to get recordset reader: %v", err)
		}
		defer func() { _ = rr.Close() }()

		row, rs, err := rr.Next()
		if err != nil {
			t.Fatalf("failed to get next row: %v", err)
		}

		val, err := row.GetValueByIndex(0, rs)
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}

		if !reflect.DeepEqual(val, []byte("hello")) {
			t.Errorf("expected []byte('hello'), got %T(%v)", val, val)
		}
	})

	t.Run("nullable-blob", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"blob_col"}).
			AddRow(nil)

		mock.ExpectQuery(query.Text()).WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, query, db.QueryContext)
		if err != nil {
			t.Fatalf("failed to get recordset reader: %v", err)
		}
		defer func() { _ = rr.Close() }()

		row, rs, err := rr.Next()
		if err != nil {
			t.Fatalf("failed to get next row: %v", err)
		}

		val, err := row.GetValueByIndex(0, rs)
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}

		if val != nil && !reflect.ValueOf(val).IsNil() {
			t.Errorf("expected nil, got %T(%v)", val, val)
		}
	})
}

type dummyEvaluator struct{}

func (dummyEvaluator) Eval(stored map[string]any) (any, error) { return "comp", nil }

func TestRecordsetReader_AdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	t.Run("nil_scan_type_and_null_byte", func(t *testing.T) {
		col1 := sqlmock.NewColumn("col_nil_scan")
		col2 := sqlmock.NewColumn("col_null_byte").OfType("BYTE", sql.NullByte{})
		rows := sqlmock.NewRowsWithColumnDefinition(col1, col2).AddRow("val", byte(10))
		smock.ExpectQuery("SELECT 1").WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT 1", nil), sdb.QueryContext)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer func() { _ = rr.Close() }()
		row, rs, err := rr.Next()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		_ = row
		_ = rs
	})

	t.Run("conversions", func(t *testing.T) {
		colFloat := sqlmock.NewColumn("f").OfType("FLOAT", float64(0))
		colInt := sqlmock.NewColumn("i").OfType("BIGINT", int64(0))
		colStr := sqlmock.NewColumn("s").OfType("VARCHAR", "")
		rows := sqlmock.NewRowsWithColumnDefinition(colFloat, colInt, colStr).
			AddRow(int(10), int(20), time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).
			AddRow(float32(1.5), int32(30), "str").
			AddRow(float64(2.5), float64(40), "str2")
		smock.ExpectQuery("SELECT 2").WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT 2", nil), sdb.QueryContext)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer func() { _ = rr.Close() }()
		for range 3 {
			_, _, err := rr.Next()
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		}
	})

	t.Run("scanValues_error", func(t *testing.T) {
		col := sqlmock.NewColumn("x").OfType("INT", 0)
		rows := sqlmock.NewRowsWithColumnDefinition(col).AddRow(1)
		smock.ExpectQuery("SELECT 3").WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT 3", nil), sdb.QueryContext)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer func() { _ = rr.Close() }()
		rr.scanColNames = append(rr.scanColNames, "extra")
		_, _, err = rr.Next()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("setValue_error", func(t *testing.T) {
		col := sqlmock.NewColumn("c").OfType("VARCHAR", "")
		rows := sqlmock.NewRowsWithColumnDefinition(col).AddRow("val")
		smock.ExpectQuery("SELECT 4").WillReturnRows(rows)

		rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT 4", nil), sdb.QueryContext)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		defer func() { _ = rr.Close() }()
		computed := recordset.NewComputedColumn("c", dummyEvaluator{})
		rr.rs = recordset.NewColumnarRecordset("test", computed)
		_, _, err = rr.Next()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
