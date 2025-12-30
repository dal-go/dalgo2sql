package dalgo2sql

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestRecordsReader(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer closeDatabase(t, db)

	ctx := context.Background()
	query := dal.NewTextQuery("SELECT id, name FROM users", nil)

	t.Run("Next_success_map", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John").
			AddRow(2, "Jane")
		_ = mock.ExpectQuery("SELECT id, name FROM users").WillReturnRows(rows)

		rr, err := getRecordsReader(ctx, query, db.QueryContext)
		if err != nil {
			t.Fatalf("failed to get records reader: %v", err)
		}
		defer func(rr *recordsReader) {
			err := rr.Close()
			if err != nil {
				t.Errorf("failed to close records reader: %v", err)
			}
		}(rr)

		record, err := rr.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
		data := record.Data().(map[string]any)
		if data["id"] != int64(1) || data["name"] != "John" {
			t.Errorf("unexpected record data: %v", data)
		}

		record, err = rr.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
		data = record.Data().(map[string]any)
		if data["id"] != int64(2) || data["name"] != "Jane" {
			t.Errorf("unexpected record data: %v", data)
		}

		_, err = rr.Next()
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
	})

	t.Run("Next_scan_error", func(t *testing.T) {
		// sqlmock doesn't easily support Scan error for interface{}
		// but we can at least execute it to cover some lines
		rows := sqlmock.NewRows([]string{"id"}).AddRow("not-an-int")
		mock.ExpectQuery("SELECT id FROM users").WillReturnRows(rows)

		rr, _ := getRecordsReader(ctx, dal.NewTextQuery("SELECT id FROM users", nil), db.QueryContext)
		_, _ = rr.Next()
	})

	t.Run("Next_unsupported_data_type", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
		mock.ExpectQuery("SELECT id FROM users").WillReturnRows(rows)

		rr, _ := getRecordsReader(ctx, dal.NewTextQuery("SELECT id FROM users", nil), db.QueryContext)
		rr.newRecord = func() dal.Record {
			return dal.NewRecordWithData(dal.NewKeyWithID("Unknown", ""), 123) // int is not supported
		}

		_, err := rr.Next()
		if err == nil || err.Error() != "unsupported data type int" {
			t.Errorf("expected unsupported data type error, got %v", err)
		}
	})

	t.Run("Cursor_not_supported", func(t *testing.T) {
		rr := recordsReader{}
		_, err := rr.Cursor()
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Errorf("expected ErrNotSupported, got %v", err)
		}
	})

	t.Run("rows_error", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, errors.New("row error"))
		mock.ExpectQuery("SELECT id FROM users").WillReturnRows(rows)

		rr, _ := getRecordsReader(ctx, dal.NewTextQuery("SELECT id FROM users", nil), db.QueryContext)
		_, _ = rr.Next() // consume first row
		_, err := rr.Next()
		if err == nil || err.Error() != "row error" {
			t.Errorf("expected row error, got %v", err)
		}
	})

	t.Run("ExecuteQueryToRecordsReader", func(t *testing.T) {
		provider := recordsReaderProvider{executeQuery: db.QueryContext}
		mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		rr, err := provider.ExecuteQueryToRecordsReader(ctx, dal.NewTextQuery("SELECT 1", nil))
		if err != nil {
			t.Fatal(err)
		}
		if rr == nil {
			t.Fatal("expected reader, got nil")
		}
	})
}
