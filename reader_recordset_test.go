package dalgo2sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestGetRecordsetReader_Binary(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

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
