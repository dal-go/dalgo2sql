package dalgo2sql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func newUsersDatabaseWithMock(t *testing.T) (*database, sqlmock.Sqlmock, func()) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
		},
	}).(*database)
	return db, mock, func() { closeDatabase(t, sqlDB) }
}

func newIncompleteUserRecord() dalrecord.Record {
	return dalrecord.NewRecordWithData(dalrecord.NewIncompleteKey("users", reflect.String, nil), &user{Name: "u1"})
}

func TestInsertWithIDGenerator(t *testing.T) {
	ctx := context.Background()

	t.Run("generates_id_on_first_attempt", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"1"})) // no rows => ID is free
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs(sqlmock.AnyArg(), "u1").
			WillReturnResult(sqlmock.NewResult(1, 1))

		record := newIncompleteUserRecord()
		if err := db.Insert(ctx, record, dal.WithRandomStringKey(10, 5)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id, ok := record.Key().ID.(string); !ok || len(id) != 10 {
			t.Fatalf("expected generated string ID of length 10, got: %v", record.Key().ID)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("retries_on_taken_id", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1)) // 1st generated ID is taken
		mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"1"})) // 2nd generated ID is free
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs(sqlmock.AnyArg(), "u1").
			WillReturnResult(sqlmock.NewResult(1, 1))

		record := newIncompleteUserRecord()
		if err := db.Insert(ctx, record, dal.WithRandomStringKey(10, 5)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("exhausts_attempts_when_all_ids_taken", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		for i := 0; i < maxIDGenerationAttempts; i++ {
			mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1)) // every generated ID is taken
		}

		record := newIncompleteUserRecord()
		err := db.Insert(ctx, record, dal.WithRandomStringKey(10, 5))
		if !errors.Is(err, dal.ErrExceedsMaxNumberOfAttempts) {
			t.Fatalf("expected ErrExceedsMaxNumberOfAttempts, got: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("returns_existence_check_error", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		existsErr := errors.New("stub: failed to check existence")
		mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
			WithArgs(sqlmock.AnyArg()).
			WillReturnError(existsErr)

		record := newIncompleteUserRecord()
		err := db.Insert(ctx, record, dal.WithRandomStringKey(10, 5))
		if !errors.Is(err, existsErr) {
			t.Fatalf("expected error wrapping existence check error, got: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("adapter_generated_id_falls_back_to_random_string", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"1"}))
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs(sqlmock.AnyArg(), "u1").
			WillReturnResult(sqlmock.NewResult(1, 1))

		record := newIncompleteUserRecord()
		if err := db.Insert(ctx, record, dal.WithAdapterGeneratedID()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id, ok := record.Key().ID.(string); !ok || len(id) != dal.DefaultRandomStringIDLength {
			t.Fatalf("expected generated string ID of length %d, got: %v", dal.DefaultRandomStringIDLength, record.Key().ID)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("insert_multi_in_transaction_with_id_generator", func(t *testing.T) {
		db, mock, close := newUsersDatabaseWithMock(t)
		defer close()

		mock.ExpectBegin()
		for i := 0; i < 2; i++ {
			mock.ExpectQuery("SELECT 1 FROM users WHERE ID = ?").
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"1"}))
			mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
				WithArgs(sqlmock.AnyArg(), "u1").
				WillReturnResult(sqlmock.NewResult(int64(i+1), 1))
		}
		mock.ExpectCommit()

		records := []dalrecord.Record{newIncompleteUserRecord(), newIncompleteUserRecord()}
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.InsertMulti(ctx, records, dal.WithRandomStringKey(10, 5))
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, record := range records {
			if id, ok := record.Key().ID.(string); !ok || len(id) != 10 {
				t.Fatalf("expected record %d to get generated 10-char ID, got: %v", i, record.Key().ID)
			}
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}
