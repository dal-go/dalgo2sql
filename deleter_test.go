package dalgo2sql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestDeleter(t *testing.T) {
	ctx := context.Background()

	t.Run("Delete", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		key := dal.NewKeyWithID("users", "u1")

		mock.ExpectExec("DELETE FROM users WHERE ID = ?").
			WithArgs("u1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = db.Delete(ctx, key)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Delete_with_options", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		rs := NewRecordset("users", Table, []dal.FieldRef{dal.Field("uid")})
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": rs,
			},
		}).(*database)
		key := dal.NewKeyWithID("users", "u1")

		mock.ExpectExec("DELETE FROM users WHERE uid = ?").
			WithArgs("u1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = db.Delete(ctx, key)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		keys := []*dal.Key{
			dal.NewKeyWithID("users", "u1"),
			dal.NewKeyWithID("users", "u2"),
		}

		// Currently deleteMulti calls deleteSingle for each key AND then deleteMultiInSingleTable
		// according to the implementation in deleter.go:55-62
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u2").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM users WHERE ID IN (?, ?)").WithArgs("u1", "u2").WillReturnResult(sqlmock.NewResult(0, 2))

		err = db.DeleteMulti(ctx, keys)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("DeleteMulti_different_tables", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		keys := []*dal.Key{
			dal.NewKeyWithID("users", "u1"),
			dal.NewKeyWithID("posts", "p1"),
		}

		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM posts WHERE ID = ?").WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 1))

		err = db.DeleteMulti(ctx, keys)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Delete_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		key := dal.NewKeyWithID("users", "u1")

		mock.ExpectExec("DELETE FROM users WHERE ID = ?").
			WithArgs("u1").
			WillReturnError(errors.New("delete error"))

		err = db.Delete(ctx, key)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("DeleteMulti_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		keys := []*dal.Key{
			dal.NewKeyWithID("users", "u1"),
			dal.NewKeyWithID("users", "u2"),
		}

		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnError(errors.New("delete error"))

		err = db.DeleteMulti(ctx, keys)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("Transaction_Delete", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})
		key := dal.NewKeyWithID("users", "u1")

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Delete(ctx, key)
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Transaction_DeleteMulti", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})
		keys := []*dal.Key{
			dal.NewKeyWithID("users", "u1"),
		}

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.DeleteMulti(ctx, keys)
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
