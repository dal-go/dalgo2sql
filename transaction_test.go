package dalgo2sql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestTransaction(t *testing.T) {
	ctx := context.Background()

	t.Run("Select", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT id FROM users").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectCommit()

		err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			q := dal.NewTextQuery("SELECT id FROM users", nil)
			reader, err := tx.(interface {
				Select(ctx context.Context, query dal.Query) (dal.Reader, error)
			}).Select(ctx, q)
			if err != nil {
				return err
			}
			if reader == nil {
				t.Fatal("expected reader, got nil")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ExecuteQueryToRecordsetReader", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT id FROM users").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		mock.ExpectCommit()

		err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			q := dal.NewTextQuery("SELECT id FROM users", nil)
			reader, err := tx.ExecuteQueryToRecordsetReader(ctx, q)
			if err != nil {
				return err
			}
			if reader == nil {
				t.Fatal("expected reader, got nil")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT Name FROM users WHERE ID = ?").WithArgs("id1").WillReturnRows(sqlmock.NewRows([]string{"Name"}).AddRow("u1"))
		mock.ExpectCommit()

		err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			u := user{}
			record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &u)
			return tx.Get(ctx, record)
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
