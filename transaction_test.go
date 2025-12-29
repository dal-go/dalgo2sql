package dalgo2sql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

type user2 struct {
	Name string
}

func TestTransaction(t *testing.T) {
	ctx := context.Background()

	t.Run("Insert", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
			return tx.Insert(ctx, record)
		})
	})

	t.Run("InsertMulti", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(2, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			records := []dal.Record{
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"}),
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u2"), &user2{Name: "Jane"}),
			}
			return tx.InsertMulti(ctx, records)
		})
	})

	t.Run("Update", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("UPDATE users SET").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Update(ctx, dal.NewKeyWithID("users", "u1"), []update.Update{
				update.ByFieldName("Name", "John"),
			})
		})
	})

	t.Run("UpdateMulti", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("UPDATE users SET").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("UPDATE users SET").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.UpdateMulti(ctx, []*dal.Key{
				dal.NewKeyWithID("users", "u1"),
				dal.NewKeyWithID("users", "u2"),
			}, []update.Update{
				update.ByFieldName("Name", "John"),
			})
		})
	})

	t.Run("UpdateRecord", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("UPDATE users SET").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
			return tx.UpdateRecord(ctx, record, []update.Update{
				update.ByFieldName("Name", "John"),
			})
		})
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.DeleteMulti(ctx, []*dal.Key{dal.NewKeyWithID("users", "u1")})
		})
	})

	t.Run("Set", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WillReturnRows(sqlmock.NewRows([]string{"ID"})) // not exists
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
			return tx.Set(ctx, record)
		})
	})

	t.Run("SetMulti", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WillReturnRows(sqlmock.NewRows([]string{"ID"})) // not exists
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			records := []dal.Record{
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"}),
			}
			return tx.SetMulti(ctx, records)
		})
	})

	t.Run("Upsert", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer sqlDB.Close()
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WillReturnRows(sqlmock.NewRows([]string{"ID"})) // not exists
		mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		_ = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			record := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user2{Name: "John"})
			return tx.(interface {
				Upsert(ctx context.Context, record dal.Record) error
			}).Upsert(ctx, record)
		})
	})

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
