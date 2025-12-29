package dalgo2sql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestSetter(t *testing.T) {
	ctx := context.Background()

	t.Run("Set_Insert", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		u := user{Name: "u1"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &u)

		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WithArgs("id1").WillReturnRows(sqlmock.NewRows([]string{"ID"}))
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnResult(sqlmock.NewResult(1, 1))

		err = db.Set(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Set_Update", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		u := user{Name: "u1"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &u)

		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WithArgs("id1").WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow("id1"))
		mock.ExpectExec("UPDATE users SET Name = ? WHERE ID = ?").WithArgs("u1", "id1").WillReturnResult(sqlmock.NewResult(1, 1))

		err = db.Set(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SetMulti", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		records := []dal.Record{
			dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &user{Name: "u1"}),
		}

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WithArgs("id1").WillReturnRows(sqlmock.NewRows([]string{"ID"}))
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err = db.SetMulti(ctx, records)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("existsSingle_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		u := user{Name: "u1"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &u)

		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WithArgs("id1").WillReturnError(errors.New("exists error"))

		err = db.Set(ctx, record)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}
