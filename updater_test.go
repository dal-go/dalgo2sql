package dalgo2sql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

func TestUpdater(t *testing.T) {
	ctx := context.Background()

	t.Run("Update", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		key := dal.NewKeyWithID("users", "id1")
		updates := []update.Update{
			update.ByFieldName("Name", "new_name"),
		}

		mock.ExpectExec("UPDATE users SET\n\tName = ?\n\tWHERE ID = ?").
			WithArgs("new_name", "id1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = db.Update(ctx, key, updates)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("UpdateMulti", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		keys := []*dal.Key{
			dal.NewKeyWithID("users", "id1"),
			dal.NewKeyWithID("users", "id2"),
		}
		updates := []update.Update{
			update.ByFieldName("Name", "new_name"),
		}

		mock.ExpectExec("UPDATE users SET\n\tName = ?\n\tWHERE ID = ?").WithArgs("new_name", "id1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE users SET\n\tName = ?\n\tWHERE ID = ?").WithArgs("new_name", "id2").WillReturnResult(sqlmock.NewResult(0, 1))

		err = db.UpdateMulti(ctx, keys, updates)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestUpserter(t *testing.T) {
	ctx := context.Background()

	t.Run("Upsert", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)

		u := user{Name: "u1"}
		record := dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &u)

		mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").WithArgs("id1").WillReturnRows(sqlmock.NewRows([]string{"ID"}))
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnResult(sqlmock.NewResult(1, 1))

		err = db.Upsert(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
