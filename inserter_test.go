package dalgo2sql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

type user struct {
	Name string `db:"Name"`
}

func TestInserter(t *testing.T) {
	ctx := context.Background()

	t.Run("Insert", func(t *testing.T) {
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

		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs("id1", "u1").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err = db.Insert(ctx, record)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InsertMulti", func(t *testing.T) {
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

		records := []dal.Record{
			dal.NewRecordWithData(dal.NewKeyWithID("users", "id1"), &user{Name: "u1"}),
			dal.NewRecordWithData(dal.NewKeyWithID("users", "id2"), &user{Name: "u2"}),
		}

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id2", "u2").WillReturnResult(sqlmock.NewResult(2, 1))
		mock.ExpectCommit()

		err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.InsertMulti(ctx, records)
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
