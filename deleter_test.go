package dalgo2sql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

func TestDeleter(t *testing.T) {
	ctx := context.Background()

	t.Run("Delete", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		key := record.NewKeyWithID("users", "u1")

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
		defer closeDatabase(t, sqlDB)

		rs := NewRecordset("users", Table, []dal.FieldRef{dal.Field("uid")})
		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": rs,
			},
		})).(*database)
		key := record.NewKeyWithID("users", "u1")

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
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
			record.NewKeyWithID("users", "u2"),
		}

		// The keys of one recordset are deleted by one IN statement.
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
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
			record.NewKeyWithID("posts", "p1"),
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
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		key := record.NewKeyWithID("users", "u1")

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
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
			record.NewKeyWithID("users", "u2"),
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
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})
		key := record.NewKeyWithID("users", "u1")

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
		defer closeDatabase(t, sqlDB)

		db := NewDatabase(sqlDB, newSchema(), DbOptions{})
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
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

	t.Run("DeleteMulti_single_key_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
		}
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnError(errors.New("exec error"))
		err = db.DeleteMulti(ctx, keys)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("DeleteMulti_multi_table_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)

		db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{})).(*database)
		keys := []*record.Key{
			record.NewKeyWithID("users", "u1"),
			record.NewKeyWithID("posts", "p1"),
		}
		mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnError(errors.New("exec error"))
		err = db.DeleteMulti(ctx, keys)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

// Delete follows the declared primary key, as every other operation does. A recordset
// that is declared for the key with no primary key, or with several columns, is
// refused with the errors Update returns for it, before any statement. The column
// named ID is the default only where no recordset is declared for the key.
func TestDelete_FollowsTheDeclaredPrimaryKey(t *testing.T) {
	plain := validNames()
	operations := []struct {
		name string
		run  func(context.Context, keyPathAPI) error
	}{
		{"delete", func(ctx context.Context, api keyPathAPI) error { return api.Delete(ctx, plain.key("u1")) }},
		{"delete-multi, one key", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*record.Key{plain.key("u1")})
		}},
		{"delete-multi, several keys", func(ctx context.Context, api keyPathAPI) error { return api.DeleteMulti(ctx, plain.keys()) }},
		{"delete-multi, after a key of an undeclared recordset", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*record.Key{record.NewKeyWithID("accounts", "a1"), plain.key("u1")})
		}},
	}
	refused := []struct {
		name      string
		recordset *Recordset
		want      error  // the error the operation wraps, if any
		wantText  string // text of the error, the one Update returns
	}{
		{"declared with no primary key", NewRecordset("users", Table, nil), nil, "primary key is not defined for users"},
		{"declared with an empty primary key", NewRecordset("users", Table, []dal.FieldRef{}), nil, "primary key is not defined for users"},
		{"declared with two primary key fields", NewRecordset("users", Table, []dal.FieldRef{dal.Field("tenant"), dal.Field("id")}),
			dal.ErrNotImplementedYet, "composite primary key"},
	}
	for _, dialect := range []string{"", dialectSQLite} {
		for _, tt := range refused {
			t.Run(dialect+"/"+tt.name, func(t *testing.T) {
				options := plain.options(dialect)
				options.Recordsets["users"] = tt.recordset
				for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return true }) {
					t.Run(r.kind, func(t *testing.T) {
						// What Update says of the same recordset is what Delete says.
						updateErr := r.api.Update(context.Background(), plain.key("u1"), plain.updates())
						if updateErr == nil || !strings.Contains(updateErr.Error(), tt.wantText) {
							t.Fatalf("Update returned %v, want one that mentions %q", updateErr, tt.wantText)
						}
						for _, op := range operations {
							err := op.run(context.Background(), r.api)
							if calls := r.recorder.calls(); len(calls) != 0 {
								t.Fatalf("%s: a statement reached the database: %q", op.name, calls)
							}
							if err == nil || !strings.Contains(err.Error(), tt.wantText) {
								t.Fatalf("%s: error = %v, want one that mentions %q", op.name, err, tt.wantText)
							}
							if tt.want != nil && (!errors.Is(err, tt.want) || !errors.Is(updateErr, tt.want)) {
								t.Fatalf("%s: error = %v, Update's = %v, want both to wrap %v", op.name, err, updateErr, tt.want)
							}
						}
					})
				}
			})
		}
	}
}

// Where no recordset is declared for the key the column ID is the default, as ever: a
// caller deletes from a plain table with no configuration. A nil entry is no recordset.
func TestDelete_WithNoRecordsetDeclaredKeepsTheIDColumn(t *testing.T) {
	cases := []struct {
		name      string
		recordset map[string]*Recordset
	}{
		{"no recordsets", nil},
		{"other recordsets only", map[string]*Recordset{"posts": NewRecordset("posts", Table, []dal.FieldRef{dal.Field("pid")})}},
		{"a nil entry", map[string]*Recordset{"users": nil}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var sent []sentStatement
			options := DbOptions{Recordsets: tt.recordset}
			keys := []*record.Key{record.NewKeyWithID("users", "u1"), record.NewKeyWithID("users", "u2")}
			if err := deleteSingle(context.Background(), options, keys[0], recordingExecutor(&sent)); err != nil {
				t.Fatal(err)
			}
			if err := deleteMulti(context.Background(), options, keys, recordingExecutor(&sent)); err != nil {
				t.Fatal(err)
			}
			want := []string{"DELETE FROM users WHERE ID = ?", "DELETE FROM users WHERE ID IN (?, ?)"}
			if len(sent) != len(want) || sent[0].text != want[0] || sent[1].text != want[1] {
				t.Errorf("statements = %q, want %q", sent, want)
			}
		})
	}
}
