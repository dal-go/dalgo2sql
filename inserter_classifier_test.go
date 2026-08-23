package dalgo2sql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// newUsersDatabaseWithClassifier is newUsersDatabaseWithMock (see
// inserter_options_test.go) with a configurable DbOptions.IsAlreadyExists
// hook, so these tests can exercise all three states: a hook that classifies
// a duplicate, a hook that never does, and no hook at all (nil).
func newUsersDatabaseWithClassifier(t *testing.T, isAlreadyExists func(error) bool) (*database, sqlmock.Sqlmock, func()) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	db := dal.BackendOf(NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
		},
		IsAlreadyExists: isAlreadyExists,
	})).(*database)
	return db, mock, func() { closeDatabase(t, sqlDB) }
}

// TestExecInsert_DuplicateKeyClassifier proves the three contractual states
// of DbOptions.IsAlreadyExists, applied at the execInsert choke point that
// every insert path (Insert, InsertMulti, and the transactional variants —
// see execInsert's doc comment) funnels through:
//   - a hook that identifies the driver error as a duplicate produces an
//     error satisfying record.IsAlreadyExists, and the original driver error
//     remains recoverable from the chain (errors.Is), never replaced;
//   - a hook that does not identify the error leaves it passing through
//     completely unchanged;
//   - a nil hook (the zero value) preserves today's behavior exactly: the
//     raw driver error passes through unwrapped, same as before this hook
//     existed.
func TestExecInsert_DuplicateKeyClassifier(t *testing.T) {
	driverErr := errors.New("stub: UNIQUE constraint failed: users.ID")

	t.Run("hook classifies a duplicate", func(t *testing.T) {
		db, mock, closeFn := newUsersDatabaseWithClassifier(t, func(err error) bool {
			return errors.Is(err, driverErr)
		})
		defer closeFn()

		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs("id1", "u1").
			WillReturnError(driverErr)

		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &user{Name: "u1"})
		err := db.Insert(context.Background(), record)

		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if !dalrecord.IsAlreadyExists(err) {
			t.Errorf("expected record.IsAlreadyExists(err) = true, got false; err = %v", err)
		}
		if !errors.Is(err, driverErr) {
			t.Errorf("expected the original driver error to remain in the chain (wrapped, not replaced); err = %v", err)
		}
		if !errors.Is(err, dalrecord.ErrRecordExists) {
			t.Errorf("expected errors.Is(err, record.ErrRecordExists) = true; err = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("hook does not classify a non-duplicate error", func(t *testing.T) {
		otherErr := errors.New("stub: connection reset by peer")
		db, mock, closeFn := newUsersDatabaseWithClassifier(t, func(err error) bool {
			return false // never classifies anything as a duplicate
		})
		defer closeFn()

		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs("id1", "u1").
			WillReturnError(otherErr)

		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &user{Name: "u1"})
		err := db.Insert(context.Background(), record)

		if !errors.Is(err, otherErr) {
			t.Fatalf("expected the original error unchanged, got: %v", err)
		}
		if dalrecord.IsAlreadyExists(err) {
			t.Errorf("expected record.IsAlreadyExists(err) = false for a non-duplicate error; err = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("nil hook preserves today's behavior exactly", func(t *testing.T) {
		db, mock, closeFn := newUsersDatabaseWithClassifier(t, nil)
		defer closeFn()

		mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").
			WithArgs("id1", "u1").
			WillReturnError(driverErr)

		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &user{Name: "u1"})
		err := db.Insert(context.Background(), record)

		if !errors.Is(err, driverErr) {
			t.Fatalf("expected the raw driver error unchanged, got: %v", err)
		}
		if err != driverErr {
			t.Errorf("expected the exact same error value (no wrapping at all) with a nil hook, got: %v", err)
		}
		if dalrecord.IsAlreadyExists(err) {
			t.Errorf("expected record.IsAlreadyExists(err) = false with a nil hook; err = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// TestExecInsert_DuplicateKeyClassifier_InsertMulti proves the hook also
// applies to InsertMulti, which loops over insertSingle/execInsert one
// record at a time rather than calling Insert.
func TestExecInsert_DuplicateKeyClassifier_InsertMulti(t *testing.T) {
	driverErr := errors.New("stub: UNIQUE constraint failed: users.ID")
	db, mock, closeFn := newUsersDatabaseWithClassifier(t, func(err error) bool {
		return errors.Is(err, driverErr)
	})
	defer closeFn()

	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &user{Name: "u1"}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id2"), &user{Name: "u2"}),
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id2", "u2").WillReturnError(driverErr)
	mock.ExpectRollback()

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.InsertMulti(ctx, records)
	})

	if !dalrecord.IsAlreadyExists(err) {
		t.Fatalf("expected record.IsAlreadyExists(err) = true from InsertMulti's second record, got: %v", err)
	}
	if !errors.Is(err, driverErr) {
		t.Errorf("expected the original driver error to remain in the chain; err = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestExecInsert_DuplicateKeyClassifier_TransactionalInsert proves the hook
// applies to transaction.Insert (as opposed to (*database).Insert), i.e. an
// insert made through an explicit RunReadwriteTransaction worker.
func TestExecInsert_DuplicateKeyClassifier_TransactionalInsert(t *testing.T) {
	driverErr := errors.New("stub: UNIQUE constraint failed: users.ID")
	db, mock, closeFn := newUsersDatabaseWithClassifier(t, func(err error) bool {
		return errors.Is(err, driverErr)
	})
	defer closeFn()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO users(ID, Name) VALUES (?, ?)").WithArgs("id1", "u1").WillReturnError(driverErr)
	mock.ExpectRollback()

	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &user{Name: "u1"})
	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record)
	})

	if !dalrecord.IsAlreadyExists(err) {
		t.Fatalf("expected record.IsAlreadyExists(err) = true from a transactional Insert, got: %v", err)
	}
	if !errors.Is(err, driverErr) {
		t.Errorf("expected the original driver error to remain in the chain; err = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
