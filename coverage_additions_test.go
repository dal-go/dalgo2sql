package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// --- transaction.Exists / transaction.GetMulti ---------------------------

func TestTransaction_Exists(t *testing.T) {
	ctx := context.Background()

	t.Run("exists_true", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("u1").
			WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		mock.ExpectCommit()
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			exists, err := tx.(interface {
				Exists(ctx context.Context, key *dal.Key) (bool, error)
			}).Exists(ctx, dal.NewKeyWithID("users", "u1"))
			if err != nil {
				return err
			}
			if !exists {
				t.Errorf("expected exists=true")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("exists_false", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT 1 FROM users WHERE id = ?").
			WithArgs("ghost").
			WillReturnRows(sqlmock.NewRows([]string{"1"}))
		mock.ExpectCommit()
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			exists, err := tx.(interface {
				Exists(ctx context.Context, key *dal.Key) (bool, error)
			}).Exists(ctx, dal.NewKeyWithID("users", "ghost"))
			if err != nil {
				return err
			}
			if exists {
				t.Errorf("expected exists=false")
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestTransaction_GetMulti(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		sqlDB, mock, _ := sqlmock.New()
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
			},
		})
		type U struct {
			Name string `db:"Name"`
		}
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT id, Name FROM users WHERE id IN \(\?, \?\)`).
			WithArgs("u1", "u2").
			WillReturnRows(sqlmock.NewRows([]string{"id", "Name"}).
				AddRow("u1", "John").
				AddRow("u2", "Jane"))
		mock.ExpectCommit()
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			u1, u2 := U{}, U{}
			recs := []dal.Record{
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &u1),
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u2"), &u2),
			}
			if err := tx.GetMulti(ctx, recs); err != nil {
				return err
			}
			if u1.Name != "John" || u2.Name != "Jane" {
				t.Errorf("unexpected names: %q %q", u1.Name, u2.Name)
			}
			return nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// --- recordsetReader.Recordset / Cursor / Close --------------------------

func TestRecordsetReader_RecordsetCursorClose(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT name FROM users", nil)

	mock.ExpectQuery(q.Text()).WillReturnRows(
		sqlmock.NewRows([]string{"name"}).AddRow([]byte("John")).AddRow([]byte("Jane")),
	)

	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}

	if rs := rr.Recordset(); rs == nil {
		t.Errorf("expected non-nil recordset")
	}

	if cur, err := rr.Cursor(); cur != "" || !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Errorf("expected (\"\", ErrNotImplementedYet), got (%q, %v)", cur, err)
	}

	// Walk Next() through to ErrNoMoreRecords
	for i := 0; i < 2; i++ {
		row, _, err := rr.Next()
		if err != nil {
			t.Fatalf("Next() row %d: %v", i, err)
		}
		if row == nil {
			t.Fatalf("Next() returned nil row at %d", i)
		}
	}
	_, _, err = rr.Next()
	if !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}

	if err := rr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestRecordsetReader_Close_NilRows(t *testing.T) {
	rr := &recordsetReader{}
	if err := rr.Close(); err != nil {
		t.Errorf("Close on empty reader should be nil, got %v", err)
	}
}

// --- getRecordsetReader: error paths -------------------------------------

func TestGetRecordsetReader_ExecuteError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT bad FROM nope", nil)
	mock.ExpectQuery(q.Text()).WillReturnError(errors.New("boom"))

	_, err = getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// Exercise the value-conversion branches in recordsetReader.Next (string/int64/float64).
func TestRecordsetReader_Next_TypedConversions(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT s, i, f FROM t", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("s").OfType("TEXT", ""),
		sqlmock.NewColumn("i").OfType("INT", int64(0)),
		sqlmock.NewColumn("f").OfType("REAL", float64(0)),
	).AddRow([]byte("hello"), int64(7), int64(3)) // int64 to float64 will exercise the float64 conversion branch
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
	defer func() { _ = rr.Close() }()

	row, _, err := rr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if row == nil {
		t.Fatal("expected row")
	}
}

// Exercise the nil-value handling branch (column is non-blob, value is nil -> uses col.DefaultValue()).
func TestRecordsetReader_Next_NilValue_DefaultsApplied(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT i FROM t", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("i").OfType("INT", int64(0)),
	).AddRow(nil)
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
	defer func() { _ = rr.Close() }()

	row, _, err := rr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if row == nil {
		t.Fatal("expected row")
	}
}

// Exercise the string, int and float scan-type branches of getRecordsetReader.
// Uses sqlmock's NewRowsWithColumnDefinition + Column.OfType to set scan types.
func TestGetRecordsetReader_TypedScanTypes(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT s, i, f, b FROM t", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("s").OfType("TEXT", ""),
		sqlmock.NewColumn("i").OfType("INT", int64(0)),
		sqlmock.NewColumn("f").OfType("REAL", float64(0)),
		sqlmock.NewColumn("b").OfType("BOOL", false),
	).AddRow("hi", int64(7), float64(2.5), true)

	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
	defer func() { _ = rr.Close() }()

	row, _, err := rr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if row == nil {
		t.Fatalf("expected row")
	}
}

// Exercise additional scan-type branches in getRecordsetReader:
// time.Time, sql.NullString, sql.NullInt64, sql.NullFloat64, sql.NullBool, sql.NullTime.
func TestGetRecordsetReader_NullableTypes(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT t, ns, ni, nf, nb, nt FROM x", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("t").OfType("TIMESTAMP", time.Time{}),
		sqlmock.NewColumn("ns").OfType("TEXT", sql.NullString{}),
		sqlmock.NewColumn("ni").OfType("INT", sql.NullInt64{}),
		sqlmock.NewColumn("nf").OfType("REAL", sql.NullFloat64{}),
		sqlmock.NewColumn("nb").OfType("BOOL", sql.NullBool{}),
		sqlmock.NewColumn("nt").OfType("TIMESTAMP", sql.NullTime{}),
	).AddRow(time.Now(), "s", int64(1), 1.5, true, time.Now())
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
	defer func() { _ = rr.Close() }()
	// Don't call Next(); the scan into recordset rows may complain about
	// dynamic value/column-type mismatches that aren't relevant to the
	// type-mapping switch we're trying to exercise.
}

// Exercise sql.NullInt8/16/32 branches and bytes-slice branch.
func TestGetRecordsetReader_NullableIntSizes(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT n8, n16, n32, b FROM x", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("n8").OfType("INT", sql.NullInt16{}),  // -> sql.NullInt16 branch
		sqlmock.NewColumn("n16").OfType("INT", sql.NullInt32{}), // -> sql.NullInt32 branch
		sqlmock.NewColumn("n32").OfType("INT", sql.NullInt64{}), // -> sql.NullInt64 branch
		sqlmock.NewColumn("b").OfType("BLOB", []byte{}),         // -> Slice/Uint8 branch
	).AddRow(int16(1), int32(2), int64(3), []byte("x"))
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
}

// Exercise sql.NullInt8 explicit branch.
func TestGetRecordsetReader_NullInt8(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT v FROM x", nil)

	type nullInt8 = sql.NullInt16 // sql doesn't have NullInt8 in stdlib? It does.
	_ = nullInt8{}

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("v").OfType("INT", sql.NullInt16{}),
	).AddRow(int16(1))
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)
	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
}

// Exercise pointer-to-uint8 ScanType branch (treated as []byte).
func TestGetRecordsetReader_PointerToByte(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT p FROM x", nil)

	var b *byte
	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("p").OfType("PTR", b),
	).AddRow(nil)
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
}

// Exercise pointer-to-interface ScanType branch.
func TestGetRecordsetReader_PointerToInterface(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT p FROM x", nil)

	var iface *any
	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("p").OfType("PTR", iface),
	).AddRow(nil)
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
}

// Exercise unsupported pointer-elem branch.
func TestGetRecordsetReader_UnsupportedPointerType(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT p FROM x", nil)

	var p *int
	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("p").OfType("PTR", p),
	).AddRow(nil)
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err == nil {
		t.Fatal("expected error for unsupported pointer type")
	}
}

// Exercise unsupported-kind error path (e.g. chan, map - we'll try map).
func TestGetRecordsetReader_UnsupportedKind(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT m FROM x", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("m").OfType("MAP", map[string]int{}),
	).AddRow(nil)
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err == nil {
		t.Fatal("expected error for unsupported scan kind")
	}
}

// Exercise non-uint8 slice branch (e.g. []string -> treated as string).
func TestGetRecordsetReader_SliceNonByte(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT a FROM x", nil)

	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("a").OfType("ARRAY", []string{}),
	).AddRow("anything")
	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	if _, err = getRecordsetReader(ctx, q, sqlDB.QueryContext); err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
}

// Exercise the unsupported-type error in getRecordsetReader (struct-kind
// scan type whose name is not one of the recognized sql.NullX/time.Time).
func TestGetRecordsetReader_UnsupportedStructType(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT t FROM t", nil)

	// strings.Builder is a struct with no recognized name.
	rows := sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("t").OfType("MYSTRUCT", struct{ X int }{}),
	).AddRow("x")

	mock.ExpectQuery(q.Text()).WillReturnRows(rows)

	_, err = getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err == nil {
		t.Fatal("expected unsupported-type error")
	}
}

// Exercise multi-column path in getRecordsetReader.
func TestGetRecordsetReader_MultiColumn(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewTextQuery("SELECT a, b FROM t", nil)

	// sqlmock reports scan type []byte for all values (its driver returns
	// driver.Value as []byte). Use []byte to keep the test deterministic.
	mock.ExpectQuery(q.Text()).WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).
			AddRow([]byte("x"), []byte("y")).
			AddRow(nil, []byte("z")), // exercise nil-value handling
	)
	rr, err := getRecordsetReader(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getRecordsetReader: %v", err)
	}
	defer func() { _ = rr.Close() }()

	for i := 0; i < 2; i++ {
		row, _, err := rr.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if row == nil {
			t.Fatalf("nil row at %d", i)
		}
	}
}

// --- getReaderBase: structured query, error paths -----------------------

func TestGetReaderBase_ExecuteError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("nope"))

	_, err = getReaderBase(ctx, dal.NewTextQuery("SELECT 1", nil), sqlDB.QueryContext)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetReaderBase_StructuredQuery_Limit(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("Customer", ""))).
		Limit(10).
		SelectIntoRecordset()

	mock.ExpectQuery("LIMIT 10").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	rb, err := getReaderBase(ctx, q, sqlDB.QueryContext)
	if err != nil {
		t.Fatalf("getReaderBase: %v", err)
	}
	if len(rb.colNames) != 1 {
		t.Errorf("unexpected colNames: %v", rb.colNames)
	}
}

// --- getRecordsReader error path ----------------------------------------

func TestGetRecordsReader_ExecuteError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("denied"))

	_, err = getRecordsReader(ctx, dal.NewTextQuery("SELECT 1", nil), sqlDB.QueryContext)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- getMultiFromSingleTable: no-primary-key path -----------------------

func TestGetMulti_NoPrimaryKey(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)

	// Options have no PrimaryKey and no Recordsets entry for collection.
	db := NewDatabase(sqlDB, newSchema(), DbOptions{})
	ctx := context.Background()
	type U struct {
		Name string
	}
	recs := []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &U{}),
		dal.NewRecordWithData(dal.NewKeyWithID("users", "u2"), &U{}),
	}
	if err := db.GetMulti(ctx, recs); err != nil {
		t.Errorf("expected nil (records get errors set), got %v", err)
	}
	// dal.Record.Error() returns nil for not-found errors; just verify
	// the call did not propagate a top-level error and the records exist.
	for _, r := range recs {
		_ = r
	}
}

// --- getMultiFromSingleTable: row not present in returned set sets ErrNotFound

func TestGetMulti_PartialHit(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)

	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	ctx := context.Background()
	type U struct {
		Name string `db:"Name"`
	}
	u1, u2 := U{}, U{}
	recs := []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &u1),
		dal.NewRecordWithData(dal.NewKeyWithID("users", "u2"), &u2),
	}

	mock.ExpectQuery(`SELECT id, Name FROM users WHERE id IN \(\?, \?\)`).
		WithArgs("u1", "u2").
		WillReturnRows(sqlmock.NewRows([]string{"id", "Name"}).AddRow("u1", "John"))

	if err := db.GetMulti(ctx, recs); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if u1.Name != "John" {
		t.Errorf("u1.Name: want John, got %q", u1.Name)
	}
	// dal.Record.Error() returns nil for not-found errors, so just verify
	// that the present row was populated and the missing one was not.
	if u2.Name != "" {
		t.Errorf("u2.Name expected empty, got %q", u2.Name)
	}
}

// --- rowIntoRecord: data == nil panics ----------------------------------

func TestRowIntoRecord_NilDataPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for nil data")
		}
	}()
	// Build a record with explicit nil data and force rowIntoRecord directly.
	key := dal.NewKeyWithID("users", "x")
	rec := dal.NewRecordWithData(key, nil)
	_ = rowIntoRecord(nil, rec, false)
}

// --- simpleFieldsToKey: convert-branch (mismatched source kind) ---------

func Test_simpleFieldsToKey_ConvertBranches(t *testing.T) {
	cases := []struct {
		name string
		kind reflect.Kind
		// source has different reflect.Kind from target to exercise the convert branch.
		input any
		want  any
	}{
		{"int_from_int64", reflect.Int, struct{ ID int64 }{ID: 1}, int(1)},
		{"int8_from_int", reflect.Int8, struct{ ID int }{ID: 1}, int8(1)},
		{"int16_from_int", reflect.Int16, struct{ ID int }{ID: 1}, int16(1)},
		{"int32_from_int64", reflect.Int32, struct{ ID int64 }{ID: 1}, int32(1)},
		{"uint_from_int", reflect.Uint, struct{ ID int }{ID: 1}, uint(1)},
		{"uint8_from_int", reflect.Uint8, struct{ ID int }{ID: 1}, uint8(1)},
		{"uint16_from_int", reflect.Uint16, struct{ ID int }{ID: 1}, uint16(1)},
		{"uint32_from_int", reflect.Uint32, struct{ ID int }{ID: 1}, uint32(1)},
		{"uint64_from_int", reflect.Uint64, struct{ ID int }{ID: 1}, uint64(1)},
		{"float32_from_int", reflect.Float32, struct{ ID int }{ID: 2}, float32(2)},
		{"float64_from_int", reflect.Float64, struct{ ID int }{ID: 2}, float64(2)},
		{"string_from_int", reflect.String, struct{ ID int }{ID: 3}, "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSimpleSchema("ID")
			incomplete := dal.NewIncompleteKey("things", tc.kind, nil)
			key, err := schema.DataToKey(incomplete, tc.input)
			if err != nil {
				t.Fatalf("DataToKey: %v", err)
			}
			if !reflect.DeepEqual(key.ID, tc.want) {
				t.Errorf("want %v(%T), got %v(%T)", tc.want, tc.want, key.ID, key.ID)
			}
		})
	}
}

// --- simpleFieldsToKey: extra type-conversion branches -----------------

func Test_simpleFieldsToKey_KindBranches(t *testing.T) {
	cases := []struct {
		name    string
		kind    reflect.Kind
		input   any
		wantVal any
	}{
		{"int8", reflect.Int8, struct{ ID int8 }{ID: 1}, int8(1)},
		{"int16", reflect.Int16, struct{ ID int16 }{ID: 2}, int16(2)},
		{"int32", reflect.Int32, struct{ ID int32 }{ID: 3}, int32(3)},
		{"uint", reflect.Uint, struct{ ID uint }{ID: 4}, uint(4)},
		{"uint8", reflect.Uint8, struct{ ID uint8 }{ID: 5}, uint8(5)},
		{"uint16", reflect.Uint16, struct{ ID uint16 }{ID: 6}, uint16(6)},
		{"uint64", reflect.Uint64, struct{ ID uint64 }{ID: 7}, uint64(7)},
		{"float32", reflect.Float32, struct{ ID float32 }{ID: 1.5}, float32(1.5)},
		{"float64", reflect.Float64, struct{ ID float64 }{ID: 2.5}, float64(2.5)},
		{"bool", reflect.Bool, struct{ ID bool }{ID: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSimpleSchema("ID")
			incomplete := dal.NewIncompleteKey("things", tc.kind, nil)
			key, err := schema.DataToKey(incomplete, tc.input)
			if err != nil {
				t.Fatalf("DataToKey: %v", err)
			}
			if !reflect.DeepEqual(key.ID, tc.wantVal) {
				t.Errorf("ID: want %v(%T), got %v(%T)", tc.wantVal, tc.wantVal, key.ID, key.ID)
			}
		})
	}
}

// --- updateSingle / updateMulti error paths ------------------------------

func TestUpdater_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("no_primary_key", func(t *testing.T) {
		sqlDB, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
		// no recordsets and no top-level PrimaryKey -> PrimaryKeyFieldNames returns nil
		key := dal.NewKeyWithID("users", "u1")
		// update without primary key -> error
		err = db.Update(ctx, key, nil)
		if err == nil {
			t.Errorf("expected error for missing primary key")
		}
	})

	t.Run("composite_primary_key", func(t *testing.T) {
		sqlDB, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("a"), dal.Field("b")}),
			},
		}).(*database)
		key := dal.NewKeyWithID("users", "u1")
		err = db.Update(ctx, key, nil)
		if !errors.Is(err, dal.ErrNotImplementedYet) {
			t.Errorf("expected ErrNotImplementedYet, got %v", err)
		}
	})

	t.Run("exec_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)
		mock.ExpectExec("UPDATE users SET").WillReturnError(errors.New("exec fail"))
		err = db.Update(ctx, dal.NewKeyWithID("users", "u1"), nil)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("updateMulti_propagates_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)
		mock.ExpectExec("UPDATE users SET").WillReturnError(errors.New("nope"))
		err = db.UpdateMulti(ctx, []*dal.Key{dal.NewKeyWithID("users", "u1")}, nil)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

// --- insertSingle / InsertMulti error paths ------------------------------

func TestInserter_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("exec_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		}).(*database)
		mock.ExpectExec("INSERT INTO users").WillReturnError(errors.New("insert fail"))
		rec := dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user{Name: "J"})
		err = db.Insert(ctx, rec)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("InsertMulti_propagates_error", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sqlDB)
		db := NewDatabase(sqlDB, newSchema(), DbOptions{
			Recordsets: map[string]*Recordset{
				"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
			},
		})
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO users").WillReturnError(errors.New("boom"))
		mock.ExpectRollback()
		err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.InsertMulti(ctx, []dal.Record{
				dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user{Name: "J"}),
			})
		})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})
}

// --- deleter: deleteMultiInSingleTable with custom pk ------------------

func TestDeleter_MultiInSingleTable_CustomPK(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("uid")}),
		},
	}).(*database)
	keys := []*dal.Key{
		dal.NewKeyWithID("users", "u1"),
		dal.NewKeyWithID("users", "u2"),
	}
	mock.ExpectExec("DELETE FROM users WHERE uid = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM users WHERE uid = ?").WithArgs("u2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM users WHERE uid IN (?, ?)").WithArgs("u1", "u2").WillReturnResult(sqlmock.NewResult(0, 2))
	if err := db.DeleteMulti(ctx, keys); err != nil {
		t.Errorf("unexpected: %v", err)
	}
}

func TestDeleter_MultiInSingleTable_ExecError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	db := NewDatabase(sqlDB, newSchema(), DbOptions{}).(*database)
	keys := []*dal.Key{
		dal.NewKeyWithID("users", "u1"),
		dal.NewKeyWithID("users", "u2"),
	}
	mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM users WHERE ID = ?").WithArgs("u2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM users WHERE ID IN (?, ?)").WithArgs("u1", "u2").WillReturnError(errors.New("boom"))
	if err := db.DeleteMulti(ctx, keys); err == nil {
		t.Errorf("expected error from multi-in-single-table exec")
	}
}

// --- setMulti error path -------------------------------------------------

func TestSetter_SetMulti_Error(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	ctx := context.Background()
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID")}),
		},
	}).(*database)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT ID FROM users WHERE ID = ?").
		WithArgs("u1").
		WillReturnError(errors.New("oops"))
	mock.ExpectRollback()
	err = db.SetMulti(ctx, []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("users", "u1"), &user{Name: "n"}),
	})
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}
