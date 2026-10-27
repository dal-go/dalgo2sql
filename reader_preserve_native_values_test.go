package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// nativeValuesScript is a SQLite table whose columns hold every storage class:
// INTEGER and NUMERIC cells hold integers, reals and NULLs, a TEXT column also
// holds a BLOB, and one INTEGER column holds all five classes in different rows.
const nativeValuesScript = `
CREATE TABLE native_values (
	id INTEGER PRIMARY KEY,
	mixed INTEGER,
	note TEXT,
	amount NUMERIC
);
INSERT INTO native_values VALUES
	(1, 7, 'plain', 9007199254740993),
	(2, 2.5, x'00ff', 2.5),
	(3, 'text', 'second', 9007199254740995),
	(4, x'0102', NULL, NULL),
	(5, NULL, 'last', 1);`

// nativeValueRows is what each row of native_values holds, in its native class.
var nativeValueRows = []struct {
	id     int64
	mixed  any
	note   any
	amount any
}{
	{1, int64(7), "plain", int64(9007199254740993)},
	{2, float64(2.5), []byte{0x00, 0xff}, float64(2.5)},
	{3, "text", "second", int64(9007199254740995)},
	{4, []byte{0x01, 0x02}, nil, nil},
	{5, nil, "last", int64(1)},
}

func nativeValuesOptions() DbOptions {
	return DbOptions{
		StructuredQueryDialect: dialectSQLite,
		PreserveNativeValues:   true,
		Recordsets: map[string]*Recordset{
			"native_values": NewRecordset("native_values", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
}

// requireNativeCell compares one cell, telling a typed nil from SQL NULL as the
// reader does.
func requireNativeCell(t *testing.T, what string, got, want any) {
	t.Helper()
	if !sameCell(got, want) {
		t.Errorf("%s = %#v (%T), want %#v (%T)", what, got, got, want, want)
	}
}

func TestPreserveNativeValuesRecordsMapGetAndGetMulti(t *testing.T) {
	ctx := context.Background()
	raw := openTestSQLiteDB(t, nativeValuesScript)
	db := NewDatabase(raw, newSchema(), nativeValuesOptions())

	t.Run("Get", func(t *testing.T) {
		for _, row := range nativeValueRows {
			data := map[string]any{}
			record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("native_values", fmt.Sprint(row.id)), data)
			if err := db.Get(ctx, record); err != nil {
				t.Fatalf("row %d: %v", row.id, err)
			}
			requireNativeCell(t, "id", data["id"], row.id)
			requireNativeCell(t, "mixed", data["mixed"], row.mixed)
			requireNativeCell(t, "note", data["note"], row.note)
			requireNativeCell(t, "amount", data["amount"], row.amount)
		}
	})

	t.Run("GetMulti", func(t *testing.T) {
		records := make([]dalrecord.Record, len(nativeValueRows))
		for i, row := range nativeValueRows {
			records[i] = dalrecord.NewRecordWithData(
				dalrecord.NewKeyWithID("native_values", fmt.Sprint(row.id)),
				map[string]any{},
			)
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatal(err)
		}
		for i, row := range nativeValueRows {
			data := records[i].Data().(map[string]any)
			requireNativeCell(t, "id", data["id"], row.id)
			requireNativeCell(t, "mixed", data["mixed"], row.mixed)
			requireNativeCell(t, "note", data["note"], row.note)
			requireNativeCell(t, "amount", data["amount"], row.amount)
			if !records[i].Exists() {
				t.Errorf("row %d is not marked as read", row.id)
			}
		}
	})
}

func TestPreserveNativeValuesRecordsReaderKeepsNativeClassesAndKeys(t *testing.T) {
	ctx := context.Background()
	raw := openTestSQLiteDB(t, nativeValuesScript)

	t.Run("a select-all keyed by the catalog", func(t *testing.T) {
		q := dal.From(dal.NewRootCollectionRef("native_values", "")).NewQuery().
			OrderBy(dal.AscendingField("id")).SelectIntoRecordset()
		reader, err := getRecordsReaderWithOptions(ctx, q, raw.QueryContext, nativeValuesOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		for i, row := range nativeValueRows {
			record, err := reader.Next()
			if err != nil {
				t.Fatalf("row %d: %v", i, err)
			}
			if record.Key().ID != row.id || record.Key().IDKind != reflect.Int64 {
				t.Errorf("row %d key = %v (kind %v), want %v (kind int64)", i, record.Key().ID, record.Key().IDKind, row.id)
			}
			data := record.Data().(map[string]any)
			requireNativeCell(t, "mixed", data["mixed"], row.mixed)
			requireNativeCell(t, "note", data["note"], row.note)
			requireNativeCell(t, "amount", data["amount"], row.amount)
		}
	})

	t.Run("a projection keys from a private helper column", func(t *testing.T) {
		q := dal.From(dal.NewRootCollectionRef("native_values", "")).NewQuery().
			OrderBy(dal.AscendingField("id")).
			SelectColumns(dal.Column{Expression: dal.Field("mixed")})
		reader, err := getRecordsReaderWithOptions(ctx, q, raw.QueryContext, nativeValuesOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		for i, row := range nativeValueRows {
			record, err := reader.Next()
			if err != nil {
				t.Fatalf("row %d: %v", i, err)
			}
			if record.Key().ID != row.id || record.Key().IDKind != reflect.Int64 {
				t.Errorf("row %d key = %v (kind %v), want %v (kind int64)", i, record.Key().ID, record.Key().IDKind, row.id)
			}
			data := record.Data().(map[string]any)
			if _, ok := data[recordIDHelperColumn]; ok {
				t.Errorf("row %d exposes the identity helper column: %#v", i, data)
			}
			requireNativeCell(t, "mixed", data["mixed"], row.mixed)
		}
	})
}

func TestPreserveNativeValuesRecordsetKeepsAllClassesInOneColumn(t *testing.T) {
	ctx := context.Background()
	raw := openTestSQLiteDB(t, nativeValuesScript)
	options := nativeValuesOptions()

	t.Run("ascending", func(t *testing.T) {
		reader, err := getRecordsetReaderWithOptions(ctx, dal.NewTextQuery(
			"SELECT mixed, note, amount FROM native_values ORDER BY id", nil), raw.QueryContext, options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rs, rows := drainRecordset(t, reader)
		if len(rows) != len(nativeValueRows) {
			t.Fatalf("read %d rows, want %d", len(rows), len(nativeValueRows))
		}
		for _, column := range []struct {
			name   string
			dbType string
			values func(row int) any
		}{
			{"mixed", "INTEGER", func(i int) any { return nativeValueRows[i].mixed }},
			{"note", "TEXT", func(i int) any { return nativeValueRows[i].note }},
			{"amount", "NUMERIC", func(i int) any { return nativeValueRows[i].amount }},
		} {
			t.Run(column.name, func(t *testing.T) {
				requireColumnType(t, rs, column.name, anyValueType, column.dbType)
				for i := range nativeValueRows {
					requireCell(t, rs, rows, column.name, i, column.values(i))
				}
			})
		}
	})

	t.Run("a first row that is NULL does not fix the column type", func(t *testing.T) {
		reader, err := getRecordsetReaderWithOptions(ctx, dal.NewTextQuery(
			"SELECT mixed FROM native_values ORDER BY id DESC", nil), raw.QueryContext, options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rs, rows := drainRecordset(t, reader)
		requireColumnType(t, rs, "mixed", anyValueType, "INTEGER")
		want := []any{nil, []byte{0x01, 0x02}, "text", float64(2.5), int64(7)}
		for i, value := range want {
			requireCell(t, rs, rows, "mixed", i, value)
		}
	})
}

func TestPreserveNativeValuesDefaultOptionKeepsLegacyNormalization(t *testing.T) {
	ctx := context.Background()
	raw := openTestSQLiteDB(t, nativeValuesScript)

	t.Run("records reader turns a TEXT blob into text", func(t *testing.T) {
		reader, err := getRecordsReaderWithOptions(ctx,
			dal.NewTextQuery("SELECT note FROM native_values ORDER BY id", nil),
			raw.QueryContext, DbOptions{StructuredQueryDialect: dialectSQLite})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		want := []any{"plain", string([]byte{0x00, 0xff}), "second", nil, "last"}
		if got := readAllRecords(t, reader, "note"); !reflect.DeepEqual(got, want) {
			t.Fatalf("note = %#v, want legacy text %#v", got, want)
		}
	})

	t.Run("recordset reader keeps its first-row integer column", func(t *testing.T) {
		reader, err := getRecordsetReaderWithOptions(ctx,
			dal.NewTextQuery("SELECT mixed FROM native_values WHERE id <= 2 ORDER BY id", nil),
			raw.QueryContext, DbOptions{StructuredQueryDialect: dialectSQLite})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rs, rows := drainRecordset(t, reader)
		requireColumnType(t, rs, "mixed", reflect.TypeOf(int64(0)), "INTEGER")
		requireCell(t, rs, rows, "mixed", 0, int64(7))
		// The legacy reader truncates the REAL cell into the integer column.
		requireCell(t, rs, rows, "mixed", 1, int64(2))
	})
}

func TestCopyNativeValueOwnsTheBytes(t *testing.T) {
	source := []byte{0x00, 0xff}
	got, ok := copyNativeValue(source).([]byte)
	if !ok {
		t.Fatalf("copyNativeValue returned %T, want []byte", copyNativeValue(source))
	}
	source[0] = 0x7f
	if !reflect.DeepEqual(got, []byte{0x00, 0xff}) {
		t.Fatalf("copied bytes = %#v, want an owned copy", got)
	}
	if other := copyNativeValue("text"); other != "text" {
		t.Fatalf("copyNativeValue(text) = %#v, want text", other)
	}
}

// TestPreserveNativeValuesCopiesDriverBytes uses the mock driver for the alias
// branch: the driver may reuse the []byte it handed out for the next row.
func TestPreserveNativeValuesCopiesDriverBytes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	source := []byte{0x01, 0x02}
	column := sqlmock.NewColumn("payload").OfType("BLOB", []byte(nil))
	mock.ExpectQuery("SELECT payload FROM values_table").
		WillReturnRows(sqlmock.NewRowsWithColumnDefinition(column).AddRow(source))
	reader, err := getRecordsReaderWithOptions(context.Background(),
		dal.NewTextQuery("SELECT payload FROM values_table", nil), db.QueryContext,
		DbOptions{StructuredQueryDialect: dialectSQLite, PreserveNativeValues: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 0x7f
	got := record.Data().(map[string]any)["payload"]
	if !reflect.DeepEqual(got, []byte{0x01, 0x02}) {
		t.Fatalf("payload = %#v, want an owned copy", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPreserveNativeValuesValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options DbOptions
		want    string
	}{
		{"default", DbOptions{}, ""},
		{"unsupported dialect", DbOptions{PreserveNativeValues: true}, "requires StructuredQueryDialect"},
		{"postgres", DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectPostgres}, "requires StructuredQueryDialect"},
		{"exact numeric", DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectSQLite, ExactNumericValues: true}, "cannot be combined with ExactNumericValues"},
		{"supported", DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectSQLite}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.options.validatePreserveNativeValues()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validatePreserveNativeValues() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validatePreserveNativeValues() = %v, want %q", err, tc.want)
			}
		})
	}
}

// failingExecute is an executeQueryFunc that fails the test when a refused read
// reaches I/O.
func failingExecute(t *testing.T) executeQueryFunc {
	t.Helper()
	return func(context.Context, string, ...any) (*sql.Rows, error) {
		t.Error("a refused read reached the database")
		return nil, errors.New("must not run")
	}
}

func TestPreserveNativeValuesRefusesBeforeIO(t *testing.T) {
	ctx := context.Background()

	t.Run("Get refuses a struct target", func(t *testing.T) {
		record := dalrecord.NewRecordWithData(
			dalrecord.NewKeyWithID("native_values", "1"),
			&struct{ ID int }{},
		)
		err := getSingle(ctx, nativeValuesOptions(), record, failingExecute(t))
		if err == nil || !strings.Contains(err.Error(), "requires map record data") {
			t.Fatalf("Get error = %v, want map-target refusal", err)
		}
	})

	t.Run("GetMulti refuses a struct target", func(t *testing.T) {
		records := []dalrecord.Record{dalrecord.NewRecordWithData(
			dalrecord.NewKeyWithID("native_values", "1"),
			&struct{ ID int }{},
		)}
		err := getMulti(ctx, nativeValuesOptions(), records, failingExecute(t))
		if err == nil || !strings.Contains(err.Error(), "requires map record data") {
			t.Fatalf("GetMulti error = %v, want map-target refusal", err)
		}
	})

	t.Run("Get refuses a non-SQLite dialect", func(t *testing.T) {
		record := dalrecord.NewRecordWithData(
			dalrecord.NewKeyWithID("native_values", "1"),
			map[string]any{},
		)
		err := getSingle(ctx, DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectPostgres}, record, failingExecute(t))
		if err == nil || !strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("Get error = %v, want dialect refusal", err)
		}
	})

	t.Run("GetMulti refuses a non-SQLite dialect", func(t *testing.T) {
		records := []dalrecord.Record{dalrecord.NewRecordWithData(
			dalrecord.NewKeyWithID("native_values", "1"),
			map[string]any{},
		)}
		err := getMulti(ctx, DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectPostgres}, records, failingExecute(t))
		if err == nil || !strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("GetMulti error = %v, want dialect refusal", err)
		}
	})

	t.Run("records reader refuses a struct record", func(t *testing.T) {
		q := dal.From(dal.NewRootCollectionRef("native_values", "")).NewQuery().
			SelectIntoRecord(func() dalrecord.Record {
				return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("native_values", ""), &struct{ ID int }{})
			})
		_, err := getRecordsReaderWithOptions(ctx, q, failingExecute(t), nativeValuesOptions())
		if err == nil || !strings.Contains(err.Error(), "requires map record data") {
			t.Fatalf("records reader error = %v, want map-target refusal", err)
		}
	})

	t.Run("records reader refuses a non-SQLite dialect", func(t *testing.T) {
		_, err := getRecordsReaderWithOptions(ctx, dal.NewTextQuery("SELECT 1", nil), failingExecute(t),
			DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectPostgres})
		if err == nil || !strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("records reader error = %v, want dialect refusal", err)
		}
	})

	t.Run("recordset reader refuses a non-SQLite dialect", func(t *testing.T) {
		_, err := getRecordsetReaderWithOptions(ctx, dal.NewTextQuery("SELECT 1", nil), failingExecute(t),
			DbOptions{PreserveNativeValues: true, StructuredQueryDialect: dialectPostgres})
		if err == nil || !strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("recordset reader error = %v, want dialect refusal", err)
		}
	})

	t.Run("database readers refuse before opening a connection", func(t *testing.T) {
		raw, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		_ = raw.Close() // any I/O would fail with "database is closed"
		db := NewDatabase(raw, newSchema(), DbOptions{
			PreserveNativeValues:   true,
			StructuredQueryDialect: dialectPostgres,
		})
		if _, err = db.ExecuteQueryToRecordsReader(ctx, dal.NewTextQuery("SELECT 1", nil)); err == nil ||
			!strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("ExecuteQueryToRecordsReader error = %v, want dialect refusal", err)
		}
		if _, err = db.ExecuteQueryToRecordsetReader(ctx, dal.NewTextQuery("SELECT 1", nil)); err == nil ||
			!strings.Contains(err.Error(), "requires StructuredQueryDialect") {
			t.Fatalf("ExecuteQueryToRecordsetReader error = %v, want dialect refusal", err)
		}
	})
}
