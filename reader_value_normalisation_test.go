package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestNormalizeValueByDatabaseType(t *testing.T) {
	special := map[string]func(float64) bool{
		"NaN":       math.IsNaN,
		"Infinity":  func(f float64) bool { return math.IsInf(f, 1) },
		"-Infinity": func(f float64) bool { return math.IsInf(f, -1) },
	}
	for text, is := range special {
		got, ok := normalizeValueByDatabaseType("NUMERIC", text).(float64)
		if !ok || !is(got) {
			t.Errorf("%s numeric text: got %v (ok=%v)", text, got, ok)
		}
	}
	tests := []struct {
		name     string
		dbType   string
		in, want any
	}{
		{"numeric string", "NUMERIC", "13.86", 13.86},
		{"numeric bytes", "NUMERIC", []byte("13.86"), 13.86},
		{"whole number", "NUMERIC", "7", float64(7)},
		{"lower case type", "numeric", "0.5", 0.5},
		{"negative", "NUMERIC", "-2.25", -2.25},
		{"explicit plus", "NUMERIC", "+2.5", 2.5},
		{"leading dot", "NUMERIC", ".5", 0.5},
		{"trailing dot", "NUMERIC", "5.", float64(5)},
		{"nil stays nil", "NUMERIC", nil, nil},
		{"unparsable string keeps text", "NUMERIC", "abc", "abc"},
		{"unparsable bytes keep text as string", "NUMERIC", []byte("abc"), "abc"},
		{"empty text keeps text", "NUMERIC", "", ""},
		{"lower case inf keeps text", "NUMERIC", "inf", "inf"},
		{"lower case nan keeps text", "NUMERIC", "nan", "nan"},
		{"hex float keeps text", "NUMERIC", "0x1p4", "0x1p4"},
		{"exponent keeps text", "NUMERIC", "1e5", "1e5"},
		{"surrounding space keeps text", "NUMERIC", " 13.86 ", " 13.86 "},
		{"out of range keeps text", "NUMERIC", "1" + strings.Repeat("0", 400), "1" + strings.Repeat("0", 400)},
		{"float64 untouched", "NUMERIC", 1.5, 1.5},
		{"int64 untouched", "NUMERIC", int64(3), int64(3)},
		// DECIMAL is MySQL's name for the exact type; MySQL callers keep their text.
		{"decimal string untouched", "DECIMAL", "13.80", "13.80"},
		{"decimal bytes untouched", "DECIMAL", []byte("13.80"), []byte("13.80")},
		{"json bytes untouched", "JSON", []byte(`{"a":1}`), []byte(`{"a":1}`)},
		{"jsonb bytes untouched", "JSONB", []byte(`[1]`), []byte(`[1]`)},
		{"jsonb string untouched", "JSONB", `[1]`, `[1]`},
		{"jsonb nil untouched", "JSONB", nil, nil},
		{"varchar bytes untouched", "VARCHAR", []byte("13.86"), []byte("13.86")},
		{"varchar string untouched", "VARCHAR", "13.86", "13.86"},
		{"empty type untouched", "", "13.86", "13.86"},
		{"money untouched", "MONEY", "$13.86", "$13.86"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeValueByDatabaseType(tt.dbType, tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %T(%v), want %T(%v)", got, got, tt.want, tt.want)
			}
		})
	}
}

func numericMock(t *testing.T, typeName string, scanType any, values ...any) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDatabase(t, db) })
	col := sqlmock.NewColumn("amount").OfType(typeName, scanType)
	rows := sqlmock.NewRowsWithColumnDefinition(col)
	for _, v := range values {
		rows.AddRow(v)
	}
	mock.ExpectQuery("SELECT amount").WillReturnRows(rows)
	return db, mock
}

func TestRecordsReader_NormalisesByDatabaseTypeName(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "NUMERIC", float64(0), "13.86", []byte("14.5"), nil, "abc")
	rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	want := []any{13.86, 14.5, nil, "abc"}
	for i, w := range want {
		rec, err := rr.Next()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		got := rec.Data().(map[string]any)["amount"]
		if !reflect.DeepEqual(got, w) {
			t.Errorf("row %d: got %T(%v), want %T(%v)", i, got, got, w, w)
		}
	}
}

// Pins behaviour that does not depend on the normaliser: the records reader
// already stores every []byte as a string.
func TestRecordsReader_JSONBytesBecomeString(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "JSONB", []byte(nil), []byte(`{"a":1}`))
	rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	rec, err := rr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Data().(map[string]any)["amount"]; got != `{"a":1}` {
		t.Errorf("got %T(%v)", got, got)
	}
}

func TestRecordsReader_NumericNaNIsRejectedForAggregates(t *testing.T) {
	// A NUMERIC that normalises to NaN must still hit the finite-value guard.
	ctx := context.Background()
	db, _ := numericMock(t, "NUMERIC", float64(0), "NaN")
	rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	rr.validateFinite = true
	if _, err = rr.Next(); err == nil || !strings.Contains(err.Error(), "non-finite") {
		t.Errorf("expected non-finite error, got %v", err)
	}
}

func TestRecordsetReader_NumericStringBecomesFloat64(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		value any
		want  float64
	}{
		{"string", "13.86", 13.86},
		{"bytes", []byte("13.86"), 13.86},
		{"nil", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := numericMock(t, "NUMERIC", float64(0), tc.value)
			rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rr.Close() }()
			row, rs, err := rr.Next()
			if err != nil {
				t.Fatal(err)
			}
			got, err := row.GetValueByIndex(0, rs)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %T(%v), want float64(%v)", got, got, tc.want)
			}
		})
	}
}

func TestRecordsetReader_StringInFloatColumnIsAnErrorNotAPanicAndNeverQuotesTheCell(t *testing.T) {
	ctx := context.Background()
	const cell = "sentinel-cell-text"
	db, _ := numericMock(t, "NUMERIC", float64(0), cell)
	rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	_, _, err = rr.Next()
	if err == nil || !strings.Contains(err.Error(), "column amount") || !strings.Contains(err.Error(), "string") {
		t.Fatalf("expected error naming the column and the Go type, got %v", err)
	}
	if strings.Contains(err.Error(), cell) {
		t.Errorf("error quotes the cell text: %v", err)
	}
}

// A column whose recordset Go type is not float64 keeps what the driver
// delivered: normalisation must not hand dalgo's strict recordset a value of
// the wrong type (it panics on a mismatch).
func TestRecordsetReader_NormalisationFollowsTheColumnGoType(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		dbType   string
		scanType any
		value    any
		want     any
	}{
		{"numeric in a []byte column", "NUMERIC", []byte(nil), []byte("13.86"), []byte("13.86")},
		{"unparsable numeric in a []byte column", "NUMERIC", []byte(nil), []byte("abc"), []byte("abc")},
		{"jsonb in a []byte column", "JSONB", []byte(nil), []byte(`{"a":1}`), []byte(`{"a":1}`)},
		{"decimal in a string column", "DECIMAL", "", []byte("13.80"), "13.80"},
		{"large decimal in a string column", "DECIMAL", "", []byte("12345678901234567890.12"), "12345678901234567890.12"},
		{"numeric in a string column", "NUMERIC", "", "13.80", "13.80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := numericMock(t, tc.dbType, tc.scanType, tc.value)
			rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rr.Close() }()
			row, rs, err := rr.Next()
			if err != nil {
				t.Fatal(err)
			}
			got, err := row.GetValueByIndex(0, rs)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestRecordsReader_DecimalIsNotNormalised(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "DECIMAL", "", []byte("13.80"), "12345678901234567890.12")
	rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	for i, want := range []string{"13.80", "12345678901234567890.12"} {
		rec, err := rr.Next()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if got := rec.Data().(map[string]any)["amount"]; got != want {
			t.Errorf("row %d: got %T(%v), want %q", i, got, got, want)
		}
	}
}

func TestRecordsetReader_NumericNaNIsRejectedForAggregates(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "NUMERIC", float64(0), "NaN")
	rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	rr.validateFinite = true
	if _, _, err = rr.Next(); err == nil || !strings.Contains(err.Error(), "non-finite") {
		t.Errorf("expected non-finite error, got %v", err)
	}
}

// Pins behaviour that does not depend on the normaliser: a string-typed
// recordset column already converts []byte.
func TestRecordsetReader_JSONBytesBecomeString(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "JSONB", "", []byte(`{"a":1}`))
	rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	row, rs, err := rr.Next()
	if err != nil {
		t.Fatal(err)
	}
	got, err := row.GetValueByIndex(0, rs)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":1}` {
		t.Errorf("got %T(%v)", got, got)
	}
}

// readAllRecords drains a records reader and fails the test on any error other
// than the end of the results.
func readAllRecords(t *testing.T, rr *recordsReader, column string) (values []any) {
	t.Helper()
	defer func() { _ = rr.Close() }()
	for {
		rec, err := rr.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return values
		}
		if err != nil {
			t.Fatalf("records reader: %v", err)
		}
		values = append(values, rec.Data().(map[string]any)[column])
	}
}

// readAllRecordset drains a recordset reader and fails the test on any error
// other than the end of the results.
func readAllRecordset(t *testing.T, rr *recordsetReader, column int) (values []any) {
	t.Helper()
	defer func() { _ = rr.Close() }()
	for {
		row, rs, err := rr.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return values
		}
		if err != nil {
			t.Fatalf("recordset reader: %v", err)
		}
		v, err := row.GetValueByIndex(column, rs)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, v)
	}
}

func openSQLite(t *testing.T, script string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDatabase(t, raw) })
	raw.SetMaxOpenConns(1)
	if _, err = raw.Exec(script); err != nil {
		t.Fatal(err)
	}
	return raw
}

// SQLite recordset reader results are unchanged: numeric-affinity and decimal
// columns already yield numbers, JSON columns yield text, and BLOBs stay BLOBs.
// The records reader changes only for a bare NUMERIC column holding a BLOB of
// decimal text, or the texts NaN, Infinity and -Infinity.
func TestReaders_SQLiteResultsUnchanged(t *testing.T) {
	ctx := context.Background()
	raw := openSQLite(t, `
		CREATE TABLE prices (id INTEGER PRIMARY KEY, amount NUMERIC, price NUMERIC(10,2), dec DECIMAL, doc JSON, docb JSONB);
		INSERT INTO prices VALUES (1, 13.86, 13.86, 13.86, '{"a":1}', '{"b":2}'), (2, 7, 7, 7, '[]', '[]'), (3, NULL, NULL, NULL, NULL, NULL);
		CREATE TABLE blobs (id INTEGER PRIMARY KEY, docb JSONB, n NUMERIC, dec DECIMAL);
		INSERT INTO blobs VALUES (1, x'7B7D', x'3133', x'3133'), (2, x'5B5D', x'616263', x'616263');
		CREATE TABLE texts (id INTEGER PRIMARY KEY, n NUMERIC);
		INSERT INTO texts VALUES (1, 'abc');
		CREATE TABLE specials (id INTEGER PRIMARY KEY, n NUMERIC, dec DECIMAL);
		INSERT INTO specials VALUES (1, 'NaN', 'NaN'), (2, 'Infinity', 'Infinity'), (3, '-Infinity', '-Infinity');
		CREATE TABLE reals (id INTEGER PRIMARY KEY, amount REAL);
		INSERT INTO reals VALUES (1, 1.5), (2, 'abc-sentinel');`)

	for _, tc := range []struct {
		column  string
		records []any // records reader
		recset  []any // recordset reader
	}{
		{"amount", []any{13.86, int64(7), nil}, []any{13.86, float64(7), float64(0)}},
		{"price", []any{13.86, int64(7), nil}, []any{13.86, float64(7), float64(0)}},
		{"dec", []any{13.86, int64(7), nil}, []any{13.86, float64(7), float64(0)}},
		{"doc", []any{`{"a":1}`, `[]`, nil}, []any{`{"a":1}`, `[]`, ""}},
		{"docb", []any{`{"b":2}`, `[]`, nil}, []any{`{"b":2}`, `[]`, ""}},
	} {
		t.Run(tc.column, func(t *testing.T) {
			text := "SELECT " + tc.column + " FROM prices ORDER BY id"
			rr, err := getRecordsReader(ctx, dal.NewTextQuery(text, nil), raw.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			if got := readAllRecords(t, rr, tc.column); !reflect.DeepEqual(got, tc.records) {
				t.Errorf("records reader: got %v, want %v", got, tc.records)
			}
			rsr, err := getRecordsetReader(ctx, dal.NewTextQuery(text, nil), raw.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			if got := readAllRecordset(t, rsr, 0); !reflect.DeepEqual(got, tc.recset) {
				t.Errorf("recordset reader: got %v, want %v", got, tc.recset)
			}
		})
	}

	t.Run("blob columns", func(t *testing.T) {
		for _, column := range []string{"docb", "n", "dec"} {
			rsr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT "+column+" FROM blobs ORDER BY id", nil), raw.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			got := readAllRecordset(t, rsr, 0)
			want := map[string][]any{
				"docb": {[]byte("{}"), []byte("[]")},
				"n":    {[]byte("13"), []byte("abc")},
				"dec":  {[]byte("13"), []byte("abc")},
			}[column]
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: got %v, want %v", column, got, want)
			}
		}
		rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT docb FROM blobs ORDER BY id", nil), raw.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := readAllRecords(t, rr, "docb"), []any{"{}", "[]"}; !reflect.DeepEqual(got, want) {
			t.Errorf("records reader docb: got %v, want %v", got, want)
		}
		// The first declared behaviour change of the records reader (the README,
		// "NUMERIC result values", lists both): a bare NUMERIC column holding a BLOB
		// of decimal text is a float64; DECIMAL is not NUMERIC and keeps the text.
		// Text that is not a number stays a string. The second is the next test.
		for column, want := range map[string][]any{
			"n":   {float64(13), "abc"},
			"dec": {"13", "abc"},
		} {
			rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT "+column+" FROM blobs ORDER BY id", nil), raw.QueryContext)
			if err != nil {
				t.Fatal(err)
			}
			if got := readAllRecords(t, rr, column); !reflect.DeepEqual(got, want) {
				t.Errorf("records reader %s: got %v, want %v", column, got, want)
			}
		}
	})

	// The second declared behaviour change of the records reader: the texts NaN,
	// Infinity and -Infinity in a bare NUMERIC column are float64 values; in a
	// DECIMAL column, which is not NUMERIC, they stay text.
	t.Run("NaN and the infinities in a NUMERIC column", func(t *testing.T) {
		rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT n FROM specials ORDER BY id", nil), raw.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		got := readAllRecords(t, rr, "n")
		if len(got) != 3 {
			t.Fatalf("got %v", got)
		}
		if f, ok := got[0].(float64); !ok || !math.IsNaN(f) {
			t.Errorf("NaN: got %T(%v)", got[0], got[0])
		}
		if f, ok := got[1].(float64); !ok || !math.IsInf(f, 1) {
			t.Errorf("Infinity: got %T(%v)", got[1], got[1])
		}
		if f, ok := got[2].(float64); !ok || !math.IsInf(f, -1) {
			t.Errorf("-Infinity: got %T(%v)", got[2], got[2])
		}
		rr, err = getRecordsReader(ctx, dal.NewTextQuery("SELECT dec FROM specials ORDER BY id", nil), raw.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := readAllRecords(t, rr, "dec"), []any{"NaN", "Infinity", "-Infinity"}; !reflect.DeepEqual(got, want) {
			t.Errorf("DECIMAL: got %v, want %v", got, want)
		}
	})

	t.Run("text row in a NUMERIC column", func(t *testing.T) {
		rr, err := getRecordsReader(ctx, dal.NewTextQuery("SELECT n FROM texts ORDER BY id", nil), raw.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := readAllRecords(t, rr, "n"), []any{"abc"}; !reflect.DeepEqual(got, want) {
			t.Errorf("records reader n: got %v, want %v", got, want)
		}
	})

	t.Run("text row in a REAL column", func(t *testing.T) {
		rsr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount FROM reals ORDER BY id", nil), raw.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rsr.Close() }()
		row, rs, err := rsr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if v, _ := row.GetValueByIndex(0, rs); v != 1.5 {
			t.Errorf("first row: got %v", v)
		}
		_, _, err = rsr.Next()
		if err == nil || strings.Contains(err.Error(), "abc-sentinel") {
			t.Errorf("expected an error that does not quote the cell, got %v", err)
		}
	})
}
