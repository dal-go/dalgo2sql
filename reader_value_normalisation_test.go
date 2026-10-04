package dalgo2sql

import (
	"context"
	"database/sql"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestNormalizeValueByDatabaseType(t *testing.T) {
	nan, ok := normalizeValueByDatabaseType("NUMERIC", "NaN").(float64)
	if !ok || !math.IsNaN(nan) {
		t.Errorf("NaN numeric text: got %v (ok=%v)", nan, ok)
	}
	tests := []struct {
		name     string
		dbType   string
		in, want any
	}{
		{"numeric string", "NUMERIC", "13.86", 13.86},
		{"numeric bytes", "NUMERIC", []byte("13.86"), 13.86},
		{"decimal string", "DECIMAL", "7", float64(7)},
		{"lower case type", "numeric", "0.5", 0.5},
		{"negative", "NUMERIC", "-2.25", -2.25},
		{"nil stays nil", "NUMERIC", nil, nil},
		{"unparsable string keeps text", "NUMERIC", "abc", "abc"},
		{"unparsable bytes keep text as string", "NUMERIC", []byte("abc"), "abc"},
		{"float64 untouched", "NUMERIC", 1.5, 1.5},
		{"int64 untouched", "NUMERIC", int64(3), int64(3)},
		{"json bytes", "JSON", []byte(`{"a":1}`), `{"a":1}`},
		{"jsonb bytes", "JSONB", []byte(`[1]`), `[1]`},
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

func TestRecordsetReader_UnparsableNumericIsAnErrorNotAPanic(t *testing.T) {
	ctx := context.Background()
	db, _ := numericMock(t, "NUMERIC", float64(0), "abc")
	rr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rr.Close() }()
	_, _, err = rr.Next()
	if err == nil || !strings.Contains(err.Error(), "column amount") || !strings.Contains(err.Error(), "abc") {
		t.Errorf("expected error naming column and text, got %v", err)
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

// SQLite results are unchanged: a NUMERIC-affinity column already yields numbers.
func TestReaders_SQLiteNumericColumnUnchanged(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, raw)
	if _, err = raw.Exec(`CREATE TABLE prices (id INTEGER PRIMARY KEY, amount NUMERIC);
		INSERT INTO prices VALUES (1, 13.86), (2, 7), (3, NULL), (4, 'abc');`); err != nil {
		t.Fatal(err)
	}
	q := dal.NewTextQuery("SELECT amount FROM prices ORDER BY id", nil)

	rr, err := getRecordsReader(ctx, q, raw.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	var got []any
	for {
		rec, err := rr.Next()
		if err != nil {
			break
		}
		got = append(got, rec.Data().(map[string]any)["amount"])
	}
	_ = rr.Close()
	if want := []any{13.86, int64(7), nil, "abc"}; !reflect.DeepEqual(got, want) {
		t.Errorf("records reader: got %v, want %v", got, want)
	}

	rsr, err := getRecordsetReader(ctx, dal.NewTextQuery("SELECT amount FROM prices WHERE id <= 3 ORDER BY id", nil), raw.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rsr.Close() }()
	var gotRS []any
	for {
		row, rs, err := rsr.Next()
		if err != nil {
			break
		}
		v, err := row.GetValueByIndex(0, rs)
		if err != nil {
			t.Fatal(err)
		}
		gotRS = append(gotRS, v)
	}
	if len(gotRS) != 3 || gotRS[0] != 13.86 || gotRS[1] != float64(7) {
		t.Errorf("recordset reader: got %v", gotRS)
	}
}
