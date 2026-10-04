package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type scanGetCity struct {
	Name       string
	Population int
	AreaSqKm   int
	Tagged     string `db:"label"`
}

func TestScanIntoData_StructColumnNamesIgnoreCase(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	query := func(rows *sqlmock.Rows) {
		mock.ExpectQuery("SELECT").WillReturnRows(rows)
	}
	t.Run("matches by case, snake_case and db tag", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name", "population", "area_sq_km", "label"}).AddRow([]byte("Tokyo"), int64(1), int64(2), "l"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		var got scanGetCity
		if err := scanIntoData(rows, &got, false); err != nil {
			t.Fatal(err)
		}
		if want := (scanGetCity{Name: "Tokyo", Population: 1, AreaSqKm: 2, Tagged: "l"}); got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("rejects a column without a field", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name", "unknown"}).AddRow("Tokyo", 1))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil || !strings.Contains(err.Error(), "no corresponding field") {
			t.Errorf("got %v", err)
		}
	})
	t.Run("scan error without a current row", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil {
			t.Error("want error")
		}
	})
	t.Run("columns error on closed rows", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		_ = rows.Close()
		if err := scanIntoData(rows, &scanGetCity{}, false); err == nil {
			t.Error("want error")
		}
	})
	t.Run("non-struct targets still use scany", func(t *testing.T) {
		query(sqlmock.NewRows([]string{"Name"}).AddRow("Tokyo"))
		rows, _ := db.QueryContext(ctx, "SELECT")
		defer func() { _ = rows.Close() }()
		rows.Next()
		var name string
		if err := scanIntoData(rows, &name, false); err != nil || name != "Tokyo" {
			t.Errorf("got %q, %v", name, err)
		}
	})
}

// bytesOnlyScanner accepts only []byte, as many JSON column types do.
type bytesOnlyScanner struct{ got []byte }

func (b *bytesOnlyScanner) Scan(value any) error {
	raw, ok := value.([]byte)
	if !ok {
		return errors.New("bytesOnlyScanner wants []byte")
	}
	b.got = append([]byte(nil), raw...)
	return nil
}

type scanGetShapes struct {
	Blob    []byte
	Doc     json.RawMessage
	Custom  bytesOnlyScanner
	Label   string
	Count   int
	Nick    *string
	Created sql.NullString
}

type scanGetAddress struct{ City string }

type scanGetNested struct {
	Name    string
	Address scanGetAddress
}

type ScanGetExported struct{ Promoted string }

type scanGetEmbedsPointer struct {
	*ScanGetExported
	Name string
}

func getScanned(t *testing.T, target any, cols []string, row ...any) error {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	args := make([]driver.Value, len(row))
	for i, v := range row {
		args[i] = v
	}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols).AddRow(args...))
	rows, err := db.QueryContext(context.Background(), "SELECT")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	rows.Next()
	return scanIntoData(rows, target, false)
}

// Only the column matcher differs from scany: values keep database/sql's
// conversions, so these shapes fill exactly as they did before.
func TestScanIntoData_StructKeepsDatabaseSQLConversions(t *testing.T) {
	t.Run("byte slice, RawMessage and a []byte-only Scanner", func(t *testing.T) {
		var got scanGetShapes
		err := getScanned(t, &got, []string{"blob", "doc", "custom"}, []byte{1, 2}, []byte(`{"a":1}`), []byte("raw"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Blob) != "\x01\x02" || string(got.Doc) != `{"a":1}` || string(got.Custom.got) != "raw" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("string field over an integer cell", func(t *testing.T) {
		var got scanGetShapes
		if err := getScanned(t, &got, []string{"label", "count"}, int64(5), "7"); err != nil {
			t.Fatal(err)
		}
		if got.Label != "5" || got.Count != 7 {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("NULL into a non-pointer field is an error, into a pointer it is nil", func(t *testing.T) {
		var got scanGetShapes
		if err := getScanned(t, &got, []string{"count"}, nil); err == nil {
			t.Error("want an error for NULL into int")
		}
		nick := "x"
		got.Nick = &nick
		if err := getScanned(t, &got, []string{"nick"}, nil); err != nil || got.Nick != nil {
			t.Errorf("got %v, %v", got.Nick, err)
		}
	})
	t.Run("NULL into a Scanner", func(t *testing.T) {
		got := scanGetShapes{Created: sql.NullString{String: "x", Valid: true}}
		if err := getScanned(t, &got, []string{"created"}, nil); err != nil || got.Created.Valid {
			t.Errorf("got %+v, %v", got.Created, err)
		}
	})
}

func TestScanIntoData_StructFallsBackToScanyWhereScanyMatched(t *testing.T) {
	t.Run("nested struct column with a dotted name", func(t *testing.T) {
		var got scanGetNested
		if err := getScanned(t, &got, []string{"name", "address.city"}, "x", "Oslo"); err != nil {
			t.Fatal(err)
		}
		if got.Name != "x" || got.Address.City != "Oslo" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("a Scanner struct with one column is scanned as a whole", func(t *testing.T) {
		var got sql.NullString
		if err := getScanned(t, &got, []string{"string"}, "x"); err != nil || got != (sql.NullString{String: "x", Valid: true}) {
			t.Errorf("got %+v, %v", got, err)
		}
	})
	t.Run("a column without a field keeps scany's error", func(t *testing.T) {
		err := getScanned(t, &scanGetCity{}, []string{"unknown"}, "x")
		if err == nil || !strings.Contains(err.Error(), "no corresponding field") {
			t.Errorf("got %v", err)
		}
	})
}

func TestScanIntoData_StructEmbeddedPointers(t *testing.T) {
	var got scanGetEmbedsPointer
	if err := getScanned(t, &got, []string{"promoted", "name"}, "p", "n"); err != nil {
		t.Fatal(err)
	}
	if got.ScanGetExported == nil || got.Promoted != "p" || got.Name != "n" {
		t.Errorf("nil embedded pointer must be allocated, got %+v", got)
	}
	var hidden scanTarget // embeds a pointer to an unexported type: it cannot be allocated
	if err := getScanned(t, &hidden, []string{"PointerPromoted"}, "p"); err == nil || !strings.Contains(err.Error(), `column "PointerPromoted"`) {
		t.Errorf("got %v", err)
	}
}

func TestScanIntoData_StructNameCollisions(t *testing.T) {
	type twoFields struct {
		UserID string
		Other  string
	}
	for name, cols := range map[string][]string{
		"differ by case":        {"UserID", "userid"},
		"differ by underscores": {"userid", "user_id"},
		"same name":             {"UserID", "UserID"},
	} {
		t.Run(name, func(t *testing.T) {
			err := getScanned(t, &twoFields{}, cols, "a", "b")
			if err == nil || !strings.Contains(err.Error(), "same field") {
				t.Errorf("got %v", err)
			}
		})
	}
	t.Run("an exact name beats a looser one", func(t *testing.T) {
		type spelled struct {
			UserID  string
			User_ID string
		}
		var got spelled
		if err := getScanned(t, &got, []string{"User_ID", "UserID"}, "a", "b"); err != nil {
			t.Fatal(err)
		}
		if got.User_ID != "a" || got.UserID != "b" {
			t.Errorf("got %+v", got)
		}
	})
}
