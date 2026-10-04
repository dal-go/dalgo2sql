package dalgo2sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type scanCity struct {
	Name       string
	Population int
	AreaSqKm   int
	IsCapital  bool
	Amount     float64
	Raw        any
}

// structTargetReader builds a reader whose records carry a *scanCity.
func structTargetReader(t *testing.T, rows *sqlmock.Rows, query dal.Query, options DbOptions) *recordsReader {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDatabase(t, db) })
	mock.ExpectQuery(".*").WillReturnRows(rows)
	rr, err := getRecordsReaderWithOptions(context.Background(), query, db.QueryContext, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rr.Close() })
	return rr
}

func structTargetQuery() dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectIntoRecord(func() dalrecord.Record {
		return dalrecord.NewRecordWithIncompleteKey("cities", reflect.String, &scanCity{})
	})
}

func TestRecordsReader_ScansIntoStruct(t *testing.T) {
	rows := sqlmock.NewRows([]string{"id", "NAME", "population", "area_sq_km", "IsCapital", "unrelated"}).
		AddRow("tokyo", []byte("Tokyo"), int64(37400068), int64(2187), int64(1), "ignored")
	options := DbOptions{Recordsets: map[string]*Recordset{
		"cities": NewRecordset("cities", Table, []dal.FieldRef{dal.Field("id")}),
	}}
	rr := structTargetReader(t, rows, structTargetQuery(), options)
	rec, err := rr.Next()
	if err != nil {
		t.Fatal(err)
	}
	got := *rec.Data().(*scanCity)
	want := scanCity{Name: "Tokyo", Population: 37400068, AreaSqKm: 2187, IsCapital: true}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if rec.Key().ID != "tokyo" {
		t.Errorf("identity column must set the key, got %v", rec.Key().ID)
	}
}

// The value normalisation of the map target applies to a struct target too.
func TestRecordsReader_StructTargetGetsNormalisedValues(t *testing.T) {
	col := func(name, dbType string, scanType any) *sqlmock.Column {
		return sqlmock.NewColumn(name).OfType(dbType, scanType)
	}
	rows := sqlmock.NewRowsWithColumnDefinition(
		col("Amount", "NUMERIC", float64(0)),
		col("Raw", "NUMERIC", float64(0)),
		col("Name", "VARCHAR", ""),
	).AddRow("13.86", "14.5", []byte("x"))
	rr := structTargetReader(t, rows, structTargetQuery(), DbOptions{})
	rec, err := rr.Next()
	if err != nil {
		t.Fatal(err)
	}
	got := *rec.Data().(*scanCity)
	if got.Amount != 13.86 || got.Raw != 14.5 || got.Name != "x" {
		t.Errorf("got %+v (Raw is %T)", got, got.Raw)
	}
}

func TestRecordsReader_StructTargetRejectsNonFiniteAggregates(t *testing.T) {
	rows := sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("Amount").OfType("NUMERIC", float64(0))).AddRow("NaN")
	rr := structTargetReader(t, rows, structTargetQuery(), DbOptions{})
	rr.validateFinite = true
	if _, err := rr.Next(); err == nil || !strings.Contains(err.Error(), "non-finite") {
		t.Errorf("want non-finite error, got %v", err)
	}
}

func TestRecordsReader_StructTargetAssignmentError(t *testing.T) {
	rows := sqlmock.NewRows([]string{"Population"}).AddRow("many")
	rr := structTargetReader(t, rows, structTargetQuery(), DbOptions{})
	if _, err := rr.Next(); err == nil || !strings.Contains(err.Error(), `column "Population"`) {
		t.Errorf("want assignment error naming the column, got %v", err)
	}
}

func TestRecordsReader_StructTargetScanError(t *testing.T) {
	rows := sqlmock.NewRows([]string{"Name"}).AddRow("x")
	rr := structTargetReader(t, rows, structTargetQuery(), DbOptions{})
	rr.scanColNames = append(rr.scanColNames, "extra") // more targets than the row has columns
	rr.visibleIndexes = append(rr.visibleIndexes, 1)
	if _, err := rr.Next(); err == nil {
		t.Error("want scan error")
	}
}

func TestRecordsReader_UnsupportedTargets(t *testing.T) {
	for name, data := range map[string]any{
		"struct value":     scanCity{},
		"pointer to int":   new(int),
		"nil struct ptr":   (*scanCity)(nil),
		"slice of any":     []any{},
		"int":              1,
		"unexported types": struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			rows := sqlmock.NewRows([]string{"Name"}).AddRow("x")
			query := dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectIntoRecord(func() dalrecord.Record {
				return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("cities", "1"), data)
			})
			rr := structTargetReader(t, rows, query, DbOptions{})
			if _, err := rr.Next(); err == nil || !strings.HasPrefix(err.Error(), "unsupported data type") {
				t.Errorf("got %v", err)
			}
		})
	}
}

type scanReaderShapes struct {
	Blob    []byte
	Doc     json.RawMessage
	Custom  bytesOnlyScanner
	Label   string
	Decimal decimalTextScanner
	Count   int
	Created sql.NullString
	Any     any
}

// decimalTextScanner keeps the exact text a driver delivers for a NUMERIC.
type decimalTextScanner struct{ text string }

func (d *decimalTextScanner) Scan(value any) error {
	switch v := value.(type) {
	case string:
		d.text = v
	case []byte:
		d.text = string(v)
	default:
		return fmt.Errorf("decimalTextScanner wants the driver text, got %T", value)
	}
	return nil
}

type ScanReaderExported struct{ Promoted string }

type scanReaderEmbedsPointer struct {
	*ScanReaderExported
	Name string
}

func shapesQuery(data any) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectIntoRecord(func() dalrecord.Record {
		return dalrecord.NewRecordWithIncompleteKey("cities", reflect.String, data)
	})
}

func nextStruct(t *testing.T, data any, rows *sqlmock.Rows) error {
	t.Helper()
	rr := structTargetReader(t, rows, shapesQuery(data), DbOptions{})
	_, err := rr.Next()
	return err
}

// A struct target takes the driver's value, so []byte reaches []byte fields
// and Scanners as []byte instead of as the string a map target stores.
func TestRecordsReader_StructTargetByteValues(t *testing.T) {
	var got scanReaderShapes
	rows := sqlmock.NewRows([]string{"blob", "doc", "custom", "label", "any"}).
		AddRow([]byte{1, 2}, []byte(`{"a":1}`), []byte("raw"), []byte("text"), []byte("as string"))
	if err := nextStruct(t, &got, rows); err != nil {
		t.Fatal(err)
	}
	if string(got.Blob) != "\x01\x02" || string(got.Doc) != `{"a":1}` || string(got.Custom.got) != "raw" || got.Label != "text" {
		t.Errorf("got %+v", got)
	}
	if got.Any != "as string" {
		t.Errorf("an interface field takes the string a map target stores, got %T(%v)", got.Any, got.Any)
	}
}

func TestRecordsReader_StructTargetNULL(t *testing.T) {
	got := scanReaderShapes{Count: 3, Created: sql.NullString{String: "x", Valid: true}}
	if err := nextStruct(t, &got, sqlmock.NewRows([]string{"count", "created", "blob"}).AddRow(nil, nil, nil)); err != nil {
		t.Fatal(err)
	}
	if got.Count != 0 || got.Created.Valid || got.Blob != nil {
		t.Errorf("NULL stores the zero value, got %+v", got)
	}
}

// NUMERIC normalisation turns the text into float64 for numeric and interface
// fields; a string field or a Scanner gets the driver's text unchanged.
func TestRecordsReader_StructTargetNumericText(t *testing.T) {
	col := func(name string) *sqlmock.Column { return sqlmock.NewColumn(name).OfType("NUMERIC", float64(0)) }
	rows := sqlmock.NewRowsWithColumnDefinition(col("Label"), col("Decimal"), col("Any")).
		AddRow("12345678901234567890.123456789", []byte("0.1000000000000000055511151231257827"), "14.5")
	var got scanReaderShapes
	if err := nextStruct(t, &got, rows); err != nil {
		t.Fatal(err)
	}
	if got.Label != "12345678901234567890.123456789" || got.Decimal.text != "0.1000000000000000055511151231257827" {
		t.Errorf("got %+v", got)
	}
	if got.Any != 14.5 {
		t.Errorf("interface field keeps the normalised value, got %T(%v)", got.Any, got.Any)
	}
}

func TestRecordsReader_StructTargetEmbeddedPointers(t *testing.T) {
	var got scanReaderEmbedsPointer
	if err := nextStruct(t, &got, sqlmock.NewRows([]string{"promoted", "name"}).AddRow("p", "n")); err != nil {
		t.Fatal(err)
	}
	if got.ScanReaderExported == nil || got.Promoted != "p" || got.Name != "n" {
		t.Errorf("nil embedded pointer must be allocated, got %+v", got)
	}
	var hidden scanTarget // embeds a pointer to an unexported type: it cannot be allocated
	if err := nextStruct(t, &hidden, sqlmock.NewRows([]string{"PointerPromoted"}).AddRow("p")); err == nil || !strings.Contains(err.Error(), `column "PointerPromoted"`) {
		t.Errorf("got %v", err)
	}
}

func TestRecordsReader_StructTargetNameCollisions(t *testing.T) {
	type twoFields struct {
		UserID string
		Other  string
	}
	for name, cols := range map[string][]string{
		"differ by case":        {"UserID", "userid"},
		"differ by underscores": {"userid", "user_id"},
	} {
		t.Run(name, func(t *testing.T) {
			err := nextStruct(t, &twoFields{}, sqlmock.NewRows(cols).AddRow("a", "b"))
			if err == nil || !strings.Contains(err.Error(), "same field") {
				t.Errorf("got %v", err)
			}
		})
	}
}

// PostgreSQL returns NUMERIC for SUM(bigint): an integer field must get every
// digit of it, not the float64 the normaliser makes.
func TestRecordsReader_StructTargetIntegerFieldsOverNumericText(t *testing.T) {
	type totals struct {
		Total int64
		Big   uint64
		Max   int64
	}
	col := func(name string) *sqlmock.Column { return sqlmock.NewColumn(name).OfType("NUMERIC", float64(0)) }
	rows := sqlmock.NewRowsWithColumnDefinition(col("Total"), col("Big"), col("Max")).
		AddRow("9007199254740993", []byte("18446744073709551615"), "9223372036854775807")
	var got totals
	if err := nextStruct(t, &got, rows); err != nil {
		t.Fatal(err)
	}
	if got != (totals{Total: 9007199254740993, Big: math.MaxUint64, Max: math.MaxInt64}) {
		t.Errorf("got %+v", got)
	}
}
