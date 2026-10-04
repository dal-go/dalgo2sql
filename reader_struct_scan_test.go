package dalgo2sql

import (
	"context"
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
