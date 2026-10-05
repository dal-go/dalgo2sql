package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	_ "modernc.org/sqlite"
)

// drainRecordset reads a recordset reader to its end. The rows are returned in result order.
func drainRecordset(t *testing.T, reader dal.RecordsetReader) (recordset.Recordset, []recordset.Row) {
	t.Helper()
	var rows []recordset.Row
	for {
		row, _, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return reader.Recordset(), rows
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		rows = append(rows, row)
	}
}

// sameCell compares what a column holds with what the test expects: want is nil for a NULL,
// and a nil that is typed (a nil []byte in an interface) is not a NULL.
func sameCell(got, want any) bool {
	if want == nil || got == nil {
		return want == nil && got == nil
	}
	return reflect.DeepEqual(got, want)
}

// requireCell reads cell (column name, row i) through every accessor the recordset has and
// requires each to answer want.
func requireCell(t *testing.T, rs recordset.Recordset, rows []recordset.Row, name string, i int, want any) {
	t.Helper()
	index := rs.GetColumnIndex(name)
	if index < 0 {
		t.Fatalf("no column %q in %v", name, columnNames(rs))
	}
	column := rs.GetColumnByIndex(index)
	got, err := column.GetValue(i)
	if err != nil || !sameCell(got, want) {
		t.Errorf("%s row %d: GetValue = %#v, %v; want %#v", name, i, got, err, want)
	}
	if values := column.Values(); i >= len(values) || !sameCell(values[i], want) {
		t.Errorf("%s row %d: Values() = %#v; want %#v at %d", name, i, values, want, i)
	}
	if got, err = rows[i].GetValueByIndex(index, rs); err != nil || !sameCell(got, want) {
		t.Errorf("%s row %d: GetValueByIndex = %#v, %v; want %#v", name, i, got, err, want)
	}
	if got, err = rows[i].GetValueByName(name, rs); err != nil || !sameCell(got, want) {
		t.Errorf("%s row %d: GetValueByName = %#v, %v; want %#v", name, i, got, err, want)
	}
	data, err := rows[i].Data(rs)
	if err != nil || index >= len(data) || !sameCell(data[index], want) {
		t.Errorf("%s row %d: Data = %#v, %v; want %#v at %d", name, i, data, err, want, index)
	}
	// The recordset answers the same for a row it is asked for again.
	if got, err = rs.GetRow(i).GetValueByIndex(index, rs); err != nil || !sameCell(got, want) {
		t.Errorf("%s row %d: GetRow(%d) = %#v, %v; want %#v", name, i, i, got, err, want)
	}
}

func columnNames(rs recordset.Recordset) []string {
	names := make([]string, rs.ColumnsCount())
	for i := range names {
		names[i] = rs.GetColumnByIndex(i).Name()
	}
	return names
}

// requireColumnType requires the column to keep the Go type and the database type it has
// always had.
func requireColumnType(t *testing.T, rs recordset.Recordset, name string, valueType reflect.Type, dbType string) {
	t.Helper()
	column := rs.GetColumnByName(name)
	if column == nil {
		t.Fatalf("no column %q in %v", name, columnNames(rs))
	}
	if column.ValueType() != valueType || column.DbType() != dbType {
		t.Errorf("%s: ValueType = %v, DbType = %q; want %v, %q", name, column.ValueType(), column.DbType(), valueType, dbType)
	}
}

// nullTestDatabase is a temporary SQLite file with one table of every column type the reader
// maps. Row 1 holds a value in each column, row 2 holds NULL in every column but its key, row 3
// holds the zero value of each type and row 4 holds NULL again, so a NULL is read in the middle
// and last of a result, and first when the result is read from row 4 down, always beside values
// and stored zeros.
func nullTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "cells.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		`CREATE TABLE cells (id INTEGER PRIMARY KEY, s TEXT, i INTEGER, f REAL, b BOOLEAN, ts TIMESTAMP, bl BLOB, n NUMERIC, v VARCHAR(5), x)`,
		`INSERT INTO cells VALUES (1, 'text', 7, 1.5, 1, '2020-01-02 03:04:05', x'0102', 3, 'v', 5)`,
		`INSERT INTO cells VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
		`INSERT INTO cells VALUES (3, '', 0, 0.0, 0, '2021-02-03 04:05:06', x'00', 0, '', 0)`,
		`INSERT INTO cells VALUES (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
		`CREATE VIEW cell_view AS SELECT id, s || 'a' AS joined, i + 1 AS next, NULL AS unset FROM cells`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return db
}

// sqliteCells is what each column of the table holds in its rows 1 and 3, as the reader returns
// it. SQLite declares a TIMESTAMP as text, so a time is read as its text.
//
// The driver takes the type of a column from the first row of the result: when that row holds
// NULL it reports none, and the column is a text column (the "text" fields), as it is on a
// result that starts with a NULL anywhere else.
var sqliteCells = []struct {
	name                   string
	valueType              reflect.Type
	dbType                 string
	one, three             any
	oneAsText, threeAsText any
}{
	{"id", reflect.TypeOf(int64(0)), "INTEGER", int64(1), int64(3), int64(1), int64(3)},
	{"s", reflect.TypeOf(""), "TEXT", "text", "", "text", ""},
	{"i", reflect.TypeOf(int64(0)), "INTEGER", int64(7), int64(0), "7", "0"},
	{"f", reflect.TypeOf(float64(0)), "REAL", 1.5, 0.0, "1.5", "0"},
	{"b", reflect.TypeOf(false), "", true, false, "1", "0"},
	{"ts", reflect.TypeOf(""), "TIMESTAMP", time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).String(), time.Date(2021, 2, 3, 4, 5, 6, 0, time.UTC).String(),
		time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC).String(), time.Date(2021, 2, 3, 4, 5, 6, 0, time.UTC).String()},
	{"bl", reflect.TypeOf([]byte(nil)), "BLOB", []byte{1, 2}, []byte{0}, "\x01\x02", "\x00"},
	{"n", reflect.TypeOf(int64(0)), "NUMERIC", int64(3), int64(0), "3", "0"},
	{"v", reflect.TypeOf(""), "VARCHAR(5)", "v", "", "v", ""},
	{"x", reflect.TypeOf(int64(0)), "", int64(5), int64(0), "5", "0"},
}

// requireSQLiteCells reads the table through reader, which returns its rows in the order of
// rowIDs, and requires NULL where the table holds it and the values beside it.
func requireSQLiteCells(t *testing.T, reader dal.RecordsetReader, rowIDs [4]int, typed bool) {
	t.Helper()
	rs, rows := drainRecordset(t, reader)
	if len(rows) != 4 {
		t.Fatalf("read %d rows, want 4", len(rows))
	}
	for _, cell := range sqliteCells {
		t.Run(cell.name, func(t *testing.T) {
			hasValue := cell.name != "id"
			one, three := cell.one, cell.three
			if !typed && hasValue {
				requireColumnType(t, rs, cell.name, reflect.TypeOf(""), "")
				one, three = cell.oneAsText, cell.threeAsText
			} else {
				requireColumnType(t, rs, cell.name, cell.valueType, cell.dbType)
			}
			for position, id := range rowIDs {
				switch {
				case id == 1:
					requireCell(t, rs, rows, cell.name, position, one)
				case id == 3:
					requireCell(t, rs, rows, cell.name, position, three)
				case hasValue:
					requireCell(t, rs, rows, cell.name, position, nil)
				default:
					requireCell(t, rs, rows, cell.name, position, int64(id))
				}
			}
		})
	}
}

func TestRecordsetReader_ANullCellIsNilOnASQLiteFile(t *testing.T) {
	db := nullTestDatabase(t)
	structured := func(order dal.OrderExpression) dal.Query {
		return dal.From(dal.NewRootCollectionRef("cells", "")).NewQuery().OrderBy(order).SelectIntoRecordset()
	}
	read := func(t *testing.T, q dal.Query, viaDatabase bool) dal.RecordsetReader {
		t.Helper()
		var reader dal.RecordsetReader
		var err error
		if viaDatabase {
			reader, err = NewDatabase(db, newSchema(), DbOptions{StructuredQueryDialect: dialectSQLite}).ExecuteQueryToRecordsetReader(context.Background(), q)
		} else {
			reader, err = getRecordsetReader(context.Background(), q, db.QueryContext)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reader.Close() })
		return reader
	}

	t.Run("a NULL in the middle and last of a text query", func(t *testing.T) {
		requireSQLiteCells(t, read(t, dal.NewTextQuery("SELECT * FROM cells ORDER BY id", nil), false), [4]int{1, 2, 3, 4}, true)
	})
	t.Run("a NULL in the middle and last of a structured query", func(t *testing.T) {
		requireSQLiteCells(t, read(t, structured(dal.AscendingField("id")), true), [4]int{1, 2, 3, 4}, true)
	})
	t.Run("a NULL first of a text query", func(t *testing.T) {
		requireSQLiteCells(t, read(t, dal.NewTextQuery("SELECT * FROM cells ORDER BY id DESC", nil), false), [4]int{4, 3, 2, 1}, false)
	})
	t.Run("a NULL first of a structured query", func(t *testing.T) {
		requireSQLiteCells(t, read(t, structured(dal.DescendingField("id")), true), [4]int{4, 3, 2, 1}, false)
	})
}

func TestRecordsetReader_ANullCellOfAViewColumnWithNoScanTypeIsNil(t *testing.T) {
	db := nullTestDatabase(t)
	reader, err := getRecordsetReader(context.Background(), dal.NewTextQuery("SELECT * FROM cell_view ORDER BY id", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rs, rows := drainRecordset(t, reader)
	// "unset" is NULL in every row and has no scan type: it is a text column that holds none.
	requireColumnType(t, rs, "unset", reflect.TypeOf(""), "")
	for i := range rows {
		requireCell(t, rs, rows, "unset", i, nil)
	}
	for i, want := range []any{"texta", nil, "a", nil} {
		requireCell(t, rs, rows, "joined", i, want)
	}
	for i, want := range []any{int64(8), nil, int64(1), nil} {
		requireCell(t, rs, rows, "next", i, want)
	}
}

func TestRecordsetReader_AnAggregateOverNoRowsIsNil(t *testing.T) {
	db := nullTestDatabase(t)
	const statement = "SELECT SUM(i) AS total, MAX(s) AS top, MAX(f) AS peak, COUNT(*) AS n FROM cells WHERE id > 100"

	check := func(t *testing.T, reader dal.RecordsetReader) {
		t.Helper()
		rs, rows := drainRecordset(t, reader)
		if len(rows) != 1 {
			t.Fatalf("read %d rows, want 1", len(rows))
		}
		for _, name := range []string{"total", "top", "peak"} {
			requireCell(t, rs, rows, name, 0, nil)
		}
		requireCell(t, rs, rows, "n", 0, int64(0))
	}

	t.Run("text query", func(t *testing.T) {
		reader, err := getRecordsetReader(context.Background(), dal.NewTextQuery(statement, nil), db.QueryContext)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		check(t, reader)
	})

	t.Run("structured aggregate", func(t *testing.T) {
		q := dal.From(dal.NewRootCollectionRef("cells", "")).NewQuery().
			WhereField("id", dal.GreaterThen, 100).
			SelectColumns(dal.SumAs(dal.Field("i"), "total"), dal.MaxAs(dal.Field("s"), "top"), dal.MaxAs(dal.Field("f"), "peak"), dal.CountAs(dal.Field("id"), "n"))
		reader, err := NewDatabase(db, newSchema(), DbOptions{StructuredQueryDialect: dialectSQLite}).ExecuteQueryToRecordsetReader(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		check(t, reader)
	})
}

// A column of each type the reader builds, read through go-sqlmock, which hands the reader
// the scan type the test declares and the value the test gives.
func TestRecordsetReader_ANullCellOfEveryColumnTypeIsNil(t *testing.T) {
	when := time.Date(2022, 3, 4, 5, 6, 7, 0, time.UTC)
	var bytePointer *byte
	var anyPointer *any
	cells := []struct {
		name      string
		column    *sqlmock.Column
		value     any
		want      any // what the reader returns for value
		valueType reflect.Type
		dbType    string
	}{
		{"text", sqlmock.NewColumn("text").OfType("VARCHAR", ""), "t", "t", reflect.TypeOf(""), "VARCHAR"},
		{"int32", sqlmock.NewColumn("int32").OfType("INT", int32(0)), int64(5), int64(5), reflect.TypeOf(int64(0)), "INT"},
		{"uint8", sqlmock.NewColumn("uint8").OfType("TINYINT", uint8(0)), int64(6), int64(6), reflect.TypeOf(int64(0)), "TINYINT"},
		{"float32", sqlmock.NewColumn("float32").OfType("FLOAT", float32(0)), 1.25, 1.25, reflect.TypeOf(float64(0)), "FLOAT"},
		{"float64", sqlmock.NewColumn("float64").OfType("DOUBLE", float64(0)), 2.5, 2.5, reflect.TypeOf(float64(0)), "DOUBLE"},
		{"bool", sqlmock.NewColumn("bool").OfType("BOOL", true), true, true, reflect.TypeOf(false), ""},
		{"time", sqlmock.NewColumn("time").OfType("TIMESTAMP", time.Time{}), when, when, reflect.TypeOf(time.Time{}), "TIMESTAMP"},
		{"bytes", sqlmock.NewColumn("bytes").OfType("BLOB", []byte{}), []byte("b"), []byte("b"), reflect.TypeOf([]byte(nil)), "BLOB"},
		{"string slice", sqlmock.NewColumn("string slice").OfType("TEXT[]", []string{}), "{a}", "{a}", reflect.TypeOf(""), "TEXT[]"},
		{"NullString", sqlmock.NewColumn("NullString").OfType("TEXT", sql.NullString{}), "s", "s", reflect.TypeOf(""), "TEXT"},
		{"NullByte", sqlmock.NewColumn("NullByte").OfType("TINYINT", sql.NullByte{}), int64(3), int64(3), reflect.TypeOf(int64(0)), "TINYINT"},
		{"NullInt16", sqlmock.NewColumn("NullInt16").OfType("SMALLINT", sql.NullInt16{}), int64(4), int16(4), reflect.TypeOf(int16(0)), "SMALLINT"},
		{"NullInt32", sqlmock.NewColumn("NullInt32").OfType("INT", sql.NullInt32{}), int64(5), int32(5), reflect.TypeOf(int32(0)), "INT"},
		{"NullInt64", sqlmock.NewColumn("NullInt64").OfType("BIGINT", sql.NullInt64{}), int64(6), int64(6), reflect.TypeOf(int64(0)), "BIGINT"},
		{"NullFloat64", sqlmock.NewColumn("NullFloat64").OfType("DOUBLE", sql.NullFloat64{}), 2.5, 2.5, reflect.TypeOf(float64(0)), "DOUBLE"},
		{"NullBool", sqlmock.NewColumn("NullBool").OfType("BOOL", sql.NullBool{}), true, true, reflect.TypeOf(false), ""},
		{"NullTime", sqlmock.NewColumn("NullTime").OfType("TIMESTAMP", sql.NullTime{}), when, when, reflect.TypeOf(time.Time{}), "TIMESTAMP"},
		{"pointer to byte", sqlmock.NewColumn("pointer to byte").OfType("BLOB", bytePointer), []byte("p"), []byte("p"), reflect.TypeOf([]byte(nil)), "BLOB"},
		{"pointer to interface", sqlmock.NewColumn("pointer to interface").OfType("ANY", anyPointer), "any", "any", reflect.TypeOf(""), ""},
		{"no scan type", sqlmock.NewColumn("no scan type"), "plain", "plain", reflect.TypeOf(""), ""},
	}
	columns := make([]*sqlmock.Column, len(cells))
	values := make([]driver.Value, len(cells))
	nulls := make([]driver.Value, len(cells))
	for i, cell := range cells {
		columns[i], values[i] = cell.column, cell.value
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	// A NULL first, a value, and a NULL last.
	mock.ExpectQuery("SELECT every type").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(columns...).AddRow(nulls...).AddRow(values...).AddRow(nulls...))

	reader, err := getRecordsetReader(context.Background(), dal.NewTextQuery("SELECT every type", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rs, rows := drainRecordset(t, reader)
	if len(rows) != 3 {
		t.Fatalf("read %d rows, want 3", len(rows))
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			requireColumnType(t, rs, cell.name, cell.valueType, cell.dbType)
			requireCell(t, rs, rows, cell.name, 0, nil)
			requireCell(t, rs, rows, cell.name, 1, cell.want)
			requireCell(t, rs, rows, cell.name, 2, nil)
		})
	}
}

// A driver that reports no column types at all (database/sql then describes each column as
// an interface) is read as bytes: a NULL is still nil, and a value is still its bytes.
func TestRecordsetReader_ANullCellOfAnInterfaceColumnIsNil(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	mock.ExpectQuery("SELECT blob").WillReturnRows(sqlmock.NewRows([]string{"blob"}).AddRow(nil).AddRow([]byte("bytes")).AddRow(nil))
	reader, err := getRecordsetReader(context.Background(), dal.NewTextQuery("SELECT blob", nil), db.QueryContext)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rs, rows := drainRecordset(t, reader)
	requireColumnType(t, rs, "blob", reflect.TypeOf([]byte(nil)), "")
	requireCell(t, rs, rows, "blob", 0, nil)
	requireCell(t, rs, rows, "blob", 1, []byte("bytes"))
	requireCell(t, rs, rows, "blob", 2, nil)
}
