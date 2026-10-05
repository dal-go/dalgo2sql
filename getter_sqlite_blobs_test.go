package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	_ "modernc.org/sqlite"
)

func TestSQLiteMapReadsPreserveBlobTextAndNullValues(t *testing.T) {
	ctx := context.Background()
	createSQL := `CREATE TABLE "Asset Table" (
		id TEXT PRIMARY KEY,
		payload BLOB,
		empty BLOB,
		nullable BLOB,
		note TEXT
	)`
	sqlDB := openTestSQLiteDB(t, createSQL)
	rows := []struct {
		id      string
		payload []byte
		empty   []byte
		null    any
		note    string
	}{
		{"asset-1", []byte{0x00, 0xff, 0x80, 0x41}, []byte{}, nil, "ordinary text"},
		{"asset-2", []byte{0x7f, 0xfe, 0x00}, []byte{}, nil, "second row"},
	}
	for _, row := range rows {
		if _, err := sqlDB.Exec(`INSERT INTO "Asset Table" (id,payload,empty,nullable,note) VALUES (?,?,x'',NULL,?)`, row.id, row.payload, row.note); err != nil {
			t.Fatal(err)
		}
	}
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"Asset Table": NewRecordset("Asset Table", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})

	t.Run("Get map", func(t *testing.T) {
		got := map[string]any{}
		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "asset-1"), got)
		if err := db.Get(ctx, record); err != nil {
			t.Fatal(err)
		}
		assertSQLiteMapValues(t, got, rows[0])
	})
	t.Run("Get pointer to nil map", func(t *testing.T) {
		var got map[string]any
		record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "asset-1"), &got)
		if err := db.Get(ctx, record); err != nil {
			t.Fatal(err)
		}
		assertSQLiteMapValues(t, got, rows[0])
	})

	t.Run("GetMulti maps", func(t *testing.T) {
		first, second := map[string]any{}, map[string]any{}
		records := []dalrecord.Record{
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "asset-1"), first),
			dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "asset-2"), second),
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatal(err)
		}
		assertSQLiteMapValues(t, first, rows[0])
		assertSQLiteMapValues(t, second, rows[1])
	})

	t.Run("structured query map records", func(t *testing.T) {
		query := dal.From(dal.NewRootCollectionRef("Asset Table", "")).NewQuery().OrderBy(dal.AscendingField("id")).SelectIntoRecordset()
		reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		for _, want := range rows {
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if record.Key().ID != want.id {
				t.Fatalf("record ID = %v, want %s", record.Key().ID, want.id)
			}
			assertSQLiteMapValues(t, record.Data().(map[string]any), want)
		}
		if _, err := reader.Next(); err != dal.ErrNoMoreRecords {
			t.Fatalf("reader.Next() after rows = %v, want ErrNoMoreRecords", err)
		}
	})
}

func TestSQLiteGetMultiMarksOnlyMissingMapRecordsNotFound(t *testing.T) {
	ctx := context.Background()
	sqlDB := openTestSQLiteDB(t, `CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT)`)
	if _, err := sqlDB.Exec(`INSERT INTO widgets VALUES ('present', 'Sprocket')`); err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"widgets": NewRecordset("widgets", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	present, missing := map[string]any{}, map[string]any{}
	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "present"), present),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "missing"), missing),
	}
	if err := db.GetMulti(ctx, records); err != nil {
		t.Fatal(err)
	}
	if present["name"] != "Sprocket" || !records[0].Exists() {
		t.Fatalf("present record = %#v, exists %v", present, records[0].Exists())
	}
	if records[1].Exists() {
		t.Fatalf("missing record unexpectedly exists: %#v", missing)
	}
}

func TestDefaultGetAndGetMultiKeepTheirHistoricalNullMapBehavior(t *testing.T) {
	ctx := context.Background()
	sqlDB := openTestSQLiteDB(t, `CREATE TABLE widgets (id TEXT PRIMARY KEY, nullable TEXT)`)
	for _, id := range []string{"one", "two"} {
		if _, err := sqlDB.Exec(`INSERT INTO widgets VALUES (?, NULL)`, id); err != nil {
			t.Fatal(err)
		}
	}
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"widgets": NewRecordset("widgets", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})

	single := map[string]any{"nullable": "keep on single Get"}
	if err := db.Get(ctx, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "one"), single)); err != nil {
		t.Fatal(err)
	}
	if single["nullable"] != "keep on single Get" {
		t.Errorf("single Get changed reused NULL map value: %#v", single)
	}

	first := map[string]any{"nullable": "delete on GetMulti"}
	second := map[string]any{"nullable": "delete on GetMulti"}
	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "one"), first),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "two"), second),
	}
	if err := db.GetMulti(ctx, records); err != nil {
		t.Fatal(err)
	}
	for _, got := range []map[string]any{first, second} {
		if _, ok := got["nullable"]; ok {
			t.Errorf("GetMulti retained reused NULL map value: %#v", got)
		}
	}
}

func TestSingleMapGetAfterNotFoundClearsPreviousRecordError(t *testing.T) {
	ctx := context.Background()
	sqlDB := openTestSQLiteDB(t, `CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT)`)
	db := NewDatabase(sqlDB, newSchema(), DbOptions{
		Recordsets: map[string]*Recordset{
			"widgets": NewRecordset("widgets", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("widgets", "new"), map[string]any{})
	if err := db.Get(ctx, record); !errors.Is(err, dalrecord.ErrRecordNotFound) {
		t.Fatalf("initial Get error = %v, want record-not-found", err)
	}
	if record.Exists() {
		t.Fatal("record should remain not found before insertion")
	}
	if _, err := sqlDB.Exec(`INSERT INTO widgets VALUES ('new', 'inserted')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(ctx, record); err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if !record.Exists() || record.Error() != nil {
		t.Fatalf("successful reused record state: exists=%v error=%v", record.Exists(), record.Error())
	}
	if got := record.Data().(map[string]any)["name"]; got != "inserted" {
		t.Fatalf("record name = %#v, want inserted", got)
	}
}

func TestNormalizeReadMapValueUsesDialectAndColumnType(t *testing.T) {
	ctx := context.Background()
	sqlDB := openTestSQLiteDB(t, `CREATE TABLE typed (blob_value BLOB, text_value TEXT, number_value INTEGER)`)
	if _, err := sqlDB.Exec(`INSERT INTO typed VALUES (x'00ff', 'plain text', 7)`); err != nil {
		t.Fatal(err)
	}
	rows, err := sqlDB.QueryContext(ctx, `SELECT blob_value, text_value, number_value FROM typed`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected one typed row")
	}

	if got := normalizeReadMapValue([]byte{0, 0xff}, columnTypes[0], "sqlite"); !reflect.DeepEqual(got, []byte{0, 0xff}) {
		t.Errorf("SQLite BLOB = %#v, want exact bytes", got)
	}
	if got := normalizeReadMapValue([]byte("text"), columnTypes[1], "sqlite"); got != "text" {
		t.Errorf("SQLite TEXT = %#v, want string", got)
	}
	if got := normalizeReadMapValue([]byte("text"), columnTypes[1], "postgres"); got != "text" {
		t.Errorf("non-SQLite byte normalization = %#v, want string", got)
	}
	if got := normalizeReadMapValue([]byte{0, 0xff}, columnTypes[2], "sqlite"); !reflect.DeepEqual(got, []byte{0, 0xff}) {
		t.Errorf("SQLite ambiguous bytes = %#v, want preserved bytes", got)
	}
	if got := normalizeReadMapValue([]byte("fallback"), nil, "sqlite"); !reflect.DeepEqual(got, []byte("fallback")) {
		t.Errorf("SQLite unknown type = %#v, want bytes", got)
	}
	if got := normalizeReadMapValue([]byte(nil), columnTypes[0], "sqlite"); !reflect.DeepEqual(got, []byte{}) {
		t.Errorf("SQLite empty BLOB = %#v, want non-nil empty bytes", got)
	}
	if got := normalizeReadMapValue(nil, columnTypes[0], "sqlite"); got != nil {
		t.Errorf("SQL NULL = %#v, want nil", got)
	}
	if columnTypeAt(columnTypes, 0) != columnTypes[0] || columnTypeAt(columnTypes, -1) != nil || columnTypeAt(columnTypes, len(columnTypes)) != nil {
		t.Fatal("columnTypeAt did not safely select or reject the requested index")
	}
}

func TestSQLiteMapReadScanFailureIsReturned(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	mock.ExpectQuery("SELECT payload FROM blobs").WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow([]byte{0xff}))
	rows, err := sqlDB.Query(`SELECT payload FROM blobs`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string]any{}
	if err := scanRowIntoMapWithOptions(rows, got, false, DbOptions{StructuredQueryDialect: "sqlite"}); err == nil {
		t.Fatal("scanRowIntoMap should return the error when Scan is called before advancing Rows")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteGetMultiReturnsClosedRowsMetadataError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	mock.ExpectQuery(`SELECT .*`).
		WithArgs("one", "two").
		WillReturnRows(sqlmock.NewRows([]string{"id", "payload"}).AddRow("one", []byte{0xff}))
	exec := func(query string, args ...interface{}) (*sql.Rows, error) {
		rows, err := sqlDB.Query(query, args...)
		if err == nil {
			_ = rows.Close()
		}
		return rows, err
	}
	options := DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"Asset Table": NewRecordset("Asset Table", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "one"), map[string]any{}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Asset Table", "two"), map[string]any{}),
	}
	if err := getMultiFromSingleTable(context.Background(), options, records, exec); err == nil {
		t.Fatal("getMultiFromSingleTable should fail closed when row metadata is unavailable")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type fakeMapColumnMetadataReader struct {
	columns    []string
	columnType []*sql.ColumnType
	columnsErr error
	typesErr   error
	closed     bool
}

func (r *fakeMapColumnMetadataReader) Columns() ([]string, error) {
	return r.columns, r.columnsErr
}

func (r *fakeMapColumnMetadataReader) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.columnType, r.typesErr
}

func (r *fakeMapColumnMetadataReader) Close() error {
	r.closed = true
	return nil
}

func TestMapColumnMetadataErrorsCloseRowsAndSkipUse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reader  fakeMapColumnMetadataReader
		wantErr error
	}{
		{name: "columns", reader: fakeMapColumnMetadataReader{columnsErr: errors.New("columns failed")}, wantErr: errors.New("columns failed")},
		{name: "column types", reader: fakeMapColumnMetadataReader{columns: []string{"payload"}, typesErr: errors.New("column types failed")}, wantErr: errors.New("column types failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			used := false
			err := withMapColumnMetadata(&tc.reader, func([]string, []*sql.ColumnType) error {
				used = true
				return nil
			})
			if err == nil || err.Error() != tc.wantErr.Error() {
				t.Fatalf("withMapColumnMetadata error = %v, want %v", err, tc.wantErr)
			}
			if !tc.reader.closed {
				t.Fatal("metadata failure did not close rows")
			}
			if used {
				t.Fatal("metadata callback ran after a failure")
			}
		})
	}
}

type failingMapRowsScanner struct {
	scanErr error
}

func (r failingMapRowsScanner) Next() bool        { return true }
func (r failingMapRowsScanner) Scan(...any) error { return r.scanErr }
func (r failingMapRowsScanner) Err() error        { return nil }

type failingMapRowsErrScanner struct{ err error }

func (r failingMapRowsErrScanner) Next() bool        { return false }
func (r failingMapRowsErrScanner) Scan(...any) error { return nil }
func (r failingMapRowsErrScanner) Err() error        { return r.err }

func TestFillMapRecordsReturnsScanError(t *testing.T) {
	wantErr := errors.New("bad row scan")
	_, err := fillMapRecords(failingMapRowsScanner{scanErr: wantErr}, []string{"id"}, nil, nil, "id", "sqlite")
	if !errors.Is(err, wantErr) {
		t.Fatalf("fillMapRecords error = %v, want %v", err, wantErr)
	}
}

func TestFillMapRecordsReturnsRowsError(t *testing.T) {
	wantErr := errors.New("row iteration failed")
	_, err := fillMapRecords(failingMapRowsErrScanner{err: wantErr}, []string{"id"}, nil, nil, "id", "sqlite")
	if !errors.Is(err, wantErr) {
		t.Fatalf("fillMapRecords error = %v, want %v", err, wantErr)
	}
}

func TestSetReadMapValueDeletesStaleNonSQLiteNull(t *testing.T) {
	got := map[string]any{"nullable": "stale"}
	setReadMapValue(reflect.ValueOf(got), "nullable", nil, "postgres")
	if _, ok := got["nullable"]; ok {
		t.Fatalf("nullable key remained in map after legacy nil assignment: %#v", got)
	}
}

func TestSetReadMapValueSupportsNamedStringMapKeys(t *testing.T) {
	type columnName string
	got := map[columnName]any{}
	setReadMapValue(reflect.ValueOf(got), "payload", []byte{0xff}, "sqlite")
	if !reflect.DeepEqual(got[columnName("payload")], []byte{0xff}) {
		t.Fatalf("named-key map value = %#v", got)
	}
}

func assertSQLiteMapValues(t *testing.T, got map[string]any, want struct {
	id      string
	payload []byte
	empty   []byte
	null    any
	note    string
}) {
	t.Helper()
	if !reflect.DeepEqual(got["payload"], want.payload) {
		t.Errorf("payload = %#v, want exact bytes %#v", got["payload"], want.payload)
	}
	empty, ok := got["empty"].([]byte)
	if !ok || empty == nil || len(empty) != 0 {
		t.Errorf("empty = %#v, want a non-nil empty []byte", got["empty"])
	}
	null, ok := got["nullable"]
	if !ok || null != nil {
		t.Errorf("nullable = %#v (present=%v), want present SQL NULL", null, ok)
	}
	if got["note"] != want.note {
		t.Errorf("note = %#v, want text %q", got["note"], want.note)
	}
}
