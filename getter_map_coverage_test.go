package dalgo2sql

import (
	"context"
	"testing"

	"github.com/dal-go/dalgo/dal"
	_ "modernc.org/sqlite"
)

func TestIsMapData(t *testing.T) {
	m := map[string]any{}
	for _, tt := range []struct {
		name string
		data any
		want bool
	}{
		{"nil", nil, false},
		{"map", m, true},
		{"pointer_to_map", &m, true},
		{"struct", struct{ Name string }{}, false},
		{"int", 42, false},
		{"map_non_string_key", map[int]any{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMapData(tt.data); got != tt.want {
				t.Errorf("isMapData(%v) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}

// TestMapDataGetIntoNilMapPointer covers scanRowIntoMap initializing a nil map
// when the record data is a *map[string]any pointing to a nil map.
func TestMapDataGetIntoNilMapPointer(t *testing.T) {
	ctx := context.Background()

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"widgets": NewRecordset("widgets", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	createSQL := `CREATE TABLE widgets (
		id   TEXT PRIMARY KEY,
		name TEXT NOT NULL
	)`
	sqlDB := openTestSQLiteDB(t, createSQL)
	db := NewDatabase(sqlDB, newSchema(), opts).(*database)

	rec := dal.NewRecordWithData(
		dal.NewKeyWithID("widgets", "w1"),
		map[string]any{"name": "Sprocket"},
	)
	if err := db.Insert(ctx, rec); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var got map[string]any // nil map
	getRec := dal.NewRecordWithData(dal.NewKeyWithID("widgets", "w1"), &got)
	if err := db.Get(ctx, getRec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected map to be initialized, got nil")
	}
	if got["name"] != "Sprocket" {
		t.Errorf("name = %v, want Sprocket", got["name"])
	}
}

// TestMapDataGetBlobAsString covers the []byte -> string conversion in
// scanRowIntoMap: BLOB columns are returned as []byte by the driver and should
// be converted to a string for usability.
func TestMapDataGetBlobAsString(t *testing.T) {
	ctx := context.Background()

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"blobs": NewRecordset("blobs", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}
	createSQL := `CREATE TABLE blobs (
		id      TEXT PRIMARY KEY,
		payload BLOB NOT NULL
	)`
	sqlDB := openTestSQLiteDB(t, createSQL)
	db := NewDatabase(sqlDB, newSchema(), opts).(*database)

	// Insert a genuine BLOB value directly so the driver returns []byte on read.
	if _, err := sqlDB.Exec(`INSERT INTO blobs (id, payload) VALUES (?, ?)`, "b1", []byte("hello-blob")); err != nil {
		t.Fatalf("insert blob: %v", err)
	}

	got := make(map[string]any)
	getRec := dal.NewRecordWithData(dal.NewKeyWithID("blobs", "b1"), got)
	if err := db.Get(ctx, getRec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s, ok := got["payload"].(string); !ok || s != "hello-blob" {
		t.Errorf("payload = %#v, want string %q", got["payload"], "hello-blob")
	}
}
