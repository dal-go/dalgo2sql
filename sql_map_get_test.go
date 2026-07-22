package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	_ "modernc.org/sqlite"
)

// openTestSQLiteDB opens an in-memory SQLite database using modernc.org/sqlite
// (pure Go, no CGo) and creates the given table schema. It returns the *sql.DB
// and a cleanup function.
func openTestSQLiteDB(t *testing.T, createSQL string) *sql.DB {
	t.Helper()
	// Each test gets an isolated in-memory database via a unique URI.
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	return db
}

func TestMapDataGetRoundTrip(t *testing.T) {
	ctx := context.Background()

	opts := DbOptions{
		Recordsets: map[string]*Recordset{
			"widgets": NewRecordset("widgets", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}

	createSQL := `CREATE TABLE widgets (
		id    TEXT PRIMARY KEY,
		name  TEXT NOT NULL,
		price TEXT NOT NULL
	)`

	newDB := func(t *testing.T) *database {
		t.Helper()
		sqlDB := openTestSQLiteDB(t, createSQL)
		return NewDatabase(sqlDB, newSchema(), opts).(*database)
	}

	t.Run("Insert_then_Get_map", func(t *testing.T) {
		db := newDB(t)

		// Insert via map[string]any.
		rec := record.NewRecordWithData(
			record.NewKeyWithID("widgets", "w1"),
			map[string]any{"name": "Sprocket", "price": "9.99"},
		)
		if err := db.Insert(ctx, rec); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		// Get back into a new map.
		got := make(map[string]any)
		getRec := record.NewRecordWithData(record.NewKeyWithID("widgets", "w1"), got)
		if err := db.Get(ctx, getRec); err != nil {
			t.Fatalf("Get: %v", err)
		}

		if got["name"] != "Sprocket" {
			t.Errorf("name = %v, want Sprocket", got["name"])
		}
		if got["price"] != "9.99" {
			t.Errorf("price = %v, want 9.99", got["price"])
		}
		// PK column is included in SELECT *, so it should be present in the map.
		if got["id"] != "w1" {
			t.Errorf("id = %v, want w1", got["id"])
		}
	})

	t.Run("Get_missing_returns_IsNotFound", func(t *testing.T) {
		db := newDB(t)

		got := make(map[string]any)
		getRec := record.NewRecordWithData(record.NewKeyWithID("widgets", "missing"), got)
		err := db.Get(ctx, getRec)
		if err == nil {
			t.Fatal("expected error for missing record, got nil")
		}
		if !record.IsNotFound(err) {
			t.Errorf("expected IsNotFound error, got: %v", err)
		}
	})

	t.Run("Set_upsert_map_then_Get", func(t *testing.T) {
		db := newDB(t)

		// Set (insert new).
		rec := record.NewRecordWithData(
			record.NewKeyWithID("widgets", "w2"),
			map[string]any{"name": "Bolt", "price": "1.50"},
		)
		if err := db.Set(ctx, rec); err != nil {
			t.Fatalf("Set (insert): %v", err)
		}

		// Get to verify.
		got := make(map[string]any)
		getRec := record.NewRecordWithData(record.NewKeyWithID("widgets", "w2"), got)
		if err := db.Get(ctx, getRec); err != nil {
			t.Fatalf("Get after Set (insert): %v", err)
		}
		if got["name"] != "Bolt" {
			t.Errorf("name = %v, want Bolt", got["name"])
		}

		// Set (update existing).
		updRec := record.NewRecordWithData(
			record.NewKeyWithID("widgets", "w2"),
			map[string]any{"name": "Big Bolt", "price": "3.00"},
		)
		if err := db.Set(ctx, updRec); err != nil {
			t.Fatalf("Set (update): %v", err)
		}

		// Get again to verify update.
		got2 := make(map[string]any)
		getRec2 := record.NewRecordWithData(record.NewKeyWithID("widgets", "w2"), got2)
		if err := db.Get(ctx, getRec2); err != nil {
			t.Fatalf("Get after Set (update): %v", err)
		}
		if got2["name"] != "Big Bolt" {
			t.Errorf("name after update = %v, want Big Bolt", got2["name"])
		}
		if got2["price"] != "3.00" {
			t.Errorf("price after update = %v, want 3.00", got2["price"])
		}
	})

	t.Run("GetMulti_map_data", func(t *testing.T) {
		db := newDB(t)

		// Insert two records.
		for _, d := range []struct {
			id    string
			name  string
			price string
		}{
			{"m1", "WidgetA", "10.00"},
			{"m2", "WidgetB", "20.00"},
		} {
			r := record.NewRecordWithData(
				record.NewKeyWithID("widgets", d.id),
				map[string]any{"name": d.name, "price": d.price},
			)
			if err := db.Insert(ctx, r); err != nil {
				t.Fatalf("Insert %s: %v", d.id, err)
			}
		}

		// GetMulti both back.
		m1 := make(map[string]any)
		m2 := make(map[string]any)
		records := []record.Record{
			record.NewRecordWithData(record.NewKeyWithID("widgets", "m1"), m1),
			record.NewRecordWithData(record.NewKeyWithID("widgets", "m2"), m2),
		}
		if err := db.GetMulti(ctx, records); err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		if m1["name"] != "WidgetA" {
			t.Errorf("m1 name = %v, want WidgetA", m1["name"])
		}
		if m2["name"] != "WidgetB" {
			t.Errorf("m2 name = %v, want WidgetB", m2["name"])
		}
	})
}
