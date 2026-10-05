package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	_ "modernc.org/sqlite"
)

func TestSQLiteLegacyReadsQuoteNativeIdentifiers(t *testing.T) {
	tables := []struct{ name, primaryKey string }{
		{"HumanResources.EmployeeDepartmentHistory", `odd"key`},
		{"Order Details", `odd"key`},
		{`odd"table`, `odd"key`},
		{"odd`table", "odd`key"},
	}
	for _, tc := range tables {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			table := tc.name
			raw, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = raw.Close() })
			raw.SetMaxOpenConns(1)

			pk := tc.primaryKey
			_, err = raw.Exec(fmt.Sprintf(
				"CREATE TABLE %s (%s TEXT PRIMARY KEY, %s TEXT NOT NULL, %s TEXT NOT NULL, %s TEXT NOT NULL)",
				quoteTestIdentifier(table), quoteTestIdentifier(pk), quoteTestIdentifier("Primary"),
				quoteTestIdentifier("Order Details"), quoteTestIdentifier("a.b"),
			))
			if err != nil {
				t.Fatal(err)
			}
			insert := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
				quoteTestIdentifier(table), quoteTestIdentifier(pk), quoteTestIdentifier("Primary"),
				quoteTestIdentifier("Order Details"), quoteTestIdentifier("a.b"))
			for _, row := range []struct{ id, primary, details, dotted string }{
				{"row-1", "first", "detail one", "dot one"},
				{"row-2", "second", "detail two", "dot two"},
			} {
				if _, err = raw.Exec(insert, row.id, row.primary, row.details, row.dotted); err != nil {
					t.Fatal(err)
				}
			}

			db := NewDatabase(raw, newSchema(), DbOptions{
				StructuredQueryDialect: "sqlite",
				Recordsets: map[string]*Recordset{
					table: NewRecordset(table, Table, []dal.FieldRef{dal.Field(pk)}),
				},
			})

			type selected struct {
				Primary string `db:"Primary"`
			}
			got := selected{}
			key := dalrecord.NewKeyWithID(table, "row-2")
			if err := db.Get(ctx, dalrecord.NewRecordWithData(key, &got)); err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Primary != "second" {
				t.Fatalf("Get selected Primary = %q, want second", got.Primary)
			}

			exists, err := db.Exists(ctx, key)
			if err != nil || !exists {
				t.Fatalf("Exists = %v, %v; want true, nil", exists, err)
			}
			exists, err = db.Exists(ctx, dalrecord.NewKeyWithID(table, "missing"))
			if err != nil || exists {
				t.Fatalf("Exists for a missing row = %v, %v; want false, nil", exists, err)
			}

			first, second := map[string]any{}, map[string]any{}
			records := []dalrecord.Record{
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, "row-1"), &first),
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, "row-2"), &second),
			}
			if err := db.GetMulti(ctx, records); err != nil {
				t.Fatalf("GetMulti: %v", err)
			}
			if len(first) != 4 || len(second) != 4 {
				t.Fatalf("GetMulti map wildcard omitted columns: first=%#v second=%#v", first, second)
			}
			if first[pk] != "row-1" || first["Order Details"] != "detail one" || first["a.b"] != "dot one" {
				t.Fatalf("GetMulti first row = %#v", first)
			}
			if second[pk] != "row-2" || second["Primary"] != "second" || second["Order Details"] != "detail two" {
				t.Fatalf("GetMulti second row = %#v", second)
			}
		})
	}
}

func quoteTestIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
