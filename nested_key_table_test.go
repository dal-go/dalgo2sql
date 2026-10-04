package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	_ "modernc.org/sqlite"
)

// One table rule: every statement a key read or write builds addresses the
// recordset getRecordsetName names for the key, that is the collections of the
// key and of its parents joined with "_", and finds the primary key of that
// recordset. A key without a parent is its collection, as ever.

const (
	// nestedTable is the recordset of the key orders/o1/lines/<id>.
	nestedTable = "lines_orders"
	// otherNestedTable is the recordset of the key quotes/q1/lines/<id>.
	otherNestedTable = "lines_quotes"
)

// nestedNames is the nameCase of the keys orders/o1/lines/<id>.
func nestedNames() nameCase {
	return nameCase{collection: "lines", pk: "id", field: "name", parent: dalrecord.NewKeyWithID("orders", "o1")}
}

// otherNestedNames is the nameCase of the keys quotes/q1/lines/<id>: the same
// leaf collection below another parent.
func otherNestedNames() nameCase {
	return nameCase{collection: "lines", pk: "id", field: "name", parent: dalrecord.NewKeyWithID("quotes", "q1")}
}

// nestedOptions configures the recordsets of the two joined names with the
// primary-key column id, and the recordsets of the leaf collection and of the
// parents with primary-key columns of their own, so that a builder that looks
// the key's own collection up instead of the joined name shows in the text of its
// statement.
func nestedOptions(dialect string) DbOptions {
	recordset := func(name, pk string) *Recordset { return NewRecordset(name, Table, []dal.FieldRef{dal.Field(pk)}) }
	return DbOptions{
		StructuredQueryDialect: dialect,
		PrimaryKey:             []string{"id"},
		Recordsets: map[string]*Recordset{
			nestedTable:      recordset(nestedTable, "id"),
			otherNestedTable: recordset(otherNestedTable, "id"),
			"lines":          recordset("lines", "line_id"),
			"orders":         recordset("orders", "order_id"),
			"quotes":         recordset("quotes", "quote_id"),
		},
	}
}

// keyShape is a kind of key the statements are checked for.
type keyShape struct {
	name    string
	c       nameCase
	table   string
	options func(dialect string) DbOptions
}

func keyShapes() []keyShape {
	return []keyShape{
		{"key without a parent", validNames(), "users", validNames().options},
		{"nested key", nestedNames(), nestedTable, nestedOptions},
	}
}

// renderStatement writes a statement template as the dialect writes it: {table}
// is the recordset, {id}, {name}, {Name}, {age} and {city} are columns, each
// quoted for sqlite.
func renderStatement(template, dialect, table string) string {
	quote := func(name string) string {
		if dialect == dialectSQLite {
			return "`" + name + "`"
		}
		return name
	}
	return strings.NewReplacer(
		"{table}", quote(table), "{id}", quote("id"), "{name}", quote("name"), "{Name}", quote("Name"),
		"{age}", quote("age"), "{city}", quote("city"),
	).Replace(template)
}

// keyTableStatements are the statements each operation of keyPathOps sends, in
// order, as templates. The statements of a key without a parent are the ones
// this package sent before the table rule was one function; the statements of
// a nested key differ from them in the table only.
var keyTableStatements = []struct {
	op string
	// rowExists is what the driver answers to a query: the one row of a record
	// that is there, or no row.
	rowExists bool
	want      []string
}{
	{"exists", false, []string{"SELECT 1 FROM {table} WHERE {id} = ?"}},
	{"get", false, []string{"SELECT {Name} FROM {table} WHERE {id} = ?"}},
	{"get-map", false, []string{"SELECT * FROM {table} WHERE {id} = ?"}},
	{"get-multi", false, []string{"SELECT * FROM {table} WHERE {id} IN (?, ?)"}},
	{"get-multi-struct", false, []string{"SELECT {id}, {Name} FROM {table} WHERE {id} IN (?, ?)"}},
	{"get-multi-one-record", false, []string{"SELECT * FROM {table} WHERE {id} = ?"}},
	{"insert", false, []string{"INSERT INTO {table}({id}, {name}) VALUES (?, ?)"}},
	{"insert-generated-id", false, []string{
		"SELECT 1 FROM {table} WHERE {id} = ?",
		"INSERT INTO {table}({id}, {name}) VALUES (?, ?)",
	}},
	{"set", false, []string{
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"INSERT INTO {table}({id}, {name}) VALUES (?, ?)",
	}},
	{"set", true, []string{
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"UPDATE {table} SET  {name} = ? WHERE {id} = ?",
	}},
	{"set-multi", false, []string{
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"INSERT INTO {table}({id}, {name}) VALUES (?, ?)",
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"INSERT INTO {table}({id}, {name}) VALUES (?, ?)",
	}},
	{"set-multi", true, []string{
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"UPDATE {table} SET  {name} = ? WHERE {id} = ?",
		"SELECT {id} FROM {table} WHERE {id} = ?",
		"UPDATE {table} SET  {name} = ? WHERE {id} = ?",
	}},
	{"update", false, []string{"UPDATE {table} SET\n\t{name} = ?\n\tWHERE {id} = ?"}},
	{"update-multi", false, []string{
		"UPDATE {table} SET\n\t{name} = ?\n\tWHERE {id} = ?",
		"UPDATE {table} SET\n\t{name} = ?\n\tWHERE {id} = ?",
	}},
	{"delete", false, []string{"DELETE FROM {table} WHERE {id} = ?"}},
	{"delete-multi", false, []string{
		"DELETE FROM {table} WHERE {id} = ?",
		"DELETE FROM {table} WHERE {id} = ?",
		"DELETE FROM {table} WHERE {id} IN (?, ?)",
	}},
}

// statementTable is the table a statement of the shapes above addresses.
var statementTable = regexp.MustCompile(`(?:FROM|INTO|UPDATE) ([^ (]+)`)

func keyPathOp(t *testing.T, name string) (run func(context.Context, keyPathAPI, nameCase) error) {
	t.Helper()
	for _, op := range keyPathOps {
		if op.name == name {
			return op.run
		}
	}
	t.Fatalf("no key path operation %q", name)
	return nil
}

func TestKeyTable_EveryOperationAddressesTheRecordsetOfTheKey(t *testing.T) {
	covered := map[string]bool{}
	for _, tt := range keyTableStatements {
		covered[tt.op] = true
	}
	if len(covered) != len(keyPathOps) {
		t.Fatalf("%d operations have statements, %d operations are run", len(covered), len(keyPathOps))
	}
	for _, shape := range keyShapes() {
		for _, dialect := range []string{"", dialectSQLite} {
			for _, tt := range keyTableStatements {
				run := keyPathOp(t, tt.op)
				name := shape.name + "/" + dialect + "/" + tt.op
				if tt.rowExists {
					name += "/record exists"
				}
				t.Run(name, func(t *testing.T) {
					want := make([]string, len(tt.want))
					for i, template := range tt.want {
						want[i] = renderStatement(template, dialect, shape.table)
					}
					recorded := keyPathAPIsAnswering(t, shape.options(dialect), func(string) bool { return tt.rowExists })
					for _, r := range recorded {
						t.Run(r.kind, func(t *testing.T) {
							// The outcome of a read is on the record; the statements are what is checked.
							if err := run(context.Background(), r.api, shape.c); errors.Is(err, ErrUnsafeName) {
								t.Fatalf("error = %v", err)
							}
							got := r.recorder.calls()
							if !reflect.DeepEqual(got, want) {
								t.Errorf("statements = %q\n want %q", got, want)
							}
							for _, text := range got {
								found := statementTable.FindStringSubmatch(text)
								if found == nil || found[1] != renderStatement("{table}", dialect, shape.table) {
									t.Errorf("statement %q addresses table %q, want %q only", text, found, shape.table)
								}
							}
						})
					}
				})
			}
		}
	}
}

// Keys of one leaf collection below different parents are different tables: a
// batch never reads, writes or deletes them as one.
func TestKeyTable_SameLeafCollectionBelowDifferentParentsIsNotOneTable(t *testing.T) {
	orders, quotes := nestedNames(), otherNestedNames()
	mapRecord := func(c nameCase, id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(c.key(id), map[string]any{"name": "v"})
	}
	cases := []struct {
		name string
		run  func(context.Context, keyPathAPI) error
		want []string
		// unordered is set where the batch visits its tables in map order.
		unordered bool
	}{
		{"get-multi, one record of each", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(orders.key("l1"), map[string]any{}),
				dalrecord.NewRecordWithData(quotes.key("l2"), map[string]any{}),
			})
		}, []string{
			"SELECT * FROM lines_orders WHERE id = ?",
			"SELECT * FROM lines_quotes WHERE id = ?",
		}, true},
		{"get-multi, two records of one and one of the other", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(orders.key("l1"), map[string]any{}),
				dalrecord.NewRecordWithData(quotes.key("l2"), map[string]any{}),
				dalrecord.NewRecordWithData(orders.key("l3"), map[string]any{}),
			})
		}, []string{
			"SELECT * FROM lines_orders WHERE id IN (?, ?)",
			"SELECT * FROM lines_quotes WHERE id = ?",
		}, true},
		{"delete-multi, alternating", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{orders.key("l1"), quotes.key("l2"), orders.key("l3")})
		}, []string{
			"DELETE FROM lines_orders WHERE id = ?",
			"DELETE FROM lines_quotes WHERE id = ?",
			"DELETE FROM lines_orders WHERE id = ?",
		}, false},
		{"delete-multi, consecutive", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{orders.key("l1"), orders.key("l2"), quotes.key("l3")})
		}, []string{
			"DELETE FROM lines_orders WHERE id = ?",
			"DELETE FROM lines_orders WHERE id = ?",
			"DELETE FROM lines_orders WHERE id IN (?, ?)",
			"DELETE FROM lines_quotes WHERE id = ?",
		}, false},
		{"update-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{orders.key("l1"), quotes.key("l2")}, orders.updates())
		}, []string{
			"UPDATE lines_orders SET\n\tname = ?\n\tWHERE id = ?",
			"UPDATE lines_quotes SET\n\tname = ?\n\tWHERE id = ?",
		}, false},
		{"set-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{mapRecord(orders, "l1"), mapRecord(quotes, "l2")})
		}, []string{
			"SELECT id FROM lines_orders WHERE id = ?",
			"INSERT INTO lines_orders(id, name) VALUES (?, ?)",
			"SELECT id FROM lines_quotes WHERE id = ?",
			"INSERT INTO lines_quotes(id, name) VALUES (?, ?)",
		}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for _, r := range keyPathAPIsAnswering(t, nestedOptions(""), func(string) bool { return false }) {
				t.Run(r.kind, func(t *testing.T) {
					for range 25 { // map order
						r.recorder.mu.Lock()
						r.recorder.statements = nil
						r.recorder.mu.Unlock()
						if err := tt.run(context.Background(), r.api); err != nil {
							t.Fatal(err)
						}
						got := r.recorder.calls()
						if tt.unordered {
							slices.Sort(got)
						}
						if !reflect.DeepEqual(got, tt.want) {
							t.Fatalf("statements = %q\n want %q", got, tt.want)
						}
					}
				})
			}
		})
	}
}

// An update of several fields writes a comma between the assignments.
func TestUpdate_SeparatesAssignmentsWithCommas(t *testing.T) {
	three := []update.Update{update.ByFieldName("name", "n"), update.ByFieldName("age", 3), update.ByFieldName("city", "c")}
	cases := []struct {
		name    string
		updates []update.Update
		want    string // a statement template
	}{
		{"one field", three[:1], "UPDATE {table} SET\n\t{name} = ?\n\tWHERE {id} = ?"},
		{"two fields", three[:2], "UPDATE {table} SET\n\t{name} = ?,\n\t{age} = ?\n\tWHERE {id} = ?"},
		{"three fields", three, "UPDATE {table} SET\n\t{name} = ?,\n\t{age} = ?,\n\t{city} = ?\n\tWHERE {id} = ?"},
	}
	for _, shape := range keyShapes() {
		for _, dialect := range []string{"", dialectSQLite} {
			for _, tt := range cases {
				t.Run(shape.name+"/"+dialect+"/"+tt.name, func(t *testing.T) {
					want := renderStatement(tt.want, dialect, shape.table)
					for _, r := range keyPathAPIsAnswering(t, shape.options(dialect), func(string) bool { return false }) {
						t.Run(r.kind, func(t *testing.T) {
							if err := r.api.Update(context.Background(), shape.c.key("k1"), tt.updates); err != nil {
								t.Fatal(err)
							}
							if err := r.api.UpdateMulti(context.Background(), shape.c.keys(), tt.updates); err != nil {
								t.Fatal(err)
							}
							if got := r.recorder.calls(); !reflect.DeepEqual(got, []string{want, want, want}) {
								t.Errorf("statements = %q\n want three of %q", got, want)
							}
						})
					}
				})
			}
		}
	}
}

// An insert whose key has an ID needs the column of that ID. A recordset with no
// primary key configured is an error of the call, not a panic.
func TestInsert_WithAKeyIDAndNoPrimaryKeyIsAnError(t *testing.T) {
	// Only the leaf collection has a primary key configured: the recordset of the
	// nested key is the joined name, which has none.
	leafOnly := DbOptions{Recordsets: map[string]*Recordset{
		"lines": NewRecordset("lines", Table, []dal.FieldRef{dal.Field("id")}),
	}}
	nested := nestedNames()
	cases := []struct {
		name      string
		options   DbOptions
		c         nameCase
		recordset string
		run       func(context.Context, keyPathAPI, nameCase) error
		// transactionOnly is set for InsertMulti, which a database handle has not.
		transactionOnly bool
	}{
		{"key without a parent", DbOptions{}, validNames(), "users", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.Insert(ctx, c.mapRecord("k1"))
		}, false},
		{"nested key", leafOnly, nested, nestedTable, func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.Insert(ctx, c.mapRecord("k1"))
		}, false},
		{"generated ID", leafOnly, nested, nestedTable, func(ctx context.Context, api keyPathAPI, c nameCase) error {
			record := dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{c.field: "v"})
			return api.Insert(ctx, record, dal.WithRandomStringKey(8, 3))
		}, false},
		{"insert-multi", leafOnly, nested, nestedTable, func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.(insertMultiAPI).InsertMulti(ctx, []dalrecord.Record{c.mapRecord("k1"), c.mapRecord("k2")})
		}, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for _, r := range keyPathAPIs(t, tt.options) {
				if tt.transactionOnly && r.kind != "transaction" {
					continue
				}
				t.Run(r.kind, func(t *testing.T) {
					defer func() {
						if p := recover(); p != nil {
							t.Fatalf("panic: %v", p)
						}
					}()
					err := tt.run(context.Background(), r.api, tt.c)
					if want := "primary key is not defined for recordset " + tt.recordset; err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("error = %v, want one containing %q", err, want)
					}
					if calls := r.recorder.calls(); len(calls) != 0 {
						t.Errorf("a statement reached the database: %q", calls)
					}
				})
			}
		})
	}
}

// nestedKeyDatabase opens a temporary SQLite file holding the table of the
// nested key orders/o1/lines/<id> and, apart from it, a table named as the
// key's own collection and one named as its parent, each with a row of its own.
func nestedKeyDatabase(t *testing.T) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "nested.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	for _, statement := range []string{
		"CREATE TABLE lines_orders (id TEXT PRIMARY KEY, name TEXT, age INTEGER)",
		"CREATE TABLE lines (id TEXT PRIMARY KEY, name TEXT, age INTEGER)",
		"CREATE TABLE orders (id TEXT PRIMARY KEY, name TEXT, age INTEGER)",
		"INSERT INTO lines VALUES ('l5', 'leaf', 1)",
		"INSERT INTO orders VALUES ('o1', 'root', 2)",
	} {
		if _, err = raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return raw
}

type tableRow struct {
	name string
	age  int64
}

// nestedKeyRoundTrip inserts, reads, updates and deletes records under nested
// keys through api, and checks after each step that the rows are in the table of
// the joined name and nowhere else. queryRow reads the database api writes to.
func nestedKeyRoundTrip(t *testing.T, api keyPathAPI, queryRow func(query string, args ...any) *sql.Row) {
	t.Helper()
	ctx := context.Background()
	c := nestedNames()
	rowOf := func(table, id string) (row tableRow, found bool) {
		t.Helper()
		err := queryRow("SELECT name, age FROM "+table+" WHERE id = ?", id).Scan(&row.name, &row.age)
		if errors.Is(err, sql.ErrNoRows) {
			return tableRow{}, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return row, true
	}
	requireRow := func(table, id string, want tableRow) {
		t.Helper()
		if got, found := rowOf(table, id); !found || got != want {
			t.Fatalf("%s row %s = %+v (found %v), want %+v", table, id, got, found, want)
		}
	}
	requireNoRow := func(table, id string) {
		t.Helper()
		if got, found := rowOf(table, id); found {
			t.Fatalf("%s row %s = %+v, want none", table, id, got)
		}
	}
	// The rows of the other two tables are the ones the database started with.
	requireOthersUntouched := func() {
		t.Helper()
		requireRow("lines", "l5", tableRow{"leaf", 1})
		requireRow("orders", "o1", tableRow{"root", 2})
	}
	record := func(id, name string, age int) dalrecord.Record {
		return dalrecord.NewRecordWithData(c.key(id), map[string]any{"name": name, "age": age})
	}
	requireData := func(r dalrecord.Record, want tableRow) {
		t.Helper()
		if err := r.Error(); err != nil || !r.Exists() {
			t.Fatalf("record %v: error %v, exists %v", r.Key(), err, r.Exists())
		}
		data := r.Data().(map[string]any)
		if data["name"] != want.name || data["age"] != want.age {
			t.Fatalf("record %v = %v, want %+v", r.Key(), data, want)
		}
	}

	// Insert and read.
	if err := api.Insert(ctx, record("l5", "first", 1)); err != nil {
		t.Fatal(err)
	}
	requireRow(nestedTable, "l5", tableRow{"first", 1})
	requireOthersUntouched()
	if exists, err := api.Exists(ctx, c.key("l5")); err != nil || !exists {
		t.Fatalf("Exists = %v, %v, want true", exists, err)
	}
	if exists, err := api.Exists(ctx, c.key("l9")); err != nil || exists {
		t.Fatalf("Exists of a missing record = %v, %v, want false", exists, err)
	}
	got := dalrecord.NewRecordWithData(c.key("l5"), map[string]any{})
	if err := api.Get(ctx, got); err != nil {
		t.Fatal(err)
	}
	requireData(got, tableRow{"first", 1})

	// Update.
	updates := []update.Update{update.ByFieldName("name", "second"), update.ByFieldName("age", 2)}
	if err := api.Update(ctx, c.key("l5"), updates); err != nil {
		t.Fatal(err)
	}
	requireRow(nestedTable, "l5", tableRow{"second", 2})
	requireOthersUntouched()

	// Set updates the record that is there and inserts the one that is not.
	if err := api.Set(ctx, record("l5", "third", 3)); err != nil {
		t.Fatal(err)
	}
	if err := api.Set(ctx, record("l6", "six", 6)); err != nil {
		t.Fatal(err)
	}
	requireRow(nestedTable, "l5", tableRow{"third", 3})
	requireRow(nestedTable, "l6", tableRow{"six", 6})
	if err := api.SetMulti(ctx, []dalrecord.Record{record("l5", "fourth", 4), record("l7", "seven", 7)}); err != nil {
		t.Fatal(err)
	}
	requireRow(nestedTable, "l5", tableRow{"fourth", 4})
	requireRow(nestedTable, "l7", tableRow{"seven", 7})
	requireOthersUntouched()

	// Read several.
	several := []dalrecord.Record{
		dalrecord.NewRecordWithData(c.key("l5"), map[string]any{}),
		dalrecord.NewRecordWithData(c.key("l6"), map[string]any{}),
		dalrecord.NewRecordWithData(c.key("l8"), map[string]any{}),
	}
	if err := api.GetMulti(ctx, several); err != nil {
		t.Fatal(err)
	}
	requireData(several[0], tableRow{"fourth", 4})
	requireData(several[1], tableRow{"six", 6})
	if several[2].Exists() {
		t.Fatalf("record l8 exists: %v", several[2].Data())
	}

	// Update several.
	if err := api.UpdateMulti(ctx, []*dalrecord.Key{c.key("l5"), c.key("l6")}, updates); err != nil {
		t.Fatal(err)
	}
	requireRow(nestedTable, "l5", tableRow{"second", 2})
	requireRow(nestedTable, "l6", tableRow{"second", 2})
	requireRow(nestedTable, "l7", tableRow{"seven", 7})
	requireOthersUntouched()

	// Delete one, then several.
	if err := api.Delete(ctx, c.key("l5")); err != nil {
		t.Fatal(err)
	}
	requireNoRow(nestedTable, "l5")
	requireRow(nestedTable, "l6", tableRow{"second", 2})
	requireOthersUntouched()
	if err := api.DeleteMulti(ctx, []*dalrecord.Key{c.key("l6"), c.key("l7")}); err != nil {
		t.Fatal(err)
	}
	requireNoRow(nestedTable, "l6")
	requireNoRow(nestedTable, "l7")
	requireOthersUntouched()
}

func TestKeyTable_SQLiteRoundTripUnderANestedKey(t *testing.T) {
	for _, dialect := range []string{"", dialectSQLite} {
		t.Run("database handle/"+dialect, func(t *testing.T) {
			raw := nestedKeyDatabase(t)
			nestedKeyRoundTrip(t, &database{db: raw, options: nestedOptions(dialect)}, raw.QueryRow)
		})
		t.Run("transaction/"+dialect, func(t *testing.T) {
			raw := nestedKeyDatabase(t)
			tx, err := raw.Begin()
			if err != nil {
				t.Fatal(err)
			}
			nestedKeyRoundTrip(t, newTransaction(tx, nestedOptions(dialect), dal.NewTransactionOptions()), tx.QueryRow)
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
