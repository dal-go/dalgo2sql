package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

// With the sqlite dialect every name is written quoted. The first statement of
// each operation, for names a plain identifier cannot spell.
func TestKeyPathNames_SQLiteStatementText(t *testing.T) {
	c := nameCase{collection: "Order Details", pk: "Order ID", field: "Unit Price"}
	want := map[string]string{
		"exists":               "SELECT 1 FROM `Order Details` WHERE `Order ID` = ?",
		"get":                  "SELECT `Name` FROM `Order Details` WHERE `Order ID` = ?",
		"get-map":              "SELECT * FROM `Order Details` WHERE `Order ID` = ?",
		"get-multi":            "SELECT * FROM `Order Details` WHERE `Order ID` IN (?, ?)",
		"get-multi-struct":     "SELECT `Order ID`, `Name` FROM `Order Details` WHERE `Order ID` IN (?, ?)",
		"get-multi-one-record": "SELECT * FROM `Order Details` WHERE `Order ID` = ?",
		"insert":               "INSERT INTO `Order Details`(`Order ID`, `Unit Price`) VALUES (?, ?)",
		"insert-generated-id":  "SELECT 1 FROM `Order Details` WHERE `Order ID` = ?",
		"set":                  "SELECT `Order ID` FROM `Order Details` WHERE `Order ID` = ?",
		"set-multi":            "SELECT `Order ID` FROM `Order Details` WHERE `Order ID` = ?",
		"update":               "UPDATE `Order Details` SET\n\t`Unit Price` = ?\n\tWHERE `Order ID` = ?",
		"update-multi":         "UPDATE `Order Details` SET\n\t`Unit Price` = ?\n\tWHERE `Order ID` = ?",
		"delete":               "DELETE FROM `Order Details` WHERE `Order ID` = ?",
		"delete-multi":         "DELETE FROM `Order Details` WHERE `Order ID` = ?",
	}
	if len(want) != len(keyPathOps) {
		t.Fatalf("%d expectations for %d operations", len(want), len(keyPathOps))
	}
	for _, op := range keyPathOps {
		t.Run(op.name, func(t *testing.T) {
			for _, r := range keyPathAPIs(t, c.options("sqlite")) {
				t.Run(r.kind, func(t *testing.T) {
					// GetMulti of one record reports a failed read on the record only.
					err := op.run(context.Background(), r.api, c)
					if err != nil && !errors.Is(err, errReachedDatabase) {
						t.Fatalf("error = %v, want the statement to reach the database", err)
					}
					if calls := r.recorder.calls(); !reflect.DeepEqual(calls, []string{want[op.name]}) {
						t.Errorf("statements = %q\n want %q", calls, []string{want[op.name]})
					}
				})
			}
		})
	}
}

// A placeholder is numbered where it is written, so a quoted name that holds a
// question mark is not mistaken for one.
func TestBuildSingleRecordQuery_PlaceholdersNextToQuotedNames(t *testing.T) {
	options := DbOptions{
		StructuredQueryDialect: "sqlite",
		Placeholder:            PlaceholderDollar,
		Recordsets:             map[string]*Recordset{"What?": NewRecordset("What?", Table, []dal.FieldRef{dal.Field("id?")})},
	}
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("What?", "k1"), map[string]any{"a?": 1, "b": 2})
	insert, err := buildSingleRecordQuery(insertOperation, options, record)
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT INTO `What?`(`id?`, `a?`, `b`) VALUES ($1, $2, $3)"; insert.text != want {
		t.Errorf("insert = %q, want %q", insert.text, want)
	}
	updated, err := buildSingleRecordQuery(updateOperation, options, record)
	if err != nil {
		t.Fatal(err)
	}
	if want := "UPDATE `What?` SET  `a?` = $1, `b` = $2 WHERE `id?` = $3"; updated.text != want {
		t.Errorf("update = %q, want %q", updated.text, want)
	}
	if !reflect.DeepEqual(updated.args, []any{1, 2, "k1"}) {
		t.Errorf("update args = %v", updated.args)
	}
}

func TestBuildSingleRecordQuery_RefusesNames(t *testing.T) {
	type pkCase struct {
		name       string
		o          operation
		collection string
		pk         string
		fields     map[string]any
		position   string
	}
	cases := []pkCase{
		{"insert, collection", insertOperation, "x; DROP TABLE y", "id", map[string]any{"a": 1}, "collection"},
		{"update, collection", updateOperation, "x; DROP TABLE y", "id", map[string]any{"a": 1}, "collection"},
		{"insert, primary key", insertOperation, "users", "id; --", map[string]any{"a": 1}, "primary key"},
		{"update, primary key", updateOperation, "users", "id; --", map[string]any{"a": 1}, "primary key"},
		{"insert, field", insertOperation, "users", "id", map[string]any{"a b": 1}, "field"},
		{"update, field", updateOperation, "users", "id", map[string]any{"a b": 1}, "field"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			options := DbOptions{Recordsets: map[string]*Recordset{
				tt.collection: NewRecordset(tt.collection, Table, []dal.FieldRef{dal.Field(tt.pk)}),
			}}
			record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(tt.collection, "k1"), tt.fields)
			q, err := buildSingleRecordQuery(tt.o, options, record)
			if !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), tt.position) {
				t.Errorf("error = %v, want one wrapping ErrUnsafeName for the %s", err, tt.position)
			}
			if q.text != "" || q.args != nil {
				t.Errorf("a refused name left a statement behind: %+v", q)
			}
		})
	}
}

func TestExistsSingle_RefusesTheCollectionOnItsOwn(t *testing.T) {
	options := validNames().options("")
	key := dalrecord.NewKeyWithID("a b", "k1")
	_, err := existsSingle(options, key, func(string, ...any) (*sql.Rows, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	})
	if !errors.Is(err, ErrUnsafeName) {
		t.Errorf("error = %v, want one wrapping ErrUnsafeName", err)
	}
}

// The batch and record checks run before these builders, so a name only reaches
// the check inside them when a builder is called on its own, or when a record
// changed in between (see TestSetSingle_ChecksTheNamesAgainWhereTheStatementIsBuilt).
func TestExistsSingle_RefusesThePrimaryKeyOnItsOwn(t *testing.T) {
	options := DbOptions{Recordsets: map[string]*Recordset{
		"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id; --")}),
	}}
	_, err := existsSingle(options, dalrecord.NewKeyWithID("users", "k1"), func(string, ...any) (*sql.Rows, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	})
	if !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), positionPrimaryKey) {
		t.Errorf("error = %v, want one wrapping ErrUnsafeName for the primary key", err)
	}
}

func TestGetMultiFromSingleTable_RefusesTheCollectionOnItsOwn(t *testing.T) {
	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("a b", "k1"), map[string]any{}),
		dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("a b", "k2"), map[string]any{}),
	}
	err := getMultiFromSingleTable(context.Background(), DbOptions{}, records, func(string, ...any) (*sql.Rows, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	})
	if !errors.Is(err, ErrUnsafeName) {
		t.Errorf("error = %v, want one wrapping ErrUnsafeName", err)
	}
	for _, record := range records {
		if !errors.Is(record.Error(), ErrUnsafeName) {
			t.Errorf("record error = %v, want one wrapping ErrUnsafeName", record.Error())
		}
	}
}

func TestExecInsert_RefusesTheNamesOnItsOwn(t *testing.T) {
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("a b", "k1"), map[string]any{"name": "v"})
	err := execInsert(context.Background(), DbOptions{}, record, func(context.Context, string, ...any) (sql.Result, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	})
	if !errors.Is(err, ErrUnsafeName) {
		t.Errorf("error = %v, want one wrapping ErrUnsafeName", err)
	}
}

func TestDeleteMultiInSingleTable_RefusesTheCollectionOnItsOwn(t *testing.T) {
	keys := []*dalrecord.Key{dalrecord.NewKeyWithID("a b", "k1"), dalrecord.NewKeyWithID("a b", "k2")}
	err := deleteMultiInSingleTable(context.Background(), DbOptions{}, keys, func(context.Context, string, ...any) (sql.Result, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	})
	if !errors.Is(err, ErrUnsafeName) {
		t.Errorf("error = %v, want one wrapping ErrUnsafeName", err)
	}
}

// shiftingRecord answers Data() with a different value each time it is asked,
// as a record whose map another goroutine changes between two reads would.
type shiftingRecord struct {
	dalrecord.Record
	key   *dalrecord.Key
	datas []any
	reads int
}

func (r *shiftingRecord) Key() *dalrecord.Key             { return r.key }
func (r *shiftingRecord) SetError(error) dalrecord.Record { return r }
func (r *shiftingRecord) Data() any {
	data := r.datas[min(r.reads, len(r.datas)-1)]
	r.reads++
	return data
}

// The names are checked again where the statement is built: a record changed
// after the check cannot carry a name the check never saw.
func TestSetSingle_ChecksTheNamesAgainWhereTheStatementIsBuilt(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sqlDB)
	options := validNames().options("")
	record := &shiftingRecord{
		key:   dalrecord.NewKeyWithID("users", "k1"),
		datas: []any{map[string]any{"name": "ok"}, map[string]any{"name; DROP TABLE y --": "x"}},
	}
	mock.ExpectQuery("SELECT id FROM users WHERE id = ?").WithArgs("k1").WillReturnRows(sqlmock.NewRows([]string{"id"}))

	err = setSingle(context.Background(), options, record, sqlDB.Query, sqlDB.ExecContext)
	if !errors.Is(err, ErrUnsafeName) {
		t.Fatalf("error = %v, want one wrapping ErrUnsafeName", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The quoted names work against a real SQLite file: a name with a space, a
// quote or a semicolon is an ordinary identifier, and is never SQL.
func TestKeyPathNames_SQLiteQuotedNamesAgainstAFile(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "names.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, raw)
	for _, statement := range []string{
		"CREATE TABLE `Order Details` (`Order ID` TEXT PRIMARY KEY, `Unit Price` INTEGER, `Qty` INTEGER)",
		"CREATE TABLE `Customer Notes` (`Note ID` TEXT PRIMARY KEY, `Name` TEXT)",
		"CREATE TABLE victim (id TEXT PRIMARY KEY)",
		"INSERT INTO victim VALUES ('kept')",
		"CREATE TABLE `evil\"; DROP TABLE victim; -- ` (`k;` TEXT PRIMARY KEY, `x\"y` TEXT)",
		"CREATE TABLE `back``tick` (id TEXT PRIMARY KEY, `co``l` TEXT)",
	} {
		if _, err = raw.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	const evil = "evil\"; DROP TABLE victim; -- "
	db := &database{db: raw, options: DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"Order Details":  NewRecordset("Order Details", Table, []dal.FieldRef{dal.Field("Order ID")}),
			"Customer Notes": NewRecordset("Customer Notes", Table, []dal.FieldRef{dal.Field("Note ID")}),
			evil:             NewRecordset(evil, Table, []dal.FieldRef{dal.Field("k;")}),
			"back`tick":      NewRecordset("back`tick", Table, []dal.FieldRef{dal.Field("id")}),
		},
	}}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	count := func(table string) (n int) {
		t.Helper()
		must(raw.QueryRow("SELECT count(*) FROM " + table).Scan(&n))
		return n
	}
	detail := func(id string) *dalrecord.Key { return dalrecord.NewKeyWithID("Order Details", id) }
	loaded := func(id string) map[string]any {
		t.Helper()
		m := map[string]any{}
		must(db.Get(ctx, dalrecord.NewRecordWithData(detail(id), m)))
		return m
	}

	t.Run("a table, a primary key and fields with spaces", func(t *testing.T) {
		must(db.Insert(ctx, dalrecord.NewRecordWithData(detail("o1"), map[string]any{"Unit Price": 10, "Qty": 2})))
		must(db.Insert(ctx, dalrecord.NewRecordWithData(detail("o2"), map[string]any{"Unit Price": 20, "Qty": 3})))
		if exists, err := db.Exists(ctx, detail("o1")); err != nil || !exists {
			t.Errorf("Exists(o1) = %v, %v", exists, err)
		}
		if exists, err := db.Exists(ctx, detail("nope")); err != nil || exists {
			t.Errorf("Exists(nope) = %v, %v", exists, err)
		}
		if m := loaded("o1"); m["Unit Price"] != int64(10) || m["Qty"] != int64(2) || m["Order ID"] != "o1" {
			t.Errorf("Get(o1) = %v", m)
		}

		must(db.Set(ctx, dalrecord.NewRecordWithData(detail("o1"), map[string]any{"Unit Price": 11, "Qty": 2})))
		must(db.Set(ctx, dalrecord.NewRecordWithData(detail("o3"), map[string]any{"Unit Price": 30, "Qty": 4})))
		must(db.SetMulti(ctx, []dalrecord.Record{
			dalrecord.NewRecordWithData(detail("o3"), map[string]any{"Unit Price": 31, "Qty": 4}),
			dalrecord.NewRecordWithData(detail("o4"), map[string]any{"Unit Price": 40, "Qty": 5}),
		}))
		if m := loaded("o1"); m["Unit Price"] != int64(11) {
			t.Errorf("after Set, Get(o1) = %v", m)
		}
		if m := loaded("o3"); m["Unit Price"] != int64(31) {
			t.Errorf("after SetMulti, Get(o3) = %v", m)
		}
		if n := count("`Order Details`"); n != 4 {
			t.Errorf("%d rows, want 4", n)
		}

		must(db.Update(ctx, detail("o1"), []update.Update{update.ByFieldName("Unit Price", 12)}))
		must(db.UpdateMulti(ctx, []*dalrecord.Key{detail("o1"), detail("o2")}, []update.Update{update.ByFieldName("Qty", 9)}))
		a, b := map[string]any{}, map[string]any{}
		must(db.GetMulti(ctx, []dalrecord.Record{
			dalrecord.NewRecordWithData(detail("o1"), a),
			dalrecord.NewRecordWithData(detail("o2"), b),
		}))
		if a["Unit Price"] != int64(12) || a["Qty"] != int64(9) || b["Unit Price"] != int64(20) || b["Qty"] != int64(9) {
			t.Errorf("GetMulti = %v, %v", a, b)
		}
		single := map[string]any{}
		must(db.GetMulti(ctx, []dalrecord.Record{dalrecord.NewRecordWithData(detail("o2"), single)}))
		if single["Unit Price"] != int64(20) {
			t.Errorf("GetMulti of one record = %v", single)
		}

		must(db.Delete(ctx, detail("o4")))
		must(db.DeleteMulti(ctx, []*dalrecord.Key{detail("o1"), detail("o2")}))
		if n := count("`Order Details`"); n != 1 {
			t.Errorf("%d rows left, want 1 (o3)", n)
		}
		must(db.DeleteMulti(ctx, []*dalrecord.Key{detail("o3")}))
		if n := count("`Order Details`"); n != 0 {
			t.Errorf("%d rows left, want 0", n)
		}
	})

	// Before names were validated, a name was pasted as written, so a caller could
	// pass `"Order Details"` (quotes included) to reach the table Order Details.
	// The name is now one literal identifier, quote characters included: SQLite
	// finds no such table, and the bare name is the way in.
	t.Run("a name that carries its own quoting no longer finds the table", func(t *testing.T) {
		const selfQuoted = `"Order Details"`
		db.options.Recordsets[selfQuoted] = NewRecordset(selfQuoted, Table, []dal.FieldRef{dal.Field("Order ID")})
		key := dalrecord.NewKeyWithID(selfQuoted, "o1")
		err := db.Get(ctx, dalrecord.NewRecordWithData(key, map[string]any{}))
		if err == nil || !strings.Contains(err.Error(), `no such table: "Order Details"`) {
			t.Errorf("Get through the self-quoted name: error = %v, want SQLite to find no such table", err)
		}
		if errors.Is(err, ErrUnsafeName) {
			t.Errorf("Get through the self-quoted name was refused as unsafe: %v", err)
		}
		// The bare name reaches the table: the row is not there, the table is.
		err = db.Get(ctx, dalrecord.NewRecordWithData(detail("o1"), map[string]any{}))
		if err == nil || strings.Contains(err.Error(), "no such table") {
			t.Errorf("Get through the bare name: error = %v, want the table found and the row not", err)
		}
	})

	t.Run("struct data and a primary key with a space", func(t *testing.T) {
		type note struct {
			Name string `db:"Name"`
		}
		key := func(id string) *dalrecord.Key { return dalrecord.NewKeyWithID("Customer Notes", id) }
		must(db.Insert(ctx, dalrecord.NewRecordWithData(key("n1"), &note{Name: "first"})))
		must(db.Insert(ctx, dalrecord.NewRecordWithData(key("n2"), &note{Name: "second"})))
		one := &note{}
		must(db.Get(ctx, dalrecord.NewRecordWithData(key("n1"), one)))
		if one.Name != "first" {
			t.Errorf("Get(n1) = %+v", one)
		}
		first, second := &note{}, &note{}
		must(db.GetMulti(ctx, []dalrecord.Record{
			dalrecord.NewRecordWithData(key("n1"), first),
			dalrecord.NewRecordWithData(key("n2"), second),
		}))
		if first.Name != "first" || second.Name != "second" {
			t.Errorf("GetMulti = %+v, %+v", first, second)
		}
		must(db.Set(ctx, dalrecord.NewRecordWithData(key("n1"), &note{Name: "changed"})))
		must(db.Get(ctx, dalrecord.NewRecordWithData(key("n1"), one)))
		if one.Name != "changed" {
			t.Errorf("after Set, Get(n1) = %+v", one)
		}
	})

	t.Run("names that look like SQL are identifiers", func(t *testing.T) {
		key := func(id string) *dalrecord.Key { return dalrecord.NewKeyWithID(evil, id) }
		must(db.Insert(ctx, dalrecord.NewRecordWithData(key("e1"), map[string]any{"x\"y": "v1"})))
		must(db.Insert(ctx, dalrecord.NewRecordWithData(key("e2"), map[string]any{"x\"y": "v2"})))
		got := map[string]any{}
		must(db.Get(ctx, dalrecord.NewRecordWithData(key("e1"), got)))
		if got["x\"y"] != "v1" || got["k;"] != "e1" {
			t.Errorf("Get(e1) = %v", got)
		}
		must(db.Update(ctx, key("e1"), []update.Update{update.ByFieldName("x\"y", "changed")}))
		must(db.Delete(ctx, key("e2")))
		if n := count("`evil\"; DROP TABLE victim; -- `"); n != 1 {
			t.Errorf("%d rows, want 1", n)
		}

		backtick := dalrecord.NewKeyWithID("back`tick", "b1")
		must(db.Insert(ctx, dalrecord.NewRecordWithData(backtick, map[string]any{"co`l": "v"})))
		must(db.Update(ctx, backtick, []update.Update{update.ByFieldName("co`l", "changed")}))
		var value string
		must(raw.QueryRow("SELECT `co``l` FROM `back``tick`").Scan(&value))
		if value != "changed" {
			t.Errorf("co`l = %q", value)
		}

		var kept string
		must(raw.QueryRow("SELECT id FROM victim").Scan(&kept))
		if kept != "kept" {
			t.Errorf("victim holds %q", kept)
		}
	})

	t.Run("a NUL is refused, not sent", func(t *testing.T) {
		err := db.Insert(ctx, dalrecord.NewRecordWithData(detail("o9"), map[string]any{"Unit\x00Price": 1}))
		if !errors.Is(err, ErrUnsafeName) {
			t.Errorf("error = %v, want one wrapping ErrUnsafeName", err)
		}
		if n := count("`Order Details`"); n != 0 {
			t.Errorf("%d rows, want 0", n)
		}
	})
}
