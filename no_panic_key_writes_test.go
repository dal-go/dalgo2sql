package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	_ "modernc.org/sqlite"
)

// No input of a key read or write reaches a panic: each of the cases below is an
// error of the call, returned before any statement is sent. They were panics in
// buildSingleRecordQuery, processPrimaryKey, getMultiFromSingleTable and reflect.

// noPanic runs the call and fails the test, instead of crashing it, if it panics.
func noPanic(t *testing.T, call func() error) (err error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("panic: %v", p)
		}
	}()
	return call()
}

type mapKeyString string

type unexportedFieldStruct struct {
	Name   string
	secret string
}

// inner is an unexported type, so the field that embeds it is unexported too.
type inner struct{ Name string }

type embeddedUnexportedStruct struct {
	inner
	Age int
}

// writeCase is a write of the users recordset (primary key id) that is refused.
type writeCase struct {
	name string
	data any
}

// refusedData is data a write cannot turn into columns.
func refusedData() []writeCase {
	pointer := &struct{ Name string }{}
	return []writeCase{
		{"nil data", nil},
		{"a number", 5},
		{"a string", "text"},
		{"a slice", []string{"a"}},
		{"a map with integer keys", map[int]string{1: "x"}},
		{"a nil pointer to a struct", (*struct{ Name string })(nil)},
		{"a pointer to a pointer", &pointer},
		{"a struct with an unexported field", &unexportedFieldStruct{Name: "n", secret: "s"}},
		{"a struct that embeds an unexported type", &embeddedUnexportedStruct{inner{"n"}, 1}},
		{"a struct with only unexported fields", &struct{ a, b int }{1, 2}},
	}
}

func TestWrites_OfDataThatCannotBeWrittenAreErrorsBeforeAnyStatement(t *testing.T) {
	c := validNames()
	good := func(id string) dalrecord.Record { return c.mapRecord(id) }
	for _, dialect := range []string{"", dialectSQLite} {
		for _, tt := range refusedData() {
			t.Run(dialect+"/"+tt.name, func(t *testing.T) {
				bad := func(id string) dalrecord.Record { return dalrecord.NewRecordWithData(c.key(id), tt.data) }
				for _, exists := range []bool{false, true} {
					for _, r := range keyPathAPIsAnswering(t, c.options(dialect), func(string) bool { return exists }) {
						calls := map[string]func() error{
							"insert": func() error { return r.api.Insert(context.Background(), bad("id1")) },
							"insert with a generated ID": func() error {
								record := dalrecord.NewRecordWithData(c.incompleteKey(), tt.data)
								return r.api.Insert(context.Background(), record, dal.WithRandomStringKey(8, 3))
							},
							"insert with the adapter's generated ID": func() error {
								record := dalrecord.NewRecordWithData(c.incompleteKey(), tt.data)
								return r.api.Insert(context.Background(), record, dal.WithAdapterGeneratedID())
							},
							"set": func() error { return r.api.Set(context.Background(), bad("id1")) },
							// A batch is refused as a whole: its first record is a good one.
							"set-multi": func() error {
								return r.api.SetMulti(context.Background(), []dalrecord.Record{good("id0"), bad("id1")})
							},
						}
						if batch, ok := r.api.(insertMultiAPI); ok {
							calls["insert-multi"] = func() error {
								return batch.InsertMulti(context.Background(), []dalrecord.Record{good("id0"), bad("id1")})
							}
							calls["insert-multi with generated IDs"] = func() error {
								records := []dalrecord.Record{
									dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{"name": "v"}),
									dalrecord.NewRecordWithData(c.incompleteKey(), tt.data),
								}
								return batch.InsertMulti(context.Background(), records, dal.WithRandomStringKey(8, 3))
							}
						}
						for name, call := range calls {
							err := noPanic(t, call)
							if !errors.Is(err, dal.ErrNotSupported) {
								t.Errorf("%s/%s: error = %v, want one wrapping dal.ErrNotSupported", r.kind, name, err)
							}
						}
						if got := r.recorder.calls(); len(got) != 0 {
							t.Errorf("%s: a statement reached the database: %q", r.kind, got)
						}
					}
				}
			})
		}
	}
}

// The statement builder is the last line: it returns the error itself.
func TestBuildSingleRecordQuery_DataThatCannotBeWrittenIsAnError(t *testing.T) {
	c := validNames()
	for _, tt := range refusedData() {
		for _, op := range []operation{insertOperation, updateOperation} {
			t.Run(tt.name, func(t *testing.T) {
				record := dalrecord.NewRecordWithData(c.key("id1"), tt.data)
				var q query
				err := noPanic(t, func() (err error) {
					q, err = buildSingleRecordQuery(op, c.options(""), record)
					return err
				})
				if !errors.Is(err, dal.ErrNotSupported) || q.text != "" || q.args != nil {
					t.Errorf("= %+v, %v; want an error wrapping dal.ErrNotSupported and no statement", q, err)
				}
			})
		}
	}
}

// The refusal names what is wrong with the data.
func TestWrites_TheRefusalOfDataSaysWhatIsWrong(t *testing.T) {
	c := validNames()
	r := keyPathAPIs(t, c.options(""))[0]
	cases := []struct {
		data any
		want []string
	}{
		{&unexportedFieldStruct{}, []string{"secret", "not exported"}},
		{map[int]string{}, []string{"keys", "int"}},
		{nil, []string{"nil"}},
		{5, []string{"int"}},
	}
	for _, tt := range cases {
		err := noPanic(t, func() error {
			return r.api.Insert(context.Background(), dalrecord.NewRecordWithData(c.key("id1"), tt.data))
		})
		for _, want := range tt.want {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("data %v: error = %v, want one that mentions %q", tt.data, err, want)
			}
		}
	}
}

// A map whose key type is a named string type is a map with string keys.
func TestInsert_MapWithANamedStringKeyTypeIsWritten(t *testing.T) {
	c := validNames()
	record := dalrecord.NewRecordWithData(c.key("id1"), map[mapKeyString]any{"name": "v", "age": 3})
	for _, r := range keyPathAPIsAnswering(t, c.options(""), func(string) bool { return false }) {
		if err := noPanic(t, func() error { return r.api.Insert(context.Background(), record) }); err != nil {
			t.Fatal(err)
		}
		if got := r.recorder.calls(); len(got) != 1 || got[0] != "INSERT INTO users(id, age, name) VALUES (?, ?, ?)" {
			t.Errorf("%s: statements = %q", r.kind, got)
		}
	}
}

// keyWithID is a key of collection whose ID is any value, a slice included: the
// constructors of the record package take comparable IDs only.
func keyWithID(collection string, id any) *dalrecord.Key {
	key := dalrecord.NewIncompleteKey(collection, reflect.String, nil)
	key.ID = id
	return key
}

// compositeOptions declares the recordset pairs with a primary key of two columns,
// beside users with one.
func compositeOptions(dialect string) DbOptions {
	return DbOptions{
		StructuredQueryDialect: dialect,
		Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
			"pairs": NewRecordset("pairs", Table, []dal.FieldRef{dal.Field("tenant"), dal.Field("id")}),
		},
	}
}

// An insert with a key ID writes the ID to the primary-key columns. The ID of a key of a
// recordset with a composite key is a slice with one value per column.
func TestInsert_IDThatDoesNotFitACompositeKeyIsAnErrorBeforeAnyStatement(t *testing.T) {
	cases := []struct {
		name string
		id   any
	}{
		{"a string", "p1"},
		{"a number", 7},
		{"a slice of too few values", []string{"t1"}},
		{"a slice of too many values", []string{"t1", "p1", "x"}},
		{"an empty slice", []string{}},
		{"a slice of a type that is not supported", []float64{1, 2}},
		{"a slice of any", []any{"t1", "p1"}},
	}
	for _, dialect := range []string{"", dialectSQLite} {
		for _, tt := range cases {
			t.Run(dialect+"/"+tt.name, func(t *testing.T) {
				newKey := func() *dalrecord.Key { return keyWithID("pairs", tt.id) }
				for _, r := range keyPathAPIsAnswering(t, compositeOptions(dialect), func(string) bool { return false }) {
					record := func() dalrecord.Record {
						return dalrecord.NewRecordWithData(newKey(), map[string]any{"name": "v"})
					}
					calls := map[string]func() error{
						"insert": func() error { return r.api.Insert(context.Background(), record()) },
					}
					if batch, ok := r.api.(insertMultiAPI); ok {
						calls["insert-multi"] = func() error {
							// A batch is refused as a whole: its first record is a good one.
							good := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), map[string]any{"name": "v"})
							return batch.InsertMulti(context.Background(), []dalrecord.Record{good, record()})
						}
					}
					for name, call := range calls {
						err := noPanic(t, call)
						if !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "pairs") {
							t.Errorf("%s/%s: error = %v, want one wrapping dal.ErrNotSupported that names the recordset", r.kind, name, err)
						}
					}
					if got := r.recorder.calls(); len(got) != 0 {
						t.Errorf("%s: a statement reached the database: %q", r.kind, got)
					}
				}
			})
		}
	}
}

// An ID that fits is still written, one value to each column.
func TestInsert_IDThatFitsACompositeKeyIsWritten(t *testing.T) {
	record := dalrecord.NewRecordWithData(keyWithID("pairs", []string{"t1", "p1"}), map[string]any{"name": "v"})
	for _, r := range keyPathAPIsAnswering(t, compositeOptions(""), func(string) bool { return false }) {
		if err := noPanic(t, func() error { return r.api.Insert(context.Background(), record) }); err != nil {
			t.Fatal(err)
		}
		if got := r.recorder.calls(); len(got) != 1 || got[0] != "INSERT INTO pairs(tenant, id, name) VALUES (?, ?, ?)" {
			t.Errorf("%s: statements = %q", r.kind, got)
		}
	}
}

// The UPDATE the builder writes for a composite key names every column of it, and an ID
// that does not fit is an error here too, though Set refuses a composite key before it
// builds one.
func TestBuildSingleRecordQuery_UpdateByACompositeKey(t *testing.T) {
	data := map[string]any{"name": "v"}
	fits := dalrecord.NewRecordWithData(keyWithID("pairs", []string{"t1", "p1"}), data)
	q, err := buildSingleRecordQuery(updateOperation, compositeOptions(""), fits)
	if err != nil || q.text != "UPDATE pairs SET  name = ? WHERE tenant = ? AND id = ?" || len(q.args) != 3 {
		t.Errorf("= %+v, %v", q, err)
	}
	doesNotFit := dalrecord.NewRecordWithData(keyWithID("pairs", "p1"), data)
	q, err = noPanicQuery(t, updateOperation, compositeOptions(""), doesNotFit)
	if !errors.Is(err, dal.ErrNotSupported) || q.text != "" || q.args != nil {
		t.Errorf("= %+v, %v; want an error wrapping dal.ErrNotSupported and no statement", q, err)
	}
}

func noPanicQuery(t *testing.T, op operation, options DbOptions, record dalrecord.Record) (q query, err error) {
	t.Helper()
	err = noPanic(t, func() (err error) {
		q, err = buildSingleRecordQuery(op, options, record)
		return err
	})
	return q, err
}

// An ID generator probes for a taken ID with a read by the key, which a composite
// key does not support: an error, before any statement.
func TestInsert_GeneratedIDIntoACompositeKeyIsAnErrorBeforeAnyStatement(t *testing.T) {
	for _, r := range keyPathAPIsAnswering(t, compositeOptions(""), func(string) bool { return false }) {
		record := dalrecord.NewRecordWithData(dalrecord.NewIncompleteKey("pairs", 24, nil), map[string]any{"name": "v"})
		err := noPanic(t, func() error {
			return r.api.Insert(context.Background(), record, dal.WithRandomStringKey(8, 3))
		})
		if !errors.Is(err, dal.ErrNotImplementedYet) {
			t.Errorf("%s: error = %v, want one wrapping dal.ErrNotImplementedYet", r.kind, err)
		}
		if got := r.recorder.calls(); len(got) != 0 {
			t.Errorf("%s: a statement reached the database: %q", r.kind, got)
		}
	}
}

// GetMulti of several records of a recordset with a composite key is an error before
// any statement, also when records of other recordsets would have been read first.
func TestGetMulti_SeveralRecordsOfACompositeKeyAreAnErrorBeforeAnyStatement(t *testing.T) {
	pair := func(id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(keyWithID("pairs", []string{"t1", id}), map[string]any{})
	}
	user := func(id string) dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", id), map[string]any{})
	}
	configurations := map[string]DbOptions{
		"declared": compositeOptions(""),
		"the primary key of the options": {PrimaryKey: []string{"tenant", "id"}, Recordsets: map[string]*Recordset{
			"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")}),
		}},
	}
	for name, options := range configurations {
		t.Run(name, func(t *testing.T) {
			for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return true }) {
				for range 25 { // recordsets are read in map order
					records := []dalrecord.Record{user("u1"), user("u2"), pair("p1"), pair("p2")}
					err := noPanic(t, func() error { return r.api.GetMulti(context.Background(), records) })
					if !errors.Is(err, dal.ErrNotImplementedYet) {
						t.Fatalf("%s: error = %v, want one wrapping dal.ErrNotImplementedYet", r.kind, err)
					}
					for _, record := range records {
						if !errors.Is(record.Error(), dal.ErrNotImplementedYet) {
							t.Fatalf("%s: record error = %v, want the refusal on every record", r.kind, record.Error())
						}
					}
				}
				if got := r.recorder.calls(); len(got) != 0 {
					t.Errorf("%s: a statement reached the database: %q", r.kind, got)
				}
			}
		})
	}
}

// processPrimaryKey returns an error for an ID that cannot address the key, and calls
// f for no column of it.
func TestProcessPrimaryKey_IDThatDoesNotFitIsAnError(t *testing.T) {
	cases := []struct {
		name       string
		primaryKey []string
		id         any
	}{
		{"unsupported type", []string{"K1", "K2"}, 1.23},
		{"unsupported slice", []string{"K1", "K2"}, []float32{1, 2}},
		{"too few values", []string{"K1", "K2"}, []string{"a"}},
		{"too many values", []string{"K1", "K2"}, []int{1, 2, 3}},
		{"too few times", []string{"T1", "T2"}, []time.Time{{}}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			err := noPanic(t, func() error {
				return processPrimaryKey(tt.primaryKey, &dalrecord.Key{ID: tt.id}, func(int, string, any) { called = true })
			})
			if !errors.Is(err, dal.ErrNotSupported) || called {
				t.Errorf("error = %v, called = %v; want an error wrapping dal.ErrNotSupported and no call", err, called)
			}
		})
	}
}

// The transaction runners roll the transaction back when the worker panics, and
// panic again: a panicking worker leaves no open transaction, which on SQLite would
// hold the lock of the file for good.
func TestTransactionRunners_APanickingWorkerLeavesNoOpenTransaction(t *testing.T) {
	type runner struct {
		name string
		// work is what the worker does before it panics: it takes the lock the
		// transaction would hold.
		run func(ctx context.Context, db *database, panicValue any) error
	}
	runners := []runner{
		{"read-write", func(ctx context.Context, db *database, panicValue any) error {
			return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
				record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("t", "from-worker"), map[string]any{"v": "w"})
				if err := tx.Insert(ctx, record); err != nil {
					return err
				}
				panic(panicValue)
			})
		}},
		{"read-only", func(ctx context.Context, db *database, panicValue any) error {
			return db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				if _, err := tx.Exists(ctx, dalrecord.NewKeyWithID("t", "seed")); err != nil {
					return err
				}
				panic(panicValue)
			})
		}},
	}
	for _, rn := range runners {
		t.Run(rn.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "panic.db")
			open := func() *sql.DB {
				raw, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = raw.Close() })
				return raw
			}
			raw, other := open(), open()
			for _, statement := range []string{
				"CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT)",
				"INSERT INTO t VALUES ('seed', 's')",
			} {
				if _, err := raw.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			db := &database{db: raw, options: DbOptions{Recordsets: map[string]*Recordset{
				"t": NewRecordset("t", Table, []dal.FieldRef{dal.Field("id")}),
			}}}

			panicValue := errors.New("the worker failed")
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_ = rn.run(context.Background(), db, panicValue)
			}()
			if recovered != panicValue {
				t.Fatalf("recovered %v, want the worker's own panic value %v", recovered, panicValue)
			}

			// Another handle can write: the panicked transaction holds no lock.
			if _, err := other.Exec("INSERT INTO t VALUES ('from-other', 'o')"); err != nil {
				t.Fatalf("a write from another handle after the panic: %v", err)
			}
			var count int
			if err := other.QueryRow("SELECT COUNT(*) FROM t WHERE id = 'from-worker'").Scan(&count); err != nil || count != 0 {
				t.Errorf("rows written by the panicked worker = %d, %v; want none", count, err)
			}
			if stats := raw.Stats(); stats.InUse != 0 {
				t.Errorf("%d connections are still in use after the panic: %+v", stats.InUse, stats)
			}
		})
	}
}
