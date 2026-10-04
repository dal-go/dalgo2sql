package dalgo2sql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

// keyPathAPI is the key-read and key-write surface that both a database handle
// and a transaction expose; every operation of it builds SQL text from names.
type keyPathAPI interface {
	Exists(context.Context, *dalrecord.Key) (bool, error)
	Get(context.Context, dalrecord.Record) error
	GetMulti(context.Context, []dalrecord.Record) error
	Insert(context.Context, dalrecord.Record, ...dal.InsertOption) error
	Set(context.Context, dalrecord.Record) error
	SetMulti(context.Context, []dalrecord.Record) error
	Update(context.Context, *dalrecord.Key, []update.Update, ...dal.Precondition) error
	UpdateMulti(context.Context, []*dalrecord.Key, []update.Update, ...dal.Precondition) error
	Delete(context.Context, *dalrecord.Key) error
	DeleteMulti(context.Context, []*dalrecord.Key) error
	ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error)
}

var (
	_ keyPathAPI = (*database)(nil)
	_ keyPathAPI = transaction{}
)

// hostileNames are names no mode may write into SQL text without refusing or
// quoting them: statement terminators, quote breakers, comment markers,
// whitespace, a NUL. The empty name is not here: the record package refuses to
// build a key or record with an empty collection, so it is tested apart.
var hostileNames = []string{
	"x; DROP TABLE y",
	`a" OR 1=1 --`,
	"order details",
	"it's",
	"nul\x00name",
	"a--b",
	"a/*b*/c",
	"a;b",
	"a`b",
	"1abc",
	"a.b",
	// Names that carry their own quoting acted as quoted identifiers while names
	// were pasted as written; no mode writes them as they are now.
	`"Order Details"`,
	"[Order Details]",
	"`x`",
	"'x'",
}

// selfQuotedNames are the hostileNames that carry their own quote characters.
var selfQuotedNames = []string{`"Order Details"`, "[Order Details]", "`x`", "'x'"}

// unquotableNames are refused even by a dialect that quotes: no quoting makes
// them safe to write into SQL text.
var unquotableNames = []string{
	"nul\x00name",
	"line\nbreak",
	"tab\tname",
	"del\x7fname",
	"c1\u0085name",
	"bad\xffutf8",
	strings.Repeat("a", maxNameBytes+1),
}

// nameCase names the three kinds of name one operation writes into SQL text.
type nameCase struct {
	collection string // collection of every key and record
	pk         string // primary-key column configured for that collection
	field      string // data field (insert, set) or update field
	// parent, when set, is the parent key of every key of the case: the keys are
	// nested, and the table they address is the joined recordset name.
	parent *dalrecord.Key
}

func validNames() nameCase { return nameCase{collection: "users", pk: "id", field: "name"} }

// options configures the case's collection with its primary key, so that
// without the name guard every operation would build a statement from it. The
// users collection is always configured with valid names, for batches that
// mix a valid collection with the case's.
func (c nameCase) options(dialect string) DbOptions {
	recordsets := map[string]*Recordset{"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")})}
	recordsets[c.collection] = NewRecordset(c.collection, Table, []dal.FieldRef{dal.Field(c.pk)})
	return DbOptions{
		StructuredQueryDialect: dialect,
		PrimaryKey:             []string{c.pk},
		Recordsets:             recordsets,
	}
}

// key returns the key of record id in the case's collection. The record
// package refuses to construct a key with an empty collection, so that one
// hostile name is built as a literal.
func (c nameCase) key(id string) *dalrecord.Key {
	if c.collection == "" {
		return &dalrecord.Key{ID: id}
	}
	if c.parent != nil {
		return dalrecord.NewKeyWithParentAndID(c.parent, c.collection, id)
	}
	return dalrecord.NewKeyWithID(c.collection, id)
}

func (c nameCase) incompleteKey() *dalrecord.Key {
	if c.collection == "" {
		return &dalrecord.Key{}
	}
	return dalrecord.NewIncompleteKey(c.collection, reflect.String, c.parent)
}

func (c nameCase) keys() []*dalrecord.Key { return []*dalrecord.Key{c.key("id1"), c.key("id2")} }

func (c nameCase) mapRecord(id string) dalrecord.Record {
	return dalrecord.NewRecordWithData(c.key(id), map[string]any{c.field: "v"})
}

// rawUpdate is an update.Update whose field name is taken verbatim, however
// hostile: update.ByFieldName turns a dotted name into a field path and
// rejects an empty one.
type rawUpdate struct {
	name  string
	value any
}

func (u rawUpdate) FieldName() string           { return u.name }
func (u rawUpdate) FieldPath() update.FieldPath { return nil }
func (u rawUpdate) Value() any                  { return u.value }

func (c nameCase) updates() []update.Update {
	return []update.Update{rawUpdate{name: c.field, value: "v"}}
}

// keyPathOps runs every key read and write once, for the names of one case.
var keyPathOps = []struct {
	name string
	// touches lists the kinds of name the operation writes into SQL text.
	touchesField bool
	run          func(context.Context, keyPathAPI, nameCase) error
}{
	{"exists", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		_, err := api.Exists(ctx, c.key("id1"))
		return err
	}},
	{"get", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Get(ctx, dalrecord.NewRecordWithData(c.key("id1"), &struct{ Name string }{}))
	}},
	{"get-map", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Get(ctx, dalrecord.NewRecordWithData(c.key("id1"), map[string]any{}))
	}},
	{"get-multi", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.GetMulti(ctx, []dalrecord.Record{
			dalrecord.NewRecordWithData(c.key("id1"), map[string]any{}),
			dalrecord.NewRecordWithData(c.key("id2"), map[string]any{}),
		})
	}},
	{"get-multi-struct", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.GetMulti(ctx, []dalrecord.Record{
			dalrecord.NewRecordWithData(c.key("id1"), &struct{ Name string }{}),
			dalrecord.NewRecordWithData(c.key("id2"), &struct{ Name string }{}),
		})
	}},
	{"get-multi-one-record", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.GetMulti(ctx, []dalrecord.Record{dalrecord.NewRecordWithData(c.key("id1"), map[string]any{})})
	}},
	{"insert", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Insert(ctx, c.mapRecord("id1"))
	}},
	{"insert-generated-id", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		record := dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{c.field: "v"})
		return api.Insert(ctx, record, dal.WithRandomStringKey(8, 3))
	}},
	{"set", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Set(ctx, c.mapRecord("id1"))
	}},
	{"set-multi", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.SetMulti(ctx, []dalrecord.Record{c.mapRecord("id1"), c.mapRecord("id2")})
	}},
	{"update", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Update(ctx, c.key("id1"), c.updates())
	}},
	{"update-multi", true, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.UpdateMulti(ctx, c.keys(), c.updates())
	}},
	{"delete", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.Delete(ctx, c.key("id1"))
	}},
	{"delete-multi", false, func(ctx context.Context, api keyPathAPI, c nameCase) error {
		return api.DeleteMulti(ctx, c.keys())
	}},
}

// recordedAPI is one key-path surface over a driver that records every
// statement and fails it.
type recordedAPI struct {
	kind     string
	api      keyPathAPI
	recorder *statementRecorder
}

// keyPathAPIs returns a database handle and a transaction, each over its own
// recording driver that fails every statement.
func keyPathAPIs(t *testing.T, options DbOptions) []recordedAPI {
	t.Helper()
	return keyPathAPIsAnswering(t, options, nil)
}

// keyPathAPIsAnswering is keyPathAPIs over drivers that answer statements as
// statementRecorder.rowExists says; nil fails every statement.
func keyPathAPIsAnswering(t *testing.T, options DbOptions, rowExists func(text string) bool) []recordedAPI {
	t.Helper()
	dbRecorder := &statementRecorder{rowExists: rowExists}
	raw := dbRecorder.open(t)
	txRecorder := &statementRecorder{rowExists: rowExists}
	tx, err := txRecorder.open(t).Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return []recordedAPI{
		{"database", &database{db: raw, options: options}, dbRecorder},
		{"transaction", newTransaction(tx, options, dal.NewTransactionOptions()), txRecorder},
	}
}

// requireRefused fails unless the operation was refused with ErrUnsafeName
// naming position, before any statement reached the database.
func requireRefused(t *testing.T, recorder *statementRecorder, err error, position string) {
	t.Helper()
	if calls := recorder.calls(); len(calls) != 0 {
		t.Fatalf("a statement reached the database: %q", calls)
	}
	if !errors.Is(err, ErrUnsafeName) {
		t.Fatalf("error = %v, want one wrapping ErrUnsafeName", err)
	}
	if errors.Is(err, errReachedDatabase) {
		t.Fatalf("error = %v: the statement reached the database", err)
	}
	if !strings.Contains(err.Error(), position) {
		t.Fatalf("error %q does not name the position %q", err, position)
	}
}

func TestKeyPathNames_HostileCollectionNeverReachesTheExecutor(t *testing.T) {
	modes := []struct {
		dialect string
		names   []string
	}{
		{"", hostileNames},
		{"sqlite", unquotableNames},
		{"example-dialect-without-reviewed-quoting", hostileNames},
	}
	for _, mode := range modes {
		for _, name := range mode.names {
			c := validNames()
			c.collection = name
			for _, op := range keyPathOps {
				t.Run(mode.dialect+"/"+op.name+"/"+strings.ToValidUTF8(name, "?"), func(t *testing.T) {
					for _, r := range keyPathAPIs(t, c.options(mode.dialect)) {
						t.Run(r.kind, func(t *testing.T) {
							requireRefused(t, r.recorder, op.run(context.Background(), r.api, c), "collection")
						})
					}
				})
			}
		}
	}
}

func TestKeyPathNames_HostileFieldNeverReachesTheExecutor(t *testing.T) {
	modes := []struct {
		dialect string
		names   []string
	}{
		{"", append([]string{""}, hostileNames...)},
		{"sqlite", append([]string{""}, unquotableNames...)},
	}
	for _, mode := range modes {
		for _, name := range mode.names {
			c := validNames()
			c.field = name
			for _, op := range keyPathOps {
				if !op.touchesField {
					continue
				}
				t.Run(mode.dialect+"/"+op.name+"/"+strings.ToValidUTF8(name, "?"), func(t *testing.T) {
					for _, r := range keyPathAPIs(t, c.options(mode.dialect)) {
						t.Run(r.kind, func(t *testing.T) {
							requireRefused(t, r.recorder, op.run(context.Background(), r.api, c), "field")
						})
					}
				})
			}
		}
	}
}

func TestKeyPathNames_HostilePrimaryKeyNeverReachesTheExecutor(t *testing.T) {
	modes := []struct {
		dialect string
		names   []string
	}{
		{"", append([]string{""}, hostileNames...)},
		{"sqlite", append([]string{""}, unquotableNames...)},
	}
	for _, mode := range modes {
		for _, name := range mode.names {
			c := validNames()
			c.pk = name
			for _, op := range keyPathOps {
				t.Run(mode.dialect+"/"+op.name+"/"+strings.ToValidUTF8(name, "?"), func(t *testing.T) {
					for _, r := range keyPathAPIs(t, c.options(mode.dialect)) {
						t.Run(r.kind, func(t *testing.T) {
							requireRefused(t, r.recorder, op.run(context.Background(), r.api, c), "primary key")
						})
					}
				})
			}
		}
	}
}

// An empty collection name cannot be built into a record, only into a key.
func TestKeyPathNames_EmptyCollectionNeverReachesTheExecutor(t *testing.T) {
	keyOps := []struct {
		name string
		run  func(context.Context, keyPathAPI, nameCase) error
	}{
		{"exists", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			_, err := api.Exists(ctx, c.key("id1"))
			return err
		}},
		{"update", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.Update(ctx, c.key("id1"), c.updates())
		}},
		{"update-multi", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.UpdateMulti(ctx, c.keys(), c.updates())
		}},
		{"delete", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.Delete(ctx, c.key("id1"))
		}},
		{"delete-multi", func(ctx context.Context, api keyPathAPI, c nameCase) error {
			return api.DeleteMulti(ctx, c.keys())
		}},
	}
	c := validNames()
	c.collection = ""
	for _, dialect := range []string{"", "sqlite"} {
		for _, op := range keyOps {
			t.Run(dialect+"/"+op.name, func(t *testing.T) {
				for _, r := range keyPathAPIs(t, c.options(dialect)) {
					t.Run(r.kind, func(t *testing.T) {
						requireRefused(t, r.recorder, op.run(context.Background(), r.api, c), "collection")
					})
				}
			})
		}
	}
}

// hostilePrimaryKeyCollection is a collection whose name is fine and whose
// configured primary-key column is not.
const hostilePrimaryKeyCollection = "accounts"

// A refused name refuses the whole batch: no statement for an earlier, valid
// key of the same call may have reached the database.
func TestKeyPathNames_BatchIsRefusedAsAWhole(t *testing.T) {
	hostile := validNames()
	hostile.collection = "x; DROP TABLE y"
	hostileField := validNames()
	hostileField.field = "name = 1; --"
	good := validNames()
	batches := []struct {
		name     string
		position string
		run      func(context.Context, keyPathAPI) error
	}{
		{"update-multi collection", "collection", func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{good.key("id1"), hostile.key("id2")}, good.updates())
		}},
		{"update-multi field", "field", func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{good.key("id1"), good.key("id2")}, hostileField.updates())
		}},
		{"delete-multi collection", "collection", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{good.key("id1"), good.key("id2"), hostile.key("id3")})
		}},
		{"set-multi collection", "collection", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{good.mapRecord("id1"), hostile.mapRecord("id2")})
		}},
		{"set-multi field", "field", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{good.mapRecord("id1"), hostileField.mapRecord("id2")})
		}},
		{"get-multi collection", "collection", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(hostile.key("id1"), map[string]any{}),
				dalrecord.NewRecordWithData(hostile.key("id2"), map[string]any{}),
			})
		}},
		// GetMulti visits the collections of a batch in map order, so a batch that
		// mixes a valid collection with a hostile one is run repeatedly: before the
		// whole batch was checked up front, the valid read was sent in about half
		// of the runs.
		{"get-multi valid and hostile collection", "collection", func(ctx context.Context, api keyPathAPI) (err error) {
			for range 25 {
				err = api.GetMulti(ctx, []dalrecord.Record{
					dalrecord.NewRecordWithData(good.key("id1"), map[string]any{}),
					dalrecord.NewRecordWithData(hostile.key("id2"), map[string]any{}),
				})
			}
			return err
		}},
		{"get-multi several valid and one hostile collection", "collection", func(ctx context.Context, api keyPathAPI) (err error) {
			for range 25 {
				err = api.GetMulti(ctx, []dalrecord.Record{
					dalrecord.NewRecordWithData(good.key("id1"), map[string]any{}),
					dalrecord.NewRecordWithData(good.key("id3"), map[string]any{}),
					dalrecord.NewRecordWithData(hostile.key("id2"), map[string]any{}),
				})
			}
			return err
		}},
		{"get-multi one valid and several hostile collection", "collection", func(ctx context.Context, api keyPathAPI) (err error) {
			for range 25 {
				err = api.GetMulti(ctx, []dalrecord.Record{
					dalrecord.NewRecordWithData(good.key("id1"), map[string]any{}),
					dalrecord.NewRecordWithData(hostile.key("id2"), map[string]any{}),
					dalrecord.NewRecordWithData(hostile.key("id3"), map[string]any{}),
				})
			}
			return err
		}},
		// A primary-key column that cannot be written refuses the batch before the
		// first record of a collection that can.
		{"set-multi primary key", "primary key", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{
				good.mapRecord("id1"),
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(hostilePrimaryKeyCollection, "id2"), map[string]any{"name": "v"}),
			})
		}},
	}
	for _, batch := range batches {
		t.Run(batch.name, func(t *testing.T) {
			// users is valid, the other collection is not; both are configured.
			options := good.options("")
			options.Recordsets[hostile.collection] = NewRecordset(hostile.collection, Table, []dal.FieldRef{dal.Field("id")})
			options.Recordsets[hostilePrimaryKeyCollection] = NewRecordset(hostilePrimaryKeyCollection, Table, []dal.FieldRef{dal.Field("id; --")})
			for _, r := range keyPathAPIs(t, options) {
				t.Run(r.kind, func(t *testing.T) {
					requireRefused(t, r.recorder, batch.run(context.Background(), r.api), batch.position)
				})
			}
		})
	}
}

// GetMulti reports a refused name both as its error and on the records, so a
// caller reading either one sees the refusal.
func TestKeyPathNames_GetMultiReportsTheRefusalOnTheRecords(t *testing.T) {
	c := validNames()
	c.collection = "x; DROP TABLE y"
	for name, records := range map[string][]dalrecord.Record{
		"one record": {dalrecord.NewRecordWithData(c.key("id1"), map[string]any{})},
		"two records": {
			dalrecord.NewRecordWithData(c.key("id1"), map[string]any{}),
			dalrecord.NewRecordWithData(c.key("id2"), map[string]any{}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := keyPathAPIs(t, c.options(""))[0]
			err := r.api.GetMulti(context.Background(), records)
			requireRefused(t, r.recorder, err, "collection")
			for _, record := range records {
				if !errors.Is(record.Error(), ErrUnsafeName) {
					t.Errorf("record error = %v, want one wrapping ErrUnsafeName", record.Error())
				}
			}
		})
	}
}

func TestKeyPathNames_GetRecordsTheRefusal(t *testing.T) {
	c := validNames()
	c.collection = "x; DROP TABLE y"
	r := keyPathAPIs(t, c.options(""))[0]
	record := dalrecord.NewRecordWithData(c.key("id1"), &struct{ Name string }{})
	err := r.api.Get(context.Background(), record)
	requireRefused(t, r.recorder, err, "collection")
	if !errors.Is(record.Error(), ErrUnsafeName) {
		t.Errorf("record error = %v, want one wrapping ErrUnsafeName", record.Error())
	}
}

// A struct field that is not a plain identifier is refused without a dialect,
// and quoted with the sqlite dialect.
func TestKeyPathNames_StructFieldNames(t *testing.T) {
	type nonASCII struct{ Имя string }
	c := validNames()
	record := func() dalrecord.Record { return dalrecord.NewRecordWithData(c.key("id1"), &nonASCII{}) }

	r := keyPathAPIs(t, c.options(""))[0]
	requireRefused(t, r.recorder, r.api.Get(context.Background(), record()), "field")
	r = keyPathAPIs(t, c.options(""))[0]
	requireRefused(t, r.recorder, r.api.GetMulti(context.Background(), []dalrecord.Record{record(), dalrecord.NewRecordWithData(c.key("id2"), &nonASCII{})}), "field")

	r = keyPathAPIs(t, c.options("sqlite"))[0]
	if err := r.api.Get(context.Background(), record()); !errors.Is(err, errReachedDatabase) {
		t.Fatalf("error = %v, want the statement to reach the database", err)
	}
	if calls := r.recorder.calls(); len(calls) != 1 || calls[0] != "SELECT `Имя` FROM `users` WHERE `id` = ?" {
		t.Errorf("statements = %q", calls)
	}
}

// select-all-keys is a structured query: without a dialect the read guard
// (SQL-01) refuses its names with an error wrapping dal.ErrNotSupported, before
// any statement is built. The sqlite structured compiler is not covered here:
// it has its own quoting and is outside this change.
func TestKeyPathNames_SelectAllKeysNeverReachesTheExecutor(t *testing.T) {
	for _, name := range hostileNames {
		queries := map[string]dal.Query{
			"collection": dal.From(dal.NewRootCollectionRef(name, "")).NewQuery().SelectKeysOnly(reflect.String),
			"field": dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
				WhereField(name, dal.Equal, "x").SelectKeysOnly(reflect.String),
		}
		for kind, query := range queries {
			t.Run(kind+"/"+strings.ToValidUTF8(name, "?"), func(t *testing.T) {
				for _, r := range keyPathAPIs(t, validNames().options("")) {
					t.Run(r.kind, func(t *testing.T) {
						_, err := r.api.ExecuteQueryToRecordsReader(context.Background(), query)
						if calls := r.recorder.calls(); len(calls) != 0 {
							t.Fatalf("a statement reached the database: %q", calls)
						}
						if !errors.Is(err, dal.ErrNotSupported) {
							t.Fatalf("error = %v, want one wrapping dal.ErrNotSupported", err)
						}
					})
				}
			})
		}
	}
}

// insertMultiAPI is the batch insert of a transaction; a database handle has none.
type insertMultiAPI interface {
	InsertMulti(context.Context, []dalrecord.Record, ...dal.InsertOption) error
}

// InsertMulti refuses the whole batch: an earlier, valid record may not be
// inserted before a later one is found to carry a name that cannot be written.
func TestKeyPathNames_InsertMultiIsRefusedAsAWhole(t *testing.T) {
	good := validNames()
	hostile := validNames()
	hostile.collection = "x; DROP TABLE y"
	hostileField := validNames()
	hostileField.field = "name = 1; --"
	batches := []struct {
		name     string
		position string
		records  func() []dalrecord.Record
		opts     []dal.InsertOption
	}{
		{"collection", "collection", func() []dalrecord.Record {
			return []dalrecord.Record{good.mapRecord("id1"), hostile.mapRecord("id2")}
		}, nil},
		{"field", "field", func() []dalrecord.Record {
			return []dalrecord.Record{good.mapRecord("id1"), hostileField.mapRecord("id2")}
		}, nil},
		{"primary key", "primary key", func() []dalrecord.Record {
			return []dalrecord.Record{
				good.mapRecord("id1"),
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(hostilePrimaryKeyCollection, "id2"), map[string]any{"name": "v"}),
			}
		}, nil},
		// With an ID generator the first statement of a record is an existence
		// check, not the insert.
		{"collection, generated IDs", "collection", func() []dalrecord.Record {
			return []dalrecord.Record{
				dalrecord.NewRecordWithData(good.incompleteKey(), map[string]any{"name": "v"}),
				dalrecord.NewRecordWithData(hostile.incompleteKey(), map[string]any{"name": "v"}),
			}
		}, []dal.InsertOption{dal.WithRandomStringKey(8, 3)}},
	}
	for _, batch := range batches {
		t.Run(batch.name, func(t *testing.T) {
			options := good.options("")
			options.Recordsets[hostile.collection] = NewRecordset(hostile.collection, Table, []dal.FieldRef{dal.Field("id")})
			options.Recordsets[hostilePrimaryKeyCollection] = NewRecordset(hostilePrimaryKeyCollection, Table, []dal.FieldRef{dal.Field("id; --")})
			r := keyPathAPIs(t, options)[1] // the transaction
			api, ok := r.api.(insertMultiAPI)
			if !ok {
				t.Fatalf("%T has no InsertMulti", r.api)
			}
			requireRefused(t, r.recorder, api.InsertMulti(context.Background(), batch.records(), batch.opts...), batch.position)
		})
	}
}

// Update prints the recordset name in the error of a recordset with no primary
// key, so a hostile parent collection must be refused before that message is
// built, not echoed raw and unbounded.
func TestKeyPathNames_UpdateRefusesAHostileParentCollection(t *testing.T) {
	// The sqlite dialect quotes "x; DROP TABLE y", so only a name it cannot quote
	// is refused there.
	cases := []struct{ dialect, hostile string }{
		{"", "x\ny"},
		{"", "x; DROP TABLE y"},
		{"sqlite", "x\ny"},
		{"sqlite", "nul\x00parent"},
	}
	for _, tt := range cases {
		label := strings.NewReplacer("\n", `\n`, "\x00", `\x00`).Replace(tt.hostile)
		t.Run(tt.dialect+"/"+label, func(t *testing.T) {
			parent := dalrecord.NewKeyWithID(tt.hostile, "p1")
			// The leaf collection is valid and configured; the joined recordset
			// name users_<parent> has no primary key configured.
			key := dalrecord.NewKeyWithParentAndID(parent, "users", "u1")
			c := validNames()
			runs := map[string]func(context.Context, keyPathAPI) error{
				"update": func(ctx context.Context, api keyPathAPI) error {
					return api.Update(ctx, key, c.updates())
				},
				"update-multi": func(ctx context.Context, api keyPathAPI) error {
					return api.UpdateMulti(ctx, []*dalrecord.Key{key}, c.updates())
				},
			}
			for name, run := range runs {
				t.Run(name, func(t *testing.T) {
					for _, r := range keyPathAPIs(t, c.options(tt.dialect)) {
						t.Run(r.kind, func(t *testing.T) {
							err := run(context.Background(), r.api)
							requireRefused(t, r.recorder, err, "collection")
							if strings.Contains(err.Error(), "\n") || len(err.Error()) > 200 {
								t.Errorf("the error echoes the name raw or unbounded: %q", err)
							}
						})
					}
				})
			}
		})
	}
}
