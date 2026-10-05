package dalgo2sql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	_ "modernc.org/sqlite"
)

// A read of one record or of several, and an operation given a nil key, are errors of the call
// when the caller's own input cannot be read into or addressed, returned before any statement is
// sent: a record with no data, a nil record, data that is not a pointer to a struct or a map with
// string keys (a struct given by value, a scalar, a slice, a pointer to a scalar, a pointer to a
// pointer), a struct with a field that is not exported, a nil map given by value, a map whose
// elements cannot hold a column value, a map with keys that are not strings, and a nil key. The
// tests below take each input in turn through every entry point that reads it, on a database
// handle and in a transaction.

// readTargetCase is one input of a read: the data each of the records of the read is given.
type readTargetCase struct {
	name string
	data func() any
}

// refusedReadTargets are the read targets a read cannot fill. Each is a function, so that the
// records of one read do not share a target.
func refusedReadTargets() []readTargetCase {
	return []readTargetCase{
		{"no data", func() any { return nil }},
		{"a struct by value", func() any { return struct{ Name string }{} }},
		{"a nil pointer to a struct", func() any { return (*struct{ Name string })(nil) }},
		{"a pointer to a struct with an unexported field", func() any { return &unexportedFieldStruct{} }},
		{"a pointer to a struct that embeds an unexported type", func() any { return &embeddedUnexportedStruct{} }},
		{"a nil map", func() any { return map[string]any(nil) }},
		{"a map whose elements are strings", func() any { return map[string]string{} }},
		{"a map whose elements are of an interface with a method", func() any { return map[string]interface{ String() string }{} }},
		{"a map with integer keys", func() any { return map[int]any{} }},
		{"a pointer to a map whose elements are strings", func() any { return &map[string]string{} }},
		{"a string", func() any { return "text" }},
		{"an integer", func() any { return 1 }},
		{"a slice", func() any { return []string{"a"} }},
		{"a pointer to a string", func() any { text := ""; return &text }},
		{"a pointer to a slice", func() any { return &[]string{} }},
		{"a pointer to a type that scans itself", func() any { return new(selfScanningText) }},
		{"a pointer to a pointer to a struct", func() any { target := &struct{ Name string }{}; return &target }},
		{"a pointer to a nil pointer to a struct", func() any { return new(*struct{ Name string }) }},
		{"a pointer to an interface", func() any { var target any = &struct{ Name string }{}; return &target }},
	}
}

// selfScanningText is a named string that takes a column value itself, as a sql.Scanner does.
type selfScanningText string

func (s *selfScanningText) Scan(value any) error {
	*s = selfScanningText(fmt.Sprint(value))
	return nil
}

// readEntryPoints are the ways to read the records of one recordset, each given the data of
// every record it reads.
func readEntryPoints(c nameCase) []struct {
	name string
	run  func(context.Context, keyPathAPI, func() any) error
} {
	record := func(id string, data func() any) dalrecord.Record {
		return dalrecord.NewRecordWithData(c.key(id), data())
	}
	return []struct {
		name string
		run  func(context.Context, keyPathAPI, func() any) error
	}{
		{"Get", func(ctx context.Context, api keyPathAPI, data func() any) error {
			return api.Get(ctx, record("id1", data))
		}},
		{"GetMulti of one record", func(ctx context.Context, api keyPathAPI, data func() any) error {
			return api.GetMulti(ctx, []dalrecord.Record{record("id1", data)})
		}},
		{"GetMulti of several records", func(ctx context.Context, api keyPathAPI, data func() any) error {
			return api.GetMulti(ctx, []dalrecord.Record{record("id1", data), record("id2", data)})
		}},
		// The records of one batch are read in turn: a refusal found in the second must stop
		// the first from being sent.
		{"GetMulti of a record that can be read and one that cannot", func(ctx context.Context, api keyPathAPI, data func() any) error {
			good := dalrecord.NewRecordWithData(c.key("id0"), map[string]any{})
			return api.GetMulti(ctx, []dalrecord.Record{good, record("id1", data)})
		}},
	}
}

func TestKeyReads_OfATargetThatCannotBeReadIntoAreErrorsBeforeAnyStatement(t *testing.T) {
	c := validNames()
	for _, dialect := range []string{"", dialectSQLite} {
		for _, target := range refusedReadTargets() {
			for _, entry := range readEntryPoints(c) {
				t.Run(dialect+"/"+target.name+"/"+entry.name, func(t *testing.T) {
					for _, r := range keyPathAPIs(t, c.options(dialect)) {
						err := noPanic(t, func() error { return entry.run(context.Background(), r.api, target.data) })
						if !errors.Is(err, dal.ErrNotSupported) {
							t.Errorf("%s: error = %v, want one wrapping dal.ErrNotSupported", r.kind, err)
						}
						if got := r.recorder.calls(); len(got) != 0 {
							t.Errorf("%s: a statement reached the database: %q", r.kind, got)
						}
					}
				})
			}
		}
	}
}

// A refused read says so on the record it could not fill, as a refused name does, so a caller
// that reads the error of each record of a batch finds it.
func TestKeyReads_ARefusedTargetIsTheErrorOfItsRecord(t *testing.T) {
	c := validNames()
	for _, r := range keyPathAPIs(t, c.options("")) {
		bad := dalrecord.NewRecordWithData(c.key("id1"), struct{ Name string }{})
		good := dalrecord.NewRecordWithData(c.key("id0"), map[string]any{})
		err := noPanic(t, func() error { return r.api.GetMulti(context.Background(), []dalrecord.Record{good, bad}) })
		if !errors.Is(err, dal.ErrNotSupported) || !errors.Is(bad.Error(), dal.ErrNotSupported) || !errors.Is(good.Error(), dal.ErrNotSupported) {
			t.Errorf("%s: error = %v, records' errors = %v, %v; want dal.ErrNotSupported in each", r.kind, err, good.Error(), bad.Error())
		}
	}
}

// The records of one batch are read by one statement and filled from its rows alike, so a batch
// of a map and a struct has no common way to be read.
func TestKeyReads_ABatchThatMixesMapAndStructTargetsIsAnErrorBeforeAnyStatement(t *testing.T) {
	c := validNames()
	type user struct{ Name string }
	for _, first := range []bool{true, false} {
		for _, r := range keyPathAPIs(t, c.options("")) {
			var records []dalrecord.Record
			for _, data := range []any{map[string]any{}, &user{}} {
				records = append(records, dalrecord.NewRecordWithData(c.key("id"+string(rune('1'+len(records)))), data))
			}
			if !first {
				records[0], records[1] = records[1], records[0]
			}
			err := noPanic(t, func() error { return r.api.GetMulti(context.Background(), records) })
			if !errors.Is(err, dal.ErrNotSupported) {
				t.Errorf("%s: error = %v, want one wrapping dal.ErrNotSupported", r.kind, err)
			}
			if got := r.recorder.calls(); len(got) != 0 {
				t.Errorf("%s: a statement reached the database: %q", r.kind, got)
			}
		}
	}
}

// A pointer to a nil map is a target: the read allocates the map, for one record as it always
// did and for several, where the map is stored into before any scan could allocate it.
func TestKeyReads_APointerToANilMapIsFilledByAReadOfSeveralRecords(t *testing.T) {
	c := validNames()
	var first, second map[string]any
	records := []dalrecord.Record{
		dalrecord.NewRecordWithData(c.key("id1"), &first),
		dalrecord.NewRecordWithData(c.key("id2"), &second),
	}
	db := openTestSQLiteDB(t, `CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT);
		INSERT INTO users VALUES ('id1', 'one'), ('id2', 'two')`)
	if err := noPanic(t, func() error {
		return (&database{db: db, options: c.options("")}).GetMulti(context.Background(), records)
	}); err != nil {
		t.Fatal(err)
	}
	if first["name"] != "one" || second["name"] != "two" {
		t.Errorf("read %v and %v, want the names one and two", first, second)
	}
}

// A target that is not a pointer to a struct or a map is refused where the table has the rows too:
// a read of several records that went on to fill one of them stored into it after the statement.
func TestKeyReads_AScalarTargetIsRefusedWhereTheRowsExist(t *testing.T) {
	c := validNames()
	db := openTestSQLiteDB(t, `CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT);
		INSERT INTO users VALUES ('id1', 'one'), ('id2', 'two')`)
	for _, target := range refusedReadTargets() {
		t.Run(target.name, func(t *testing.T) {
			records := []dalrecord.Record{
				dalrecord.NewRecordWithData(c.key("id1"), target.data()),
				dalrecord.NewRecordWithData(c.key("id2"), target.data()),
			}
			err := noPanic(t, func() error {
				return (&database{db: db, options: c.options(dialectSQLite)}).GetMulti(context.Background(), records)
			})
			if !errors.Is(err, dal.ErrNotSupported) {
				t.Errorf("error = %v, want one wrapping dal.ErrNotSupported", err)
			}
		})
	}
}

// A nil record is an error of the call, as a record that has no data is: Get, and GetMulti with a
// nil in any position of its batch, return an error wrapping dal.ErrNotSupported before any
// statement, and the records of the batch that are not nil say so too.
func TestKeyReads_ANilRecordIsAnErrorBeforeAnyStatement(t *testing.T) {
	c := validNames()
	for _, dialect := range []string{"", dialectSQLite} {
		for _, entry := range []struct {
			name     string
			run      func(context.Context, keyPathAPI, dalrecord.Record) error
			withGood bool // the batch holds the record that is not nil, which says so too
		}{
			{"Get", func(ctx context.Context, api keyPathAPI, good dalrecord.Record) error { return api.Get(ctx, nil) }, false},
			{"GetMulti of one nil", func(ctx context.Context, api keyPathAPI, good dalrecord.Record) error {
				return api.GetMulti(ctx, []dalrecord.Record{nil})
			}, false},
			{"GetMulti with a nil first", func(ctx context.Context, api keyPathAPI, good dalrecord.Record) error {
				return api.GetMulti(ctx, []dalrecord.Record{nil, good})
			}, true},
			{"GetMulti with a nil last", func(ctx context.Context, api keyPathAPI, good dalrecord.Record) error {
				return api.GetMulti(ctx, []dalrecord.Record{good, nil})
			}, true},
		} {
			t.Run(dialect+"/"+entry.name, func(t *testing.T) {
				for _, r := range keyPathAPIs(t, c.options(dialect)) {
					good := dalrecord.NewRecordWithData(c.key("id1"), map[string]any{})
					err := noPanic(t, func() error { return entry.run(context.Background(), r.api, good) })
					if !errors.Is(err, dal.ErrNotSupported) || errors.Is(err, errReachedDatabase) {
						t.Errorf("%s: error = %v, want one wrapping dal.ErrNotSupported that is not the database's", r.kind, err)
					}
					if entry.withGood && !errors.Is(good.Error(), dal.ErrNotSupported) {
						t.Errorf("%s: the record that is not nil has error %v, want one wrapping dal.ErrNotSupported", r.kind, good.Error())
					}
					if got := r.recorder.calls(); len(got) != 0 {
						t.Errorf("%s: a statement reached the database: %q", r.kind, got)
					}
				}
			})
		}
	}
}

// nilKeyOperations are the operations that take a key, or a record that has one, given a nil key.
func nilKeyOperations() []struct {
	name string
	run  func(context.Context, keyPathAPI) error
} {
	noKey := func(data any) dalrecord.Record { return mockRecord{key: nil, data: data} }
	updates := []update.Update{rawUpdate{name: "name", value: "v"}}
	return []struct {
		name string
		run  func(context.Context, keyPathAPI) error
	}{
		{"Exists", func(ctx context.Context, api keyPathAPI) error { _, err := api.Exists(ctx, nil); return err }},
		{"Get", func(ctx context.Context, api keyPathAPI) error { return api.Get(ctx, noKey(map[string]any{})) }},
		{"GetMulti", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{noKey(map[string]any{}), noKey(map[string]any{})})
		}},
		{"Insert", func(ctx context.Context, api keyPathAPI) error {
			return api.Insert(ctx, noKey(map[string]any{"name": "v"}))
		}},
		{"Set", func(ctx context.Context, api keyPathAPI) error {
			return api.Set(ctx, noKey(map[string]any{"name": "v"}))
		}},
		{"SetMulti", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{noKey(map[string]any{"name": "v"})})
		}},
		{"Update", func(ctx context.Context, api keyPathAPI) error { return api.Update(ctx, nil, updates) }},
		{"UpdateMulti", func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{nil}, updates)
		}},
		{"Delete", func(ctx context.Context, api keyPathAPI) error { return api.Delete(ctx, nil) }},
		{"DeleteMulti", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{dalrecord.NewKeyWithID("users", "id0"), nil})
		}},
	}
}

func TestKeyOperations_GivenANilKeyAreErrorsBeforeAnyStatement(t *testing.T) {
	c := validNames()
	for _, dialect := range []string{"", dialectSQLite} {
		for _, op := range nilKeyOperations() {
			t.Run(dialect+"/"+op.name, func(t *testing.T) {
				for _, r := range keyPathAPIs(t, c.options(dialect)) {
					err := noPanic(t, func() error { return op.run(context.Background(), r.api) })
					if !errors.Is(err, dal.ErrNotSupported) || errors.Is(err, errReachedDatabase) {
						t.Errorf("%s: error = %v, want one wrapping dal.ErrNotSupported that is not the database's", r.kind, err)
					}
					if got := r.recorder.calls(); len(got) != 0 {
						t.Errorf("%s: a statement reached the database: %q", r.kind, got)
					}
				}
			})
		}
	}
}

// getSelectFields is given a record with no key, or a key with no collection, only by a caller
// inside the package, and says so as an error.
func TestGetSelectFields_RefusesAKeyThatNamesNoRecordset(t *testing.T) {
	data := &struct{ Name string }{}
	for _, tt := range []struct {
		name   string
		record dalrecord.Record
	}{
		{"no key", mockRecord{key: nil, data: data}},
		{"a key with no collection", mockRecord{key: &dalrecord.Key{}, data: data}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := noPanicFields(t, func() ([]string, error) { return getSelectFields(true, DbOptions{}, tt.record) })
			if !errors.Is(err, dal.ErrNotSupported) || fields != nil {
				t.Errorf("= %v, %v; want no fields and an error wrapping dal.ErrNotSupported", fields, err)
			}
		})
	}
}

func noPanicFields(t *testing.T, call func() ([]string, error)) (fields []string, err error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("panic: %v", p)
		}
	}()
	return call()
}
