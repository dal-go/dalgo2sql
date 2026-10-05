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

// A write with nothing to write is an error of the call: no statement is sent
// and nothing panics.

// noFieldRecords are records of the users recordset (primary key ID) that carry
// no field to write.
func noFieldRecords() map[string]func(id string) dalrecord.Record {
	key := func(id string) *dalrecord.Key { return dalrecord.NewKeyWithID("users", id) }
	return map[string]func(string) dalrecord.Record{
		"empty map":    func(id string) dalrecord.Record { return dalrecord.NewRecordWithData(key(id), map[string]any{}) },
		"empty struct": func(id string) dalrecord.Record { return dalrecord.NewRecordWithData(key(id), &struct{}{}) },
		"map of the primary key only": func(id string) dalrecord.Record {
			return dalrecord.NewRecordWithData(key(id), map[string]any{"ID": id})
		},
		"struct of the primary key only": func(id string) dalrecord.Record {
			return dalrecord.NewRecordWithData(key(id), &struct{ ID string }{id})
		},
	}
}

func TestUpdate_WithNoUpdatesIsAnErrorBeforeAnyStatement(t *testing.T) {
	c := validNames()
	for name, updates := range map[string][]update.Update{"nil": nil, "empty": {}} {
		for _, dialect := range []string{"", dialectSQLite} {
			t.Run(name+"/"+dialect, func(t *testing.T) {
				for _, r := range keyPathAPIsAnswering(t, c.options(dialect), func(string) bool { return true }) {
					t.Run(r.kind, func(t *testing.T) {
						for op, run := range map[string]func() error{
							"update":       func() error { return r.api.Update(context.Background(), c.key("id1"), updates) },
							"update-multi": func() error { return r.api.UpdateMulti(context.Background(), c.keys(), updates) },
							"update-multi, no keys": func() error {
								return r.api.UpdateMulti(context.Background(), nil, updates)
							},
						} {
							if err := run(); !errors.Is(err, ErrNoFieldsToWrite) {
								t.Errorf("%s: error = %v, want one wrapping ErrNoFieldsToWrite", op, err)
							}
						}
						if calls := r.recorder.calls(); len(calls) != 0 {
							t.Errorf("a statement reached the database: %q", calls)
						}
					})
				}
			})
		}
	}
}

func TestSet_WithNoFieldIsAnErrorBeforeAnyStatement(t *testing.T) {
	c := validNames()
	c.pk = "ID"
	for name, newRecord := range noFieldRecords() {
		for _, exists := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("panic: %v", p)
					}
				}()
				for _, r := range keyPathAPIsAnswering(t, c.options(""), func(string) bool { return exists }) {
					t.Run(r.kind, func(t *testing.T) {
						errs := map[string]error{
							"set": r.api.Set(context.Background(), newRecord("id1")),
							// The batch is refused as a whole: the first record is a good one.
							"set-multi": r.api.SetMulti(context.Background(), []dalrecord.Record{c.mapRecord("id0"), newRecord("id1")}),
						}
						for op, err := range errs {
							if !errors.Is(err, ErrNoFieldsToWrite) {
								t.Errorf("%s: error = %v, want one wrapping ErrNoFieldsToWrite", op, err)
							}
						}
						if calls := r.recorder.calls(); len(calls) != 0 {
							t.Errorf("a statement reached the database: %q", calls)
						}
					})
				}
			})
		}
	}
}

// An insert with a key ID writes the ID, so it needs no field; one with neither
// has no column to write.
func TestInsert_WithNoFieldNeedsAKeyID(t *testing.T) {
	c := validNames()
	withID := dalrecord.NewRecordWithData(c.key("id1"), map[string]any{})
	noID := dalrecord.NewRecordWithData(c.incompleteKey(), map[string]any{})
	for _, r := range keyPathAPIsAnswering(t, c.options(""), func(string) bool { return false }) {
		t.Run(r.kind, func(t *testing.T) {
			if err := r.api.Insert(context.Background(), withID); err != nil {
				t.Fatal(err)
			}
			if got := r.recorder.calls(); len(got) != 1 || got[0] != "INSERT INTO users(id) VALUES (?)" {
				t.Fatalf("statements = %q", got)
			}
		})
	}
	for _, r := range keyPathAPIs(t, c.options("")) {
		t.Run(r.kind+"/no ID", func(t *testing.T) {
			if err := r.api.Insert(context.Background(), noID); !errors.Is(err, ErrNoFieldsToWrite) {
				t.Errorf("error = %v, want one wrapping ErrNoFieldsToWrite", err)
			}
		})
	}
	tx := keyPathAPIs(t, c.options(""))[1]
	err := tx.api.(insertMultiAPI).InsertMulti(context.Background(), []dalrecord.Record{c.mapRecord("id0"), noID})
	if !errors.Is(err, ErrNoFieldsToWrite) {
		t.Errorf("InsertMulti: error = %v, want one wrapping ErrNoFieldsToWrite", err)
	}
	if calls := tx.recorder.calls(); len(calls) != 0 {
		t.Errorf("a statement reached the database: %q", calls)
	}
}

// The statement builder is the last line: it returns the error too.
func TestBuildSingleRecordQuery_NoFieldsIsAnError(t *testing.T) {
	options := DbOptions{Recordsets: map[string]*Recordset{
		"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("ID"), dal.Field("Name")}),
	}}
	// Name is part of the primary key, so there is no field left to update.
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &user2{Name: "John"})
	q, err := buildSingleRecordQuery(updateOperation, options, record)
	if !errors.Is(err, ErrNoFieldsToWrite) {
		t.Errorf("update: error = %v, want one wrapping ErrNoFieldsToWrite", err)
	}
	if q.text != "" || q.args != nil {
		t.Errorf("an error left a statement behind: %+v", q)
	}
	noColumn := dalrecord.NewRecordWithData(dalrecord.NewIncompleteKey("users", reflect.String, nil), map[string]any{})
	if q, err = buildSingleRecordQuery(insertOperation, options, noColumn); !errors.Is(err, ErrNoFieldsToWrite) || q.text != "" {
		t.Errorf("insert: = %+v, %v; want an error wrapping ErrNoFieldsToWrite and no statement", q, err)
	}
}

// A recordset declared with no primary key has no column to select by: GetMulti
// of several records is an error, not an index out of range.
func TestGetMulti_DeclaredRecordsetWithNoPrimaryKeyIsAnError(t *testing.T) {
	options := DbOptions{
		PrimaryKey: []string{"id"},
		Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, nil)},
	}
	for _, r := range keyPathAPIs(t, options) {
		t.Run(r.kind, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("panic: %v", p)
				}
			}()
			records := []dalrecord.Record{
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &struct{ Name string }{}),
				dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id2"), &struct{ Name string }{}),
			}
			if err := r.api.GetMulti(context.Background(), records); err == nil {
				t.Error("no error")
			}
			if calls := r.recorder.calls(); len(calls) != 0 {
				t.Errorf("a statement reached the database: %q", calls)
			}
			for _, record := range records {
				if record.Error() == nil {
					t.Error("the refusal is not on the record")
				}
			}
		})
	}
}

func TestGetSelectFields_DeclaredRecordsetWithNoPrimaryKeyIsAnError(t *testing.T) {
	options := DbOptions{Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, nil)}}
	record := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "id1"), &struct{ Name string }{})
	fields, err := getSelectFields(true, options, record)
	if err == nil || fields != nil {
		t.Errorf("= %v, %v; want an error and no fields", fields, err)
	}
	// Without the primary key in the list, the recordset is not asked.
	if fields, err = getSelectFields(false, options, record); err != nil || len(fields) != 1 {
		t.Errorf("= %v, %v", fields, err)
	}
}

// UpdateMulti checks, before its first statement, that the recordset of every key has one
// primary key to find the row by: on a database handle the keys are updated one by one, and
// a key that is refused later would leave the earlier ones updated.
func TestUpdateMulti_EveryKeysRecordsetNeedsOnePrimaryKeyBeforeTheFirstStatement(t *testing.T) {
	c := validNames()
	options := compositeOptions("")
	options.Recordsets["users"] = NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")})
	options.Recordsets["empty"] = NewRecordset("empty", Table, nil)
	cases := []struct {
		name     string
		other    *dalrecord.Key
		wantText string
		want     error
	}{
		{"a recordset that is not declared", dalrecord.NewKeyWithID("accounts", "a1"), "primary key is not defined for accounts", nil},
		{"a recordset declared with no primary key", dalrecord.NewKeyWithID("empty", "e1"), "primary key is not defined for empty", nil},
		{"a recordset with a composite primary key", dalrecord.NewKeyWithID("pairs", "p1"), "composite primary key", dal.ErrNotImplementedYet},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return true }) {
				err := r.api.UpdateMulti(context.Background(), []*dalrecord.Key{c.key("u1"), tt.other}, c.updates())
				if err == nil || !strings.Contains(err.Error(), tt.wantText) || !strings.Contains(err.Error(), "#2 of 2") {
					t.Errorf("%s: error = %v, want the second key refused with %q", r.kind, err, tt.wantText)
				}
				if tt.want != nil && !errors.Is(err, tt.want) {
					t.Errorf("%s: error = %v, want one wrapping %v", r.kind, err, tt.want)
				}
				if calls := r.recorder.calls(); len(calls) != 0 {
					t.Errorf("%s: a statement reached the database: %q", r.kind, calls)
				}
			}
		})
	}
}
