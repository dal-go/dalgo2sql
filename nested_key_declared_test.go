package dalgo2sql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// One rule for nested keys in every operation: a key with a parent is refused,
// before any statement, unless a recordset is declared under its joined name.

// undeclaredNestedOptions declares the recordsets of the leaf collection and of
// the parent, and of an unrelated collection, but not the joined name.
func undeclaredNestedOptions(dialect string) DbOptions {
	recordset := func(name string) *Recordset { return NewRecordset(name, Table, []dal.FieldRef{dal.Field("id")}) }
	return DbOptions{
		StructuredQueryDialect: dialect,
		PrimaryKey:             []string{"id"},
		Recordsets: map[string]*Recordset{
			"users":  recordset("users"),
			"lines":  recordset("lines"),
			"orders": recordset("orders"),
		},
	}
}

type nestedKeyOperation struct {
	name string
	run  func(context.Context, keyPathAPI) error
}

// nestedKeyOperations runs each of the eleven operations once, with the nested
// key orders/o1/lines/<id> and, in a second set, with a batch that holds that
// key after a key of a declared recordset.
func nestedKeyOperations() []nestedKeyOperation {
	nested, plain := nestedNames(), validNames()
	return []nestedKeyOperation{
		{"exists", func(ctx context.Context, api keyPathAPI) error {
			_, err := api.Exists(ctx, nested.key("l1"))
			return err
		}},
		{"get", func(ctx context.Context, api keyPathAPI) error {
			return api.Get(ctx, dalrecord.NewRecordWithData(nested.key("l1"), &struct{ Name string }{}))
		}},
		{"get-map", func(ctx context.Context, api keyPathAPI) error {
			return api.Get(ctx, dalrecord.NewRecordWithData(nested.key("l1"), map[string]any{}))
		}},
		{"get-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{
				dalrecord.NewRecordWithData(nested.key("l1"), map[string]any{}),
				dalrecord.NewRecordWithData(nested.key("l2"), map[string]any{}),
			})
		}},
		{"get-multi-one-record", func(ctx context.Context, api keyPathAPI) error {
			return api.GetMulti(ctx, []dalrecord.Record{dalrecord.NewRecordWithData(nested.key("l1"), map[string]any{})})
		}},
		{"get-multi-after-a-declared-recordset", func(ctx context.Context, api keyPathAPI) (err error) {
			for range 25 { // recordsets are read in map order
				err = api.GetMulti(ctx, []dalrecord.Record{
					dalrecord.NewRecordWithData(plain.key("u1"), map[string]any{}),
					dalrecord.NewRecordWithData(nested.key("l1"), map[string]any{}),
				})
			}
			return err
		}},
		{"insert", func(ctx context.Context, api keyPathAPI) error {
			return api.Insert(ctx, nested.mapRecord("l1"))
		}},
		{"insert-generated-id", func(ctx context.Context, api keyPathAPI) error {
			return api.Insert(ctx, dalrecord.NewRecordWithData(nested.incompleteKey(), map[string]any{"name": "v"}), dal.WithRandomStringKey(8, 3))
		}},
		{"insert-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.(insertMultiAPI).InsertMulti(ctx, []dalrecord.Record{plain.mapRecord("u1"), nested.mapRecord("l1")})
		}},
		{"set", func(ctx context.Context, api keyPathAPI) error {
			return api.Set(ctx, nested.mapRecord("l1"))
		}},
		{"set-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.SetMulti(ctx, []dalrecord.Record{plain.mapRecord("u1"), nested.mapRecord("l1")})
		}},
		{"update", func(ctx context.Context, api keyPathAPI) error {
			return api.Update(ctx, nested.key("l1"), nested.updates())
		}},
		{"update-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.UpdateMulti(ctx, []*dalrecord.Key{plain.key("u1"), nested.key("l1")}, nested.updates())
		}},
		{"delete", func(ctx context.Context, api keyPathAPI) error {
			return api.Delete(ctx, nested.key("l1"))
		}},
		{"delete-multi", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{plain.key("u1"), plain.key("u2"), nested.key("l1")})
		}},
		{"delete-multi-only-nested", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{nested.key("l1"), nested.key("l2")})
		}},
	}
}

func TestNestedKey_WithoutADeclaredRecordsetSendsNoStatement(t *testing.T) {
	for _, dialect := range []string{"", dialectSQLite, "postgres"} {
		for _, op := range nestedKeyOperations() {
			t.Run(dialect+"/"+op.name, func(t *testing.T) {
				for _, r := range keyPathAPIsAnswering(t, undeclaredNestedOptions(dialect), func(string) bool { return true }) {
					if op.name == "insert-multi" && r.kind != "transaction" {
						continue // a database handle has no InsertMulti
					}
					t.Run(r.kind, func(t *testing.T) {
						err := op.run(context.Background(), r.api)
						if calls := r.recorder.calls(); len(calls) != 0 {
							t.Fatalf("a statement reached the database: %q", calls)
						}
						if !errors.Is(err, ErrUndeclaredNestedRecordset) {
							t.Fatalf("error = %v, want one wrapping ErrUndeclaredNestedRecordset", err)
						}
					})
				}
			})
		}
	}
}

func TestNestedKey_RefusalNamesTheRecordsetToDeclare(t *testing.T) {
	r := keyPathAPIs(t, undeclaredNestedOptions(""))[0]
	err := r.api.Delete(context.Background(), nestedNames().key("l1"))
	for _, want := range []string{nestedTable, "Recordsets"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want one that names %q", err, want)
		}
	}
}

// GetMulti and Get record the refusal on the records, as they do any refusal.
func TestNestedKey_GetRecordsTheRefusal(t *testing.T) {
	nested := nestedNames()
	r := keyPathAPIs(t, undeclaredNestedOptions(""))[0]
	single := dalrecord.NewRecordWithData(nested.key("l1"), map[string]any{})
	batch := []dalrecord.Record{
		dalrecord.NewRecordWithData(nested.key("l2"), map[string]any{}),
		dalrecord.NewRecordWithData(nested.key("l3"), map[string]any{}),
	}
	_ = r.api.Get(context.Background(), single)
	_ = r.api.GetMulti(context.Background(), batch)
	for _, record := range append(batch, single) {
		if !errors.Is(record.Error(), ErrUndeclaredNestedRecordset) {
			t.Errorf("record error = %v, want one wrapping ErrUndeclaredNestedRecordset", record.Error())
		}
	}
}

// A hostile name is still refused as an unsafe name, whether or not the joined
// name is declared.
func TestNestedKey_AHostileNameIsStillAnUnsafeName(t *testing.T) {
	hostile := nestedNames()
	hostile.parent = dalrecord.NewKeyWithID("x; DROP TABLE y", "p1")
	r := keyPathAPIs(t, undeclaredNestedOptions(""))[0]
	requireRefused(t, r.recorder, r.api.Delete(context.Background(), hostile.key("l1")), "collection")
}

// A declared recordset is the one that is written, whatever its primary key.
func TestNestedKey_DeclaredRecordsetIsWritten(t *testing.T) {
	options := undeclaredNestedOptions("")
	options.Recordsets[nestedTable] = NewRecordset(nestedTable, Table, []dal.FieldRef{dal.Field("line_id")})
	r := keyPathAPIsAnswering(t, options, func(string) bool { return false })[0]
	if err := r.api.Delete(context.Background(), nestedNames().key("l1")); err != nil {
		t.Fatal(err)
	}
	if got := r.recorder.calls(); len(got) != 1 || got[0] != "DELETE FROM lines_orders WHERE line_id = ?" {
		t.Errorf("statements = %q", got)
	}
}

// Delete follows the declared primary key: a nested key whose joined recordset is
// declared with no primary key column, or with several, is refused as Update
// refuses it, before any statement. It used to send the ID of the key alone,
// as DELETE ... WHERE ID = ?, against the column named ID: the natural schema of
// nested rows has a composite key, and the statement matched the row of every parent.
func TestNestedKey_DeleteFollowsTheDeclaredPrimaryKey(t *testing.T) {
	nested, plain := nestedNames(), validNames()
	operations := []nestedKeyOperation{
		{"delete", func(ctx context.Context, api keyPathAPI) error {
			return api.Delete(ctx, nested.key("l1"))
		}},
		{"delete-multi, one key", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{nested.key("l1")})
		}},
		{"delete-multi, several keys", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{nested.key("l1"), nested.key("l2")})
		}},
		{"delete-multi, after a key that could be deleted", func(ctx context.Context, api keyPathAPI) error {
			return api.DeleteMulti(ctx, []*dalrecord.Key{plain.key("u1"), nested.key("l1")})
		}},
	}
	cases := []struct {
		name      string
		recordset *Recordset
		want      error  // the error the operation wraps, if any
		wantText  string // text of the error
	}{
		{"declared with nil", nil, ErrUndeclaredNestedRecordset, nestedTable},
		{"declared with no primary key", NewRecordset(nestedTable, Table, nil),
			nil, "primary key is not defined for " + nestedTable},
		{"declared with an empty primary key", NewRecordset(nestedTable, Table, []dal.FieldRef{}),
			nil, "primary key is not defined for " + nestedTable},
		{"declared with two primary key fields", NewRecordset(nestedTable, Table, []dal.FieldRef{dal.Field("order_id"), dal.Field("id")}),
			dal.ErrNotImplementedYet, "composite primary key"},
	}
	for _, dialect := range []string{"", dialectSQLite} {
		for _, tt := range cases {
			for _, op := range operations {
				t.Run(dialect+"/"+tt.name+"/"+op.name, func(t *testing.T) {
					options := undeclaredNestedOptions(dialect)
					options.Recordsets[nestedTable] = tt.recordset
					for _, r := range keyPathAPIsAnswering(t, options, func(string) bool { return true }) {
						t.Run(r.kind, func(t *testing.T) {
							err := op.run(context.Background(), r.api)
							if calls := r.recorder.calls(); len(calls) != 0 {
								t.Fatalf("a statement reached the database: %q", calls)
							}
							if err == nil || !strings.Contains(err.Error(), tt.wantText) {
								t.Fatalf("error = %v, want one that mentions %q", err, tt.wantText)
							}
							if tt.want != nil && !errors.Is(err, tt.want) {
								t.Fatalf("error = %v, want one wrapping %v", err, tt.want)
							}
						})
					}
				})
			}
		}
	}
}
