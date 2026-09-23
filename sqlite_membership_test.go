package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	_ "modernc.org/sqlite"
)

func TestSQLiteMembershipThreeValuedLogic(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value)"); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id    int
		value any
	}{{1, int64(1)}, {2, int64(2)}, {3, nil}, {4, "1"}} {
		if _, err := db.Exec("INSERT INTO items (id, value) VALUES (?, ?)", row.id, row.value); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name  string
		id    int
		list  []any
		in    *bool
		notIn *bool
	}{
		{"matched with NULL", 1, []any{int64(1), nil}, truth(true), truth(false)},
		{"unmatched with NULL", 2, []any{int64(1), nil}, nil, nil},
		{"NULL left", 3, []any{int64(1)}, nil, nil},
		{"NULL left empty", 3, []any{}, truth(false), truth(true)},
		{"unmatched empty", 2, []any{}, truth(false), truth(true)},
		{"unmatched", 2, []any{int64(1)}, truth(false), truth(true)},
		{"only NULL right", 2, []any{nil}, nil, nil},
		{"string is not number", 4, []any{int64(1)}, truth(false), truth(true)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, operator := range []dal.Operator{dal.In, dal.NotIn} {
				condition := dal.NewComparison(dal.Field("value"), operator, dal.Array{Value: tc.list})
				expression, args, err := compileSQLCondition(condition, "")
				if err != nil {
					t.Fatal(err)
				}
				var got sql.NullBool
				args = append(args, tc.id)
				if err := db.QueryRow("SELECT ("+expression+") FROM items WHERE id = ?", args...).Scan(&got); err != nil {
					t.Fatal(err)
				}
				want := tc.in
				if operator == dal.NotIn {
					want = tc.notIn
				}
				if want == nil && got.Valid || want != nil && (!got.Valid || got.Bool != *want) {
					t.Errorf("%s: result = %+v, want %v", operator, got, want)
				}
			}
		})
	}
}

func truth(value bool) *bool { return &value }

func TestCompileSQLiteNotInBindsValues(t *testing.T) {
	value := "x') OR 1=1 --"
	q := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
		Where(dal.NewComparison(dal.Field("value"), dal.NotIn, dal.Array{Value: []string{"a", value}})).
		SelectColumns(dal.Column{Expression: dal.Field("id")})
	query, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "WHERE NOT ((+`value` COLLATE BINARY) = ? OR (+`value` COLLATE BINARY) = ?)") {
		t.Fatalf("unexpected SQL: %s", query)
	}
	if strings.Contains(query, value) || !reflect.DeepEqual(args, []any{"a", value}) {
		t.Fatalf("unsafe or misplaced bindings: SQL=%s args=%#v", query, args)
	}
	if _, _, err := compileSQLCondition(dal.NewComparison(dal.Field("value"), dal.NotIn, dal.NewConstant("a")), ""); err == nil || !strings.Contains(err.Error(), "NOT IN requires an array") {
		t.Fatalf("invalid RHS error = %v", err)
	}
}

func TestSQLiteNotInQueryReturnsExpectedRows(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value); INSERT INTO items VALUES (1, 1), (2, 2), (3, NULL), (4, '1')"); err != nil {
		t.Fatal(err)
	}
	backend := dal.NewDB(&database{db: raw, options: DbOptions{StructuredQueryDialect: "sqlite"}})
	for _, tc := range []struct {
		name string
		list []any
		want []int64
	}{
		{"number", []any{int64(1)}, []int64{2, 4}},
		{"number and NULL", []any{int64(1), nil}, nil},
		{"empty", []any{}, []int64{1, 2, 3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
				Where(dal.NewComparison(dal.Field("value"), dal.NotIn, dal.Array{Value: tc.list})).
				OrderBy(dal.AscendingField("id")).
				SelectColumns(dal.Column{Expression: dal.Field("id")})
			reader, err := backend.ExecuteQueryToRecordsReader(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			records, err := dal.ReadAllToRecords(context.Background(), reader)
			_ = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			var ids []int64
			for _, record := range records {
				data := record.Data().(map[string]any)
				ids = append(ids, data["id"].(int64))
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("ids = %v, want %v", ids, tc.want)
			}
		})
	}
}
