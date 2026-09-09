package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/condeval"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestCompileStructuredSQLParameterizedAndQuoted(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef(`users" WHERE 1=1 --`, "")).NewQuery().
		Where(dal.NewGroupCondition(dal.And,
			dal.WhereField(`status"] OR 1=1 --`, dal.Equal, `open'] bracket`),
			dal.WhereField("tenantID", dal.Equal, "t1"),
			dal.WhereField("role", dal.In, []string{"admin", "member"}),
		)).OrderBy(dal.DescendingField("createdAt")).Offset(3).Limit(5).
		SelectColumns(dal.Column{Expression: dal.Field("name"), Alias: `display"name`})

	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `name` AS `display\"name` FROM `users\" WHERE 1=1 --` WHERE ((+`status\"] OR 1=1 --` COLLATE BINARY) = ? AND (+`tenantID` COLLATE BINARY) = ? AND ((+`role` COLLATE BINARY) = ? OR (+`role` COLLATE BINARY) = ?)) ORDER BY (`createdAt` COLLATE BINARY) DESC LIMIT ? OFFSET ?"
	if text != want {
		t.Fatalf("SQL:\n got %s\nwant %s", text, want)
	}
	if !reflect.DeepEqual(args, []any{`open'] bracket`, "t1", "admin", "member", 5, 3}) {
		t.Fatalf("args = %#v", args)
	}
	if strings.Contains(text, `open'] bracket`) {
		t.Fatal("literal was interpolated into SQL")
	}
}

func TestRecordsReaderKeepsProjectedRowIdentityPrivate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectColumns(dal.Column{Expression: dal.Field("name")})
	mock.ExpectQuery("SELECT `name`, `id` AS `__dalgo_record_id` FROM `users`").
		WillReturnRows(sqlmock.NewRows([]string{"name", recordIDHelperColumn}).AddRow("Ann", "u1"))
	r, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"users": NewRecordset("users", Table, []dal.FieldRef{dal.Field("id")})}})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Key().ID != "u1" || rec.Key().Collection() != "users" {
		t.Fatalf("key = %#v", rec.Key())
	}
	data := rec.Data().(map[string]any)
	if !reflect.DeepEqual(data, map[string]any{"name": "Ann"}) {
		t.Fatalf("data = %#v", data)
	}
}

func TestCompileStructuredSQLOffsetWithoutLimit(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Offset(2).SelectKeysOnly(reflect.String)
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if text != "SELECT * FROM `users` LIMIT -1 OFFSET ?" || !reflect.DeepEqual(args, []any{2}) {
		t.Fatalf("%q %#v", text, args)
	}
}

func TestCompileStructuredSQLRejectsUnsupportedShapes(t *testing.T) {
	aliased := dal.From(dal.NewRootCollectionRef("users", "u")).NewQuery().SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(aliased); err == nil {
		t.Fatal("expected alias rejection")
	}
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where(dal.NewComparison(dal.Field("x"), "!=", dal.Constant{Value: 1})).SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(q); err == nil {
		t.Fatal("expected operator rejection")
	}
	group := dal.From(dal.NewCollectionGroupRef("users", "")).NewQuery().SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(group); err == nil {
		t.Fatal("expected collection-group rejection")
	}
	badValue := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where(dal.NewComparison(dal.Field("x"), dal.Equal, dal.Constant{Value: map[string]any{"x": 1}})).SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(badValue); err == nil {
		t.Fatal("expected SQL value rejection")
	}
}

func TestSQLiteUnknownPolicyFieldFails(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT); INSERT INTO users VALUES ('u1', 'Ann')`); err != nil {
		t.Fatal(err)
	}
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("missing", dal.Equal, "missing").SelectKeysOnly(reflect.String)
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(context.Background(), text, args...); err == nil {
		t.Fatal("unknown quoted field must fail instead of comparing two string literals")
	}
}

func TestSQLiteOptInRecordsetAndCancellation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("tenant", dal.Equal, "t1").SelectColumns(dal.Column{Expression: dal.Field("name")})
	mock.ExpectQuery(regexp.QuoteMeta("SELECT `name` FROM `users` WHERE (+`tenant` COLLATE BINARY) = ?")).WithArgs("t1").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("Ann"))
	r, err := getRecordsetReaderWithDialect(context.Background(), q, db.QueryContext, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := getReaderBaseWithDialect(canceled, q, db.QueryContext, "sqlite"); err == nil {
		t.Fatal("expected canceled query to fail")
	}
	if _, err := getReaderBaseWithDialect(context.Background(), q, db.QueryContext, "sqlite-unknown"); err == nil {
		t.Fatal("expected unknown dialect rejection")
	}
}

func TestSQLitePolicyPredicatesIgnoreDeclaredCollationAndAffinity(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec("CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, tenant TEXT COLLATE NOCASE); INSERT INTO customers VALUES ('a','Hidden','a'),('b','Visible','A'),('c','TextNumber','1')"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		condition dal.Condition
		want      string
	}{
		{"equal", dal.WhereField("tenant", dal.Equal, "A"), "b"},
		{"in", dal.WhereField("tenant", dal.In, []string{"A"}), "b"},
		{"numeric-equality", dal.WhereField("tenant", dal.Equal, 1), ""},
		{"numeric-range", dal.WhereField("tenant", dal.LessThen, 2), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")})}})
			policy := access.MustPolicy("owner", access.Collection("customers", access.Allow(access.Query, "read").Where(tc.condition).Fields("id", "name")))
			secured, err := access.SecureDB(db, access.WithDatabasePolicies(policy))
			if err != nil {
				t.Fatal(err)
			}
			query := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().OrderBy(dal.AscendingField("id")).Limit(1).SelectColumns(dal.Column{Expression: dal.Field("name")})
			reader, err := secured.ExecuteQueryToRecordsReader(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			row, err := reader.Next()
			if tc.want == "" {
				if !errors.Is(err, io.EOF) {
					t.Fatalf("affinity widened policy: row=%v err=%v", row, err)
				}
				return
			}
			if err != nil || row.Key().ID != tc.want {
				t.Fatalf("collation widened policy before paging: row=%v err=%v", row, err)
			}
			if _, exists := row.Data().(map[string]any)["tenant"]; exists {
				t.Fatal("private predicate field leaked")
			}
		})
	}
}

// The real query must agree with DALgo's JSON/binary64 predicate semantics,
// including when SQLite stores an INTEGER that binary64 cannot represent.
func TestSQLiteNumericPolicyConformanceBeforeLimit(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec("CREATE TABLE customers (id TEXT PRIMARY KEY, score, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	values := []any{int64(9007199254740993), int64(9007199254740992), int64(9007199254740994), int64(-9007199254740993), int64(-9007199254740992), float64(0.1), "1", int64(1)}
	for i, v := range values {
		if _, err = raw.Exec("INSERT INTO customers VALUES (?, ?, 'Customer')", fmt.Sprintf("%02d", i), v); err != nil {
			t.Fatal(err)
		}
	}
	db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")})}})
	for _, operator := range []dal.Operator{dal.Equal, dal.In, dal.GreaterThen, dal.GreaterOrEqual, dal.LessThen, dal.LessOrEqual} {
		for _, bound := range []any{int64(9007199254740992), int64(9007199254740993), float64(9007199254740992), int64(-9007199254740992), float32(0.1), 1} {
			t.Run(fmt.Sprintf("%s/%v/%T", operator, bound, bound), func(t *testing.T) {
				right := bound
				if operator == dal.In {
					right = []any{bound}
				}
				var expression dal.Expression = dal.Constant{Value: right}
				if operator == dal.In {
					expression = dal.Array{Value: right}
				}
				condition := dal.NewComparison(dal.Field("score"), operator, expression)
				expected := ""
				for i, v := range values {
					match, e := condeval.Match(map[string]any{"score": v}, condition)
					if e != nil {
						t.Fatal(e)
					}
					if match {
						expected = fmt.Sprintf("%02d", i)
						break
					}
				}
				policy := access.MustPolicy("owner", access.Collection("customers", access.Allow(access.Query, "read").Where(condition).Fields("id", "name")))
				secured, e := access.SecureDB(db, access.WithDatabasePolicies(policy))
				if e != nil {
					t.Fatal(e)
				}
				q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().OrderBy(dal.AscendingField("id")).Limit(1).SelectColumns(dal.Column{Expression: dal.Field("name")})
				reader, e := secured.ExecuteQueryToRecordsReader(context.Background(), q)
				if e != nil {
					t.Fatal(e)
				}
				defer reader.Close()
				row, e := reader.Next()
				if expected == "" {
					if !errors.Is(e, io.EOF) {
						t.Fatalf("unexpected authorized row %v: %v", row, e)
					}
					return
				}
				if e != nil || row.Key().ID != expected {
					t.Fatalf("query disagrees with DALgo before limit: row=%v err=%v expected=%s", row, e, expected)
				}
			})
		}
	}
}
