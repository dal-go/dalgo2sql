package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
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
	want := "SELECT `name` AS `display\"name` FROM `users\" WHERE 1=1 --` WHERE (`status\"] OR 1=1 --` = ? AND `tenantID` = ? AND `role` IN (?, ?)) ORDER BY `createdAt` DESC LIMIT ? OFFSET ?"
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
	mock.ExpectQuery("SELECT `name` FROM `users` WHERE `tenant` = \\?").WithArgs("t1").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("Ann"))
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
