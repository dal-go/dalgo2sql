package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/dal-go/dalgo/access"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

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

// TestCompileStructuredSQLSourceAlias covers the DTQL shape datatug's
// customer-invoices demo query actually ships (`from: {name: Invoice,
// alias: i}` with unqualified column/where/orderBy field references):
// the alias must appear in the FROM clause and unqualified fields must
// still compile even though a source alias is declared.
func TestCompileStructuredSQLSourceAlias(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("Invoice", "i")).NewQuery().
		WhereField("CustomerId", dal.Equal, 3).
		OrderBy(dal.DescendingField("InvoiceDate")).
		SelectColumns(
			dal.Column{Expression: dal.Field("InvoiceId")},
			dal.Column{Expression: dal.Field("Total")},
		)
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `InvoiceId`, `Total` FROM `Invoice` AS `i` WHERE `CustomerId` = ? ORDER BY `InvoiceDate` DESC"
	if text != want {
		t.Fatalf("SQL:\n got %s\nwant %s", text, want)
	}
	if !reflect.DeepEqual(args, []any{3}) {
		t.Fatalf("args = %#v", args)
	}
}

// TestCompileStructuredSQLQualifiedFieldReferences proves a FieldRef whose
// Source() matches the declared alias compiles to an alias-qualified
// column and that Source() must match exactly — a field claiming a
// different, unrelated source is rejected rather than silently resolved
// against the single table this dialect supports (joins are unsupported,
// so any other qualifier is definitionally unresolvable).
func TestCompileStructuredSQLQualifiedFieldReferences(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("Invoice", "i")).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("i", "CustomerId"), dal.Equal, dal.Constant{Value: 3})).
		OrderBy(dal.Descending(dal.NewFieldRef("i", "InvoiceDate"))).
		SelectColumns(dal.Column{Expression: dal.NewFieldRef("i", "InvoiceId")})
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT `i`.`InvoiceId` FROM `Invoice` AS `i` WHERE `i`.`CustomerId` = ? ORDER BY `i`.`InvoiceDate` DESC"
	if text != want {
		t.Fatalf("SQL:\n got %s\nwant %s", text, want)
	}
	if !reflect.DeepEqual(args, []any{3}) {
		t.Fatalf("args = %#v", args)
	}
}

// TestCompileStructuredSQLRejectsAliasContainingQuoteChar proves alias
// validation, not identifier-quoting, is what stops an alias from
// breaking out of the backtick-quoted identifier this dialect emits: a
// backtick in the alias is rejected outright rather than accepted and
// escaped.
func TestCompileStructuredSQLRejectsAliasContainingQuoteChar(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("users", "u`x")).NewQuery().SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(q); err == nil {
		t.Fatal("expected alias containing a backtick to be rejected")
	}
}

func TestRecordsReaderKeepsProjectedRowIdentityPrivate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
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
	invalidAlias := dal.From(dal.NewRootCollectionRef("users", `u"; DROP TABLE users --`)).NewQuery().SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(invalidAlias); err == nil {
		t.Fatal("expected non-identifier alias rejection")
	}
	unknownQualifier := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("other", "x"), dal.Equal, dal.Constant{Value: 1})).
		SelectKeysOnly(reflect.String)
	if _, _, err := compileStructuredSQL(unknownQualifier); err == nil {
		t.Fatal("expected unknown source qualifier rejection")
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
	defer func() { _ = db.Close() }()
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
	defer func() { _ = db.Close() }()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("tenant", dal.Equal, "t1").SelectColumns(dal.Column{Expression: dal.Field("name")})
	mock.ExpectQuery(regexp.QuoteMeta("SELECT `name` FROM `users` WHERE (+`tenant` COLLATE BINARY) = ?")).WithArgs("t1").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("Ann"))
	r, err := getRecordsetReaderWithDialect(context.Background(), q, db.QueryContext, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := getReaderBaseWithDialect(canceled, q, db.QueryContext, "sqlite"); err == nil {
		t.Fatal("expected canceled query to fail")
	}
	if _, err := getReaderBaseWithDialect(context.Background(), q, db.QueryContext, "sqlite-unknown"); err == nil {
		t.Fatal("expected unknown dialect rejection")
	}
}

// stubValuer is a minimal driver.Valuer, proving validateSQLValue accepts
// any type that knows how to represent itself as a driver value, not just
// the specific Go kinds it special-cases.
type stubValuer struct{ v any }

func (s stubValuer) Value() (driver.Value, error) { return s.v, nil }

func TestValidateSQLValueAcceptedAndRejectedKinds(t *testing.T) {
	accepted := []any{
		nil,
		stubValuer{v: "x"},
		time.Now(),
		true,
		42,
		int64(42),
		3.14,
		[]byte("blob"),
	}
	for _, value := range accepted {
		if err := validateSQLValue(value); err != nil {
			t.Errorf("validateSQLValue(%#v) = %v, want nil", value, err)
		}
	}

	rejected := []any{
		[]int{1, 2},
		map[string]any{"x": 1},
		struct{ X int }{X: 1},
	}
	for _, value := range rejected {
		if err := validateSQLValue(value); err == nil {
			t.Errorf("validateSQLValue(%#v) = nil, want an error", value)
		}
	}
}

func TestPrimaryKeyForQueryFallbacks(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectKeysOnly(reflect.String)

	if pk := primaryKeyForQuery(DbOptions{}, dal.NewTextQuery("SELECT 1", nil)); pk != "" {
		t.Errorf("non-structured query: primaryKeyForQuery = %q, want empty", pk)
	}

	multiField := NewRecordset("users", Table, []dal.FieldRef{dal.Field("id"), dal.Field("tenant")})
	if pk := primaryKeyForQuery(DbOptions{Recordsets: map[string]*Recordset{"users": multiField}}, q); pk != "" {
		t.Errorf("multi-field recordset key: primaryKeyForQuery = %q, want empty (falls through)", pk)
	}

	if pk := primaryKeyForQuery(DbOptions{PrimaryKey: []string{"id"}}, q); pk != "id" {
		t.Errorf("DbOptions.PrimaryKey fallback: primaryKeyForQuery = %q, want id", pk)
	}

	if pk := primaryKeyForQuery(DbOptions{}, q); pk != "" {
		t.Errorf("nothing configured: primaryKeyForQuery = %q, want empty", pk)
	}
}

// TestTransaction_ExecuteQueryToRecordsReader proves the dal.QueryExecutor
// entrypoint a policy wrapper calls (e.g. dal-go/dalgo's
// access.SecureReadSession.ExecuteQueryToRecordsReader) resolves through a
// transaction the same way tx.Select already does — structured_sql_test.go's
// other tests exercise the standalone reader constructors directly, but this
// is the one the layered-ACL chain actually calls in production.
func TestTransaction_ExecuteQueryToRecordsReader(t *testing.T) {
	_, mock, db, closer, err := newDatabase(t)
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT \\* FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("Ann"))
	mock.ExpectCommit()

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectKeysOnly(reflect.String)
	err = db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			return err
		}
		rec, err := reader.Next()
		if err != nil {
			return err
		}
		data, ok := rec.Data().(map[string]any)
		if !ok || data["name"] != "Ann" {
			t.Errorf("record data = %#v, want name=Ann", rec.Data())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunReadonlyTransaction: %v", err)
	}
}

func TestSelectsIdentityField(t *testing.T) {
	cases := []struct {
		name    string
		columns []dal.Column
		field   string
		want    bool
	}{
		{"matches bare field", []dal.Column{{Expression: dal.Field("id")}}, "id", true},
		{"matches field aliased to itself", []dal.Column{{Expression: dal.Field("id"), Alias: "id"}}, "id", true},
		{"field present but aliased away", []dal.Column{{Expression: dal.Field("id"), Alias: "other"}}, "id", false},
		{"different field name", []dal.Column{{Expression: dal.Field("name")}}, "id", false},
		{"non-field expression", []dal.Column{{Expression: dal.Constant{Value: 1}}}, "id", false},
	}
	for _, tc := range cases {
		if got := selectsIdentityField(tc.columns, tc.field); got != tc.want {
			t.Errorf("%s: selectsIdentityField() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestUnusedHelperColumn(t *testing.T) {
	if got := unusedHelperColumn(nil); got != recordIDHelperColumn {
		t.Errorf("no columns: unusedHelperColumn = %q, want %q", got, recordIDHelperColumn)
	}

	// A query that already (however unusually) selects the helper column
	// name itself must fall back to a suffixed alternative rather than
	// silently colliding with real projected data.
	collision := []dal.Column{{Expression: dal.Field(recordIDHelperColumn)}}
	if got := unusedHelperColumn(collision); got == recordIDHelperColumn {
		t.Errorf("collision: unusedHelperColumn = %q, want a suffixed alternative", got)
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
