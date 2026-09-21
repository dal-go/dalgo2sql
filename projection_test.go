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

func TestCompileStructuredSQLWildcardExclusion(t *testing.T) {
	tests := []struct {
		name string
		q    dal.StructuredQuery
		want string
	}{
		{
			name: "unqualified",
			q: dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
				SelectColumns(dal.AllColumnsExcept("email", "missing")),
			want: "SELECT * FROM `customers`",
		},
		{
			name: "qualified",
			q: dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
				SelectColumns(dal.AllColumnsExceptFrom("c", "email")),
			want: "SELECT `c`.* FROM `customers` AS `c`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, args, err := compileStructuredSQL(tt.q)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || len(args) != 0 {
				t.Fatalf("compileStructuredSQL() = %q, %#v; want %q, no args", got, args, tt.want)
			}
		})
	}
}

func TestEmitSQLQualifiedWildcardExclusionUsesLegacySingleSourceWildcard(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
		SelectColumns(dal.AllColumnsExceptFrom("c", "email"))
	if got, want := emitSQL(q), "SELECT * FROM customers"; got != want {
		t.Fatalf("emitSQL() = %q, want %q", got, want)
	}
}

func TestWildcardExclusionRecordsReader(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
		SelectColumns(dal.AllColumnsExceptFrom("c", "email", "password_hash", "email"))
	expectSQLiteSourceColumns(mock, "customers", "id", "name", "email", "created_at")
	mock.ExpectQuery("SELECT `id`, `name`, `created_at` FROM `customers` AS `c`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "created_at"}).
			AddRow("c1", "Ada", "2026-09-20"))

	r, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	record, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"id": "c1", "name": "Ada", "created_at": "2026-09-20"}
	if got := record.Data().(map[string]any); !reflect.DeepEqual(got, want) {
		t.Fatalf("record data = %#v, want %#v", got, want)
	}
	if record.Key().ID != "c1" {
		t.Fatalf("record key ID = %#v, want c1", record.Key().ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWildcardExclusionMasksRecordsReader(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
		SelectColumns(dal.AllColumnsExceptFrom("c", "Billing*", "Password*", "missing*"))
	expectSQLiteSourceColumns(mock, "customers", "id", "name", "billing_address", "PASSWORD_HASH", "created_at")
	mock.ExpectQuery("SELECT `id`, `name`, `created_at` FROM `customers` AS `c`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "created_at"}).
			AddRow("c1", "Ada", "2026-09-20"))

	r, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	record, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"id": "c1", "name": "Ada", "created_at": "2026-09-20"}
	if got := record.Data().(map[string]any); !reflect.DeepEqual(got, want) {
		t.Fatalf("record data = %#v, want %#v", got, want)
	}
	if record.Key().ID != "c1" {
		t.Fatalf("record key ID = %#v, want c1", record.Key().ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWildcardExclusionRecordsetReaderPreservesOrder(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("email", "missing"))
	expectSQLiteSourceColumns(mock, "customers", "id", "name", "email", "created_at")
	mock.ExpectQuery("SELECT `id`, `name`, `created_at` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "created_at"}).
			AddRow([]byte("c1"), []byte("Ada"), []byte("2026-09-20")))

	r, err := getRecordsetReaderWithDialect(context.Background(), q, db.QueryContext, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got := r.Recordset().Columns(); len(got) != 3 || got[0].Name() != "id" || got[1].Name() != "name" || got[2].Name() != "created_at" {
		t.Fatalf("recordset columns = %#v", got)
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"c1", "Ada", "2026-09-20"} {
		value, valueErr := row.GetValueByIndex(i, rs)
		if valueErr != nil {
			t.Fatal(valueErr)
		}
		if stringValue := valueString(value); stringValue != want {
			t.Fatalf("value %d = %q, want %q", i, stringValue, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWildcardExclusionMasksRecordsetReaderPreservesOrder(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("Billing*", "Password*", "missing*"))
	expectSQLiteSourceColumns(mock, "customers", "id", "name", "BillingEmail", "password_hash", "created_at")
	mock.ExpectQuery("SELECT `id`, `name`, `created_at` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "created_at"}).
			AddRow([]byte("c1"), []byte("Ada"), []byte("2026-09-20")))

	r, err := getRecordsetReaderWithDialect(context.Background(), q, db.QueryContext, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got := r.Recordset().Columns(); len(got) != 3 || got[0].Name() != "id" || got[1].Name() != "name" || got[2].Name() != "created_at" {
		t.Fatalf("recordset columns = %#v", got)
	}
	row, rs, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"c1", "Ada", "2026-09-20"} {
		value, valueErr := row.GetValueByIndex(i, rs)
		if valueErr != nil {
			t.Fatal(valueErr)
		}
		if stringValue := valueString(value); stringValue != want {
			t.Fatalf("value %d = %q, want %q", i, stringValue, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func valueString(value any) string {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return value.(string)
}

func expectSQLiteSourceColumns(mock sqlmock.Sqlmock, table string, names ...string) {
	rows := sqlmock.NewRows([]string{"name", "hidden"})
	for _, name := range names {
		rows.AddRow(name, 0)
	}
	mock.ExpectQuery("SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid").
		WithArgs(table).WillReturnRows(rows)
}

func TestWildcardProjectionValidation(t *testing.T) {
	base := dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery()
	tests := []struct {
		name    string
		columns []dal.Column
	}{
		{name: "empty exclusions", columns: []dal.Column{dal.AllColumnsExcept()}},
		{name: "empty name", columns: []dal.Column{dal.AllColumnsExcept("")}},
		{name: "unknown source", columns: []dal.Column{dal.AllColumnsExceptFrom("x", "email")}},
		{name: "alias", columns: []dal.Column{{Alias: "rest", Wildcard: &dal.WildcardProjection{Exclude: []string{"email"}}}}},
		{name: "expression", columns: []dal.Column{{Expression: dal.Field("id"), Wildcard: &dal.WildcardProjection{Exclude: []string{"email"}}}}},
		{name: "multiple", columns: []dal.Column{dal.AllColumnsExcept("email"), dal.AllColumnsExcept("secret")}},
		{name: "not first", columns: []dal.Column{{Expression: dal.Field("id")}, dal.AllColumnsExcept("email")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := compileStructuredSQL(base.SelectColumns(tt.columns...)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestWildcardProjectionRejectsJoinedSources(t *testing.T) {
	from := dal.From(dal.NewRootCollectionRef("customers", "c")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("orders", "o"), dal.JoinInner),
	)
	q := from.NewQuery().SelectColumns(dal.AllColumnsExceptFrom("c", "email"))
	if _, err := planWildcardProjection(q); err == nil {
		t.Fatal("expected joined wildcard projection to be rejected")
	}
}

func TestWildcardProjectionKeepsExplicitTail(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(
		dal.AllColumnsExcept("email"),
		dal.Column{Expression: dal.Field("id"), Alias: "helper"},
	)
	plan, err := planWildcardProjection(q)
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.visibleIndexes([]string{"id", "email", "helper"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("visible indexes = %#v, want [0 2]", got)
	}
}

func TestWildcardProjectionMaskKeepsExplicitTail(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(
		dal.AllColumnsExcept("Billing*"),
		dal.Column{Expression: dal.Field("id"), Alias: "BillingHelper"},
	)
	plan, err := planWildcardProjection(q)
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.visibleIndexes([]string{"id", "BILLING_EMAIL", "BillingHelper"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("visible indexes = %#v, want [0 2]", got)
	}
}

func TestSQLiteWildcardMaskPushdownRealDatabase(t *testing.T) {
	db := openTestSQLiteDB(t, "CREATE TABLE customers (id TEXT, name TEXT, BillingSecret BLOB, PasswordHash TEXT)")
	if _, err := db.Exec("INSERT INTO customers VALUES (?, ?, ?, ?)", "c1", "Ada", []byte("large secret"), "hash"); err != nil {
		t.Fatal(err)
	}
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(
		dal.AllColumnsExcept("Billing*", "Password*"),
		dal.Column{Expression: dal.Field("name"), Alias: "BillingDisplay"},
	)
	queries := make([]string, 0, 2)
	execute := func(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
		queries = append(queries, text)
		return db.QueryContext(ctx, text, args...)
	}
	r, err := getRecordsetReaderWithDialect(context.Background(), q, execute, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if len(queries) != 2 || queries[0] != "SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid" ||
		queries[1] != "SELECT `id`, `name`, `name` AS `BillingDisplay` FROM `customers`" {
		t.Fatalf("queries = %#v", queries)
	}
	if strings.Contains(queries[1], "BillingSecret") || strings.Contains(queries[1], "PasswordHash") {
		t.Fatalf("excluded columns remain in data query: %s", queries[1])
	}
	if got := r.Recordset().Columns(); len(got) != 3 || got[2].Name() != "BillingDisplay" {
		t.Fatalf("visible columns = %#v", got)
	}
	if _, _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWildcardPushdownUsesSchemaNamesWithFullColumnLabels(t *testing.T) {
	db := openTestSQLiteDB(t, "CREATE TABLE customers (id TEXT, BillingSecret TEXT, computed TEXT GENERATED ALWAYS AS (id || '-computed') VIRTUAL)")
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA short_column_names=OFF; PRAGMA full_column_names=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO customers (id, BillingSecret) VALUES ('c1', 'secret')"); err != nil {
		t.Fatal(err)
	}
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("Billing*"))
	var dataQuery string
	execute := func(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
		if !strings.Contains(text, "pragma_table_xinfo") {
			dataQuery = text
		}
		return db.QueryContext(ctx, text, args...)
	}
	r, err := getReaderBaseWithDialect(context.Background(), q, execute, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.rows.Close() }()
	if dataQuery != "SELECT `id`, `computed` FROM `customers`" {
		t.Fatalf("data query = %q", dataQuery)
	}
	if !r.rows.Next() {
		t.Fatal("expected one row")
	}
	values, err := r.scanValues()
	if err != nil || len(values) != 2 || valueString(values[0]) != "c1" || valueString(values[1]) != "c1-computed" {
		t.Fatalf("values = %#v, err = %v", values, err)
	}
}

func TestSQLiteSourceColumnsOmitOnlyVirtualTableHiddenColumns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("Billing*"))
	mock.ExpectQuery("SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid").WithArgs("customers").
		WillReturnRows(sqlmock.NewRows([]string{"name", "hidden"}).
			AddRow("id", 0).AddRow("internal", 1).AddRow("virtual_generated", 2).AddRow("stored_generated", 3))
	names, err := sqliteSourceColumns(context.Background(), q, db.QueryContext)
	if err != nil || !reflect.DeepEqual(names, []string{"id", "virtual_generated", "stored_generated"}) {
		t.Fatalf("source columns = %#v, err = %v", names, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWildcardAllExcludedRetainsEmptyResultFallback(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("*"))
	expectSQLiteSourceColumns(mock, "customers", "id", "secret")
	mock.ExpectQuery("SELECT * FROM `customers`").WillReturnRows(sqlmock.NewRows([]string{"id", "secret"}).AddRow("c1", "value"))
	r, err := getReaderBaseWithDialect(context.Background(), q, db.QueryContext, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.rows.Close() }()
	if len(r.colNames) != 0 || !r.rows.Next() {
		t.Fatalf("expected one row with zero visible columns, got %#v", r.colNames)
	}
	values, err := r.scanValues()
	if err != nil || len(values) != 0 {
		t.Fatalf("values = %#v, err = %v", values, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWildcardDuplicateMetadataNamesRetainResultFiltering(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("secret"))
	plan, err := planWildcardProjection(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plan.expandSQLiteWildcard(q, []string{"id", "ID", "secret"}); ok {
		t.Fatal("ambiguous source column names must not be expanded")
	}
}

func TestWildcardExclusionIdentityHelperDoesNotHideSameNamedSourceColumn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("id"))
	expectSQLiteSourceColumns(mock, "customers", "id", "__dalgo_record_id")
	mock.ExpectQuery("SELECT `__dalgo_record_id`, `id` AS `__dalgo_record_id` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"__dalgo_record_id", "__dalgo_record_id"}).
			AddRow("source-value", "c1"))

	r, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	record, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if record.Key().ID != "c1" {
		t.Fatalf("record key ID = %#v, want c1", record.Key().ID)
	}
	want := map[string]any{"__dalgo_record_id": "source-value"}
	if got := record.Data().(map[string]any); !reflect.DeepEqual(got, want) {
		t.Fatalf("record data = %#v, want %#v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWildcardExclusionMaskIdentityHelperDoesNotHideSameNamedSourceColumn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("ID*"))
	expectSQLiteSourceColumns(mock, "customers", "id", "__dalgo_record_id")
	mock.ExpectQuery("SELECT `__dalgo_record_id`, `id` AS `__dalgo_record_id` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"__dalgo_record_id", "__dalgo_record_id"}).
			AddRow("source-value", "c1"))

	r, err := getRecordsReaderWithOptions(context.Background(), q, db.QueryContext, DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	record, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if record.Key().ID != "c1" {
		t.Fatalf("record key ID = %#v, want c1", record.Key().ID)
	}
	want := map[string]any{"__dalgo_record_id": "source-value"}
	if got := record.Data().(map[string]any); !reflect.DeepEqual(got, want) {
		t.Fatalf("record data = %#v, want %#v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
