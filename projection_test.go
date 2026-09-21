package dalgo2sql

import (
	"context"
	"reflect"
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
	mock.ExpectQuery("SELECT `c`.* FROM `customers` AS `c`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email", "created_at"}).
			AddRow("c1", "Ada", "ada@example.test", "2026-09-20"))

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
	mock.ExpectQuery("SELECT `c`.* FROM `customers` AS `c`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "billing_address", "PASSWORD_HASH", "created_at"}).
			AddRow("c1", "Ada", "1 Main St", "secret", "2026-09-20"))

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
	mock.ExpectQuery("SELECT * FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email", "created_at"}).
			AddRow([]byte("c1"), []byte("Ada"), []byte("ada@example.test"), []byte("2026-09-20")))

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
	mock.ExpectQuery("SELECT * FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "BillingEmail", "password_hash", "created_at"}).
			AddRow([]byte("c1"), []byte("Ada"), []byte("ada@example.test"), []byte("secret"), []byte("2026-09-20")))

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

func TestWildcardExclusionIdentityHelperDoesNotHideSameNamedSourceColumn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)

	q := dal.From(dal.NewRootCollectionRef("customers", "")).NewQuery().
		SelectColumns(dal.AllColumnsExcept("id"))
	mock.ExpectQuery("SELECT *, `id` AS `__dalgo_record_id` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "__dalgo_record_id", "__dalgo_record_id"}).
			AddRow("c1", "source-value", "c1"))

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
	mock.ExpectQuery("SELECT *, `id` AS `__dalgo_record_id` FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"id", "__dalgo_record_id", "__dalgo_record_id"}).
			AddRow("c1", "source-value", "c1"))

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
