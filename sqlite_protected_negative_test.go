package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

type safeSQLValuer struct{}

func (safeSQLValuer) Value() (driver.Value, error) { return "safe", nil }

type unsupportedSQLExpression struct{}

func (unsupportedSQLExpression) String() string { return "unsupported" }

type unsupportedSQLCondition struct{}

func (unsupportedSQLCondition) String() string { return "unsupported" }

func TestSQLiteCompilerRejectsNonPortablePredicateValues(t *testing.T) {
	valid := []any{nil, safeSQLValuer{}, time.Unix(0, 0), true, "text", int8(1), uint64(1), float32(1), []byte("blob")}
	for _, value := range valid {
		if err := validateSQLValue(value); err != nil {
			t.Fatalf("portable value %T rejected: %v", value, err)
		}
	}
	if err := validateSQLValue([]string{"not", "a", "scalar"}); err == nil {
		t.Fatal("non-byte slice accepted as a SQL scalar")
	}
	if _, _, err := compileSQLExpression(dal.NewFieldRef("other", "tenant")); err == nil {
		t.Fatal("qualified field accepted by single-source compiler")
	}
	if _, _, err := compileSQLExpression(unsupportedSQLExpression{}); err == nil {
		t.Fatal("unknown expression accepted")
	}
	if _, err := arrayValues("not-an-array"); err == nil {
		t.Fatal("IN accepted a non-array value")
	}
	if _, err := arrayValues([]any{make(chan int)}); err == nil {
		t.Fatal("IN accepted an unsupported element")
	}
	if _, err := normalizedNumber(math.NaN()); err == nil {
		t.Fatal("non-JSON number accepted")
	}

	tests := []struct {
		name      string
		condition dal.Condition
		wantErr   bool
	}{
		{name: "constant left", condition: dal.NewComparison(dal.Constant{Value: "tenant"}, dal.Equal, dal.Constant{Value: "A"}), wantErr: true},
		{name: "IN requires array", condition: dal.NewComparison(dal.Field("tenant"), dal.In, dal.Constant{Value: "A"}), wantErr: true},
		{name: "IN empty denies", condition: dal.NewComparison(dal.Field("tenant"), dal.In, dal.Array{Value: []any{}})},
		{name: "IN bool rejected", condition: dal.NewComparison(dal.Field("tenant"), dal.In, dal.Array{Value: []any{true}}), wantErr: true},
		{name: "unsupported comparison", condition: dal.NewComparison(dal.Field("tenant"), dal.Operator("LIKE"), dal.Constant{Value: "A"}), wantErr: true},
		{name: "bool rejected", condition: dal.NewComparison(dal.Field("tenant"), dal.Equal, dal.Constant{Value: true}), wantErr: true},
		{name: "null equality", condition: dal.NewComparison(dal.Field("tenant"), dal.Equal, dal.Constant{Value: nil})},
		{name: "null ordering denies", condition: dal.NewComparison(dal.Field("tenant"), dal.GreaterThen, dal.Constant{Value: nil})},
		{name: "field ordering", condition: dal.NewComparison(dal.Field("left"), dal.GreaterThen, dal.Field("right"))},
		{name: "bad group operator", condition: dal.NewGroupCondition(dal.Operator("X"), dal.WhereField("tenant", dal.Equal, "A")), wantErr: true},
		{name: "empty group", condition: dal.NewGroupCondition(dal.And), wantErr: true},
		{name: "bad child", condition: dal.NewGroupCondition(dal.And, unsupportedSQLCondition{}), wantErr: true},
		{name: "unknown condition", condition: unsupportedSQLCondition{}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := compileSQLCondition(tc.condition)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	if got := fmt.Sprint(quoteSQLIdentifier("a`b")); got != "`a``b`" {
		t.Fatalf("identifier quoting = %q", got)
	}
}

func TestTransactionStructuredReaderUsesSafeCompilerAndIdentityFallback(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT `name`, `id` AS `__dalgo_record_id` FROM `users`").
		WillReturnRows(sqlmock.NewRows([]string{"name", recordIDHelperColumn}).AddRow("Ada", "u1"))
	mock.ExpectCommit()
	db := NewDatabase(sqlDB, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"id"}})
	query := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().SelectColumns(dal.Column{Expression: dal.Field("name")})
	if err = db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, query)
		if err != nil {
			return err
		}
		defer reader.Close()
		row, err := reader.Next()
		if err != nil {
			return err
		}
		if row.Key().ID != "u1" {
			t.Fatalf("projected row key = %v", row.Key())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := primaryKeyForQuery(DbOptions{}, dal.NewTextQuery("SELECT 1", nil)); got != "" {
		t.Fatalf("text query primary key = %q", got)
	}
	if got := primaryKeyForQuery(DbOptions{}, query); got != "" {
		t.Fatalf("unconfigured structured query primary key = %q", got)
	}
}

func TestNormalizeSQLiteScalarStrictPortableValues(t *testing.T) {
	tests := []struct {
		name    string
		column  sqliteColumn
		value   any
		want    any
		wantErr bool
	}{
		{name: "optional null", column: sqliteColumn{kind: "TEXT"}, want: nil},
		{name: "required null", column: sqliteColumn{kind: "TEXT", required: true}, wantErr: true},
		{name: "text", column: sqliteColumn{kind: "TEXT"}, value: "safe", want: "safe"},
		{name: "text rejects bytes", column: sqliteColumn{kind: "TEXT"}, value: []byte("ambiguous"), wantErr: true},
		{name: "integer", column: sqliteColumn{kind: "INTEGER"}, value: int(7), want: int64(7)},
		{name: "integral real", column: sqliteColumn{kind: "INT"}, value: float64(7), want: int64(7)},
		{name: "fractional integer", column: sqliteColumn{kind: "BIGINT"}, value: 7.5, wantErr: true},
		{name: "unsafe integer real", column: sqliteColumn{kind: "INTEGER"}, value: float64(1<<54 + 1), wantErr: true},
		{name: "real from integer", column: sqliteColumn{kind: "REAL"}, value: int64(7), want: float64(7)},
		{name: "real rejects unsafe integer", column: sqliteColumn{kind: "DOUBLE"}, value: int64(1<<54 + 1), wantErr: true},
		{name: "real rejects nan", column: sqliteColumn{kind: "FLOAT"}, value: math.NaN(), wantErr: true},
		{name: "real rejects infinity", column: sqliteColumn{kind: "REAL"}, value: math.Inf(1), wantErr: true},
		{name: "bool", column: sqliteColumn{kind: "BOOLEAN"}, value: true, want: true},
		{name: "bool integer", column: sqliteColumn{kind: "BOOL"}, value: int64(1), want: true},
		{name: "bool rejects other integer", column: sqliteColumn{kind: "BOOL"}, value: int64(2), wantErr: true},
		{name: "unknown affinity", column: sqliteColumn{kind: "BLOB"}, value: "value", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeSQLiteScalar(tc.column, tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalizeSQLiteScalar(%#v) accepted", tc.value)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %#v, %v; want %#v", got, err, tc.want)
			}
		})
	}
}

func TestSQLiteProtectedPreparationRejectsAmbiguousSchemas(t *testing.T) {
	tests := []struct {
		name   string
		create string
		pk     []dal.FieldRef
	}{
		{name: "default", create: "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT DEFAULT 'implicit')", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "non text primary key", create: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "composite primary key", create: "CREATE TABLE items (id TEXT, part TEXT, PRIMARY KEY(id, part))", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "generated column", create: "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, folded TEXT GENERATED ALWAYS AS (lower(name)))", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "foreign key", create: "CREATE TABLE parents (id TEXT PRIMARY KEY); CREATE TABLE items (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents(id))", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "missing database primary key", create: "CREATE TABLE items (id TEXT, name TEXT)", pk: []dal.FieldRef{dal.Field("id")}},
		{name: "configured composite key", create: "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)", pk: []dal.FieldRef{dal.Field("id"), dal.Field("name")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if _, err = raw.Exec(tc.create); err != nil {
				t.Fatal(err)
			}
			conn, err := raw.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			storage := &sqliteProtectedStorage{db: raw, options: DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, tc.pk)}}}
			session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
			op, err := access.NewProtectedInsert("insert", record.NewKeyWithID("items", "1"), map[string]any{"name": "value"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = session.prepare(context.Background(), op); err == nil {
				t.Fatal("ambiguous schema was accepted for protected execution")
			}
		})
	}
}

func TestSQLiteProtectedPreparationRejectsNonCanonicalMutations(t *testing.T) {
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL); INSERT INTO items VALUES ('1','before')"); err != nil {
		t.Fatal(err)
	}
	conn, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	storage := &sqliteProtectedStorage{db: raw, options: DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")})}}}
	session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
	key := record.NewKeyWithID("items", "1")
	mismatchedID, _ := access.NewProtectedInsert("mismatch", key, map[string]any{"id": "2", "name": "value"})
	unknownField, _ := access.NewProtectedInsert("unknown", key, map[string]any{"name": "value", "extra": "hidden"})
	missingRequired, _ := access.NewProtectedInsert("required", key, map[string]any{})
	nestedUpdate, _ := access.NewProtectedUpdate("nested", key, []update.Update{update.ByFieldPath(update.FieldPath{"profile", "name"}, "value")}, "")
	deleteUpdate, _ := access.NewProtectedUpdate("delete", key, []update.Update{update.ByFieldName("name", update.DeleteField)}, "")
	for _, op := range []access.ProtectedOperation{mismatchedID, unknownField, missingRequired, nestedUpdate, deleteUpdate} {
		if _, err = session.prepare(context.Background(), op); err == nil {
			t.Fatalf("non-canonical mutation %q was accepted", op.ID())
		}
	}
	parented, _ := access.NewProtectedInsert("parented", record.NewKeyWithParentAndID(record.NewKeyWithID("accounts", "a"), "items", "1"), map[string]any{"name": "value"})
	unknownCollection, _ := access.NewProtectedInsert("unknown-collection", record.NewKeyWithID("unknown", "1"), map[string]any{"name": "value"})
	for _, op := range []access.ProtectedOperation{parented, unknownCollection} {
		if _, err = session.prepare(context.Background(), op); err == nil {
			t.Fatalf("unsupported target %q was accepted", op.ID())
		}
	}
	if _, err = session.prepare(context.Background(), access.ProtectedOperation{}); err == nil {
		t.Fatal("operation without a key was accepted")
	}
}

func TestSQLiteProtectedExecuteRequiresPreparedWritableSession(t *testing.T) {
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	conn, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	op, _ := access.NewProtectedInsert("insert", record.NewKeyWithID("items", "1"), map[string]any{"name": "value"})
	storage := &sqliteProtectedStorage{db: raw, options: DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")})}}}
	for _, session := range []*sqliteProtectedSession{
		{conn: conn, storage: storage, alive: true, write: false, ops: []access.ProtectedOperation{op}},
		{conn: conn, storage: storage, alive: true, write: true, ops: []access.ProtectedOperation{op}},
		{conn: conn, storage: storage, alive: true, write: true, executed: true, ops: []access.ProtectedOperation{op}, prepared: []sqlitePrepared{{operation: op}}},
	} {
		if err = session.execute(context.Background()); err == nil {
			t.Fatal("invalid protected execution state was accepted")
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	prepared := sqlitePrepared{operation: op, primaryKey: "id", evidence: access.ProtectedEvidence{CandidateImage: map[string]any{"id": "1", "name": "value"}}}
	session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true, write: true, ops: []access.ProtectedOperation{op}, prepared: []sqlitePrepared{prepared}}
	if err = session.execute(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled execution: %v", err)
	}
	read, _ := access.NewProtectedRead("read", access.Get, record.NewKeyWithID("items", "1"))
	readSession := &sqliteProtectedSession{conn: conn, storage: storage, alive: true, write: true, ops: []access.ProtectedOperation{read}, prepared: []sqlitePrepared{{operation: read, primaryKey: "id"}}}
	if err = readSession.execute(context.Background()); err == nil {
		t.Fatal("read operation entered protected mutation execution")
	}
}

func TestSQLiteProtectedMutationsAndUnexpectedCountsRollback(t *testing.T) {
	policy := access.MustPolicy("owner", access.Scope("customers", access.AnyID,
		access.Allow(access.Insert|access.Set|access.Delete, "mutate")))
	raw, _, coordinator := sqliteProtectedFixture(t, policy)
	ctx := context.Background()
	execute := func(op access.ProtectedOperation) error {
		return coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(session access.ExecutionSession) error {
			_, err := session.Execute(ctx)
			return err
		})
	}
	insert, _ := access.NewProtectedInsert("insert", record.NewKeyWithID("customers", "c"), map[string]any{"name": "New", "tenant": "C", "secret": "private"})
	if err := execute(insert); err != nil {
		t.Fatal(err)
	}
	set, _ := access.NewProtectedSet("set", record.NewKeyWithID("customers", "c"), map[string]any{"name": "Replaced", "tenant": "C", "secret": "private"}, "")
	if err := execute(set); err != nil {
		t.Fatal(err)
	}
	assertSQLiteName(t, raw, "c", "Replaced")
	deleteOp, _ := access.NewProtectedDelete("delete", record.NewKeyWithID("customers", "c"), "")
	if err := execute(deleteOp); err != nil {
		t.Fatal(err)
	}
	missing, _ := access.NewProtectedDelete("missing", record.NewKeyWithID("customers", "missing"), "")
	if err := execute(missing); err == nil {
		t.Fatal("delete of a missing row reported successful execution")
	}
	var count int
	if err := raw.QueryRow("SELECT count(*) FROM customers WHERE id='c'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("delete rollback state: count=%d err=%v", count, err)
	}
}

func TestSQLiteProtectedSessionCannotEscapeCallback(t *testing.T) {
	policy := access.MustPolicy("owner", access.Scope("customers", access.AnyID, access.Allow(access.Get, "read").Fields("name")))
	_, _, coordinator := sqliteProtectedFixture(t, policy)
	ctx := context.Background()
	op, _ := access.NewProtectedEvidenceRead("read", access.Get, record.NewKeyWithID("customers", "a"), [][]string{{"name"}})
	var escaped access.InspectionSession
	if err := coordinator.WithinInspection(ctx, []access.ProtectedOperation{op}, func(session access.InspectionSession) error {
		escaped = session
		_, err := session.Evidence(ctx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.Evidence(ctx); err == nil {
		t.Fatal("inspection session remained usable after callback")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := coordinator.WithinInspection(cancelled, []access.ProtectedOperation{op}, func(access.InspectionSession) error {
		return errors.New("callback should not run")
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled inspection: %v", err)
	}
}
