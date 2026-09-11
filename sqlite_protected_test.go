package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/dal-go/record/update"
	_ "modernc.org/sqlite"
)

type sqliteTestLease struct{ policies []access.Policy }

func (l sqliteTestLease) Policies() []access.Policy { return l.policies }
func (sqliteTestLease) Revision() string            { return "test" }
func (sqliteTestLease) Release()                    {}

type protectedSQLDB interface {
	dal.DB
	dal.WriteSession
}

func sqliteProtectedFixture(t *testing.T, policy access.Policy) (*sql.DB, protectedSQLDB, *access.EnforcementCoordinator) {
	t.Helper()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	for _, statement := range []string{"CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, tenant TEXT, secret TEXT)", "INSERT INTO customers VALUES ('a','Before','A','hidden'),('b','Second','B','hidden')"} {
		if _, err = raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")})}})
	factory := db.(*sqliteProtectedFactory)
	secured, coordinator, err := factory.ConfigureProtectedAccess(access.MandatoryParticipant{LayerID: "owner", Provider: func(context.Context) (access.PolicyLease, error) {
		return sqliteTestLease{[]access.Policy{policy}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return raw, secured.(protectedSQLDB), coordinator
}

func TestSQLiteProtectedInspectionWriteAndRollback(t *testing.T) {
	policy := access.MustPolicy("owner", access.Scope("customers", access.AnyID, access.Allow(access.Update, "writer").Where(dal.WhereField("tenant", dal.Equal, "A")).Fields("name")))
	raw, db, coordinator := sqliteProtectedFixture(t, policy)
	ctx := context.Background()
	operation, err := access.NewProtectedUpdate("u1", record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "After")}, "")
	if err != nil {
		t.Fatal(err)
	}
	err = coordinator.WithinInspection(ctx, []access.ProtectedOperation{operation}, func(session access.InspectionSession) error {
		if _, ok := session.(access.ExecutionSession); ok {
			t.Fatal("inspection exposes execution")
		}
		assessment, err := session.Assess(ctx)
		if err != nil {
			return err
		}
		if assessment.Outcome != access.AssessmentAllow {
			t.Fatalf("inspection: %+v", assessment)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSQLiteName(t, raw, "a", "Before")
	// A write-only actor is admitted without using the public Get surface.
	if err = db.Get(ctx, record.NewRecordWithData(record.NewKeyWithID("customers", "a"), map[string]any{})); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("public read: %v", err)
	}
	if err = db.Update(ctx, record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "After")}); err != nil {
		t.Fatal(err)
	}
	assertSQLiteName(t, raw, "a", "After")
	if err = db.Update(ctx, record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("secret", "leak")}); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("hidden mutation: %v", err)
	}
	assertSQLiteName(t, raw, "a", "After")
	first, _ := access.NewProtectedUpdate("u1", record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "Partial")}, "")
	second, _ := access.NewProtectedUpdate("u2", record.NewKeyWithID("customers", "b"), []update.Update{update.ByFieldName("name", "Denied")}, "")
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{first, second}, func(session access.ExecutionSession) error { _, err := session.Execute(ctx); return err })
	if !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("batch denial: %v", err)
	}
	assertSQLiteName(t, raw, "a", "After")
	assertSQLiteName(t, raw, "b", "Second")
	sentinel := errors.New("caller abort")
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{first}, func(session access.ExecutionSession) error {
		if _, err := session.Execute(ctx); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	assertSQLiteName(t, raw, "a", "After")
	called := false
	err = db.RunReadwriteTransaction(ctx, func(context.Context, dal.ReadwriteTransaction) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("dynamic callback entered protected profile")
	}
}

func TestSQLiteProtectedRejectsTriggersAndWaitCancellation(t *testing.T) {
	policy := access.MustPolicy("owner", access.Scope("customers", access.AnyID, access.Allow(access.ReadWrite, "all")))
	raw, db, _ := sqliteProtectedFixture(t, policy)
	if _, err := raw.Exec("CREATE TRIGGER mutate AFTER UPDATE ON customers BEGIN UPDATE customers SET secret='changed' WHERE id=NEW.id; END"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(context.Background(), record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "Bad")}); err == nil {
		t.Fatal("trigger profile accepted")
	}
	assertSQLiteName(t, raw, "a", "Before")
	if _, err := raw.Exec("DROP TRIGGER mutate"); err != nil {
		t.Fatal(err)
	}
	lock, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	if _, err = lock.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = lock.ExecContext(context.Background(), "ROLLBACK") }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = db.Update(ctx, record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "Late")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock deadline: %v", err)
	}
}

func assertSQLiteName(t *testing.T, db *sql.DB, id, want string) {
	t.Helper()
	var actual string
	if err := db.QueryRow("SELECT name FROM customers WHERE id=?", id).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != want {
		t.Fatalf("row %s: got %q, want %q", id, actual, want)
	}
}

func TestSQLiteProtectedWholeImageCASAndReceipt(t *testing.T) {
	policy := access.MustPolicy("owner", access.Scope("customers", access.AnyID, access.Allow(access.Get, "read").Fields("name"), access.Allow(access.Update, "write").Fields("name")))
	raw, _, coordinator := sqliteProtectedFixture(t, policy)
	ctx := context.Background()
	key := record.NewKeyWithID("customers", "a")
	read, _ := access.NewProtectedEvidenceRead("r", access.Get, key, [][]string{{"name"}})
	revision := func() string {
		t.Helper()
		var token string
		err := coordinator.WithinInspection(ctx, []access.ProtectedOperation{read}, func(s access.InspectionSession) error {
			e, err := s.Evidence(ctx)
			if err == nil {
				token = e[0].DataRevision
			}
			return err
		})
		if err != nil || token == "" {
			t.Fatalf("evidence %v %q", err, token)
		}
		return token
	}
	before := revision()
	if _, err := raw.Exec("UPDATE customers SET secret='changed-hidden' WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	if before == revision() {
		t.Fatal("hidden field did not invalidate revision")
	}
	stale, _ := access.NewProtectedUpdate("u", key, []update.Update{update.ByFieldName("name", "After")}, before)
	err := coordinator.WithinExecution(ctx, []access.ProtectedOperation{stale}, func(s access.ExecutionSession) error { _, err := s.Execute(ctx); return err })
	if !errors.Is(err, access.ErrDataRevisionConflict) {
		t.Fatalf("stale revision %v", err)
	}
	current := revision()
	op, _ := access.NewProtectedUpdate("u", key, []update.Update{update.ByFieldName("name", "After")}, current)
	var receipt string
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(s access.ExecutionSession) error {
		if _, err := s.Execute(ctx); err != nil {
			return err
		}
		revisions, err := s.Revisions(ctx)
		receipt = revisions["u"]
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt == "" || receipt != revision() {
		t.Fatal("committed receipt does not match next full-image revision")
	}
}

func TestSQLiteProtectedFactoryActivationAndCollation(t *testing.T) {
	raw, _, _ := sqliteProtectedFixture(t, access.MustPolicy("deny", access.Scope("customers", access.AnyID, access.Deny(access.Update, "deny"))))
	factory := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")})}}).(*sqliteProtectedFactory)
	participant, _ := access.NewStaticParticipant("owner", access.MustPolicy("deny", access.Scope("customers", access.AnyID, access.Deny(access.Update, "deny"))))
	if _, _, err := factory.ConfigureProtectedAccess(participant); err != nil {
		t.Fatal(err)
	}
	invoked := false
	err := factory.RunReadwriteTransaction(context.Background(), func(context.Context, dal.ReadwriteTransaction) error { invoked = true; return nil })
	if err == nil || invoked {
		t.Fatal("retained factory bypassed protected transaction boundary")
	}
	if _, _, err = factory.ConfigureProtectedAccess(participant); err == nil {
		t.Fatal("factory could be reconfigured")
	}
	if _, err = raw.Exec("DROP TABLE customers"); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec("CREATE TABLE customers (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	another := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{"customers": NewRecordset("customers", Table, []dal.FieldRef{dal.Field("id")})}}).(*sqliteProtectedFactory)
	allow, _ := access.NewStaticParticipant("owner", access.MustPolicy("allow", access.Scope("customers", access.AnyID, access.Allow(access.Insert, "insert"))))
	_, coordinator, err := another.ConfigureProtectedAccess(allow)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := access.NewProtectedInsert("a", record.NewKeyWithID("customers", "a"), map[string]any{"name": "one"})
	b, _ := access.NewProtectedInsert("b", record.NewKeyWithID("customers", "A"), map[string]any{"name": "two"})
	invoked = false
	err = coordinator.WithinExecution(context.Background(), []access.ProtectedOperation{a, b}, func(s access.ExecutionSession) error {
		invoked = true
		_, err := s.Execute(context.Background())
		return err
	})
	if err == nil || invoked {
		t.Fatal("non-binary canonical identity accepted")
	}
	var count int
	if err = raw.QueryRow("SELECT count(*) FROM customers").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unexpected stored rows %d %v", count, err)
	}
}

func TestSQLiteProtectedMalformedRowPreparationIsNotAuthority(t *testing.T) {
	raw, _, coordinator := sqliteProtectedFixture(t, access.MustPolicy("owner", access.Scope("customers", access.AnyID, access.Allow(access.Get|access.Update, "allow").Fields("name"))))
	if _, err := raw.Exec("UPDATE customers SET secret=x'010203' WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	op, _ := access.NewProtectedUpdate("u", record.NewKeyWithID("customers", "a"), []update.Update{update.ByFieldName("name", "After")}, "")
	entered := false
	err := coordinator.WithinInspection(ctx, []access.ProtectedOperation{op}, func(s access.InspectionSession) error {
		entered = true
		assessment, err := s.Assess(ctx)
		if err != nil {
			return err
		}
		if assessment.Complete || assessment.Outcome == access.AssessmentAllow {
			t.Fatalf("malformed row treated as complete: %+v", assessment)
		}
		visible, err := s.ReadVisibility(ctx)
		if err != nil {
			return err
		}
		if visible["u"] {
			t.Fatal("malformed row leaked visibility")
		}
		return nil
	})
	if err != nil || !entered {
		t.Fatalf("row-dependent failure escaped safe inspection: %v", err)
	}
	err = coordinator.WithinExecution(ctx, []access.ProtectedOperation{op}, func(s access.ExecutionSession) error { _, err := s.Execute(ctx); return err })
	if !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("malformed row execution: %v", err)
	}
	var name string
	if err = raw.QueryRow("SELECT name FROM customers WHERE id='a'").Scan(&name); err != nil || name != "Before" {
		t.Fatalf("malformed row changed %q %v", name, err)
	}
}
