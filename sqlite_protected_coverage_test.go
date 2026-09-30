package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type dummyLease struct {
	policies []access.Policy
}

func (d dummyLease) Policies() []access.Policy { return d.policies }
func (d dummyLease) Revision() string          { return "rev" }
func (d dummyLease) Release()                  {}

func TestSQLiteProtected_ConfigureProtectedAccess(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, raw)

	db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets: map[string]*Recordset{
			"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")}),
		},
	})
	factory := db.(*sqliteProtectedFactory)

	t.Run("coordinator_error_duplicate_layer", func(t *testing.T) {
		p1 := access.MandatoryParticipant{LayerID: "dup"}
		p2 := access.MandatoryParticipant{LayerID: "dup"}
		_, _, err := factory.ConfigureProtectedAccess(p1, p2)
		if err == nil {
			t.Fatal("expected error on duplicate layer ID")
		}
	})

	t.Run("provider_branches", func(t *testing.T) {
		// Test provider closure created inside ConfigureProtectedAccess:
		// 1. Participant with nil Provider (skipped)
		// 2. Participant with error
		// 3. Participant with nil lease
		// 4. Participant with empty policies
		pNilProvider := access.MandatoryParticipant{
			LayerID: "nil-prov",
			Validator: func(ctx context.Context, op access.ProtectedOperation, img map[string]any) error {
				return nil
			},
		}
		pErrProvider := access.MandatoryParticipant{
			LayerID: "err-prov",
			Provider: func(ctx context.Context) (access.PolicyLease, error) {
				return nil, errors.New("provider failed")
			},
		}
		pNilLease := access.MandatoryParticipant{
			LayerID: "nil-lease",
			Provider: func(ctx context.Context) (access.PolicyLease, error) {
				return nil, nil
			},
		}
		pEmptyPolicies := access.MandatoryParticipant{
			LayerID: "empty-lease",
			Provider: func(ctx context.Context) (access.PolicyLease, error) {
				return dummyLease{policies: nil}, nil
			},
		}

		// Configure factory with these participants
		f := &sqliteProtectedFactory{DB: db, storage: factory.storage}
		securedDB, _, err := f.ConfigureProtectedAccess(pNilProvider, pErrProvider)
		if err != nil {
			t.Fatal(err)
		}
		// Trigger provider call via securedDB
		rec := record.NewRecordWithData(record.NewKeyWithID("items", "1"), &struct{ Name string }{})
		err = securedDB.Get(ctx, rec)
		if err == nil {
			t.Fatal("expected error when provider fails")
		}

		// Nil lease
		f2 := &sqliteProtectedFactory{DB: db, storage: factory.storage}
		securedDB2, _, err := f2.ConfigureProtectedAccess(pNilLease)
		if err != nil {
			t.Fatal(err)
		}
		err = securedDB2.Get(ctx, rec)
		if err == nil {
			t.Fatal("expected error for nil lease")
		}

		// Empty policies
		f3 := &sqliteProtectedFactory{DB: db, storage: factory.storage}
		securedDB3, _, err := f3.ConfigureProtectedAccess(pEmptyPolicies)
		if err != nil {
			t.Fatal(err)
		}
		err = securedDB3.Get(ctx, rec)
		if err == nil {
			t.Fatal("expected error for empty policies")
		}
	})
}

func TestSQLiteProtected_Within_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("db_closed_conn_error", func(t *testing.T) {
		raw, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		storage := &sqliteProtectedStorage{db: raw}
		_ = raw.Close()
		err = storage.WithinProtectedInspection(ctx, nil, func(access.ProtectedInspectionStorage) error { return nil })
		if err == nil {
			t.Fatal("expected error on closed db")
		}
	})

	t.Run("begin_generic_error", func(t *testing.T) {
		sdb, smock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, sdb)

		smock.ExpectExec("BEGIN").WillReturnError(errors.New("generic begin error"))
		storage := &sqliteProtectedStorage{db: sdb}
		err = storage.WithinProtectedInspection(ctx, nil, func(access.ProtectedInspectionStorage) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "generic begin error") {
			t.Fatalf("expected generic begin error, got %v", err)
		}
	})

	t.Run("commit_context_canceled", func(t *testing.T) {
		raw, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, raw)

		ctxCancel, cancel := context.WithCancel(ctx)
		storage := &sqliteProtectedStorage{db: raw}
		err = storage.WithinProtectedExecution(ctxCancel, nil, func(s access.ProtectedExecutionStorage) error {
			// Mark session executed then cancel context
			exec := s.(sqliteExecution)
			exec.session.executed = true
			cancel()
			return nil
		})
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

func TestSQLiteProtected_Evidence_Errors(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, raw)

	if _, err := raw.Exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT);"); err != nil {
		t.Fatal(err)
	}
	conn, err := raw.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	storage := &sqliteProtectedStorage{db: raw, options: DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")})}}}

	t.Run("session_not_alive_or_canceled", func(t *testing.T) {
		sNotAlive := &sqliteProtectedSession{conn: conn, storage: storage, alive: false}
		_, err := sNotAlive.evidence(ctx)
		if err == nil || !strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("expected unavailable, got %v", err)
		}

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		sAlive := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		_, err = sAlive.evidence(canceledCtx)
		if err == nil || !strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("expected unavailable, got %v", err)
		}
	})

	t.Run("evidence_large_payload", func(t *testing.T) {
		oneMB := strings.Repeat("A", 1<<20)
		var ops []access.ProtectedOperation
		for i := 0; i < 9; i++ {
			idStr := string(rune('1' + i))
			key := record.NewKeyWithID("items", idStr)
			op, _ := access.NewProtectedInsert("big"+idStr, key, map[string]any{"id": idStr, "name": oneMB})
			ops = append(ops, op)
		}
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true, ops: ops}
		_, err := session.evidence(ctx)
		if err == nil {
			t.Fatal("expected error on >8MB evidence")
		}
	})
}

func TestSQLiteProtected_Prepare_ErrorsWithMock(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	conn, err := sdb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	storage := &sqliteProtectedStorage{db: sdb, options: DbOptions{Recordsets: map[string]*Recordset{"items": NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")})}}}
	key := record.NewKeyWithID("items", "1")
	op, _ := access.NewProtectedRead("read", access.Get, key)

	t.Run("trigger_check_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnError(errors.New("trigger query failed"))
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "trigger query failed") {
			t.Fatalf("expected trigger query failed, got %v", err)
		}
	})

	t.Run("fks_query_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnError(errors.New("fk query failed"))
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "fk query failed") {
			t.Fatalf("expected fk query failed, got %v", err)
		}
	})

	t.Run("fks_stream_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		rows := sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, errors.New("fk stream failed"))
		smock.ExpectQuery("PRAGMA foreign_key_list").WillReturnRows(rows)
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "fk stream failed") {
			t.Fatalf("expected fk stream failed, got %v", err)
		}
	})

	t.Run("incoming_fk_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnError(errors.New("incoming fk failed"))
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "incoming fk failed") {
			t.Fatalf("expected incoming fk failed, got %v", err)
		}
	})

	t.Run("table_xinfo_query_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA table_xinfo").
			WillReturnError(errors.New("xinfo query failed"))
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "xinfo query failed") {
			t.Fatalf("expected xinfo query failed, got %v", err)
		}
	})

	t.Run("table_xinfo_scan_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA table_xinfo").
			WillReturnRows(sqlmock.NewRows([]string{"cid"}).AddRow(1)) // only 1 column instead of 7
		_, err := session.prepare(ctx, op)
		if err == nil {
			t.Fatal("expected xinfo scan error")
		}
	})

	t.Run("table_xinfo_rows_err", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		rows := sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}).
			AddRow(0, "id", "TEXT", 1, nil, 1, 0).
			RowError(0, errors.New("xinfo stream failed"))
		smock.ExpectQuery("PRAGMA table_xinfo").WillReturnRows(rows)
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "xinfo stream failed") {
			t.Fatalf("expected xinfo stream failed, got %v", err)
		}
	})

	t.Run("dataRows_query_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA table_xinfo").
			WillReturnRows(sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}).
				AddRow(0, "id", "TEXT", 1, nil, 1, 0))
		smock.ExpectQuery("SELECT \\* FROM `items` WHERE `id` = \\?").
			WillReturnError(errors.New("data query failed"))
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "data query failed") {
			t.Fatalf("expected data query failed, got %v", err)
		}
	})

	t.Run("dataRows_scan_error", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA table_xinfo").
			WillReturnRows(sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}).
				AddRow(0, "id", "TEXT", 1, nil, 1, 0))
		// Return 2 columns when only 1 is expected
		smock.ExpectQuery("SELECT \\* FROM `items` WHERE `id` = \\?").
			WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow("1", "extra_val"))
		_, err := session.prepare(ctx, op)
		if err == nil {
			t.Fatal("expected data scan error")
		}
	})

	t.Run("dataRows_rows_err", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		smock.ExpectQuery("SELECT sql FROM sqlite_schema WHERE type='table'").
			WillReturnRows(sqlmock.NewRows([]string{"sql"}).AddRow("CREATE TABLE items (id TEXT PRIMARY KEY);"))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema WHERE type='trigger'").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA foreign_key_list").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		smock.ExpectQuery("SELECT count\\(\\*\\) FROM sqlite_schema AS schema").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		smock.ExpectQuery("PRAGMA table_xinfo").
			WillReturnRows(sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}).
				AddRow(0, "id", "TEXT", 1, nil, 1, 0))
		rows := sqlmock.NewRows([]string{"id"}).
			AddRow("1").
			RowError(0, errors.New("data stream failed"))
		smock.ExpectQuery("SELECT \\* FROM `items` WHERE `id` = \\?").
			WillReturnRows(rows)
		_, err := session.prepare(ctx, op)
		if err == nil || !strings.Contains(err.Error(), "data stream failed") {
			t.Fatalf("expected data stream failed, got %v", err)
		}
	})
}

func TestSQLiteProtected_Scalars_Coverage(t *testing.T) {
	// Lines 419, 429, 436:
	// INTEGER with int64
	v, err := normalizeSQLiteScalar(sqliteColumn{name: "i", kind: "INTEGER"}, int64(123))
	if err != nil || v != int64(123) {
		t.Fatalf("unexpected int64: %v, %v", v, err)
	}

	// REAL with float64
	v, err = normalizeSQLiteScalar(sqliteColumn{name: "f", kind: "REAL"}, float64(1.23))
	if err != nil || v != float64(1.23) {
		t.Fatalf("unexpected float64: %v, %v", v, err)
	}

	// REAL with int
	v, err = normalizeSQLiteScalar(sqliteColumn{name: "f", kind: "REAL"}, int(123))
	if err != nil || v != float64(123) {
		t.Fatalf("unexpected int->float64: %v, %v", v, err)
	}
}

func TestSQLiteProtected_Execute_Coverage(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	conn, err := sdb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	storage := &sqliteProtectedStorage{db: sdb}
	key := record.NewKeyWithID("items", "1")

	t.Run("update_empty_sets", func(t *testing.T) {
		// Update candidate image has only the primary key column (id), so sets is empty
		op, _ := access.NewProtectedUpdate("up", key, nil, "")
		prepared := sqlitePrepared{
			operation:  op,
			primaryKey: "id",
			evidence: access.ProtectedEvidence{
				CandidateImage: map[string]any{"id": "1"},
				Exists:         true,
			},
		}
		session := &sqliteProtectedSession{
			conn:     conn,
			storage:  storage,
			alive:    true,
			write:    true,
			ops:      []access.ProtectedOperation{op},
			prepared: []sqlitePrepared{prepared},
		}
		err := session.execute(ctx)
		if err == nil {
			t.Fatal("expected error on empty sets")
		}
	})

	t.Run("exec_context_error", func(t *testing.T) {
		op, _ := access.NewProtectedDelete("del", key, "")
		prepared := sqlitePrepared{
			operation:  op,
			primaryKey: "id",
		}
		session := &sqliteProtectedSession{
			conn:     conn,
			storage:  storage,
			alive:    true,
			write:    true,
			ops:      []access.ProtectedOperation{op},
			prepared: []sqlitePrepared{prepared},
		}
		smock.ExpectExec("DELETE FROM `items` WHERE `id` = \\?").
			WithArgs("1").
			WillReturnError(errors.New("delete failed"))
		err := session.execute(ctx)
		if err == nil || !strings.Contains(err.Error(), "delete failed") {
			t.Fatalf("expected delete failed, got %v", err)
		}
	})

	t.Run("rows_affected_error", func(t *testing.T) {
		op, _ := access.NewProtectedDelete("del", key, "")
		prepared := sqlitePrepared{
			operation:  op,
			primaryKey: "id",
		}
		session := &sqliteProtectedSession{
			conn:     conn,
			storage:  storage,
			alive:    true,
			write:    true,
			ops:      []access.ProtectedOperation{op},
			prepared: []sqlitePrepared{prepared},
		}
		smock.ExpectExec("DELETE FROM `items` WHERE `id` = \\?").
			WithArgs("1").
			WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))
		err := session.execute(ctx)
		if err == nil || !strings.Contains(err.Error(), "rows affected error") {
			t.Fatalf("expected rows affected error, got %v", err)
		}
	})

	t.Run("count_not_one", func(t *testing.T) {
		op, _ := access.NewProtectedDelete("del", key, "")
		prepared := sqlitePrepared{
			operation:  op,
			primaryKey: "id",
		}
		session := &sqliteProtectedSession{
			conn:     conn,
			storage:  storage,
			alive:    true,
			write:    true,
			ops:      []access.ProtectedOperation{op},
			prepared: []sqlitePrepared{prepared},
		}
		smock.ExpectExec("DELETE FROM `items` WHERE `id` = \\?").
			WithArgs("1").
			WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows affected
		err := session.execute(ctx)
		if err == nil || !strings.Contains(err.Error(), "unexpected record count") {
			t.Fatalf("expected unexpected record count, got %v", err)
		}
	})

	t.Run("read_action_unsupported_in_execute", func(t *testing.T) {
		op, _ := access.NewProtectedRead("read", access.Get, key)
		prepared := sqlitePrepared{
			operation:  op,
			primaryKey: "id",
		}
		session := &sqliteProtectedSession{
			conn:     conn,
			storage:  storage,
			alive:    true,
			write:    true,
			ops:      []access.ProtectedOperation{op},
			prepared: []sqlitePrepared{prepared},
		}
		err := session.execute(ctx)
		if err == nil {
			t.Fatal("expected unsupportedSQLite on read action in execute")
		}
	})
}

func TestSQLiteProtected_Prepare_MoreCoverage(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, raw)

	conn, err := raw.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT);"); err != nil {
		t.Fatal(err)
	}

	storage := &sqliteProtectedStorage{
		db: raw,
		options: DbOptions{
			Recordsets: map[string]*Recordset{
				"items":         NewRecordset("items", Table, []dal.FieldRef{dal.Field("id")}),
				"missing_table": NewRecordset("missing_table", Table, []dal.FieldRef{dal.Field("id")}),
			},
		},
	}

	t.Run("table_not_in_sqlite_schema", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		key := record.NewKeyWithID("missing_table", "1")
		op, _ := access.NewProtectedRead("r", access.Get, key)
		_, err := session.prepare(ctx, op)
		if err == nil {
			t.Fatal("expected error for table not in sqlite_schema")
		}
	})

	t.Run("prepare_candidate_large_payload", func(t *testing.T) {
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		bigString := strings.Repeat("A", 9<<20) // 9MB > 8MB
		key := record.NewKeyWithID("items", "1")
		op, _ := access.NewProtectedInsert("big", key, map[string]any{"id": "1", "name": bigString})
		_, err := session.prepare(ctx, op)
		if err == nil {
			t.Fatal("expected error on >8MB candidate revision")
		}
	})

	t.Run("prepare_pre_image_large_payload", func(t *testing.T) {
		bigString := strings.Repeat("B", 9<<20) // 9MB > 8MB
		if _, err := conn.ExecContext(ctx, "INSERT INTO items VALUES ('big_pre', ?)", bigString); err != nil {
			t.Fatal(err)
		}
		session := &sqliteProtectedSession{conn: conn, storage: storage, alive: true}
		key := record.NewKeyWithID("items", "big_pre")
		op, _ := access.NewProtectedRead("r", access.Get, key)
		_, err := session.prepare(ctx, op)
		if err == nil {
			t.Fatal("expected error on >8MB pre-image revision")
		}
	})
}
