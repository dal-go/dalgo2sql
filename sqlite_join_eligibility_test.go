package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type dummyCompiler struct{}

func (dummyCompiler) CompileNativeStructuredQuery(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error) {
	return "", nil, nil
}

func TestSQLiteJoinEligibility_DatabaseAndTransaction(t *testing.T) {
	ctx := context.Background()

	t.Run("database_native_join_without_compiler", func(t *testing.T) {
		dtb := &database{options: DbOptions{
			NativeJoinEligibility: func(ctx context.Context, q dal.StructuredQuery) error { return nil },
		}}
		err := dtb.CanExecuteJoin(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "requires a native structured query compiler") {
			t.Fatalf("expected compiler error, got %v", err)
		}
	})

	t.Run("database_native_join_with_compiler", func(t *testing.T) {
		called := false
		dtb := &database{options: DbOptions{
			NativeJoinEligibility: func(ctx context.Context, q dal.StructuredQuery) error {
				called = true
				return nil
			},
			NativeStructuredQueryCompiler: dummyCompiler{},
		}}
		err := dtb.CanExecuteJoin(ctx, nil)
		if err != nil || !called {
			t.Fatalf("expected nil err and called true, got %v", err)
		}
	})

	t.Run("transaction_native_join_without_compiler", func(t *testing.T) {
		tx := transaction{sqlOptions: DbOptions{
			NativeJoinEligibility: func(ctx context.Context, q dal.StructuredQuery) error { return nil },
		}}
		err := tx.CanExecuteJoin(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "requires a native structured query compiler") {
			t.Fatalf("expected compiler error, got %v", err)
		}
	})
}

func TestSQLiteJoinEligibility_SourcesAndPreflight(t *testing.T) {
	ctx := context.Background()

	t.Run("duplicate_source_alias", func(t *testing.T) {
		from := dal.From(dal.NewRootCollectionRef("t1", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t2", "a"), dal.JoinInner, sqlJoin("a", "id", "a", "id")),
		)
		_, err := sqliteJoinSources(from)
		if err == nil || !strings.Contains(err.Error(), "duplicate source alias") {
			t.Fatalf("expected duplicate alias error, got %v", err)
		}
	})

	t.Run("nested_join_walk_error", func(t *testing.T) {
		badChild := dal.From(dal.NewRootCollectionRef("t2", "")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t3", "bad-alias!"), dal.JoinInner, sqlJoin("t2", "id", "bad-alias!", "id")),
		)
		from := dal.From(dal.NewRootCollectionRef("t1", "")).Join(
			dal.NewJoinedFrom(badChild, dal.JoinInner, sqlJoin("t1", "id", "t2", "id")),
		)
		_, err := sqliteJoinSources(from)
		if err == nil {
			t.Fatal("expected walk error on nested join")
		}
	})

	t.Run("nested_join_preflight_error", func(t *testing.T) {
		raw, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(t, raw)
		if _, err := raw.Exec("CREATE TABLE t1 (id INTEGER); CREATE TABLE t2 (id INTEGER); CREATE TABLE t3 (id INTEGER);"); err != nil {
			t.Fatal(err)
		}

		childJoin := dal.From(dal.NewRootCollectionRef("t2", "t2")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("t3", "t3"), dal.JoinInner, dal.NewFieldRef("t2", "id")),
		)
		from := dal.From(dal.NewRootCollectionRef("t1", "t1")).Join(
			dal.NewJoinedFrom(childJoin, dal.JoinInner, sqlJoin("t1", "id", "t2", "id")),
		)
		sources, err := sqliteJoinSources(from)
		if err != nil {
			t.Fatal(err)
		}
		err = preflightSQLiteJoinKeys(ctx, from, sources, raw.QueryContext, "from")
		if err == nil || !strings.Contains(err.Error(), "must be a comparison") {
			t.Fatalf("expected error from child preflight, got %v", err)
		}
	})

	t.Run("sqliteCollectionSource_custom_source", func(t *testing.T) {
		qs := dal.NewQuerySource(nil, "derived")
		_, err := sqliteCollectionSource(qs)
		if err == nil || !strings.Contains(err.Error(), "has no SQLite table metadata") {
			t.Fatalf("expected metadata error, got %v", err)
		}

		sources := map[string]sqliteJoinSource{
			"derived": {source: qs, class: make(map[string]sqliteJoinKeyClass)},
		}
		_, err = sqliteJoinKeyClassFor(ctx, sources, dal.NewFieldRef("derived", "id"), nil)
		if err == nil || !strings.Contains(err.Error(), "has no SQLite table metadata") {
			t.Fatalf("expected metadata error, got %v", err)
		}
	})

	t.Run("sqliteJoinFields_unsupported_source_and_dialect", func(t *testing.T) {
		qs := dal.NewQuerySource(nil, "derived")
		_, err := sqliteJoinFields(ctx, qs, "sqlite", nil)
		if err == nil || !strings.Contains(err.Error(), "has no SQLite table metadata") {
			t.Fatalf("expected metadata error, got %v", err)
		}

		_, err = sqliteJoinFields(ctx, dal.NewRootCollectionRef("t", ""), "mysql", nil)
		if err == nil || !strings.Contains(err.Error(), "available only for the SQLite") {
			t.Fatalf("expected dialect error, got %v", err)
		}
	})

	t.Run("sqliteDeclaredColumnType_table_columns_error", func(t *testing.T) {
		queryFailure := func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("db broken")
		}
		_, err := sqliteDeclaredColumnType(ctx, dal.NewRootCollectionRef("t", ""), "id", queryFailure)
		if err == nil || !strings.Contains(err.Error(), "db broken") {
			t.Fatalf("expected db broken error, got %v", err)
		}
	})
}

func TestSQLiteJoinEligibility_TableColumnsAndPreflight_MockErrors(t *testing.T) {
	ctx := context.Background()
	sdb, smock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, sdb)

	t.Run("sqliteTableColumns_scan_error", func(t *testing.T) {
		smock.ExpectQuery("PRAGMA table_info").WillReturnRows(sqlmock.NewRows([]string{"cid"}).AddRow(1))
		_, err := sqliteTableColumns(ctx, dal.NewRootCollectionRef("t", ""), sdb.QueryContext)
		if err == nil || !strings.Contains(err.Error(), "scan SQLite table metadata") {
			t.Fatalf("expected scan error, got %v", err)
		}
	})

	t.Run("sqliteTableColumns_rows_err", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk"}).
			AddRow(1, "id", "INTEGER", 0, nil, 1).
			RowError(0, errors.New("pragma stream error"))
		smock.ExpectQuery("PRAGMA table_info").WillReturnRows(rows)
		_, err := sqliteTableColumns(ctx, dal.NewRootCollectionRef("t", ""), sdb.QueryContext)
		if err == nil || !strings.Contains(err.Error(), "pragma stream error") {
			t.Fatalf("expected pragma stream error, got %v", err)
		}
	})

	t.Run("sqlitePreflightColumnValues_compile_table_error", func(t *testing.T) {
		parent := dalrecord.NewKeyWithID("parent", "p1")
		child := dal.NewCollectionRef("child", "", parent)
		err := sqlitePreflightColumnValues(ctx, child, "id", sqliteJoinNumber, sdb.QueryContext)
		if err == nil || !strings.Contains(err.Error(), "parented collection sources") {
			t.Fatalf("expected parented collection error, got %v", err)
		}
	})

	t.Run("sqlitePreflightColumnValues_scan_error", func(t *testing.T) {
		smock.ExpectQuery("SELECT typeof.*FROM.*WHERE.*IS NOT NULL").
			WillReturnRows(sqlmock.NewRows([]string{"typeof"}).AddRow("integer")) // 1 column instead of 2
		err := sqlitePreflightColumnValues(ctx, dal.NewRootCollectionRef("t", ""), "id", sqliteJoinNumber, sdb.QueryContext)
		if err == nil || !strings.Contains(err.Error(), "scan SQLite JOIN key") {
			t.Fatalf("expected scan error, got %v", err)
		}
	})

	t.Run("sqlitePreflightColumnValues_rows_err", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"typeof", "val"}).
			AddRow("integer", int64(1)).
			RowError(0, errors.New("preflight stream error"))
		smock.ExpectQuery("SELECT typeof.*FROM.*WHERE.*IS NOT NULL").
			WillReturnRows(rows)
		err := sqlitePreflightColumnValues(ctx, dal.NewRootCollectionRef("t", ""), "id", sqliteJoinNumber, sdb.QueryContext)
		if err == nil || !strings.Contains(err.Error(), "preflight stream error") {
			t.Fatalf("expected rows error, got %v", err)
		}
	})
}
