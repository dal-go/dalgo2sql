package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	_ "modernc.org/sqlite"
)

func recursiveSQLAdapterQuery() dal.StructuredQuery {
	invoices := dal.From(dal.NewRootCollectionRef("invoices", "i")).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("i", "customer_id"), dal.Equal, dal.NewFieldRef("c", "id"))).
		SelectColumns(dal.Column{Expression: dal.NewFieldRef("i", "id")})
	return dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
		Where(dal.NewExistsCondition(invoices)).
		SelectColumns(dal.Column{Expression: dal.NewFieldRef("c", "id")})
}

func TestSQLiteFrameworkExecutesRecursiveQueryThroughLeafScans(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE customers (id INTEGER PRIMARY KEY);
		CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER);
		INSERT INTO customers (id) VALUES (1), (2);
		INSERT INTO invoices (id, customer_id) VALUES (10, 1);`); err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	reader, err := db.ExecuteQueryToRecordsReader(ctx, recursiveSQLAdapterQuery())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := dal.ReadAllToRecords(ctx, reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0].Data().(map[string]any)["id"]) != "1" {
		if len(rows) == 1 {
			t.Fatalf("recursive SQLite row = %#v", rows[0].Data())
		}
		t.Fatalf("recursive SQLite row count = %d", len(rows))
	}
}

func requireRecursiveAdapterRejection(t *testing.T, err error) {
	t.Helper()
	var denied *access.DeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("error = %T %v, want access.DeniedError", err, err)
	}
	if denied.Decision.Operation != access.Query || denied.Decision.Code != access.CodeEnforcementUnsupported || denied.Decision.Scope != access.DecisionScopeOperation {
		t.Fatalf("decision = %#v, want query capability rejection", denied.Decision)
	}
	if got, want := denied.Decision.Explanation, "raw dalgo2sql adapter does not support recursive structured queries"; got != want {
		t.Fatalf("explanation = %q, want %q", got, want)
	}
}

func TestRawSQLAdapterRejectsRecursiveQueryBeforeIO(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	// Construct the raw backend directly. NewDatabase deliberately wraps this
	// backend in DALgo's generic recursive planner, while this test verifies
	// the adapter boundary for callers that reach the raw query routes.
	db := &database{
		recordsReaderProvider: recordsReaderProvider{executeQuery: sqlDB.QueryContext},
		db:                    sqlDB,
		schema:                newSchema(),
		options:               DbOptions{StructuredQueryDialect: "sqlite"},
	}
	query := recursiveSQLAdapterQuery()

	t.Run("records", func(t *testing.T) {
		_, err := db.ExecuteQueryToRecordsReader(context.Background(), query)
		requireRecursiveAdapterRejection(t, err)
	})
	t.Run("recordset", func(t *testing.T) {
		_, err := db.ExecuteQueryToRecordsetReader(context.Background(), query)
		requireRecursiveAdapterRejection(t, err)
	})
	// No query expectation is registered: a call to database/sql would fail
	// this assertion and prove the guard was reached too late.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("recursive query reached SQL I/O: %v", err)
	}
}

func TestRawSQLAdapterLeavesFlatAndJoinQueriesOnExistingRoutes(t *testing.T) {
	flat := dal.From(dal.NewRootCollectionRef("customers", "c")).NewQuery().
		SelectColumns(dal.Column{Expression: dal.NewFieldRef("c", "id")})
	if err := rejectRawRecursiveStructuredQuery(flat); err != nil {
		t.Fatalf("flat query rejected: %v", err)
	}
	if err := rejectRawRecursiveStructuredQuery(chinookNestedSQLQuery()); err != nil {
		t.Fatalf("JOIN query rejected: %v", err)
	}
}
