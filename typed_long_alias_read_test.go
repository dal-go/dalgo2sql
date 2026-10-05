package dalgo2sql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// ErrNotSupported is DALgo's signal to use its generic engine, and DALgo has the fallback
// only for a join: it asks the adapter (CanExecuteJoin, which compiles the whole query)
// before it plans one. A query over one source goes to the adapter's reader with no such
// step, so the refusal is the read's error. The text of the compiler and the README say
// so; this is what they describe, for the 64-byte alias PostgreSQL cannot spell.
func TestALongAliasFailsAOneSourceReadAndDeclinesAJoin(t *testing.T) {
	ctx := context.Background()
	long := strings.Repeat("a", 64)
	options := DbOptions{StructuredQueryDialect: "postgres"}

	t.Run("a query over one source fails the read, with no statement after the catalog's", func(t *testing.T) {
		raw, mock := newPostgresReadMock(t)
		mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(postgresReadCatalogRows(`"Album"`, "AlbumId", "Title"))
		db := NewDatabase(raw, newSchema(), options)
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), long))
		reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
		if !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "output name") {
			t.Fatalf("ExecuteQueryToRecordsReader() = %v, %v; want the read to fail with ErrNotSupported", reader, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a join is declined, and DALgo plans it for its generic engine", func(t *testing.T) {
		raw, mock := newPostgresReadMock(t)
		expectPostgresJoinCatalog(mock, `"Album"`, `"Artist"`)
		q := postgresJoinQuery("ArtistId", "ArtistId", typedTestColumn(typedTestQualified("r", "Name"), long))
		plan, err := dal.PlanJoin(ctx, q, &database{db: raw, options: options})
		if err != nil {
			t.Fatal(err)
		}
		if plan.Strategy != dal.JoinGeneric || !strings.Contains(plan.Reason, "output name") {
			t.Fatalf("PlanJoin() = %+v, want the generic strategy for the long alias", plan)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

// A join whose select list repeats an output name is declined for native execution, so
// DALgo plans it for its generic engine, which refuses it: the adapter never returns a
// result with one of the two columns dropped.
func TestPostgresJoinWithARepeatedOutputNameIsPlannedForTheGenericEngine(t *testing.T) {
	ctx := context.Background()
	options := DbOptions{StructuredQueryDialect: "postgres"}
	q := postgresJoinQuery("ArtistId", "ArtistId",
		typedTestColumn(typedTestQualified("a", "ArtistId"), ""), typedTestColumn(typedTestQualified("r", "ArtistId"), ""))
	for _, inTransaction := range []bool{false, true} {
		name := "on the database handle"
		if inTransaction {
			name = "in a transaction"
		}
		t.Run(name, func(t *testing.T) {
			err := postgresJoinCheck(t, inTransaction, options, q, func(mock sqlmock.Sqlmock) { expectPostgresJoinCatalog(mock, `"Album"`, `"Artist"`) })
			var joinErr *dal.JoinValidationError
			if !errors.As(err, &joinErr) || joinErr.Category != "join_field" || !strings.Contains(err.Error(), "join_plan: PostgreSQL cannot natively compile query") {
				t.Fatalf("CanExecuteJoin() error = %v, want a decline that carries the join_field error", err)
			}
		})
	}
	t.Run("DALgo plans the generic strategy", func(t *testing.T) {
		raw, mock := newPostgresReadMock(t)
		expectPostgresJoinCatalog(mock, `"Album"`, `"Artist"`)
		plan, err := dal.PlanJoin(ctx, q, &database{db: raw, options: options})
		if err != nil {
			t.Fatal(err)
		}
		if plan.Strategy != dal.JoinGeneric || !strings.Contains(plan.Reason, "duplicate output name") {
			t.Fatalf("PlanJoin() = %+v, want the generic strategy for the repeated name", plan)
		}
	})
}
