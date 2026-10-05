package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

const joinKeyScript = `CREATE TABLE album (id INTEGER PRIMARY KEY, title TEXT, artist_id INTEGER);
CREATE TABLE artist (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO artist VALUES (101, 'Queen'), (102, 'Abba');
INSERT INTO album VALUES (1, 'Jazz', 101), (2, 'Arrival', 102);`

// joinKeyQuery joins album (the base, a) to artist (r) on artist_id. The two tables have a
// column of the same name, id, whose values differ, so a result column or a key taken
// from the wrong table shows.
func joinKeyQuery(columns ...dal.Column) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("album", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("artist", "r"), dal.JoinInner, typedTestJoinOn("a", "artist_id", "r", "id")),
	).NewQuery().OrderBy(dal.Ascending(typedTestQualified("a", "id"))).SelectColumns(columns...)
}

// readJoinRecords reads q from db inside a read transaction, as DALgo does for a join
// the adapter runs natively, and returns each record's key and data.
func readJoinRecords(ctx context.Context, db dal.DB, q dal.StructuredQuery) (keys []any, data []map[string]any, err error) {
	err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		for {
			record, err := reader.Next()
			if errors.Is(err, dal.ErrNoMoreRecords) {
				return nil
			}
			if err != nil {
				return err
			}
			keys = append(keys, record.Key().ID)
			data = append(data, record.Data().(map[string]any))
		}
	})
	return keys, data, err
}

// The same SQLite database read natively and read by DALgo's generic join: the generic
// engine is the reference for what a join returns. Forcing it is a matter of declining
// the native plan, which a join eligibility hook does without a native compiler.
func joinKeyDatabases(t *testing.T) (native, generic dal.DB) {
	t.Helper()
	raw := openNullTestDB(t, joinKeyScript)
	options := DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"id"}}
	native = NewDatabase(raw, newSchema(), options)
	options.NativeJoinEligibility = func(context.Context, dal.StructuredQuery) error { return errors.New("generic engine only") }
	generic = NewDatabase(raw, newSchema(), options)
	return native, generic
}

func TestCompileStructuredSQLRefusesARepeatedOutputNameInAJoin(t *testing.T) {
	qualified := typedTestQualified
	col := typedTestColumn
	for _, tc := range []struct {
		name  string
		query dal.StructuredQuery
		path  string
	}{
		{"two sources' columns of one name", joinKeyQuery(col(qualified("a", "id"), ""), col(qualified("r", "id"), "")), "columns[1]"},
		{"one alias on two columns", joinKeyQuery(col(qualified("a", "title"), "x"), col(qualified("r", "name"), "x")), "columns[1]"},
		{"an alias that is another column's own name", joinKeyQuery(col(qualified("a", "title"), ""), col(qualified("r", "name"), "title")), "columns[1]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, _, err := compileStructuredSQL(tc.query)
			var joinErr *dal.JoinValidationError
			if !errors.As(err, &joinErr) {
				t.Fatalf("compileStructuredSQL() = %q, %v; want the generic engine's kind of error", text, err)
			}
			if joinErr.Category != "join_field" || joinErr.Path != tc.path || !strings.HasPrefix(joinErr.Message, "duplicate output name ") {
				t.Fatalf("error = %#v, want join_field at %s: duplicate output name", joinErr, tc.path)
			}
			// The join is not accepted for native execution, so DALgo plans it for its own
			// engine, which refuses it (below).
			raw := openNullTestDB(t, joinKeyScript)
			tx, err := raw.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if err := canExecuteSQLiteJoin(context.Background(), tc.query, "sqlite", tx.QueryContext); !errors.As(err, &joinErr) || joinErr.Category != "join_field" {
				t.Fatalf("canExecuteSQLiteJoin() = %v, want it declined for the repeated name", err)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		query dal.StructuredQuery
	}{
		{"one expression asked twice", joinKeyQuery(col(qualified("a", "title"), ""), col(qualified("a", "title"), ""))},
		{"distinct names", joinKeyQuery(col(qualified("a", "id"), ""), col(qualified("r", "id"), "artist"))},
		{"an expression without a name the compiler can know", joinKeyQuery(col(qualified("a", "id"), ""), col(dal.Binary(qualified("r", "id"), dal.Add, dal.NewConstant(1)), ""))},
		{"a lone source", dal.From(dal.NewRootCollectionRef("album", "")).NewQuery().SelectColumns(col(typedTestField("title"), ""), col(typedTestField("title"), ""))},
	} {
		t.Run(tc.name+" compiles", func(t *testing.T) {
			if _, _, err := compileStructuredSQL(tc.query); err != nil {
				t.Fatalf("compileStructuredSQL() error = %v", err)
			}
		})
	}
}

// A read of the same query through the adapter and through DALgo's generic engine says
// the same: the repeated name is refused, where it used to return a result with one of
// the two columns dropped.
func TestSQLiteJoinWithARepeatedOutputNameIsRefusedLikeTheGenericEngine(t *testing.T) {
	ctx := context.Background()
	native, generic := joinKeyDatabases(t)
	q := joinKeyQuery(typedTestColumn(typedTestQualified("a", "id"), ""), typedTestColumn(typedTestQualified("r", "id"), ""))
	var want, got *dal.JoinValidationError
	if _, _, err := readJoinRecords(ctx, generic, q); !errors.As(err, &want) {
		t.Fatalf("the generic engine returned %v, want a join validation error: the reference of this test is gone", err)
	}
	_, _, err := readJoinRecords(ctx, native, q)
	if !errors.As(err, &got) || *got != *want {
		t.Fatalf("the native read returned %v, want what the generic engine returns: %v", err, want)
	}
}
