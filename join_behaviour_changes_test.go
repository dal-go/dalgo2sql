package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// Two behaviour changes of the join keys (the change that made a join key each record by
// the base row) were declared in its description and had no test of their own. They are
// pinned here as they are.

// (1) A join with a select list always carries the base's key as a column of its own,
// qualified by the base. On a mount whose configured key is not a column of the base, a
// select list that names a column of that name of another table (r.id) used to be read, and
// the joined table's column keyed the records; the key column is now asked of the base, and
// the base has none, so the read fails. DALgo has planned the join natively by then:
// CanExecuteJoin compiles the query without the key column, and accepts it. A select-all of
// the same mount, which no column is added to, keeps the placeholder key.

func TestPostgresJoinWithAKeyThatIsNotAColumnOfTheBaseFailsWhenTheSelectListNamesAnotherTablesColumnOfThatName(t *testing.T) {
	ctx := context.Background()
	exact := func(name string) string { return `"` + name + `"` }
	album := []string{"title", "artist_id"} // the configured key, id, is artist's
	options := DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"id"}}
	selectList := pgJoinKeyQuery(typedTestColumn(typedTestQualified("a", "title"), ""), typedTestColumn(typedTestQualified("r", "id"), ""))

	t.Run("the join is accepted for native execution", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		pgJoinKeyCatalog(mock, album, exact)
		if err := (&database{db: db, options: options}).CanExecuteJoin(ctx, selectList); err != nil {
			t.Fatalf("CanExecuteJoin() error = %v, want it accepted: the key column is not part of the check", err)
		}
	})
	t.Run("the read fails, naming the column and the base, and runs no statement", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		pgJoinKeyCatalog(mock, album, exact)
		_, err := getRecordsReaderWithOptions(ctx, selectList, db.QueryContext, options)
		if err == nil || !strings.Contains(err.Error(), `field "id" is not a column of source "a"`) {
			t.Fatalf("error = %v, want the key named as no column of the base", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a select-all of the same mount keeps the placeholder key", func(t *testing.T) {
		db, mock := newPostgresReadMock(t)
		pgJoinKeyCatalog(mock, album, exact)
		mock.ExpectQuery(`SELECT * FROM "album" AS "a" INNER JOIN "artist" AS "r" ON ("a"."artist_id" = "r"."id")`).
			WillReturnRows(sqlmock.NewRows([]string{"title", "artist_id", "id", "name"}).AddRow("Jazz", int64(101), int64(101), "Queen"))
		reader, err := getRecordsReaderWithOptions(ctx, pgJoinKeyQuery(), db.QueryContext, options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		record, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := record.Key().ID; got != recordIDHelperColumn {
			t.Fatalf("record key = %v, want the placeholder: a column of the joined table never keys a record", got)
		}
	})
}

func TestSQLiteJoinWithAKeyThatIsNotAColumnOfTheBaseFailsWhenTheSelectListNamesAnotherTablesColumnOfThatName(t *testing.T) {
	ctx := context.Background()
	raw := openNullTestDB(t, `CREATE TABLE album (album_id INTEGER PRIMARY KEY, title TEXT, artist_id INTEGER);
CREATE TABLE artist (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO artist VALUES (101, 'Queen'), (102, 'Abba');
INSERT INTO album VALUES (1, 'Jazz', 101), (2, 'Arrival', 102);`)
	join := func(columns ...dal.Column) dal.StructuredQuery {
		return dal.From(dal.NewRootCollectionRef("album", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("artist", "r"), dal.JoinInner, typedTestJoinOn("a", "artist_id", "r", "id")),
		).NewQuery().SelectColumns(columns...)
	}
	selectList := join(typedTestColumn(typedTestQualified("a", "title"), ""), typedTestColumn(typedTestQualified("r", "id"), ""))
	database := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"id"}}) // artist's id

	t.Run("the join is accepted for native execution", func(t *testing.T) {
		tx, err := raw.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := canExecuteSQLiteJoin(ctx, selectList, "sqlite", tx.QueryContext); err != nil {
			t.Fatalf("canExecuteSQLiteJoin() error = %v, want it accepted: the key column is not part of the check", err)
		}
	})
	t.Run("the read fails with the server's error", func(t *testing.T) {
		_, _, err := readJoinRecords(ctx, database, selectList)
		if err == nil || !strings.Contains(err.Error(), "no such column") {
			t.Fatalf("error = %v, want the server's: the base has no such column", err)
		}
	})
	t.Run("a select-all of the same mount keeps the placeholder key", func(t *testing.T) {
		keys, _, err := readJoinRecords(ctx, database, join())
		if err != nil {
			t.Fatal(err)
		}
		if want := []any{recordIDHelperColumn, recordIDHelperColumn}; !reflect.DeepEqual(keys, want) {
			t.Fatalf("keys = %v, want the placeholder: a column of the joined table never keys a record", keys)
		}
	})
}

// (2) A grouped join with no select list whose group keys share a name (GROUP BY a.id,
// r.id) has the group keys as its implicit columns, which are checked for a repeated output
// name too. Both compilers refuse it, so the join is declined for native execution and DALgo
// runs it in its bounded generic engine, whose own check of the select list sees none: the
// query is not refused, and it is not pushed down.

func groupedJoinWithSharedGroupKeyNames(album, artist dal.CollectionRef, onAlbum, onArtist string) dal.StructuredQuery {
	return dal.From(album).Join(
		dal.NewJoinedSource(artist, dal.JoinInner, typedTestJoinOn("a", onAlbum, "r", onArtist)),
	).NewQuery().GroupBy(typedTestQualified("a", "id"), typedTestQualified("r", "id")).SelectColumns()
}

func TestAGroupedJoinWhoseGroupKeysShareANameIsDeclinedForNativeExecution(t *testing.T) {
	ctx := context.Background()
	repeated := func(t *testing.T, err error) {
		t.Helper()
		var joinErr *dal.JoinValidationError
		if !errors.As(err, &joinErr) || joinErr.Category != "join_field" || !strings.HasPrefix(joinErr.Message, "duplicate output name ") {
			t.Fatalf("error = %v, want join_field: duplicate output name", err)
		}
	}
	t.Run("the SQLite compiler refuses it, and the join is declined", func(t *testing.T) {
		q := groupedJoinWithSharedGroupKeyNames(dal.NewRootCollectionRef("album", "a"), dal.NewRootCollectionRef("artist", "r"), "artist_id", "id")
		_, _, err := compileStructuredSQL(q)
		repeated(t, err)
		raw := openNullTestDB(t, joinKeyScript)
		tx, err := raw.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		repeated(t, canExecuteSQLiteJoin(ctx, q, "sqlite", tx.QueryContext))
	})
	t.Run("the typed PostgreSQL compiler refuses it, and the join is declined", func(t *testing.T) {
		q := groupedJoinWithSharedGroupKeyNames(dal.NewRootCollectionRef("Album", "a"), dal.NewRootCollectionRef("Artist", "r"), "ArtistId", "ArtistId")
		_, err := compileTypedStatement(q, newPostgresDialect(postgresExact), typedCatalogFacts{})
		repeated(t, err)
		// Both tables' own id is what the group keys name: a.id and r.id.
		q = groupedJoinWithSharedGroupKeyNames(dal.NewRootCollectionRef("album", "a"), dal.NewRootCollectionRef("artist", "r"), "artist_id", "id")
		db, mock := newPostgresReadMock(t)
		pgJoinKeyCatalog(mock, []string{"id", "title", "artist_id"}, func(name string) string { return `"` + name + `"` })
		err = (&database{db: db, options: DbOptions{StructuredQueryDialect: "postgres"}}).CanExecuteJoin(ctx, q)
		repeated(t, err)
		if !strings.Contains(err.Error(), "join_plan: PostgreSQL cannot natively compile query") {
			t.Fatalf("CanExecuteJoin() error = %v, want a decline", err)
		}
	})
	t.Run("DALgo runs it in its generic engine and does not refuse it", func(t *testing.T) {
		native, _ := joinKeyDatabases(t)
		q := groupedJoinWithSharedGroupKeyNames(dal.NewRootCollectionRef("album", "a"), dal.NewRootCollectionRef("artist", "r"), "artist_id", "id")
		keys, _, err := readJoinRecords(ctx, native, q)
		if err != nil {
			t.Fatalf("the read returned %v, want a result from the generic engine", err)
		}
		if len(keys) != 2 {
			t.Fatalf("keys = %v, want a record for each of the two groups", keys)
		}
	})
}
