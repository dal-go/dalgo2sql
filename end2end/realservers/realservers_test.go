// Package realservers runs the typed MySQL, Oracle and SQL Server dialects against
// real servers.
//
// Each engine's test needs its connection string in an environment variable and is
// skipped without it, unless DALGO2SQL_REQUIRE_REAL_SERVERS is set, which makes a
// missing server a failure (CI sets it: .github/workflows/real-servers.yml):
//
//	DALGO2SQL_MYSQL_DSN=root:secret@tcp(127.0.0.1:3306)/dalgo go test ./realservers -run MySQL
//	DALGO2SQL_ORACLE_URL=oracle://dalgo:secret@127.0.0.1:1521/FREEPDB1 go test ./realservers -run Oracle
//	DALGO2SQL_SQLSERVER_URL='sqlserver://sa:Secret-123@127.0.0.1:1433?database=master' go test ./realservers -run SQLServer
//
// The tests create and drop their own tables (Artist and Album, with quoted
// mixed-case names) in the schema the connection uses.
package realservers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2sql"
)

const requireRealServers = "DALGO2SQL_REQUIRE_REAL_SERVERS"

// openRealServer opens the server named by env, or skips the test (fails it when
// DALGO2SQL_REQUIRE_REAL_SERVERS is set). It waits up to a minute for the server to
// answer, as a container that reports healthy may still be opening its database.
func openRealServer(t *testing.T, driverName, env string) *sql.DB {
	t.Helper()
	dsn := os.Getenv(env)
	if dsn == "" {
		if os.Getenv(requireRealServers) != "" {
			t.Fatalf("%s is not set and %s requires the real-server tests to run", env, requireRealServers)
		}
		t.Skipf("%s is not set", env)
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	deadline := time.Now().Add(time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("the server named by %s does not answer: %v", env, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// execAll runs DDL and DML statements one by one.
func execAll(t *testing.T, db *sql.DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// norm writes a value the way the expectations below spell it, whatever Go type the
// driver chose: NULL, a number in its shortest form, or the text.
func norm(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return norm(string(v))
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return norm(f)
		}
		return v
	case float32:
		return norm(float64(v))
	case float64:
		if v == math.Trunc(v) && math.Abs(v) < 1e15 {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(math.Round(v*1e6)/1e6, 'f', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	}
	return fmt.Sprint(value)
}

// rows reads the query and writes each record as its values for columns, in order.
func rows(t *testing.T, db dal.DB, q dal.StructuredQuery, columns ...string) []string {
	t.Helper()
	records, err := dal.ExecuteQueryAndReadAllToRecords(context.Background(), q, db)
	if err != nil {
		t.Fatalf("%s: %v", q.String(), err)
	}
	out := make([]string, len(records))
	for i, record := range records {
		data, ok := record.Data().(map[string]any)
		if !ok {
			t.Fatalf("record %d holds %T, want a map", i, record.Data())
		}
		values := make([]string, len(columns))
		for j, column := range columns {
			value, present := data[column]
			if !present {
				t.Fatalf("record %d has no column %q: %v", i, column, data)
			}
			values[j] = norm(value)
		}
		out[i] = strings.Join(values, "|")
	}
	return out
}

func expectRows(t *testing.T, name string, got []string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got %q\nwant %q", name, got, want)
	}
}

func field(name string) dal.FieldRef  { return dal.Field(name) }
func constant(value any) dal.Constant { return dal.Constant{Value: value} }
func column(e dal.Expression, alias string) dal.Column {
	return dal.Column{Expression: e, Alias: alias}
}
func from(name, alias string) dal.FromSource { return dal.From(dal.NewRootCollectionRef(name, alias)) }

// exerciseTypedDialect runs every read DataTug sends, over the fixture both engines
// hold: Artist(ArtistId, Name NULL) and Album(AlbumId, Title, ArtistId, Price NULL).
// notFound is the spelling of Album that names no table, and suggested the table the
// error must point at.
func exerciseTypedDialect(t *testing.T, sqlDB *sql.DB, dialect, notFound string) {
	db := dalgo2sql.NewDatabase(sqlDB, dalgo2sql.NewSimpleSchema("ID"), dalgo2sql.DbOptions{StructuredQueryDialect: dialect})
	album := func() dal.IQueryBuilder { return from("Album", "").NewQuery() }
	artist := func() dal.IQueryBuilder { return from("Artist", "").NewQuery() }

	expectRows(t, "where, order by, limit", rows(t, db, album().
		Where(dal.NewComparison(field("ArtistId"), dal.Equal, constant(2))).
		OrderBy(dal.Ascending(field("Title"))).Limit(5).
		SelectColumns(column(field("Title"), ""), column(field("AlbumId"), "id")), "Title", "id"),
		"Balls to the Wall|2", "Restless and Wild|3")

	expectRows(t, "limit and offset", rows(t, db, album().OrderBy(dal.Ascending(field("AlbumId"))).Limit(2).Offset(1).
		SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "2", "3")
	expectRows(t, "offset alone", rows(t, db, album().OrderBy(dal.Ascending(field("AlbumId"))).Offset(3).
		SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "4")

	expectRows(t, "NULL first ascending", rows(t, db, artist().OrderBy(dal.Ascending(field("Name"))).
		SelectColumns(column(field("Name"), "")), "Name"), "NULL", "AC/DC", "Accept")
	expectRows(t, "NULL last descending", rows(t, db, artist().OrderBy(dal.Descending(field("Name"))).
		SelectColumns(column(field("Name"), "")), "Name"), "Accept", "AC/DC", "NULL")

	expectRows(t, "group by, aggregates, having, order by an alias", rows(t, db, album().
		GroupBy(field("ArtistId")).
		Having(dal.NewComparison(field("albums"), dal.GreaterThen, constant(1))).
		OrderBy(dal.Descending(field("albums")), dal.Ascending(field("artist"))).
		SelectColumns(column(field("ArtistId"), "artist"), dal.CountAs(field("AlbumId"), "albums"),
			dal.SumAs(field("Price"), "total"), dal.AverageAs(field("Price"), "mean"), dal.MinAs(field("Title"), "first"), dal.MaxAs(field("Price"), "top")),
		"artist", "albums", "total", "mean", "first", "top"),
		"1|2|17.49|8.745|For Those About to Rock|9.99", "2|2|5|5|Balls to the Wall|5")
	expectRows(t, "count(*) and count distinct", rows(t, db, album().
		SelectColumns(dal.CountAs(dal.Star(), "n"), column(dal.NewAggregate(dal.COUNT, true, field("ArtistId")), "artists")), "n", "artists"), "4|2")

	expectRows(t, "IN, NOT IN and an empty IN", rows(t, db, album().
		Where(dal.NewGroupCondition(dal.And,
			dal.NewComparison(field("AlbumId"), dal.In, dal.NewArray([]int{1, 2, 3})),
			dal.NewComparison(field("Title"), dal.NotIn, dal.NewArray([]string{"Restless and Wild"})),
			dal.NewComparison(field("AlbumId"), dal.NotIn, dal.NewArray([]int{})),
		)).OrderBy(dal.Ascending(field("AlbumId"))).SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "1", "2")
	expectRows(t, "an empty IN finds nothing", rows(t, db, album().
		Where(dal.NewComparison(field("AlbumId"), dal.In, dal.NewArray([]int{}))).SelectColumns(column(field("AlbumId"), "")), "AlbumId"))
	expectRows(t, "IS NULL and equality with a string", rows(t, db, album().
		Where(dal.NewGroupCondition(dal.Or, dal.NewIsNullCondition(field("Price")), dal.NewComparison(field("Title"), dal.Equal, constant("Restless and Wild")))).
		OrderBy(dal.Ascending(field("AlbumId"))).SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "2", "3")
	expectRows(t, "a float constant against a decimal column", rows(t, db, album().
		Where(dal.NewComparison(field("Price"), dal.Equal, constant(9.99))).SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "1")
	expectRows(t, "a float32 constant reads as its decimal text", rows(t, db, album().
		Where(dal.NewComparison(field("Price"), dal.Equal, constant(float32(7.5)))).SelectColumns(column(field("AlbumId"), "")), "AlbumId"), "4")

	expectRows(t, "arithmetic, and division by zero is NULL", rows(t, db, album().
		Where(dal.NewComparison(field("AlbumId"), dal.Equal, constant(1))).
		SelectColumns(column(dal.Binary(field("AlbumId"), dal.Divide, constant(0)), "byZero"),
			column(dal.Binary(field("AlbumId"), dal.Divide, constant(4)), "quarter"),
			column(dal.Binary(field("ArtistId"), dal.Multiply, constant(int64(math.MaxInt64))), "wide"),
			column(dal.Binary(field("Price"), dal.Add, field("AlbumId")), "sum")),
		"byZero", "quarter", "wide", "sum"), "NULL|0.25|9223372036854776000|10.99")

	expectRows(t, "an alias named as another column orders by the alias's expression", rows(t, db, album().
		OrderBy(dal.Ascending(field("Title"))).
		SelectColumns(column(field("AlbumId"), "Title"), column(field("Title"), "AlbumId")), "Title"), "1", "2", "3", "4")

	expectRows(t, "an inner join", rows(t, db, dal.From(dal.NewRootCollectionRef("Album", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner,
			dal.NewComparison(dal.NewFieldRef("a", "ArtistId"), dal.Equal, dal.NewFieldRef("r", "ArtistId")))).NewQuery().
		OrderBy(dal.Ascending(dal.NewFieldRef("a", "AlbumId"))).
		SelectColumns(column(dal.NewFieldRef("a", "Title"), "title"), column(dal.NewFieldRef("r", "Name"), "artist")), "title", "artist"),
		"For Those About to Rock|AC/DC", "Balls to the Wall|Accept", "Restless and Wild|Accept", "Let There Be Rock|AC/DC")
	expectRows(t, "a left join keeps the artist without albums", rows(t, db, dal.From(dal.NewRootCollectionRef("Artist", "r")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Album", "a"), dal.JoinLeft,
			dal.NewComparison(dal.NewFieldRef("r", "ArtistId"), dal.Equal, dal.NewFieldRef("a", "ArtistId")))).NewQuery().
		Where(dal.NewIsNullCondition(dal.NewFieldRef("a", "AlbumId"))).
		SelectColumns(column(dal.NewFieldRef("r", "ArtistId"), "id")), "id"), "3")

	expectRows(t, "a select-all minus an exclusion", rows(t, db, album().
		Where(dal.NewComparison(field("AlbumId"), dal.Equal, constant(4))).
		SelectColumns(dal.AllColumnsExcept("Price", "ArtistId")), "AlbumId", "Title"), "4|Let There Be Rock")

	// The Go types the driver hands back, for a reader of the log.
	typed, err := dal.ExecuteQueryAndReadAllToRecords(context.Background(), album().GroupBy(field("ArtistId")).OrderBy(dal.Ascending(field("ArtistId"))).Limit(1).
		SelectColumns(column(field("ArtistId"), ""), dal.CountAs(field("AlbumId"), "n"), dal.SumAs(field("Price"), "sum"), dal.MaxAs(field("Price"), "max"), dal.MaxAs(field("Title"), "title")), db)
	if err != nil || len(typed) != 1 {
		t.Fatalf("typed read = %v, %v", typed, err)
	}
	for _, name := range []string{"ArtistId", "n", "sum", "max", "title"} {
		value := typed[0].Data().(map[string]any)[name]
		t.Logf("%s: %s returns %T %v", dialect, name, value, value)
	}

	records, err := dal.ExecuteQueryAndReadAllToRecords(context.Background(), album().OrderBy(dal.Ascending(field("AlbumId"))).Limit(1).SelectColumns(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || norm(records[0].Key().ID) != "1" {
		t.Errorf("a select-all is keyed by the primary key the catalog names: %v", records)
	}

	_, err = dal.ExecuteQueryAndReadAllToRecords(context.Background(), from(notFound, "").NewQuery().SelectColumns(), db)
	var missing *dalgo2sql.TableNotFoundError
	if !errors.As(err, &missing) || missing.SuggestedName != "Album" {
		t.Errorf("a table spelt %q = %v, want not found with Album suggested", notFound, err)
	}
}
