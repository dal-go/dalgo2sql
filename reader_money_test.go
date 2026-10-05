package dalgo2sql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// The DTQL money option asks for exact decimal results, which a statement computes in
// double precision, so the typed compiler refuses a query that carries it. The compiler
// looks for the option on the query it is handed, and the records reader hands it a
// wrapper of its own when it adds the key column or the key order to the query: a wrapper
// that does not carry the option on turns the refusal into a statement in double
// precision. The reader reads the option from the caller's query, before it wraps it.
func TestRecordsReaderRefusesTheMoneyOptionWhateverItWrapsTheQueryIn(t *testing.T) {
	ctx := context.Background()
	money := &dal.MoneyConfig{MinorUnitScale: 2, DivisionScale: 4, Rounding: "halfEven"}
	albums := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	ratio := typedTestColumn(dal.Binary(typedTestField("Total"), dal.Divide, typedTestField("AlbumId")), "ratio")
	keyed := DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"AlbumId"}}
	for _, tc := range []struct {
		name    string
		query   dal.StructuredQuery
		options DbOptions
	}{
		// The select list names no key, so the reader adds one with dal.WithColumns.
		{"a division, a select list that does not name the key", typedMoneyQuery{StructuredQuery: albums().SelectColumns(ratio), money: money}, keyed},
		{"the key named by another spelling is still added", typedMoneyQuery{StructuredQuery: albums().SelectColumns(ratio, typedTestColumn(typedTestField("AlbumId"), "id")), money: money}, keyed},
		// A keys-only query is ordered by the key with a wrapper of the reader's own.
		{"a keys-only read", typedMoneyQuery{StructuredQuery: albums().SelectKeysOnly(reflect.Int), money: money}, keyed},
		{"no key to add", typedMoneyQuery{StructuredQuery: albums().SelectColumns(ratio), money: money}, DbOptions{StructuredQueryDialect: "postgres"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The mock expects no statement: a refusal needs neither the catalog nor the
			// server.
			db, mock := newPostgresReadMock(t)
			reader, err := getRecordsReaderWithOptions(ctx, tc.query, db.QueryContext, tc.options)
			if !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "money") {
				t.Fatalf("getRecordsReaderWithOptions() = %v, %v; want the money refusal, which is ErrNotSupported", reader, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("a query that carries no money option is read, wrapped or not", func(t *testing.T) {
		for _, q := range []dal.StructuredQuery{
			typedMoneyQuery{StructuredQuery: albums().SelectColumns(ratio)},
			albums().SelectColumns(ratio),
		} {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
				AddRow(`"Album"`, "AlbumId", "integer", "N", int64(23), int64(0), true, false, false).
				AddRow(`"Album"`, "Total", "integer", "N", int64(23), int64(0), false, false, false))
			mock.ExpectQuery(`SELECT (("Total")::double precision / NULLIF(("AlbumId")::double precision, 0)) AS "ratio", "AlbumId" AS "__dalgo_record_id" FROM "Album"`).
				WillReturnRows(sqlmock.NewRows([]string{"ratio", "__dalgo_record_id"}))
			reader, err := getRecordsReaderWithOptions(ctx, q, db.QueryContext, keyed)
			if err != nil {
				t.Fatal(err)
			}
			_ = reader.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("another emitter does not read the option, and is not refused for it", func(t *testing.T) {
		// Only the typed compiler promises to honour or refuse the option; the reader
		// does not refuse for a compiler that never read it.
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery("SELECT `Total` FROM `Album`").WillReturnRows(sqlmock.NewRows([]string{"Total"}))
		q := typedMoneyQuery{StructuredQuery: albums().SelectColumns(typedTestColumn(typedTestField("Total"), "")), money: money}
		reader, err := getRecordsReaderWithOptions(ctx, q, db.QueryContext, DbOptions{StructuredQueryDialect: "sqlite"})
		if err != nil {
			t.Fatal(err)
		}
		_ = reader.Close()
	})
}
