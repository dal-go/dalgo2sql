package dalgo2sql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// errStreamBroke is the server error that ends a result in the middle of its stream: a
// timeout, a cancelled statement, or an overflow at one row.
var errStreamBroke = errors.New("server error at row 2")

// streamRows is a result of three rows whose third (index 2) fails: the driver delivers
// two rows and then the error, so database/sql reports the end of the stream with the
// error in Rows.Err and never with io.EOF. The cells are bytes because sqlmock reports no
// column type, which the recordset reader reads as a byte column.
func streamRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "title"}).
		AddRow([]byte("1"), []byte("One")).
		AddRow([]byte("2"), []byte("Two")).
		AddRow([]byte("3"), []byte("Three")).
		RowError(2, errStreamBroke)
}

// streamReads runs one read of every dialect against a server that fails at row 2, and
// hands each reader to check. A reader is a records reader and a recordset reader, so
// every reader this package builds is held to the same rule: a stream error is never the
// end of a result.
func streamReads(t *testing.T, check func(t *testing.T, next func() error)) {
	t.Helper()
	ctx := context.Background()
	text := dal.NewTextQuery("SELECT id, title FROM album", nil)
	structured := typedTestFrom("album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("id"), ""), typedTestColumn(typedTestField("title"), ""))
	for _, tc := range []struct {
		name    string
		dialect string
		query   dal.Query
		expect  func(mock sqlmock.Sqlmock)
	}{
		{"no dialect, a text query", "", text, func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT id, title FROM album").WillReturnRows(streamRows())
		}},
		{"no dialect, a structured query", "", structured, func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT id, title\nFROM album").WillReturnRows(streamRows())
		}},
		{"sqlite", "sqlite", structured, func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("SELECT `id`, `title` FROM `album`").WillReturnRows(streamRows())
		}},
		{"postgres", "postgres", structured, func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
				AddRow(`"album"`, "id", "integer", "N", int64(23), int64(0), true, false).
				AddRow(`"album"`, "title", "text", "S", int64(25), int64(0), false, false))
			mock.ExpectQuery(`SELECT "id", "title" FROM "album"`).WillReturnRows(streamRows())
		}},
	} {
		for _, reader := range []string{"records", "recordset"} {
			t.Run(tc.name+", "+reader+" reader", func(t *testing.T) {
				db, mock := newPostgresReadMock(t)
				tc.expect(mock)
				options := DbOptions{StructuredQueryDialect: tc.dialect}
				var next func() error
				switch reader {
				case "records":
					rr, err := getRecordsReaderWithOptions(ctx, tc.query, db.QueryContext, options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = rr.Close() })
					next = func() error { _, err := rr.Next(); return err }
				default:
					rr, err := getRecordsetReaderWithOptions(ctx, tc.query, db.QueryContext, options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = rr.Close() })
					next = func() error { _, _, err := rr.Next(); return err }
				}
				check(t, next)
			})
		}
	}
}

func TestReadersReturnTheStreamErrorNotTheEndOfTheResult(t *testing.T) {
	streamReads(t, func(t *testing.T, next func() error) {
		for row := 0; row < 2; row++ {
			if err := next(); err != nil {
				t.Fatalf("row %d: Next() error = %v, want the row", row, err)
			}
		}
		err := next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			t.Fatal("Next() reported the end of the result where the server failed: two rows would be taken for the whole result")
		}
		if !errors.Is(err, errStreamBroke) {
			t.Fatalf("Next() error = %v, want the server's error", err)
		}
		// The error stays: asking again does not turn it into a clean end.
		if err := next(); !errors.Is(err, errStreamBroke) {
			t.Fatalf("a second Next() error = %v, want the server's error again", err)
		}
	})
}
