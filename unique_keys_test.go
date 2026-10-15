package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	dalrecord "github.com/dal-go/record"
)

// The rule every read keeps: within one read, no two records have the same key (a record
// with no key, which dalgotest.HasNoKey says, may repeat). The tests here hold every read
// path of this package to it with dalgotest.AssertUniqueKeys over fixtures a constant key
// cannot pass by accident: several rows, repeated non-key values, and one table whose
// rows are all alike.
//
// A new provider is one more row of uniqueKeysProviders, and a new read path one more row
// of its reads.

// invoiceFixture is a table like Chinook's Invoice: a single-column integer primary key,
// hundreds of rows, and columns whose values repeat (the regression of the constant key
// "__dalgo_record_id" on every row of such a table).
const invoiceRows = 412

const uniqueKeysScript = `
CREATE TABLE Invoice (
	InvoiceId      INTEGER NOT NULL PRIMARY KEY,
	CustomerId     INTEGER NOT NULL,
	BillingCountry TEXT,
	Total          NUMERIC
);
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 412)
INSERT INTO Invoice SELECT i, 1 + (i % 59), CASE i % 4 WHEN 0 THEN 'USA' WHEN 1 THEN 'France' ELSE 'Brazil' END, 0.99 + (i % 3) FROM n;

CREATE TABLE Customer (
	CustomerId INTEGER NOT NULL PRIMARY KEY,
	Country    TEXT
);
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 59)
INSERT INTO Customer SELECT i, CASE i % 2 WHEN 0 THEN 'USA' ELSE 'France' END FROM n;

CREATE TABLE CompositePk (Region TEXT NOT NULL, Code INTEGER NOT NULL, Label TEXT, PRIMARY KEY (Region, Code));
INSERT INTO CompositePk VALUES ('eu', 1, 'same'), ('eu', 2, 'same'), ('us', 1, 'same'), ('us', 2, 'same');

CREATE TABLE NoPk (Name TEXT, Qty INTEGER);
INSERT INTO NoPk VALUES ('bolt', 10), ('bolt', 10), ('nut', 10), ('bolt', 20), ('bolt', 10);

CREATE VIEW InvoiceView AS SELECT InvoiceId, BillingCountry FROM Invoice;

CREATE TABLE "Order Details" (OrderID INTEGER PRIMARY KEY, ProductID INTEGER, Quantity INTEGER);
INSERT INTO "Order Details" VALUES (10248, 11, 12), (10249, 11, 12), (10250, 41, 10), (10251, 11, 12);

CREATE TABLE NullablePk (Code TEXT PRIMARY KEY, Label TEXT);
INSERT INTO NullablePk VALUES (NULL, 'a'), (NULL, 'b'), ('x', 'c'), (NULL, 'd');

CREATE TABLE UntypedPk (K PRIMARY KEY, Label TEXT);
INSERT INTO UntypedPk VALUES (1, 'int'), ('1', 'text'), (x'31', 'blob');

CREATE TABLE DescPk (Id INTEGER PRIMARY KEY DESC, Label TEXT);
INSERT INTO DescPk VALUES (NULL, 'a'), (NULL, 'b'), (1, 'c');

CREATE TABLE WithoutRowidPk (Id INTEGER PRIMARY KEY, Label TEXT) WITHOUT ROWID;
INSERT INTO WithoutRowidPk VALUES (3, 'same'), (4, 'same');

CREATE TABLE BlobPk (K TEXT NOT NULL PRIMARY KEY, Label TEXT);
INSERT INTO BlobPk VALUES ('ab', 'text'), (x'6162', 'blob'), ('cd', 'text');

CREATE TABLE BlobInIntPk (K INT NOT NULL PRIMARY KEY, Label TEXT);
INSERT INTO BlobInIntPk VALUES (1, 'int'), (x'31', 'blob');

CREATE TABLE TextPk (Slug TEXT NOT NULL PRIMARY KEY, Label TEXT);
INSERT INTO TextPk VALUES ('a', 'same'), ('b', 'same'), ('c', 'same');
`

// uniqueKeysRead is one read path: a query and the number of records it returns.
type uniqueKeysRead struct {
	name  string
	query dal.Query
	rows  int
	// keys, when set, are the keys the records must carry, in order: the keys a source with
	// a one-column primary key is read by, and the ordinals of every other.
	keys func(t *testing.T, got []dalrecord.Record)
}

// uniqueKeysProvider is one way of opening a database whose reads are held to the rule.
type uniqueKeysProvider struct {
	name  string
	open  func(t *testing.T) dal.DB
	reads []uniqueKeysRead
}

func openUniqueKeysDB(t *testing.T, options DbOptions) dal.DB {
	t.Helper()
	return NewDatabase(openNullTestDB(t, uniqueKeysScript), newSchema(), options)
}

func ukFrom(name, alias string) dal.FromSource { return typedTestFrom(name, alias) }

func ukField(name string) dal.Column { return typedTestColumn(typedTestField(name), "") }

func ukColumns(source string, names ...string) []dal.Column {
	columns := make([]dal.Column, len(names))
	for i, name := range names {
		columns[i] = typedTestColumn(typedTestField(name), "")
	}
	return columns
}

// ordinalKeys checks the records are keyed 0, 1, 2, ... in order.
func ordinalKeys(t *testing.T, got []dalrecord.Record) {
	t.Helper()
	for i, r := range got {
		if want := fmt.Sprint(i); r.Key().ID != want {
			t.Fatalf("record %d has the key %v, want the ordinal %q", i, r.Key().ID, want)
		}
	}
}

// keysFrom checks the records are keyed by the first column the data lists as id.
func keyedBy(column string) func(t *testing.T, got []dalrecord.Record) {
	return func(t *testing.T, got []dalrecord.Record) {
		t.Helper()
		for i, r := range got {
			if want := r.Data().(map[string]any)[column]; r.Key().ID != want {
				t.Fatalf("record %d has the key %v, want the value of %s, %v", i, r.Key().ID, column, want)
			}
		}
	}
}

// keyedByHidden checks the records are keyed by a primary key the select list does not
// carry, so that the key is read from a helper column that is not in the data.
func keyedByHidden(want func(i int) any) func(t *testing.T, got []dalrecord.Record) {
	return func(t *testing.T, got []dalrecord.Record) {
		t.Helper()
		for i, r := range got {
			if w := want(i); r.Key().ID != w {
				t.Fatalf("record %d has the key %v, want %v", i, r.Key().ID, w)
			}
			if _, ok := r.Data().(map[string]any)[recordIDHelperColumn]; ok {
				t.Fatalf("record %d exposes the helper column in its data: %v", i, r.Data())
			}
		}
	}
}

func selectAll(name string) dal.Query { return ukFrom(name, "").NewQuery().SelectColumns() }

func selectColumns(name string, columns ...string) dal.Query {
	return ukFrom(name, "").NewQuery().SelectColumns(ukColumns(name, columns...)...)
}

// sqliteReads are the reads every SQLite provider below runs.
func sqliteReads() []uniqueKeysRead {
	groupBy := ukFrom("Invoice", "").NewQuery().GroupBy(typedTestField("BillingCountry")).
		SelectColumns(ukField("BillingCountry"), dal.Count())
	count := ukFrom("Invoice", "").NewQuery().SelectColumns(dal.Count())
	oneToMany := dal.From(dal.NewRootCollectionRef("Customer", "c")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Invoice", "i"), dal.JoinInner, typedTestJoinOn("c", "CustomerId", "i", "CustomerId")),
	).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("c", "Country"), ""), typedTestColumn(typedTestQualified("i", "Total"), ""))
	manyToOne := dal.From(dal.NewRootCollectionRef("Invoice", "i")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Customer", "c"), dal.JoinInner, typedTestJoinOn("i", "CustomerId", "c", "CustomerId")),
	).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("i", "Total"), ""), typedTestColumn(typedTestQualified("c", "Country"), ""))
	return []uniqueKeysRead{
		{"the Invoice regression: a select-all of a single-column integer primary key", selectAll("Invoice"), invoiceRows, nil},
		{"select columns without the key", selectColumns("Invoice", "BillingCountry", "Total"), invoiceRows, nil},
		{"select columns with the key", selectColumns("Invoice", "InvoiceId", "BillingCountry"), invoiceRows, nil},
		{"a text primary key", selectAll("TextPk"), 3, nil},
		{"a composite primary key, select-all", selectAll("CompositePk"), 4, nil},
		{"a composite primary key, select columns", selectColumns("CompositePk", "Label"), 4, nil},
		{"a table with no primary key and rows that are all alike", selectAll("NoPk"), 5, nil},
		{"a table with no primary key, select columns", selectColumns("NoPk", "Name"), 5, nil},
		{"a view", selectAll("InvoiceView"), invoiceRows, nil},
		{"a table whose name needs quoting", selectAll("Order Details"), 4, nil},
		{"a nullable primary key that holds NULL", selectAll("NullablePk"), 4, nil},
		{"a primary key with no type, holding 1 and '1'", selectAll("UntypedPk"), 3, nil},
		{"INTEGER PRIMARY KEY DESC, which accepts NULL", selectAll("DescPk"), 3, nil},
		{"a WITHOUT ROWID table", selectAll("WithoutRowidPk"), 2, nil},
		{"a text key holding a BLOB and the text of the same bytes", selectAll("BlobPk"), 3, nil},
		{"an integer key holding a BLOB and the integer of the same text", selectAll("BlobInIntPk"), 2, nil},
		{"a GROUP BY", groupBy, 3, nil},
		{"an aggregate over the whole source", count, 1, nil},
		{"a one-to-many join", oneToMany, invoiceRows, nil},
		{"a many-to-one join", manyToOne, invoiceRows, nil},
	}
}

func uniqueKeysProviders() []uniqueKeysProvider {
	return []uniqueKeysProvider{
		{
			name: "SQLite dialect, no key declared",
			open: func(t *testing.T) dal.DB { return openUniqueKeysDB(t, DbOptions{StructuredQueryDialect: "sqlite"}) },
			reads: append(sqliteReads(),
				uniqueKeysRead{"a keys-only read", ukFrom("Invoice", "").NewQuery().SelectKeysOnly(reflect.Int64), invoiceRows, nil},
			),
		},
		{
			name: "SQLite dialect, DbOptions.PrimaryKey declared",
			open: func(t *testing.T) dal.DB {
				return openUniqueKeysDB(t, DbOptions{StructuredQueryDialect: "sqlite", PrimaryKey: []string{"InvoiceId"}})
			},
			reads: []uniqueKeysRead{
				{"select-all of the declared key", selectAll("Invoice"), invoiceRows, keyedBy("InvoiceId")},
				{"select columns without the declared key", selectColumns("Invoice", "BillingCountry"), invoiceRows,
					keyedByHidden(func(i int) any { return int64(i + 1) })},
			},
		},
		{
			name: "SQLite dialect, a recordset declared",
			open: func(t *testing.T) dal.DB {
				return openUniqueKeysDB(t, DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{
					"Invoice":       NewRecordset("Invoice", Table, []dal.FieldRef{dal.Field("InvoiceId")}),
					"CompositePk":   NewRecordset("CompositePk", Table, []dal.FieldRef{dal.Field("Region"), dal.Field("Code")}),
					"NoPk":          NewRecordset("NoPk", Table, nil),
					"Order Details": NewRecordset("Order Details", Table, []dal.FieldRef{dal.Field("OrderID")}),
				}})
			},
			reads: []uniqueKeysRead{
				{"select-all of the declared key", selectAll("Invoice"), invoiceRows, keyedBy("InvoiceId")},
				{"a declared key of a table whose name needs quoting", selectAll("Order Details"), 4, keyedBy("OrderID")},
				{"a declared composite key falls back to the ordinal", selectAll("CompositePk"), 4, ordinalKeys},
				{"a declared recordset with no key falls back to the ordinal", selectAll("NoPk"), 5, ordinalKeys},
			},
		},
		{
			name: "the legacy emitter",
			open: func(t *testing.T) dal.DB { return openUniqueKeysDB(t, DbOptions{}) },
			reads: []uniqueKeysRead{
				{"select-all of a single-column primary key", selectAll("Invoice"), invoiceRows, ordinalKeys},
				{"select columns", selectColumns("Invoice", "BillingCountry", "Total"), invoiceRows, ordinalKeys},
				{"a table with no primary key", selectAll("NoPk"), 5, ordinalKeys},
				{"a view", selectAll("InvoiceView"), invoiceRows, ordinalKeys},
				{"a GROUP BY", ukFrom("Invoice", "").NewQuery().GroupBy(typedTestField("BillingCountry")).
					SelectColumns(ukField("BillingCountry"), dal.Count()), 3, ordinalKeys},
			},
		},
	}
}

// readAllRecords reads every record of q.
func readUniqueKeysRecords(t *testing.T, db dal.DB, q dal.Query) []dalrecord.Record {
	t.Helper()
	ctx := context.Background()
	var records []dalrecord.Record
	err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		reader, err := tx.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = reader.Close() }()
		for {
			r, err := reader.Next()
			if errors.Is(err, dal.ErrNoMoreRecords) {
				return nil
			}
			if err != nil {
				return err
			}
			records = append(records, r)
		}
	})
	if err != nil {
		t.Fatalf("reading %v: %v", q, err)
	}
	return records
}

func TestEveryReadKeepsItsKeysUniqueWithinTheRead(t *testing.T) {
	for _, provider := range uniqueKeysProviders() {
		for _, read := range provider.reads {
			t.Run(provider.name+"/"+read.name, func(t *testing.T) {
				records := readUniqueKeysRecords(t, provider.open(t), read.query)
				if len(records) != read.rows {
					t.Fatalf("read %d records, want %d", len(records), read.rows)
				}
				dalgotest.AssertUniqueKeys(t, records)
				for i, r := range records {
					if dalgotest.HasNoKey(r.Key()) {
						t.Fatalf("record %d has no key: a read of a recordset keys every record", i)
					}
					if r.Key().ID == recordIDHelperColumn {
						t.Fatalf("record %d has the constant key %q", i, recordIDHelperColumn)
					}
				}
				if read.keys != nil {
					read.keys(t, records)
				}
			})
		}
	}
}

// A single-column primary key that SQLite lets no value of repeat or be absent keys the
// records by its own values, as text where the column holds text and as the integer where
// it holds integers; every other source is keyed by ordinal.
func TestTheSQLiteDialectKeysBySingleColumnPrimaryKeysAndByOrdinalOtherwise(t *testing.T) {
	db := openUniqueKeysDB(t, DbOptions{StructuredQueryDialect: "sqlite"})
	for _, tc := range []struct {
		name  string
		query dal.Query
		rows  int
		keys  func(t *testing.T, got []dalrecord.Record)
	}{
		{"a single-column integer primary key", selectAll("Invoice"), invoiceRows, keyedBy("InvoiceId")},
		{"a key the select list leaves out", selectColumns("Invoice", "BillingCountry"), invoiceRows,
			keyedByHidden(func(i int) any { return int64(i + 1) })},
		{"a text primary key", selectAll("TextPk"), 3, keyedBy("Slug")},
		{"a table whose name needs quoting", selectAll("Order Details"), 4, keyedBy("OrderID")},
		{"a composite primary key", selectAll("CompositePk"), 4, ordinalKeys},
		{"no primary key", selectAll("NoPk"), 5, ordinalKeys},
		{"a view", selectAll("InvoiceView"), invoiceRows, ordinalKeys},
		{"a nullable primary key", selectAll("NullablePk"), 4, ordinalKeys},
		{"a primary key with no type", selectAll("UntypedPk"), 3, ordinalKeys},
		{"INTEGER PRIMARY KEY DESC", selectAll("DescPk"), 3, ordinalKeys},
		{"a WITHOUT ROWID table, whose key is never NULL", selectAll("WithoutRowidPk"), 2, keyedBy("Id")},
		{"a text key that holds a BLOB", selectAll("BlobPk"), 3, ordinalKeys},
		{"an integer key that holds a BLOB", selectAll("BlobInIntPk"), 2, ordinalKeys},
		{"a GROUP BY", ukFrom("Invoice", "").NewQuery().GroupBy(typedTestField("BillingCountry")).
			SelectColumns(ukField("BillingCountry"), dal.Count()), 3, ordinalKeys},
		{"a join", dal.From(dal.NewRootCollectionRef("Customer", "c")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Invoice", "i"), dal.JoinInner, typedTestJoinOn("c", "CustomerId", "i", "CustomerId")),
		).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("c", "Country"), "")), invoiceRows, ordinalKeys},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := readUniqueKeysRecords(t, db, tc.query)
			if len(records) != tc.rows {
				t.Fatalf("read %d records, want %d", len(records), tc.rows)
			}
			tc.keys(t, records)
		})
	}
}

// The catalog is asked once per read, for a source nobody declared a key for, and not at all
// when a key is declared, the query reads into a record, groups, or joins.
func TestTheSQLiteDialectAsksItsCatalogOnlyForAnUndeclaredSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options DbOptions
		query   dal.StructuredQuery
		asked   bool
	}{
		{"an undeclared source", DbOptions{}, ukFrom("Invoice", "").NewQuery().SelectColumns(), true},
		{"a declared PrimaryKey", DbOptions{PrimaryKey: []string{"InvoiceId"}}, ukFrom("Invoice", "").NewQuery().SelectColumns(), false},
		{"a declared recordset", DbOptions{Recordsets: map[string]*Recordset{"Invoice": NewRecordset("Invoice", Table, []dal.FieldRef{dal.Field("InvoiceId")})}},
			ukFrom("Invoice", "").NewQuery().SelectColumns(), false},
		{"a GROUP BY", DbOptions{}, ukFrom("Invoice", "").NewQuery().GroupBy(typedTestField("BillingCountry")).SelectColumns(ukField("BillingCountry"), dal.Count()), false},
		{"a read into a record", DbOptions{}, ukFrom("Invoice", "").NewQuery().SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("Invoice", "made-by-the-caller"), map[string]any{})
		}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := tc.options
			options.StructuredQueryDialect = "sqlite"
			raw := openNullTestDB(t, uniqueKeysScript)
			var statements []string
			execute := func(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
				statements = append(statements, statement)
				return raw.QueryContext(ctx, statement, args...)
			}
			reader, err := getRecordsReaderWithOptions(context.Background(), tc.query, execute, options)
			if err != nil {
				t.Fatal(err)
			}
			_ = reader.Close()
			asked := len(statements) > 0 && statements[0] == `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?)`
			if asked != tc.asked {
				t.Fatalf("statements = %q, catalog asked = %v, want %v", statements, asked, tc.asked)
			}
		})
	}
}

// A source in an attached database is asked for its key there.
func TestTheSQLiteDialectAsksTheCatalogOfTheSourcesSchema(t *testing.T) {
	raw := openNullTestDB(t, `ATTACH DATABASE ':memory:' AS other;
CREATE TABLE other.Thing (ThingId INTEGER PRIMARY KEY, Label TEXT);
INSERT INTO other.Thing VALUES (7, 'same'), (9, 'same');
CREATE TABLE main.Thing (Label TEXT);
INSERT INTO main.Thing VALUES ('x');`)
	db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	q := dal.From(dal.NewQualifiedRootCollectionRef("other", "Thing", "")).NewQuery().SelectColumns(ukField("Label"))
	records := readUniqueKeysRecords(t, db, q)
	keyedByHidden(func(i int) any { return []int64{7, 9}[i] })(t, records)
	dalgotest.AssertUniqueKeys(t, records)
}

// A failed catalog lookup is the read's failure, naming the table.
func TestTheSQLiteDialectReportsAFailedCatalogLookup(t *testing.T) {
	failure := errors.New("the connection is gone")
	execute := func(context.Context, string, ...any) (*sql.Rows, error) { return nil, failure }
	_, err := getRecordsReaderWithOptions(context.Background(), selectAll("Invoice").(dal.StructuredQuery), execute, DbOptions{StructuredQueryDialect: "sqlite"})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v, want the lookup's", err)
	}
}

// The typed PostgreSQL path keys by the catalog's primary key when it is one column and by
// ordinal otherwise; its reads run against a scripted server, which answers with rows whose
// non-key values repeat.
func TestThePostgresDialectKeepsItsKeysUniqueWithinTheRead(t *testing.T) {
	titles := func(rows ...int64) [][]driver.Value {
		out := make([][]driver.Value, len(rows))
		for i, id := range rows {
			out[i] = []driver.Value{id, "Same title"}
		}
		return out
	}
	grouped := ukFrom("Album", "").NewQuery().GroupBy(typedTestField("Title")).
		SelectColumns(ukField("Title"), dal.Count())
	for _, tc := range []struct {
		name      string
		columns   []keyColumn
		query     dal.Query
		statement string
		names     []string
		rows      [][]driver.Value
		want      []any
	}{
		{"a single-column primary key", []keyColumn{{"AlbumId", true}, {"Title", false}}, selectAll("Album"),
			`SELECT * FROM "Album"`, []string{"AlbumId", "Title"}, titles(10, 11, 12, 13), []any{int64(10), int64(11), int64(12), int64(13)}},
		{"a composite primary key", []keyColumn{{"AlbumId", true}, {"Title", true}}, selectAll("Album"),
			`SELECT * FROM "Album"`, []string{"AlbumId", "Title"}, titles(1, 1, 2, 2), []any{"0", "1", "2", "3"}},
		{"no primary key, or a view", []keyColumn{{"AlbumId", false}, {"Title", false}}, selectAll("Album"),
			`SELECT * FROM "Album"`, []string{"AlbumId", "Title"}, titles(1, 1, 1), []any{"0", "1", "2"}},
		{"a GROUP BY", []keyColumn{{"AlbumId", true}, {"Title", false}}, grouped,
			`SELECT "Title", COUNT(*) AS "COUNT(*)" FROM "Album" GROUP BY "Title"`, []string{"Title", "COUNT(*)"},
			[][]driver.Value{{"A", int64(2)}, {"B", int64(2)}, {"C", int64(2)}}, []any{"0", "1", "2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Album"`).WillReturnRows(keyCatalogRows(`"Album"`, tc.columns...))
			result := sqlmock.NewRows(tc.names)
			for _, row := range tc.rows {
				result.AddRow(row...)
			}
			mock.ExpectQuery(tc.statement).WillReturnRows(result)
			reader, err := getRecordsReaderWithOptions(context.Background(), tc.query, db.QueryContext, DbOptions{StructuredQueryDialect: "postgres"})
			if err != nil {
				t.Fatal(err)
			}
			var records []dalrecord.Record
			for {
				r, err := reader.Next()
				if errors.Is(err, dal.ErrNoMoreRecords) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				records = append(records, r)
			}
			_ = reader.Close()
			dalgotest.AssertUniqueKeys(t, records)
			keys := make([]any, len(records))
			for i, r := range records {
				keys[i] = r.Key().ID
			}
			if !reflect.DeepEqual(keys, tc.want) {
				t.Fatalf("keys = %v, want %v", keys, tc.want)
			}
		})
	}
}

// A source that is not a table, a catalog answer that cannot be read and a stream that fails
// halfway are the read's failures, not a reason to key by anything else.
func TestTheSQLiteDialectReportsWhatItCannotReadOfItsCatalog(t *testing.T) {
	group := dal.From(dal.NewCollectionGroupRef("Invoice", "")).NewQuery().SelectColumns()
	table := selectAll("Invoice").(dal.StructuredQuery)
	failure := errors.New("disk I/O error")
	catalog := func(name, declared string, notNull, pk any) func(mock sqlmock.Sqlmock) {
		return func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(sqliteKeyCatalogStatement).WithArgs("Invoice").
				WillReturnRows(sqlmock.NewRows([]string{"name", "type", "notnull", "pk"}).AddRow(name, declared, notNull, pk))
		}
	}
	indexes := `SELECT COUNT(*) FROM pragma_index_list(?) WHERE origin = 'pk'`
	blobs := `SELECT COUNT(*) FROM (SELECT 1 FROM ` + "`Invoice`" + ` WHERE typeof(` + "`K`" + `) = 'blob' LIMIT 1)`
	for _, tc := range []struct {
		name   string
		query  dal.StructuredQuery
		script func(mock sqlmock.Sqlmock)
		want   string
	}{
		{"a source that is not a table", group, func(sqlmock.Sqlmock) {}, "no SQLite table metadata"},
		{"a pk that is not a number", table, catalog("InvoiceId", "INTEGER", 1, "first"), "scan SQLite table metadata"},
		{"a stream that fails", table, func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(sqliteKeyCatalogStatement).WithArgs("Invoice").WillReturnRows(
				sqlmock.NewRows([]string{"name", "type", "notnull", "pk"}).AddRow("InvoiceId", "INTEGER", 1, 1).RowError(0, failure))
		}, "disk I/O error"},
		{"an index lookup that fails", table, func(mock sqlmock.Sqlmock) {
			catalog("InvoiceId", "INTEGER", 1, 1)(mock)
			mock.ExpectQuery(indexes).WithArgs("Invoice").WillReturnError(failure)
		}, "inspect SQLite indexes"},
		{"an index count that cannot be read", table, func(mock sqlmock.Sqlmock) {
			catalog("InvoiceId", "INTEGER", 1, 1)(mock)
			mock.ExpectQuery(indexes).WithArgs("Invoice").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("many"))
		}, "inspect SQLite indexes"},
		{"a key column that holds a BLOB lookup that fails", table, func(mock sqlmock.Sqlmock) {
			catalog("K", "TEXT", 1, 1)(mock)
			mock.ExpectQuery(blobs).WillReturnError(failure)
		}, "inspect SQLite key column"},
		{"a table whose name cannot be quoted", selectAll("bad\x01table").(dal.StructuredQuery), func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(sqliteKeyCatalogStatement).WithArgs("bad\x01table").
				WillReturnRows(sqlmock.NewRows([]string{"name", "type", "notnull", "pk"}).AddRow("K", "TEXT", 1, 1))
		}, "bad"},
		{"a key column whose name cannot be quoted", table, catalog("bad\x01name", "TEXT", 1, 1), "name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			tc.script(mock)
			_, err := getRecordsReaderWithOptions(context.Background(), tc.query, db.QueryContext, DbOptions{StructuredQueryDialect: "sqlite"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one that says %q", err, tc.want)
			}
		})
	}
}
