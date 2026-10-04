package dalgo2sql

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func TestIsKeysOnlyQuery(t *testing.T) {
	builder := dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery()
	into := func() dalrecord.Record {
		return dalrecord.NewRecordWithIncompleteKey("cities", reflect.String, &scanCity{})
	}
	tests := []struct {
		name  string
		query dal.StructuredQuery
		want  bool
	}{
		{"keys only", builder.SelectKeysOnly(reflect.String), true},
		{"into record", builder.SelectIntoRecord(into), false},
		{"columns", builder.SelectColumns(dal.Column{Expression: dal.Field("Name")}), false},
		{"into recordset", builder.SelectIntoRecordset(), false},
	}
	for _, tt := range tests {
		if got := isKeysOnlyQuery(tt.query); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestOrderedByKey(t *testing.T) {
	q := orderedByKey{StructuredQuery: dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectKeysOnly(reflect.String), key: "ID"}
	orders := q.OrderBy()
	if len(orders) != 1 || !reflect.DeepEqual(orders[0], dal.AscendingField("ID")) {
		t.Errorf("got %v", orders)
	}
}

func TestRecordsReader_KeysOnlyQueryIsOrderedByPrimaryKey(t *testing.T) {
	cities := dal.NewRootCollectionRef("cities", "")
	keysOnly := dal.From(cities).NewQuery().SelectKeysOnly(reflect.String)
	byPopulation := dal.From(cities).NewQuery().OrderBy(dal.DescendingField("Population")).SelectKeysOnly(reflect.String)
	withID := DbOptions{Recordsets: map[string]*Recordset{
		"cities": NewRecordset("cities", Table, []dal.FieldRef{dal.Field("ID")}),
	}}
	tests := []struct {
		name    string
		query   dal.Query
		options DbOptions
		legacy  string // regexp for the legacy emitter
		sqlite  string // regexp for the SQLite compiler
	}{
		{"primary key known", keysOnly, withID,
			"^SELECT \\* FROM cities ORDER BY ID$", "^SELECT \\* FROM `cities` ORDER BY \\(`ID` COLLATE BINARY\\)$"},
		{"primary key from DbOptions.PrimaryKey", keysOnly, DbOptions{PrimaryKey: []string{"ID"}},
			"^SELECT \\* FROM cities ORDER BY ID$", "^SELECT \\* FROM `cities` ORDER BY \\(`ID` COLLATE BINARY\\)$"},
		{"explicit order wins", byPopulation, withID,
			"^SELECT \\* FROM cities ORDER BY Population DESC$", "^SELECT \\* FROM `cities` ORDER BY \\(`Population` COLLATE BINARY\\) DESC$"},
		{"primary key unknown", keysOnly, DbOptions{},
			"^SELECT \\* FROM cities$", "^SELECT \\* FROM `cities`$"},
	}
	for _, tt := range tests {
		for dialect, pattern := range map[string]string{"": tt.legacy, "sqlite": tt.sqlite} {
			t.Run(tt.name+"/"+dialect, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
				if err != nil {
					t.Fatal(err)
				}
				defer closeDatabase(t, db)
				mock.ExpectQuery(pattern).WillReturnRows(sqlmock.NewRows([]string{"ID"}))
				options := tt.options
				options.StructuredQueryDialect = dialect
				rr, err := getRecordsReaderWithOptions(context.Background(), tt.query, db.QueryContext, options)
				if err != nil {
					t.Fatal(err)
				}
				_ = rr.Close()
				if err = mock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

// executedSQL returns the statement the reader sends for query.
func executedSQL(t *testing.T, query dal.Query, options DbOptions) string {
	t.Helper()
	var executed string
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
		executed = actual
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(t, db)
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"ID"}))
	rr, err := getRecordsReaderWithOptions(context.Background(), query, db.QueryContext, options)
	if err != nil {
		t.Fatalf("reader must open: %v", err)
	}
	_ = rr.Close()
	return executed
}

// A keys-only JOIN query keeps the statement it had before keys-only queries
// were ordered: an unqualified ORDER BY on the primary key is refused by the
// SQLite compiler for a JOIN and ambiguous on PostgreSQL and MySQL.
func TestRecordsReader_KeysOnlyJoinQueryIsNotOrdered(t *testing.T) {
	cities := dal.From(dal.NewRootCollectionRef("cities", "cities")).Join(
		dal.NewJoinedFrom(dal.From(dal.NewRootCollectionRef("countries", "c")), dal.JoinInner,
			sqlJoin("cities", "CountryID", "c", "ID")),
	).NewQuery().SelectKeysOnly(reflect.String)
	withID := DbOptions{PrimaryKey: []string{"ID"}}
	for _, dialect := range []string{"", "sqlite"} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			withID.StructuredQueryDialect = dialect
			unordered := DbOptions{StructuredQueryDialect: dialect}
			got := executedSQL(t, cities, withID)
			if want := executedSQL(t, cities, unordered); got != want || strings.Contains(got, "ORDER BY") {
				t.Errorf("JOIN statement changed:\n got %s\nwant %s", got, want)
			}
		})
	}
}

func TestOrderedByKey_StringRendersTheOrder(t *testing.T) {
	q := orderedByKey{StructuredQuery: dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectKeysOnly(reflect.String), key: "ID"}
	if got := q.String(); !strings.HasSuffix(got, "ORDER BY ID") {
		t.Errorf("String() must render the wrapper's own order, got %q", got)
	}
}

// The legacy text emitter refuses names that are not plain identifiers, so a
// primary key spelled that way keeps the unordered statement it always had;
// the SQLite compiler quotes the name and orders.
func TestRecordsReader_KeysOnlyOrderWithUnusualPrimaryKeyName(t *testing.T) {
	keysOnly := dal.From(dal.NewRootCollectionRef("cities", "")).NewQuery().SelectKeysOnly(reflect.String)
	for _, key := range []string{"my id", "a`b"} {
		legacy := executedSQL(t, keysOnly, DbOptions{PrimaryKey: []string{key}})
		if legacy != "SELECT * FROM cities" {
			t.Errorf("legacy emitter, key %q: got %q", key, legacy)
		}
		quoted := executedSQL(t, keysOnly, DbOptions{PrimaryKey: []string{key}, StructuredQueryDialect: "sqlite"})
		if !strings.Contains(quoted, "ORDER BY") || strings.Contains(quoted, "ORDER BY "+key) {
			t.Errorf("sqlite compiler, key %q must quote the name, got %q", key, quoted)
		}
	}
}
