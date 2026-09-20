package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func aggregationQuery() dal.StructuredQuery {
	count := dal.Count()
	count.Alias = "orders"
	return dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().
		WhereField("paid", dal.Equal, 1).
		GroupBy(dal.Field("country")).
		Having(dal.NewComparison(dal.Field("revenue"), dal.GreaterThen, dal.NewConstant(10))).
		OrderBy(dal.Descending(dal.Field("revenue"))).
		Limit(2).
		SelectColumns(
			dal.Column{Expression: dal.Field("country")},
			count,
			dal.CountDistinctAs(dal.Field("customer_id"), "customers"),
			dal.SumAs(dal.Binary(dal.Field("quantity"), dal.Multiply, dal.Field("unit_price")), "revenue"),
			dal.SumDistinctAs(dal.Field("unit_price"), "distinct_price_sum"),
			dal.AverageDistinctAs(dal.Field("unit_price"), "avg_price"),
			dal.MinAs(dal.Field("unit_price"), "minimum_price"),
			dal.MaxAs(dal.Field("unit_price"), "maximum_price"),
		)
}

func TestCompileStructuredSQLAggregation(t *testing.T) {
	text, args, err := compileStructuredSQL(aggregationQuery())
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"COUNT(*) AS `orders`",
		"COUNT(DISTINCT ((CASE WHEN typeof(`customer_id`)",
		"CASE WHEN typeof(`quantity`)",
		"AS REAL) AS `revenue`",
		"SUM(DISTINCT CASE WHEN typeof(`unit_price`)",
		"AS `distinct_price_sum`",
		"AVG(DISTINCT CASE WHEN typeof(`unit_price`)",
		"AS `avg_price`",
		"MIN(((CASE WHEN typeof(`unit_price`)",
		"MAX(((CASE WHEN typeof(`unit_price`)",
		"GROUP BY ((CASE WHEN typeof(`country`)",
		"HAVING",
		"ORDER BY (",
		"COLLATE BINARY) DESC",
		"LIMIT ?",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("SQL missing %q:\n%s", fragment, text)
		}
	}
	if !reflect.DeepEqual(args, []any{float64(1), float64(10), 2}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestCompileParameterizedAggregateBindsEveryOccurrence(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().SelectColumns(
		dal.SumAs(dal.Binary(dal.Field("quantity"), dal.Multiply, dal.NewConstant(2)), "total"),
	)
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if placeholders := strings.Count(text, "?"); placeholders != len(args) {
		t.Fatalf("SQL has %d placeholders and %d args:\n%s", placeholders, len(args), text)
	}
	for _, arg := range args {
		if arg != 2 {
			t.Fatalf("args = %#v", args)
		}
	}
}

func TestSQLiteParameterizedAggregateAcrossResultStages(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	}()
	if _, err := raw.Exec(`CREATE TABLE items (category TEXT, quantity INTEGER);
		INSERT INTO items VALUES ('A', 1), ('A', 2);`); err != nil {
		t.Fatal(err)
	}
	expression := dal.NewAggregate(dal.SUM, false, dal.Binary(dal.Field("quantity"), dal.Multiply, dal.NewConstant(2)))
	q := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
		GroupBy(dal.Field("category")).
		Having(dal.NewComparison(expression, dal.GreaterThen, dal.NewConstant(3))).
		OrderBy(dal.Descending(expression)).
		SelectColumns(dal.Column{Expression: dal.Field("category")}, dal.Column{Expression: expression, Alias: "total"})
	db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close aggregation reader: %v", err)
		}
	}()
	rec, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Data().(map[string]any)["total"]; got != float64(6) {
		t.Fatalf("total = %#v", got)
	}
}

func TestCompileAggregateComparisonBindsEveryOccurrence(t *testing.T) {
	left := dal.NewAggregate(dal.SUM, false, dal.Binary(dal.Field("x"), dal.Multiply, dal.NewConstant(2)))
	right := dal.NewAggregate(dal.AVERAGE, false, dal.Binary(dal.Field("y"), dal.Multiply, dal.NewConstant(3)))
	q := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
		Having(dal.NewComparison(left, dal.GreaterThen, right)).
		SelectColumns(dal.Column{Expression: left, Alias: "total"})
	text, args, err := compileStructuredSQL(q)
	if err != nil {
		t.Fatal(err)
	}
	if placeholders := strings.Count(text, "?"); placeholders != len(args) {
		t.Fatalf("SQL has %d placeholders and %d args:\n%s\n%#v", placeholders, len(args), text, args)
	}
}

func TestSQLiteNativeAggregation(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	}()
	_, err = raw.Exec(`CREATE TABLE orders (
        id TEXT PRIMARY KEY, country TEXT, customer_id TEXT,
        quantity INTEGER, unit_price REAL, paid INTEGER
    );
    INSERT INTO orders VALUES
        ('1','IE','a',2,10,1),
        ('2','IE','a',1,20,1),
        ('3','US','b',1,5,1),
        ('4','US',NULL,1,NULL,1),
        ('5','IE','c',50,10,0);`)
	if err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(raw, newSchema(), DbOptions{
		StructuredQueryDialect: "sqlite",
		Recordsets:             map[string]*Recordset{"orders": NewRecordset("orders", Table, []dal.FieldRef{dal.Field("id")})},
	})
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), aggregationQuery())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close aggregation reader: %v", err)
		}
	}()
	record, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	row := record.Data().(map[string]any)
	if row["country"] != "IE" || row["orders"] != int64(2) || row["customers"] != int64(1) || row["revenue"] != float64(40) || row["distinct_price_sum"] != float64(30) || row["avg_price"] != float64(15) || row["minimum_price"] != float64(10) || row["maximum_price"] != float64(20) {
		t.Fatalf("row = %#v", row)
	}
	if _, err := reader.Next(); err != dal.ErrNoMoreRecords {
		t.Fatalf("next err = %v", err)
	}
}

func TestSQLiteUngroupedEmptyAggregation(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	}()
	if _, err := raw.Exec(`CREATE TABLE empty_orders (id TEXT PRIMARY KEY, amount REAL)`); err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	count := dal.Count()
	count.Alias = "rows"
	q := dal.From(dal.NewRootCollectionRef("empty_orders", "")).NewQuery().
		SelectColumns(count, dal.SumAs(dal.Field("amount"), "sum"))
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close aggregation reader: %v", err)
		}
	}()
	rec, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	row := rec.Data().(map[string]any)
	if row["rows"] != int64(0) || row["sum"] != nil {
		t.Fatalf("row = %#v", row)
	}
}

func TestSQLiteCapabilitiesKeepFirstLastLocal(t *testing.T) {
	capabilities := (&database{options: DbOptions{StructuredQueryDialect: "sqlite"}}).QueryCapabilities()
	if !capabilities.GroupBy || !capabilities.Aggregate.Count || !capabilities.Aggregate.SumDistinct {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	if capabilities.Aggregate.First || capabilities.Aggregate.Last {
		t.Fatalf("FIRST/LAST must not be advertised without aggregate ordering")
	}
}

func TestSQLiteAggregationRejectsNonFiniteResult(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	}()
	if _, err := raw.Exec(`CREATE TABLE values_to_sum (amount REAL);
		INSERT INTO values_to_sum VALUES (1e308), (1e308);`); err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	q := dal.From(dal.NewRootCollectionRef("values_to_sum", "")).NewQuery().
		SelectColumns(dal.SumAs(dal.Field("amount"), "total"))

	records, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := records.Next(); err == nil || !strings.Contains(err.Error(), "non-finite aggregate result") {
		t.Fatalf("records error = %v", err)
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}

	recordsetReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := recordsetReader.Close(); err != nil {
			t.Errorf("close aggregation recordset reader: %v", err)
		}
	}()
	if _, _, err := recordsetReader.Next(); err == nil || !strings.Contains(err.Error(), "non-finite aggregate result") {
		t.Fatalf("recordset error = %v", err)
	}
}
