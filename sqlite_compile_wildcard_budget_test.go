package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestSQLiteCompileBudgetWildcardMetadataAndRewriteRefuseBeforeData(t *testing.T) {
	for _, nameBytes := range []int{9000, 2000} {
		t.Run(fmt.Sprint(nameBytes), func(t *testing.T) {
			columns := make([]string, 128)
			for i := range columns {
				columns[i] = fmt.Sprintf("`c%d_%s` INTEGER", i, strings.Repeat("n", nameBytes))
			}
			db := openTestSQLiteDB(t, "CREATE TABLE Numbers("+strings.Join(columns, ",")+")")
			query := dal.From(dal.NewRootCollectionRef("Numbers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("excluded"))
			metadata, data, empty := 0, 0, 0
			execute := func(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
				switch {
				case text == "":
					empty++
					return nil, errors.New("empty SQL reached native dispatcher")
				case strings.Contains(text, "pragma_table_xinfo"):
					metadata++
				default:
					data++
				}
				return db.QueryContext(ctx, text, args...)
			}
			reader, err := getRecordsetReaderWithOptions(context.Background(), query, execute, DbOptions{StructuredQueryDialect: "sqlite", PreserveNativeValues: true, SQLiteStrictNumbers: true})
			if reader != nil || !errors.Is(err, errSQLiteCompileBudget) || metadata != 1 || data != 0 || empty != 0 {
				t.Fatalf("metadata expansion escaped: metadata%d,data%d,empty%d,%v", metadata, data, empty, err)
			}
			t.Logf("metadata=%d data=%d emptySQL=%d refused=%v", metadata, data, empty, err != nil)
		})
	}
	// Both original and rewritten queries are individually supported. Reusing
	// the original compilation's budget refuses their combined construction.
	db := openTestSQLiteDB(t, "CREATE TABLE Numbers(value INTEGER, secret TEXT)")
	medium := budgetArithmetic(2, int64(1))
	query := dal.From(dal.NewRootCollectionRef("Numbers", "")).NewQuery().SelectColumns(
		dal.AllColumnsExcept("secret"), dal.Column{Expression: medium, Alias: "first"}, dal.Column{Expression: medium, Alias: "second"})
	metadata, data := 0, 0
	execute := func(ctx context.Context, text string, args ...any) (*sql.Rows, error) {
		if strings.Contains(text, "pragma_table_xinfo") {
			metadata++
		} else {
			data++
		}
		return db.QueryContext(ctx, text, args...)
	}
	reader, err := getRecordsetReaderWithOptions(context.Background(), query, execute, DbOptions{StructuredQueryDialect: "sqlite", PreserveNativeValues: true, SQLiteStrictNumbers: true})
	if reader != nil || !errors.Is(err, errSQLiteCompileBudget) || metadata != 1 || data != 0 || !strings.Contains(err.Error(), "expanded SQLite wildcard") {
		t.Fatalf("rewritten compilation error lost: metadata%d,data%d,%v", metadata, data, err)
	}
}

func TestSQLiteCompileBudgetWildcardKeepsSupportedResultAndDefault(t *testing.T) {
	db := openTestSQLiteDB(t, "CREATE TABLE Numbers(value INTEGER, secret TEXT)")
	if _, err := db.Exec("INSERT INTO Numbers VALUES(7,'private')"); err != nil {
		t.Fatal(err)
	}
	query := dal.From(dal.NewRootCollectionRef("Numbers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("secret"))
	for _, strict := range []bool{false, true} {
		reader, err := getReaderBaseWithOptions(context.Background(), query, db.QueryContext, DbOptions{StructuredQueryDialect: "sqlite", SQLiteStrictNumbers: strict})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(reader.colNames, []string{"value"}) || !reader.rows.Next() {
			t.Fatal("supported wildcard lost result", strict, reader.colNames)
		}
		var value int64
		if err := reader.rows.Scan(&value); err != nil || value != 7 {
			t.Fatal("wildcard value changed", value, err)
		}
		if err := reader.rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteCompileBudgetCatalogAndExpansionShareExhaustion(t *testing.T) {
	db := openTestSQLiteDB(t, "CREATE TABLE Numbers(value INTEGER, secret TEXT)")
	query := dal.From(dal.NewRootCollectionRef("Numbers", "")).NewQuery().SelectColumns(dal.AllColumnsExcept("secret"))
	for _, exhaust := range []func(*sqliteCompileBudget){
		func(b *sqliteCompileBudget) { b.bytes = sqliteCompileBytes },
		func(b *sqliteCompileBudget) { b.values = sqliteCompileBytes },
		func(b *sqliteCompileBudget) { b.nodes = sqliteCompileNodes },
		func(b *sqliteCompileBudget) { b.inputs = sqliteCompileBytes },
		func(b *sqliteCompileBudget) { b.slots = sqliteCompileSlots },
	} {
		emitter := newSQLiteEmitter(true)
		exhaust(emitter.budget)
		if names, err := emitter.sourceColumns(context.Background(), query, db.QueryContext); names != nil || !errors.Is(err, errSQLiteCompileBudget) {
			t.Fatal("metadata staging ignored prior work", names, err)
		}
	}
	plan, err := planWildcardProjection(query)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		names []string
		alter func(*sqliteCompileBudget, *wildcardProjectionPlan)
	}{
		{[]string{"value"}, func(b *sqliteCompileBudget, _ *wildcardProjectionPlan) { b.slots = sqliteCompileSlots }},
		{[]string{"value"}, func(_ *sqliteCompileBudget, p *wildcardProjectionPlan) { p.explicitCount = -1 }},
		{make([]string, sqliteCompileSlots+1), func(_ *sqliteCompileBudget, _ *wildcardProjectionPlan) {}},
		{[]string{"value"}, func(_ *sqliteCompileBudget, p *wildcardProjectionPlan) { p.explicitCount = sqliteCompileSlots }},
		{[]string{"value"}, func(b *sqliteCompileBudget, _ *wildcardProjectionPlan) { b.nodes = sqliteCompileNodes }},
		{[]string{"value"}, func(b *sqliteCompileBudget, _ *wildcardProjectionPlan) { b.slots = sqliteCompileSlots - 1 }},
		{[]string{strings.Repeat("x", sqliteCompileBytes/6+1)}, func(_ *sqliteCompileBudget, _ *wildcardProjectionPlan) {}},
		{[]string{"value"}, func(b *sqliteCompileBudget, _ *wildcardProjectionPlan) { b.bytes = sqliteCompileBytes }},
	} {
		emitter, local := newSQLiteEmitter(true), *plan
		fixture.alter(emitter.budget, &local)
		if expanded, ok, err := emitter.expandWildcard(local, query, fixture.names); expanded != nil || ok || !errors.Is(err, errSQLiteCompileBudget) {
			t.Fatal("expansion allocated after exhausted work", ok, err)
		}
	}
}
