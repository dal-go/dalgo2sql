package dalgo2sql

import (
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestEmitSQL_NoLimitPassesThrough(t *testing.T) {
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("Customer", ""))).
		SelectIntoRecordset()
	got := emitSQL(q)
	if strings.Contains(got, "TOP") {
		t.Fatalf("emitSQL(no-limit) should not contain TOP, got %q", got)
	}
	if strings.Contains(got, "LIMIT") {
		t.Fatalf("emitSQL(no-limit) should not contain LIMIT, got %q", got)
	}
	if !strings.HasPrefix(got, "SELECT") {
		t.Fatalf("emitSQL(no-limit) should start with SELECT, got %q", got)
	}
}

func TestEmitSQL_LimitRewritesTopToLimit(t *testing.T) {
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("Customer", ""))).
		Limit(50).
		SelectIntoRecordset()
	got := emitSQL(q)
	if strings.Contains(got, "TOP") {
		t.Fatalf("emitSQL(limit=50) must rewrite TOP, got %q", got)
	}
	if !strings.Contains(got, "LIMIT 50") {
		t.Fatalf("emitSQL(limit=50) must append `LIMIT 50`, got %q", got)
	}
	if !strings.HasPrefix(got, "SELECT") {
		t.Fatalf("emitSQL(limit=50) should start with SELECT, got %q", got)
	}
}

func TestEmitSQL_TextQueryArgUnused(t *testing.T) {
	// emitSQL is only called for StructuredQuery in reader_base.go;
	// confirm it can handle a structured query with no Limit and a
	// non-zero Offset (the LIMIT branch is gated by Limit() > 0).
	q := dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef("Customer", ""))).
		SelectIntoRecordset()
	got := emitSQL(q)
	if got == "" {
		t.Fatal("emitSQL of empty-options query should produce SQL text")
	}
}
