package dalgo2sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dal-go/dalgo/adapters/dalgo2memory"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type aggregationHashBackend struct{ dal.Backend }

func (aggregationHashBackend) QueryCapabilities() dal.QueryCapabilities {
	return dal.QueryCapabilities{}
}

func TestAggregationNativeStreamingHashParity(t *testing.T) {
	ctx := context.Background()
	const first = int64(9_007_199_254_740_992)
	const second = int64(9_007_199_254_740_993)
	rows := []map[string]any{
		{"bucket": first, "tag": "A", "amount": 1},
		{"bucket": second, "tag": "a", "amount": 2},
		{"bucket": int64(7), "tag": "z", "amount": 4},
	}
	query := dal.From(dal.NewRootCollectionRef("events", "")).NewQuery().
		GroupBy(dal.Field("bucket")).
		OrderBy(dal.Descending(dal.Field("bucket"))).
		SelectColumns(
			dal.Column{Expression: dal.Field("bucket")},
			dal.CountDistinctAs(dal.Field("tag"), "tags"),
			dal.MinAs(dal.Field("tag"), "first_tag"),
			dal.MaxAs(dal.Field("tag"), "last_tag"),
			dal.SumAs(dal.Field("amount"), "total"),
			dal.SumAs(dal.Binary(dal.Field("bucket"), dal.Subtract, dal.NewConstant(first)), "delta"),
			dal.SumAs(dal.Binary(dal.Field("amount"), dal.Divide, dal.NewConstant(0)), "division_by_zero"),
		)

	memory := dalgo2memory.New(dalgo2memory.SingleWriterProfile())
	writes, ok := dal.As[dal.WriteSession](memory)
	if !ok {
		t.Fatal("memory backend does not expose writes")
	}
	for i, row := range rows {
		if err := writes.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("events", string(rune('1'+i))), &row)); err != nil {
			t.Fatal(err)
		}
	}

	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TABLE events (bucket INTEGER, tag TEXT COLLATE NOCASE, amount INTEGER);
		INSERT INTO events VALUES
		(9007199254740992, 'A', 1),
		(9007199254740993, 'a', 2),
		(7, 'z', 4);`); err != nil {
		t.Fatal(err)
	}
	native := NewDatabase(raw, newSchema(), DbOptions{StructuredQueryDialect: "sqlite"})
	hash := dal.NewDB(aggregationHashBackend{Backend: dal.BackendOf(memory)})

	want := readAggregationRows(t, ctx, memory, query)
	if got := readAggregationRows(t, ctx, hash, query); !aggregationRowsEqual(want, got) {
		t.Fatalf("hash rows = %#v, streaming rows = %#v", got, want)
	}
	if got := readAggregationRows(t, ctx, native, query); !aggregationRowsEqual(want, got) {
		t.Fatalf("native rows = %#v, streaming rows = %#v", got, want)
	}
	if len(want) != 2 || want[0]["tags"] != int64(2) || want[0]["first_tag"] != "A" || want[0]["last_tag"] != "a" || want[0]["delta"] != float64(0) || want[0]["division_by_zero"] != nil {
		t.Fatalf("parity fixture did not exercise portable equality: %#v", want)
	}
}

func readAggregationRows(t *testing.T, ctx context.Context, db dal.DB, query dal.StructuredQuery) []map[string]any {
	t.Helper()
	reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var rows []map[string]any
	for {
		rec, err := reader.Next()
		if err == dal.ErrNoMoreRecords {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, rec.Data().(map[string]any))
	}
}

func aggregationRowsEqual(left, right []map[string]any) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if len(left[i]) != len(right[i]) {
			return false
		}
		for key, value := range left[i] {
			if right[i][key] != value {
				return false
			}
		}
	}
	return true
}
