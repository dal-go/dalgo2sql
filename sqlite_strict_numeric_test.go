package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/tugql"
	"modernc.org/sqlite"
)

func strictSQLiteTestSession(t *testing.T, script string, strict bool) dal.ReadSession {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "owned.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err = db.Exec(script); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	session, err := NewReadSession(tx, DbOptions{StructuredQueryDialect: "sqlite", PreserveNativeValues: true, SQLiteStrictNumbers: strict})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func strictSQLiteTestQuery(t *testing.T, text string) dal.StructuredQuery {
	t.Helper()
	q, err := tugql.Deserialize([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func strictSQLiteTestRead(t *testing.T, session dal.ReadSession, text string) ([][]any, error) {
	t.Helper()
	reader, err := session.ExecuteQueryToRecordsetReader(context.Background(), strictSQLiteTestQuery(t, text))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	var result [][]any
	for {
		row, rs, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		var cells []any
		for i := range rs.Columns() {
			cell, err := row.GetValueByIndex(i, rs)
			if err != nil {
				t.Fatal(err)
			}
			cells = append(cells, cell)
		}
		result = append(result, cells)
	}
}

func TestSQLiteStrictNumbersNativePrecisionAndPolicyPredicates(t *testing.T) {
	script := `CREATE TABLE Numbers(value INTEGER,other INTEGER);INSERT INTO Numbers VALUES(9007199254740992,9007199254740993),(9007199254740993,9007199254740992),(9223372036854775807,-9223372036854775808);`
	strict := strictSQLiteTestSession(t, script, true)
	rows, err := strictSQLiteTestRead(t, strict, "from: {name: Numbers}\ncolumns: [{field: value}]")
	if err != nil || !reflect.DeepEqual(rows, [][]any{{int64(9007199254740992)}, {int64(9007199254740993)}, {int64(math.MaxInt64)}}) {
		t.Fatalf("native projection=%#v,%v", rows, err)
	}
	rows, err = strictSQLiteTestRead(t, strict, "from: {name: Numbers}\ncolumns: [{field: value}, {field: other}]\norderBy: [{field: value, desc: true}]")
	if err != nil || !reflect.DeepEqual(rows, [][]any{{int64(math.MaxInt64), int64(math.MinInt64)}, {int64(9007199254740993), int64(9007199254740992)}, {int64(9007199254740992), int64(9007199254740993)}}) {
		t.Fatalf("exact native int64 ordering/projection=%#v,%v", rows, err)
	}
	rows, err = strictSQLiteTestRead(t, strict, "from: {name: Numbers}\ncolumns: [{aggregate: {function: count, args: [{star: true}]}, as: counted}]")
	if err != nil || !reflect.DeepEqual(rows, [][]any{{int64(3)}}) {
		t.Fatalf("exact COUNT=%#v,%v", rows, err)
	}
	for _, text := range []string{
		"from: {name: Numbers}\nwhere: {op: '==', left: {field: value}, right: {field: other}}",
		"from: {name: Numbers}\nwhere: {op: '>=', left: {field: other}, right: {value: 1}}",
		"from: {name: Numbers}\nwhere: {op: In, left: {field: value}, right: {values: [1, 2]}}",
		"from: {name: Numbers}\ncolumns: [{field: value}, {aggregate: {function: count, args: [{star: true}]}, as: count}]\ngroupBy: [{field: value}]",
		"from: {name: Numbers}\ncolumns: [{aggregate: {function: max, args: [{field: value}]}, as: maximum}]",
		"from: {name: Numbers}\ncolumns: [{aggregate: {function: min, args: [{field: other}]}, as: minimum}]",
		"from: {name: Numbers}\ncolumns: [{aggregate: {function: count, distinct: true, args: [{field: value}]}, as: counted}]",
		"from: {name: Numbers}\ncolumns: [{aggregate: {function: sum, args: [{field: value}]}, as: total}]",
		"from: {name: Numbers}\ncolumns: [{aggregate: {function: avg, args: [{field: value}]}, as: mean}]",
		"from: {name: Numbers, alias: n, joins: [{type: inner, from: {name: Numbers, alias: x}, on: [{op: '==', left: {field: value, source: n}, right: {field: other, source: x}}]}]}\ncolumns: [{field: value, source: n, as: v}]",
	} {
		rows, err := strictSQLiteTestRead(t, strict, text)
		var native *sqlite.Error
		if rows != nil || !errors.As(err, &native) || native.Code()&255 != 1 {
			t.Fatalf("unsafe values were not a whole native refusal: %s: %#v,%v", text, rows, err)
		}
	}
	// This control preserves the old default contract and demonstrates the exact
	// wrong-match the opt-in profile refuses instead of silently accepting it.
	legacy := strictSQLiteTestSession(t, script, false)
	rows, err = strictSQLiteTestRead(t, legacy, "from: {name: Numbers}\ncolumns: [{field: value}]\nwhere: {op: '==', left: {field: value}, right: {field: other}}")
	if err != nil || len(rows) != 2 {
		t.Fatalf("legacy control=%#v,%v", rows, err)
	}
}

func TestSQLiteStrictNumbersNativeArithmeticAggregatesAndText(t *testing.T) {
	session := strictSQLiteTestSession(t, `CREATE TABLE ValuesTable(value,other);INSERT INTO ValuesTable VALUES(9007199254740991,1),(1,2),(-9007199254740991,3);CREATE TABLE TextTable(value TEXT);INSERT INTO TextTable VALUES('9007199254740993'),('9.007199254740993e15'),('1');`, true)
	tests := []struct {
		text string
		want [][]any
	}{
		{"from: {name: ValuesTable}\ncolumns: [{aggregate: {function: sum, args: [{field: value}]}, as: total}]", [][]any{{float64(1)}}},
		{"from: {name: ValuesTable}\ncolumns: [{aggregate: {function: avg, args: [{field: value}]}, as: mean}]", [][]any{{float64(1) / 3}}},
		{"from: {name: ValuesTable}\ncolumns: [{binary: {op: '/', left: {field: other}, right: {value: 2}}, as: ratio}]", [][]any{{float64(.5)}, {float64(1)}, {float64(1.5)}}},
		{"from: {name: ValuesTable}\ncolumns: [{binary: {op: '/', left: {field: other}, right: {value: 0}}, as: ratio}]", [][]any{{nil}, {nil}, {nil}}},
		{"from: {name: ValuesTable}\ncolumns: [{aggregate: {function: sum, args: [{value: 1}]}, as: total}, {aggregate: {function: avg, args: [{value: 2}]}, as: mean}]", [][]any{{float64(3), float64(2)}}},
		{"from: {name: TextTable}\nwhere: {op: '==', left: {field: value}, right: {value: 1}}", nil},
		{"from: {name: TextTable}\ncolumns: [{aggregate: {function: sum, args: [{field: value}]}, as: total}]", [][]any{{nil}}},
	}
	for _, test := range tests {
		got, err := strictSQLiteTestRead(t, session, test.text)
		if err != nil || !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s: %#v,%v; want%#v", test.text, got, err, test.want)
		}
	}
	rows, err := strictSQLiteTestRead(t, session, "from: {name: ValuesTable}\ncolumns: [{binary: {op: '+', left: {field: value}, right: {value: 1}}, as: unsafe}]")
	var native *sqlite.Error
	if rows != nil || !errors.As(err, &native) || native.Code()&255 != 1 {
		t.Fatalf("unsafe arithmetic result was not refused=%#v,%v", rows, err)
	}
	for _, script := range []string{
		`CREATE TABLE ValuesTable(value);INSERT INTO ValuesTable VALUES(4503599627370496),(4503599627370496);`,
		`CREATE TABLE ValuesTable(value);WITH RECURSIVE seed(n) AS(VALUES(1) UNION ALL SELECT n+1 FROM seed WHERE n<2049) INSERT INTO ValuesTable SELECT 9007199254740991 FROM seed;`,
	} {
		session := strictSQLiteTestSession(t, script, true)
		for _, aggregate := range []string{"sum", "avg"} {
			rows, err := strictSQLiteTestRead(t, session, "from: {name: ValuesTable}\ncolumns: [{aggregate: {function: "+aggregate+", args: [{field: value}]}, as: total}]")
			if err == nil || rows != nil {
				t.Fatalf("range/overflow accepted=%#v,%v", rows, err)
			}
		}
	}
}

func TestSQLiteStrictNumbersOptionsAndNumericConstants(t *testing.T) {
	for _, options := range []DbOptions{{SQLiteStrictNumbers: true}, {SQLiteStrictNumbers: true, StructuredQueryDialect: "postgres"}, {SQLiteStrictNumbers: true, StructuredQueryDialect: "sqlite", NativeStructuredQueryCompiler: dummyCompiler{}}} {
		if err := options.validatePreserveNativeValues(); err == nil {
			t.Fatalf("bad strict configuration admitted=%#v", options)
		}
	}
	for _, value := range []any{int64(math.MinInt64), int64(math.MaxInt64), float64(9007199254740992)} {
		if _, err := (sqliteEmitter{strictNumbers: true}).normalizedNumber(value); err == nil {
			t.Fatalf("unsafe constant admitted=%v", value)
		}
	}
	if _, err := (sqliteEmitter{strictNumbers: true}).normalizedNumber("9007199254740993"); err == nil {
		t.Fatal("numeric text constant coerced")
	}
}
