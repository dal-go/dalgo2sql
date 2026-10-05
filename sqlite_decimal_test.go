package dalgo2sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"modernc.org/sqlite"
)

func TestSQLiteDecimalRegistrationErrors(t *testing.T) {
	registrationErr := errors.New("registration failed")
	for _, failure := range []struct {
		name string
		want string
	}{
		{name: "scalar", want: "decimal_add"},
		{name: "aggregate", want: "decimal_sum"},
		{name: "collation", want: "DECIMAL"},
	} {
		t.Run(failure.name, func(t *testing.T) {
			registrars := sqliteDecimalRegistrars{
				scalar: func(name string, _ int32, _ func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error)) error {
					if failure.name == "scalar" && name == "decimal_add" {
						return registrationErr
					}
					return nil
				},
				aggregate: func(name string, _ *sqlite.FunctionImpl) error {
					if failure.name == "aggregate" && name == "decimal_sum" {
						return registrationErr
					}
					return nil
				},
				collation: func(name string, _ func(string, string) int) error {
					if failure.name == "collation" && name == "DECIMAL" {
						return registrationErr
					}
					return nil
				},
			}
			err := registerSQLiteDecimalFunctionsWith(registrars)
			if !errors.Is(err, registrationErr) || !strings.Contains(err.Error(), failure.want) {
				t.Fatalf("registration error = %v, want wrapped %q error", err, failure.want)
			}
		})
	}
}

func TestSQLiteDecimalParsingAndBounds(t *testing.T) {
	for _, test := range []struct {
		value driver.Value
		want  string
	}{{[]byte("12.30"), "12.3"}, {int64(12), "12"}, {int(12), "12"}} {
		value := test.value
		parsed, err := parseExactDecimal(value)
		if err != nil || parsed.text() != test.want {
			t.Errorf("parseExactDecimal(%T) = %q, %v; want %q", value, parsed.text(), err, test.want)
		}
	}
	for _, value := range []driver.Value{
		float64(1), "", strings.Repeat("9", maxDecimalDigits+1), "0." + strings.Repeat("0", maxDecimalDigits+1),
	} {
		if _, err := parseExactDecimal(value); err == nil {
			t.Errorf("parseExactDecimal(%T %v) succeeded", value, value)
		}
	}
	if got := (exactDecimal{}).text(); got != "0" {
		t.Errorf("zero text = %q", got)
	}
	for _, pair := range [][2]string{{"-2", "-10"}, {"1", "bad"}, {"bad", "1"}, {"bad", "text"}} {
		if got := compareDecimalText(pair[0], pair[1]); got == 0 {
			t.Errorf("compareDecimalText(%q, %q) unexpectedly equal", pair[0], pair[1])
		}
	}
	if got := compareDecimalText("bad", "text"); got != -1 {
		t.Errorf("invalid text collation = %d", got)
	}
	if _, err := decimalFromRat(big.NewRat(1, 3)); err == nil {
		t.Error("decimalFromRat accepted a repeating decimal")
	}
	if _, err := decimalFromRat(new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Lsh(big.NewInt(1), 39))); err == nil {
		t.Error("decimalFromRat accepted a result scale over 38")
	}
}

func TestSQLiteDecimalAggregateErrorStateAndFinal(t *testing.T) {
	aggregate := &decimalAggregate{average: true, count: math.MaxInt64}
	if err := aggregate.Step(nil, []driver.Value{"1", int64(2)}); err == nil {
		t.Fatal("aggregate accepted a row-count overflow")
	}
	if err := aggregate.Step(nil, []driver.Value{"1", int64(2)}); err == nil {
		t.Fatal("aggregate did not preserve its first error")
	}
	if _, err := aggregate.result(); err == nil {
		t.Fatal("aggregate result dropped its stored error")
	}
	aggregate.Final(nil)
	if aggregate.count != 0 || aggregate.seen {
		t.Fatalf("Final retained aggregate state: %+v", aggregate)
	}
}

func TestSQLiteDecimalFunctions(t *testing.T) {
	if err := RegisterSQLiteDecimalFunctions(); err != nil {
		t.Fatal(err)
	}
	db := openTestSQLiteDB(t, `CREATE TABLE decimal_values (amount DECIMAL_TEXT(38, 10))`)
	db.SetMaxOpenConns(2)

	for _, tc := range []struct{ query, want string }{
		{`SELECT decimal_add('0.1','0.2')`, "0.3"},
		{`SELECT decimal_sub('0.3','0.1')`, "0.2"},
		{`SELECT decimal_mul('0.5','1')`, "0.5"},
		{`SELECT decimal_mul('0.2','1')`, "0.2"},
		{`SELECT decimal_add('9007199254740993','1')`, "9007199254740994"},
		{`SELECT decimal_mul('123456789012345678.123','2')`, "246913578024691356.246"},
		{`SELECT decimal_mul('-0.125','0.8')`, "-0.1"},
		{`SELECT decimal_div('1','6',3)`, "0.167"},
		{`SELECT decimal_round('2.5',0)`, "2"},
		{`SELECT decimal_round('3.5',0)`, "4"},
		{`SELECT decimal_round('-2.5',0)`, "-2"},
		{`SELECT decimal_round('-3.5',0)`, "-4"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			var got string
			if err := db.QueryRow(tc.query).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}

	var compared int64
	if err := db.QueryRow(`SELECT decimal_cmp('9007199254740993','9007199254740992')`).Scan(&compared); err != nil || compared != 1 {
		t.Fatalf("large exact compare = %d, %v", compared, err)
	}
	for _, tc := range []struct {
		a, b string
		want int64
	}{{"-10", "-2", -1}, {"1.00", "1", 0}, {"-1", "0", -1}} {
		if err := db.QueryRow(`SELECT decimal_cmp(?, ?)`, tc.a, tc.b).Scan(&compared); err != nil || compared != tc.want {
			t.Errorf("decimal_cmp(%q, %q) = %d, %v; want %d", tc.a, tc.b, compared, err, tc.want)
		}
	}
	for _, args := range [][2]string{{"bad", "1"}, {"1", "bad"}} {
		if _, err := db.Exec(`SELECT decimal_cmp(?, ?)`, args[0], args[1]); err == nil {
			t.Errorf("decimal_cmp accepted %q and %q", args[0], args[1])
		}
	}
	var ordered string
	if err := db.QueryRow(`SELECT v FROM (SELECT '10' AS v UNION ALL SELECT '2') ORDER BY v COLLATE DECIMAL LIMIT 1`).Scan(&ordered); err != nil || ordered != "2" {
		t.Fatalf("DECIMAL collation order = %q, %v", ordered, err)
	}

	if _, err := db.Exec(`INSERT INTO decimal_values VALUES ('0.1'),('0.2'),(NULL)`); err != nil {
		t.Fatal(err)
	}
	var stored string
	if _, err := db.Exec(`INSERT INTO decimal_values(amount) VALUES ('13.8000')`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT amount FROM decimal_values WHERE amount = '13.8000'`).Scan(&stored); err != nil || stored != "13.8000" {
		t.Fatalf("stored decimal text = %q, %v", stored, err)
	}
	adapter := NewDatabase(db, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite"})
	query := dal.NewTextQuery(`SELECT amount FROM decimal_values WHERE amount = '13.8000'`, nil)
	records, err := adapter.ExecuteQueryToRecordsReader(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	row, err := records.Next()
	if err != nil {
		_ = records.Close()
		t.Fatal(err)
	}
	if got := row.Data().(map[string]any)["amount"]; got != "13.8000" {
		_ = records.Close()
		t.Fatalf("DAL record decimal = %#v", got)
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}
	recordsetRows, err := adapter.ExecuteQueryToRecordsetReader(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	rsRow, rs, err := recordsetRows.Next()
	if err != nil {
		_ = recordsetRows.Close()
		t.Fatal(err)
	}
	if got, err := rsRow.GetValueByName("amount", rs); err != nil || got != "13.8000" {
		_ = recordsetRows.Close()
		t.Fatalf("DAL recordset decimal = %#v, %v", got, err)
	}
	if err := recordsetRows.Close(); err != nil {
		t.Fatal(err)
	}
	var sum, average sql.NullString
	if err := db.QueryRow(`SELECT decimal_sum(amount), decimal_avg(amount, 2) FROM decimal_values WHERE amount IN ('0.1', '0.2')`).Scan(&sum, &average); err != nil {
		t.Fatal(err)
	}
	if !sum.Valid || sum.String != "0.3" || !average.Valid || average.String != "0.15" {
		t.Fatalf("sum/average = %#v / %#v", sum, average)
	}
	var nullSum, nullAverage sql.NullString
	if err := db.QueryRow(`SELECT decimal_sum(amount), decimal_avg(amount, 2) FROM decimal_values WHERE 0`).Scan(&nullSum, &nullAverage); err != nil {
		t.Fatal(err)
	}
	if nullSum.Valid || nullAverage.Valid {
		t.Fatalf("empty aggregates = %#v / %#v, want NULL", nullSum, nullAverage)
	}
	if err := db.QueryRow(`SELECT decimal_sum(amount), decimal_avg(amount, 2) FROM decimal_values WHERE amount IS NULL`).Scan(&nullSum, &nullAverage); err != nil {
		t.Fatal(err)
	}
	if nullSum.Valid || nullAverage.Valid {
		t.Fatalf("all-NULL aggregates = %#v / %#v, want NULL", nullSum, nullAverage)
	}
	var scalarNull sql.NullString
	if err := db.QueryRow(`SELECT decimal_add(NULL, '1')`).Scan(&scalarNull); err != nil || scalarNull.Valid {
		t.Fatalf("NULL propagation = %#v, %v", scalarNull, err)
	}

	for _, query := range []string{
		`SELECT decimal_add('1e3','1')`,
		`SELECT decimal_add('NaN','1')`,
		`SELECT decimal_add('1.2.3','1')`,
		`SELECT decimal_add('1','bad')`,
		`SELECT decimal_add(1.25, 1)`,
		`SELECT decimal_div('1','0',2)`,
		`SELECT decimal_div('bad','1',2)`,
		`SELECT decimal_div('1','bad',2)`,
		`SELECT decimal_div('1','2','3')`,
		`SELECT decimal_div('1','2',39)`,
		`SELECT decimal_round('1',39)`,
		`SELECT decimal_round('bad',1)`,
		`SELECT decimal_round('1','2')`,
		`SELECT decimal_add('99999999999999999999999999999999999999','1')`,
		`SELECT decimal_mul('99999999999999999999999999999999999999','10')`,
		`SELECT decimal_div('99999999999999999999999999999999999999','0.1',1)`,
		`SELECT decimal_round('99999999999999999999999999999999999999',1)`,
		`SELECT decimal_avg('1','2')`,
	} {
		if _, err := db.Exec(query); err == nil {
			t.Errorf("%s unexpectedly succeeded", query)
		}
	}
	badAggregateErr := db.QueryRow(`SELECT decimal_sum('1e3')`).Scan(&sum)
	if badAggregateErr == nil || !strings.Contains(strings.ToLower(badAggregateErr.Error()), "decimal") {
		t.Errorf("malformed aggregate input error = %v", badAggregateErr)
	}
	if _, err := db.Exec(`SELECT decimal_avg(value, scale) FROM (SELECT '1' AS value, 1 AS scale UNION ALL SELECT '2', 2)`); err == nil {
		t.Error("decimal_avg accepted a variable scale in one group")
	}
	large := strings.Repeat("9", maxDecimalDigits)
	if _, err := db.Exec(`SELECT decimal_sum(value) FROM (SELECT ? AS value UNION ALL SELECT '1')`, large); err == nil {
		t.Error("decimal_sum silently accepted an over-precision result")
	}
	if _, err := db.Exec(`SELECT decimal_avg(?, 38)`, large); err == nil {
		t.Error("decimal_avg silently accepted an over-precision result")
	}
	if _, err := db.Exec(`SELECT decimal_sum(amount) OVER (ORDER BY rowid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM decimal_values`); err == nil {
		t.Error("decimal_sum unexpectedly accepted an unsupported sliding window")
	}
	if err := RegisterSQLiteDecimalFunctions(); err != nil {
		t.Errorf("second registration call: %v", err)
	}

	// The registration is installed on the driver, so a second connection can
	// execute the function without depending on which pool connection runs it.
	conn1, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn1.Close(); err != nil {
			t.Errorf("close SQLite connection 1: %v", err)
		}
	}()
	conn2, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn2.Close(); err != nil {
			t.Errorf("close SQLite connection 2: %v", err)
		}
	}()
	for i, conn := range []*sql.Conn{conn1, conn2} {
		var result string
		if err := conn.QueryRowContext(t.Context(), `SELECT decimal_add('0.1','0.2')`).Scan(&result); err != nil || result != "0.3" {
			t.Fatalf("connection %d decimal result = %q, %v", i+1, result, err)
		}
	}
}
