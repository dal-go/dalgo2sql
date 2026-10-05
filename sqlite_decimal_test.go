package dalgo2sql

import (
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLiteDecimalFunctions(t *testing.T) {
	if err := RegisterSQLiteDecimalFunctions(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	for _, tc := range []struct{ query, want string }{
		{`SELECT decimal_add('0.1','0.2')`, "0.3"},
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
	var ordered string
	if err := db.QueryRow(`SELECT v FROM (SELECT '10' AS v UNION ALL SELECT '2') ORDER BY v COLLATE DECIMAL LIMIT 1`).Scan(&ordered); err != nil || ordered != "2" {
		t.Fatalf("DECIMAL collation order = %q, %v", ordered, err)
	}

	if _, err := db.Exec(`CREATE TABLE decimal_values (amount DECIMAL_TEXT(38, 10)); INSERT INTO decimal_values VALUES ('0.1'),('0.2'),(NULL)`); err != nil {
		t.Fatal(err)
	}
	var stored string
	if _, err := db.Exec(`INSERT INTO decimal_values(amount) VALUES ('13.8000')`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT amount FROM decimal_values WHERE amount = '13.8000'`).Scan(&stored); err != nil || stored != "13.8000" {
		t.Fatalf("stored decimal text = %q, %v", stored, err)
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
	var scalarNull sql.NullString
	if err := db.QueryRow(`SELECT decimal_add(NULL, '1')`).Scan(&scalarNull); err != nil || scalarNull.Valid {
		t.Fatalf("NULL propagation = %#v, %v", scalarNull, err)
	}

	for _, query := range []string{
		`SELECT decimal_add('1e3','1')`,
		`SELECT decimal_add('NaN','1')`,
		`SELECT decimal_add('1.2.3','1')`,
		`SELECT decimal_add(1.25, 1)`,
		`SELECT decimal_div('1','0',2)`,
		`SELECT decimal_round('1',39)`,
		`SELECT decimal_add('99999999999999999999999999999999999999','1')`,
	} {
		if _, err := db.Exec(query); err == nil {
			t.Errorf("%s unexpectedly succeeded", query)
		}
	}
	var badAggregateErr error
	badAggregateErr = db.QueryRow(`SELECT decimal_sum('1e3')`).Scan(&sum)
	if badAggregateErr == nil || !strings.Contains(strings.ToLower(badAggregateErr.Error()), "decimal") {
		t.Errorf("malformed aggregate input error = %v", badAggregateErr)
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
	defer conn1.Close()
	conn2, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	for i, conn := range []*sql.Conn{conn1, conn2} {
		var result string
		if err := conn.QueryRowContext(t.Context(), `SELECT decimal_add('0.1','0.2')`).Scan(&result); err != nil || result != "0.3" {
			t.Fatalf("connection %d decimal result = %q, %v", i+1, result, err)
		}
	}
}
