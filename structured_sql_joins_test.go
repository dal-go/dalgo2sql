package dalgo2sql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	_ "modernc.org/sqlite"
)

func sqlJoin(leftSource, leftField, rightSource, rightField string) dal.Condition {
	return dal.NewComparison(
		dal.NewFieldRef(leftSource, leftField),
		dal.Equal,
		dal.NewFieldRef(rightSource, rightField),
	)
}

func chinookNestedSQLQuery() dal.StructuredQuery {
	employees := dal.From(dal.NewQualifiedRootCollectionRef("main", "Employee", "e"))
	customers := dal.From(dal.NewQualifiedRootCollectionRef("main", "Customer", "c")).Join(
		dal.NewJoinedFrom(employees, dal.JoinLeft, sqlJoin("c", "SupportRepId", "e", "EmployeeId")),
	)
	return dal.From(dal.NewQualifiedRootCollectionRef("main", "Invoice", "i")).Join(
		dal.NewJoinedFrom(customers, dal.JoinInner, sqlJoin("i", "CustomerId", "c", "CustomerId")),
	).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("c", "CustomerId"), dal.GreaterOrEqual, dal.NewConstant(10))).
		OrderBy(dal.Descending(dal.NewFieldRef("i", "InvoiceId"))).
		Limit(10).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("i", "InvoiceId"), Alias: "invoice_id"},
			dal.Column{Expression: dal.NewFieldRef("c", "FirstName"), Alias: "customer"},
			dal.Column{Expression: dal.NewFieldRef("e", "FirstName"), Alias: "employee"},
		)
}

func TestCompileStructuredSQLRecursiveJoinsPreserveSubtreeAndSchema(t *testing.T) {
	contents, err := os.ReadFile("testdata/joins/chinook-nested.dtql.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprintf("%x", sha256.Sum256(contents)), "2eeef93ffb02a4272f06f6ab909b2db4cbbb9a2a1df280ae4c47851534f583b3"; got != want {
		t.Fatalf("canonical JOIN fixture SHA-256 = %s, want %s", got, want)
	}

	text, args, err := compileStructuredSQL(chinookNestedSQLQuery())
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"FROM `main`.`Invoice` AS `i` INNER JOIN (`main`.`Customer` AS `c` LEFT JOIN `main`.`Employee` AS `e` ON",
		"`i`.`CustomerId`",
		"`c`.`CustomerId`",
		"`c`.`SupportRepId`",
		"`e`.`EmployeeId`",
		") ON (",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("SQL does not preserve nested relation shape; missing %q:\n%s", fragment, text)
		}
	}
	if got, want := fmt.Sprint(args), "[10 10]"; got != want {
		t.Fatalf("args = %s, want %s", got, want)
	}
}

func TestCanonicalJoinFixtureManifestPinsEveryVendoredFixture(t *testing.T) {
	contents, err := os.ReadFile("testdata/joins/canonical-fixtures.sha256.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SourceCommit string            `json:"sourceCommit"`
		Files        map[string]string `json:"files"`
	}
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) == 0 {
		t.Fatal("canonical fixture manifest is empty")
	}
	if got, want := manifest.SourceCommit, "d393914bf9f9fe4a09fbba3188219e24f86ea284"; got != want {
		t.Fatalf("canonical fixture source commit = %q, want exact %q", got, want)
	}
	for name, want := range manifest.Files {
		fixture, err := os.ReadFile("testdata/joins/" + name)
		if err != nil {
			t.Fatalf("read fixture %q: %v", name, err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(fixture)); got != want {
			t.Fatalf("fixture %q SHA-256 = %s, want %s", name, got, want)
		}
	}
}

func TestSQLiteJoinPlanningUsesTransactionSnapshotForNativePushdown(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE A (id INTEGER); CREATE TABLE B (a_id REAL); CREATE TABLE C (b_id REAL); CREATE TABLE TextB (a_id TEXT); CREATE TABLE UnsafeA (id BLOB); CREATE TABLE UnsafeB (a_id BLOB);
		CREATE TABLE UnsafeIntegerA (id INTEGER); CREATE TABLE UnsafeIntegerB (id REAL); CREATE TABLE UnsafeRealA (id REAL); CREATE TABLE UnsafeRealB (id REAL); CREATE TABLE FractionalA (id REAL); CREATE TABLE FractionalB (id REAL);
		INSERT INTO A VALUES (1); INSERT INTO B VALUES (1.0); INSERT INTO C VALUES (1.0); INSERT INTO TextB VALUES ('1'); INSERT INTO UnsafeA VALUES (x'01'); INSERT INTO UnsafeB VALUES (x'01');
		INSERT INTO UnsafeIntegerA VALUES (9007199254740992); INSERT INTO UnsafeIntegerB VALUES (9007199254740992.0); INSERT INTO UnsafeRealA VALUES (9007199254740992.0); INSERT INTO UnsafeRealB VALUES (9007199254740992.0);
		INSERT INTO FractionalA VALUES (1.5); INSERT INTO FractionalB VALUES (1.5);`); err != nil {
		t.Fatal(err)
	}
	eligible := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "id")})

	backend := &database{db: raw, options: DbOptions{StructuredQueryDialect: "sqlite"}}
	plan, err := dal.PlanJoin(context.Background(), eligible, backend)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Strategy != dal.JoinGeneric {
		t.Fatalf("pooled database plan = %#v, want generic because preflight cannot share its snapshot with execution", plan)
	}

	tx, err := raw.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	transactionPlan, err := dal.PlanJoin(context.Background(), eligible, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if transactionPlan.Strategy != dal.JoinNative {
		t.Fatalf("transaction plan = %#v, want native", transactionPlan)
	}
	if err := canExecuteSQLiteJoin(context.Background(), dal.From(dal.NewRootCollectionRef("A", "a")).NewQuery().SelectColumns(), "sqlite", tx.QueryContext); err != nil {
		t.Fatalf("single-source SQLite eligibility = %v, want no error", err)
	}
	nestedRight := dal.From(dal.NewRootCollectionRef("B", "b")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("C", "c"), dal.JoinInner, sqlJoin("b", "a_id", "c", "b_id")),
	)
	nestedEligible := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedFrom(nestedRight, dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns()
	nestedPlan, err := dal.PlanJoin(context.Background(), nestedEligible, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if nestedPlan.Strategy != dal.JoinNative {
		t.Fatalf("nested transaction plan = %#v, want native", nestedPlan)
	}

	unsupportedWildcard := dal.WithColumns(eligible, []dal.Column{{Wildcard: &dal.WildcardProjection{Source: "a", Exclude: []string{"id"}}}})
	unsupportedWildcardPlan, err := dal.PlanJoin(context.Background(), unsupportedWildcard, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if unsupportedWildcardPlan.Strategy != dal.JoinGeneric || !strings.Contains(unsupportedWildcardPlan.Reason, "join_plan") {
		t.Fatalf("wildcard plan = %#v, want generic join_plan decline", unsupportedWildcardPlan)
	}

	unsupportedFirst := dal.WithColumns(eligible, []dal.Column{dal.FirstAs(dal.NewFieldRef("a", "id"), "first_id")})
	unsupportedFirstPlan, err := dal.PlanJoin(context.Background(), unsupportedFirst, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if unsupportedFirstPlan.Strategy != dal.JoinGeneric || !strings.Contains(unsupportedFirstPlan.Reason, "join_plan") {
		t.Fatalf("FIRST plan = %#v, want generic join_plan decline", unsupportedFirstPlan)
	}

	unsafe := dal.From(dal.NewRootCollectionRef("UnsafeA", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("UnsafeB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "id")})
	unsafePlan, err := dal.PlanJoin(context.Background(), unsafe, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if unsafePlan.Strategy != dal.JoinGeneric || !strings.Contains(unsafePlan.Reason, "join_key_type") {
		t.Fatalf("unsafe key plan = %#v, want generic join_key_type decline", unsafePlan)
	}

	incompatible := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("TextB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns()
	incompatiblePlan, err := dal.PlanJoin(context.Background(), incompatible, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if incompatiblePlan.Strategy != dal.JoinGeneric || !strings.Contains(incompatiblePlan.Reason, "join_key_type") {
		t.Fatalf("incompatible key plan = %#v, want generic join_key_type decline", incompatiblePlan)
	}

	for _, unsafeNumber := range []dal.StructuredQuery{
		dal.From(dal.NewRootCollectionRef("UnsafeIntegerA", "a")).Join(dal.NewJoinedSource(dal.NewRootCollectionRef("UnsafeIntegerB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id"))).NewQuery().SelectColumns(),
		dal.From(dal.NewRootCollectionRef("UnsafeRealA", "a")).Join(dal.NewJoinedSource(dal.NewRootCollectionRef("UnsafeRealB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id"))).NewQuery().SelectColumns(),
	} {
		plan, err := dal.PlanJoin(context.Background(), unsafeNumber, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
		if err != nil {
			t.Fatal(err)
		}
		if plan.Strategy != dal.JoinGeneric || !strings.Contains(plan.Reason, "join_key_type") {
			t.Fatalf("unsafe numeric key plan = %#v, want generic join_key_type decline", plan)
		}
	}

	fractional := dal.From(dal.NewRootCollectionRef("FractionalA", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("FractionalB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "id")),
	).NewQuery().SelectColumns()
	fractionalPlan, err := dal.PlanJoin(context.Background(), fractional, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if fractionalPlan.Strategy != dal.JoinNative {
		t.Fatalf("fractional numeric plan = %#v, want native", fractionalPlan)
	}

	child := dal.From(dal.NewRootCollectionRef("B", "b")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "c"), dal.JoinInner, sqlJoin("c", "a_id", "a", "id")),
	)
	correlated := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedFrom(child, dal.JoinLeft, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "id")})
	correlatedPlan, err := dal.PlanJoin(context.Background(), correlated, newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()))
	if err != nil {
		t.Fatal(err)
	}
	if correlatedPlan.Strategy != dal.JoinGeneric || !strings.Contains(correlatedPlan.Reason, "join_plan") {
		t.Fatalf("correlated plan = %#v, want generic join_plan decline", correlatedPlan)
	}
}

func TestSQLiteJoinFieldsUsePragmaOrderAndDialect(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE JoinFields (third TEXT, first INTEGER, second REAL)`); err != nil {
		t.Fatal(err)
	}
	source := dal.NewRootCollectionRef("JoinFields", "j")
	backend := &database{db: raw, options: DbOptions{StructuredQueryDialect: "sqlite"}}
	fields, err := backend.JoinFields(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(fields), "[third first second]"; got != want {
		t.Fatalf("database JoinFields = %s, want %s", got, want)
	}

	tx, err := raw.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	txFields, err := newTransaction(tx, DbOptions{StructuredQueryDialect: "sqlite"}, dal.NewTransactionOptions()).JoinFields(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(txFields), "[third first second]"; got != want {
		t.Fatalf("transaction JoinFields = %s, want %s", got, want)
	}
	if _, err := (&database{db: raw}).JoinFields(context.Background(), source); err == nil || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("non-SQLite JoinFields error = %v, want join_plan decline", err)
	}
}

func TestSQLiteGenericJoinExpandsQualifiedWildcard(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE WildcardA (id INTEGER, name TEXT); CREATE TABLE WildcardB (a_id INTEGER); INSERT INTO WildcardA VALUES (1, 'Ada'); INSERT INTO WildcardB VALUES (1);`); err != nil {
		t.Fatal(err)
	}
	backend := &database{db: raw, options: DbOptions{StructuredQueryDialect: "sqlite"}}
	query := dal.From(dal.NewRootCollectionRef("WildcardA", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("WildcardB", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectColumns(dal.Column{Wildcard: &dal.WildcardProjection{Source: "a", Exclude: []string{"id"}}})
	reader, err := dal.NewDB(backend).ExecuteQueryToRecordsReader(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	records, err := dal.ReadAllToRecords(context.Background(), reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || fmt.Sprint(records[0].Data()) != "map[name:Ada]" {
		t.Fatalf("generic wildcard records = %#v, want one name-only row", records)
	}
}

func TestSQLiteJoinEligibilityRejectsNonportableMetadataAndValues(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE Typed (id INTEGER, text_key TEXT, blob_key INTEGER); INSERT INTO Typed VALUES (1, 'one', x'01')`); err != nil {
		t.Fatal(err)
	}
	execute := raw.QueryContext
	source := dal.NewRootCollectionRef("Typed", "t")
	if err := canExecuteSQLiteJoin(context.Background(), nil, "sqlite", execute); err == nil || !strings.Contains(err.Error(), "join_shape") {
		t.Fatalf("nil query error = %v, want join_shape", err)
	}
	if err := canExecuteSQLiteJoin(context.Background(), dal.From(source).NewQuery().SelectColumns(), "postgres", execute); err == nil || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("non-SQLite native error = %v, want join_plan", err)
	}
	if _, err := sqliteDeclaredColumnType(context.Background(), source, "missing", execute); err == nil || !strings.Contains(err.Error(), "join_field") {
		t.Fatalf("missing SQLite field error = %v, want join_field", err)
	}
	if err := sqlitePreflightColumnValues(context.Background(), source, "blob_key", sqliteJoinNumber, execute); err == nil || !strings.Contains(err.Error(), "join_key_type") {
		t.Fatalf("BLOB SQLite value error = %v, want join_key_type", err)
	}

	pointer := &source
	classes := map[string]sqliteJoinSource{"t": {source: pointer, class: make(map[string]sqliteJoinKeyClass)}}
	if class, err := sqliteJoinKeyClassFor(context.Background(), classes, dal.NewFieldRef("t", "id"), execute); err != nil || class != sqliteJoinNumber {
		t.Fatalf("pointer collection class = %q, %v", class, err)
	}
	if class, err := sqliteJoinKeyClassFor(context.Background(), classes, dal.NewFieldRef("t", "text_key"), execute); err != nil || class != sqliteJoinText {
		t.Fatalf("text collection class = %q, %v", class, err)
	}
	if _, err := sqliteJoinKeyClassFor(context.Background(), classes, dal.NewFieldRef("missing", "id"), execute); err == nil || !strings.Contains(err.Error(), "join_scope") {
		t.Fatalf("unknown source error = %v, want join_scope", err)
	}

	for _, test := range []struct {
		declared string
		want     sqliteJoinKeyClass
		ok       bool
	}{
		{declared: "INTEGER", want: sqliteJoinNumber, ok: true},
		{declared: "VARCHAR", want: sqliteJoinText, ok: true},
		{declared: "BLOB"},
	} {
		got, ok := sqlitePortableJoinClass(test.declared)
		if got != test.want || ok != test.ok {
			t.Fatalf("SQLite class(%q) = %q, %v; want %q, %v", test.declared, got, ok, test.want, test.ok)
		}
	}
	for _, value := range []any{int64(9007199254740992), float64(9007199254740992), math.Inf(1)} {
		if err := sqliteValidatePortableJoinNumber(value, "id"); err == nil || !strings.Contains(err.Error(), "join_key_type") {
			t.Fatalf("unsafe number %v error = %v, want join_key_type", value, err)
		}
	}
	if err := sqliteValidatePortableJoinNumber(1.5, "id"); err != nil {
		t.Fatalf("fractional number should stay portable: %v", err)
	}

	malformed := dal.From(dal.NewRootCollectionRef("Typed", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Typed", "b"), dal.JoinInner),
	).NewQuery().SelectColumns()
	if err := canExecuteSQLiteJoin(context.Background(), malformed, "sqlite", execute); err == nil || !strings.Contains(err.Error(), "join_shape") {
		t.Fatalf("malformed tree error = %v, want join_shape", err)
	}
	queryFailure := func(context.Context, string, ...any) (*sql.Rows, error) {
		return nil, errors.New("metadata unavailable")
	}
	if _, err := sqliteJoinFields(context.Background(), source, "sqlite", queryFailure); err == nil || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("metadata query error = %v, want join_plan", err)
	}
	if err := sqlitePreflightColumnValues(context.Background(), source, "id", sqliteJoinNumber, queryFailure); err == nil || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("key preflight query error = %v, want join_plan", err)
	}
	qualified := dal.NewQualifiedRootCollectionRef("main", "Typed", "t")
	if fields, err := sqliteJoinFields(context.Background(), qualified, "sqlite", execute); err != nil || fmt.Sprint(fields) != "[id text_key blob_key]" {
		t.Fatalf("schema-qualified fields = %v, %v", fields, err)
	}
	if _, err := sqliteJoinSources(nil); err == nil || !strings.Contains(err.Error(), "join_shape") {
		t.Fatalf("missing join source error = %v, want join_shape", err)
	}
	invalidAlias := dal.From(dal.NewRootCollectionRef("Typed", "bad alias"))
	if _, err := sqliteJoinSources(invalidAlias); err == nil || !strings.Contains(err.Error(), "plain identifier") {
		t.Fatalf("invalid SQLite source alias error = %v", err)
	}
	for _, condition := range []dal.Condition{
		dal.NewGroupCondition(dal.And),
		dal.NewComparison(dal.NewConstant(1), dal.Equal, dal.NewConstant(1)),
	} {
		invalidOn := dal.From(dal.NewRootCollectionRef("Typed", "a")).Join(
			dal.NewJoinedSource(dal.NewRootCollectionRef("Typed", "b"), dal.JoinInner, condition),
		)
		if err := preflightSQLiteJoinKeys(context.Background(), invalidOn, nil, execute, "from"); err == nil || !strings.Contains(err.Error(), "join_shape") {
			t.Fatalf("invalid ON preflight error = %v, want join_shape", err)
		}
	}
}

func TestSQLiteRecursiveJoinsOneToManyLeftAndTypedKeys(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	fixture, err := os.ReadFile("testdata/joins/chinook-mini.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(string(fixture)); err != nil {
		t.Fatal(err)
	}
	text, args, err := compileStructuredSQL(chinookNestedSQLQuery())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := raw.Query(text, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var invoiceID int
		var customer string
		var employee sql.NullString
		if err := rows.Scan(&invoiceID, &customer, &employee); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d/%s/%v/%s", invoiceID, customer, employee.Valid, employee.String))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"3/Bea/false/", "2/Ada/true/Evan", "1/Ada/true/Evan"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}

	if _, err := raw.Exec(`CREATE TABLE IntegerKeys (id INTEGER); CREATE TABLE RealKeys (id REAL); CREATE TABLE TextKeys (id TEXT);
		INSERT INTO IntegerKeys VALUES (1); INSERT INTO RealKeys VALUES (1.0); INSERT INTO TextKeys VALUES ('1');`); err != nil {
		t.Fatal(err)
	}
	from := dal.From(dal.NewRootCollectionRef("IntegerKeys", "i")).
		Join(dal.NewJoinedSource(dal.NewRootCollectionRef("RealKeys", "r"), dal.JoinInner, sqlJoin("i", "id", "r", "id"))).
		Join(dal.NewJoinedSource(dal.NewRootCollectionRef("TextKeys", "t"), dal.JoinLeft, sqlJoin("i", "id", "t", "id")))
	typedQuery := from.NewQuery().OrderBy(dal.Ascending(dal.NewFieldRef("i", "id"))).SelectColumns(
		dal.Column{Expression: dal.NewFieldRef("i", "id"), Alias: "integer_id"},
		dal.Column{Expression: dal.NewFieldRef("r", "id"), Alias: "real_id"},
		dal.Column{Expression: dal.NewFieldRef("t", "id"), Alias: "text_id"},
	)
	text, args, err = compileStructuredSQL(typedQuery)
	if err != nil {
		t.Fatal(err)
	}
	var integerID int
	var realID float64
	var textID sql.NullString
	if err := raw.QueryRow(text, args...).Scan(&integerID, &realID, &textID); err != nil {
		t.Fatal(err)
	}
	if integerID != 1 || realID != 1 || textID.Valid {
		t.Fatalf("typed join = integer:%d real:%v text:%#v", integerID, realID, textID)
	}
}

func TestCompileStructuredSQLRejectsCorrelatedNestedJoin(t *testing.T) {
	child := dal.From(dal.NewRootCollectionRef("B", "b")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("C", "c"), dal.JoinInner, sqlJoin("c", "AId", "a", "Id")),
	)
	query := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedFrom(child, dal.JoinLeft, sqlJoin("a", "Id", "b", "AId")),
	).NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "Id")})
	_, _, err := compileStructuredSQL(query)
	if err == nil || !strings.Contains(err.Error(), "from.joins[0].from.joins[0].on[0]") || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("correlated nested ON error = %v", err)
	}
}

func TestCompileStructuredSQLJoinCompositionClauses(t *testing.T) {
	count := dal.NewAggregate(dal.COUNT, false, dal.Star())
	query := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().
		Where(dal.NewComparison(dal.NewFieldRef("a", "id"), dal.GreaterThen, dal.NewConstant(0))).
		GroupBy(dal.NewFieldRef("a", "id")).
		Having(dal.NewComparison(count, dal.GreaterThen, dal.NewConstant(0))).
		OrderBy(dal.Descending(dal.NewFieldRef("", "total"))).
		Limit(3).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("a", "id"), Alias: "id"},
			dal.Column{Expression: count, Alias: "total"},
		)
	text, args, err := compileStructuredSQL(query)
	if err != nil {
		t.Fatal(err)
	}
	for _, clause := range []string{" WHERE ", " GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ?"} {
		if !strings.Contains(text, clause) {
			t.Fatalf("compiled JOIN SQL missing %q:\n%s", clause, text)
		}
	}
	if got, want := fmt.Sprint(args), "[0 0 3]"; got != want {
		t.Fatalf("composition args = %s, want %s", got, want)
	}
}

func TestCompileStructuredSQLNativeJoinRewritesAggregateAliasesInGroupedHavingAndOrder(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE A (id INTEGER); CREATE TABLE B (a_id INTEGER);
		INSERT INTO A VALUES (1), (2); INSERT INTO B VALUES (1), (1), (2);`); err != nil {
		t.Fatal(err)
	}
	count := dal.NewAggregate(dal.COUNT, false, dal.Star())
	query := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().
		GroupBy(dal.NewFieldRef("a", "id")).
		Having(dal.NewGroupCondition(dal.And,
			dal.NewComparison(dal.NewFieldRef("", "total"), dal.GreaterOrEqual, dal.NewConstant(1)),
			dal.NewComparison(dal.NewFieldRef("", "id"), dal.GreaterThen, dal.NewConstant(0)),
		)).
		OrderBy(dal.Descending(dal.Binary(dal.NewFieldRef("", "total"), dal.Add, dal.NewConstant(1)))).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("a", "id"), Alias: "id"},
			dal.Column{Expression: count, Alias: "total"},
		)
	text, args, err := compileStructuredSQL(query)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, " HAVING ") || !strings.Contains(text, " AND ") || !strings.Contains(text, " ORDER BY ") {
		t.Fatalf("grouped native JOIN did not preserve HAVING/ORDER expressions:\n%s", text)
	}
	rows, err := raw.Query(text, args...)
	if err != nil {
		t.Fatalf("execute grouped native JOIN: %v\n%s", err, text)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var id, total int
		if err := rows.Scan(&id, &total); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d:%d", id, total))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := "[1:2 2:1]"; fmt.Sprint(got) != want {
		t.Fatalf("grouped native JOIN rows = %v, want %s", got, want)
	}
}

func TestCompileSQLJoinOnRequiresKnownQualifiedEqualityFields(t *testing.T) {
	sources := map[string]struct{}{"a": {}, "b": {}}
	valid := []dal.Condition{
		sqlJoin("a", "id", "b", "a_id"),
		sqlJoin("a", "tenant_id", "b", "tenant_id"),
	}
	compiled, err := compileSQLJoinOn(valid, sources, "from.joins[0].on")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled, " AND ") || !strings.Contains(compiled, "`a`.`id`") || !strings.Contains(compiled, "`b`.`a_id`") {
		t.Fatalf("composite ON = %s", compiled)
	}
	for _, test := range []struct {
		name       string
		conditions []dal.Condition
		want       string
	}{
		{name: "empty", want: "join_shape"},
		{name: "operator", conditions: []dal.Condition{dal.NewComparison(dal.NewFieldRef("a", "id"), dal.Operator("!="), dal.NewFieldRef("b", "a_id"))}, want: "join_operator"},
		{name: "unqualified", conditions: []dal.Condition{dal.NewComparison(dal.NewFieldRef("", "id"), dal.Equal, dal.NewFieldRef("b", "a_id"))}, want: "must name a source"},
		{name: "unknown source", conditions: []dal.Condition{dal.NewComparison(dal.NewFieldRef("missing", "id"), dal.Equal, dal.NewFieldRef("b", "a_id"))}, want: "unknown source"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := compileSQLJoinOn(test.conditions, sources, "from.joins[0].on"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ON error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSQLiteNativeJoinEligibilityDeclinesUnrepresentableQualifiedFieldsBeforeExecution(t *testing.T) {
	raw, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err := raw.Exec(`CREATE TABLE A (id INTEGER); CREATE TABLE B (a_id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	execute := raw.QueryContext
	root := dal.From(dal.NewRootCollectionRef("A", "a"))
	join := dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id"))
	root.Join(join)
	unqualifiedProjection := root.NewQuery().SelectColumns(dal.Column{Expression: dal.Field("id")})
	if err := canExecuteSQLiteJoin(context.Background(), unqualifiedProjection, "sqlite", execute); err == nil || !strings.Contains(err.Error(), "join_plan") {
		t.Fatalf("unqualified native projection error = %v, want join_plan decline", err)
	}
	missingKey := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "missing")),
	).NewQuery().SelectColumns(dal.Column{Expression: dal.NewFieldRef("a", "id")})
	if err := canExecuteSQLiteJoin(context.Background(), missingKey, "sqlite", execute); err == nil || !strings.Contains(err.Error(), "join_field") {
		t.Fatalf("missing native ON key error = %v, want join_field decline", err)
	}
}

func TestCompileSQLJoinWhereInUsesPortableTypedPredicates(t *testing.T) {
	sources := map[string]struct{}{"a": {}}
	field := dal.NewFieldRef("a", "id")
	condition := dal.NewComparison(field, dal.In, dal.Array{Value: []any{nil, 1, "one"}})
	text, args, err := compileSQLConditionWithSources(condition, sources, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, " IS ?") || !strings.Contains(text, "CAST(? AS REAL)") || !strings.Contains(text, " OR ") {
		t.Fatalf("portable IN SQL = %s", text)
	}
	if got, want := fmt.Sprint(args), "[<nil> 1 one]"; got != want {
		t.Fatalf("portable IN args = %s, want %s", got, want)
	}
	empty, args, err := compileSQLConditionWithSources(dal.NewComparison(field, dal.In, dal.Array{Value: []any{}}), sources, true)
	if err != nil || empty != "0 = 1" || len(args) != 0 {
		t.Fatalf("empty IN = %q %#v %v", empty, args, err)
	}
	nullEqual, args, err := compileSQLConditionWithSources(dal.NewComparison(field, dal.Equal, dal.NewConstant(nil)), sources, true)
	if err != nil || nullEqual != "(+`a`.`id` COLLATE BINARY) IS NULL" || len(args) != 0 {
		t.Fatalf("null equality = %q %#v %v", nullEqual, args, err)
	}
	nullRange, args, err := compileSQLConditionWithSources(dal.NewComparison(field, dal.GreaterThen, dal.NewConstant(nil)), sources, true)
	if err != nil || nullRange != "0 = 1" || len(args) != 0 {
		t.Fatalf("null range = %q %#v %v", nullRange, args, err)
	}
	textRange, _, err := compileSQLConditionWithSources(dal.NewComparison(field, dal.LessThen, dal.NewConstant("z")), sources, true)
	if err != nil || !strings.Contains(textRange, "typeof((+`a`.`id` COLLATE BINARY)) = 'text'") {
		t.Fatalf("text range = %q %v", textRange, err)
	}
	numberRange, _, err := compileSQLConditionWithSources(dal.NewComparison(field, dal.GreaterOrEqual, dal.NewConstant(1)), sources, true)
	if err != nil || !strings.Contains(numberRange, "typeof(") || !strings.Contains(numberRange, "'integer','real'") {
		t.Fatalf("numeric range = %q %v", numberRange, err)
	}
	for _, test := range []struct {
		name      string
		condition dal.Condition
		want      string
	}{
		{name: "constant left", condition: dal.NewComparison(dal.NewConstant(1), dal.Equal, dal.NewConstant(1)), want: "comparison left operand"},
		{name: "nonarray IN", condition: dal.NewComparison(field, dal.In, dal.NewConstant(1)), want: "IN requires an array"},
		{name: "boolean IN", condition: dal.NewComparison(field, dal.In, dal.Array{Value: []any{true}}), want: "boolean predicates"},
		{name: "boolean comparison", condition: dal.NewComparison(field, dal.Equal, dal.NewConstant(true)), want: "boolean predicates"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := compileSQLConditionWithSources(test.condition, sources, true); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("condition error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileStructuredSQLJoinRejectsIncompletePlanningInputs(t *testing.T) {
	if _, _, err := compileStructuredSQL(nil); err == nil || !strings.Contains(err.Error(), "requires a source") {
		t.Fatalf("nil structured query error = %v", err)
	}
	root := dal.From(dal.NewRootCollectionRef("A", "a"))
	root.Join(dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinInner, sqlJoin("a", "id", "b", "a_id")))
	cursor := root.NewQuery().StartFrom("cursor").SelectIntoRecord(nil)
	if _, _, err := compileStructuredSQL(cursor); err == nil || !strings.Contains(err.Error(), "unsupported cursor") {
		t.Fatalf("joined cursor error = %v", err)
	}
	invalidAggregate := root.NewQuery().SelectColumns(
		dal.CountAs(dal.Star(), "count"),
		dal.Column{Expression: dal.NewFieldRef("a", "id"), Alias: "id"},
	)
	if _, _, err := compileStructuredSQL(invalidAggregate); err == nil || !strings.Contains(err.Error(), "invalid aggregation") {
		t.Fatalf("invalid joined aggregation error = %v", err)
	}
	unsupportedJoin := dal.From(dal.NewRootCollectionRef("A", "a")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("B", "b"), dal.JoinRight, sqlJoin("a", "id", "b", "a_id")),
	).NewQuery().SelectIntoRecord(nil)
	if _, _, err := compileStructuredSQL(unsupportedJoin); err == nil || !strings.Contains(err.Error(), "join_type") {
		t.Fatalf("unsupported JOIN type error = %v", err)
	}
}

func TestCompileSQLJoinTableSourcesHandleCollectionPointersSafely(t *testing.T) {
	source := dal.NewQualifiedRootCollectionRef("main", "Invoice", "i")
	compiled, err := compileSQLTableSource(&source)
	if err != nil || compiled != "`main`.`Invoice` AS `i`" {
		t.Fatalf("pointer collection source = %q, %v", compiled, err)
	}
	var nilSource *dal.CollectionRef
	if _, err := compileSQLTableSource(nilSource); err == nil || !strings.Contains(err.Error(), "unsupported structured SQL source") {
		t.Fatalf("nil collection source error = %v", err)
	}
}
