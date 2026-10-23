package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// typedNumberedCatalogColumns are the columns the MySQL and Oracle catalog queries return.
var typedNumberedCatalogColumns = []string{"k", "COLUMN_NAME", "DATA_TYPE", "not_null", "pk", "collation", "bytewise"}

func TestTypedDialectFor(t *testing.T) {
	for _, tc := range []struct {
		options DbOptions
		want    typedDialect
		err     string
	}{
		{options: DbOptions{StructuredQueryDialect: "postgres"}, want: newPostgresDialect(postgresExact)},
		{options: DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower}, want: newPostgresDialect(postgresFoldLower)},
		{options: DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: "upper"}, err: `unsupported identifier case "upper"`},
		{options: DbOptions{StructuredQueryDialect: "mysql"}, want: mysqlDialect{}},
		{options: DbOptions{StructuredQueryDialect: "mysql", IdentifierCase: IdentifierCaseExact}, want: mysqlDialect{}},
		{options: DbOptions{StructuredQueryDialect: "mysql", IdentifierCase: IdentifierCaseFoldLower}, err: `unsupported identifier case "fold-lower" for the mysql dialect`},
		{options: DbOptions{StructuredQueryDialect: "oracle"}, want: oracleDialect{}},
		{options: DbOptions{StructuredQueryDialect: "oracle", IdentifierCase: "upper"}, err: `unsupported identifier case "upper" for the oracle dialect`},
		{options: DbOptions{StructuredQueryDialect: "sqlserver"}, want: sqlserverDialect{}},
		{options: DbOptions{StructuredQueryDialect: "sqlite"}, err: `unsupported structured query dialect "sqlite"`},
	} {
		got, err := typedDialectFor(tc.options)
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("typedDialectFor(%+v) error = %v, want %q", tc.options, err, tc.err)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("typedDialectFor(%+v) = %#v, %v; want %#v", tc.options, got, err, tc.want)
		}
	}
	for name, typed := range map[string]bool{"postgres": true, "mysql": true, "oracle": true, "sqlserver": true, "sqlite": false, "": false, "mssql": false} {
		if isTypedDialect(name) != typed {
			t.Fatalf("isTypedDialect(%q) = %v", name, !typed)
		}
	}
	for dialect, name := range map[string]string{"postgres": "PostgreSQL", "mysql": "MySQL", "oracle": "Oracle", "sqlserver": "SQL Server"} {
		if got := typedEngineName(dialect); got != name {
			t.Fatalf("typedEngineName(%q) = %q, want %q", dialect, got, name)
		}
	}
}

func TestMySQLAndOracleDialectFragments(t *testing.T) {
	for _, tc := range []struct {
		dialect                 typedDialect
		asc, desc, notNullAsc   string
		aggregate, count, empty string
		notEmpty                string
	}{
		{mysqlDialect{}, "x ASC", "x DESC", "x ASC", "SUM(x)", "COUNT(x)", "FALSE", "TRUE"},
		{oracleDialect{}, "x ASC NULLS FIRST", "x DESC NULLS LAST", "x ASC", "(CAST(SUM(x) AS BINARY_DOUBLE))", "COUNT(x)", "(1 = 0)", "(1 = 1)"},
		{sqlserverDialect{}, "x ASC", "x DESC", "x ASC", "SUM(x)", "COUNT(x)", "(1 = 0)", "(1 = 1)"},
	} {
		d := tc.dialect
		if got := d.orderItem("x", false, false); got != tc.asc {
			t.Fatalf("%T.orderItem asc = %q", d, got)
		}
		if got := d.orderItem("x", true, false); got != tc.desc {
			t.Fatalf("%T.orderItem desc = %q", d, got)
		}
		if got := d.orderItem("x", false, true); got != tc.notNullAsc {
			t.Fatalf("%T.orderItem not null = %q", d, got)
		}
		if got := d.aggregateResult(dal.SUM, "SUM(x)"); got != tc.aggregate {
			t.Fatalf("%T.aggregateResult = %q", d, got)
		}
		if got := d.aggregateResult(dal.COUNT, "COUNT(x)"); got != tc.count {
			t.Fatalf("%T.aggregateResult count = %q", d, got)
		}
		if d.emptyIn(false) != tc.empty || d.emptyIn(true) != tc.notEmpty {
			t.Fatalf("%T.emptyIn = %q, %q", d, d.emptyIn(false), d.emptyIn(true))
		}
		if d.capabilities() != (postgresDialect{}).capabilities() {
			t.Fatalf("%T.capabilities = %+v", d, d.capabilities())
		}
		if _, err := d.window("ROW_NUMBER", nil, nil, nil); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("%T.window error = %v", d, err)
		}
		if clause, args := d.limitOffset(0, 0); clause != "" || args != nil {
			t.Fatalf("%T.limitOffset(0, 0) = %q, %v", d, clause, args)
		}
		if _, err := d.quoteIdent(""); err == nil {
			t.Fatalf("%T.quoteIdent accepted an empty name", d)
		}
		if _, _, err := d.bind(struct{}{}); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("%T.bind error = %v", d, err)
		}
	}
	rules := []struct {
		rules           typedEngineRules
		alias           string
		byPosition      bool
		qualifiesFields bool
		paging          string
		average         string
	}{
		{typedPostgresRules{}, " AS ", true, false, "", "x"},
		{mysqlDialect{}, " AS ", true, true, "", "CAST((x) AS DOUBLE)"},
		{oracleDialect{}, " ", false, true, "", "x"},
		{sqlserverDialect{}, " AS ", false, true, "(SELECT NULL)", "CAST((x) AS FLOAT)"},
	}
	for _, tc := range rules {
		if tc.rules.sourceAliasKeyword() != tc.alias || tc.rules.groupByPosition() != tc.byPosition || tc.rules.qualifiesBareFields() != tc.qualifiesFields ||
			tc.rules.pagingOrderBy() != tc.paging || tc.rules.aggregateArgument(dal.AVERAGE, "x") != tc.average || tc.rules.aggregateArgument(dal.MIN, "x") != "x" {
			t.Fatalf("%T rules = %q, %v, %v", tc.rules, tc.rules.sourceAliasKeyword(), tc.rules.groupByPosition(), tc.rules.qualifiesBareFields())
		}
	}
	if _, ok := typedRulesOf(newPostgresDialect(postgresExact)).(typedPostgresRules); !ok {
		t.Fatal("the PostgreSQL dialect does not read as PostgreSQL")
	}
}

func TestTypedCatalogInt(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  int64
		ok    bool
	}{
		{int64(7), 7, true}, {float64(3), 3, true}, {true, 1, true}, {false, 0, true},
		{[]byte("12"), 12, true}, {"-4", -4, true},
		{1.5, 0, false}, {float64(1 << 60), 0, false}, {"x", 0, false}, {nil, 0, false}, {int32(1), 0, false},
	} {
		got, err := typedCatalogInt(tc.value)
		if tc.ok != (err == nil) || got != tc.want {
			t.Fatalf("typedCatalogInt(%#v) = %d, %v", tc.value, got, err)
		}
	}
}

func TestMySQLAndOracleTypeCategories(t *testing.T) {
	for _, tc := range []struct {
		category func(string) (typedTypeCategory, bool)
		dataType string
		want     typedTypeCategory
		noJoin   bool
	}{
		{mysqlTypeCategory, "VARCHAR", typedTypeText, false},
		{mysqlTypeCategory, "bigint", typedTypeNumber, false},
		{mysqlTypeCategory, "datetime", typedTypeTime, false},
		{mysqlTypeCategory, "varbinary", typedTypeBinary, false},
		{mysqlTypeCategory, "json", typedTypeOther, true},
		{mysqlTypeCategory, "point", typedTypeOther, true},
		{mysqlTypeCategory, "enum", typedTypeOther, false},
		{oracleTypeCategory, "NVARCHAR2", typedTypeText, false},
		{oracleTypeCategory, "NUMBER", typedTypeNumber, false},
		{oracleTypeCategory, "BINARY_DOUBLE", typedTypeNumber, false},
		{oracleTypeCategory, "DATE", typedTypeTime, false},
		{oracleTypeCategory, "TIMESTAMP(6) WITH TIME ZONE", typedTypeTime, false},
		{oracleTypeCategory, "TIMESTAMP(6) WITH LOCAL TIME ZONE", typedTypeOther, false},
		{oracleTypeCategory, "RAW", typedTypeBinary, false},
		{oracleTypeCategory, "BOOLEAN", typedTypeBoolean, false},
		{oracleTypeCategory, "CLOB", typedTypeOther, true},
		{oracleTypeCategory, "JSON", typedTypeOther, true},
		{oracleTypeCategory, "INTERVAL DAY(2) TO SECOND(6)", typedTypeOther, false},
		{sqlserverTypeCategory, "nvarchar", typedTypeText, false},
		{sqlserverTypeCategory, "money", typedTypeNumber, false},
		{sqlserverTypeCategory, "datetime2", typedTypeTime, false},
		{sqlserverTypeCategory, "varbinary", typedTypeBinary, false},
		{sqlserverTypeCategory, "bit", typedTypeBoolean, false},
		{sqlserverTypeCategory, "ntext", typedTypeOther, true},
		{sqlserverTypeCategory, "uniqueidentifier", typedTypeOther, false},
	} {
		got, noJoin := tc.category(tc.dataType)
		if got != tc.want || noJoin != tc.noJoin {
			t.Fatalf("category(%q) = %v, %v; want %v, %v", tc.dataType, got, noJoin, tc.want, tc.noJoin)
		}
	}
}

// numberedCatalogCase is one dialect's catalog lookup, as the mock expects it.
type numberedCatalogCase struct {
	name    string
	dialect typedDialect
	query   func(int) string
	// albumAndArtist are the arguments of a lookup of Album and S.Artist.
	albumAndArtist []driver.Value
}

var numberedCatalogCases = []numberedCatalogCase{
	{"mysql", mysqlDialect{}, mysqlCatalogQuery, []driver.Value{nil, "Album", "S", "Artist"}},
	{"oracle", oracleDialect{}, oracleCatalogQuery, []driver.Value{"", "Album", "S", "Artist"}},
	{"sqlserver", sqlserverDialect{}, sqlserverCatalogQuery, []driver.Value{"[Album]", "[S].[Artist]"}},
}

func TestMySQLAndOracleCatalogFacts(t *testing.T) {
	ctx := context.Background()
	for _, tc := range numberedCatalogCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			sources := []typedSourceName{{Name: "Album"}, {Schema: "S", Name: "Artist"}, {Name: "Album"}}
			mock.ExpectQuery(tc.query(2)).WithArgs(tc.albumAndArtist...).
				WillReturnRows(sqlmock.NewRows(typedNumberedCatalogColumns).
					AddRow(int64(1), "AlbumId", "int", int64(1), int64(1), int64(0), int64(1)).
					AddRow("1", "Title", "varchar", []byte("0"), false, int64(255), int64(0)).
					AddRow(float64(2), "Name", "VARCHAR2", true, int64(0), "0", true))
			facts, err := tc.dialect.catalogFacts(ctx, db.QueryContext, sources)
			if err != nil {
				t.Fatal(err)
			}
			if facts.Fold != nil {
				t.Fatal("the dialect folds no name, yet its facts carry a fold")
			}
			album, _ := facts.source(typedSourceName{Name: "Album"})
			artist, _ := facts.source(typedSourceName{Schema: "S", Name: "Artist"})
			if len(album.Columns) != 2 || len(artist.Columns) != 1 {
				t.Fatalf("facts = %+v", facts)
			}
			if id := album.Columns[0]; !id.NotNull || !id.PrimaryKey || id.NonDeterministicCollation {
				t.Fatalf("AlbumId = %+v", id)
			}
			if title := album.Columns[1]; title.NotNull || title.PrimaryKey || !title.NonDeterministicCollation {
				t.Fatalf("Title = %+v", title)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMySQLAndOracleCatalogFactsFailures(t *testing.T) {
	ctx := context.Background()
	album := []typedSourceName{{Name: "Album"}}
	for _, tc := range numberedCatalogCases {
		t.Run(tc.name, func(t *testing.T) {
			if facts, err := tc.dialect.catalogFacts(ctx, nil, nil); err != nil || facts.Sources != nil {
				t.Fatalf("no sources = %+v, %v", facts, err)
			}
			if _, err := tc.dialect.catalogFacts(ctx, nil, []typedSourceName{{Name: ""}}); err == nil || !strings.Contains(err.Error(), "table name") {
				t.Fatalf("an empty table name = %v", err)
			}
			if _, err := tc.dialect.catalogFacts(ctx, nil, []typedSourceName{{Schema: strings.Repeat("s", 200), Name: "t"}}); err == nil || !strings.Contains(err.Error(), "schema name") {
				t.Fatalf("a long schema name = %v", err)
			}
			for name, rows := range map[string]*sqlmock.Rows{
				"a row for a source not asked for": sqlmock.NewRows(typedNumberedCatalogColumns).AddRow(int64(2), "c", "int", int64(0), int64(0), int64(0), int64(1)),
				"a number the catalog cannot give": sqlmock.NewRows(typedNumberedCatalogColumns).AddRow(int64(1), "c", "int", "yes", int64(0), int64(0), int64(1)),
				"a row that does not scan":         sqlmock.NewRows([]string{"k"}).AddRow(int64(1)),
				"an error while reading":           sqlmock.NewRows(typedNumberedCatalogColumns).AddRow(int64(1), "c", "int", int64(0), int64(0), int64(0), int64(1)).RowError(0, errors.New("broken")),
			} {
				db, mock := newPostgresReadMock(t)
				mock.ExpectQuery(tc.query(1)).WillReturnRows(rows)
				if _, err := tc.dialect.catalogFacts(ctx, db.QueryContext, album); err == nil || !strings.HasPrefix(err.Error(), "catalog facts: ") {
					t.Fatalf("%s: error = %v", name, err)
				}
			}
			db, mock := newPostgresReadMock(t)
			mock.ExpectQuery(tc.query(1)).WillReturnError(errors.New("down"))
			if _, err := tc.dialect.catalogFacts(ctx, db.QueryContext, album); err == nil || !strings.Contains(err.Error(), "down") {
				t.Fatalf("a failed lookup = %v", err)
			}
		})
	}
}

func TestMySQLAndOracleSuggestSource(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		dialect typedDialect
		query   string
		none    any
	}{
		{"mysql", mysqlDialect{}, mysqlSuggestionQuery, nil},
		{"oracle", oracleDialect{}, oracleSuggestionQuery, ""},
		{"sqlserver", sqlserverDialect{}, sqlserverSuggestionQuery, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidates := func() *sqlmock.Rows {
				return sqlmock.NewRows([]string{"schema", "name"}).AddRow("app", "ALBUM").AddRow("other", "Artist").AddRow("app", "Invoice")
			}
			for _, c := range []struct {
				source typedSourceName
				args   []driver.Value
				want   typedSourceName
				found  bool
			}{
				{typedSourceName{Name: "Album"}, []driver.Value{tc.none, tc.none}, typedSourceName{Name: "ALBUM"}, true},
				{typedSourceName{Name: "Invoce"}, []driver.Value{tc.none, tc.none}, typedSourceName{Name: "Invoice"}, true},
				{typedSourceName{Schema: "app", Name: "Artist"}, []driver.Value{"app", "app"}, typedSourceName{Schema: "other", Name: "Artist"}, true},
				{typedSourceName{Name: "Nothing"}, []driver.Value{tc.none, tc.none}, typedSourceName{}, false},
			} {
				db, mock := newPostgresReadMock(t)
				mock.ExpectQuery(tc.query).WithArgs(c.args...).WillReturnRows(candidates())
				got, found, err := tc.dialect.suggestSource(ctx, db.QueryContext, c.source)
				if err != nil || found != c.found || got != c.want {
					t.Fatalf("suggestSource(%+v) = %+v, %v, %v; want %+v, %v", c.source, got, found, err, c.want, c.found)
				}
			}
			for name, expect := range map[string]func(sqlmock.Sqlmock){
				"a failed lookup": func(m sqlmock.Sqlmock) { m.ExpectQuery(tc.query).WillReturnError(errors.New("down")) },
				"a row that does not scan": func(m sqlmock.Sqlmock) {
					m.ExpectQuery(tc.query).WillReturnRows(sqlmock.NewRows([]string{"schema"}).AddRow("x"))
				},
				"an error while reading": func(m sqlmock.Sqlmock) {
					m.ExpectQuery(tc.query).WillReturnRows(sqlmock.NewRows([]string{"schema", "name"}).AddRow("a", "b").RowError(0, errors.New("broken")))
				},
			} {
				db, mock := newPostgresReadMock(t)
				expect(mock)
				if _, _, err := tc.dialect.suggestSource(ctx, db.QueryContext, typedSourceName{Name: "x"}); err == nil || !strings.HasPrefix(err.Error(), "suggest a table: ") {
					t.Fatalf("%s: error = %v", name, err)
				}
			}
		})
	}
}

// A read with the MySQL or Oracle dialect asks the catalog first, on one pinned
// connection, then runs the statement the typed compiler wrote from the facts; a
// table the catalog does not know is a TableNotFoundError with the name that
// differs only in case suggested.
func TestMySQLAndOracleReadsGoThroughTheTypedCompiler(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		dialect   string
		catalog   string
		none      any
		statement string
		suggest   string
	}{
		{"mysql", mysqlCatalogQuery(1), nil, "SELECT `Album`.`Title`, `Album`.`AlbumId` AS `__dalgo_record_id` FROM `Album` WHERE `Album`.`AlbumId` > ? ORDER BY `Album`.`AlbumId` ASC LIMIT ?", mysqlSuggestionQuery},
		{"oracle", oracleCatalogQuery(1), "", `SELECT "Album"."Title", "Album"."AlbumId" AS "__dalgo_record_id" FROM "Album" WHERE "Album"."AlbumId" > :1 ORDER BY "Album"."AlbumId" ASC FETCH FIRST :2 ROWS ONLY`, oracleSuggestionQuery},
		{"sqlserver", sqlserverCatalogQuery(1), nil, `SELECT "Album"."Title", "Album"."AlbumId" AS "__dalgo_record_id" FROM "Album" WHERE "Album"."AlbumId" > @p1 ORDER BY "Album"."AlbumId" ASC OFFSET @p2 ROWS FETCH NEXT @p3 ROWS ONLY`, sqlserverSuggestionQuery},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			catalogArgs, statementArgs := []driver.Value{tc.none, "Album"}, []driver.Value{int64(1), int64(5)}
			if tc.dialect == "sqlserver" {
				catalogArgs, statementArgs = []driver.Value{"[Album]"}, []driver.Value{int64(1), int64(0), int64(5)}
			}
			mock.ExpectQuery(tc.catalog).WithArgs(catalogArgs...).
				WillReturnRows(sqlmock.NewRows(typedNumberedCatalogColumns).
					AddRow(int64(1), "AlbumId", "int", int64(1), int64(1), int64(0), int64(1)).
					AddRow(int64(1), "Title", "varchar", int64(1), int64(0), int64(0), int64(1)))
			mock.ExpectQuery(tc.statement).WithArgs(statementArgs...).
				WillReturnRows(sqlmock.NewRows([]string{"Title", "__dalgo_record_id"}).AddRow("One", int64(1)))
			database := NewDatabase(db, NewSimpleSchema("ID"), DbOptions{StructuredQueryDialect: tc.dialect})
			q := typedTestFrom("Album", "").NewQuery().
				Where(dal.NewComparison(typedTestField("AlbumId"), dal.GreaterThen, typedTestConst(1))).
				OrderBy(dal.Ascending(typedTestField("AlbumId"))).Limit(5).
				SelectColumns(typedTestColumn(typedTestField("Title"), ""))
			reader, err := database.ExecuteQueryToRecordsReader(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if got := record.Data(); !reflect.DeepEqual(got, map[string]any{"Title": "One"}) {
				t.Fatalf("record = %#v", got)
			}
			if id := record.Key().ID; id != int64(1) {
				t.Fatalf("the record is keyed %#v, want the primary key the catalog names", id)
			}
			_ = reader.Close()

			mock.ExpectQuery(tc.catalog).WillReturnRows(sqlmock.NewRows(typedNumberedCatalogColumns))
			mock.ExpectQuery(tc.suggest).WillReturnRows(sqlmock.NewRows([]string{"schema", "name"}).AddRow("app", "ALBUM"))
			_, err = database.ExecuteQueryToRecordsReader(ctx, q)
			var notFound *TableNotFoundError
			if !errors.As(err, &notFound) || notFound.SuggestedName != "ALBUM" || !notFound.CaseSensitive {
				t.Fatalf("an unknown table = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The join check and the join fields of the generic engine read the same catalog.
func TestMySQLAndOracleJoinCheckAndJoinFields(t *testing.T) {
	ctx := context.Background()
	for _, dialect := range []string{"mysql", "oracle", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			options := DbOptions{StructuredQueryDialect: dialect}
			catalog, _ := typedDialectFor(options)
			query := map[string]func(int) string{"mysql": mysqlCatalogQuery, "oracle": oracleCatalogQuery, "sqlserver": sqlserverCatalogQuery}[dialect]
			db, mock := newPostgresReadMock(t)
			database := &database{db: db, options: options}

			mock.ExpectQuery(query(1)).WillReturnRows(sqlmock.NewRows(typedNumberedCatalogColumns).
				AddRow(int64(1), "ArtistId", "int", int64(1), int64(1), int64(0), int64(1)).
				AddRow(int64(1), "Name", "varchar", int64(0), int64(0), int64(0), int64(1)))
			fields, err := database.JoinFields(ctx, dal.NewRootCollectionRef("Artist", ""))
			if err != nil || !reflect.DeepEqual(fields, []string{"ArtistId", "Name"}) {
				t.Fatalf("JoinFields = %v, %v", fields, err)
			}

			join := typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "Name"))).
				NewQuery().SelectColumns()
			mock.ExpectQuery(query(2)).WillReturnRows(sqlmock.NewRows(typedNumberedCatalogColumns).
				AddRow(int64(1), "ArtistId", "int", int64(1), int64(0), int64(0), int64(1)).
				AddRow(int64(2), "ArtistId", "int", int64(1), int64(1), int64(0), int64(1)).
				AddRow(int64(2), "Name", "varchar", int64(0), int64(0), int64(0), int64(1)))
			err = database.CanExecuteJoin(ctx, join)
			if err == nil || !strings.Contains(err.Error(), "join_plan: "+typedEngineName(dialect)+" cannot natively compile query") || !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("CanExecuteJoin = %v", err)
			}
			if _, ok := catalog.(typedEngineRules); !ok {
				t.Fatal("the dialect has no engine rules")
			}
			if recordNameFold(options) != nil {
				t.Fatal("a dialect that folds no name has a fold")
			}
		})
	}
}

// badAggregateArgument is a dialect whose aggregate argument drops the operand, and the
// value it carries with it.
type badAggregateArgument struct{ sqlserverDialect }

func (badAggregateArgument) aggregateArgument(string, string) string { return "1" }

// An aggregate argument that breaks the operand rules is refused, as every fragment is.
func TestTypedCompilerRefusesAnAggregateArgumentThatDropsItsOperand(t *testing.T) {
	q := typedTestFrom("Invoice", "").NewQuery().SelectColumns(dal.SumAs(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1)), "s"))
	if _, _, err := compileTypedSQL(q, badAggregateArgument{}, typedCatalogFacts{}); err == nil || !strings.Contains(err.Error(), "placeholders") {
		t.Fatalf("error = %v, want the fragment refused", err)
	}
}

// Two spellings of one relation (Album and S.Album, where S is the current schema) are
// two sources, and the catalog answers each with every column.
func TestMySQLAndOracleCatalogFactsForTwoSpellingsOfOneRelation(t *testing.T) {
	for _, tc := range numberedCatalogCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			rows := sqlmock.NewRows(typedNumberedCatalogColumns)
			for k := int64(1); k <= 2; k++ {
				rows.AddRow(k, "AlbumId", "int", int64(1), int64(1), int64(0), int64(1))
			}
			mock.ExpectQuery(tc.query(2)).WillReturnRows(rows)
			facts, err := tc.dialect.catalogFacts(context.Background(), db.QueryContext, []typedSourceName{{Name: "Album"}, {Schema: "S", Name: "Album"}})
			if err != nil {
				t.Fatal(err)
			}
			for _, source := range []typedSourceName{{Name: "Album"}, {Schema: "S", Name: "Album"}} {
				if known, ok := facts.source(source); !ok || len(known.Columns) != 1 {
					t.Fatalf("%+v = %+v", source, known)
				}
			}
		})
	}
}

// NaN and the infinities are refused by every dialect whose engine has none, and Oracle
// refuses the empty string and empty bytes, which it reads as NULL.
func TestMySQLOracleSQLServerRefuseConstantsTheEngineCannotHold(t *testing.T) {
	for _, dialect := range []typedDialect{mysqlDialect{}, oracleDialect{}, sqlserverDialect{}} {
		for _, value := range []any{math.NaN(), math.Inf(1), float32(math.Inf(-1))} {
			if _, _, err := dialect.bind(value); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("%T.bind(%v) error = %v", dialect, value, err)
			}
		}
	}
	for _, value := range []any{"", []byte{}} {
		if _, _, err := (oracleDialect{}).bind(value); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("oracle bind(%#v) error = %v", value, err)
		}
	}
	if marker, arg, err := (oracleDialect{}).bind([]byte{1}); err != nil || marker != "?" || !reflect.DeepEqual(arg, []byte{1}) {
		t.Fatalf("oracle bind(bytes) = %q, %v, %v", marker, arg, err)
	}
}

// go-ora delivers a NUMBER as decimal text; both readers turn it into an int64 when the
// column's scale is 0 or unconstrained and the text is whole, else a float64, or keep
// the text with ExactNumericValues.
func TestOracleNumbersAreReadAsNumbers(t *testing.T) {
	ctx := context.Background()
	columns := func() *sqlmock.Rows {
		return sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("id").WithPrecisionAndScale(10, 0),
			sqlmock.NewColumn("n").WithPrecisionAndScale(38, 255),
			sqlmock.NewColumn("price").WithPrecisionAndScale(10, 2),
			sqlmock.NewColumn("big").WithPrecisionAndScale(38, 0),
			sqlmock.NewColumn("odd").WithPrecisionAndScale(10, 2),
			sqlmock.NewColumn("title"),
			sqlmock.NewColumn("double").WithPrecisionAndScale(10, 2),
		).AddRow("7", []byte("2"), "9.99", "99999999999999999999", "x", "Rock", 1.5)
	}
	query := dal.NewTextQuery("SELECT 1", nil)
	for _, tc := range []struct {
		exact bool
		want  map[string]any
	}{
		{false, map[string]any{"id": int64(7), "n": int64(2), "price": 9.99, "big": 1e20, "odd": "x", "title": "Rock", "double": 1.5}},
		{true, map[string]any{"id": int64(7), "n": int64(2), "price": "9.99", "big": "99999999999999999999", "odd": "x", "title": "Rock", "double": 1.5}},
	} {
		db, mock := newPostgresReadMock(t)
		mock.ExpectQuery("SELECT 1").WillReturnRows(columns())
		reader, err := getRecordsReaderWithOptions(ctx, query, db.QueryContext, DbOptions{StructuredQueryDialect: "oracle", ExactNumericValues: tc.exact})
		if err != nil {
			t.Fatal(err)
		}
		record, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := record.Data(); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("exact %v: record = %#v, want %#v", tc.exact, got, tc.want)
		}
		_ = reader.Close()
	}

	db, mock := newPostgresReadMock(t)
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("NUMBER", int64(0)).WithPrecisionAndScale(10, 0),
		sqlmock.NewColumn("price").OfType("NUMBER", float64(0)).WithPrecisionAndScale(10, 2),
	).AddRow("7", "9.99"))
	reader, err := getRecordsetReaderWithOptions(ctx, query, db.QueryContext, DbOptions{StructuredQueryDialect: "oracle"})
	if err != nil {
		t.Fatal(err)
	}
	row, rs, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	id, _ := row.GetValueByIndex(0, rs)
	price, _ := row.GetValueByIndex(1, rs)
	if id != int64(7) || price != 9.99 {
		t.Fatalf("recordset row = %#v, %#v", id, price)
	}
	_ = reader.Close()
	if got := oracleNumberValue(nil, "7", false); got != "7" {
		t.Fatalf("no column type = %#v", got)
	}
}
