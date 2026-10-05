package dalgo2sql

import (
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

func TestCompileTypedSQLSelectList(t *testing.T) {
	runTypedGoldens(t, []typedGolden{
		{
			name:    "no columns selects star",
			query:   typedTestFrom("Album", "").NewQuery().SelectColumns(),
			wantSQL: `SELECT * FROM "Album"`,
		},
		{
			name: "fields and aliases are quoted",
			query: typedTestFrom("Album", "").NewQuery().SelectColumns(
				typedTestColumn(typedTestField("Title"), ""),
				typedTestColumn(typedTestField("AlbumId"), "id"),
			),
			wantSQL: `SELECT "Title", "AlbumId" AS "id" FROM "Album"`,
		},
		{
			name: "schema and alias",
			query: dal.From(dal.NewQualifiedRootCollectionRef("public", "Album", "a")).NewQuery().SelectColumns(
				typedTestColumn(typedTestQualified("a", "Title"), ""),
			),
			wantSQL: `SELECT "a"."Title" FROM "public"."Album" AS "a"`,
		},
		{
			name:    "pointer collection source",
			query:   dal.From(typedTestPtr(dal.NewRootCollectionRef("Album", ""))).NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), "")),
			wantSQL: `SELECT "Title" FROM "Album"`,
		},
		{
			name: "constant with alias is bound",
			query: typedTestFrom("Album", "").NewQuery().SelectColumns(
				typedTestColumn(typedTestConst(5), "five"),
			),
			wantSQL:  `SELECT $1::bigint AS "five" FROM "Album"`,
			wantArgs: []any{5},
		},
		{
			name: "unaliased arithmetic is named by its text",
			query: typedTestFrom("Album", "").NewQuery().SelectColumns(
				typedTestColumn(dal.Binary(typedTestField("a"), dal.Add, typedTestField("b")), ""),
			),
			wantSQL: `SELECT ("a" + "b") AS "(a + b)" FROM "Album"`,
		},
	})
}

func TestCompileTypedSQLWildcardExpansion(t *testing.T) {
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Album"}: {Columns: []typedColumnFact{
			{Name: "AlbumId"}, {Name: "Title"}, {Name: "Secret"}, {Name: "SecretNote"},
		}},
	}}
	runTypedGoldens(t, []typedGolden{
		{
			name:    "exact exclusion",
			facts:   facts,
			query:   typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept("Secret", "SecretNote")),
			wantSQL: `SELECT "AlbumId", "Title" FROM "Album"`,
		},
		{
			name:  "mask exclusion keeps explicit columns after the wildcard",
			facts: facts,
			query: typedTestFrom("Album", "").NewQuery().SelectColumns(
				dal.AllColumnsExcept("secret*"),
				typedTestColumn(typedTestField("Title"), "again"),
			),
			wantSQL: `SELECT "AlbumId", "Title", "Title" AS "again" FROM "Album"`,
		},
	})
	// Without a fold the dialect matches names exactly, and so does an exclusion.
	t.Run("an exclusion is exact when nothing folds", func(t *testing.T) {
		typedGolden{
			facts:   facts,
			query:   typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept("secret", "SECRETNOTE")),
			wantSQL: `SELECT "AlbumId", "Title", "Secret", "SecretNote" FROM "Album"`,
		}.run(t)
	})
	t.Run("without facts the wildcard is unsupported", func(t *testing.T) {
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept("Secret"))
		expectTypedUnsupported(t, q, nil, typedCatalogFacts{}, "catalog facts")
	})
	t.Run("excluding every column is unsupported", func(t *testing.T) {
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept("*"))
		expectTypedUnsupported(t, q, nil, facts, "every column")
	})
	t.Run("a catalog name the dialect refuses fails the expansion", func(t *testing.T) {
		bad := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{{Name: "Album"}: {Columns: []typedColumnFact{{Name: ""}}}}}
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept("Secret"))
		expectTypedInvalid(t, q, nil, bad, "column 0")
	})
	t.Run("a malformed wildcard is unsupported", func(t *testing.T) {
		q := typedTestFrom("Album", "").NewQuery().SelectColumns(dal.AllColumnsExcept())
		expectTypedUnsupported(t, q, nil, facts, "at least one exclusion")
	})
}

func TestCompileTypedSQLFrom(t *testing.T) {
	nested := func(onSource string) dal.StructuredQuery {
		employees := dal.From(dal.NewQualifiedRootCollectionRef("main", "Employee", "e"))
		customers := dal.From(dal.NewQualifiedRootCollectionRef("main", "Customer", "c")).Join(
			dal.NewJoinedFrom(employees, dal.JoinLeft, typedTestJoinOn(onSource, "SupportRepId", "e", "EmployeeId")),
		)
		return dal.From(dal.NewQualifiedRootCollectionRef("main", "Invoice", "i")).Join(
			dal.NewJoinedFrom(customers, dal.JoinInner, typedTestJoinOn("i", "CustomerId", "c", "CustomerId")),
		).NewQuery().SelectColumns(
			typedTestColumn(typedTestQualified("i", "InvoiceId"), "invoice_id"),
			typedTestColumn(typedTestQualified("e", "FirstName"), "employee"),
		)
	}
	runTypedGoldens(t, []typedGolden{
		{
			name: "inner join",
			query: typedTestFrom("Album", "a").Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
			).NewQuery().SelectColumns(
				typedTestColumn(typedTestQualified("a", "Title"), ""),
				typedTestColumn(typedTestQualified("r", "Name"), "artist"),
			),
			wantSQL: `SELECT "a"."Title", "r"."Name" AS "artist" FROM "Album" AS "a" INNER JOIN "Artist" AS "r" ON ("a"."ArtistId" = "r"."ArtistId")`,
		},
		{
			name: "left join with two key pairs and a default join type",
			query: typedTestFrom("Album", "a").Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinLeft,
					typedTestJoinOn("a", "ArtistId", "r", "ArtistId"), typedTestJoinOn("a", "Region", "r", "Region")),
			).Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Genre", "g"), "", typedTestJoinOn("a", "GenreId", "g", "GenreId")),
			).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("g", "Name"), "")),
			wantSQL: `SELECT "g"."Name" FROM "Album" AS "a" LEFT JOIN "Artist" AS "r" ON ("a"."ArtistId" = "r"."ArtistId" AND "a"."Region" = "r"."Region") INNER JOIN "Genre" AS "g" ON ("a"."GenreId" = "g"."GenreId")`,
		},
		{
			name:  "nested join keeps its parentheses",
			query: nested("c"),
			wantSQL: `SELECT "i"."InvoiceId" AS "invoice_id", "e"."FirstName" AS "employee" FROM "main"."Invoice" AS "i" ` +
				`INNER JOIN ("main"."Customer" AS "c" LEFT JOIN "main"."Employee" AS "e" ON ("c"."SupportRepId" = "e"."EmployeeId")) ` +
				`ON ("i"."CustomerId" = "c"."CustomerId")`,
		},
	})
	t.Run("a nested ON naming an outer alias is refused", func(t *testing.T) {
		expectTypedUnsupported(t, nested("i"), nil, typedCatalogFacts{}, "outside its subtree")
	})
}

func TestCompileTypedSQLWhere(t *testing.T) {
	stamp := time.Date(2026, 10, 4, 12, 30, 0, 0, time.UTC)
	hostile := `O'Brien'; DROP TABLE x; --`
	album := func(conditions ...dal.Condition) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().Where(conditions...).SelectColumns(typedTestColumn(typedTestField("Title"), ""))
	}
	const head = `SELECT "Title" FROM "Album" WHERE `
	runTypedGoldens(t, []typedGolden{
		{name: "equal", query: album(typedTestEq(typedTestField("AlbumId"), 5)), wantSQL: head + `"AlbumId" = $1::bigint`, wantArgs: []any{5}},
		{name: "greater", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.GreaterThen, typedTestConst(5))), wantSQL: head + `"AlbumId" > $1::bigint`, wantArgs: []any{5}},
		{name: "greater or equal", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.GreaterOrEqual, typedTestConst(5))), wantSQL: head + `"AlbumId" >= $1::bigint`, wantArgs: []any{5}},
		{name: "less", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.LessThen, typedTestConst(5))), wantSQL: head + `"AlbumId" < $1::bigint`, wantArgs: []any{5}},
		{name: "less or equal", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.LessOrEqual, typedTestConst(5))), wantSQL: head + `"AlbumId" <= $1::bigint`, wantArgs: []any{5}},
		{name: "fraction", query: album(typedTestEq(typedTestField("Price"), 9.99)), wantSQL: head + `"Price" = $1::numeric`, wantArgs: []any{9.99}},
		{name: "bool", query: album(typedTestEq(typedTestField("Active"), true)), wantSQL: head + `"Active" = $1::boolean`, wantArgs: []any{true}},
		{name: "time", query: album(typedTestEq(typedTestField("Released"), stamp)), wantSQL: head + `"Released" = $1::timestamptz`, wantArgs: []any{stamp}},
		{name: "bytes", query: album(typedTestEq(typedTestField("Blob"), []byte{1, 2})), wantSQL: head + `"Blob" = $1::bytea`, wantArgs: []any{[]byte{1, 2}}},
		{name: "text is untyped so dates and uuids work", query: album(typedTestEq(typedTestField("Title"), hostile)), wantSQL: head + `"Title" = $1`, wantArgs: []any{hostile}},
		{name: "equal null is IS NULL", query: album(typedTestEq(typedTestField("Title"), nil)), wantSQL: head + `"Title" IS NULL`},
		{name: "ordering against null stays unknown", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.GreaterThen, typedTestConst(nil))), wantSQL: head + `"AlbumId" > $1`, wantArgs: []any{nil}},
		{name: "field against field", query: album(dal.NewComparison(typedTestField("A"), dal.LessThen, typedTestField("B"))), wantSQL: head + `"A" < "B"`},
		{name: "is null", query: album(dal.NewIsNullCondition(typedTestField("Title"))), wantSQL: head + `"Title" IS NULL`},
		{name: "is not null", query: album(dal.NewIsNotNullCondition(typedTestField("Title"))), wantSQL: head + `"Title" IS NOT NULL`},
		{
			name:     "arithmetic on the left",
			query:    album(dal.NewComparison(dal.Binary(typedTestField("A"), dal.Add, typedTestConst(1)), dal.GreaterThen, typedTestConst(10))),
			wantSQL:  head + `("A" + $1::bigint) > $2::bigint`,
			wantArgs: []any{1, 10},
		},
		{
			name:     "is null of arithmetic",
			query:    album(dal.NewIsNullCondition(dal.Binary(typedTestField("A"), dal.Multiply, typedTestConst(2)))),
			wantSQL:  head + `("A" * $1::bigint) IS NULL`,
			wantArgs: []any{2},
		},
		{
			name:     "nested groups keep order and numbering",
			query:    album(dal.NewGroupCondition(dal.And, typedTestEq(typedTestField("A"), 1), dal.NewGroupCondition(dal.Or, dal.NewComparison(typedTestField("B"), dal.GreaterThen, typedTestConst(2)), dal.NewIsNullCondition(typedTestField("C"))))),
			wantSQL:  head + `("A" = $1::bigint AND ("B" > $2::bigint OR "C" IS NULL))`,
			wantArgs: []any{1, 2},
		},
		{
			name:     "several conditions are ANDed by the builder",
			query:    album(typedTestEq(typedTestField("A"), 1), typedTestEq(typedTestField("B"), "x")),
			wantSQL:  head + `("A" = $1::bigint AND "B" = $2)`,
			wantArgs: []any{1, "x"},
		},
		{
			name:     "IN",
			query:    album(dal.NewComparison(typedTestField("AlbumId"), dal.In, dal.NewArray([]int{1, 2, 3}))),
			wantSQL:  head + `"AlbumId" IN ($1::bigint, $2::bigint, $3::bigint)`,
			wantArgs: []any{1, 2, 3},
		},
		{
			name:     "NOT IN keeps three-valued logic by binding NULL",
			query:    album(dal.NewComparison(typedTestField("AlbumId"), dal.NotIn, dal.NewArray([]any{1, nil, "x"}))),
			wantSQL:  head + `"AlbumId" NOT IN ($1::bigint, $2, $3)`,
			wantArgs: []any{1, nil, "x"},
		},
		{
			name:     "IN with arithmetic on the left",
			query:    album(dal.NewComparison(dal.Binary(typedTestField("A"), dal.Subtract, typedTestConst(1)), dal.In, dal.NewArray([]string{"p"}))),
			wantSQL:  head + `("A" - $1::bigint) IN ($2)`,
			wantArgs: []any{1, "p"},
		},
		{name: "empty IN is false", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.In, dal.NewArray([]int{}))), wantSQL: head + `FALSE`},
		{name: "empty NOT IN is true", query: album(dal.NewComparison(typedTestField("AlbumId"), dal.NotIn, dal.NewArray([]int{}))), wantSQL: head + `TRUE`},
		{
			name:     "question style keeps the neutral marker",
			dialect:  &fakeTypedDialect{style: typedPlaceholderStyle{IdentQuote: '"'}, caps: fullFakeTypedCapabilities(), identLimit: 63},
			query:    album(typedTestEq(typedTestField("A"), 1), typedTestEq(typedTestField("B"), 2)),
			wantSQL:  `SELECT "Title" FROM "Album" WHERE ("A" = ?::bigint AND "B" = ?::bigint)`,
			wantArgs: []any{1, 2},
		},
	})
}

func TestCompileTypedSQLAggregates(t *testing.T) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	count := typedTestColumn(dal.NewAggregate(dal.COUNT, false, dal.Star()), "")
	runTypedGoldens(t, []typedGolden{
		{
			name: "every native aggregate over one group",
			query: invoice().SelectColumns(
				count,
				dal.CountAs(typedTestField("A"), "c"),
				dal.CountDistinctAs(typedTestField("A"), "cd"),
				dal.SumAs(typedTestField("B"), "s"),
				dal.SumDistinctAs(typedTestField("B"), "sd"),
				dal.AverageAs(typedTestField("B"), "a"),
				dal.AverageDistinctAs(typedTestField("B"), "ad"),
				dal.MinAs(typedTestField("C"), "mn"),
				dal.MaxAs(typedTestField("C"), "mx"),
			),
			wantSQL: `SELECT COUNT(*) AS "COUNT(*)", COUNT("A") AS "c", COUNT(DISTINCT "A") AS "cd", ` +
				`CAST(SUM("B") AS double precision) AS "s", CAST(SUM(DISTINCT "B") AS double precision) AS "sd", ` +
				`CAST(AVG("B") AS double precision) AS "a", CAST(AVG(DISTINCT "B") AS double precision) AS "ad", ` +
				`MIN("C") AS "mn", MAX("C") AS "mx" FROM "Invoice"`,
		},
		{
			name: "group by having order by limit offset",
			query: invoice().
				GroupBy(typedTestField("BillingCountry")).
				Having(dal.NewComparison(dal.NewAggregate(dal.COUNT, false, dal.Star()), dal.GreaterThen, typedTestConst(5))).
				OrderBy(dal.Descending(typedTestField("total")), dal.Ascending(typedTestField("BillingCountry"))).
				Limit(10).Offset(20).
				SelectColumns(typedTestColumn(typedTestField("BillingCountry"), ""), dal.SumAs(typedTestField("Total"), "total")),
			wantSQL: `SELECT "BillingCountry", CAST(SUM("Total") AS double precision) AS "total" FROM "Invoice" ` +
				`GROUP BY "BillingCountry" HAVING COUNT(*) > $1::bigint ` +
				`ORDER BY CAST(SUM("Total") AS double precision) DESC NULLS LAST, "BillingCountry" ASC NULLS FIRST LIMIT $2 OFFSET $3`,
			wantArgs: []any{5, 10, 20},
		},
		{
			name:    "group by without columns selects the group keys",
			query:   invoice().GroupBy(typedTestField("BillingCountry"), typedTestField("BillingCity")).SelectColumns(),
			wantSQL: `SELECT "BillingCountry", "BillingCity" FROM "Invoice" GROUP BY "BillingCountry", "BillingCity"`,
		},
		{
			name: "having aliases are rewritten to expressions",
			query: invoice().
				GroupBy(typedTestField("BillingCountry")).
				Having(dal.NewComparison(typedTestField("total"), dal.GreaterOrEqual, typedTestConst(100.5)), dal.NewIsNotNullCondition(typedTestField("total"))).
				SelectColumns(typedTestColumn(typedTestField("BillingCountry"), ""), dal.SumAs(typedTestField("Total"), "total")),
			wantSQL: `SELECT "BillingCountry", CAST(SUM("Total") AS double precision) AS "total" FROM "Invoice" GROUP BY "BillingCountry" ` +
				`HAVING (CAST(SUM("Total") AS double precision) >= $1::numeric AND CAST(SUM("Total") AS double precision) IS NOT NULL)`,
			wantArgs: []any{100.5},
		},
		{
			name: "having over a nested group of aliases and arithmetic",
			query: invoice().
				GroupBy(typedTestField("BillingCountry")).
				Having(dal.NewGroupCondition(dal.Or,
					dal.NewComparison(dal.Binary(typedTestField("total"), dal.Add, typedTestConst(1)), dal.GreaterThen, typedTestConst(2)),
					dal.NewIsNullCondition(typedTestField("total")))).
				SelectColumns(typedTestColumn(typedTestField("BillingCountry"), ""), dal.SumAs(typedTestField("Total"), "total")),
			wantSQL: `SELECT "BillingCountry", CAST(SUM("Total") AS double precision) AS "total" FROM "Invoice" GROUP BY "BillingCountry" ` +
				`HAVING ((CAST(SUM("Total") AS double precision) + $1::bigint) > $2::bigint OR CAST(SUM("Total") AS double precision) IS NULL)`,
			wantArgs: []any{1, 2},
		},
		{
			name: "a grouped expression carrying a constant is referenced by ordinal",
			query: invoice().
				GroupBy(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1))).
				OrderBy(dal.Ascending(typedTestField("bucket"))).
				SelectColumns(typedTestColumn(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1)), "bucket"), typedTestColumn(dal.NewAggregate(dal.COUNT, false, dal.Star()), "n")),
			wantSQL:  `SELECT ("Total" + $1::bigint) AS "bucket", COUNT(*) AS "n" FROM "Invoice" GROUP BY 1 ORDER BY 1 ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			name: "an unselected grouped expression carrying a constant stays inline",
			query: invoice().
				GroupBy(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1))).
				SelectColumns(typedTestColumn(dal.NewAggregate(dal.COUNT, false, dal.Star()), "n")),
			wantSQL:  `SELECT COUNT(*) AS "n" FROM "Invoice" GROUP BY ("Total" + $1::bigint)`,
			wantArgs: []any{1},
		},
		{
			name: "a grouped field without constants stays inline in order by",
			query: invoice().
				GroupBy(typedTestField("Country")).
				OrderBy(dal.Descending(typedTestField("Country"))).
				SelectColumns(typedTestColumn(typedTestField("Country"), "c"), dal.CountAs(typedTestField("Id"), "n")),
			wantSQL: `SELECT "Country" AS "c", COUNT("Id") AS "n" FROM "Invoice" GROUP BY "Country" ORDER BY "Country" DESC NULLS LAST`,
		},
		{
			name: "having over a grouped alias without constants is rewritten",
			query: invoice().
				GroupBy(typedTestField("Country")).
				Having(dal.NewComparison(typedTestField("c"), dal.Equal, typedTestConst("FR"))).
				SelectColumns(typedTestColumn(typedTestField("Country"), "c"), dal.CountAs(typedTestField("Id"), "n")),
			wantSQL:  `SELECT "Country" AS "c", COUNT("Id") AS "n" FROM "Invoice" GROUP BY "Country" HAVING "Country" = $1`,
			wantArgs: []any{"FR"},
		},
		{
			name: "a constant inside an aggregate argument is bound",
			query: invoice().SelectColumns(
				dal.SumAs(dal.Binary(typedTestField("B"), dal.Multiply, typedTestConst(2)), "s"),
				dal.CountAs(typedTestField("B"), "c"),
			),
			wantSQL:  `SELECT CAST(SUM(("B" * $1::bigint)) AS double precision) AS "s", COUNT("B") AS "c" FROM "Invoice"`,
			wantArgs: []any{2},
		},
		{
			name: "an aggregate argument carrying a constant is rewritten in HAVING and ORDER BY",
			query: invoice().
				GroupBy(typedTestField("Country")).
				Having(dal.NewComparison(typedTestField("s"), dal.GreaterThen, typedTestConst(5))).
				OrderBy(dal.Descending(typedTestField("s"))).
				SelectColumns(typedTestColumn(typedTestField("Country"), ""), dal.SumAs(dal.Binary(typedTestField("B"), dal.Multiply, typedTestConst(2)), "s")),
			wantSQL: `SELECT "Country", CAST(SUM(("B" * $1::bigint)) AS double precision) AS "s" FROM "Invoice" GROUP BY "Country" ` +
				`HAVING CAST(SUM(("B" * $2::bigint)) AS double precision) > $3::bigint ORDER BY 2 DESC NULLS LAST`,
			wantArgs: []any{2, 2, 5},
		},
		{
			name: "an aggregate alias carrying a constant, nested in ORDER BY, stays inline",
			query: invoice().
				GroupBy(typedTestField("Country")).
				OrderBy(dal.Ascending(dal.Binary(typedTestField("n"), dal.Multiply, typedTestConst(2)))).
				SelectColumns(typedTestColumn(typedTestField("Country"), ""), typedTestColumn(dal.Binary(dal.Count().Expression, dal.Add, typedTestConst(1)), "n")),
			wantSQL: `SELECT "Country", (COUNT(*) + $1::bigint) AS "n" FROM "Invoice" GROUP BY "Country" ` +
				`ORDER BY ((COUNT(*) + $2::bigint) * $3::bigint) ASC NULLS FIRST`,
			wantArgs: []any{1, 1, 2},
		},
		{
			name: "a grouped alias by ordinal does not stop a later item carrying its own constant",
			query: invoice().
				GroupBy(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1))).
				OrderBy(dal.Ascending(typedTestField("bucket")), dal.Descending(dal.Binary(typedTestField("n"), dal.Multiply, typedTestConst(2)))).
				SelectColumns(typedTestColumn(dal.Binary(typedTestField("Total"), dal.Add, typedTestConst(1)), "bucket"), typedTestColumn(dal.Count().Expression, "n")),
			wantSQL: `SELECT ("Total" + $1::bigint) AS "bucket", COUNT(*) AS "n" FROM "Invoice" GROUP BY 1 ` +
				`ORDER BY 1 ASC NULLS FIRST, (COUNT(*) * $2::bigint) DESC NULLS LAST`,
			wantArgs: []any{1, 2},
		},
		{
			name: "having over an aggregate alias that carries a constant is fine",
			query: invoice().
				Having(dal.NewComparison(typedTestField("total"), dal.GreaterThen, typedTestConst(1))).
				SelectColumns(typedTestColumn(dal.Binary(dal.NewAggregate(dal.SUM, false, typedTestField("Total")), dal.Add, typedTestConst(1)), "total")),
			wantSQL: `SELECT (CAST(SUM("Total") AS double precision) + $1::bigint) AS "total" FROM "Invoice" ` +
				`HAVING (CAST(SUM("Total") AS double precision) + $2::bigint) > $3::bigint`,
			wantArgs: []any{1, 1, 1},
		},
	})
}

func TestCompileTypedSQLOrderBy(t *testing.T) {
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		// ArtistId is on both sides because the join keys below read it: a field of a
		// known source must be one of its columns.
		{Name: "Album"}:  {Columns: []typedColumnFact{{Name: "AlbumId", NotNull: true}, {Name: "Title"}, {Name: "ArtistId", DataType: "int4", Category: typedTypeNumber}}},
		{Name: "Artist"}: {Columns: []typedColumnFact{{Name: "ArtistId", NotNull: true, DataType: "int4", Category: typedTypeNumber}, {Name: "Name"}}},
	}}
	album := func() dal.IQueryBuilder { return typedTestFrom("Album", "").NewQuery() }
	runTypedGoldens(t, []typedGolden{
		{
			name:    "NULLS clause matches DALgo when nothing is known",
			query:   album().OrderBy(dal.Ascending(typedTestField("AlbumId")), dal.Descending(typedTestField("Title"))).SelectColumns(),
			wantSQL: `SELECT * FROM "Album" ORDER BY "AlbumId" ASC NULLS FIRST, "Title" DESC NULLS LAST`,
		},
		{
			name:    "NOT NULL columns omit the NULLS clause so an index can serve the order",
			facts:   facts,
			query:   album().OrderBy(dal.Ascending(typedTestField("AlbumId")), dal.Descending(typedTestField("AlbumId")), dal.Descending(typedTestField("Title"))).SelectColumns(),
			wantSQL: `SELECT * FROM "Album" ORDER BY "AlbumId" ASC, "AlbumId" DESC, "Title" DESC NULLS LAST`,
		},
		{
			name:  "a qualified NOT NULL column on the preserved side of a left join",
			facts: facts,
			query: typedTestFrom("Album", "a").Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinLeft, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
			).NewQuery().OrderBy(dal.Ascending(typedTestQualified("a", "AlbumId")), dal.Ascending(typedTestQualified("r", "ArtistId"))).SelectColumns(),
			wantSQL: `SELECT * FROM "Album" AS "a" LEFT JOIN "Artist" AS "r" ON ("a"."ArtistId" = "r"."ArtistId") ORDER BY "a"."AlbumId" ASC, "r"."ArtistId" ASC NULLS FIRST`,
		},
		{
			name:  "nullability propagates through a nested left join",
			facts: facts,
			query: func() dal.StructuredQuery {
				inner := dal.From(dal.NewRootCollectionRef("Artist", "r"))
				return typedTestFrom("Album", "a").Join(
					dal.NewJoinedFrom(inner, dal.JoinLeft, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
				).NewQuery().OrderBy(dal.Ascending(typedTestQualified("r", "ArtistId"))).SelectColumns()
			}(),
			wantSQL: `SELECT * FROM "Album" AS "a" LEFT JOIN "Artist" AS "r" ON ("a"."ArtistId" = "r"."ArtistId") ORDER BY "r"."ArtistId" ASC NULLS FIRST`,
		},
		{
			name:  "an inner join below a left join is nullable too",
			facts: facts,
			query: func() dal.StructuredQuery {
				artists := dal.From(dal.NewRootCollectionRef("Artist", "r")).Join(
					dal.NewJoinedSource(dal.NewRootCollectionRef("Album", "b"), dal.JoinInner, typedTestJoinOn("r", "ArtistId", "b", "ArtistId")),
				)
				return typedTestFrom("Album", "a").Join(
					dal.NewJoinedFrom(artists, dal.JoinLeft, typedTestJoinOn("a", "ArtistId", "r", "ArtistId")),
				).NewQuery().OrderBy(dal.Ascending(typedTestQualified("b", "AlbumId")), dal.Ascending(typedTestQualified("a", "AlbumId"))).SelectColumns()
			}(),
			wantSQL: `SELECT * FROM "Album" AS "a" LEFT JOIN ("Artist" AS "r" INNER JOIN "Album" AS "b" ON ("r"."ArtistId" = "b"."ArtistId")) ON ("a"."ArtistId" = "r"."ArtistId") ` +
				`ORDER BY "b"."AlbumId" ASC NULLS FIRST, "a"."AlbumId" ASC`,
		},
		{
			name:  "an aggregate is never assumed NOT NULL",
			facts: facts,
			query: album().OrderBy(dal.Ascending(dal.NewAggregate(dal.MIN, false, typedTestField("AlbumId")))).
				SelectColumns(typedTestColumn(dal.NewAggregate(dal.MIN, false, typedTestField("AlbumId")), "m")),
			wantSQL: `SELECT MIN("AlbumId") AS "m" FROM "Album" ORDER BY MIN("AlbumId") ASC NULLS FIRST`,
		},
		{
			name:    "an unknown source falls back to the NULLS clause",
			facts:   facts,
			query:   typedTestFrom("Genre", "").NewQuery().OrderBy(dal.Ascending(typedTestField("GenreId"))).SelectColumns(),
			wantSQL: `SELECT * FROM "Genre" ORDER BY "GenreId" ASC NULLS FIRST`,
		},
		{
			name:     "an alias selecting a constant-carrying expression is ordered by ordinal",
			query:    album().OrderBy(dal.Descending(typedTestField("shifted"))).SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(1)), "shifted")),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::bigint) AS "shifted" FROM "Album" ORDER BY 2 DESC NULLS LAST`,
			wantArgs: []any{1},
		},
		{
			name: "a selected expression written out again is ordered by ordinal",
			query: album().OrderBy(dal.Ascending(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(1)))).
				SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(1)), "shifted")),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::bigint) AS "shifted" FROM "Album" ORDER BY 2 ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			name: "an alias selecting a constant-carrying expression, nested in ORDER BY of an ungrouped query, is bound inline",
			query: album().OrderBy(dal.Descending(dal.Binary(typedTestField("shifted"), dal.Multiply, typedTestConst(2)))).
				SelectColumns(typedTestColumn(typedTestField("Title"), ""), typedTestColumn(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(1)), "shifted")),
			wantSQL:  `SELECT "Title", ("AlbumId" + $1::bigint) AS "shifted" FROM "Album" ORDER BY (("AlbumId" + $2::bigint) * $3::bigint) DESC NULLS LAST`,
			wantArgs: []any{1, 1, 2},
		},
		{
			name:     "an unselected expression carrying a constant is bound inline",
			query:    album().OrderBy(dal.Ascending(dal.Binary(typedTestField("AlbumId"), dal.Add, typedTestConst(1)))).SelectColumns(typedTestColumn(typedTestField("Title"), "")),
			wantSQL:  `SELECT "Title" FROM "Album" ORDER BY ("AlbumId" + $1::bigint) ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			name:    "an expanded wildcard shifts the ordinal",
			facts:   typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{{Name: "Album"}: {Columns: []typedColumnFact{{Name: "A"}, {Name: "B"}, {Name: "X"}}}}},
			query:   album().OrderBy(dal.Ascending(typedTestField("n"))).SelectColumns(dal.AllColumnsExcept("X"), typedTestColumn(dal.Binary(typedTestField("A"), dal.Add, typedTestConst(1)), "n")),
			wantSQL: `SELECT "A", "B", ("A" + $1::bigint) AS "n" FROM "Album" ORDER BY 3 ASC NULLS FIRST`, wantArgs: []any{1},
		},
	})
}

// TestCompileTypedSQLBareNamesAreReadAsInputColumns covers every place the
// compiler writes a bare, unqualified name into ORDER BY, GROUP BY or HAVING,
// where the server could read it as an output column (a select alias) instead of
// the input column DALgo means. PostgreSQL's rules, from its SELECT reference:
//
//   - ORDER BY: a name that stands alone is matched against output columns first,
//     and only then against input columns; inside an expression it is an input
//     column. So a bare ORDER BY name is the one place a capture can happen, and
//     orderBy refuses it (typed_sql_errors_test.go has the refusals). What is
//     safe is pinned here: a bare name no output column shares, one every
//     same-named output column agrees with, a qualified name, and a name nested
//     in an expression.
//   - GROUP BY: "in case of ambiguity, a GROUP BY name will be interpreted as an
//     input-column name rather than an output column name". DALgo groups by the
//     input expression, so the bare name is already the right reading; and when no
//     input column has the name, the only output it can reach is one the validation
//     required to be grouped as well, so the partition is the same. The compiler
//     never rewrites a GROUP BY name to an alias.
//   - HAVING: output names are not visible to HAVING at all, so a bare name is an
//     input column.
func TestCompileTypedSQLBareNamesAreReadAsInputColumns(t *testing.T) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	runTypedGoldens(t, []typedGolden{
		{
			name: "ORDER BY alias of a column no other output shares its name with",
			query: invoice().OrderBy(dal.Descending(typedTestField("a"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("Name"), "n")),
			wantSQL: `SELECT "Total" AS "a", "Name" AS "n" FROM "Invoice" ORDER BY "Total" DESC NULLS LAST`,
		},
		{
			name: "ORDER BY alias of a column that is also selected under its own name",
			query: invoice().OrderBy(dal.Ascending(typedTestField("a"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("Total"), "")),
			wantSQL: `SELECT "Total" AS "a", "Total" FROM "Invoice" ORDER BY "Total" ASC NULLS FIRST`,
		},
		{
			name: "ORDER BY a selected column under its own name",
			query: invoice().OrderBy(dal.Ascending(typedTestField("Total"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), ""), typedTestColumn(typedTestField("Name"), "a")),
			wantSQL: `SELECT "Total", "Name" AS "a" FROM "Invoice" ORDER BY "Total" ASC NULLS FIRST`,
		},
		{
			// Qualified, the name is never an output column, whatever the select list says.
			name: "ORDER BY a qualified name that another select alias reuses",
			query: typedTestFrom("Invoice", "i").NewQuery().OrderBy(dal.Ascending(typedTestQualified("i", "Total"))).
				SelectColumns(typedTestColumn(typedTestField("Name"), "Total")),
			wantSQL: `SELECT "Name" AS "Total" FROM "Invoice" AS "i" ORDER BY "i"."Total" ASC NULLS FIRST`,
		},
		{
			// Inside an expression a name is an input column, so the capture the bare
			// form would suffer cannot happen and the query stays native.
			name: "ORDER BY alias nested in an expression, when another select alias reuses the column's name",
			query: invoice().OrderBy(dal.Ascending(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("Name"), "Total")),
			wantSQL:  `SELECT "Total" AS "a", "Name" AS "Total" FROM "Invoice" ORDER BY ("Total" + $1::bigint) ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			// In a join every name is qualified, so an alias resolves to a qualified column.
			name: "ORDER BY alias in a join, when another select alias reuses the column's name",
			query: typedTestFrom("Invoice", "i").Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Customer", "c"), dal.JoinInner, typedTestJoinOn("i", "CustomerId", "c", "CustomerId")),
			).NewQuery().OrderBy(dal.Ascending(typedTestField("a"))).
				SelectColumns(typedTestColumn(typedTestQualified("i", "Total"), "a"), typedTestColumn(typedTestQualified("c", "Name"), "Total")),
			wantSQL: `SELECT "i"."Total" AS "a", "c"."Name" AS "Total" FROM "Invoice" AS "i" INNER JOIN "Customer" AS "c" ON ("i"."CustomerId" = "c"."CustomerId") ` +
				`ORDER BY "i"."Total" ASC NULLS FIRST`,
		},
		{
			// The group key a is an input column, which GROUP BY prefers to the output
			// COUNT(b) AS a; the statement groups by the column, as DALgo does.
			name: "GROUP BY a column that another select alias reuses",
			query: invoice().GroupBy(typedTestField("a")).
				SelectColumns(typedTestColumn(typedTestField("a"), "x"), dal.CountAs(typedTestField("b"), "a")),
			wantSQL: `SELECT "a" AS "x", COUNT("b") AS "a" FROM "Invoice" GROUP BY "a"`,
		},
		{
			// HAVING cannot see the output a, so the rewritten bare "a" is the group key.
			name: "HAVING alias of a group key, when another select alias reuses the key's name",
			query: invoice().GroupBy(typedTestField("a")).
				Having(dal.NewComparison(typedTestField("x"), dal.GreaterThen, typedTestConst(1))).
				SelectColumns(typedTestColumn(typedTestField("a"), "x"), dal.CountAs(typedTestField("b"), "a")),
			wantSQL:  `SELECT "a" AS "x", COUNT("b") AS "a" FROM "Invoice" GROUP BY "a" HAVING "a" > $1::bigint`,
			wantArgs: []any{1},
		},
		{
			// A dialect that quotes names as written keeps Total and total apart, so
			// ORDER BY Total reads the input column and the output total is not a
			// capture. The same statement is refused under a folding dialect
			// (typed_sql_errors_test.go), which is why the guard compares what is written.
			name: "ORDER BY a column that differs from a select alias only in case, with exact quoting",
			query: invoice().OrderBy(dal.Ascending(typedTestField("Total"))).
				SelectColumns(typedTestColumn(typedTestField("Name"), "total")),
			wantSQL: `SELECT "Name" AS "total" FROM "Invoice" ORDER BY "Total" ASC NULLS FIRST`,
		},
		{
			name: "ORDER BY an alias whose column name is spelled in another case than the alias that follows it, with exact quoting",
			query: invoice().OrderBy(dal.Descending(typedTestField("a"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("Name"), "total")),
			wantSQL: `SELECT "Total" AS "a", "Name" AS "total" FROM "Invoice" ORDER BY "Total" DESC NULLS LAST`,
		},
	})
}

// TestCompileTypedSQLBareNamesUnderAFoldingDialect repeats
// TestCompileTypedSQLBareNamesAreReadAsInputColumns for a dialect whose quoteIdent
// folds case (newFoldingFakeTypedDialect), where two spellings that differ only in
// case are one identifier to the server. The refusals are in
// typed_sql_errors_test.go; these are the statements that must still compile.
//
// ORDER BY compares the quoted names the compiler wrote: a bare name is a capture
// only when an output carries the same written name and is not the column itself.
// GROUP BY and HAVING need no comparison under any case rule, because GROUP BY
// reads an input column before an output and HAVING cannot see outputs; the two
// goldens at the end show the written text is the input column's name.
func TestCompileTypedSQLBareNamesUnderAFoldingDialect(t *testing.T) {
	for _, folding := range foldingTypedDialects(t) {
		t.Run(folding.name, func(t *testing.T) { checkBareNamesUnderAFoldingDialect(t, folding) })
	}
}

func checkBareNamesUnderAFoldingDialect(t *testing.T, folding foldingTypedDialect) {
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	runTypedGoldens(t, []typedGolden{
		{
			// The second output is the column Total written as TOTAL: same identifier,
			// so the server's output total is the column itself, not another expression.
			name:    "ORDER BY alias of a column that is also selected under another spelling of its own name",
			dialect: folding.dialect,
			query: invoice().OrderBy(dal.Ascending(typedTestField("a"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("TOTAL"), "")),
			wantSQL: `SELECT "total" AS "a", "total" FROM "invoice" ORDER BY "total" ASC NULLS FIRST`,
		},
		{
			name:    "ORDER BY a column selected under an alias that folds to its own name",
			dialect: folding.dialect,
			query: invoice().OrderBy(dal.Descending(typedTestField("Total"))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "TOTAL")),
			wantSQL: `SELECT "total" AS "total" FROM "invoice" ORDER BY "total" DESC NULLS LAST`,
		},
		{
			name:    "ORDER BY a column that no output shares a written name with",
			dialect: folding.dialect,
			query: invoice().OrderBy(dal.Ascending(typedTestField("Total"))).
				SelectColumns(typedTestColumn(typedTestField("Name"), "n"), typedTestColumn(typedTestField("Total"), "a")),
			wantSQL: `SELECT "name" AS "n", "total" AS "a" FROM "invoice" ORDER BY "total" ASC NULLS FIRST`,
		},
		{
			name:    "ORDER BY a column and a select-all whose column folds to the same name",
			dialect: folding.dialect,
			facts:   typedCatalogFacts{Fold: folding.fold, Sources: map[typedSourceName]typedSourceFacts{{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "name"}, {Name: "secret"}}}}},
			query: invoice().OrderBy(dal.Ascending(typedTestField("Total"))).
				SelectColumns(dal.AllColumnsExcept("secret")),
			wantSQL: `SELECT "total", "name" FROM "invoice" ORDER BY "total" ASC NULLS FIRST`,
		},
		{
			name:    "ORDER BY a qualified name that another select alias takes under a folded spelling",
			dialect: folding.dialect,
			query: typedTestFrom("Invoice", "i").NewQuery().OrderBy(dal.Ascending(typedTestQualified("i", "Total"))).
				SelectColumns(typedTestColumn(typedTestField("Name"), "total")),
			wantSQL: `SELECT "name" AS "total" FROM "invoice" AS "i" ORDER BY "i"."total" ASC NULLS FIRST`,
		},
		{
			name:    "ORDER BY alias nested in an expression, when another select alias takes the column's folded name",
			dialect: folding.dialect,
			query: invoice().OrderBy(dal.Ascending(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)))).
				SelectColumns(typedTestColumn(typedTestField("Total"), "a"), typedTestColumn(typedTestField("Name"), "total")),
			wantSQL:  `SELECT "total" AS "a", "name" AS "total" FROM "invoice" ORDER BY (` + folding.operand(`"total"`) + ` + ` + folding.operand(`$1::bigint`) + `) ASC NULLS FIRST`,
			wantArgs: []any{folding.arg(1)},
		},
		{
			name:    "GROUP BY a column that another select alias takes under a folded spelling",
			dialect: folding.dialect,
			query: invoice().GroupBy(typedTestField("A")).
				SelectColumns(typedTestColumn(typedTestField("A"), "x"), dal.CountAs(typedTestField("b"), "a")),
			wantSQL: `SELECT "a" AS "x", COUNT("b") AS "a" FROM "invoice" GROUP BY "a"`,
		},
		{
			name:    "HAVING alias of a group key, when another select alias takes the key's folded name",
			dialect: folding.dialect,
			query: invoice().GroupBy(typedTestField("A")).
				Having(dal.NewComparison(typedTestField("x"), dal.GreaterThen, typedTestConst(1))).
				SelectColumns(typedTestColumn(typedTestField("A"), "x"), dal.CountAs(typedTestField("b"), "a")),
			wantSQL:  `SELECT "a" AS "x", COUNT("b") AS "a" FROM "invoice" GROUP BY "a" HAVING "a" > $1::bigint`,
			wantArgs: []any{folding.arg(1)},
		},
	})
}

func TestCompileTypedSQLArithmetic(t *testing.T) {
	album := func(expression dal.Expression) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(expression, "r"))
	}
	runTypedGoldens(t, []typedGolden{
		{name: "add", query: album(dal.Binary(typedTestField("a"), dal.Add, typedTestField("b"))), wantSQL: `SELECT ("a" + "b") AS "r" FROM "Album"`},
		{name: "subtract", query: album(dal.Binary(typedTestField("a"), dal.Subtract, typedTestConst(2))), wantSQL: `SELECT ("a" - $1::bigint) AS "r" FROM "Album"`, wantArgs: []any{2}},
		{name: "multiply", query: album(dal.Binary(typedTestConst(2.5), dal.Multiply, typedTestField("b"))), wantSQL: `SELECT ($1::numeric * "b") AS "r" FROM "Album"`, wantArgs: []any{2.5}},
		{
			name:    "divide is the dialect's business",
			query:   album(dal.Binary(typedTestField("a"), dal.Divide, typedTestField("b"))),
			wantSQL: `SELECT (CAST("a" AS double precision) / NULLIF(CAST("b" AS double precision), 0)) AS "r" FROM "Album"`,
		},
		{
			name:     "nested arithmetic keeps argument order",
			query:    album(dal.Binary(dal.Binary(typedTestField("a"), dal.Add, typedTestConst(1)), dal.Divide, dal.Binary(typedTestField("b"), dal.Subtract, typedTestConst(2)))),
			wantSQL:  `SELECT (CAST(("a" + $1::bigint) AS double precision) / NULLIF(CAST(("b" - $2::bigint) AS double precision), 0)) AS "r" FROM "Album"`,
			wantArgs: []any{1, 2},
		},
	})
}

func TestCompileTypedSQLLimitAndOffset(t *testing.T) {
	album := func(limit, offset int) dal.StructuredQuery {
		return typedTestFrom("Album", "").NewQuery().Limit(limit).Offset(offset).SelectColumns()
	}
	runTypedGoldens(t, []typedGolden{
		{name: "none", query: album(0, 0), wantSQL: `SELECT * FROM "Album"`},
		{name: "limit", query: album(10, 0), wantSQL: `SELECT * FROM "Album" LIMIT $1`, wantArgs: []any{10}},
		{name: "offset only", query: album(0, 5), wantSQL: `SELECT * FROM "Album" OFFSET $1`, wantArgs: []any{5}},
		{name: "both", query: album(10, 5), wantSQL: `SELECT * FROM "Album" LIMIT $1 OFFSET $2`, wantArgs: []any{10, 5}},
	})
}

func TestCompileTypedSQLNumberingFollowsTextOrderAcrossClauses(t *testing.T) {
	q := typedTestFrom("Invoice", "").NewQuery().
		Where(typedTestEq(typedTestField("a"), 1)).
		GroupBy(typedTestField("g")).
		Having(dal.NewComparison(dal.NewAggregate(dal.SUM, false, typedTestField("t")), dal.GreaterThen, typedTestConst(2))).
		OrderBy(dal.Ascending(dal.Binary(typedTestField("g"), dal.Add, typedTestConst(3)))).
		Limit(4).Offset(5).
		SelectColumns(typedTestColumn(typedTestField("g"), ""), typedTestColumn(dal.Binary(dal.Count().Expression, dal.Add, typedTestConst(6)), "n"))
	typedGolden{
		query: q,
		wantSQL: `SELECT "g", (COUNT(*) + $1::bigint) AS "n" FROM "Invoice" WHERE "a" = $2::bigint GROUP BY "g" ` +
			`HAVING CAST(SUM("t") AS double precision) > $3::bigint ORDER BY ("g" + $4::bigint) ASC NULLS FIRST LIMIT $5 OFFSET $6`,
		wantArgs: []any{6, 1, 2, 3, 4, 5},
	}.run(t)
}

func TestCompileTypedSQLIdentifiersWithPlaceholderAndQuoteCharacters(t *testing.T) {
	weird := `we?ird"col`
	q := typedTestFrom(`tab?le"x`, "").NewQuery().
		Where(dal.NewGroupCondition(dal.And, typedTestEq(typedTestField(weird), 1), typedTestEq(typedTestField(`?`), 2), typedTestEq(typedTestField(`""`), 3))).
		OrderBy(dal.Ascending(typedTestField(weird))).
		SelectColumns(typedTestColumn(typedTestField(weird), `a?"b`))
	typedGolden{
		query: q,
		wantSQL: `SELECT "we?ird""col" AS "a?""b" FROM "tab?le""x" WHERE ("we?ird""col" = $1::bigint AND "?" = $2::bigint AND """""" = $3::bigint) ` +
			`ORDER BY "we?ird""col" ASC NULLS FIRST`,
		wantArgs: []any{1, 2, 3},
	}.run(t)
}

func TestCompileTypedSQLHostileValueNeverReachesTheText(t *testing.T) {
	hostile := `x'); DROP TABLE "Album"; -- $1 ?`
	q := typedTestFrom("Album", "").NewQuery().
		Where(typedTestEq(typedTestField("Title"), hostile)).
		SelectColumns(typedTestColumn(typedTestField("Title"), ""))
	text, args, err := compileTypedSQL(q, newFakeTypedDialect(), typedCatalogFacts{})
	if err != nil {
		t.Fatal(err)
	}
	if want := `SELECT "Title" FROM "Album" WHERE "Title" = $1`; text != want {
		t.Fatalf("SQL = %q, want %q", text, want)
	}
	if len(args) != 1 || args[0] != hostile {
		t.Fatalf("args = %#v, want the hostile value as the only argument", args)
	}
}

func TestCompileTypedSQLLaunchJourneyStatement(t *testing.T) {
	// The launch journey in one statement: a join, WHERE, GROUP BY, HAVING and
	// ORDER BY through select aliases, LIMIT and OFFSET, every field qualified,
	// and values numbered across clauses in text order.
	invoices := dal.NewRootCollectionRef("Invoice", "i")
	customers := dal.NewRootCollectionRef("Customer", "c")
	q := dal.From(invoices).Join(
		dal.NewJoinedSource(customers, dal.JoinInner, typedTestJoinOn("i", "CustomerId", "c", "CustomerId")),
	).NewQuery().
		Where(dal.NewGroupCondition(dal.And,
			typedTestEq(typedTestQualified("i", "BillingCountry"), "FR"),
			dal.NewComparison(typedTestQualified("i", "Total"), dal.GreaterThen, typedTestConst(9.5)),
		)).
		GroupBy(typedTestQualified("c", "Country")).
		Having(dal.NewComparison(typedTestField("revenue"), dal.GreaterThen, typedTestConst(1000))).
		OrderBy(dal.Descending(typedTestField("revenue")), dal.Ascending(typedTestField("country"))).
		Limit(10).Offset(5).
		SelectColumns(
			typedTestColumn(typedTestQualified("c", "Country"), "country"),
			dal.SumAs(typedTestQualified("i", "Total"), "revenue"),
			dal.CountAs(typedTestQualified("i", "InvoiceId"), "invoices"),
		)
	typedGolden{
		query: q,
		wantSQL: `SELECT "c"."Country" AS "country", CAST(SUM("i"."Total") AS double precision) AS "revenue", COUNT("i"."InvoiceId") AS "invoices" ` +
			`FROM "Invoice" AS "i" INNER JOIN "Customer" AS "c" ON ("i"."CustomerId" = "c"."CustomerId") ` +
			`WHERE ("i"."BillingCountry" = $1 AND "i"."Total" > $2::numeric) GROUP BY "c"."Country" ` +
			`HAVING CAST(SUM("i"."Total") AS double precision) > $3::bigint ` +
			`ORDER BY CAST(SUM("i"."Total") AS double precision) DESC NULLS LAST, "c"."Country" ASC NULLS FIRST LIMIT $4 OFFSET $5`,
		wantArgs: []any{"FR", 9.5, 1000, 10, 5},
	}.run(t)
}

func TestCompileTypedSQLQualifiersWithQuoteCharacters(t *testing.T) {
	// A source alias or schema that holds the quote character or the placeholder
	// character is quoted wherever it qualifies a field, and numbering skips it.
	t.Run("single source", func(t *testing.T) {
		typedGolden{
			query: dal.From(dal.NewQualifiedRootCollectionRef(`sch"ema`, `ta?ble`, `a"l?`)).NewQuery().
				Where(typedTestEq(typedTestQualified(`a"l?`, `x"?`), 1)).
				SelectColumns(typedTestColumn(typedTestQualified(`a"l?`, `x"?`), "")),
			wantSQL:  `SELECT "a""l?"."x""?" FROM "sch""ema"."ta?ble" AS "a""l?" WHERE "a""l?"."x""?" = $1::bigint`,
			wantArgs: []any{1},
		}.run(t)
	})
	t.Run("join", func(t *testing.T) {
		typedGolden{
			query: dal.From(dal.NewQualifiedRootCollectionRef(`s"1`, "Album", `a"?`)).Join(
				dal.NewJoinedSource(dal.NewQualifiedRootCollectionRef(`s?2`, "Artist", `r"?"`), dal.JoinLeft, typedTestJoinOn(`a"?`, "ArtistId", `r"?"`, "ArtistId")),
			).NewQuery().
				Where(typedTestEq(typedTestQualified(`r"?"`, "Name"), "x")).
				SelectColumns(typedTestColumn(typedTestQualified(`a"?`, "Title"), "")),
			wantSQL: `SELECT "a""?"."Title" FROM "s""1"."Album" AS "a""?" LEFT JOIN "s?2"."Artist" AS "r""?""" ON ("a""?"."ArtistId" = "r""?"""."ArtistId") ` +
				`WHERE "r""?"""."Name" = $1`,
			wantArgs: []any{"x"},
		}.run(t)
	})
}

func TestCompileTypedSQLScanBoundedSingleSource(t *testing.T) {
	// A scan on a lone source means: take the bounded, ordered rows first, then
	// apply the rest of the statement. One SELECT can say that only when the
	// statement is the bounded read itself, which is the leaf query DALgo's
	// generic join and federated executor send: the same ORDER BY as the scan and
	// a LIMIT within it, nothing else. Every other statement over a scan-bounded
	// source is refused (see the refusal table in typed_sql_errors_test.go).
	scanned := dal.NewRootCollectionRef("Invoice", "i").WithScan(10, dal.Descending(typedTestField("Total")))
	orderOnly := dal.NewRootCollectionRef("Customer", "c").WithScan(0, dal.Ascending(typedTestField("Name")))
	limitOnly := dal.NewRootCollectionRef("Invoice", "i").WithScan(10)
	leaf := func(source dal.RecordsetSource) dal.IQueryBuilder {
		c := source.(dal.CollectionRef)
		return dal.From(source).NewQuery().OrderBy(c.ScanOrders()...).Limit(c.ScanLimit())
	}
	runTypedGoldens(t, []typedGolden{
		{
			name:     "the leaf query the generic join sends",
			query:    leaf(scanned).SelectColumns(),
			wantSQL:  `SELECT * FROM "Invoice" AS "i" ORDER BY "Total" DESC NULLS LAST LIMIT $1`,
			wantArgs: []any{10},
		},
		{
			name:     "the leaf query over a pointer source",
			query:    dal.From(typedTestPtr(scanned)).NewQuery().OrderBy(scanned.ScanOrders()...).Limit(10).SelectColumns(),
			wantSQL:  `SELECT * FROM "Invoice" AS "i" ORDER BY "Total" DESC NULLS LAST LIMIT $1`,
			wantArgs: []any{10},
		},
		{
			name:     "a statement limit below the scan limit",
			query:    leaf(scanned).Limit(4).SelectColumns(),
			wantSQL:  `SELECT * FROM "Invoice" AS "i" ORDER BY "Total" DESC NULLS LAST LIMIT $1`,
			wantArgs: []any{4},
		},
		{
			name:     "the projection stays free",
			query:    leaf(scanned).SelectColumns(typedTestColumn(typedTestField("Total"), "")),
			wantSQL:  `SELECT "Total" FROM "Invoice" AS "i" ORDER BY "Total" DESC NULLS LAST LIMIT $1`,
			wantArgs: []any{10},
		},
		{
			name:     "an alias that names the scan field after itself restates the same order",
			query:    leaf(scanned).SelectColumns(typedTestColumn(typedTestField("Total"), "Total")),
			wantSQL:  `SELECT "Total" AS "Total" FROM "Invoice" AS "i" ORDER BY "Total" DESC NULLS LAST LIMIT $1`,
			wantArgs: []any{10},
		},
		{
			name:    "a scan order without a scan limit",
			query:   leaf(orderOnly).SelectColumns(),
			wantSQL: `SELECT * FROM "Customer" AS "c" ORDER BY "Name" ASC NULLS FIRST`,
		},
		{
			name:     "a scan order without a scan limit, under a statement limit",
			query:    leaf(orderOnly).Limit(3).SelectColumns(),
			wantSQL:  `SELECT * FROM "Customer" AS "c" ORDER BY "Name" ASC NULLS FIRST LIMIT $1`,
			wantArgs: []any{3},
		},
		{
			name:     "a scan limit without a scan order",
			query:    leaf(limitOnly).SelectColumns(),
			wantSQL:  `SELECT * FROM "Invoice" AS "i" LIMIT $1`,
			wantArgs: []any{10},
		},
		{
			// The federated executor routes a source in a named database to that
			// database before it sends the leaf, so the compiler, which is handed
			// the connection of that database, renders the table by its own name.
			name:    "a lone source in a named database",
			query:   dal.From(dal.NewDatabaseCollectionRef("db1", "", "Invoice", "i")).NewQuery().SelectColumns(),
			wantSQL: `SELECT * FROM "Invoice" AS "i"`,
		},
	})
}

func TestCompileTypedSQLQueryTypeThatCanCarryMoneyButCarriesNone(t *testing.T) {
	// The refusal table has the query that does carry the option.
	typedGolden{
		query:   typedMoneyQuery{StructuredQuery: typedTestFrom("Album", "").NewQuery().SelectColumns()},
		wantSQL: `SELECT * FROM "Album"`,
	}.run(t)
}

func TestCompileTypedSQLIdentityFields(t *testing.T) {
	// dal.ID names a real column and is rendered as one; dal.DocumentID has no
	// column of its own and is refused (see the unsupported-node table).
	typedGolden{
		query:    typedTestFrom("Album", "").NewQuery().Where(dal.ID("AlbumId", 7)).SelectColumns(),
		wantSQL:  `SELECT * FROM "Album" WHERE "AlbumId" = $1::bigint`,
		wantArgs: []any{7},
	}.run(t)
}

func TestCompileTypedSQLArgumentLimitIsPerStatement(t *testing.T) {
	// Exactly the limit is accepted; the refusal tests cover one more.
	q := typedTestFrom("Album", "").NewQuery().
		Where(dal.NewComparison(typedTestField("a"), dal.In, dal.NewArray(make([]int, typedMaxArguments)))).
		SelectColumns()
	text, args, err := compileTypedSQL(q, newFakeTypedDialect(), typedCatalogFacts{})
	if err != nil || len(args) != typedMaxArguments || !strings.HasSuffix(text, "$65535::bigint)") {
		t.Fatalf("compileTypedSQL() with %d values: %d args, error %v", typedMaxArguments, len(args), err)
	}
}

func typedTestPtr[T any](value T) *T { return &value }

// TestCompileTypedSQLNamesThatAreColumnsOfAKnownSource pins what must keep
// compiling beside the refusals in typed_sql_errors_test.go: a name the catalog
// facts list is a column, and PostgreSQL reads a bare name as a column before it
// reads it as the whole row, so a column that shares its name with its table or
// alias is read as the column. A qualified column the facts list compiles too.
func TestCompileTypedSQLNamesThatAreColumnsOfAKnownSource(t *testing.T) {
	facts := typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
		{Name: "Invoice"}:  {Columns: []typedColumnFact{{Name: "Invoice"}, {Name: "x"}, {Name: "Total"}, {Name: "CustomerId", DataType: "int4", Category: typedTypeNumber}}},
		{Name: "Customer"}: {Columns: []typedColumnFact{{Name: "CustomerId", DataType: "int8", Category: typedTypeNumber}, {Name: "Name"}}},
	}}
	runTypedGoldens(t, []typedGolden{
		{
			name:    "a column named like its table",
			facts:   facts,
			query:   typedTestFrom("Invoice", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Invoice"), "")),
			wantSQL: `SELECT "Invoice" FROM "Invoice"`,
		},
		{
			name:  "a column named like its alias, in every clause",
			facts: facts,
			query: typedTestFrom("Invoice", "x").NewQuery().
				Where(typedTestEq(typedTestField("x"), 1)).
				OrderBy(dal.Ascending(typedTestField("x"))).
				SelectColumns(typedTestColumn(typedTestField("x"), "")),
			wantSQL:  `SELECT "x" FROM "Invoice" AS "x" WHERE "x" = $1::bigint ORDER BY "x" ASC NULLS FIRST`,
			wantArgs: []any{1},
		},
		{
			name:    "a qualified column",
			facts:   facts,
			query:   typedTestFrom("Invoice", "i").NewQuery().SelectColumns(typedTestColumn(typedTestQualified("i", "Total"), "")),
			wantSQL: `SELECT "i"."Total" FROM "Invoice" AS "i"`,
		},
		{
			name:  "a qualified column named like its alias",
			facts: facts,
			query: typedTestFrom("Invoice", "x").NewQuery().SelectColumns(typedTestColumn(typedTestQualified("x", "x"), "")),
			// x.x is the column x of x
			wantSQL: `SELECT "x"."x" FROM "Invoice" AS "x"`,
		},
		{
			name:  "join keys that the facts list",
			facts: facts,
			query: typedTestFrom("Invoice", "i").Join(
				dal.NewJoinedSource(dal.NewRootCollectionRef("Customer", "c"), dal.JoinInner, typedTestJoinOn("i", "CustomerId", "c", "CustomerId")),
			).NewQuery().SelectColumns(typedTestColumn(typedTestQualified("c", "Name"), "")),
			wantSQL: `SELECT "c"."Name" FROM "Invoice" AS "i" INNER JOIN "Customer" AS "c" ON ("i"."CustomerId" = "c"."CustomerId")`,
		},
		{
			// The facts know nothing of Genre, so the name is not checked; it is not
			// Genre's identity either.
			name:    "a source the facts do not know, with a bare name that is not its identity",
			facts:   facts,
			query:   typedTestFrom("Genre", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Name"), "")),
			wantSQL: `SELECT "Name" FROM "Genre"`,
		},
		{
			// Only a bare name can be the whole row. A qualified name is not compared
			// with the identity, and without facts it is not checked at all, which is why
			// the typedDialect contract has the readers pass facts for every source.
			name:    "a qualified name written like its alias, without facts",
			query:   typedTestFrom("Invoice", "x").NewQuery().SelectColumns(typedTestColumn(typedTestQualified("x", "x"), "")),
			wantSQL: `SELECT "x"."x" FROM "Invoice" AS "x"`,
		},
		{
			// With an alias the table's own name is hidden (PostgreSQL), so the bare
			// name Genre is no whole-row reference here and is not refused.
			name:    "a source with an alias, with a bare name equal to its table name",
			query:   typedTestFrom("Genre", "g").NewQuery().SelectColumns(typedTestColumn(typedTestField("Genre"), "")),
			wantSQL: `SELECT "Genre" FROM "Genre" AS "g"`,
		},
	})
}

// TestCompileTypedSQLFactsLookupsUseTheNameTheDialectWrites: under a folding
// dialect the compiler writes "total" for Total and TOTAL alike, so a facts lookup
// may match only a catalog column whose own name is "total". A catalog that holds
// Total (NOT NULL) and total (nullable) must never lend Total's NOT NULL to the
// column the server will sort, or the NULLS clause is dropped and the nullable
// column sorts with NULL on the wrong side.
func TestCompileTypedSQLFactsLookupsUseTheNameTheDialectWrites(t *testing.T) {
	for _, folding := range foldingTypedDialects(t) {
		t.Run(folding.name, func(t *testing.T) { checkFactsLookupsUseTheNameTheDialectWrites(t, folding) })
	}
}

func checkFactsLookupsUseTheNameTheDialectWrites(t *testing.T, folding foldingTypedDialect) {
	colliding := typedCatalogFacts{Fold: folding.fold, Sources: map[typedSourceName]typedSourceFacts{
		{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "Total", NotNull: true}, {Name: "total"}, {Name: "secret"}}},
	}}
	invoice := func() dal.IQueryBuilder { return typedTestFrom("Invoice", "").NewQuery() }
	runTypedGoldens(t, []typedGolden{
		{
			name:    "ORDER BY keeps the NULLS clause of the nullable column the server sorts",
			dialect: folding.dialect,
			facts:   colliding,
			query:   invoice().OrderBy(dal.Ascending(typedTestField("total"))).SelectColumns(typedTestColumn(typedTestField("total"), "")),
			wantSQL: `SELECT "total" FROM "invoice" ORDER BY "total" ASC NULLS FIRST`,
		},
		{
			name:    "the same, spelled as the mixed-case catalog name",
			dialect: folding.dialect,
			facts:   colliding,
			query:   invoice().OrderBy(dal.Descending(typedTestField("Total"))).SelectColumns(typedTestColumn(typedTestField("total"), "")),
			wantSQL: `SELECT "total" FROM "invoice" ORDER BY "total" DESC NULLS LAST`,
		},
	})
	t.Run("a select-all that would write a name the catalog does not hold is refused", func(t *testing.T) {
		q := invoice().SelectColumns(dal.AllColumnsExcept("secret"))
		expectTypedUnsupported(t, q, folding.dialect, colliding, `catalog column "Total"`)
	})
	// The refusal is on every catalog column the dialect cannot write, before the
	// exclusions apply (typedDialect, name resolution): excluding that very column
	// does not rescue the select-all.
	t.Run("a select-all is refused even when it excludes the column the dialect cannot write", func(t *testing.T) {
		q := invoice().SelectColumns(dal.AllColumnsExcept("Total"))
		expectTypedUnsupported(t, q, folding.dialect, colliding, `catalog column "Total"`)
	})
	// An exclusion is matched against the catalog's names after both are folded with
	// the dialect's rule: the statement writes the folded name, so catalog email with
	// the exclusion Email must leave the column out. Matched as written it fails open
	// and the column is returned.
	t.Run("an exclusion in another case than the catalog name excludes it", func(t *testing.T) {
		facts := typedCatalogFacts{Fold: folding.fold, Sources: map[typedSourceName]typedSourceFacts{
			{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "email"}, {Name: "emailnote"}, {Name: "secret"}}},
		}}
		runTypedGoldens(t, []typedGolden{
			{
				name: "exact name, other case", dialect: folding.dialect, facts: facts,
				query:   invoice().SelectColumns(dal.AllColumnsExcept("Email")),
				wantSQL: `SELECT "total", "emailnote", "secret" FROM "invoice"`,
			},
			{
				name: "upper case", dialect: folding.dialect, facts: facts,
				query:   invoice().SelectColumns(dal.AllColumnsExcept("SECRET", "EMAIL")),
				wantSQL: `SELECT "total", "emailnote" FROM "invoice"`,
			},
			{
				name: "mask in capitals", dialect: folding.dialect, facts: facts,
				query:   invoice().SelectColumns(dal.AllColumnsExcept("EMAIL*")),
				wantSQL: `SELECT "total", "secret" FROM "invoice"`,
			},
		})
	})
	t.Run("a select-all over names that are their own folded form compiles", func(t *testing.T) {
		lower := typedCatalogFacts{Fold: folding.fold, Sources: map[typedSourceName]typedSourceFacts{
			{Name: "invoice"}: {Columns: []typedColumnFact{{Name: "total"}, {Name: "secret"}}},
		}}
		typedGolden{
			dialect: folding.dialect,
			facts:   lower,
			query:   invoice().SelectColumns(dal.AllColumnsExcept("secret")),
			wantSQL: `SELECT "total" FROM "invoice"`,
		}.run(t)
	})
}
