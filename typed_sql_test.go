package dalgo2sql

import (
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
		{Name: "Album"}:  {Columns: []typedColumnFact{{Name: "AlbumId", NotNull: true}, {Name: "Title"}}},
		{Name: "Artist"}: {Columns: []typedColumnFact{{Name: "ArtistId", NotNull: true}, {Name: "Name"}}},
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

func typedTestPtr[T any](value T) *T { return &value }
