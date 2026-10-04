# dalgo2sql

SQL adapter for [DALgo](https://github.com/dal-go/dalgo) - a Database Abstraction Layer in Go.

<!-- dev-approach:v1 -->
## Our approach to development

We build with our own tooling:

- **[SpecScore](https://specscore.md)** — specify requirements as `SpecScore.md` artifacts
- **[SpecStudio](https://specscore.studio)** — author & manage specs across their lifecycle
- **[inGitDB](https://ingitdb.com)** — store structured data in Git where applicable
- **[DALgo](https://dalgo.io)** — data access layer for Go
- **[cover100.dev](https://cover100.dev)** — drive toward 100% test coverage
- **[DataTug](https://datatug.io)** — query & explore data
<!-- /dev-approach -->

## Status

[![Lint, Vet, Build, Test](https://github.com/dal-go/dalgo2sql/actions/workflows/ci.yml/badge.svg?cache=1)](https://github.com/dal-go/dalgo2sql/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/dal-go/dalgo2sql)](https://goreportcard.com/report/github.com/dal-go/dalgo2sql)
[![GoDoc](https://godoc.org/github.com/dal-go/dalgo2sql?status.svg)](https://godoc.org/github.com/dal-go/dalgo2sql)

## Usage

    go get github.com/dal-go/dalgo2sql

## SQLite structured aggregation

With `DbOptions.StructuredQueryDialect` set to `sqlite`, structured DALgo queries
render GROUP BY, COUNT, SUM, AVG, MIN, MAX, DISTINCT aggregates, HAVING, alias
rewrites, result ordering and pagination natively. SUM/AVG normalize numeric
inputs to REAL for parity with DALgo's generic `float64` fallback. FIRST/LAST
are deliberately not advertised until DALgo models aggregate-local ordering;
using unspecified SQLite row order would not be deterministic.

## Which path compiles a structured query

A structured DALgo query reaches SQL by one of three paths, and which one is
decided by the engine, not by the query.

- **SQLite** (`DbOptions.StructuredQueryDialect: "sqlite"`) uses the SQLite
  structured emitter, `compileStructuredSQL`, described above. It is built around
  SQLite's dynamic typing and has its own path; the typed compiler does not touch
  it.
- **PostgreSQL** uses the typed SQL compiler, not the legacy emitter.
  `compileTypedSQL(query, dialect, facts)` renders one `SELECT` for a statically
  typed engine, `newPostgresDialect(mode)` supplies PostgreSQL's spelling, and
  `dialect.catalogFacts` reads the `facts` (every column of each source, their type
  categories and NOT NULL flags) with one catalog query. Every constant is a bound
  argument and every name goes through the dialect's quoting, so no value is
  written into the statement text. The two identifier modes are exact (names as the
  query spells them, for databases created with quoted mixed-case names) and fold
  to lower case (for databases created by dalgo2postgres, which stores lower-case
  names). A query the compiler cannot run faithfully in one statement is refused
  with an error matching `dal.ErrNotSupported`, which is the signal for DALgo's
  generic engine; it is never handed to the legacy emitter. The exact SQL and
  arguments of every supported shape are in `testdata/postgres`. The compiler and
  the dialect are in the package but `DbOptions` does not select them yet:
  `StructuredQueryDialect: "postgres"` is refused as an unsupported dialect until
  the readers dispatch to the typed compiler.
- **No dialect, no native compiler** uses the legacy text emitter, described next.
  PostgreSQL is not meant to be served by it; a PostgreSQL source stays on it only
  until it is opened with the dialect above.

### Legacy text path

A source opened with no `StructuredQueryDialect` and no
`NativeStructuredQueryCompiler` renders structured queries through the legacy
text emitter. That is every such source, among them datatug-cli SQLite sources
opened without the `sqlite` dialect and openvaultdb-go's PostgreSQL and MySQL
mounts today. The emitter pastes names and values straight into the statement, so
it fails closed: a name or value it cannot prove plain (brackets, backslashes,
control characters, quotes inside JSON-rendered slices, `<`, `>`, `&`, U+2028,
U+2029, invalid UTF-8, a `[]byte` constant, named slice types, non-finite
numbers, times outside the JSON range) gets an error wrapping
`dal.ErrNotSupported`, and no SQL is executed.

## NUMERIC result values

SQLite results are unchanged in the recordset reader. In the records reader a
bare `NUMERIC` column changes in two cases: a BLOB holding decimal text, and the
texts `NaN`, `Infinity` and `-Infinity`, become `float64`. With lib/pq the
recordset reader keeps `NUMERIC` as `[]byte`; only the records reader converts.
Text that is not a number stays a string, and a `float64`-typed recordset column
refuses it with an error.

## End2end - is a separate module

For end-to-end testing a SQLite driver is used.
To avoid bringing a dependency to SQLite into the consumers of dalgo2sql,
the [end2end](end2end) tests are in a separate module.

This is an unusual approach, as usually you would want to bring dependency to underlying driver with a dalgo adapter.
But this is not a case for this adapter as `database/sql` that is referenced by `dalgo2sql` is an abstraction layer
and consumer is free to choose the underlying driver.

## Reading rows into structs

Both the records reader (queries that read into a record) and single-record
`Get` accept a pointer to a struct as the record data. A column reaches a field
by its `db` tag, or by the Go field name without a tag; `db:"-"` and unexported
fields are never filled. Fields promoted from embedded structs count, and a nil
embedded struct pointer is allocated when a column needs it (not when its type
is unexported, which is an error).

The fields are listed breadth-first, as scany does: every exported field is
reachable by its own name, including promoted fields that share a Go name but
carry different tags (two embedded structs that each have an `ID` tagged
`user_id` and `account_id`, or an outer `ID` tagged `id` beside an embedded
`ID` tagged `legacy_id`). An embedded struct tagged `db:"-"` is skipped with all
its fields. A column is matched to a field by exact name first, then without
regard to case, then without regard to case and underscores, so `AreaSqKm`,
`areasqkm` and `area_sq_km` all reach a field `AreaSqKm`. Where two fields spell
the same name, the shallower one wins, and at one depth the first in declaration
order. Two columns of one row that reach the same field are an error, never a
silent overwrite.

One difference in precedence from scany: the exact spelling is tried first, so
with an untagged `Name` declared before a field tagged `db:"name"`, the column
`name` reaches the tagged field (scany gave it to `Name`, whose snake_case name
is also `name`); the column `Name` still reaches the untagged one.

**Records reader.** A column without a field is skipped (the identity column of
a record is not a field of its data). `NULL` stores the zero value, or calls
`Scan(nil)` on a `sql.Scanner`. Values follow what a map target gets, with
these differences by field type:

- a `sql.Scanner` receives the driver's value unchanged, so a decimal type sees
  the exact `NUMERIC` text and a JSON type sees `[]byte`;
- a `string` field takes the driver's text, including the exact `NUMERIC` text;
- a `[]byte` or `json.RawMessage` field takes a copy of the driver's bytes;
- an integer field takes the integer the driver's text spells, so a `NUMERIC`
  such as `9007199254740993` (what PostgreSQL returns for `SUM` of a `bigint`)
  keeps every digit and the whole `int64` and `uint64` range is reachable; a
  whole `NUMERIC` with a scale (`9007199254740993.00`, what `SUM` over
  `numeric(p,2)` returns) is read from the part before the point, so it keeps
  every digit too; a fraction is an error;
- float and `bool` fields take the normalised value (`NUMERIC` becomes
  `float64`); `time.Time` and `any` fields take it as is (`[]byte` becomes
  `string` for an `any` field, as in a map). Every field is checked for
  overflow.

**Get.** Only the column matcher differs from the scany library this path used
before; values are still scanned by `database/sql` into the field addresses, so
its conversions (an integer cell into a `string` field, `[]byte` into
`json.RawMessage` or a `sql.Scanner`, `NULL` being an error for a non-pointer
field) are unchanged. scany stays in charge of a `sql.Scanner` target with one
column and of a row that has a dotted column name no field matches: scany
accepts dotted names for nested struct fields (`address.city`), and for such a
row it matches every other column by its own spelling too. Any other column
without a field is an error that names the column and the struct type
(`column "typo": no corresponding field in T`). What now differs: names match
without regard to case and underscores (scany matches `db` tags exactly and Go
names as snake_case), so a SQLite column `Name` reaches the field `Name`; column
names scany rejected can match; two columns that reach one field are an error;
and an exact name wins over a looser one (see the precedence note above).

## Keys-only query order

A keys-only query (`SelectKeysOnly`) with no `ORDER BY` is ordered ascending by
the primary key (from `DbOptions.Recordsets`, else `DbOptions.PrimaryKey`), so
the order is defined and repeatable on each database. Text keys sort by that
database's collation (the SQLite compiler uses `BINARY`; the legacy emitter,
PostgreSQL and MySQL use the column's own collation). A query that names an
order keeps it. Two cases keep the statement they always had: a query with joins
(the unqualified key is refused by the SQLite compiler and ambiguous elsewhere),
and, on the legacy text emitter, a primary key that is not a plain identifier.

## License

Free to use and open source under [MIT License](LICENSE). 
