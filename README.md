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

A column is matched to a field by exact name first, then without regard to case,
then without regard to case and underscores, so `AreaSqKm`, `areasqkm` and
`area_sq_km` all reach a field `AreaSqKm`. Where two fields spell the same name,
the first in declaration order wins. Two columns of one row that reach the same
field are an error, never a silent overwrite.

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
  keeps every digit and the whole `int64` and `uint64` range is reachable;
  text that is not a plain integer (`12.00`) is converted from the normalised
  value, which is exact only up to 2^53, and a fraction is an error;
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
names scany rejected can match; and two columns that reach one field are an
error.

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
