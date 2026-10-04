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

## Legacy text path (no dialect, no native compiler)

A source opened with no `StructuredQueryDialect` and no
`NativeStructuredQueryCompiler` renders structured queries through the legacy
text emitter. That is every such source, among them datatug-cli SQLite sources
opened without the `sqlite` dialect and openvaultdb-go's PostgreSQL and MySQL
mounts. The emitter pastes names and values straight into the statement, so it
fails closed: a name or value it cannot prove plain (brackets, backslashes,
control characters, quotes inside JSON-rendered slices, `<`, `>`, `&`, U+2028,
U+2029, invalid UTF-8, a `[]byte` constant, named slice types, non-finite
numbers, times outside the JSON range) gets an error wrapping
`dal.ErrNotSupported`, and no SQL is executed. The PostgreSQL and MySQL mounts
stay on this guarded path until the PostgreSQL adapter work gives them a native
compiler.

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

## License

Free to use and open source under [MIT License](LICENSE). 
