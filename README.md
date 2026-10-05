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

A structured DALgo query reaches SQL by one of four paths. They are tried in this
order, and which one runs is decided by how the source was opened, not by the query.

1. **A native compiler** (`DbOptions.NativeStructuredQueryCompiler`). When one is
   set it writes the whole statement, and `StructuredQueryDialect` is not
   consulted. It is for trusted adapter dialects that bring their own compiler.
2. **SQLite** (`DbOptions.StructuredQueryDialect: "sqlite"`) uses the SQLite
   structured emitter, `compileStructuredSQL`, described above. It is built around
   SQLite's dynamic typing and has its own path; the typed compiler does not touch
   it.
3. **PostgreSQL** (`DbOptions.StructuredQueryDialect: "postgres"`) uses the typed SQL
   compiler, not the legacy emitter. It is described in the next section.
4. **No dialect, no native compiler** uses the legacy text emitter, described after
   that. A PostgreSQL source should not stay on it; open it with the dialect above.

Any other non-empty dialect is refused with an error when a structured query is
read.

### PostgreSQL

With `StructuredQueryDialect: "postgres"` a structured read runs in two steps on
one connection: one catalog query that reads every column of each source the query
names (their types, NOT NULL flags and collations), then the one `SELECT` the typed
compiler writes from that answer. `compileTypedSQL(query, dialect, facts)` is the
compiler and `newPostgresDialect(mode)` is PostgreSQL's spelling; the exact SQL and
arguments of every supported shape are in `testdata/postgres`.

- **No value is written into the statement.** Every constant is a bound argument and
  every name goes through the dialect's quoting (`"` doubled, NUL and names over 63
  bytes refused). Pass Go integers for whole numbers: a whole number that arrives as
  a `float64` binds as `numeric`, which is correct but cannot use an index on an
  integer column. A constant that does not match its column (a number against text)
  is a server error, not an empty result.
- **Identifier case** is `DbOptions.IdentifierCase`: `IdentifierCaseExact` (the
  default) writes names as the query spells them, for databases created with quoted
  mixed-case names; `IdentifierCaseFoldLower` writes them lower-cased, for databases
  created by dalgo2postgres. The result keeps the names the query asked for in both
  modes (`Total` comes back as `Total`, an unaliased `COUNT(*)` as `COUNT(*)`). A select list
  whose names differ only in case is refused under `IdentifierCaseFoldLower`. Do not
  put a field mask or access check on such a mount unless the names it compares are
  folded first. A configured primary key is matched to the result's column by the same
  fold, so a key configured as `ID` keys the records of a column the server returns as
  `id`.
- **A table that is not there** fails with `*TableNotFoundError` (it matches
  `ErrTableNotFound`), which names the table and the nearest name that exists
  (`SuggestedSchema` and `SuggestedName`; both empty when none is near):
  `table "album" not found; did you mean "Album"? Table names are case-sensitive.`
  Under `IdentifierCaseFoldLower` only a name the mount can write is suggested.
  A sequence, a name that resolves to nothing and a relation with no readable column
  are all "not found"; a source is never compiled without its catalog facts.
- **Native aggregation and joins.** The adapter reports GROUP BY, HAVING, ORDER BY,
  COUNT, SUM and AVG with their DISTINCT forms, MIN and MAX as native
  (`QueryCapabilities`), so DALgo runs them on the server. FIRST and LAST, and any
  query with a subquery, stay in DALgo's generic engine. A join is accepted
  (`CanExecuteJoin`) when each ON pair has the same type category (numbers with
  numbers, text with text) or the same type, on the database handle as in a
  transaction; it is declined when the types differ or a key's type has no usable
  equality (json, xml, geometric types, `oid` and the `reg*` types), when the
  catalog does not hold a key, or when the compiler cannot write the query.
  `JoinFields` lists a table's columns from the catalog, under the names the catalog
  has, whatever the identifier case.
- **Arithmetic** is on double precision: `+`, `-`, `*` and `/` read both operands as
  `double precision`, as DALgo's generic engine does, so an integer overflow cannot
  differ between engines; division by zero is NULL. SUM and AVG are cast to double
  precision. NULLs sort first ascending and last descending, as in DALgo, and the
  `NULLS` clause is left out for a NOT NULL column.
- **A query the compiler cannot run faithfully in one statement** is refused with an
  error matching `dal.ErrNotSupported`, among them a result column whose alias, or whose
  expression text, is over 63 bytes. DALgo falls back to its generic engine only where
  it has a fallback: a join, which it asks the adapter to accept first (`CanExecuteJoin`
  compiles the whole query, and DALgo runs the generic join when it declines), and a
  query with a subquery. For a query over one source the refusal is the read's error:
  a 64-byte alias fails the read, and the remedy is a shorter alias. It is never handed
  to the legacy emitter.
- **A join keys each record by the base row** (the first source of `FROM`), as DALgo's
  generic join does, whichever columns the select list names. The records reader adds
  the base's configured primary key to the statement as a column of its own, qualified
  by the base's alias, and reads the key from it by position, so a column of a joined
  table that carries the key's name never keys a record. A select-all over joins, which
  no column can be added to, is keyed from the base's own column of that name (the
  catalog's list of the base's columns, a statement of its own on SQLite); a key the
  base has no column for leaves the records with the placeholder key, as a select-all
  over one source does. The same holds for the SQLite dialect. A select list that gives
  two expressions of a join one output name (`a.id` and `r.id`) is refused with
  `join_field: duplicate output name`, the error DALgo's generic engine gives for the
  same query, with the SQLite dialect too.
- **Records are keyed by the source's primary key.** The records reader keys each record
  by the primary key column of the recordset declared for the source in
  `DbOptions.Recordsets` (else `DbOptions.PrimaryKey`). With no key configured, the key is
  the source's primary key as the catalog reports it, when that is exactly one column. The
  one catalog query that serves the compiler serves the key, so a read still sends one. A
  source with no primary key, with a composite one, a view, a materialized view and a foreign
  table (the catalog reports none for these), and a grouped or aggregated query, key their
  rows by ordinal: the position of the row in the result, from 0, as decimal text. A recordset
  that is declared with no single-column primary key keeps the literal ID
  `__dalgo_record_id`, as does a read with the SQLite dialect, the legacy emitter or a native
  compiler of the caller's and no key configured: they have no catalog to ask. On a fold-lower
  mount a declared recordset is looked up by the query's spelling of the source, then by that
  spelling folded as the catalog lookup folds it, so a recordset declared as `album` is found
  by a query that says `Album` or `ALBUM`.
- **A stream error is never the end of a result.** Both readers return the error the
  server raised in the middle of a result (a timeout, an overflow at one row) from
  `Next`, instead of `ErrNoMoreRecords`; a read whose context ended returns the
  context's error.
- **The DTQL money option** is refused (`ErrNotSupported`) whatever the select list:
  the records reader reads the option from the caller's query before it adds the key
  column to it.
- **No protected-write factory.** `NewDatabase` returns the plain adapter for
  PostgreSQL; the protected read/write profile exists for SQLite only.

The catalog query and the statement need the same connection, so the database handle
takes one connection for the life of a structured read and gives it back when the
reader is closed, when it is read to its end, or when the context of the read ends,
as the pool does for a read of its own.

### Legacy text path

A source opened with no `StructuredQueryDialect` and no
`NativeStructuredQueryCompiler` renders structured queries through the legacy
text emitter. A source leaves this path by setting a dialect: `sqlite` for SQLite
and `postgres` for PostgreSQL, which is selectable. MySQL has no dialect, so a
MySQL source stays on the legacy path, and so does a PostgreSQL or SQLite source
opened without one, until its consumer sets it.

The emitter pastes names and values straight into the statement, so it fails
closed. What it refuses, by where it is found (each refusal is an error wrapping
`dal.ErrNotSupported`, and no SQL is executed):

- **A name** (collection, schema, alias, field, column alias, function name) that
  is not a plain identifier.
- **A string constant**, anywhere (a plain value, an `IN` list, inside a slice),
  that holds a backslash, a bracket or a control character.
- **A string inside a slice passed as one constant** (`dal.Constant` holding
  a `[]string` or `[]any`, which `encoding/json` renders) that also holds a double
  quote, `<`, `>`, `&`, U+2028, U+2029 or invalid UTF-8, which json writes as an
  escape or replaces with U+FFFD. A plain value and an `IN` list may hold these
  characters.
- **A constant of a kind whose text cannot be checked**: a `[]byte` (or any byte
  slice, nested ones included), a named slice type, a non-finite number (NaN,
  Infinity, -Infinity, `float32` included), a time outside the range JSON can
  write, a time inside an `IN` list, and any Go type that is not a scalar or an
  unnamed slice of scalars.
- **An aggregate** other than COUNT, SUM, AVG, MIN and MAX, and a keys-only query
  that would be ordered by a field that is a reserved word (such as a primary key
  named `order`), because the emitter writes the name unquoted and the server
  rejects it.

## NUMERIC result values

Since dalgo2sql v0.21.0 the readers turn the text a driver delivers for a column
typed `NUMERIC` (pgx delivers PostgreSQL's `NUMERIC` as text) into a `float64`
when it is decimal text, or exactly `NaN`, `Infinity` or `-Infinity`. Text that is
not a number stays a string, and a `float64`-typed recordset column refuses it
with an error. The records reader converts every such column; the recordset reader
converts a `float64`-typed column, and with lib/pq, which delivers `NUMERIC` as
`[]byte`, it keeps `[]byte`.

Compared with dalgo2sql v0.20, SQLite results are unchanged in the recordset
reader. In the records reader a bare `NUMERIC` column changes in two cases: a BLOB
holding decimal text, and the texts `NaN`, `Infinity` and `-Infinity`, become
`float64`. A `DECIMAL` column is not `NUMERIC` and keeps its text.

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

An exact spelling beats depth, as it beats a looser spelling: with an untagged
`ID` in the outer struct and an embedded field tagged `db:"id"`, the column `id`
reaches the tagged embedded field, not the outer one (scany gave it to the outer
`ID`, whose snake_case name is also `id`); the column `ID` still reaches the outer
field.

A struct field that is named (not embedded) is one column, named after its `db`
tag or its Go name, whatever its type. scany's `db:""` on a named struct field,
which maps the nested struct's fields without a prefix, is not supported: those
fields are not reached, and a column that names one of them has no field.

One difference in precedence from scany: the exact spelling is tried first, so
with an untagged `Name` declared before a field tagged `db:"name"`, the column
`name` reaches the tagged field (scany gave it to `Name`, whose snake_case name
is also `name`); the column `Name` still reaches the untagged one.

**Records reader.** A column without a field is skipped (the identity column of
a record is not a field of its data). `NULL` is `nil` in a pointer, an
interface or a `[]byte` field, calls `Scan(nil)` on a `sql.Scanner`, and is an
error naming the column for any other field, as it is for `Get` (it used to store
the zero value). Values follow what a map target gets, with these differences by
field type:

- a `sql.Scanner` receives the driver's value unchanged, so a decimal type sees
  the exact `NUMERIC` text and a JSON type sees `[]byte`;
- a `string` field takes the driver's text, including the exact `NUMERIC` text;
- a `[]byte` or `json.RawMessage` field takes a copy of the driver's bytes;
- an integer field takes the integer the driver's text spells, so a `NUMERIC`
  such as `9007199254740993` (what PostgreSQL returns for `SUM` of a `bigint`)
  keeps every digit and the whole `int64` and `uint64` range is reachable; a
  whole `NUMERIC` with a scale (`9007199254740993.00`, what `SUM` over
  `numeric(p,2)` returns) is read from the part before the point, so it keeps
  every digit too; a fraction is an error, and so is a whole number outside the
  field's range (`-9223372036854775809` for an `int64` is an error, never the
  nearest bound);
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
the primary key (from `DbOptions.Recordsets`, else `DbOptions.PrimaryKey`, else, with
the PostgreSQL dialect, the primary key the catalog reports), so
the order is defined and repeatable on each database. Text keys sort by that
database's collation (the SQLite compiler uses `BINARY`; the legacy emitter,
PostgreSQL and MySQL use the column's own collation). A query that names an
order keeps it. Two cases keep the statement they always had: a query with joins
(the unqualified key is refused by the SQLite compiler and ambiguous elsewhere),
and, on the legacy text emitter, a primary key that is not a plain identifier.

## License

Free to use and open source under [MIT License](LICENSE). 
