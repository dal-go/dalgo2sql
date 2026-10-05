package dalgo2sql

import (
	"context"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

// DbOptions provides database sqlOptions for DALgo - // TODO: document why & how to use
type DbOptions struct {
	ID         string
	PrimaryKey []string
	// Recordsets declares the recordsets (tables) of key reads and writes, by name,
	// with the primary key each is looked up by.
	//
	// The recordset of a key without a parent is declared under the key's
	// collection. The recordset of a nested key is declared under the collections
	// of the key and of its parents joined with "_", the key's own collection
	// first: the key orders/o1/lines/l5 addresses the table "lines_orders", and its
	// recordset is declared under "lines_orders". A nested key whose joined name is
	// not declared is refused in every operation, with an error wrapping
	// ErrUndeclaredNestedRecordset, before any statement is sent.
	//
	// A parent's ID is in no statement. The key orders/o1/lines/l5 and the key
	// orders/o2/lines/l5 address the same row of "lines_orders", and so does the
	// key of a collection named "lines_orders" with the ID l5: a SQL recordset
	// cannot tell the rows of different parents apart. Declare a nested recordset
	// only for a table whose primary key identifies the row without its parent.
	//
	// Delete follows the declared primary key, as every other operation does. A
	// key whose recordset is declared with no primary key, or with more than one
	// column, is refused before any statement (the errors Update returns). The
	// column ID is the default of Delete and DeleteMulti only where no recordset
	// is declared for the key, a nil entry being none.
	Recordsets map[string]*Recordset
	// Placeholder controls how SQL parameter markers are emitted.
	// The zero value (PlaceholderQuestion) uses "?" — compatible with
	// SQLite, MySQL, and most other drivers.  Set to PlaceholderDollar
	// for PostgreSQL, which requires "$1", "$2", … positional markers.
	Placeholder PlaceholderDialect
	// StructuredQueryDialect names the SQL dialect whose identifier quoting has
	// been reviewed. Empty preserves legacy emission; "sqlite" and "postgres" are
	// supported; any other value makes a structured read fail. It decides two
	// things:
	//
	//   - Structured reads: "sqlite" opts them into safe dialect-specific
	//     compilation, and so does "postgres", which reads the catalog facts of every
	//     source first, on the connection the statement runs on, and compiles with
	//     bound values only (see IdentifierCase and the README).
	//   - The collection, field and primary-key names of every key read and write
	//     (Exists, Get, GetMulti, Insert, Set, SetMulti, Update, UpdateMulti,
	//     Delete, DeleteMulti): with "sqlite" a name is written quoted, and only an
	//     empty name, a control character, invalid UTF-8 or more than 255 bytes is
	//     refused; with an empty or any other value, "postgres" included (its
	//     reviewed quoting covers structured reads, not these), a name must be a plain
	//     identifier (letters, digits and underscores, not starting with a digit,
	//     at most 255 bytes), or the call fails with ErrUnsafeName and sends no
	//     statement.
	//
	// A name that carries its own quoting ("Order Details" with the quote
	// characters, [Order Details]) is no longer a quoted identifier: with "sqlite"
	// it is one literal name, quote characters included, and finds no table, so
	// pass the bare name; with no dialect it is refused.
	StructuredQueryDialect string
	// NativeJoinHintTranslator optionally translates validated DALgo JOIN
	// algorithm preferences into trusted, dialect-owned SQL fragments. The
	// translator receives the complete relation tree so it can preserve each
	// edge's independent preference order. Nil is the adapter's explicit ignore
	// policy: a custom compiler receives no fragments. It also preserves the
	// existing SQL byte stream, which is how SQLite ignores JOIN hints by default.
	NativeJoinHintTranslator NativeJoinHintTranslator
	// NativeJoinEligibility is the adapter's opt-in semantic proof for native
	// JOIN execution with NativeStructuredQueryCompiler. Both fields are
	// required for a non-SQLite native path; a nil hook preserves SQLite's
	// transaction-local key preflight and declines other dialects.
	NativeJoinEligibility NativeJoinEligibility
	// NativeStructuredQueryCompiler emits a complete structured SQL query for a
	// trusted adapter dialect. It receives translated JOIN hint fragments on the
	// actual read path. dalgo2sql provides no SQL Server or Oracle compiler.
	NativeStructuredQueryCompiler NativeStructuredQueryCompiler
	// IdentifierCase is read with StructuredQueryDialect "postgres" and picks how
	// names are written: as the query spells them (the default), or folded to lower
	// case. Any other value makes every structured read, join check and JoinFields
	// call fail with an error that names it. See IdentifierCase.
	IdentifierCase IdentifierCase

	// IsAlreadyExists reports whether err — the raw error returned by the
	// underlying database/sql driver for a failed INSERT — represents a
	// duplicate-key violation (a unique or primary-key constraint failure).
	// Detection is driver-specific — pgx's *pgconn.PgError with code
	// "23505", go-sql-driver/mysql's *mysql.MySQLError with number 1062,
	// modernc.org/sqlite's *sqlite.Error with an SQLITE_CONSTRAINT_* code —
	// so dalgo2sql cannot recognize it on its own. The wrapping adapter
	// (dalgo2postgres, dalgo2mysql, dalgo2sqlite, …) supplies this hook.
	//
	// The zero value (nil) is backward compatible: it preserves today's
	// behavior exactly, and the raw driver error passes through unwrapped.
	// When set and it reports true for an insert's error, dalgo2sql wraps
	// that error with record.ErrRecordExists (see execInsert) so callers
	// can test it with record.IsAlreadyExists — the driver error itself is
	// preserved in the chain, never replaced, so existing callers matching
	// on error text or type keep working.
	IsAlreadyExists func(err error) bool
}

// IdentifierCase says how a SQL dialect that quotes every name writes the names a
// query spells. It is read with StructuredQueryDialect "postgres"; the other dialects
// ignore it. The zero value is IdentifierCaseExact.
//
// A mount that folds case matches names by their lower-case form on both sides of
// every check, and the result of a query keeps the names the query asked for: a
// field written Total is selected as "total" and comes back as Total. No field mask
// or access check may be applied on such a mount unless the names it compares are
// folded first, or Total would pass a mask that names total.
type IdentifierCase string

const (
	// IdentifierCaseExact writes every name as the query spells it, inside quotes,
	// so Album and album are two tables. It is the mode for databases whose objects
	// were created with quoted mixed-case names, and the default.
	IdentifierCaseExact IdentifierCase = "exact"
	// IdentifierCaseFoldLower writes every name lower-cased, inside quotes, as
	// dalgo2postgres's DDL stores them, so a database it created is read back with
	// any spelling of a name.
	IdentifierCaseFoldLower IdentifierCase = "fold-lower"
)

// NativeJoinEligibility lets a concrete adapter validate whether its native
// compiler can preserve DALgo JOIN semantics for one complete query.
type NativeJoinEligibility func(context.Context, dal.StructuredQuery) error

// NativeStructuredQueryCompiler emits a complete structured query for a
// concrete adapter dialect. Its implementation owns parameter syntax,
// identifier quoting, and all dialect semantics.
type NativeStructuredQueryCompiler interface {
	CompileNativeStructuredQuery(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error)
}

// NativeJoinHintTranslator translates a complete, validated relation tree for
// one native SQL query. It is implemented by trusted database adapters, never
// from DTQL input. An error prevents SQL from being emitted.
type NativeJoinHintTranslator interface {
	TranslateNativeJoinHints(dal.FromSource) (NativeJoinHintFragments, error)
}

// NativeJoinHintFragments identifies where a dialect places its trusted SQL
// JOIN hints. JoinOperators is keyed by DALgo structural paths such as
// "from.joins[0]" and is emitted between the JOIN type and JOIN keyword, so a
// SQL Server adapter can return "HASH" for `INNER HASH JOIN`. AfterSelect and
// AfterQuery support dialects such as Oracle that place optimizer hints after
// SELECT or at the end of the statement. HandledPaths must acknowledge every
// hinted edge as applied or intentionally ignored by adapter policy; a rejected
// preference is returned as an error. This prevents a configured translator
// from silently dropping a per-edge preference while using only a statement
// level fragment.
type NativeJoinHintFragments struct {
	AfterSelect   string
	JoinOperators map[string]string
	AfterQuery    string
	HandledPaths  []string
}

func primaryKeyForQuery(options DbOptions, query dal.Query) string {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q.From() == nil || q.From().Base() == nil {
		return ""
	}
	if rs := options.Recordsets[q.From().Base().Name()]; rs != nil {
		if fields := rs.PrimaryKey(); len(fields) == 1 {
			return fields[0].Name()
		}
	}
	if len(options.PrimaryKey) == 1 {
		return options.PrimaryKey[0]
	}
	return ""
}

func (o DbOptions) GetRecordsetByKey(key *record.Key) *Recordset {
	rsName := getRecordsetName(key)
	return o.Recordsets[rsName]
}

func (o DbOptions) PrimaryKeyFieldNames(key *record.Key) (primaryKey []string) {
	rs := o.GetRecordsetByKey(key)
	if pk := rs.PrimaryKey(); len(pk) > 0 {
		primaryKey = make([]string, len(pk))
		for i, f := range pk {
			primaryKey[i] = f.Name()
		}
		return
	}
	return nil
}
