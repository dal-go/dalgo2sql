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
	Recordsets map[string]*Recordset
	// Placeholder controls how SQL parameter markers are emitted.
	// The zero value (PlaceholderQuestion) uses "?" — compatible with
	// SQLite, MySQL, and most other drivers.  Set to PlaceholderDollar
	// for PostgreSQL, which requires "$1", "$2", … positional markers.
	Placeholder PlaceholderDialect
	// StructuredQueryDialect opts structured reads into safe dialect-specific
	// compilation. Empty preserves legacy emission; "sqlite" is supported.
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
