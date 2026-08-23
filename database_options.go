package dalgo2sql

import (
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
