package dalgo2sql

import (
	"errors"

	"modernc.org/sqlite"
)

// sqliteConstraintPrimaryKey and sqliteConstraintUnique are the extended
// SQLite result codes modernc.org/sqlite reports for a primary-key or
// unique-index constraint violation (see modernc.org/sqlite/lib:
// SQLITE_CONSTRAINT_PRIMARYKEY = 1555, SQLITE_CONSTRAINT_UNIQUE = 2067).
// They are hardcoded here rather than importing the generated
// modernc.org/sqlite/lib package for two integers.
const (
	sqliteConstraintPrimaryKey = 1555
	sqliteConstraintUnique     = 2067
)

// sqliteIsAlreadyExists is a DbOptions.IsAlreadyExists classifier for
// modernc.org/sqlite, the driver dalgo2sql's own conformance test (see
// conformance_test.go) runs against.
//
// dalgo2sql carries no runtime (non-test) dependency on any driver's error
// type — detecting a duplicate-key violation is inherently driver-specific,
// which is exactly why DbOptions.IsAlreadyExists exists as a hook a wrapping
// adapter fills in. This function is that hook's minimal implementation for
// modernc.org/sqlite, kept test-only so the shared dalgotest conformance
// suite's unconditional "rejects an Insert over an existing key with
// record.IsAlreadyExists" check can be satisfied here, in the base package,
// without weakening or skipping that check. A production wrapping adapter
// (e.g. dalgo2sqlite) supplies its own equivalent.
func sqliteIsAlreadyExists(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() {
	case sqliteConstraintPrimaryKey, sqliteConstraintUnique:
		return true
	default:
		return false
	}
}
