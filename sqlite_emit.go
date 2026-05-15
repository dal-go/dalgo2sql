package dalgo2sql

import (
	"fmt"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// emitSQL returns SQL text from a dal.StructuredQuery using SQLite/ANSI
// LIMIT N syntax (rather than T-SQL "SELECT TOP N"). It is a stopgap
// shim until upstream dalgo gains dialect-aware SQL emission — see the
// `dalgo-dialect-aware-sql-emission` Idea in
// `dal-go/dalgo/spec/ideas/`. Reader_base.go consumes the result for
// SQL-text-backed drivers (SQLite today; PostgreSQL planned).
//
// Behavior: takes the structured-query's existing String() output and,
// if a Limit is set, rewrites the leading `SELECT TOP N` into a
// trailing `LIMIT N` clause. The output is SQLite-compatible and works
// for any other SQL backend that accepts the ANSI LIMIT form
// (PostgreSQL, MySQL).
func emitSQL(q dal.StructuredQuery) string {
	text := q.String()
	if limit := q.Limit(); limit > 0 {
		topPrefix := fmt.Sprintf("SELECT TOP %d", limit)
		if strings.HasPrefix(text, topPrefix) {
			text = "SELECT" + text[len(topPrefix):]
			text += fmt.Sprintf("\nLIMIT %d", limit)
		}
	}
	return text
}
