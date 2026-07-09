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
// Behavior: takes the structured-query's existing String() output and:
//  1. Rewrites `FROM [table]` bracket-quoting (T-SQL / MSSQL style) to
//     plain `FROM table` — Postgres and SQLite both reject the bracket form.
//  2. If a Limit is set, rewrites the leading `SELECT TOP N` into a
//     trailing `LIMIT N` clause. The output is SQLite-compatible and works
//     for any other SQL backend that accepts the ANSI LIMIT form
//     (PostgreSQL, MySQL).
func emitSQL(q dal.StructuredQuery) string {
	text := q.String()
	// Strip square-bracket table-name quoting emitted by dal.structuredQuery.String().
	// The dal package currently emits `FROM [tableName]` which is MSSQL syntax;
	// SQLite and PostgreSQL use bare names or double-quotes.
	text = stripBracketIdents(text)
	if limit := q.Limit(); limit > 0 {
		topPrefix := fmt.Sprintf("SELECT TOP %d", limit)
		if strings.HasPrefix(text, topPrefix) {
			text = "SELECT" + text[len(topPrefix):]
			text += fmt.Sprintf("\nLIMIT %d", limit)
		}
	}
	return text
}

// stripBracketIdents replaces all occurrences of [identifier] with
// the bare identifier.  This converts MSSQL-style bracket quoting
// (emitted by dal.structuredQuery.String()) into SQL that SQLite and
// PostgreSQL accept.
func stripBracketIdents(sql string) string {
	var sb strings.Builder
	i := 0
	for i < len(sql) {
		if sql[i] == '[' {
			j := strings.IndexByte(sql[i+1:], ']')
			if j >= 0 {
				sb.WriteString(sql[i+1 : i+1+j])
				i = i + 1 + j + 1
				continue
			}
		}
		sb.WriteByte(sql[i])
		i++
	}
	return sb.String()
}
