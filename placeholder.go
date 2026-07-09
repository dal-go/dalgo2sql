package dalgo2sql

import (
	"fmt"
	"strings"
)

// PlaceholderDialect selects how positional SQL parameters are formatted.
// The zero value (PlaceholderQuestion) is backward-compatible with all
// drivers that accept "?" — SQLite, MySQL, etc.
type PlaceholderDialect int

const (
	// PlaceholderQuestion emits "?" for every parameter (default, SQLite/MySQL style).
	PlaceholderQuestion PlaceholderDialect = iota
	// PlaceholderDollar emits "$1", "$2", … (PostgreSQL style).
	PlaceholderDollar
)

// placeholder returns the SQL placeholder for the n-th argument (1-based).
// For PlaceholderQuestion the index is ignored and "?" is returned.
// For PlaceholderDollar "$n" is returned.
func (d PlaceholderDialect) placeholder(n int) string {
	if d == PlaceholderDollar {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// rewritePlaceholders rewrites a SQL string that contains "?" placeholders
// into the format required by the dialect.  For PlaceholderQuestion the
// string is returned unchanged.  For PlaceholderDollar each "?" is replaced
// with "$1", "$2", … in order.
func (d PlaceholderDialect) rewritePlaceholders(sql string) string {
	if d != PlaceholderDollar {
		return sql
	}
	var sb strings.Builder
	n := 1
	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' {
			fmt.Fprintf(&sb, "$%d", n)
			n++
		} else {
			sb.WriteByte(sql[i])
		}
	}
	return sb.String()
}
