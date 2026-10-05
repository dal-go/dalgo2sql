package dalgo2sql

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

// ErrTableNotFound matches, through errors.Is, the error a read returns for a table
// the database does not have under that name: see TableNotFoundError.
var ErrTableNotFound = errors.New("table not found")

// TableNotFoundError is the error a structured read returns, with the PostgreSQL
// dialect, for a source the database does not resolve under the name the query
// spells: no such table, view, materialized view, foreign table or partitioned table
// (a sequence is none of these), or a relation with no readable column. It names the
// table as the query wrote it, and the nearest name that does exist, when one is near
// enough to be a guess at what the caller meant.
//
// errors.Is(err, ErrTableNotFound) matches it. The message names the table the
// query asked for, quoted, never anything else the query held.
type TableNotFoundError struct {
	// Schema and Name are the source as the query wrote it; Schema is empty for a
	// source without one.
	Schema, Name string
	// Suggestion is the nearest existing name, schema included when the query named
	// one, or the zero value when no name is near.
	Suggestion typedSourceName
	// CaseSensitive says the dialect matches names exactly, so a name that differs
	// from an existing one only in case is a different name.
	CaseSensitive bool
}

func (e *TableNotFoundError) Error() string {
	var message strings.Builder
	message.WriteString("table " + typedMessageName(e.Schema, e.Name) + " not found")
	if e.Suggestion.Name != "" {
		message.WriteString("; did you mean " + typedMessageName(e.Suggestion.Schema, e.Suggestion.Name) + "?")
	}
	if e.CaseSensitive {
		message.WriteString(" Table names are case-sensitive.")
	}
	return message.String()
}

// Is makes errors.Is(err, ErrTableNotFound) true for every TableNotFoundError.
func (e *TableNotFoundError) Is(target error) bool { return target == ErrTableNotFound }

// typedMessageMaxName bounds how much of a name an error message carries: a name is
// the caller's, and may be very long.
const typedMessageMaxName = 80

// typedMessageName writes a table name for an error message, quoted, with its schema
// when it has one, and cut short when it is very long.
func typedMessageName(schema, name string) string {
	quote := func(part string) string {
		if len(part) > typedMessageMaxName {
			cut := typedMessageMaxName
			for cut > 0 && !utf8.RuneStart(part[cut]) {
				cut--
			}
			return strconv.Quote(part[:cut]) + "..."
		}
		return strconv.Quote(part)
	}
	if schema == "" {
		return quote(name)
	}
	return quote(schema) + "." + quote(name)
}

// typedFactsForQuery reads the catalog facts of every source the query names, with
// one lookup through dialect.catalogFacts on execute, and fails with a
// *TableNotFoundError for a source the facts do not know. A source is never compiled
// without facts: with them the compiler can check that a field is a column of its
// source, which it cannot do without (a qualified x.to_json on a source it does not
// know would be sent to the server, which reads it as a function of the row and
// returns the whole row inside one value). The lookup is keyed by the query's own
// spelling of the source, folded, schema included (typedCatalogFacts.source).
//
// The caller runs the statement on the same connection or transaction as execute.
func typedFactsForQuery(ctx context.Context, dialect typedDialect, execute executeQueryFunc, from dal.FromSource) (typedCatalogFacts, error) {
	sources := typedQuerySources(from)
	facts, err := dialect.catalogFacts(ctx, execute, sources)
	if err != nil {
		return typedCatalogFacts{}, err
	}
	for _, source := range sources {
		if _, known := facts.source(source); known {
			continue
		}
		notFound := &TableNotFoundError{Schema: source.Schema, Name: source.Name, CaseSensitive: facts.Fold == nil}
		// The hint is a courtesy: a lookup that fails leaves the error without one.
		if suggestion, ok, err := dialect.suggestSource(ctx, execute, source); err == nil && ok {
			notFound.Suggestion = suggestion
		}
		return typedCatalogFacts{}, notFound
	}
	return facts, nil
}

// compileTypedRead is what a reader does for a statically typed engine: the facts of
// every source, then the compiler. It returns the statement and the names the query
// asked for each column.
func compileTypedRead(ctx context.Context, q dal.StructuredQuery, dialect typedDialect, execute executeQueryFunc) (typedStatement, error) {
	facts, err := typedFactsForQuery(ctx, dialect, execute, q.From())
	if err != nil {
		return typedStatement{}, err
	}
	return compileTypedStatement(q, dialect, facts)
}

// typedNearestName picks, among candidates, the name nearest to asked: first one that
// differs from it only in case, else the one with the smallest edit distance when that
// is small enough to be a typo (two edits, or a third of the name, and always fewer
// edits than the name has characters). It returns -1 when
// no candidate is near. A candidate equal to asked is never chosen: it is the name that
// was not found.
func typedNearestName(asked string, candidates []string) int {
	lowerAsked := strings.ToLower(asked)
	for i, candidate := range candidates {
		if candidate != asked && strings.ToLower(candidate) == lowerAsked {
			return i
		}
	}
	length := utf8.RuneCountInString(asked)
	best, bestDistance := -1, min(max(2, length/3), length-1)+1
	for i, candidate := range candidates {
		if candidate == asked {
			continue
		}
		if distance := typedEditDistance(lowerAsked, strings.ToLower(candidate)); distance < bestDistance {
			best, bestDistance = i, distance
		}
	}
	return best
}

// typedEditDistance is the Levenshtein distance between two strings, in runes.
func typedEditDistance(a, b string) int {
	left, right := []rune(a), []rune(b)
	previous := make([]int, len(right)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(left); i++ {
		current := make([]int, len(right)+1)
		current[0] = i
		for j := 1; j <= len(right); j++ {
			cost := 1
			if left[i-1] == right[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous = current
	}
	return previous[len(right)]
}
