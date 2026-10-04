package dalgo2sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// typedMarker is the neutral placeholder every fragment of a typed statement
// uses while it is being assembled. Fragments are built independently and in an
// order that differs from the text (a grouped expression may be replaced by its
// ordinal after it was compiled), so numbering them while compiling would be
// fragile. numberTypedPlaceholders numbers them once, over the final text.
const typedMarker = '?'

// typedPlaceholderStyle describes how a dialect spells placeholders.
type typedPlaceholderStyle struct {
	// Prefix, when not empty, makes the nth marker "<Prefix>n": "$" gives $1,
	// $2, ... Empty keeps the marker as "?" (SQLite and MySQL style).
	Prefix string
	// IdentQuote is the byte that opens and closes a quoted identifier. An
	// identifier may legally contain the marker, so nothing between two quotes
	// is numbered. A doubled quote inside an identifier toggles twice and
	// leaves the scan in the right state.
	IdentQuote byte
}

// numberTypedPlaceholders rewrites the markers of text into the dialect's
// style, skipping quoted identifiers, and checks that the text carries exactly
// one marker per argument: a mismatch means a fragment lost or invented a
// parameter, and executing it could bind a value to the wrong position.
//
// It understands quoted identifiers and nothing else, so it also refuses any
// text that holds a string literal or a comment outside one: a marker inside
// either would be numbered, and no value is ever meant to be written into the
// text. A dialect fragment is the only way such text could appear.
func numberTypedPlaceholders(text string, style typedPlaceholderStyle, arguments int) (string, error) {
	var out strings.Builder
	out.Grow(len(text) + 2*arguments)
	inIdentifier := false
	markers := 0
	for i := 0; i < len(text); i++ {
		b := text[i]
		switch {
		case b == style.IdentQuote:
			inIdentifier = !inIdentifier
		case inIdentifier:
			// Nothing inside a quoted identifier is a marker or a literal.
		case b == typedMarker:
			markers++
			if style.Prefix != "" {
				out.WriteString(style.Prefix)
				out.WriteString(strconv.Itoa(markers))
				continue
			}
		case b == '\'':
			return "", errors.New("typed SQL: a fragment holds a string literal; values are bound, never written into the text")
		case typedOpensComment(text, i):
			return "", errors.New("typed SQL: a fragment holds a comment, which the placeholder scan cannot see through")
		}
		out.WriteByte(b)
	}
	if inIdentifier {
		return "", fmt.Errorf("typed SQL: unterminated quoted identifier")
	}
	if markers != arguments {
		return "", fmt.Errorf("typed SQL: %d placeholders for %d arguments", markers, arguments)
	}
	return out.String(), nil
}

// typedOpensComment reports whether a line or block comment starts at text[i].
func typedOpensComment(text string, i int) bool {
	if i+1 >= len(text) {
		return false
	}
	pair := text[i : i+2]
	return pair == "--" || pair == "/*"
}

// countTypedMarkers counts the markers of text that stand outside a quoted
// identifier, the same way numberTypedPlaceholders does.
func countTypedMarkers(text string, quote byte) int {
	inIdentifier := false
	markers := 0
	for i := 0; i < len(text); i++ {
		switch b := text[i]; {
		case b == quote:
			inIdentifier = !inIdentifier
		case b == typedMarker && !inIdentifier:
			markers++
		}
	}
	return markers
}

// checkTypedFragment enforces operand rule 1 of the typedDialect contract on one
// fragment a dialect built around already rendered operands. own is the number
// of markers the dialect itself introduces (bind and limitOffset write one per
// argument they return; every other fragment writes none).
//
// The fragment must hold exactly the markers its operands and own bring, so no
// operand that carries a value is dropped or repeated, and every operand that
// carries a value must appear verbatim, in the order it was passed. The compiler
// appends the operands' arguments in that order and the numbering pass pairs
// the nth marker of the text with the nth argument, so a fragment that swapped
// two operands would keep the count right and bind values to the wrong places.
// An operand without a marker carries no argument, so it may be repeated, moved
// or rewritten freely.
func checkTypedFragment(fragment string, quote byte, own int, operands ...string) error {
	want := own
	for _, operand := range operands {
		want += countTypedMarkers(operand, quote)
	}
	if got := countTypedMarkers(fragment, quote); got != want {
		return fmt.Errorf("typed SQL: the dialect wrote %d placeholders where %d belong", got, want)
	}
	rest := fragment
	for _, operand := range operands {
		if countTypedMarkers(operand, quote) == 0 {
			continue
		}
		at := strings.Index(rest, operand)
		if at < 0 {
			return errors.New("typed SQL: the dialect did not write an operand carrying a value verbatim and in argument order")
		}
		rest = rest[at+len(operand):]
	}
	return nil
}
