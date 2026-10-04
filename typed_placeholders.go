package dalgo2sql

import (
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
		case b == typedMarker && !inIdentifier:
			markers++
			if style.Prefix != "" {
				out.WriteString(style.Prefix)
				out.WriteString(strconv.Itoa(markers))
				continue
			}
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
