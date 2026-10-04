package dalgo2sql

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"unicode"
	"unicode/utf8"

	dalrecord "github.com/dal-go/record"
)

// ErrUnsafeName is wrapped by the error a key read or write returns when a
// collection, field or primary-key name may not be written into SQL text. The
// statement is not sent. Test for it with errors.Is.
//
// Names are the one thing of a key read or write that is not a bound
// parameter, so every name is checked before it enters SQL text:
//
//   - With a dialect whose identifier quoting has been reviewed
//     (DbOptions.StructuredQueryDialect "sqlite"), a name must not be empty,
//     contain a control character (NUL included) or invalid UTF-8, or be longer
//     than 255 bytes. It is then written quoted with the quote character
//     escaped, so a name with a space, a quote or a comment marker is an
//     ordinary identifier.
//   - With no dialect, or one without reviewed quoting, a name must be a plain
//     identifier: a letter or underscore followed by letters, digits or
//     underscores, ASCII only, and no longer than 255 bytes. It is written as
//     given, so the statement text of every name accepted is the text this
//     package has always sent.
//
// A name used to be pasted into the statement as written, so one that carried
// its own quoting ("Order Details" with the quote characters, [Order Details],
// a name between backticks) acted as a quoted identifier. It no longer does:
// with no dialect such a name is refused, and with "sqlite" it is one literal
// name, quote characters included, that finds no table. A caller passes the
// bare name (Order Details) with the "sqlite" dialect. With no dialect there is
// no way to reach a table or column that needs quoting until that engine has
// reviewed quoting.
var ErrUnsafeName = errors.New("unsafe SQL name")

// The positions a name can have in a key read or write; an ErrUnsafeName error
// names the one that was refused.
const (
	positionCollection = "collection"
	positionField      = "field"
	positionPrimaryKey = "primary key"
)

const (
	// dialectSQLite is the StructuredQueryDialect whose identifier quoting
	// (quoteSQLIdentifier) has been reviewed.
	dialectSQLite = "sqlite"
	// maxNameBytes bounds every name, quoted or plain. SQLite sets no limit of
	// its own; MySQL refuses a name over 64 bytes, and PostgreSQL truncates one
	// over 63 bytes with a notice, so a longer name would address the table named
	// by its first 63 bytes. The bound keeps error messages and statements short;
	// a name of 64 to 255 bytes is still accepted, and PostgreSQL still truncates
	// it.
	maxNameBytes = 255
	// maxNameInError is how many characters of a refused name its error shows.
	maxNameInError = 32
)

// unsafeNameError is the error returned for a refused name. It holds no more
// of the name than maxNameInError characters.
type unsafeNameError struct {
	position  string
	name      string // at most maxNameInError characters of the refused name
	truncated bool
	reason    string
}

func newUnsafeNameError(position, name, reason string) *unsafeNameError {
	err := &unsafeNameError{position: position, name: name, reason: reason}
	characters := 0
	for i := range name {
		if characters == maxNameInError {
			err.name, err.truncated = name[:i], true
			break
		}
		characters++
	}
	return err
}

func (e *unsafeNameError) Error() string {
	truncated := ""
	if e.truncated {
		truncated = " (truncated)"
	}
	return fmt.Sprintf("%v: %s name %s%s %s", ErrUnsafeName, e.position, strconv.Quote(e.name), truncated, e.reason)
}

func (e *unsafeNameError) Unwrap() error { return ErrUnsafeName }

// reviewedIdentifierQuoting returns the identifier quoting of a dialect whose
// quoting has been reviewed, and nil for any other dialect, including none.
func reviewedIdentifierQuoting(dialect string) func(string) string {
	if dialect == dialectSQLite {
		return quoteSQLIdentifier
	}
	return nil
}

// quotableNameProblem says why name cannot be written quoted, or "" if it can.
func quotableNameProblem(name string) string {
	switch {
	case name == "":
		return "is empty"
	case len(name) > maxNameBytes:
		return "is too long"
	case !utf8.ValidString(name):
		return "is not valid UTF-8"
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "contains a control character"
		}
	}
	return ""
}

// sqlIdentifier returns name as it may be written into SQL text, or an error
// wrapping ErrUnsafeName that names position. See ErrUnsafeName for the rules.
// Every statement a key read or write builds gets each name through it.
func (o DbOptions) sqlIdentifier(position, name string) (string, error) {
	quote := reviewedIdentifierQuoting(o.StructuredQueryDialect)
	if quote == nil {
		if len(name) > maxNameBytes {
			return "", newUnsafeNameError(position, name, "is too long")
		}
		if !isPlainSQLIdentifier(name) {
			return "", newUnsafeNameError(position, name, "is not a plain identifier")
		}
		return name, nil
	}
	if problem := quotableNameProblem(name); problem != "" {
		return "", newUnsafeNameError(position, name, problem)
	}
	return quote(name), nil
}

// recordsetIdentifier returns the recordset name getRecordsetName derives from
// key as it may be written into SQL text. It is the table of every statement a
// key read or write builds, nested key or not. Each collection of the key's path
// is validated before the names are joined, and the joined name is validated as
// the one identifier it becomes.
func (o DbOptions) recordsetIdentifier(key *dalrecord.Key) (string, error) {
	for segment := key; segment != nil; segment = segment.Parent() {
		if _, err := o.sqlIdentifier(positionCollection, segment.Collection()); err != nil {
			return "", err
		}
	}
	return o.sqlIdentifier(positionCollection, getRecordsetName(key))
}

// recordFieldNames lists the field names a write of data names in SQL text:
// the fields of a struct, the keys of a string-keyed map. Any other data has
// none; the statement builder refuses it on its own.
func recordFieldNames(data any) (names []string) {
	val := reflect.ValueOf(data)
	if kind := val.Kind(); kind == reflect.Interface || kind == reflect.Pointer {
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			names = append(names, val.Type().Field(i).Name)
		}
	case reflect.Map:
		if val.Type().Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				names = append(names, key.String())
			}
		}
	}
	return names
}

// checkRecordNames refuses a record whose collection, primary-key or field names
// may not be written into SQL text. The statement builders refuse the same names
// on their own; this runs first so that a write whose first statement is not the
// one that carries the names (an existence check), and a batch of writes of which
// an earlier record would be written before a later one is refused, send nothing
// at all.
func (o DbOptions) checkRecordNames(record dalrecord.Record) error {
	key := record.Key()
	if _, err := o.recordsetIdentifier(key); err != nil {
		return err
	}
	for _, name := range o.PrimaryKeyFieldNames(key) {
		if _, err := o.sqlIdentifier(positionPrimaryKey, name); err != nil {
			return err
		}
	}
	// The statement builders read the data the same way: Data() panics while the
	// record's error is unset or set to a failure, and SetError(nil) clears both.
	record.SetError(nil)
	for _, name := range recordFieldNames(record.Data()) {
		if _, err := o.sqlIdentifier(positionField, name); err != nil {
			return err
		}
	}
	return nil
}
