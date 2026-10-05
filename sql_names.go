package dalgo2sql

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
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
//   - With DbOptions.StructuredQueryDialect "postgres" a name must also be no
//     longer than 63 bytes, which is all PostgreSQL keeps of an identifier: a
//     longer name would address the table or column named by its first 63 bytes.
//     The name of a nested key is the joined name, which is checked as one.
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

// ErrUndeclaredNestedRecordset is wrapped by the error every key read and write
// returns for a key with a parent when no recordset is declared under its joined
// name in DbOptions.Recordsets. The statement is not sent. Test for it with
// errors.Is.
//
// It is one rule for nested keys in every operation (Exists, Get, GetMulti,
// Insert, InsertMulti, Set, SetMulti, Update, UpdateMulti, Delete and
// DeleteMulti, on a database and on a transaction): a SQL recordset is a table,
// the table of a nested key is named by the collections of the key and of its
// parents joined with "_", the key's own collection first, and it is the
// declaration of that recordset that says the table exists and has the primary
// key the key's ID is looked up in. A key without a parent is not affected.
var ErrUndeclaredNestedRecordset = errors.New("nested key without a declared recordset")

// The positions a name can have in a key read or write; an ErrUnsafeName error
// names the one that was refused.
const (
	positionCollection = "collection"
	positionField      = "field"
	positionPrimaryKey = "primary key"
	// The positions of the names a structured SQLite query writes.
	positionAlias  = "alias"
	positionSchema = "schema"
)

const (
	// dialectSQLite is the StructuredQueryDialect whose identifier quoting
	// (quoteSQLIdentifier) has been reviewed.
	dialectSQLite = "sqlite"
	// dialectPostgres is the StructuredQueryDialect of PostgreSQL. Its key paths write a
	// plain identifier unquoted, so a name over postgresMaxIdentifierBytes is refused.
	dialectPostgres = "postgres"
	// maxNameBytes bounds every name, quoted or plain, of every dialect. SQLite sets
	// no limit of its own and MySQL refuses a name over 64 bytes; the bound keeps
	// error messages and statements short. PostgreSQL keeps 63 bytes of an identifier
	// and silently cuts the rest, so under the "postgres" dialect the bound is
	// postgresMaxIdentifierBytes, the limit the typed compiler holds: a longer name
	// would address the table or column named by its first 63 bytes, and sqlIdentifier
	// refuses it.
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

// isReservedSQLWord reports whether name, in any case, is one of reservedSQLWords: a word
// that any of the engines reserves (see reserved_words.go). The legacy text emitter writes
// names unquoted, so it refuses a keys-only query ordered by one.
func isReservedSQLWord(name string) bool { return reservedSQLWords[strings.ToLower(name)] }

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
		if o.StructuredQueryDialect == dialectPostgres && len(name) > postgresMaxIdentifierBytes {
			return "", newUnsafeNameError(position, name, "is too long for PostgreSQL: it keeps 63 bytes")
		}
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

// errNilKey is the error of an operation that is given a nil key, which names no recordset.
// Every operation reaches the table through recordsetIdentifier before it builds a statement.
var errNilKey = fmt.Errorf("%w: the key is nil", dal.ErrNotSupported)

// recordsetIdentifier returns the recordset name getRecordsetName derives from
// key as it may be written into SQL text. It is the table of every statement a
// key read or write builds, nested key or not. Each collection of the key's path
// is validated before the names are joined, and the joined name is validated as
// the one identifier it becomes. A key with a parent is refused unless a recordset
// is declared under the joined name (ErrUndeclaredNestedRecordset): every
// operation reaches the table through here, before any statement, so that is one
// rule for all of them.
func (o DbOptions) recordsetIdentifier(key *dalrecord.Key) (string, error) {
	if key == nil {
		return "", errNilKey
	}
	for segment := key; segment != nil; segment = segment.Parent() {
		if _, err := o.sqlIdentifier(positionCollection, segment.Collection()); err != nil {
			return "", err
		}
	}
	identifier, err := o.sqlIdentifier(positionCollection, getRecordsetName(key))
	if err != nil {
		return "", err
	}
	if name := getRecordsetName(key); key.Parent() != nil && o.Recordsets[name] == nil {
		return "", fmt.Errorf("%w: the key has a parent, so a recordset must be declared as %q in DbOptions.Recordsets",
			ErrUndeclaredNestedRecordset, name)
	}
	return identifier, nil
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
// may not be written into SQL text, and one whose data is not a struct with exported
// fields or a map with string keys (an error wrapping dal.ErrNotSupported). The
// statement builders refuse the same names on their own; this runs first so that a
// write whose first statement is not the one that carries the names (an existence
// check), and a batch of writes of which an earlier record would be written before
// a later one is refused, send nothing at all.
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
	// Data a write cannot turn into columns is refused here too, so that it is
	// before any statement: an ID generator checks for a taken ID, and an earlier
	// record of a batch is written, before the statement builder sees the data.
	fields, err := recordDataFields(getRecordsetName(key), record.Data())
	if err != nil {
		return err
	}
	for _, field := range fields {
		if _, err := o.sqlIdentifier(positionField, field.name); err != nil {
			return err
		}
	}
	return nil
}

// checkRecordColumns refuses a record that has no column to write, before any
// statement: for operation updateOperation (Set, which updates a row that is
// there) a field that is not a column of the primary key; for insertOperation a
// key ID or a field. buildSingleRecordQuery refuses the same records on its own.
func (o DbOptions) checkRecordColumns(record dalrecord.Record, op operation) error {
	key := record.Key()
	primaryKey := o.PrimaryKeyFieldNames(key)
	record.SetError(nil)
	fields := 0
	for _, name := range recordFieldNames(record.Data()) {
		if !slices.Contains(primaryKey, name) {
			fields++
		}
	}
	if fields > 0 || (op == insertOperation && key.ID != nil) {
		return nil
	}
	return fmt.Errorf("%w: the record of recordset %s has no field to write", ErrNoFieldsToWrite, getRecordsetName(key))
}
