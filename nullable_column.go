package dalgo2sql

import (
	"fmt"
	"reflect"

	"github.com/dal-go/dalgo/recordset"
)

// nullableColumn is the recordset column the recordset readers build for a result column.
// It holds its values as the Go type T and marks each cell that is NULL, so a NULL is read
// back as nil from GetValue, Values and the accessors of a row, and never as the zero value
// of T, which is a value a cell can hold. The column answers ValueType as the zero value of
// T, as the typed columns of DALgo do, so a consumer that aligns numbers by it still does.
//
// DALgo's typed columns cannot hold a NULL: their SetValue asserts the value to T, which a
// nil is not, and a bitmap column of booleans has no third state.
type nullableColumn[T any] struct {
	name   string
	dbType string
	zero   T
	values []T
	// null[row] is set when the cell of that row is NULL; values[row] then holds the zero
	// value.
	null []bool
}

var _ recordset.Column[any] = (*nullableColumn[string])(nil)

// newNullableColumn returns the column called name, of the database type dbType, that holds
// values of the Go type of zero.
func newNullableColumn[T any](name string, zero T, dbType string) *nullableColumn[T] {
	return &nullableColumn[T]{name: name, dbType: dbType, zero: zero}
}

func (c *nullableColumn[T]) Name() string { return c.name }

func (c *nullableColumn[T]) DbType() string { return c.dbType }

// DefaultValue is the value of a cell that has not been set: the zero value of T. A row is
// created with it (recordset.ColumnarRecordset.NewRow) and its cells are set when it is read.
func (c *nullableColumn[T]) DefaultValue() any { return c.zero }

func (c *nullableColumn[T]) ValueType() reflect.Type { return reflect.TypeOf(c.zero) }

// IsBitmap is false: the values are held in a slice, one cell each.
func (c *nullableColumn[T]) IsBitmap() bool { return false }

// Add appends a cell: nil for a NULL, else a value of type T.
func (c *nullableColumn[T]) Add(value any) error {
	typed, null, err := c.cell(value)
	if err != nil {
		return err
	}
	c.values, c.null = append(c.values, typed), append(c.null, null)
	return nil
}

// cell returns what a cell holds for value: nil is a NULL, and any value must be of type T.
// The error names the types and never the value.
func (c *nullableColumn[T]) cell(value any) (typed T, null bool, err error) {
	if value == nil {
		return c.zero, true, nil
	}
	typed, ok := value.(T)
	if !ok {
		return c.zero, false, fmt.Errorf("a %T value does not fit column %s of type %s", value, c.name, c.ValueType())
	}
	return typed, false, nil
}

// GetValue returns the cell of row: nil for a NULL, else a value of type T.
func (c *nullableColumn[T]) GetValue(row int) (any, error) {
	if row < 0 || row >= len(c.values) {
		return nil, fmt.Errorf("row %d out of range for %d rows", row, len(c.values))
	}
	if c.null[row] {
		return nil, nil
	}
	return c.values[row], nil
}

// SetValue sets the cell of row: nil makes it NULL, and a value of type T is the cell's value.
func (c *nullableColumn[T]) SetValue(row int, value any) error {
	if row < 0 || row >= len(c.values) {
		return fmt.Errorf("row %d out of range for %d rows", row, len(c.values))
	}
	typed, null, err := c.cell(value)
	if err != nil {
		return err
	}
	c.values[row], c.null[row] = typed, null
	return nil
}

// Values returns a copy of the cells, nil for each NULL.
func (c *nullableColumn[T]) Values() []any {
	values := make([]any, len(c.values))
	for i, value := range c.values {
		if !c.null[i] {
			values[i] = value
		}
	}
	return values
}
