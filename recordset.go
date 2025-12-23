package dalgo2sql

import "github.com/dal-go/dalgo/dal"

// Field defines field
type Field struct {
	Name string
}

func (v Field) String() string {
	return v.Name
}

// RecordsetType defines type of a database recordset
type RecordsetType = int

const (
	// Table identifies a table in a database
	Table RecordsetType = iota
	// View identifies a view in a database
	//View
	// StoredProcedure identifies a stored procedure in a database
	//StoredProcedure
)

// Recordset hold recordset settings
type Recordset struct {
	name       string
	t          RecordsetType
	primaryKey []dal.FieldRef // Primary keys by table name
}

func (v *Recordset) Name() string {
	return v.name
}

func (v *Recordset) PrimaryKey() []dal.FieldRef {
	if v == nil {
		return nil
	}
	pk := make([]dal.FieldRef, len(v.primaryKey))
	copy(pk, v.primaryKey)
	return pk
}

func (v *Recordset) PrimaryKeyFieldNames() []string {
	pk := make([]string, len(v.primaryKey))
	for i, f := range v.primaryKey {
		pk[i] = f.Name()
	}
	return pk
}

func NewRecordset(name string, t RecordsetType, primaryKey []dal.FieldRef) *Recordset {
	return &Recordset{
		name:       name,
		t:          t,
		primaryKey: primaryKey,
	}
}

func (v *Recordset) Type() RecordsetType {
	return v.t
}
