package dalgo2sql

import "github.com/dal-go/dalgo/dal"

// DbOptions provides database sqlOptions for DALgo - // TODO: document why & how to use
type DbOptions struct {
	ID         string
	PrimaryKey []string
	Recordsets map[string]*Recordset
	// Placeholder controls how SQL parameter markers are emitted.
	// The zero value (PlaceholderQuestion) uses "?" — compatible with
	// SQLite, MySQL, and most other drivers.  Set to PlaceholderDollar
	// for PostgreSQL, which requires "$1", "$2", … positional markers.
	Placeholder PlaceholderDialect
}

func (o DbOptions) GetRecordsetByKey(key *dal.Key) *Recordset {
	rsName := getRecordsetName(key)
	return o.Recordsets[rsName]
}

func (o DbOptions) PrimaryKeyFieldNames(key *dal.Key) (primaryKey []string) {
	rs := o.GetRecordsetByKey(key)
	if pk := rs.PrimaryKey(); len(pk) > 0 {
		primaryKey = make([]string, len(pk))
		for i, f := range pk {
			primaryKey[i] = f.Name()
		}
		return
	}
	return nil
}
