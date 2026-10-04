package dalgo2sql

import (
	"github.com/dal-go/record"
	"strings"
)

// getRecordsetName returns the name of the recordset a key addresses: the
// collections of the key and of its parents, the key's own first, joined with
// "_". For a key without a parent it is the key's collection.
//
// It is the one table rule of every statement a key read or write builds, which
// reaches it through DbOptions.recordsetIdentifier (the table) and
// DbOptions.PrimaryKeyFieldNames (the primary key): Exists, Get, GetMulti,
// Insert, Set, SetMulti, Update, UpdateMulti, Delete and DeleteMulti. A builder
// does not write key.Collection() as the table, as that is the table of a
// different recordset for a nested key.
func getRecordsetName(key *record.Key) string {
	path := make([]string, 0, key.Level()+1)
	for key != nil {
		path = append(path, key.Collection())
		key = key.Parent()
	}
	return strings.Join(path, "_")
}
