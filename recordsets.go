package dalgo2sql

import (
	"github.com/dal-go/record"
	"strings"
)

func getRecordsetName(key *record.Key) string {
	path := make([]string, 0, key.Level()+1)
	for key != nil {
		path = append(path, key.Collection())
		key = key.Parent()
	}
	return strings.Join(path, "_")
}
