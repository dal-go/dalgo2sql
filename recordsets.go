package dalgo2sql

import (
	"strings"

	"github.com/dal-go/dalgo/dal"
)

func getRecordsetName(key *dal.Key) string {
	path := make([]string, 0, key.Level()+1)
	for key != nil {
		path = append(path, key.Collection())
		key = key.Parent()
	}
	return strings.Join(path, "_")
}
