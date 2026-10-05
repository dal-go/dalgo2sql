package dalgo2sql

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	dalrecord "github.com/dal-go/record"
)

// sentStatement is a statement with the arguments it was sent with.
type sentStatement struct {
	text string
	args []any
}

func recordingExecutor(sent *[]sentStatement) statementExecutor {
	return func(_ context.Context, text string, args ...any) (sql.Result, error) {
		*sent = append(*sent, sentStatement{text, args})
		return nil, nil
	}
}

// Every key of DeleteMulti is deleted by one statement: a recordset with several
// consecutive keys is deleted with one IN statement, not with a statement per key
// followed by the IN statement; a lone key has its own.
func TestDeleteMulti_SendsEachKeyOnce(t *testing.T) {
	key := func(collection, id string) *dalrecord.Key { return dalrecord.NewKeyWithID(collection, id) }
	cases := []struct {
		name string
		keys []*dalrecord.Key
		want []sentStatement
	}{
		{"no keys", nil, nil},
		{"one key", []*dalrecord.Key{key("users", "u1")}, []sentStatement{
			{"DELETE FROM users WHERE ID = ?", []any{"u1"}},
		}},
		{"two keys", []*dalrecord.Key{key("users", "u1"), key("users", "u2")}, []sentStatement{
			{"DELETE FROM users WHERE ID IN (?, ?)", []any{"u1", "u2"}},
		}},
		{"runs of keys of different recordsets", []*dalrecord.Key{
			key("users", "u1"), key("users", "u2"), key("users", "u3"), key("posts", "p1"), key("users", "u4"),
		}, []sentStatement{
			{"DELETE FROM users WHERE ID IN (?, ?, ?)", []any{"u1", "u2", "u3"}},
			{"DELETE FROM posts WHERE ID = ?", []any{"p1"}},
			{"DELETE FROM users WHERE ID = ?", []any{"u4"}},
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var sent []sentStatement
			if err := deleteMulti(context.Background(), DbOptions{}, tt.keys, recordingExecutor(&sent)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(sent, tt.want) {
				t.Errorf("statements = %q\n want %q", sent, tt.want)
			}
		})
	}
}
