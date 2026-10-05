package dalgo2sql

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

// PostgreSQL keeps 63 bytes of an identifier and silently cuts the rest, so a key path
// that wrote a name of 64 to 255 bytes would address the table or column named by the
// first 63. Under the postgres dialect every key path refuses such a name, in every
// position, before any statement; the other dialects keep their limit of 255 bytes.

const pgNameBytes = postgresMaxIdentifierBytes

func pgNameOf(n int) string { return strings.Repeat("n", n) }

// runKeyPathOp runs op for the case under dialect, over a database handle and a
// transaction, and returns the statements and error of each surface.
func runKeyPathOp(t *testing.T, options DbOptions, c nameCase, run func(context.Context, keyPathAPI, nameCase) error) map[string]struct {
	calls []string
	err   error
} {
	t.Helper()
	out := map[string]struct {
		calls []string
		err   error
	}{}
	for _, r := range keyPathAPIs(t, options) {
		err := run(context.Background(), r.api, c)
		out[r.kind] = struct {
			calls []string
			err   error
		}{r.recorder.calls(), err}
	}
	return out
}

// requireSameStatementsAsOtherDialect: a name of 63 bytes is accepted by the postgres
// dialect and the statements are the ones the dialect-less path writes.
func requireAcceptedAsMainWrites(t *testing.T, c nameCase, optionsFor func(dialect string) DbOptions, run func(context.Context, keyPathAPI, nameCase) error, name string) {
	t.Helper()
	postgres := runKeyPathOp(t, optionsFor("postgres"), c, run)
	main := runKeyPathOp(t, optionsFor(""), c, run)
	for kind, got := range postgres {
		if len(got.calls) == 0 {
			t.Fatalf("%s: no statement was sent for a name of %d bytes (error %v)", kind, len(name), got.err)
		}
		if !slices.Equal(got.calls, main[kind].calls) {
			t.Fatalf("%s: statements = %q, want the ones main writes %q", kind, got.calls, main[kind].calls)
		}
		if errors.Is(got.err, ErrUnsafeName) {
			t.Fatalf("%s: the name of %d bytes was refused: %v", kind, len(name), got.err)
		}
		// The recorder fails every statement, so a write that checks for the row first never
		// sends the statement that carries a field name; its statements are still main's.
		if name != "" && !strings.Contains(strings.Join(got.calls, "\n"), name) {
			t.Fatalf("%s: no statement carries the name: %q", kind, got.calls)
		}
	}
}

func requireRefusedBeforeAnyStatement(t *testing.T, options DbOptions, c nameCase, run func(context.Context, keyPathAPI, nameCase) error, position string) {
	t.Helper()
	for _, r := range keyPathAPIs(t, options) {
		t.Run(r.kind, func(t *testing.T) {
			requireRefused(t, r.recorder, run(context.Background(), r.api, c), position)
		})
	}
}

func TestKeyPathNames_PostgresKeepsSixtyThreeBytesOfAName(t *testing.T) {
	for _, op := range keyPathOps {
		t.Run("collection/"+op.name, func(t *testing.T) {
			ok := validNames()
			ok.collection = pgNameOf(pgNameBytes)
			requireAcceptedAsMainWrites(t, ok, ok.options, op.run, ok.collection)

			long := validNames()
			long.collection = pgNameOf(pgNameBytes + 1)
			requireRefusedBeforeAnyStatement(t, long.options("postgres"), long, op.run, positionCollection)
		})
		t.Run("primary key/"+op.name, func(t *testing.T) {
			ok := validNames()
			ok.pk = pgNameOf(pgNameBytes)
			requireAcceptedAsMainWrites(t, ok, ok.options, op.run, ok.pk)

			long := validNames()
			long.pk = pgNameOf(pgNameBytes + 1)
			requireRefusedBeforeAnyStatement(t, long.options("postgres"), long, op.run, positionPrimaryKey)
		})
		if !op.touchesField {
			continue
		}
		t.Run("field/"+op.name, func(t *testing.T) {
			ok := validNames()
			ok.field = pgNameOf(pgNameBytes)
			requireAcceptedAsMainWrites(t, ok, ok.options, op.run, "")

			long := validNames()
			long.field = pgNameOf(pgNameBytes + 1)
			requireRefusedBeforeAnyStatement(t, long.options("postgres"), long, op.run, positionField)
		})
	}
}

// A parent key's collection is a name of the statement too: the table of a nested key is
// the joined name, and the parent's collection is one of its parts.
func TestKeyPathNames_PostgresRefusesALongCollectionOfAParentKey(t *testing.T) {
	for _, op := range keyPathOps {
		t.Run(op.name, func(t *testing.T) {
			c := validNames()
			c.collection = "child"
			c.parent = dalrecord.NewKeyWithID(pgNameOf(pgNameBytes+1), "p1")
			requireRefusedBeforeAnyStatement(t, pgNestedOptions(c, "postgres"), c, op.run, positionCollection)
		})
	}
}

// pgNestedOptions declares the recordset of the case's nested key under its joined name.
func pgNestedOptions(c nameCase, dialect string) DbOptions {
	options := c.options(dialect)
	joined := getRecordsetName(c.key("id1"))
	options.Recordsets[joined] = NewRecordset(joined, Table, []dal.FieldRef{dal.Field(c.pk)})
	return options
}

// Short collections can join into a name over 63 bytes: the joined name is the table, so
// it is refused although each collection of the path is short.
func TestKeyPathNames_PostgresRefusesAJoinedNameOverSixtyThreeBytes(t *testing.T) {
	nested := func(childBytes int) nameCase {
		c := validNames()
		c.collection = pgNameOf(childBytes)
		c.parent = dalrecord.NewKeyWithID(strings.Repeat("p", 31), "p1")
		return c
	}
	if joined := getRecordsetName(nested(31).key("id1")); len(joined) != pgNameBytes {
		t.Fatalf("the joined name of the accepted case is %d bytes, want %d", len(joined), pgNameBytes)
	}
	for _, op := range keyPathOps {
		t.Run(op.name, func(t *testing.T) {
			ok := nested(31)
			requireAcceptedAsMainWrites(t, ok, func(dialect string) DbOptions { return pgNestedOptions(ok, dialect) }, op.run, getRecordsetName(ok.key("id1")))

			long := nested(32)
			requireRefusedBeforeAnyStatement(t, pgNestedOptions(long, "postgres"), long, op.run, positionCollection)
		})
	}
}

// Delete needs no declared recordset: with none it deletes by the column ID, so the
// collection of a caller's key alone reaches the server.
func TestKeyPathNames_PostgresRefusesALongCollectionOfADeleteWithNoDeclaredRecordset(t *testing.T) {
	ops := map[string]func(context.Context, keyPathAPI, nameCase) error{
		"delete":       func(ctx context.Context, api keyPathAPI, c nameCase) error { return api.Delete(ctx, c.key("id1")) },
		"delete-multi": func(ctx context.Context, api keyPathAPI, c nameCase) error { return api.DeleteMulti(ctx, c.keys()) },
	}
	for name, run := range ops {
		t.Run(name, func(t *testing.T) {
			ok := nameCase{collection: pgNameOf(pgNameBytes)}
			undeclared := func(dialect string) DbOptions { return DbOptions{StructuredQueryDialect: dialect} }
			requireAcceptedAsMainWrites(t, ok, undeclared, run, ok.collection)
			for kind, got := range runKeyPathOp(t, undeclared("postgres"), ok, run) {
				if len(got.calls) != 1 || !strings.HasPrefix(got.calls[0], "DELETE FROM "+ok.collection+" WHERE ID ") {
					t.Fatalf("%s: statements = %q, want one DELETE of the column ID", kind, got.calls)
				}
			}

			long := nameCase{collection: pgNameOf(pgNameBytes + 1)}
			requireRefusedBeforeAnyStatement(t, undeclared("postgres"), long, run, positionCollection)
		})
	}
}

// Every other dialect keeps its limit: a name of 64 to 255 bytes is written as it was.
func TestKeyPathNames_OtherDialectsStillAcceptNamesOverSixtyThreeBytes(t *testing.T) {
	for _, dialect := range []string{"", "sqlite", "example-dialect-without-reviewed-quoting"} {
		for _, size := range []int{pgNameBytes + 1, maxNameBytes} {
			t.Run(dialect+"/"+pgNameOf(1)+string(rune('0'+size%10)), func(t *testing.T) {
				c := validNames()
				c.collection = pgNameOf(size)
				for _, op := range keyPathOps {
					for kind, got := range runKeyPathOp(t, c.options(dialect), c, op.run) {
						if len(got.calls) == 0 {
							t.Fatalf("%s/%s: no statement for a name of %d bytes (error %v)", op.name, kind, size, got.err)
						}
					}
				}
			})
		}
	}
}
