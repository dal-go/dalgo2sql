package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

// One rule for the names a SQLite statement writes: quotableNameProblem, the rule
// of the key reads and writes. The emitter and the protected path refuse a name
// that is empty, too long, invalid UTF-8 or holds a control character, and write
// every other name quoted.

// unquotableSQLiteNames are names quotableNameProblem refuses, one for each of its
// reasons.
func unquotableSQLiteNames() map[string]string {
	return map[string]string{
		"empty":         "",
		"NUL":           "nul\x00name",
		"newline":       "line\nbreak",
		"tab":           "tab\tname",
		"DEL":           "del\x7fname",
		"C1":            "c1\u0085name",
		"invalid UTF-8": "bad\xffutf8",
		"too long":      strings.Repeat("a", maxNameBytes+1),
	}
}

func TestQuotableNameProblem(t *testing.T) {
	for name, value := range unquotableSQLiteNames() {
		if quotableNameProblem(value) == "" {
			t.Errorf("%s: not refused", name)
		}
	}
	for _, value := range []string{"users", "Order Details", "a`b", "it's", `a"b`, "x; DROP TABLE y", "é", strings.Repeat("a", maxNameBytes)} {
		if problem := quotableNameProblem(value); problem != "" {
			t.Errorf("%q: refused: %s", value, problem)
		}
	}
}

func TestQuoteCheckedSQLIdentifier(t *testing.T) {
	if got, err := quoteCheckedSQLIdentifier(positionField, "a`b"); err != nil || got != "`a``b`" {
		t.Errorf("= %q, %v", got, err)
	}
	for name, value := range unquotableSQLiteNames() {
		got, err := quoteCheckedSQLIdentifier(positionField, value)
		if got != "" || !errors.Is(err, ErrUnsafeName) || !strings.Contains(err.Error(), positionField) {
			t.Errorf("%s: = %q, %v; want a refusal wrapping ErrUnsafeName that names the position", name, got, err)
		}
	}
}

// The structured-query emitter refuses such a name wherever it would write one.
func TestCompileStructuredSQL_RefusesNamesThatCannotBeQuoted(t *testing.T) {
	for kind, name := range unquotableSQLiteNames() {
		queries := map[string]func(name string) dal.StructuredQuery{
			"field in WHERE": func(name string) dal.StructuredQuery {
				return guardQuery(baseFrom(), func(b dal.IQueryBuilder) dal.IQueryBuilder { return b.Where(dal.Field(name).EqualTo(1)) })
			},
			"qualified field in a join": func(name string) dal.StructuredQuery {
				from := dal.From(dal.NewRootCollectionRef("customers", "c")).Join(
					dal.NewJoinedSource(dal.NewRootCollectionRef("orders", "o"), dal.JoinLeft, sqlJoin("c", "id", "o", "customer_id")),
				)
				return from.NewQuery().Where(dal.NewComparison(dal.NewFieldRef("c", name), dal.Equal, dal.NewConstant(1))).SelectColumns(dal.Column{Expression: dal.NewFieldRef("c", "id")})
			},
			"field in the select list": func(name string) dal.StructuredQuery {
				return dal.NewQueryBuilder(baseFrom()).SelectColumns(dal.Column{Expression: dal.Field(name)})
			},
			"column alias": func(name string) dal.StructuredQuery {
				return dal.NewQueryBuilder(baseFrom()).SelectColumns(dal.Column{Expression: dal.Field("a"), Alias: name})
			},
			"collection": func(name string) dal.StructuredQuery {
				return guardQuery(dal.From(dal.NewRootCollectionRef(name, "")), nil)
			},
			"schema": func(name string) dal.StructuredQuery {
				return guardQuery(dal.From(dal.NewQualifiedRootCollectionRef(name, "customers", "")), nil)
			},
		}
		for position, build := range queries {
			t.Run(position+"/"+kind, func(t *testing.T) {
				if position == "column alias" && name == "" {
					t.Skip("an empty alias is no alias")
				}
				if (position == "collection" || position == "schema") && name == "" {
					t.Skip("the record package builds no collection and no schema with an empty name")
				}
				text, _, err := compileStructuredSQL(build(name))
				if text != "" || !errors.Is(err, ErrUnsafeName) {
					t.Errorf("compileStructuredSQL = %q, %v; want no text and an error wrapping ErrUnsafeName", text, err)
				}
			})
		}
	}
}

// A grouped column with no alias is named by its expression, so a string constant
// in the expression is a name too.
func TestCompileStructuredSQL_RefusesAGroupOutputNameThatCannotBeQuoted(t *testing.T) {
	for _, constant := range []string{"x\ny", "x\ty"} {
		expression := dal.Binary(dal.Field("a"), dal.Add, dal.NewConstant(constant))
		q := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().
			GroupBy(expression).
			SelectColumns(dal.Column{Expression: expression}, dal.Column{Expression: dal.NewAggregate(dal.COUNT, false, dal.Field("a")), Alias: "n"})
		if text, _, err := compileStructuredSQL(q); text != "" || !errors.Is(err, ErrUnsafeName) {
			t.Errorf("%q: compileStructuredSQL = %q, %v; want an error wrapping ErrUnsafeName", constant, text, err)
		}
	}
}

// The join preparation of the SQLite path refuses them too, before it sends the
// PRAGMA or the preflight query.
func TestSQLiteJoinPreparation_RefusesNamesThatCannotBeQuoted(t *testing.T) {
	execute := func(context.Context, string, ...any) (*sql.Rows, error) {
		t.Fatal("a statement was sent")
		return nil, nil
	}
	for kind, name := range unquotableSQLiteNames() {
		if name == "" {
			continue // the record package builds no collection with an empty name
		}
		t.Run(kind, func(t *testing.T) {
			source := dal.NewRootCollectionRef(name, "")
			if _, err := sqliteTableColumns(context.Background(), source, execute); !errors.Is(err, ErrUnsafeName) {
				t.Errorf("table: error = %v, want one wrapping ErrUnsafeName", err)
			}
			if _, err := sqliteTableColumns(context.Background(), dal.NewQualifiedRootCollectionRef(name, "t", ""), execute); !errors.Is(err, ErrUnsafeName) {
				t.Errorf("schema: error = %v, want one wrapping ErrUnsafeName", err)
			}
			err := sqlitePreflightColumnValues(context.Background(), dal.NewRootCollectionRef("t", ""), name, sqliteJoinNumber, execute)
			if !errors.Is(err, ErrUnsafeName) {
				t.Errorf("field: error = %v, want one wrapping ErrUnsafeName", err)
			}
		})
	}
}

// A table or primary-key column the protected path would have written is refused
// when its name is one the rule refuses: such a name can exist in a database,
// quoted, and was written.
func TestSQLiteProtected_RefusesANameThatCannotBeQuoted(t *testing.T) {
	cases := map[string]struct{ table, pk string }{
		"table":       {"tab\tname", "id"},
		"primary key": {"plain", "id\tcol"},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "db.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = raw.Close() })
			for _, statement := range []string{
				"CREATE TABLE " + quoteSQLIdentifier(tt.table) + " (" + quoteSQLIdentifier(tt.pk) + " TEXT PRIMARY KEY, name TEXT)",
				"INSERT INTO " + quoteSQLIdentifier(tt.table) + " VALUES ('a', 'Before')",
			} {
				if _, err = raw.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			db := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite", Recordsets: map[string]*Recordset{
				tt.table: NewRecordset(tt.table, Table, []dal.FieldRef{dal.Field(tt.pk)}),
			}})
			secured, _, err := db.(*sqliteProtectedFactory).ConfigureProtectedAccess(access.MandatoryParticipant{LayerID: "owner", Provider: func(context.Context) (access.PolicyLease, error) {
				return sqliteTestLease{[]access.Policy{access.MustPolicy("owner", access.Scope(tt.table, access.AnyID, access.Allow(access.ReadWrite, "all")))}}, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			protected := secured.(protectedSQLDB)
			err = protected.Update(context.Background(), dalrecord.NewKeyWithID(tt.table, "a"), []update.Update{update.ByFieldName("name", "After")})
			if !errors.Is(err, access.ErrAccessDenied) {
				t.Fatalf("error = %v, want access denied", err)
			}
			var got string
			if err = raw.QueryRow("SELECT name FROM " + quoteSQLIdentifier(tt.table)).Scan(&got); err != nil || got != "Before" {
				t.Errorf("name = %q, %v; want the row unchanged", got, err)
			}
		})
	}
}

// select-all-keys under the sqlite dialect: a name that cannot be quoted is
// refused before any statement reaches the database. A structured read of that
// dialect reports every refusal as an access denial that names nothing.
func TestKeyPathNames_SelectAllKeysUnderSQLiteNeverReachesTheExecutor(t *testing.T) {
	for kind, name := range unquotableSQLiteNames() {
		queries := map[string]func() dal.Query{
			"collection": func() dal.Query {
				return dal.From(dal.NewRootCollectionRef(name, "")).NewQuery().SelectKeysOnly(reflect.String)
			},
			"field": func() dal.Query {
				return dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField(name, dal.Equal, "x").SelectKeysOnly(reflect.String)
			},
		}
		for position, build := range queries {
			t.Run(position+"/"+kind, func(t *testing.T) {
				if position == "collection" && name == "" {
					t.Skip("the record package builds no collection with an empty name")
				}
				for _, r := range keyPathAPIs(t, validNames().options(dialectSQLite)) {
					t.Run(r.kind, func(t *testing.T) {
						_, err := r.api.ExecuteQueryToRecordsReader(context.Background(), build())
						if calls := r.recorder.calls(); len(calls) != 0 {
							t.Fatalf("a statement reached the database: %q", calls)
						}
						if !errors.Is(err, access.ErrAccessDenied) {
							t.Fatalf("error = %v, want an access denial", err)
						}
					})
				}
			})
		}
	}
}
