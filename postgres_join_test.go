package dalgo2sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

func TestDatabaseQueryCapabilitiesByDialect(t *testing.T) {
	native := dal.QueryCapabilities{
		GroupBy: true, Having: true, OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count: true, CountDistinct: true,
			Sum: true, SumDistinct: true,
			Avg: true, AvgDistinct: true,
			Min: true, Max: true,
		},
	}
	for _, tc := range []struct {
		dialect string
		want    dal.QueryCapabilities
	}{
		{"", dal.QueryCapabilities{}},
		{"sqlite", native},
		{"postgres", native},
		{"mysql", dal.QueryCapabilities{}},
	} {
		t.Run("dialect "+tc.dialect, func(t *testing.T) {
			got := (&database{options: DbOptions{StructuredQueryDialect: tc.dialect}}).QueryCapabilities()
			if got != tc.want {
				t.Fatalf("QueryCapabilities() = %+v, want %+v", got, tc.want)
			}
			// FIRST and LAST need an input order no SQL engine guarantees, and neither
			// dialect promises a group-key order or a stable row order.
			if got.GroupKeyOrder || got.StableRowOrder || got.Aggregate.First || got.Aggregate.Last || got.Aggregate.OrderBy {
				t.Fatalf("QueryCapabilities() = %+v claims what the engines do not promise", got)
			}
		})
	}
	t.Run("the aggregation planner runs a grouped query natively with the PostgreSQL dialect", func(t *testing.T) {
		q := typedTestFrom("Invoice", "").NewQuery().GroupBy(typedTestField("Country")).
			SelectColumns(typedTestColumn(typedTestField("Country"), ""), dal.SumAs(typedTestField("Total"), "revenue"))
		plan, err := dal.PlanAggregation(q, (&database{options: DbOptions{StructuredQueryDialect: "postgres"}}).QueryCapabilities())
		if err != nil || plan.Strategy != dal.AggregationNative {
			t.Fatalf("PlanAggregation() = %+v, %v; want native", plan, err)
		}
		plan, err = dal.PlanAggregation(q, (&database{}).QueryCapabilities())
		if err != nil || plan.Strategy == dal.AggregationNative {
			t.Fatalf("PlanAggregation() with no dialect = %+v, %v; want a generic strategy", plan, err)
		}
	})
}

// No protected-write factory exists for PostgreSQL: the SQLite one (a read-only
// profile with checked writes) is built on SQLite's file semantics, so NewDatabase
// hands back the plain adapter for the PostgreSQL dialect, and for every dialect but
// SQLite.
func TestNewDatabaseOffersNoProtectedWritesForPostgreSQL(t *testing.T) {
	raw, _ := newPostgresReadMock(t)
	for _, dialect := range []string{"postgres", "", "mysql"} {
		got := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: dialect})
		if _, protected := got.(*sqliteProtectedFactory); protected {
			t.Fatalf("NewDatabase(%q) returned the SQLite protected-write factory", dialect)
		}
	}
	if _, protected := NewDatabase(raw, dal.NewSchema(nil, nil), DbOptions{StructuredQueryDialect: "sqlite"}).(*sqliteProtectedFactory); !protected {
		t.Fatal("NewDatabase(sqlite) no longer returns the protected factory: the control of this test is broken")
	}
}

// expectPostgresJoinCatalog answers the catalog query of a join test: Album(AlbumId integer,
// Title text, Doc json, Ref oid), Artist(ArtistId integer, Name text, Doc json,
// Ref oid), whichever of them the query asks for.
func expectPostgresJoinCatalog(mock sqlmock.Sqlmock, asked ...string) {
	rows := sqlmock.NewRows(postgresCatalogColumns)
	columns := map[string][][]driver.Value{
		`"Album"`: {
			{"AlbumId", "integer", "N", int64(23), int64(0), true, false},
			{"ArtistId", "integer", "N", int64(23), int64(0), true, false},
			{"Title", "text", "S", int64(25), int64(0), false, false},
			{"Doc", "json", "U", int64(114), int64(0), false, false},
			{"Ref", "oid", "N", int64(26), int64(0), false, false},
		},
		`"Artist"`: {
			{"ArtistId", "integer", "N", int64(23), int64(0), true, false},
			{"Name", "text", "S", int64(25), int64(0), false, false},
			{"Doc", "json", "U", int64(114), int64(0), false, false},
			{"Ref", "oid", "N", int64(26), int64(0), false, false},
		},
	}
	args := make([]driver.Value, len(asked))
	for i, relation := range asked {
		args[i] = relation
		for _, column := range columns[relation] {
			rows.AddRow(append([]driver.Value{relation}, column...)...)
		}
	}
	mock.ExpectQuery(postgresCatalogQuery(len(asked))).WithArgs(args...).WillReturnRows(rows)
}

func postgresJoinQuery(albumField, artistField string, columns ...dal.Column) dal.StructuredQuery {
	if len(columns) == 0 {
		columns = []dal.Column{typedTestColumn(typedTestQualified("r", "Name"), "")}
	}
	return typedTestFrom("Album", "a").Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("a", albumField, "r", artistField)),
	).NewQuery().SelectColumns(columns...)
}

// postgresJoinCheck runs CanExecuteJoin with the PostgreSQL dialect, on the database
// handle or in a transaction, against a mock that answers what answer expects, and
// returns what it said. It fails the test when an expected statement did not run.
func postgresJoinCheck(t *testing.T, inTransaction bool, options DbOptions, q dal.StructuredQuery, answer func(sqlmock.Sqlmock)) error {
	t.Helper()
	ctx := context.Background()
	db, mock := newPostgresReadMock(t)
	var check func() error
	if inTransaction {
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		check = func() error { return newTransaction(tx, options, dal.NewTransactionOptions()).CanExecuteJoin(ctx, q) }
	} else {
		check = func() error { return (&database{db: db, options: options}).CanExecuteJoin(ctx, q) }
	}
	if answer != nil {
		answer(mock)
	}
	err := check()
	if metErr := mock.ExpectationsWereMet(); metErr != nil {
		t.Fatal(metErr)
	}
	return err
}

func TestPostgresCanExecuteJoin(t *testing.T) {
	options := DbOptions{StructuredQueryDialect: "postgres"}
	both := func(asked ...string) func(sqlmock.Sqlmock) {
		return func(mock sqlmock.Sqlmock) { expectPostgresJoinCatalog(mock, asked...) }
	}
	for _, inTransaction := range []bool{false, true} {
		name := "on the database handle"
		if inTransaction {
			name = "in a transaction"
		}
		check := func(t *testing.T, q dal.StructuredQuery, answer func(sqlmock.Sqlmock)) error {
			t.Helper()
			return postgresJoinCheck(t, inTransaction, options, q, answer)
		}
		t.Run(name, func(t *testing.T) {
			t.Run("two numbers are accepted", func(t *testing.T) {
				if err := check(t, postgresJoinQuery("ArtistId", "ArtistId"), both(`"Album"`, `"Artist"`)); err != nil {
					t.Fatalf("CanExecuteJoin() error = %v, want it accepted", err)
				}
			})
			t.Run("two texts are accepted", func(t *testing.T) {
				if err := check(t, postgresJoinQuery("Title", "Name"), both(`"Album"`, `"Artist"`)); err != nil {
					t.Fatalf("CanExecuteJoin() error = %v, want it accepted", err)
				}
			})
			for _, tc := range []struct {
				name, album, artist, fragment string
			}{
				{"a number against text", "ArtistId", "Name", "key types differ"},
				{"two json columns, which have no equality", "Doc", "Doc", "cannot be compared in a JOIN"},
				{"two oid columns", "Ref", "Ref", "cannot be compared in a JOIN"},
				{"a key that is not a column of its table", "Missing", "ArtistId", `field "Missing" is not a column`},
			} {
				t.Run(tc.name+" is declined", func(t *testing.T) {
					err := check(t, postgresJoinQuery(tc.album, tc.artist), both(`"Album"`, `"Artist"`))
					if err == nil || !strings.Contains(err.Error(), "join_plan: PostgreSQL cannot natively compile query") || !strings.Contains(err.Error(), tc.fragment) {
						t.Fatalf("CanExecuteJoin() error = %v, want a decline that mentions %q", err, tc.fragment)
					}
				})
			}
			t.Run("a decline for a type is DALgo's generic-engine signal", func(t *testing.T) {
				err := check(t, postgresJoinQuery("ArtistId", "Name"), both(`"Album"`, `"Artist"`))
				if !errors.Is(err, dal.ErrNotSupported) {
					t.Fatalf("CanExecuteJoin() error = %v, want ErrNotSupported in the chain", err)
				}
			})
			t.Run("a query with a wildcard is declined: the generic engine expands it from JoinFields", func(t *testing.T) {
				err := check(t, postgresJoinQuery("ArtistId", "ArtistId", dal.AllColumnsExcept("Doc")), both(`"Album"`, `"Artist"`))
				if !errors.Is(err, dal.ErrNotSupported) {
					t.Fatalf("CanExecuteJoin() error = %v, want a decline", err)
				}
			})
			t.Run("a table that is not there is the read's own error, with its hint", func(t *testing.T) {
				q := typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artst", "r"), dal.JoinInner, typedTestJoinOn("a", "ArtistId", "r", "ArtistId"))).
					NewQuery().SelectColumns()
				err := check(t, q, func(mock sqlmock.Sqlmock) {
					expectPostgresJoinCatalog(mock, `"Album"`, `"Artst"`)
					mock.ExpectQuery(postgresSuggestionQuery).WithArgs("").WillReturnRows(newPostgresSuggestionRows([2]string{"public", "Artist"}))
				})
				var notFound *TableNotFoundError
				if !errors.As(err, &notFound) || notFound.Name != "Artst" || notFound.SuggestedName != "Artist" {
					t.Fatalf("CanExecuteJoin() error = %v, want table not found with a hint", err)
				}
			})
			t.Run("a query without a join needs no catalog lookup", func(t *testing.T) {
				if err := check(t, typedTestFrom("Album", "").NewQuery().SelectColumns(), nil); err != nil {
					t.Fatalf("CanExecuteJoin() error = %v", err)
				}
			})
			t.Run("a join tree DALgo rejects is rejected before any lookup", func(t *testing.T) {
				empty := typedTestFrom("Album", "a").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner)).NewQuery().SelectColumns()
				if err := check(t, empty, nil); err == nil || !strings.Contains(err.Error(), "join_shape") {
					t.Fatalf("CanExecuteJoin() error = %v, want join_shape", err)
				}
			})
			t.Run("no query and no source are refused", func(t *testing.T) {
				if err := check(t, nil, nil); err == nil || !strings.Contains(err.Error(), "from is required") {
					t.Fatalf("CanExecuteJoin(nil) error = %v", err)
				}
			})
			t.Run("an identifier case the package does not define is refused", func(t *testing.T) {
				err := postgresJoinCheck(t, inTransaction, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: "upper"}, postgresJoinQuery("ArtistId", "ArtistId"), nil)
				if err == nil || !strings.Contains(err.Error(), `"upper"`) {
					t.Fatalf("CanExecuteJoin() error = %v", err)
				}
			})
		})
	}
	t.Run("the hook of a native compiler keeps its precedence", func(t *testing.T) {
		hooked := DbOptions{StructuredQueryDialect: "postgres", NativeStructuredQueryCompiler: nativeCompilerFunc(func(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error) { return "", nil, nil }),
			NativeJoinEligibility: func(context.Context, dal.StructuredQuery) error { return errors.New("the hook decided") }}
		err := (&database{options: hooked}).CanExecuteJoin(context.Background(), postgresJoinQuery("ArtistId", "ArtistId"))
		if err == nil || err.Error() != "the hook decided" {
			t.Fatalf("CanExecuteJoin() error = %v, want the hook's", err)
		}
	})
}

// nativeCompilerFunc adapts a function to NativeStructuredQueryCompiler.
type nativeCompilerFunc func(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error)

func (f nativeCompilerFunc) CompileNativeStructuredQuery(q dal.StructuredQuery, fragments NativeJoinHintFragments) (string, []any, error) {
	return f(q, fragments)
}

func TestPostgresJoinFields(t *testing.T) {
	ctx := context.Background()
	options := DbOptions{StructuredQueryDialect: "postgres"}
	album := dal.NewRootCollectionRef("Album", "a")
	run := func(t *testing.T, inTransaction bool, options DbOptions, source dal.RecordsetSource, answer func(sqlmock.Sqlmock)) ([]string, error) {
		t.Helper()
		db, mock := newPostgresReadMock(t)
		var fields func() ([]string, error)
		if inTransaction {
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			fields = func() ([]string, error) {
				return newTransaction(tx, options, dal.NewTransactionOptions()).JoinFields(ctx, source)
			}
		} else {
			fields = func() ([]string, error) { return (&database{db: db, options: options}).JoinFields(ctx, source) }
		}
		if answer != nil {
			answer(mock)
		}
		got, err := fields()
		if metErr := mock.ExpectationsWereMet(); metErr != nil {
			t.Fatal(metErr)
		}
		return got, err
	}
	for _, inTransaction := range []bool{false, true} {
		name := "on the database handle"
		if inTransaction {
			name = "in a transaction"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("the columns in table order, in the catalog's spelling", func(t *testing.T) {
				got, err := run(t, inTransaction, options, album, func(mock sqlmock.Sqlmock) { expectPostgresJoinCatalog(mock, `"Album"`) })
				if want := []string{"AlbumId", "ArtistId", "Title", "Doc", "Ref"}; err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("JoinFields() = %v, %v; want %v", got, err, want)
				}
			})
			t.Run("a pointer source is read like a value", func(t *testing.T) {
				got, err := run(t, inTransaction, options, &album, func(mock sqlmock.Sqlmock) { expectPostgresJoinCatalog(mock, `"Album"`) })
				if err != nil || len(got) != 5 {
					t.Fatalf("JoinFields() = %v, %v", got, err)
				}
			})
			t.Run("a table that is not there is table-not-found", func(t *testing.T) {
				_, err := run(t, inTransaction, options, dal.NewRootCollectionRef("Nowhere", ""), func(mock sqlmock.Sqlmock) {
					mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"Nowhere"`).WillReturnRows(sqlmock.NewRows(postgresCatalogColumns))
					mock.ExpectQuery(postgresSuggestionQuery).WithArgs("").WillReturnRows(newPostgresSuggestionRows())
				})
				if !errors.Is(err, ErrTableNotFound) {
					t.Fatalf("JoinFields() error = %v, want table not found", err)
				}
			})
			t.Run("a source that is no table is declined", func(t *testing.T) {
				_, err := run(t, inTransaction, options, dal.NewQuerySource(typedTestFrom("Album", "").NewQuery().SelectColumns(), "s"), nil)
				if !errors.Is(err, dal.ErrNotSupported) {
					t.Fatalf("JoinFields() error = %v, want ErrNotSupported", err)
				}
			})
			// The generic engine abandons the whole join when JoinFields fails, and reads its
			// leaves with a select-all that names no column, so a column the dialect cannot
			// write by name must not fail the call.
			t.Run("a column the dialect cannot write under its own name is listed as the catalog has it", func(t *testing.T) {
				got, err := run(t, inTransaction, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: IdentifierCaseFoldLower}, album, func(mock sqlmock.Sqlmock) {
					mock.ExpectQuery(postgresCatalogQuery(1)).WithArgs(`"album"`).WillReturnRows(sqlmock.NewRows(postgresCatalogColumns).
						AddRow(`"album"`, "albumid", "integer", "N", int64(23), int64(0), true, false).
						AddRow(`"album"`, "Title", "text", "S", int64(25), int64(0), false, false))
				})
				if want := []string{"albumid", "Title"}; err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("JoinFields() = %v, %v; want %v", got, err, want)
				}
			})
			t.Run("an identifier case the package does not define is refused", func(t *testing.T) {
				_, err := run(t, inTransaction, DbOptions{StructuredQueryDialect: "postgres", IdentifierCase: "upper"}, album, nil)
				if err == nil || !strings.Contains(err.Error(), `"upper"`) {
					t.Fatalf("JoinFields() error = %v", err)
				}
			})
		})
	}
}

// A join CanExecuteJoin accepts is a join the records reader can run. The reader adds
// the base table's configured primary key to the select list, so each record is keyed,
// and in a join that column must name its source like every other: DALgo has already
// chosen the native plan when the read starts and does not retry.
func TestPostgresAcceptedJoinIsReadAndKeyed(t *testing.T) {
	ctx := context.Background()
	joinStatement := `SELECT "r"."Name", "a"."AlbumId" AS "__dalgo_record_id" FROM "Album" AS "a" INNER JOIN "Artist" AS "r" ON ("a"."ArtistId" = "r"."ArtistId")`
	for _, tc := range []struct {
		name    string
		options DbOptions
	}{
		{"a primary key of the table's recordset", DbOptions{StructuredQueryDialect: "postgres", Recordsets: map[string]*Recordset{"Album": NewRecordset("Album", Table, []dal.FieldRef{dal.Field("AlbumId")})}}},
		{"a primary key of the whole database", DbOptions{StructuredQueryDialect: "postgres", PrimaryKey: []string{"AlbumId"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresReadMock(t)
			q := postgresJoinQuery("ArtistId", "ArtistId")
			expectPostgresJoinCatalog(mock, `"Album"`, `"Artist"`)
			if err := (&database{db: db, options: tc.options}).CanExecuteJoin(ctx, q); err != nil {
				t.Fatalf("CanExecuteJoin() error = %v, want it accepted", err)
			}
			expectPostgresJoinCatalog(mock, `"Album"`, `"Artist"`)
			mock.ExpectQuery(joinStatement).WillReturnRows(sqlmock.NewRows([]string{"Name", "__dalgo_record_id"}).AddRow("Queen", int64(7)))
			reader, err := getRecordsReaderWithOptions(ctx, q, db.QueryContext, tc.options)
			if err != nil {
				t.Fatalf("an accepted join cannot be read: %v", err)
			}
			defer func() { _ = reader.Close() }()
			record, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if got := record.Key().ID; got != int64(7) {
				t.Fatalf("record key = %v, want the AlbumId 7", got)
			}
			if got, want := record.Data(), map[string]any{"Name": "Queen"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("record data = %#v, want %#v (the key column is not part of the data)", got, want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A native compiler of the caller's, even with the PostgreSQL dialect named, is handed
// the query as it always was: the key column unqualified. Only the compilers of this
// package need it qualified, the typed PostgreSQL one and the SQLite one, which refuse
// an unqualified field in a join.
func TestRecordIdentityFieldIsQualifiedOnlyForTheCompilersOfThisPackageInAJoin(t *testing.T) {
	join := postgresJoinQuery("ArtistId", "ArtistId")
	single := typedTestFrom("Album", "").NewQuery().SelectColumns(typedTestColumn(typedTestField("Title"), ""))
	native := nativeCompilerFunc(func(dal.StructuredQuery, NativeJoinHintFragments) (string, []any, error) { return "", nil, nil })
	for _, tc := range []struct {
		name    string
		options DbOptions
		q       dal.StructuredQuery
		source  string
	}{
		{"the typed compiler, a join: the base's alias", DbOptions{StructuredQueryDialect: "postgres"}, join, "a"},
		{"the typed compiler, a join of a source with no alias: its name", DbOptions{StructuredQueryDialect: "postgres"},
			typedTestFrom("Album", "").Join(dal.NewJoinedSource(dal.NewRootCollectionRef("Artist", "r"), dal.JoinInner, typedTestJoinOn("Album", "ArtistId", "r", "ArtistId"))).NewQuery().SelectColumns(), "Album"},
		{"the typed compiler, no join", DbOptions{StructuredQueryDialect: "postgres"}, single, ""},
		{"a native compiler, a join", DbOptions{StructuredQueryDialect: "postgres", NativeStructuredQueryCompiler: native}, join, ""},
		{"the SQLite compiler, a join: the base's alias", DbOptions{StructuredQueryDialect: "sqlite"}, join, "a"},
		{"a dialect the package does not compile, a join", DbOptions{StructuredQueryDialect: "mysql"}, join, ""},
		{"no dialect, a join", DbOptions{}, join, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := recordIdentityField(tc.options, tc.q, "AlbumId")
			if field.Source() != tc.source || field.Name() != "AlbumId" {
				t.Fatalf("recordIdentityField() = %q.%q, want %q.AlbumId", field.Source(), field.Name(), tc.source)
			}
		})
	}
}
