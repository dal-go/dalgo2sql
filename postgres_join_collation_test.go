package dalgo2sql

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
)

// The server refuses to compare two columns whose collations are different and both not the
// database's default (SQLSTATE 42P22, indeterminate collation), although both are text. Such a
// join is not planned natively: the compiler declines it, as it does every join it cannot
// write, and DALgo's own engine runs it.

const (
	collationOfCodes = int64(12345)
	collationOfNames = int64(67890)
)

// expectCollationCatalog answers the catalog query for Album(ArtistId, Code, Tag, Plain) and
// Artist(ArtistId, Code, Tag, Plain): Album.Code has collation 12345 and Artist.Code 67890, both
// Tag columns have 12345, and the Plain columns have the default.
func expectCollationCatalog(mock sqlmock.Sqlmock, asked ...string) {
	rows := sqlmock.NewRows(postgresCatalogColumns)
	text := func(collation int64) []driver.Value {
		return []driver.Value{"text", "S", int64(25), int64(0), false, false, false, collation}
	}
	columns := map[string][][]driver.Value{
		`"Album"`: {
			append([]driver.Value{"ArtistId", "integer", "N", int64(23), int64(0), true, false, false}, int64(0)),
			append([]driver.Value{"Code"}, text(collationOfCodes)...),
			append([]driver.Value{"Tag"}, text(collationOfCodes)...),
			append([]driver.Value{"Plain"}, text(0)...),
		},
		`"Artist"`: {
			append([]driver.Value{"ArtistId", "integer", "N", int64(23), int64(0), true, false, false}, int64(0)),
			append([]driver.Value{"Code"}, text(collationOfNames)...),
			append([]driver.Value{"Tag"}, text(collationOfCodes)...),
			append([]driver.Value{"Plain"}, text(0)...),
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

func TestPostgresCanExecuteJoinDeclinesTwoKeysWithDifferentCollationsOfTheirOwn(t *testing.T) {
	options := DbOptions{StructuredQueryDialect: "postgres"}
	answer := func(mock sqlmock.Sqlmock) { expectCollationCatalog(mock, `"Album"`, `"Artist"`) }
	for _, inTransaction := range []bool{false, true} {
		for _, tc := range []struct {
			name, album, artist string
			declined            bool
		}{
			{"different collations of their own", "Code", "Code", true},
			{"the same collation of its own", "Tag", "Tag", false},
			{"a collation of its own against the default", "Code", "Plain", false},
			{"the default against a collation of its own", "Plain", "Code", false},
			{"two defaults", "Plain", "Plain", false},
			{"two numbers", "ArtistId", "ArtistId", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := postgresJoinCheck(t, inTransaction, options, postgresJoinQuery(tc.album, tc.artist, typedTestColumn(typedTestQualified("r", "Plain"), "")), answer)
				if !tc.declined {
					if err != nil {
						t.Fatalf("CanExecuteJoin() error = %v, want it accepted", err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), "join_plan: PostgreSQL cannot natively compile query") || !strings.Contains(err.Error(), "different collations") {
					t.Fatalf("CanExecuteJoin() error = %v, want a decline that names the collations", err)
				}
				if !errors.Is(err, dal.ErrNotSupported) {
					t.Fatalf("CanExecuteJoin() error = %v, want ErrNotSupported in the chain: the signal for DALgo's generic engine", err)
				}
			})
		}
	}
}

// The compiler is asked the same on its own, from facts.
func TestCompileTypedSQLDeclinesTwoKeysWithDifferentCollationsOfTheirOwn(t *testing.T) {
	facts := func(albumCollation, artistCollation int64) typedCatalogFacts {
		column := func(collation int64) typedSourceFacts {
			return typedSourceFacts{Columns: []typedColumnFact{{Name: "Code", DataType: "text", Category: typedTypeText, Collation: collation}, {Name: "Name", DataType: "text", Category: typedTypeText}}}
		}
		return typedCatalogFacts{Sources: map[typedSourceName]typedSourceFacts{
			{Name: "Album"}:  column(albumCollation),
			{Name: "Artist"}: column(artistCollation),
		}}
	}
	q := postgresJoinQuery("Code", "Code", typedTestColumn(typedTestQualified("r", "Name"), ""))
	dialect := newPostgresDialect(postgresExact)
	if _, _, err := compileTypedSQL(q, dialect, facts(collationOfCodes, collationOfNames)); !errors.Is(err, dal.ErrNotSupported) || !strings.Contains(err.Error(), "different collations") {
		t.Errorf("error = %v, want a refusal that names the collations", err)
	}
	for _, same := range [][2]int64{{collationOfCodes, collationOfCodes}, {0, collationOfNames}, {collationOfCodes, 0}, {0, 0}} {
		if _, _, err := compileTypedSQL(q, dialect, facts(same[0], same[1])); err != nil {
			t.Errorf("collations %v: error = %v, want it compiled", same, err)
		}
	}
}
