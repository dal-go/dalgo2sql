package dalgo2sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

// keyedBySQLiteCatalog reports whether the key of the records of q is for the SQLite catalog
// to say: the SQLite dialect's own compiler runs the read, and nothing else names the key (no
// recordset declared for the source, and no DbOptions.PrimaryKey). A grouped or aggregated
// query has no source row to key, a read into a record keeps that record's own key, a query
// with no base source has no source to ask about, and a read with joins can return one base
// row many times, so its rows are keyed by ordinal.
func keyedBySQLiteCatalog(options DbOptions, q dal.StructuredQuery) bool {
	return options.NativeStructuredQueryCompiler == nil && options.StructuredQueryDialect == "sqlite" &&
		!dal.HasAggregation(q) && q.IntoRecord() == nil &&
		q.From() != nil && q.From().Base() != nil && len(q.From().Joins()) == 0 &&
		len(options.PrimaryKey) == 0 && recordsetForQuery(options, q) == nil
}

// sqliteCatalogPrimaryKey names the column that is the whole of the primary key of the base
// source of q, as the SQLite catalog stores it, when the values of that column are guaranteed
// to be one per row and never absent: it is "" (the rows are keyed by ordinal) for a source
// with no primary key, a composite one, a view, a source the catalog does not know, and a key
// column that can hold NULL or values the text of which can repeat.
//
// A primary key column cannot repeat a value, but SQLite lets it hold NULL unless it is
// declared NOT NULL or is the INTEGER PRIMARY KEY that aliases the rowid (the declaration
// INTEGER PRIMARY KEY DESC is not that alias: it makes an index, which the catalog shows as
// an index of origin "pk", and accepts NULL). A column with no declared type keeps the
// integer 1 and the text '1' as two values, and a BLOB and a TEXT value of the same bytes are
// two values that one text spells. Each would give two records one key, so such a column is
// not used. An empty text is the one value a NOT NULL text key can hold that is no key
// (dalgotest.HasNoKey), and it is unique too.
//
// The checks cost a statement for an INTEGER key, and for any other key one pass over the key
// column to find a BLOB in it.
func sqliteCatalogPrimaryKey(ctx context.Context, q dal.StructuredQuery, execute executeQueryFunc) (string, error) {
	source, err := sqliteCollectionSource(q.From().Base())
	if err != nil {
		return "", err
	}
	key, keyColumns, err := sqlitePrimaryKeyColumns(ctx, source, execute)
	if err != nil {
		return "", err
	}
	if keyColumns != 1 || !sqliteKeyColumnHasOneStorageClass(key.declared) {
		return "", nil
	}
	// The rowid alias holds integers only, and never NULL: nothing else to check.
	aliased, err := sqliteKeyIsRowidAlias(ctx, source, key.declared, execute)
	if err != nil || aliased {
		return key.name, err
	}
	if !key.notNull {
		return "", nil
	}
	blobs, err := sqliteKeyHoldsBlob(ctx, source, key.name, execute)
	if err != nil || blobs {
		return "", err
	}
	return key.name, nil
}

type sqliteKeyInfo struct {
	name, declared string
	notNull        bool
}

// sqlitePrimaryKeyColumns reads the columns of source from the catalog and returns the
// last column of its primary key and how many columns the key has.
func sqlitePrimaryKeyColumns(ctx context.Context, source dal.CollectionRef, execute executeQueryFunc) (key sqliteKeyInfo, keyColumns int, err error) {
	statement, args := `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?)`, []any{source.Name()}
	if schema := source.Schema(); schema != "" {
		statement, args = `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?, ?)`, []any{source.Name(), schema}
	}
	rows, err := execute(ctx, statement, args...)
	if err != nil {
		return key, 0, fmt.Errorf("inspect SQLite table %q: %w", source.Name(), err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, declared string
		var notNull, pk int
		if err := rows.Scan(&name, &declared, &notNull, &pk); err != nil {
			return key, 0, fmt.Errorf("scan SQLite table metadata: %w", err)
		}
		if pk > 0 {
			keyColumns++
			key = sqliteKeyInfo{name, declared, notNull != 0}
		}
	}
	if err := rows.Err(); err != nil {
		return key, 0, fmt.Errorf("inspect SQLite table metadata: %w", err)
	}
	return key, keyColumns, nil
}

// sqliteKeyColumnHasOneStorageClass reports whether a column declared with the type declared
// has an INTEGER or TEXT affinity, which stores a value of the column as an integer or as
// text, whichever the value is, so that two values never share one text. (A BLOB is checked
// for apart: no affinity converts it.)
func sqliteKeyColumnHasOneStorageClass(declared string) bool {
	upper := strings.ToUpper(strings.TrimSpace(declared))
	return strings.Contains(upper, "INT") || strings.Contains(upper, "CHAR") || strings.Contains(upper, "CLOB") || strings.Contains(upper, "TEXT")
}

// sqliteKeyIsRowidAlias reports whether the key column is the rowid alias, which holds one
// integer per row and no NULL: it is declared exactly INTEGER and no index of origin "pk"
// backs the key, which is how the catalog tells the alias from INTEGER PRIMARY KEY DESC and
// from a key of a WITHOUT ROWID table.
func sqliteKeyIsRowidAlias(ctx context.Context, source dal.CollectionRef, declared string, execute executeQueryFunc) (bool, error) {
	if strings.ToUpper(strings.TrimSpace(declared)) != "INTEGER" {
		return false, nil
	}
	statement, args := `SELECT COUNT(*) FROM pragma_index_list(?) WHERE origin = 'pk'`, []any{source.Name()}
	if schema := source.Schema(); schema != "" {
		statement, args = `SELECT COUNT(*) FROM pragma_index_list(?, ?) WHERE origin = 'pk'`, []any{source.Name(), schema}
	}
	var indexes int
	if err := queryOneInt(ctx, execute, statement, args, &indexes); err != nil {
		return false, fmt.Errorf("inspect SQLite indexes of table %q: %w", source.Name(), err)
	}
	return indexes == 0, nil
}

// sqliteKeyHoldsBlob reports whether the key column holds a BLOB in any row.
func sqliteKeyHoldsBlob(ctx context.Context, source dal.CollectionRef, column string, execute executeQueryFunc) (bool, error) {
	table, err := compileSQLTableSource(source)
	if err != nil {
		return false, err
	}
	quoted, err := quoteCheckedSQLIdentifier(positionField, column)
	if err != nil {
		return false, err
	}
	var blobs int
	if err := queryOneInt(ctx, execute, "SELECT COUNT(*) FROM (SELECT 1 FROM "+table+" WHERE typeof("+quoted+") = 'blob' LIMIT 1)", nil, &blobs); err != nil {
		return false, fmt.Errorf("inspect SQLite key column %q: %w", column, err)
	}
	return blobs > 0, nil
}

// queryOneInt runs a statement that returns one integer.
func queryOneInt(ctx context.Context, execute executeQueryFunc, statement string, args []any, into *int) error {
	rows, err := execute(ctx, statement, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(into); err != nil {
			return err
		}
	}
	return rows.Err()
}
