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
// declared NOT NULL or is the INTEGER PRIMARY KEY that aliases the rowid, and a column with
// no declared type keeps the integer 1 and the text '1' as two values. Both would give two
// records one key, so such a column is not used.
func sqliteCatalogPrimaryKey(ctx context.Context, q dal.StructuredQuery, execute executeQueryFunc) (string, error) {
	source, err := sqliteCollectionSource(q.From().Base())
	if err != nil {
		return "", err
	}
	statement, args := `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?)`, []any{source.Name()}
	if schema := source.Schema(); schema != "" {
		statement, args = `SELECT name, type, "notnull", pk FROM pragma_table_xinfo(?, ?)`, []any{source.Name(), schema}
	}
	rows, err := execute(ctx, statement, args...)
	if err != nil {
		return "", fmt.Errorf("inspect SQLite table %q: %w", source.Name(), err)
	}
	defer func() { _ = rows.Close() }()
	var key struct {
		name, declared string
		notNull        bool
	}
	keyColumns := 0
	for rows.Next() {
		var name, declared string
		var notNull, pk int
		if err := rows.Scan(&name, &declared, &notNull, &pk); err != nil {
			return "", fmt.Errorf("scan SQLite table metadata: %w", err)
		}
		if pk > 0 {
			keyColumns++
			key.name, key.declared, key.notNull = name, declared, notNull != 0
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("inspect SQLite table metadata: %w", err)
	}
	if keyColumns != 1 || !sqliteKeyColumnIsUnique(key.declared, key.notNull) {
		return "", nil
	}
	return key.name, nil
}

// sqliteKeyColumnIsUnique reports whether a single-column primary key declared with the type
// declared holds one distinct, present value per row: the column has an INTEGER or TEXT
// affinity, which stores every value of the column as one storage class, and it cannot be
// NULL, being declared NOT NULL or being exactly INTEGER (the rowid alias).
func sqliteKeyColumnIsUnique(declared string, notNull bool) bool {
	upper := strings.ToUpper(strings.TrimSpace(declared))
	integer := strings.Contains(upper, "INT")
	text := !integer && (strings.Contains(upper, "CHAR") || strings.Contains(upper, "CLOB") || strings.Contains(upper, "TEXT"))
	if !integer && !text {
		return false
	}
	return notNull || upper == "INTEGER"
}
