package dalgo2sql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// typedCatalogRow is one column of one source, as the MySQL and Oracle catalog
// queries return it.
type typedCatalogRow struct {
	source     int
	column     string
	dataType   string
	notNull    bool
	primaryKey bool
	collation  int64
	bytewise   bool
}

// typedCatalogFactsByCase runs a catalog query that numbers its sources (the MySQL,
// Oracle and SQL Server ones): query(n) selects the columns of n sources, bound as
// args lists them, and its first column says which source a row belongs to.
// Names are matched exactly (Fold is nil), and the facts are keyed by the query's
// own spelling. quote checks each name as the statement will write it, so a name the
// statement could not write is never looked up. A source the catalog does not list
// is absent.
func typedCatalogFactsByCase(ctx context.Context, execute executeQueryFunc, sources []typedSourceName,
	quote func(string) (string, error), query func(int) string, args func([]typedSourceName) []any, fact func(typedCatalogRow) typedColumnFact,
) (typedCatalogFacts, error) {
	facts := typedCatalogFacts{}
	if len(sources) == 0 {
		return facts, nil
	}
	var asked []typedSourceName
	seen := map[typedSourceName]bool{}
	for _, source := range sources {
		if _, err := quote(source.Name); err != nil {
			return typedCatalogFacts{}, fmt.Errorf("catalog facts: table name: %w", err)
		}
		if source.Schema != "" {
			if _, err := quote(source.Schema); err != nil {
				return typedCatalogFacts{}, fmt.Errorf("catalog facts: schema name: %w", err)
			}
		}
		if !seen[source] {
			seen[source] = true
			asked = append(asked, source)
		}
	}
	rows, err := execute(ctx, query(len(asked)), args(asked)...)
	if err != nil {
		return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	facts.Sources = make(map[typedSourceName]typedSourceFacts, len(asked))
	for rows.Next() {
		var number, notNull, primaryKey, collation, bytewise any
		var row typedCatalogRow
		if err := rows.Scan(&number, &row.column, &row.dataType, &notNull, &primaryKey, &collation, &bytewise); err != nil {
			return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
		}
		if err := typedCatalogNumbers(&row, number, notNull, primaryKey, collation, bytewise); err != nil {
			return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
		}
		if row.source < 1 || row.source > len(asked) {
			return typedCatalogFacts{}, errors.New("catalog facts: the catalog answered for a relation that was not asked for")
		}
		key := asked[row.source-1]
		source := facts.Sources[key]
		source.Columns = append(source.Columns, fact(row))
		facts.Sources[key] = source
	}
	if err := rows.Err(); err != nil {
		return typedCatalogFacts{}, fmt.Errorf("catalog facts: %w", err)
	}
	return facts, nil
}

// typedSuggestSource runs a suggestion query that returns (schema, name) pairs and
// picks the nearest to source, as the PostgreSQL dialect does: a name in another
// schema spelt as asked first, then typedNearestName. Names are matched exactly.
func typedSuggestSource(ctx context.Context, execute executeQueryFunc, source typedSourceName, query string, args ...any) (typedSourceName, bool, error) {
	rows, err := execute(ctx, query, args...)
	if err != nil {
		return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []typedSourceName
	var names []string
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
		}
		if source.Schema == "" {
			schema = ""
		}
		found = append(found, typedSourceName{Schema: schema, Name: name})
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return typedSourceName{}, false, fmt.Errorf("suggest a table: %w", err)
	}
	for _, candidate := range found {
		if candidate.Name == source.Name && candidate.Schema != source.Schema {
			return candidate, true, nil
		}
	}
	if nearest := typedNearestName(source.Name, names); nearest >= 0 {
		return found[nearest], true, nil
	}
	return typedSourceName{}, false, nil
}

// typedCatalogNumbers reads the numeric columns of a catalog row. A driver hands a
// number back as it likes (int64, float64, a decimal in text or bytes, a bool for a
// truth value), so each is read through typedCatalogInt.
func typedCatalogNumbers(row *typedCatalogRow, number, notNull, primaryKey, collation, bytewise any) error {
	values := make([]int64, 5)
	for i, value := range []any{number, notNull, primaryKey, collation, bytewise} {
		n, err := typedCatalogInt(value)
		if err != nil {
			return err
		}
		values[i] = n
	}
	row.source, row.notNull, row.primaryKey, row.collation, row.bytewise = int(values[0]), values[1] != 0, values[2] != 0, values[3], values[4] != 0
	return nil
}

// typedCatalogInt reads one whole number of a catalog row.
func typedCatalogInt(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case float64:
		if v == math.Trunc(v) && math.Abs(v) <= 1<<53 {
			return int64(v), nil
		}
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case []byte:
		return typedCatalogInt(string(v))
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, nil
		}
	}
	return 0, fmt.Errorf("the catalog returned %T where a whole number belongs", value)
}

// typedSchemaNameArgs binds every source as (schema, name), in order, as the MySQL
// and Oracle catalog queries read them. schema writes the schema argument of a source
// (NULL or the empty string for none).
func typedSchemaNameArgs(schema func(string) any) func([]typedSourceName) []any {
	return func(sources []typedSourceName) []any {
		args := make([]any, 0, 2*len(sources))
		for _, source := range sources {
			args = append(args, schema(source.Schema), source.Name)
		}
		return args
	}
}
