package dalgo2sql

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

type sqliteJoinKeyClass string

const (
	sqliteJoinNumber sqliteJoinKeyClass = "number"
	sqliteJoinText   sqliteJoinKeyClass = "text"
)

type sqliteJoinSource struct {
	source dal.RecordsetSource
	class  map[string]sqliteJoinKeyClass
}

// CanExecuteJoin accepts only complete, ordinary SQLite relation trees whose
// ON keys have declared and observed portable types. It is deliberately a
// preflight rather than a compiler hint: SQLite cannot raise DALgo's
// join_key_type diagnostic from a JOIN expression after it has started
// producing rows.
func (dtb *database) CanExecuteJoin(_ context.Context, _ dal.StructuredQuery) error {
	// A pool query can move to a different connection after a preflight. The
	// adapter therefore declines direct native execution rather than claiming
	// a runtime key check it cannot keep atomic with the JOIN itself.
	return fmt.Errorf("join_plan: SQLite native JOIN key preflight requires a read transaction")
}

// CanExecuteJoin lets the transaction path make the same decision with its
// transaction-local view of schema and key values.
func (t transaction) CanExecuteJoin(ctx context.Context, q dal.StructuredQuery) error {
	return canExecuteSQLiteJoin(ctx, q, t.sqlOptions.StructuredQueryDialect, t.tx.QueryContext)
}

// JoinFields supplies the schema order needed by DALgo's generic JOIN wildcard
// expansion. A direct database read may inspect its pool connection; a generic
// transaction read uses its transaction snapshot instead.
func (dtb *database) JoinFields(ctx context.Context, source dal.RecordsetSource) ([]string, error) {
	return sqliteJoinFields(ctx, source, dtb.options.StructuredQueryDialect, dtb.db.QueryContext)
}

func (t transaction) JoinFields(ctx context.Context, source dal.RecordsetSource) ([]string, error) {
	return sqliteJoinFields(ctx, source, t.sqlOptions.StructuredQueryDialect, t.tx.QueryContext)
}

func canExecuteSQLiteJoin(ctx context.Context, q dal.StructuredQuery, dialect string, execute executeQueryFunc) error {
	if dialect != "sqlite" {
		return fmt.Errorf("join_plan: native JOIN is available only for the SQLite structured-query dialect")
	}
	if q == nil || q.From() == nil {
		return fmt.Errorf("join_shape: from is required")
	}
	if err := dal.ValidateJoinTree(q.From()); err != nil {
		return err
	}
	if len(q.From().Joins()) == 0 {
		return nil
	}
	// A native plan promises the complete structured query, rather than merely
	// the relation tree. Check every clause before doing the key preflight so an
	// unsupported wildcard, FIRST/LAST aggregate, or expression takes DALgo's
	// generic path instead of failing after native planning was selected.
	if _, _, err := compileStructuredSQL(q); err != nil {
		return fmt.Errorf("join_plan: SQLite cannot natively compile query: %w", err)
	}
	sources, err := sqliteJoinSources(q.From())
	if err != nil {
		return err
	}
	return preflightSQLiteJoinKeys(ctx, q.From(), sources, execute, "from")
}

func sqliteJoinSources(from dal.FromSource) (map[string]sqliteJoinSource, error) {
	sources := make(map[string]sqliteJoinSource)
	var walk func(dal.FromSource, string) error
	walk = func(node dal.FromSource, path string) error {
		if node == nil || node.Base() == nil {
			return fmt.Errorf("%s: join_shape: relation requires a source", path)
		}
		source := node.Base()
		if _, err := compileSQLTableSource(source); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		identity := sourceSQLIdentity(source)
		if _, exists := sources[identity]; exists {
			return fmt.Errorf("%s: join_scope: duplicate source alias %q", path, identity)
		}
		sources[identity] = sqliteJoinSource{source: source, class: make(map[string]sqliteJoinKeyClass)}
		for i, join := range node.Joins() {
			child := join.From()
			if child == nil {
				child = dal.From(join.RecordsetSource)
			}
			if err := walk(child, fmt.Sprintf("%s.joins[%d].from", path, i)); err != nil {
				return err
			}
		}
		return nil
	}
	return sources, walk(from, "from")
}

func preflightSQLiteJoinKeys(ctx context.Context, from dal.FromSource, sources map[string]sqliteJoinSource, execute executeQueryFunc, path string) error {
	for i, join := range from.Joins() {
		joinPath := fmt.Sprintf("%s.joins[%d]", path, i)
		for onIndex, condition := range join.On() {
			comparison, ok := condition.(dal.Comparison)
			if !ok {
				return fmt.Errorf("%s.on[%d]: join_shape: ON must be a comparison", joinPath, onIndex)
			}
			left, leftOK := comparison.Left.(dal.FieldRef)
			right, rightOK := comparison.Right.(dal.FieldRef)
			if !leftOK || !rightOK {
				return fmt.Errorf("%s.on[%d]: join_shape: ON operands must be fields", joinPath, onIndex)
			}
			leftClass, err := sqliteJoinKeyClassFor(ctx, sources, left, execute)
			if err != nil {
				return fmt.Errorf("%s.on[%d].left: %w", joinPath, onIndex, err)
			}
			rightClass, err := sqliteJoinKeyClassFor(ctx, sources, right, execute)
			if err != nil {
				return fmt.Errorf("%s.on[%d].right: %w", joinPath, onIndex, err)
			}
			if leftClass != rightClass {
				return fmt.Errorf("%s.on[%d]: join_key_type: %s keys cannot compare with %s keys", joinPath, onIndex, leftClass, rightClass)
			}
		}
		child := join.From()
		if child == nil {
			child = dal.From(join.RecordsetSource)
		}
		if err := preflightSQLiteJoinKeys(ctx, child, sources, execute, joinPath+".from"); err != nil {
			return err
		}
	}
	return nil
}

func sqliteJoinKeyClassFor(ctx context.Context, sources map[string]sqliteJoinSource, field dal.FieldRef, execute executeQueryFunc) (sqliteJoinKeyClass, error) {
	source, ok := sources[field.Source()]
	if !ok {
		return "", fmt.Errorf("join_scope: unknown source %q", field.Source())
	}
	if class, ok := source.class[field.Name()]; ok {
		return class, nil
	}
	collection, err := sqliteCollectionSource(source.source)
	if err != nil {
		return "", err
	}
	declared, err := sqliteDeclaredColumnType(ctx, collection, field.Name(), execute)
	if err != nil {
		return "", err
	}
	class, ok := sqlitePortableJoinClass(declared)
	if !ok {
		return "", fmt.Errorf("join_key_type: SQLite declaration %q is not a portable JOIN key", declared)
	}
	if err := sqlitePreflightColumnValues(ctx, collection, field.Name(), class, execute); err != nil {
		return "", err
	}
	source.class[field.Name()] = class
	sources[field.Source()] = source
	return class, nil
}

func sqliteCollectionSource(source dal.RecordsetSource) (dal.CollectionRef, error) {
	if collection, ok := source.(dal.CollectionRef); ok {
		return collection, nil
	}
	if pointer, ok := source.(*dal.CollectionRef); ok && pointer != nil {
		return *pointer, nil
	}
	return dal.CollectionRef{}, fmt.Errorf("join_plan: source %q has no SQLite table metadata", source.Name())
}

func sqlitePortableJoinClass(declared string) (sqliteJoinKeyClass, bool) {
	declared = strings.ToUpper(strings.TrimSpace(declared))
	switch {
	case strings.Contains(declared, "INT"), strings.Contains(declared, "REAL"), strings.Contains(declared, "FLOA"), strings.Contains(declared, "DOUB"):
		return sqliteJoinNumber, true
	case strings.Contains(declared, "CHAR"), strings.Contains(declared, "CLOB"), strings.Contains(declared, "TEXT"):
		return sqliteJoinText, true
	default:
		return "", false
	}
}

func sqliteDeclaredColumnType(ctx context.Context, source dal.CollectionRef, field string, execute executeQueryFunc) (string, error) {
	columns, err := sqliteTableColumns(ctx, source, execute)
	if err != nil {
		return "", err
	}
	for _, column := range columns {
		if column.name == field {
			return column.declared, nil
		}
	}
	return "", fmt.Errorf("join_field: field %q is not present in SQLite table %q", field, source.Name())
}

type sqliteTableColumn struct {
	name     string
	declared string
}

func sqliteJoinFields(ctx context.Context, source dal.RecordsetSource, dialect string, execute executeQueryFunc) ([]string, error) {
	if dialect != "sqlite" {
		return nil, fmt.Errorf("join_plan: schema fields are available only for the SQLite structured-query dialect")
	}
	collection, err := sqliteCollectionSource(source)
	if err != nil {
		return nil, err
	}
	columns, err := sqliteTableColumns(ctx, collection, execute)
	if err != nil {
		return nil, err
	}
	fields := make([]string, len(columns))
	for i, column := range columns {
		fields[i] = column.name
	}
	return fields, nil
}

func sqliteTableColumns(ctx context.Context, source dal.CollectionRef, execute executeQueryFunc) ([]sqliteTableColumn, error) {
	pragma := "PRAGMA "
	if schema := source.Schema(); schema != "" {
		pragma += quoteSQLIdentifier(schema) + "."
	}
	pragma += "table_info(" + quoteSQLIdentifier(source.Name()) + ")"
	rows, err := execute(ctx, pragma)
	if err != nil {
		return nil, fmt.Errorf("join_plan: inspect SQLite table %q: %w", source.Name(), err)
	}
	defer func() { _ = rows.Close() }()
	var columns []sqliteTableColumn
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, declared string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &declared, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("join_plan: scan SQLite table metadata: %w", err)
		}
		columns = append(columns, sqliteTableColumn{name: name, declared: declared})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("join_plan: inspect SQLite table metadata: %w", err)
	}
	return columns, nil
}

func sqlitePreflightColumnValues(ctx context.Context, source dal.CollectionRef, field string, class sqliteJoinKeyClass, execute executeQueryFunc) error {
	table, err := compileSQLTableSource(source)
	if err != nil {
		return err
	}
	column := quoteSQLIdentifier(field)
	rows, err := execute(ctx, "SELECT typeof("+column+"), "+column+" FROM "+table+" WHERE "+column+" IS NOT NULL")
	if err != nil {
		return fmt.Errorf("join_plan: preflight SQLite JOIN key %q: %w", field, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sqliteType string
		var value any
		if err := rows.Scan(&sqliteType, &value); err != nil {
			return fmt.Errorf("join_plan: scan SQLite JOIN key %q: %w", field, err)
		}
		valid := (class == sqliteJoinNumber && (sqliteType == "integer" || sqliteType == "real")) || (class == sqliteJoinText && sqliteType == "text")
		if !valid {
			return fmt.Errorf("join_key_type: SQLite key %q has runtime type %q", field, sqliteType)
		}
		if class == sqliteJoinNumber {
			if err := sqliteValidatePortableJoinNumber(value, field); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("join_plan: preflight SQLite JOIN key %q: %w", field, err)
	}
	return nil
}

func sqliteValidatePortableJoinNumber(value any, field string) error {
	const maxSafeInteger = float64(9007199254740991)
	switch number := value.(type) {
	case int64:
		if number > int64(maxSafeInteger) || number < -int64(maxSafeInteger) {
			return fmt.Errorf("join_key_type: SQLite key %q integer exceeds portable safe range", field)
		}
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return fmt.Errorf("join_key_type: SQLite key %q is not finite", field)
		}
		if math.Trunc(number) == number && (number > maxSafeInteger || number < -maxSafeInteger) {
			return fmt.Errorf("join_key_type: SQLite key %q integer exceeds portable safe range", field)
		}
	}
	return nil
}

var _ dal.NativeJoinProvider = (*database)(nil)
var _ dal.NativeJoinProvider = (*transaction)(nil)
var _ dal.JoinFieldsProvider = (*database)(nil)
var _ dal.JoinFieldsProvider = (*transaction)(nil)
