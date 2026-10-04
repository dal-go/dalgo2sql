package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dal-go/record"
	"strings"
)

type statementExecutor = func(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

func (dtb *database) Delete(ctx context.Context, key *record.Key) error {
	return deleteSingle(ctx, dtb.options, key, dtb.db.ExecContext)
}

func (t transaction) Delete(ctx context.Context, key *record.Key) error {
	return deleteSingle(ctx, t.sqlOptions, key, t.tx.ExecContext)
}

func (dtb *database) DeleteMulti(ctx context.Context, keys []*record.Key) error {
	return deleteMulti(ctx, dtb.options, keys, dtb.db.ExecContext)
}

// deleteTarget returns the table of collection and its primary-key column as
// they may be written into SQL text: the column of the recordset's single
// primary key, or ID when the collection has none declared.
func deleteTarget(options DbOptions, collection string) (table, pkColumn string, err error) {
	if table, err = options.sqlIdentifier(positionCollection, collection); err != nil {
		return "", "", err
	}
	pkName := "ID"
	if rs, hasOptions := options.Recordsets[collection]; hasOptions && len(rs.PrimaryKey()) == 1 {
		pkName = rs.PrimaryKey()[0].Name()
	}
	if pkColumn, err = options.sqlIdentifier(positionPrimaryKey, pkName); err != nil {
		return "", "", err
	}
	return table, pkColumn, nil
}

func deleteSingle(ctx context.Context, options DbOptions, key *record.Key, exec statementExecutor) error {
	table, pkColumn, err := deleteTarget(options, key.Collection())
	if err != nil {
		return err
	}
	//goland:noinspection SqlNoDataSourceInspection
	query := fmt.Sprintf("DELETE FROM %v WHERE %v = %s", table, pkColumn, options.Placeholder.placeholder(1))
	if _, err = exec(ctx, query, key.ID); err != nil {
		return err
	}
	return nil
}

func deleteMulti(ctx context.Context, options DbOptions, keys []*record.Key, exec statementExecutor) error {
	// The whole batch is checked before its first statement: collections are
	// deleted from one after the other.
	for _, key := range keys {
		if _, _, err := deleteTarget(options, key.Collection()); err != nil {
			return err
		}
	}
	var prevTable string
	var tableKeys []*record.Key
	deleteByKeys := func(table string, keys []*record.Key) error {
		if len(keys) == 1 {
			return deleteSingle(ctx, options, keys[0], exec)
		}
		for _, key := range keys {
			if err := deleteSingle(ctx, options, key, exec); err != nil {
				return err
			}
		}
		if err := deleteMultiInSingleTable(ctx, options, keys, exec); err != nil {
			return err
		}
		return nil // TODO: code above commented out as tests are failing for RAMSQL driver.
	}
	for i, key := range keys {
		kind := key.Collection()
		if kind == prevTable {
			tableKeys = append(tableKeys, key)
			continue
		}
		if prevTable != "" {
			if err := deleteByKeys(prevTable, tableKeys); err != nil {
				return err
			}
		}
		prevTable = kind
		tableKeys = make([]*record.Key, 1, len(keys)-i)
		tableKeys[0] = key
	}
	if len(tableKeys) > 0 {
		if err := deleteByKeys(prevTable, tableKeys); err != nil {
			return err
		}
	}
	return nil
}

func deleteMultiInSingleTable(ctx context.Context, options DbOptions, keys []*record.Key, exec statementExecutor) error {
	table, pkCol, err := deleteTarget(options, keys[0].Collection())
	if err != nil {
		return err
	}

	query := fmt.Sprintf("DELETE FROM %v WHERE %v IN (", table, pkCol)
	args := make([]interface{}, len(keys))
	q := make([]string, len(keys))
	for i, key := range keys {
		args[i] = key.ID
		q[i] = options.Placeholder.placeholder(i + 1)
	}
	query += strings.Join(q, ", ") + ")"
	if _, err = exec(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func (t transaction) DeleteMulti(ctx context.Context, keys []*record.Key) error {
	return deleteMulti(ctx, t.sqlOptions, keys, t.tx.ExecContext)
}
