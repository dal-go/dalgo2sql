package dalgo2sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
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

// deleteTarget returns the table of key and its primary-key column as they may
// be written into SQL text: the table is the recordset of the key, the column is
// that of the recordset's single primary key.
//
// A recordset declared for the key (a non-nil entry of DbOptions.Recordsets under
// the key's recordset name) is followed as every other operation follows it: with no
// primary key, or with several columns, the key is refused with the errors Update
// returns, before any statement. The column ID is the default only where no recordset
// is declared for the key.
func deleteTarget(options DbOptions, key *record.Key) (table, pkColumn string, err error) {
	if table, err = options.recordsetIdentifier(key); err != nil {
		return "", "", err
	}
	pkName := "ID"
	if options.GetRecordsetByKey(key) != nil {
		switch primaryKey := options.PrimaryKeyFieldNames(key); len(primaryKey) {
		case 0:
			return "", "", fmt.Errorf("primary key is not defined for %s", getRecordsetName(key))
		case 1:
			pkName = primaryKey[0]
		default:
			return "", "", fmt.Errorf("%w: delete by composite primary key is not supported yet", dal.ErrNotImplementedYet)
		}
	}
	if pkColumn, err = options.sqlIdentifier(positionPrimaryKey, pkName); err != nil {
		return "", "", err
	}
	return table, pkColumn, nil
}

func deleteSingle(ctx context.Context, options DbOptions, key *record.Key, exec statementExecutor) error {
	table, pkColumn, err := deleteTarget(options, key)
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
	// The whole batch is checked before its first statement: recordsets are
	// deleted from one after the other.
	for _, key := range keys {
		if _, _, err := deleteTarget(options, key); err != nil {
			return err
		}
	}
	var prevRecordset string
	var tableKeys []*record.Key
	// The keys of one recordset are deleted by one statement: the key's own, or an
	// IN statement for several.
	deleteByKeys := func(keys []*record.Key) error {
		if len(keys) == 1 {
			return deleteSingle(ctx, options, keys[0], exec)
		}
		return deleteMultiInSingleTable(ctx, options, keys, exec)
	}
	for i, key := range keys {
		recordset := getRecordsetName(key)
		if recordset == prevRecordset {
			tableKeys = append(tableKeys, key)
			continue
		}
		if prevRecordset != "" {
			if err := deleteByKeys(tableKeys); err != nil {
				return err
			}
		}
		prevRecordset = recordset
		tableKeys = make([]*record.Key, 1, len(keys)-i)
		tableKeys[0] = key
	}
	if len(tableKeys) > 0 {
		if err := deleteByKeys(tableKeys); err != nil {
			return err
		}
	}
	return nil
}

func deleteMultiInSingleTable(ctx context.Context, options DbOptions, keys []*record.Key, exec statementExecutor) error {
	table, pkCol, err := deleteTarget(options, keys[0])
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
