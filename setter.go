package dalgo2sql

import (
	"context"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func (dtb *database) Set(ctx context.Context, record dalrecord.Record) error {
	return setSingle(ctx, dtb.options, record, dtb.db.Query, dtb.db.ExecContext)
}

func (t transaction) Set(ctx context.Context, record dalrecord.Record) error {
	return setSingle(ctx, t.sqlOptions, record, t.tx.Query, t.tx.ExecContext)
}

func (dtb *database) SetMulti(ctx context.Context, records []dalrecord.Record) error {
	err := dtb.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return setMulti(ctx, dtb.options, records, dtb.db.Query, dtb.db.ExecContext)
	})
	return err

}

func (t transaction) SetMulti(ctx context.Context, records []dalrecord.Record) error {
	return setMulti(ctx, t.sqlOptions, records, t.tx.Query, t.tx.ExecContext)
}

func setSingle(ctx context.Context, options DbOptions, record dalrecord.Record, execQuery queryExecutor, exec statementExecutor) error {
	// The existence check below sends a statement before the write is built, so
	// the names the write will carry are checked first.
	if err := options.checkRecordNames(record); err != nil {
		return err
	}
	key := record.Key()
	exists, err := existsSingle(options, key, execQuery)
	if err != nil {
		return fmt.Errorf("failed to check if record exists: %w", err)
	}
	var o operation
	if exists {
		o = updateOperation
	} else {
		o = insertOperation
	}
	qry, err := buildSingleRecordQuery(o, options, record)
	if err != nil {
		return err
	}
	if _, err := exec(ctx, qry.text, qry.args...); err != nil {
		return err
	}
	return nil
}

func setMulti(ctx context.Context, options DbOptions, records []dalrecord.Record, execQuery queryExecutor, execStatement statementExecutor) error {
	// The whole batch is checked before its first statement: records are set one
	// by one, outside a transaction on a database handle.
	for i, record := range records {
		if err := options.checkRecordNames(record); err != nil {
			return fmt.Errorf("failed to set record #%d of %d: %w", i+1, len(records), err)
		}
	}
	// TODO(help-wanted): insertOperation of multiple rows at once as: "INSERT INTO table (colA, colB) VALUES (a1, b2), (a2, b2)"
	for i, record := range records {
		if err := setSingle(ctx, options, record, execQuery, execStatement); err != nil {
			return fmt.Errorf("failed to set record #%d of %d: %w", i+1, len(records), err)
		}
	}
	return nil
}

func existsSingle(options DbOptions, key *dalrecord.Key, execQuery queryExecutor) (bool, error) {
	table, err := options.sqlIdentifier(positionCollection, key.Collection())
	if err != nil {
		return false, err
	}
	pk := options.PrimaryKeyFieldNames(key)
	if len(pk) != 1 {
		return false, fmt.Errorf("%w: composite primary keys are not suported yet", dal.ErrNotImplementedYet)
	}
	pkName, err := options.sqlIdentifier(positionPrimaryKey, pk[0])
	if err != nil {
		return false, err
	}
	where := pkName + " = " + options.Placeholder.placeholder(1)
	// `SELECT 1` is not supported by some SQL drivers so select 1st column from primary key
	queryText := fmt.Sprintf("SELECT %s FROM %s WHERE %s", pkName, table, where)
	rows, err := execQuery(queryText, key.ID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	return rows.Next(), rows.Err()
}
