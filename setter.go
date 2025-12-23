package dalgo2sql

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Set(ctx context.Context, record dal.Record) error {
	return setSingle(ctx, dtb.options, record, dtb.db.Query, dtb.db.ExecContext)
}

func (t transaction) Set(ctx context.Context, record dal.Record) error {
	return setSingle(ctx, t.sqlOptions, record, t.tx.Query, t.tx.ExecContext)
}

func (dtb *database) SetMulti(ctx context.Context, records []dal.Record) error {
	err := dtb.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return setMulti(ctx, dtb.options, records, dtb.db.Query, dtb.db.ExecContext)
	})
	return err

}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	return setMulti(ctx, t.sqlOptions, records, t.tx.Query, t.tx.ExecContext)
}

func setSingle(ctx context.Context, options DbOptions, record dal.Record, execQuery queryExecutor, exec statementExecutor) error {
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
	qry := buildSingleRecordQuery(o, options, record)
	if _, err := exec(ctx, qry.text, qry.args...); err != nil {
		return err
	}
	return nil
}

func setMulti(ctx context.Context, options DbOptions, records []dal.Record, execQuery queryExecutor, execStatement statementExecutor) error {
	// TODO(help-wanted): insertOperation of multiple rows at once as: "INSERT INTO table (colA, colB) VALUES (a1, b2), (a2, b2)"
	for i, record := range records {
		if err := setSingle(ctx, options, record, execQuery, execStatement); err != nil {
			return fmt.Errorf("failed to set record #%d of %d: %w", i+1, len(records), err)
		}
	}
	return nil
}

func existsSingle(options DbOptions, key *dal.Key, execQuery queryExecutor) (bool, error) {
	collection := key.Collection()
	pk := options.PrimaryKeyFieldNames(key)
	var where string
	if len(pk) == 1 {
		where = pk[0] + " = ?"
	} else {
		return false, fmt.Errorf("%w: composite primary keys are not suported yet", dal.ErrNotImplementedYet)
	}
	// `SELECT 1` is not supported by some SQL drivers so select 1st column from primary key
	queryText := fmt.Sprintf("SELECT %s FROM %s WHERE %s", pk[0], collection, where)
	rows, err := execQuery(queryText, key.ID)
	return err == nil && rows.Next(), err
}
