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

func setSingle(ctx context.Context, options Options, record dal.Record, execQuery queryExecutor, exec statementExecutor) error {
	key := record.Key()
	exists, err := existsSingle(options, key, execQuery)
	if err != nil {
		return fmt.Errorf("failed to check if record exists: %w", err)
	}
	var o operation
	if exists {
		o = update
	} else {
		o = insert
	}
	qry := buildSingleRecordQuery(o, options, record)
	if _, err := exec(ctx, qry.text, qry.args...); err != nil {
		return err
	}
	return nil
}

func setMulti(ctx context.Context, options Options, records []dal.Record, execQuery queryExecutor, execStatement statementExecutor) error {
	// TODO(help-wanted): insert of multiple rows at once as: "INSERT INTO table (colA, colB) VALUES (a1, b2), (a2, b2)"
	for i, record := range records {
		if err := setSingle(ctx, options, record, execQuery, execStatement); err != nil {
			return fmt.Errorf("failed to set record #%d of %d: %w", i+1, len(records), err)
		}
	}
	return nil
}

func existsSingle(options Options, key *dal.Key, execQuery queryExecutor) (bool, error) {
	collection := key.Collection()
	var col = "ID"
	var where = "ID = ?"
	if rs, hasOptions := options.Recordsets[collection]; hasOptions && len(rs.PrimaryKey) == 1 {
		col = rs.PrimaryKey[0].Name
		where = col + " = ?"
	}
	queryText := fmt.Sprintf("SELECT %v FROM %v WHERE ", col, collection) + where
	rows, err := execQuery(queryText, key.ID)
	return err == nil && rows.Next(), err
}
