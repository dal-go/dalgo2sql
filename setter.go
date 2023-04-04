package dalgo2sql

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Set(ctx context.Context, record dal.Record) error {
	return setSingle(ctx, dtb.options, record, dtb.db.Query, dtb.db.Exec)
}

func (t transaction) Set(ctx context.Context, record dal.Record) error {
	return setSingle(ctx, t.sqlOptions, record, t.tx.Query, t.tx.Exec)
}

func (dtb *database) SetMulti(ctx context.Context, records []dal.Record) error {
	return setMulti(ctx, dtb.options, records, dtb.db.Query, dtb.db.Exec)
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	return setMulti(ctx, t.sqlOptions, records, t.tx.Query, t.tx.Exec)
}

func setSingle(_ context.Context, options Options, record dal.Record, execQuery queryExecutor, exec statementExecutor) error {
	exists, err := existsSingle(options, record.Key(), execQuery)
	if err != nil {
		return fmt.Errorf("failed to check if record exists: %w", err)
	}
	var qry query
	if exists {
		qry = buildSingleRecordQuery(update, options, record)
	} else {
		qry = buildSingleRecordQuery(insert, options, record)
	}
	if _, err := exec(qry.text, qry.args...); err != nil {
		return err
	}
	return nil
}

func setMulti(ctx context.Context, options Options, records []dal.Record, execQuery queryExecutor, execStatement statementExecutor) error {
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
