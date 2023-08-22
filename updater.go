package dalgo2sql

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return updateSingle(ctx, dtb.options, dtb.db.ExecContext, key, updates, preconditions...)
}

func (t transaction) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return updateSingle(ctx, t.sqlOptions, t.tx.ExecContext, key, updates, preconditions...)
}

func (dtb *database) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return updateMulti(ctx, dtb.options, dtb.db.ExecContext, keys, updates, preconditions...)
}

func (t transaction) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	return updateMulti(ctx, t.sqlOptions, t.tx.ExecContext, keys, updates, preconditions...)
}

func updateSingle(ctx context.Context, options Options, execStatement statementExecutor, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	qry := query{
		text: fmt.Sprintf("UPDATE %v SET", key.Collection()),
	}
	for _, update := range updates {
		qry.text += fmt.Sprintf("\n\t%v = ?", update.Field)
		qry.args = append(qry.args, update.Value)
	}
	collection := key.Collection()
	if rs, hasOptions := options.Recordsets[collection]; hasOptions && len(rs.PrimaryKey) == 1 {
		qry.text += fmt.Sprintf("\n\tWHERE %v = ?", rs.PrimaryKey[0].Name)
	} else {
		qry.text += "\n\tWHERE ID = ?"
	}
	qry.args = append(qry.args, key.ID)
	result, err := execStatement(ctx, qry.text, qry.args...)
	if err != nil {
		return fmt.Errorf("failed to update a single record: %w", err)
	}
	if count, err := result.RowsAffected(); err == nil && count > 1 {
		return fmt.Errorf("expected to update a single row, number of affected rows: %v", count)
	}
	return nil
}

func updateMulti(ctx context.Context, options Options, execStatement statementExecutor, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	for i, key := range keys {
		if err := updateSingle(ctx, options, execStatement, key, updates, preconditions...); err != nil {
			return fmt.Errorf("failed to update record #%d of %d: %w", i+1, len(keys), err)
		}
	}
	return nil
}
