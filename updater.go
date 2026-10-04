package dalgo2sql

import (
	"context"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

func (dtb *database) Update(ctx context.Context, key *record.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	return updateSingle(ctx, dtb.options, dtb.db.ExecContext, key, updates, preconditions...)
}

func (t transaction) Update(ctx context.Context, key *record.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	return updateSingle(ctx, t.sqlOptions, t.tx.ExecContext, key, updates, preconditions...)
}

func (dtb *database) UpdateMulti(ctx context.Context, keys []*record.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	return updateMulti(ctx, dtb.options, dtb.db.ExecContext, keys, updates, preconditions...)
}

func (t transaction) UpdateMulti(ctx context.Context, keys []*record.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	return updateMulti(ctx, t.sqlOptions, t.tx.ExecContext, keys, updates, preconditions...)
}

// updateTarget holds the names of one update as they may be written into SQL
// text.
type updateTarget struct {
	table  string
	fields []string // one per update, in order
	pk     string   // the primary-key column, when the recordset has exactly one
}

// renderUpdateNames returns the names an update of key by updates writes into
// SQL text, or an error wrapping ErrUnsafeName for the first one refused.
func renderUpdateNames(options DbOptions, key *record.Key, updates []update.Update) (updateTarget, error) {
	table, err := options.sqlIdentifier(positionCollection, key.Collection())
	if err != nil {
		return updateTarget{}, err
	}
	target := updateTarget{table: table, fields: make([]string, len(updates))}
	for i, u := range updates {
		if target.fields[i], err = options.sqlIdentifier(positionField, u.FieldName()); err != nil {
			return updateTarget{}, err
		}
	}
	if primaryKey := options.PrimaryKeyFieldNames(key); len(primaryKey) == 1 {
		if target.pk, err = options.sqlIdentifier(positionPrimaryKey, primaryKey[0]); err != nil {
			return updateTarget{}, err
		}
	}
	return target, nil
}

func updateSingle(ctx context.Context, options DbOptions, execStatement statementExecutor, key *record.Key, updates []update.Update, _ ...dal.Precondition) error {
	target, err := renderUpdateNames(options, key, updates)
	if err != nil {
		return err
	}
	qry := query{
		text: fmt.Sprintf("UPDATE %v SET", target.table),
	}
	n := 1
	for i, u := range updates {
		qry.text += fmt.Sprintf("\n\t%v = %s", target.fields[i], options.Placeholder.placeholder(n))
		qry.args = append(qry.args, u.Value())
		n++
	}
	primaryKey := options.PrimaryKeyFieldNames(key)
	switch len(primaryKey) {
	case 0:
		return fmt.Errorf("primary key is not defined for %s", getRecordsetName(key))
	case 1:
		qry.text += fmt.Sprintf("\n\tWHERE %v = %s", target.pk, options.Placeholder.placeholder(n))
	default:
		return fmt.Errorf("%w: updateOperation by composite primary key is not supported yet", dal.ErrNotImplementedYet)
	}
	qry.args = append(qry.args, key.ID)
	result, err := execStatement(ctx, qry.text, qry.args...)
	if err != nil {
		return fmt.Errorf("failed to updateOperation a single record: %w", err)
	}
	if count, err := result.RowsAffected(); err == nil && count > 1 {
		return fmt.Errorf("expected to updateOperation a single row, number of affected rows: %v", count)
	}
	return nil
}

func updateMulti(ctx context.Context, options DbOptions, execStatement statementExecutor, keys []*record.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	// The whole batch is checked before its first statement: keys are updated
	// one by one, outside a transaction on a database handle.
	for i, key := range keys {
		if _, err := renderUpdateNames(options, key, updates); err != nil {
			return fmt.Errorf("failed to updateOperation record #%d of %d: %w", i+1, len(keys), err)
		}
	}
	for i, key := range keys {
		if err := updateSingle(ctx, options, execStatement, key, updates, preconditions...); err != nil {
			return fmt.Errorf("failed to updateOperation record #%d of %d: %w", i+1, len(keys), err)
		}
	}
	return nil
}
