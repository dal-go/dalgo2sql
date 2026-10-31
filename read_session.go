package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
)

// NewReadSession adapts an already-open SQL transaction to DALgo's read-only
// surface. The caller owns the transaction and must finish it; this function
// neither begins nor commits a transaction and does not certify native read-only
// enforcement. No native handle or write method is exposed by the returned type.
//
// Options and their slice/map containers are copied. Any interface or function
// implementation supplied in options must remain immutable for the session's
// lifetime. A host requiring a frozen compiler should use only built-in dialect
// options and no custom hooks.
func NewReadSession(tx *sql.Tx, options DbOptions) (dal.ReadSession, error) {
	if tx == nil {
		return nil, errors.New("dalgo2sql: read session requires a transaction")
	}
	if err := options.validatePreserveNativeValues(); err != nil {
		return nil, err
	}
	options.PrimaryKey = append([]string(nil), options.PrimaryKey...)
	if options.Recordsets != nil {
		records := make(map[string]*Recordset, len(options.Recordsets))
		for name, source := range options.Recordsets {
			if source == nil {
				records[name] = nil
				continue
			}
			cloned := *source
			cloned.primaryKey = append([]dal.FieldRef(nil), source.primaryKey...)
			records[name] = &cloned
		}
		options.Recordsets = records
	}
	return &sqlReadSession{inner: newTransaction(tx, options, dal.NewTransactionOptions(dal.TxWithReadonly()))}, nil
}

type sqlReadSession struct{ inner transaction }

var _ dal.ReadSession = (*sqlReadSession)(nil)

func (s *sqlReadSession) Get(ctx context.Context, record dalrecord.Record) error {
	return s.inner.Get(ctx, record)
}

func (s *sqlReadSession) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	return s.inner.GetMulti(ctx, records)
}

func (s *sqlReadSession) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return s.inner.ExecuteQueryToRecordsReader(ctx, query)
}

func (s *sqlReadSession) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	return s.inner.ExecuteQueryToRecordsetReader(ctx, query, options...)
}

func (s *sqlReadSession) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	return s.inner.Exists(ctx, key)
}
