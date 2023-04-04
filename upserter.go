package dalgo2sql

import (
	"context"
	"github.com/dal-go/dalgo/dal"
)

func (dtb *database) Upsert(ctx context.Context, record dal.Record) error {
	return dtb.Set(ctx, record)
}

func (t transaction) Upsert(ctx context.Context, record dal.Record) error {
	return t.Set(ctx, record)
}
