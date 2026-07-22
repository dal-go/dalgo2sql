package dalgo2sql

import (
	"context"
	dalrecord "github.com/dal-go/record"
)

func (dtb *database) Upsert(ctx context.Context, record dalrecord.Record) error {
	return dtb.Set(ctx, record)
}

func (t transaction) Upsert(ctx context.Context, record dalrecord.Record) error {
	return t.Set(ctx, record)
}
