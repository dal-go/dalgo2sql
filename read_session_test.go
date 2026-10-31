package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func TestNewReadSessionPinnedReadOnlySurface(t *testing.T) {
	ctx := context.Background()
	raw := openTestSQLiteDB(t, nativeValuesScript)
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	options := nativeValuesOptions()
	options.PrimaryKey = []string{"id"}
	options.Recordsets["absent"] = nil
	session, err := NewReadSession(tx, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(dal.WriteSession); ok {
		t.Fatal("read session exposes writes")
	}
	if _, ok := session.(dal.Transaction); ok {
		t.Fatal("read session exposes transaction")
	}
	if _, ok := session.(io.Closer); ok {
		t.Fatal("read session exposes native lifetime")
	}
	// Every mutable container is isolated from subsequent caller edits.
	options.PrimaryKey[0] = "wrong"
	options.Recordsets["native_values"].primaryKey[0] = dal.Field("wrong")
	delete(options.Recordsets, "native_values")
	options.Recordsets["absent"] = NewRecordset("wrong", Table, nil)
	pinned := session.(*sqlReadSession).inner.sqlOptions
	if pinned.PrimaryKey[0] != "id" || pinned.Recordsets["absent"] != nil {
		t.Fatal("options alias caller containers")
	}
	if exists, err := session.Exists(ctx, dalrecord.NewKeyWithID("native_values", int64(1))); err != nil || !exists {
		t.Fatalf("Exists=%v, %v", exists, err)
	}
	one := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("native_values", int64(1)), map[string]any{})
	if err := session.Get(ctx, one); err != nil {
		t.Fatal(err)
	}
	requireNativeCell(t, "Get native integer", one.Data().(map[string]any)["amount"], int64(9007199254740993))
	two := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("native_values", int64(2)), map[string]any{})
	if err := session.GetMulti(ctx, []dalrecord.Record{one, two}); err != nil {
		t.Fatal(err)
	}
	requireNativeCell(t, "GetMulti BLOB", two.Data().(map[string]any)["note"], []byte{0, 255})
	q := dal.From(dal.NewRootCollectionRef("native_values", "")).NewQuery().Limit(2).SelectIntoRecordset()
	reader, err := session.ExecuteQueryToRecordsetReader(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := drainRecordset(t, reader)
	if len(rows) != 2 {
		t.Fatalf("recordset rows=%d", len(rows))
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := session.ExecuteQueryToRecordsReader(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := records.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Key().ID, int64(1)) {
		t.Fatalf("record key=%#v", rec.Key().ID)
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO native_values(id) VALUES(9)"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("transaction ownership changed: %v", err)
	}
}

func TestNewReadSessionRejectsInvalidAndFailedReaders(t *testing.T) {
	if session, err := NewReadSession(nil, DbOptions{}); session != nil || err == nil {
		t.Fatalf("nil tx: %v, %v", session, err)
	}
	raw := openTestSQLiteDB(t, "CREATE TABLE example(id INTEGER)")
	tx, err := raw.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	if session, err := NewReadSession(tx, DbOptions{PreserveNativeValues: true}); session != nil || err == nil {
		t.Fatalf("unsupported native values: %v, %v", session, err)
	}
	session, err := NewReadSession(tx, DbOptions{StructuredQueryDialect: "sqlite", PreserveNativeValues: true})
	if err != nil {
		t.Fatal(err)
	}
	q := dal.NewTextQuery("SELECT * FROM absent_table", nil)
	if reader, err := session.ExecuteQueryToRecordsReader(context.Background(), q); reader != nil || err == nil {
		t.Fatalf("failed records reader: %#v, %v", reader, err)
	}
	if reader, err := session.ExecuteQueryToRecordsetReader(context.Background(), q); reader != nil || err == nil {
		t.Fatalf("failed recordset reader: %#v, %v", reader, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if reader, err := session.ExecuteQueryToRecordsetReader(context.Background(), dal.NewTextQuery("SELECT 1", nil)); reader != nil || !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("rolled-back session: %#v, %v", reader, err)
	}
}
