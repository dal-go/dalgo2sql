package dalgo2sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"strings"
	"testing"
	"time"
)

func TestKeyedReadCanceledGroupAnnotatesUnfinished(t *testing.T) {
	for _, groups := range [][]int{{2}, {1, 1}, {2, 1}, {1, 2}, {2, 2}} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var records []dalrecord.Record
			opts := keyedContextOptions()
			for i, n := range groups {
				name := fmt.Sprintf("t%d", i)
				opts.Recordsets[name] = NewRecordset(name, Table, []dal.FieldRef{dal.Field("id")})
				for j := 0; j < n; j++ {
					records = append(records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(name, j), map[string]any{}))
				}
			}
			calls := 0
			err := getMulti(ctx, opts, records, func(ctx context.Context, _ string, _ ...any) (*sql.Rows, error) {
				calls++
				cancel()
				return nil, ctx.Err()
			})
			if !errors.Is(err, context.Canceled) || calls != 1 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
			for i, r := range records {
				if !errors.Is(r.Error(), context.Canceled) {
					t.Errorf("record %d: Error()=%v Exists()=%v; unfinished record should carry cancellation", i, r.Error(), r.Exists())
				}
			}
		})
	}
}

func TestKeyedReadModerncPoolDeadlineRecordState(t *testing.T) {
	raw := openTestSQLiteDB(t, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
	raw.SetMaxOpenConns(1)
	held, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	records := keyedContextRecords("items")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = (&database{db: raw, options: keyedContextOptions()}).GetMulti(ctx, records)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	for i, r := range records {
		if !errors.Is(r.Error(), context.DeadlineExceeded) {
			t.Errorf("record %d: Error()=%v Exists()=%v; expected deadline", i, r.Error(), r.Exists())
		}
	}
}

func TestKeyedReadPostFirstRowFailureMarksUnfinished(t *testing.T) {
	raw, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	rows := sqlmock.NewRows([]string{"id", "label"}).AddRow(int64(-1), "one").AddRow(int64(-2), "two").RowError(1, context.Canceled)
	mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()
	records := keyedContextRecords("items")
	err = (&database{db: raw, options: keyedContextOptions()}).GetMulti(context.Background(), records)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if records[0].Error() != nil || records[0].Data().(map[string]any)["label"] != "one" {
		t.Fatal("completed row lost")
	}
	if !errors.Is(records[1].Error(), context.Canceled) {
		t.Errorf("unfinished record Error()=%v Exists()=%v", records[1].Error(), records[1].Exists())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestKeyedReadNormalMissing(t *testing.T) {
	raw := openTestSQLiteDB(t, "CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT); INSERT INTO items VALUES(-1,'one')")
	db := &database{db: raw, options: keyedContextOptions()}
	records := keyedContextRecords("items")
	if err := db.GetMulti(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if !records[0].Exists() || records[1].Exists() || records[1].Error() != nil {
		t.Fatal("ordinary missing semantics changed")
	}
	exists, err := db.Exists(context.Background(), records[1].Key())
	if exists || err != nil {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
	if err := db.Get(context.Background(), records[1]); !errors.Is(err, dalrecord.ErrRecordNotFound) {
		t.Fatal(err)
	}
}

func TestKeyedReadLaterFailurePreservesCompletedGroups(t *testing.T) {
	for _, groups := range [][]int{{1, 1}, {1, 2}, {2, 1}, {2, 2}} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = raw.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := keyedContextOptions()
			var records []dalrecord.Record
			for i, count := range groups {
				table := fmt.Sprintf("t%d", i)
				opts.Recordsets[table] = NewRecordset(table, Table, []dal.FieldRef{dal.Field("id")})
				for j := 0; j < count; j++ {
					records = append(records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(table, fmt.Sprint(j)), map[string]any{}))
				}
			}
			calls := 0
			completedTable := ""
			err = getMulti(ctx, opts, records, func(got context.Context, q string, args ...any) (*sql.Rows, error) {
				calls++
				if calls == 2 {
					cancel()
					return nil, got.Err()
				}
				for i, count := range groups {
					table := fmt.Sprintf("t%d", i)
					if !strings.Contains(q, "FROM `"+table+"` WHERE") {
						continue
					}
					completedTable = table
					rows := sqlmock.NewRows([]string{"id", "label"})
					for j := 0; j < count; j++ {
						rows.AddRow(fmt.Sprint(j), "filled")
					}
					mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()
					return raw.QueryContext(got, q, args...)
				}
				t.Fatalf("unknown query: %s", q)
				return nil, nil
			})
			if !errors.Is(err, context.Canceled) || calls != 2 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
			for _, r := range records {
				if r.Key().Collection() == completedTable {
					if r.Error() != nil || !r.Exists() || r.Data().(map[string]any)["label"] != "filled" {
						t.Fatal("completed group lost its result")
					}
				} else if !errors.Is(r.Error(), context.Canceled) {
					t.Fatalf("unfinished record error=%v", r.Error())
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			assertKeyedNoRowsInUse(t, raw)
		})
	}
}

func TestKeyedReadPartialMapAndStructFailures(t *testing.T) {
	for _, structData := range []bool{false, true} {
		for _, scanFailure := range []bool{false, true} {
			if scanFailure && !structData {
				continue
			}
			t.Run(fmt.Sprintf("struct=%t/scan=%t", structData, scanFailure), func(t *testing.T) {
				raw, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = raw.Close() }()
				failure := errors.New("iteration failed after first row")
				rows := sqlmock.NewRows([]string{"id", "label"}).AddRow("one", 1).AddRow("two", "bad")
				if !scanFailure {
					rows.RowError(1, failure)
				}
				mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()
				var records []dalrecord.Record
				for _, id := range []string{"one", "two"} {
					var data any = map[string]any{}
					if structData {
						data = &struct{ Label int }{}
					}
					records = append(records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", id), data))
				}
				err = (&database{db: raw, options: keyedContextOptions()}).GetMulti(context.Background(), records)
				if scanFailure {
					if err == nil || !strings.Contains(err.Error(), "converting driver.Value") {
						t.Fatalf("scan err=%v", err)
					}
				} else if !errors.Is(err, failure) {
					t.Fatalf("err=%v", err)
				}
				if records[0].Error() != nil || !records[0].Exists() {
					t.Fatal("completed first row was changed")
				}
				if structData {
					if records[0].Data().(*struct{ Label int }).Label != 1 {
						t.Fatal("completed struct lost data")
					}
				} else if records[0].Data().(map[string]any)["label"] != int64(1) {
					t.Fatal("completed map lost data")
				}
				if records[1].Error() == nil || !errors.Is(records[1].Error(), err) {
					t.Fatalf("unfinished record error=%v want %v", records[1].Error(), err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
				assertKeyedNoRowsInUse(t, raw)
			})
		}
	}
}
