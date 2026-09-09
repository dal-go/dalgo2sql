package dalgo2sql

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/condeval"
	"github.com/dal-go/dalgo/dal"
)

// The factory is available only on an explicitly opted-in SQLite mount.
// ConfigureProtectedAccess returns a secured facade, never its SQL handle.
type sqliteProtectedFactory struct {
	dal.DB
	storage     *sqliteProtectedStorage
	configureMu sync.Mutex
	configured  bool
}

func (f *sqliteProtectedFactory) ConfigureProtectedAccess(participants ...access.MandatoryParticipant) (dal.DB, *access.EnforcementCoordinator, error) {
	f.configureMu.Lock()
	defer f.configureMu.Unlock()
	if f.configured {
		return nil, nil, unsupportedSQLite()
	}
	coordinator, err := access.NewEnforcementCoordinator(f.storage, participants...)
	if err != nil {
		return nil, nil, err
	}
	pinned := append([]access.MandatoryParticipant(nil), participants...)
	provider := func(ctx context.Context) ([]access.Policy, error) {
		var policies []access.Policy
		for _, participant := range pinned {
			if participant.Provider == nil {
				continue
			}
			lease, err := participant.Provider(ctx)
			if err != nil {
				return nil, err
			}
			if lease == nil {
				return nil, unsupportedSQLite()
			}
			current := lease.Policies()
			lease.Release()
			if len(current) == 0 {
				return nil, unsupportedSQLite()
			}
			policies = append(policies, current...)
		}
		return policies, nil
	}
	db, err := access.SecureDB(f.DB, access.WithEnforcementCoordinator(coordinator), access.WithDatabasePolicyProvider(provider))
	if err == nil {
		f.configured = true
		f.DB = db
	}
	return db, coordinator, err
}

type sqliteProtectedStorage struct {
	db      *sql.DB
	options DbOptions
	secret  [32]byte
}

func newSQLiteProtectedFactory(db dal.DB, raw *sql.DB, options DbOptions) dal.DB {
	storage := &sqliteProtectedStorage{db: raw, options: options}
	if _, err := rand.Read(storage.secret[:]); err != nil {
		panic("SQLite revision entropy unavailable")
	}
	return &sqliteProtectedFactory{DB: db, storage: storage}
}

func (s *sqliteProtectedStorage) WithinProtectedInspection(ctx context.Context, ops []access.ProtectedOperation, callback func(access.ProtectedInspectionStorage) error) error {
	return s.within(ctx, ops, false, func(session *sqliteProtectedSession) error { return callback(sqliteInspection{session}) })
}
func (s *sqliteProtectedStorage) WithinProtectedExecution(ctx context.Context, ops []access.ProtectedOperation, callback func(access.ProtectedExecutionStorage) error) error {
	return s.within(ctx, ops, true, func(session *sqliteProtectedSession) error { return callback(sqliteExecution{session}) })
}

func (s *sqliteProtectedStorage) within(ctx context.Context, ops []access.ProtectedOperation, write bool, callback func(*sqliteProtectedSession) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	begin := "BEGIN"
	if write {
		begin = "BEGIN IMMEDIATE"
	}
	for {
		_, err = conn.ExecContext(ctx, begin)
		if err == nil {
			break
		}
		var code interface{ Code() int }
		if !errors.As(err, &code) || code.Code()&255 != 5 && code.Code()&255 != 6 {
			return err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	// Cleanup is bounded independently after ingress cancellation; it performs
	// rollback only. Cancellation never starts a background mutation.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "ROLLBACK")
	}()
	session := &sqliteProtectedSession{conn: conn, storage: s, ops: append([]access.ProtectedOperation(nil), ops...), alive: true, write: write}
	defer func() { session.mu.Lock(); session.alive = false; session.mu.Unlock() }()
	if err = callback(session); err != nil {
		return err
	}
	if !write || !session.executed {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

type sqliteColumn struct {
	name, kind string
	required   bool
}
type sqlitePrepared struct {
	operation  access.ProtectedOperation
	evidence   access.ProtectedEvidence
	columns    []sqliteColumn
	primaryKey string
}
type sqliteProtectedSession struct {
	mu                     sync.Mutex
	conn                   *sql.Conn
	storage                *sqliteProtectedStorage
	ops                    []access.ProtectedOperation
	prepared               []sqlitePrepared
	alive, write, executed bool
}
type sqliteInspection struct{ session *sqliteProtectedSession }
type sqliteExecution struct{ session *sqliteProtectedSession }

func (s sqliteInspection) Evidence(ctx context.Context) ([]access.ProtectedEvidence, error) {
	return s.session.evidence(ctx)
}
func (s sqliteExecution) Evidence(ctx context.Context) ([]access.ProtectedEvidence, error) {
	return s.session.evidence(ctx)
}
func (s sqliteExecution) Execute(ctx context.Context) error { return s.session.execute(ctx) }

func (s *sqliteProtectedSession) evidence(ctx context.Context) ([]access.ProtectedEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.alive || ctx.Err() != nil {
		return nil, fmt.Errorf("protected SQLite session is unavailable")
	}
	if s.prepared == nil {
		totalBytes := 0
		for _, op := range s.ops {
			prepared, err := s.prepare(ctx, op)
			if err != nil {
				var failed *sqlitePreparationError
				if !errors.As(err, &failed) {
					return nil, err
				}
				prepared = sqlitePrepared{operation: op, evidence: access.ProtectedEvidence{OperationID: op.ID(), CanonicalTarget: op.CanonicalTarget(), PreparationError: failed}}
			}
			encoded, err := json.Marshal(prepared.evidence)
			if err != nil {
				return nil, unsupportedSQLite()
			}
			totalBytes += len(encoded)
			if totalBytes > 8<<20 {
				return nil, unsupportedSQLite()
			}
			s.prepared = append(s.prepared, prepared)
		}
	}
	result := make([]access.ProtectedEvidence, len(s.prepared))
	for i, prepared := range s.prepared {
		result[i] = prepared.evidence
		if result[i].PreImage != nil {
			result[i].PreImage = condeval.CloneMap(result[i].PreImage)
		}
		if result[i].CandidateImage != nil {
			result[i].CandidateImage = condeval.CloneMap(result[i].CandidateImage)
		}
	}
	return result, nil
}

func unsupportedSQLite() error {
	return &access.DeniedError{Decision: access.Decision{Code: access.CodeEnforcementUnsupported, Scope: access.DecisionScopeOperation, Effect: "deny"}}
}

func (s *sqliteProtectedSession) prepare(ctx context.Context, op access.ProtectedOperation) (sqlitePrepared, error) {
	key := op.Key()
	if key == nil || key.Parent() != nil {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	table := key.Collection()
	rs := s.storage.options.Recordsets[table]
	if rs == nil || len(rs.PrimaryKey()) != 1 {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	pk := rs.PrimaryKey()[0].Name()
	var create string
	if err := s.conn.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", table).Scan(&create); err != nil {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	upper := strings.ToUpper(create)
	if strings.Contains(upper, "VIRTUAL") || strings.Contains(upper, "CHECK") || strings.Contains(upper, "COLLATE") {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	var triggers int
	if err := s.conn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name=?", table).Scan(&triggers); err != nil {
		return sqlitePrepared{}, err
	}
	if triggers > 0 {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	fks, err := s.conn.QueryContext(ctx, "PRAGMA foreign_key_list("+quoteSQLIdentifier(table)+")")
	if err != nil {
		return sqlitePrepared{}, err
	}
	hasFK := fks.Next()
	err = fks.Err()
	_ = fks.Close()
	if err != nil {
		return sqlitePrepared{}, err
	}
	if hasFK {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	rows, err := s.conn.QueryContext(ctx, "PRAGMA table_xinfo("+quoteSQLIdentifier(table)+")")
	if err != nil {
		return sqlitePrepared{}, err
	}
	var columns []sqliteColumn
	primaryCount := 0
	for rows.Next() {
		var index, required, primary, hidden int
		var name, kind string
		var defaultValue any
		if err = rows.Scan(&index, &name, &kind, &required, &defaultValue, &primary, &hidden); err != nil {
			_ = rows.Close()
			return sqlitePrepared{}, err
		}
		if hidden != 0 || defaultValue != nil {
			_ = rows.Close()
			return sqlitePrepared{}, unsupportedSQLite()
		}
		if primary > 0 {
			primaryCount++
			if name != pk || strings.ToUpper(kind) != "TEXT" {
				_ = rows.Close()
				return sqlitePrepared{}, unsupportedSQLite()
			}
		}
		columns = append(columns, sqliteColumn{name: name, kind: strings.ToUpper(kind), required: required != 0 || primary > 0})
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return sqlitePrepared{}, err
	}
	if len(columns) == 0 || primaryCount != 1 {
		return sqlitePrepared{}, unsupportedSQLite()
	}
	dataRows, err := s.conn.QueryContext(ctx, "SELECT * FROM "+quoteSQLIdentifier(table)+" WHERE "+quoteSQLIdentifier(pk)+" = ?", key.ID)
	if err != nil {
		return sqlitePrepared{}, err
	}
	exists := dataRows.Next()
	var pre map[string]any
	if exists {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		if err = dataRows.Scan(destinations...); err != nil {
			_ = dataRows.Close()
			return sqlitePrepared{}, err
		}
		pre = make(map[string]any, len(columns))
		for i, column := range columns {
			v, e := normalizeSQLiteScalar(column, values[i])
			if e != nil {
				_ = dataRows.Close()
				return sqlitePrepared{}, &sqlitePreparationError{e}
			}
			pre[column.name] = v
		}
	}
	err = dataRows.Err()
	_ = dataRows.Close()
	if err != nil {
		return sqlitePrepared{}, err
	}
	var candidate map[string]any
	switch op.Action() {
	case access.Insert, access.Set:
		candidate = op.Data()
	case access.Update:
		if pre != nil {
			candidate = condeval.CloneMap(pre)
		}
		for _, change := range op.Updates() {
			if len(change.Path) != 1 || change.Delete {
				return sqlitePrepared{}, &sqlitePreparationError{unsupportedSQLite()}
			}
			if exists {
				candidate[change.Path[0]] = change.Value
			}
		}
	case access.Delete, access.Get, access.Exists:
	default:
		return sqlitePrepared{}, &sqlitePreparationError{unsupportedSQLite()}
	}
	if candidate != nil {
		if supplied, ok := candidate[pk]; ok && supplied != key.ID {
			return sqlitePrepared{}, &sqlitePreparationError{unsupportedSQLite()}
		}
		candidate[pk] = key.ID
		known := map[string]bool{}
		for _, column := range columns {
			known[column.name] = true
			value, err := normalizeSQLiteScalar(column, candidate[column.name])
			if err != nil {
				return sqlitePrepared{}, &sqlitePreparationError{err}
			}
			candidate[column.name] = value
		}
		for name := range candidate {
			if !known[name] {
				return sqlitePrepared{}, &sqlitePreparationError{unsupportedSQLite()}
			}
		}
	}
	revisionFor := func(present bool, data map[string]any) (string, error) {
		encoded, err := json.Marshal(struct {
			Target string
			Exists bool
			Data   map[string]any
		}{op.CanonicalTarget(), present, data})
		if err != nil || len(encoded) > 8<<20 {
			return "", unsupportedSQLite()
		}
		mac := hmac.New(sha256.New, s.storage.secret[:])
		_, _ = mac.Write(encoded)
		return "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil)), nil
	}
	revision, err := revisionFor(exists, pre)
	if err != nil {
		return sqlitePrepared{}, &sqlitePreparationError{err}
	}
	candidateRevision, err := revisionFor(candidate != nil, candidate)
	if err != nil {
		return sqlitePrepared{}, &sqlitePreparationError{err}
	}
	return sqlitePrepared{operation: op, columns: columns, primaryKey: pk, evidence: access.ProtectedEvidence{OperationID: op.ID(), CanonicalTarget: op.CanonicalTarget(), SnapshotToken: revision, DataRevision: revision, CandidateRevision: candidateRevision, Exists: exists, Complete: true, PreImage: pre, CandidateImage: candidate}}, nil
}

func normalizeSQLiteScalar(column sqliteColumn, value any) (any, error) {
	if value == nil {
		if column.required {
			return nil, unsupportedSQLite()
		}
		return nil, nil
	}
	switch column.kind {
	case "TEXT":
		if text, ok := value.(string); ok {
			return text, nil
		}
	case "INTEGER", "INT", "BIGINT":
		switch n := value.(type) {
		case int:
			return int64(n), nil
		case int64:
			return n, nil
		case float64:
			if math.Abs(n) <= 1<<53 && math.Trunc(n) == n {
				return int64(n), nil
			}
		}
	case "REAL", "DOUBLE", "FLOAT":
		switch n := value.(type) {
		case float64:
			if !math.IsNaN(n) && !math.IsInf(n, 0) {
				return n, nil
			}
		case int64:
			if n >= -(1<<53) && n <= 1<<53 {
				return float64(n), nil
			}
		case int:
			return float64(n), nil
		}
	case "BOOLEAN", "BOOL":
		switch v := value.(type) {
		case bool:
			return v, nil
		case int64:
			if v == 0 || v == 1 {
				return v == 1, nil
			}
		}
	}
	return nil, unsupportedSQLite()
}

func (s *sqliteProtectedSession) execute(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.alive || !s.write || s.executed || len(s.prepared) != len(s.ops) {
		return unsupportedSQLite()
	}
	s.executed = true
	for _, prepared := range s.prepared {
		if err := ctx.Err(); err != nil {
			return err
		}
		op := prepared.operation
		table := quoteSQLIdentifier(op.Key().Collection())
		pk := quoteSQLIdentifier(prepared.primaryKey)
		var statement string
		var values []any
		switch op.Action() {
		case access.Get, access.Exists:
			return unsupportedSQLite()
		case access.Delete:
			statement = "DELETE FROM " + table + " WHERE " + pk + " = ?"
			values = []any{op.Key().ID}
		case access.Insert, access.Set, access.Update:
			names := make([]string, 0, len(prepared.evidence.CandidateImage))
			for name := range prepared.evidence.CandidateImage {
				names = append(names, name)
			}
			sort.Strings(names)
			if op.Action() == access.Insert || op.Action() == access.Set && !prepared.evidence.Exists {
				quoted := make([]string, len(names))
				placeholders := make([]string, len(names))
				for i, name := range names {
					quoted[i] = quoteSQLIdentifier(name)
					placeholders[i] = "?"
					values = append(values, prepared.evidence.CandidateImage[name])
				}
				statement = "INSERT INTO " + table + " (" + strings.Join(quoted, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")"
			} else {
				sets := []string{}
				for _, name := range names {
					if name == prepared.primaryKey {
						continue
					}
					sets = append(sets, quoteSQLIdentifier(name)+" = ?")
					values = append(values, prepared.evidence.CandidateImage[name])
				}
				if len(sets) == 0 {
					return unsupportedSQLite()
				}
				values = append(values, op.Key().ID)
				statement = "UPDATE " + table + " SET " + strings.Join(sets, ",") + " WHERE " + pk + " = ?"
			}
		default:
			return unsupportedSQLite()
		}
		result, err := s.conn.ExecContext(ctx, statement, values...)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("protected mutation affected unexpected record count")
		}
	}
	return nil
}

// Row-dependent preparation failures enter the coordinator without images so
// disclosure can use the same safe projection as other unreadable points.
type sqlitePreparationError struct{ error }
