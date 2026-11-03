package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// DefaultIdempotencyTTL is the default retention window for idempotency keys.
// A 24-hour window covers client retry storms, network partitions, and
// delayed job queues while keeping the idempotency_keys table compact under
// periodic retention sweeps.
const DefaultIdempotencyTTL = 24 * time.Hour

// DefaultIdempotencySweepBatchSize bounds how many expired idempotency rows a
// single retention sweep deletes at once so maintenance never holds a long
// table lock against live write requests.
const DefaultIdempotencySweepBatchSize = 500

var (
	// ErrIdempotencyKeyMismatch is returned when a caller reuses an
	// unexpired Idempotency-Key with a different HTTP method, target path,
	// or request body hash. Callers map this to 422 Unprocessable Entity.
	ErrIdempotencyKeyMismatch = errors.New("idempotency key already used with different request parameters")

	// ErrIdempotencyInFlight is returned when another request with the same
	// tenant and Idempotency-Key is currently executing and has not yet
	// recorded its final status.
	ErrIdempotencyInFlight = errors.New("idempotency key request is currently in flight")
)

// IdempotencyOutcome reports whether ClaimIdempotencyKey reserved a fresh
// slot for execution or matched an existing record.
type IdempotencyOutcome int

const (
	// IdempotencyStarted means the caller won the uniqueness reservation
	// and must execute the operation, then call CompleteIdempotencyKey (or
	// DeleteIdempotencyKey on transient 5xx/cancellation failure).
	IdempotencyStarted IdempotencyOutcome = iota + 1

	// IdempotencyReplayed means a completed record with an identical
	// request fingerprint already exists and should be replayed verbatim.
	IdempotencyReplayed
)

// IdempotencyRecord holds the persisted state for one (tenant_id, idempotency_key)
// pair. Raw request bodies are never stored; only RequestHash (SHA-256) is
// persisted.
type IdempotencyRecord struct {
	TenantID     string
	Key          string
	RequestHash  string
	Method       string
	Path         string
	StatusCode   int
	ContentType  string
	ResponseBody []byte
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

// HashIdempotencyRequest computes the deterministic SHA-256 fingerprint of a
// write request's method, path (including query string), and raw body bytes.
func HashIdempotencyRequest(method, path string, body []byte) string {
	bodySum := sha256.Sum256(body)
	h := sha256.New()
	_, _ = h.Write([]byte(method))
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write([]byte(path))
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write(bodySum[:])
	return hex.EncodeToString(h.Sum(nil))
}

// IdempotencyStore is the persistence boundary for write-endpoint idempotency
// keys. Like TenantStore, it is a separate capability interface implemented by
// Postgres, SQLite, and MemoryIdempotencyStore so read-only/ingester mocks do
// not need to stub it.
type IdempotencyStore interface {
	ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error)
	CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error
	DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error
	GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error)
	DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error)
}

var (
	_ IdempotencyStore = (*Postgres)(nil)
	_ IdempotencyStore = (*SQLite)(nil)
	_ IdempotencyStore = (*MemoryIdempotencyStore)(nil)
)

// --- Postgres implementation ---

// ClaimIdempotencyKey atomically evicts any expired row for (tenant_id, key)
// and attempts to insert a new in-flight reservation (status_code = 0). If an
// unexpired row already exists, it validates the request fingerprint and
// returns either the stored response or ErrIdempotencyInFlight /
// ErrIdempotencyKeyMismatch.
func (p *Postgres) ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	now = now.UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	} else {
		rec.CreatedAt = rec.CreatedAt.UTC()
	}
	rec.ExpiresAt = rec.ExpiresAt.UTC()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("begin idempotency claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Remove any expired entry for this exact (tenant_id, idempotency_key)
	// inside the same transaction so a key presented after its retention
	// window is treated as brand new.
	if _, err := tx.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE tenant_id = $1 AND idempotency_key = $2 AND expires_at <= $3`,
		rec.TenantID, rec.Key, now,
	); err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("purging expired idempotency key: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (
			tenant_id, idempotency_key, request_hash, method, path,
			status_code, content_type, response_body, created_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, 0, '', ''::bytea, $6, $7)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		rec.TenantID, rec.Key, rec.RequestHash, rec.Method, rec.Path,
		rec.CreatedAt, rec.ExpiresAt,
	)
	if err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("inserting idempotency key: %w", err)
	}

	if tag.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return 0, IdempotencyRecord{}, fmt.Errorf("committing idempotency claim: %w", err)
		}
		return IdempotencyStarted, rec, nil
	}

	existing, err := scanPostgresIdempotencyRow(tx.QueryRow(ctx, `
		SELECT tenant_id, idempotency_key, request_hash, method, path,
		       status_code, content_type, response_body, created_at, expires_at
		FROM idempotency_keys
		WHERE tenant_id = $1 AND idempotency_key = $2`,
		rec.TenantID, rec.Key,
	))
	if err != nil {
		return 0, IdempotencyRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("committing idempotency lookup: %w", err)
	}

	if existing.RequestHash != rec.RequestHash || existing.Method != rec.Method || existing.Path != rec.Path {
		return 0, IdempotencyRecord{}, ErrIdempotencyKeyMismatch
	}
	if existing.StatusCode == 0 {
		return 0, existing, ErrIdempotencyInFlight
	}
	return IdempotencyReplayed, existing, nil
}

// CompleteIdempotencyKey stores the final status code, content-type, and
// response body for a previously claimed key.
func (p *Postgres) CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error {
	body := rec.ResponseBody
	if body == nil {
		body = []byte{}
	}
	_, err := p.pool.Exec(ctx, `
		UPDATE idempotency_keys
		SET status_code = $1, content_type = $2, response_body = $3
		WHERE tenant_id = $4 AND idempotency_key = $5`,
		rec.StatusCode, rec.ContentType, body, rec.TenantID, rec.Key,
	)
	if err != nil {
		return fmt.Errorf("completing idempotency key: %w", err)
	}
	return nil
}

// DeleteIdempotencyKey removes an in-flight or aborted key reservation.
func (p *Postgres) DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error {
	_, err := p.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE tenant_id = $1 AND idempotency_key = $2`,
		tenantID, key,
	)
	if err != nil {
		return fmt.Errorf("deleting idempotency key: %w", err)
	}
	return nil
}

// GetIdempotencyKey reads an unexpired idempotency record for (tenantID, key).
func (p *Postgres) GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	return scanPostgresIdempotencyRow(p.pool.QueryRow(ctx, `
		SELECT tenant_id, idempotency_key, request_hash, method, path,
		       status_code, content_type, response_body, created_at, expires_at
		FROM idempotency_keys
		WHERE tenant_id = $1 AND idempotency_key = $2 AND expires_at > $3`,
		tenantID, key, now.UTC(),
	))
}

// DeleteExpiredIdempotencyKeys deletes up to limit expired rows using
// idx_idempotency_keys_expires_at so each sweep stays bounded and cheap.
func (p *Postgres) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultIdempotencySweepBatchSize
	}
	tag, err := p.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE ctid IN (
			SELECT ctid FROM idempotency_keys
			WHERE expires_at <= $1
			ORDER BY expires_at ASC
			LIMIT $2
		)`, now.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("sweeping expired idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

func scanPostgresIdempotencyRow(row pgx.Row) (IdempotencyRecord, error) {
	var rec IdempotencyRecord
	err := row.Scan(
		&rec.TenantID,
		&rec.Key,
		&rec.RequestHash,
		&rec.Method,
		&rec.Path,
		&rec.StatusCode,
		&rec.ContentType,
		&rec.ResponseBody,
		&rec.CreatedAt,
		&rec.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyRecord{}, ErrNotFound
	}
	if err != nil {
		return IdempotencyRecord{}, fmt.Errorf("scanning idempotency key: %w", err)
	}
	return rec, nil
}

// --- SQLite implementation ---

// ClaimIdempotencyKey implements IdempotencyStore on SQLite.
func (s *SQLite) ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	now = now.UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	} else {
		rec.CreatedAt = rec.CreatedAt.UTC()
	}
	rec.ExpiresAt = rec.ExpiresAt.UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("begin sqlite idempotency claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM idempotency_keys
		WHERE tenant_id = ? AND idempotency_key = ? AND expires_at <= ?`,
		rec.TenantID, rec.Key, formatTime(now),
	); err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("purging expired sqlite idempotency key: %w", err)
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (
			tenant_id, idempotency_key, request_hash, method, path,
			status_code, content_type, response_body, created_at, expires_at
		) VALUES (?, ?, ?, ?, ?, 0, '', X'', ?, ?)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		rec.TenantID, rec.Key, rec.RequestHash, rec.Method, rec.Path,
		formatTime(rec.CreatedAt), formatTime(rec.ExpiresAt),
	)
	if err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("inserting sqlite idempotency key: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("checking sqlite idempotency rows affected: %w", err)
	}
	if affected == 1 {
		if err := tx.Commit(); err != nil {
			return 0, IdempotencyRecord{}, fmt.Errorf("committing sqlite idempotency claim: %w", err)
		}
		return IdempotencyStarted, rec, nil
	}

	existing, err := scanSQLiteIdempotencyRow(tx.QueryRowContext(ctx, `
		SELECT tenant_id, idempotency_key, request_hash, method, path,
		       status_code, content_type, response_body, created_at, expires_at
		FROM idempotency_keys
		WHERE tenant_id = ? AND idempotency_key = ?`,
		rec.TenantID, rec.Key,
	))
	if err != nil {
		return 0, IdempotencyRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return 0, IdempotencyRecord{}, fmt.Errorf("committing sqlite idempotency lookup: %w", err)
	}

	if existing.RequestHash != rec.RequestHash || existing.Method != rec.Method || existing.Path != rec.Path {
		return 0, IdempotencyRecord{}, ErrIdempotencyKeyMismatch
	}
	if existing.StatusCode == 0 {
		return 0, existing, ErrIdempotencyInFlight
	}
	return IdempotencyReplayed, existing, nil
}

// CompleteIdempotencyKey implements IdempotencyStore on SQLite.
func (s *SQLite) CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error {
	body := rec.ResponseBody
	if body == nil {
		body = []byte{}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE idempotency_keys
		SET status_code = ?, content_type = ?, response_body = ?
		WHERE tenant_id = ? AND idempotency_key = ?`,
		rec.StatusCode, rec.ContentType, body, rec.TenantID, rec.Key,
	)
	if err != nil {
		return fmt.Errorf("completing sqlite idempotency key: %w", err)
	}
	return nil
}

// DeleteIdempotencyKey implements IdempotencyStore on SQLite.
func (s *SQLite) DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM idempotency_keys
		WHERE tenant_id = ? AND idempotency_key = ?`,
		tenantID, key,
	)
	if err != nil {
		return fmt.Errorf("deleting sqlite idempotency key: %w", err)
	}
	return nil
}

// GetIdempotencyKey implements IdempotencyStore on SQLite.
func (s *SQLite) GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	return scanSQLiteIdempotencyRow(s.db.QueryRowContext(ctx, `
		SELECT tenant_id, idempotency_key, request_hash, method, path,
		       status_code, content_type, response_body, created_at, expires_at
		FROM idempotency_keys
		WHERE tenant_id = ? AND idempotency_key = ? AND expires_at > ?`,
		tenantID, key, formatTime(now.UTC()),
	))
}

// DeleteExpiredIdempotencyKeys implements IdempotencyStore on SQLite.
func (s *SQLite) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultIdempotencySweepBatchSize
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM idempotency_keys
		WHERE rowid IN (
			SELECT rowid FROM idempotency_keys
			WHERE expires_at <= ?
			ORDER BY expires_at ASC
			LIMIT ?
		)`, formatTime(now.UTC()), limit)
	if err != nil {
		return 0, fmt.Errorf("sweeping expired sqlite idempotency keys: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func scanSQLiteIdempotencyRow(row *sql.Row) (IdempotencyRecord, error) {
	var (
		rec              IdempotencyRecord
		createdAtRaw     string
		expiresAtRaw     string
	)
	err := row.Scan(
		&rec.TenantID,
		&rec.Key,
		&rec.RequestHash,
		&rec.Method,
		&rec.Path,
		&rec.StatusCode,
		&rec.ContentType,
		&rec.ResponseBody,
		&createdAtRaw,
		&expiresAtRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return IdempotencyRecord{}, ErrNotFound
	}
	if err != nil {
		return IdempotencyRecord{}, fmt.Errorf("scanning sqlite idempotency key: %w", err)
	}
	rec.CreatedAt = parseTime(createdAtRaw)
	rec.ExpiresAt = parseTime(expiresAtRaw)
	return rec, nil
}

// --- GuardedStore & TracingStore forwarding ---

func (s *guardedStore) ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return 0, IdempotencyRecord{}, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.ClaimIdempotencyKey")
	defer cancel()
	start := time.Now()
	outcome, out, err := inner.ClaimIdempotencyKey(ctx, rec, now)
	s.logSlowQuery(ctx, "store.ClaimIdempotencyKey", start, err)
	return outcome, out, err
}

func (s *guardedStore) CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.CompleteIdempotencyKey")
	defer cancel()
	start := time.Now()
	err := inner.CompleteIdempotencyKey(ctx, rec)
	s.logSlowQuery(ctx, "store.CompleteIdempotencyKey", start, err)
	return err
}

func (s *guardedStore) DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.DeleteIdempotencyKey")
	defer cancel()
	start := time.Now()
	err := inner.DeleteIdempotencyKey(ctx, tenantID, key)
	s.logSlowQuery(ctx, "store.DeleteIdempotencyKey", start, err)
	return err
}

func (s *guardedStore) GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return IdempotencyRecord{}, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.GetIdempotencyKey")
	defer cancel()
	start := time.Now()
	out, err := inner.GetIdempotencyKey(ctx, tenantID, key, now)
	s.logSlowQuery(ctx, "store.GetIdempotencyKey", start, err)
	return out, err
}

func (s *guardedStore) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return 0, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.DeleteExpiredIdempotencyKeys")
	defer cancel()
	start := time.Now()
	n, err := inner.DeleteExpiredIdempotencyKeys(ctx, now, limit)
	s.logSlowQuery(ctx, "store.DeleteExpiredIdempotencyKeys", start, err)
	return n, err
}

func (s *TracingStore) ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return 0, IdempotencyRecord{}, ErrUnsupported
	}
	return inner.ClaimIdempotencyKey(ctx, rec, now)
}

func (s *TracingStore) CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	return inner.CompleteIdempotencyKey(ctx, rec)
}

func (s *TracingStore) DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	return inner.DeleteIdempotencyKey(ctx, tenantID, key)
}

func (s *TracingStore) GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return IdempotencyRecord{}, ErrUnsupported
	}
	return inner.GetIdempotencyKey(ctx, tenantID, key, now)
}

func (s *TracingStore) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	inner, ok := s.Store.(IdempotencyStore)
	if !ok {
		return 0, ErrUnsupported
	}
	return inner.DeleteExpiredIdempotencyKeys(ctx, now, limit)
}

// --- MemoryIdempotencyStore (for in-memory unit tests without SQL backing) ---

type idempotencyCompositeKey struct {
	tenantID string
	key      string
}

// MemoryIdempotencyStore is a concurrency-safe in-memory implementation of
// IdempotencyStore with the exact same (tenant_id, idempotency_key) uniqueness,
// request-hash verification, in-flight reservation, and TTL semantics as the
// Postgres and SQLite implementations.
type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[idempotencyCompositeKey]IdempotencyRecord
}

// NewMemoryIdempotencyStore constructs an empty MemoryIdempotencyStore.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{
		records: make(map[idempotencyCompositeKey]IdempotencyRecord),
	}
}

func (m *MemoryIdempotencyStore) ClaimIdempotencyKey(_ context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	now = now.UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	} else {
		rec.CreatedAt = rec.CreatedAt.UTC()
	}
	rec.ExpiresAt = rec.ExpiresAt.UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	ck := idempotencyCompositeKey{tenantID: rec.TenantID, key: rec.Key}
	if existing, ok := m.records[ck]; ok {
		if !existing.ExpiresAt.After(now) {
			delete(m.records, ck)
		} else {
			if existing.RequestHash != rec.RequestHash || existing.Method != rec.Method || existing.Path != rec.Path {
				return 0, IdempotencyRecord{}, ErrIdempotencyKeyMismatch
			}
			if existing.StatusCode == 0 {
				return 0, cloneIdempotencyRecord(existing), ErrIdempotencyInFlight
			}
			return IdempotencyReplayed, cloneIdempotencyRecord(existing), nil
		}
	}

	stored := cloneIdempotencyRecord(rec)
	stored.StatusCode = 0
	stored.ContentType = ""
	stored.ResponseBody = nil
	m.records[ck] = stored
	return IdempotencyStarted, cloneIdempotencyRecord(stored), nil
}

func (m *MemoryIdempotencyStore) CompleteIdempotencyKey(_ context.Context, rec IdempotencyRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ck := idempotencyCompositeKey{tenantID: rec.TenantID, key: rec.Key}
	existing, ok := m.records[ck]
	if !ok {
		return ErrNotFound
	}
	existing.StatusCode = rec.StatusCode
	existing.ContentType = rec.ContentType
	existing.ResponseBody = append([]byte(nil), rec.ResponseBody...)
	m.records[ck] = existing
	return nil
}

func (m *MemoryIdempotencyStore) DeleteIdempotencyKey(_ context.Context, tenantID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.records, idempotencyCompositeKey{tenantID: tenantID, key: key})
	return nil
}

func (m *MemoryIdempotencyStore) GetIdempotencyKey(_ context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	now = now.UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	ck := idempotencyCompositeKey{tenantID: tenantID, key: key}
	existing, ok := m.records[ck]
	if !ok || !existing.ExpiresAt.After(now) {
		return IdempotencyRecord{}, ErrNotFound
	}
	return cloneIdempotencyRecord(existing), nil
}

func (m *MemoryIdempotencyStore) DeleteExpiredIdempotencyKeys(_ context.Context, now time.Time, limit int) (int64, error) {
	now = now.UTC()
	if limit <= 0 {
		limit = DefaultIdempotencySweepBatchSize
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var deleted int64
	for ck, rec := range m.records {
		if !rec.ExpiresAt.After(now) {
			delete(m.records, ck)
			deleted++
			if int(deleted) >= limit {
				break
			}
		}
	}
	return deleted, nil
}

func cloneIdempotencyRecord(rec IdempotencyRecord) IdempotencyRecord {
	out := rec
	if rec.ResponseBody != nil {
		out.ResponseBody = append([]byte(nil), rec.ResponseBody...)
	}
	return out
}
