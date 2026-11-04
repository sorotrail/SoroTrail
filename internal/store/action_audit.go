package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sorotrail/sorotrail/internal/apikey"
)

// ActorType constants classify whether an administrative action was initiated
// by a human operator or by automation (CI/CD, bot, scheduled job).
const (
	ActorTypePerson     = "person"
	ActorTypeAutomation = "automation"
)

// ActionAuditStatus constants record whether an administrative action succeeded
// or was rejected/failed.
const (
	ActionAuditStatusSucceeded = "succeeded"
	ActionAuditStatusFailed    = "failed"
)

// DefaultActionAuditRetentionAge is the default retention window for
// administrative action audit records (365 days), kept strictly separate from
// the high-volume event retention window.
const (
	DefaultActionAuditRetentionAge   = 365 * 24 * time.Hour
	DefaultActionAuditPruneBatchSize = 5_000
	DefaultActionAuditQueryLimit     = 100
	MaxActionAuditQueryLimit         = 1_000
)

// ErrActionAuditChainBroken is returned by VerifyActionAuditChain when a record's
// PrevHash or EntryHash does not match the deterministic hash chain.
var ErrActionAuditChainBroken = errors.New("action audit hash chain verification failed")

// ErrTenantScopeRequired is returned when QueryActionAuditLogs is called with a
// zero-value or unset ActionAuditScope.
var ErrTenantScopeRequired = errors.New("action audit query requires an explicit operator or tenant scope")

// ActionAuditRecord is a single immutable administrative action audit entry.
type ActionAuditRecord struct {
	ID           int64          `json:"id"`
	ActorID      string         `json:"actor_id"`
	ActorType    string         `json:"actor_type"`
	TenantID     *int64         `json:"tenant_id,omitempty"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	Method       string         `json:"method"`
	Route        string         `json:"route"`
	RemoteAddr   string         `json:"remote_addr"`
	RequestID    string         `json:"request_id"`
	Status       string         `json:"status"`
	HTTPStatus   int            `json:"http_status"`
	ErrorCode    string         `json:"error_code,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	PrevHash     string         `json:"prev_hash"`
	EntryHash    string         `json:"entry_hash"`
	CreatedAt    time.Time      `json:"created_at"`
}

type actionAuditScopeMode uint8

const (
	actionAuditScopeUnset actionAuditScopeMode = iota
	actionAuditScopeOperator
	actionAuditScopeTenant
)

// ActionAuditScope enforces mandatory tenant isolation when querying
// administrative action audit logs. A zero-value ActionAuditScope fails closed
// with ErrTenantScopeRequired.
type ActionAuditScope struct {
	mode     actionAuditScopeMode
	tenantID int64
}

// OperatorActionAuditScope returns a scope that permits reading all
// administrative action audit records across all tenants and operator actions.
func OperatorActionAuditScope() ActionAuditScope {
	return ActionAuditScope{mode: actionAuditScopeOperator}
}

// TenantActionAuditScope returns a scope restricted to a single positive
// tenant ID. If tenantID <= 0, the returned scope remains unset and fails
// closed on query.
func TenantActionAuditScope(tenantID int64) ActionAuditScope {
	if tenantID <= 0 {
		return ActionAuditScope{}
	}
	return ActionAuditScope{
		mode:     actionAuditScopeTenant,
		tenantID: tenantID,
	}
}

// IsOperator reports whether the scope grants full operator visibility.
func (s ActionAuditScope) IsOperator() bool {
	return s.mode == actionAuditScopeOperator
}

// TenantID returns the scoped tenant ID and true when the scope is bound to a
// specific tenant.
func (s ActionAuditScope) TenantID() (int64, bool) {
	if s.mode != actionAuditScopeTenant || s.tenantID <= 0 {
		return 0, false
	}
	return s.tenantID, true
}

func (s ActionAuditScope) validate() error {
	switch s.mode {
	case actionAuditScopeOperator:
		return nil
	case actionAuditScopeTenant:
		if s.tenantID <= 0 {
			return ErrTenantScopeRequired
		}
		return nil
	default:
		return ErrTenantScopeRequired
	}
}

// ActionAuditFilter specifies query predicates for administrative action audit
// logs. Scope is mandatory; omitting it fails closed with ErrTenantScopeRequired.
type ActionAuditFilter struct {
	Scope        ActionAuditScope
	TenantID     *int64
	Action       string
	ResourceType string
	ResourceID   string
	ActorID      string
	ActorType    string
	Status       string
	RequestID    string
	Since        time.Time
	Until        time.Time
	Limit        int
}

// ActionAuditStore is the capability interface implemented by stores that
// persist and query administrative action audit logs (#1277).
type ActionAuditStore interface {
	RecordActionAudit(ctx context.Context, rec ActionAuditRecord) (ActionAuditRecord, error)
	QueryActionAuditLogs(ctx context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error)
	PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

var (
	_ ActionAuditStore = (*Postgres)(nil)
	_ ActionAuditStore = (*SQLite)(nil)
	_ ActionAuditStore = (*TracingStore)(nil)
	_ ActionAuditStore = (*guardedStore)(nil)
	_ ActionAuditStore = (*MemoryActionAuditStore)(nil)
)

// NormalizeActionAuditRecord sanitizes actor identifiers, resource identifiers,
// and metadata so secret material is never persisted, and fills defaults for
// ActorID, ActorType, Status, and CreatedAt.
func NormalizeActionAuditRecord(rec ActionAuditRecord) ActionAuditRecord {
	rec.ActorID = SanitizeActorIdentifier(rec.ActorID)
	if rec.ActorID == "" {
		rec.ActorID = "anonymous"
	}
	switch strings.ToLower(strings.TrimSpace(rec.ActorType)) {
	case ActorTypeAutomation, "bot", "scheduled_job", "service", "ci":
		rec.ActorType = ActorTypeAutomation
	default:
		rec.ActorType = ActorTypePerson
	}
	rec.Action = strings.TrimSpace(rec.Action)
	rec.ResourceType = strings.TrimSpace(rec.ResourceType)
	rec.ResourceID = SanitizeAuditValueString(strings.TrimSpace(rec.ResourceID))
	rec.Method = strings.TrimSpace(rec.Method)
	rec.Route = strings.TrimSpace(rec.Route)
	rec.RemoteAddr = strings.TrimSpace(rec.RemoteAddr)
	rec.RequestID = strings.TrimSpace(rec.RequestID)
	switch strings.ToLower(strings.TrimSpace(rec.Status)) {
	case ActionAuditStatusFailed:
		rec.Status = ActionAuditStatusFailed
	default:
		rec.Status = ActionAuditStatusSucceeded
	}
	rec.ErrorCode = strings.TrimSpace(rec.ErrorCode)
	rec.Metadata = SanitizeActionAuditMetadata(rec.Metadata)
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	} else {
		rec.CreatedAt = rec.CreatedAt.UTC().Truncate(time.Microsecond)
	}
	return rec
}

// SanitizeActorIdentifier ensures a raw API key or bearer secret is never
// stored as an actor_id. If a full key ("sorotrail_<prefix>_<secret>" or
// "st_<prefix>_<secret>") is passed, only "<scheme>_<prefix>" is returned.
func SanitizeActorIdentifier(actorID string) string {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return ""
	}
	if redacted, ok := apikey.RedactIdentifier(actorID); ok {
		return redacted
	}
	lower := strings.ToLower(actorID)
	if strings.HasPrefix(lower, "bearer ") {
		token := strings.TrimSpace(actorID[len("bearer "):])
		if redacted, ok := apikey.RedactIdentifier(token); ok {
			return redacted
		}
		return "bearer-redacted"
	}
	return actorID
}

// SanitizeAuditValueString redacts any embedded raw API key or secret token in
// a string value while preserving public key prefixes.
func SanitizeAuditValueString(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if redacted, ok := apikey.RedactIdentifier(v); ok {
		return redacted
	}
	return v
}

func isSecretFieldName(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	switch k {
	case "secret", "key", "raw_key", "api_key", "apikey", "password", "token", "bearer", "authorization", "webhook_secret", "secret_hash":
		return true
	}
	return strings.Contains(k, "secret") || strings.Contains(k, "password") || strings.Contains(k, "token")
}

// SanitizeActionAuditMetadata returns a deep-sanitized copy of metadata with
// secret keys removed or converted to public prefixes so raw credentials can
// never be written to the audit log.
func SanitizeActionAuditMetadata(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		trimmedKey := strings.TrimSpace(k)
		if trimmedKey == "" {
			continue
		}
		if isSecretFieldName(trimmedKey) {
			// If a full API key was passed under "key" or "api_key", extract
			// only its public prefix under "key_prefix" and drop the secret.
			if s, ok := v.(string); ok {
				if redacted, ok := apikey.RedactIdentifier(s); ok {
					if _, exists := out["key_prefix"]; !exists {
						out["key_prefix"] = redacted
					}
				}
			}
			continue
		}
		switch typed := v.(type) {
		case string:
			out[trimmedKey] = SanitizeAuditValueString(typed)
		case map[string]any:
			out[trimmedKey] = SanitizeActionAuditMetadata(typed)
		default:
			out[trimmedKey] = typed
		}
	}
	return out
}

// ComputeActionAuditHash computes a deterministic SHA-256 hex digest over
// prevHash and the canonical fields of rec. Any post-hoc modification to a
// record or deletion of an intermediate record invalidates the hash chain.
func ComputeActionAuditHash(prevHash string, rec ActionAuditRecord) string {
	tenantPart := ""
	if rec.TenantID != nil {
		tenantPart = strconv.FormatInt(*rec.TenantID, 10)
	}
	metaJSON := canonicalMetadataJSON(rec.Metadata)
	var b strings.Builder
	b.WriteString(prevHash)
	b.WriteByte('|')
	b.WriteString(rec.ActorID)
	b.WriteByte('|')
	b.WriteString(rec.ActorType)
	b.WriteByte('|')
	b.WriteString(tenantPart)
	b.WriteByte('|')
	b.WriteString(rec.Action)
	b.WriteByte('|')
	b.WriteString(rec.ResourceType)
	b.WriteByte('|')
	b.WriteString(rec.ResourceID)
	b.WriteByte('|')
	b.WriteString(rec.Method)
	b.WriteByte('|')
	b.WriteString(rec.Route)
	b.WriteByte('|')
	b.WriteString(rec.RemoteAddr)
	b.WriteByte('|')
	b.WriteString(rec.RequestID)
	b.WriteByte('|')
	b.WriteString(rec.Status)
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(rec.HTTPStatus))
	b.WriteByte('|')
	b.WriteString(rec.ErrorCode)
	b.WriteByte('|')
	b.WriteString(metaJSON)
	b.WriteByte('|')
	b.WriteString(rec.CreatedAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano))

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func canonicalMetadataJSON(meta map[string]any) string {
	if len(meta) == 0 {
		return "{}"
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

// VerifyActionAuditChain verifies that the provided audit records form an
// unbroken, tamper-evident hash chain. Records may be passed in ascending or
// descending ID order; they are sorted ascending by ID before verification.
func VerifyActionAuditChain(records []ActionAuditRecord) error {
	if len(records) == 0 {
		return nil
	}
	ordered := make([]ActionAuditRecord, len(records))
	copy(ordered, records)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID < ordered[j].ID
	})
	for i, rec := range ordered {
		if i > 0 && rec.PrevHash != ordered[i-1].EntryHash {
			return fmt.Errorf("%w at id=%d: prev_hash %q != preceding entry_hash %q",
				ErrActionAuditChainBroken, rec.ID, rec.PrevHash, ordered[i-1].EntryHash)
		}
		expected := ComputeActionAuditHash(rec.PrevHash, rec)
		if rec.EntryHash != expected {
			return fmt.Errorf("%w at id=%d: entry_hash %q != computed %q",
				ErrActionAuditChainBroken, rec.ID, rec.EntryHash, expected)
		}
	}
	return nil
}

func effectiveActionAuditLimit(limit int) int {
	if limit <= 0 {
		return DefaultActionAuditQueryLimit
	}
	if limit > MaxActionAuditQueryLimit {
		return MaxActionAuditQueryLimit
	}
	return limit
}

// --- Postgres implementation ---

// RecordActionAudit implements ActionAuditStore on Postgres.
func (p *Postgres) RecordActionAudit(ctx context.Context, rec ActionAuditRecord) (ActionAuditRecord, error) {
	rec = NormalizeActionAuditRecord(rec)
	metaBytes := []byte(canonicalMetadataJSON(rec.Metadata))

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ActionAuditRecord{}, fmt.Errorf("begin action audit tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize hash-chain appends so concurrent administrative actions always
	// form a linear, unbranched hash chain.
	if _, err := tx.Exec(ctx, `LOCK TABLE admin_action_audit IN EXCLUSIVE MODE`); err != nil {
		return ActionAuditRecord{}, fmt.Errorf("locking admin_action_audit: %w", err)
	}

	var prevHash string
	err = tx.QueryRow(ctx, `SELECT entry_hash FROM admin_action_audit ORDER BY id DESC LIMIT 1`).Scan(&prevHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ActionAuditRecord{}, fmt.Errorf("reading previous action audit hash: %w", err)
	}
	rec.PrevHash = prevHash
	rec.EntryHash = ComputeActionAuditHash(rec.PrevHash, rec)

	err = tx.QueryRow(ctx, `
		INSERT INTO admin_action_audit (
			actor_id, actor_type, tenant_id, action, resource_type, resource_id,
			method, route, remote_addr, request_id, status, http_status,
			error_code, metadata, prev_hash, entry_hash, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12,
			$13, $14::jsonb, $15, $16, $17
		)
		RETURNING id, created_at`,
		rec.ActorID, rec.ActorType, rec.TenantID, rec.Action, rec.ResourceType, rec.ResourceID,
		rec.Method, rec.Route, rec.RemoteAddr, rec.RequestID, rec.Status, rec.HTTPStatus,
		rec.ErrorCode, string(metaBytes), rec.PrevHash, rec.EntryHash, rec.CreatedAt,
	).Scan(&rec.ID, &rec.CreatedAt)
	if err != nil {
		return ActionAuditRecord{}, fmt.Errorf("inserting action audit record: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ActionAuditRecord{}, fmt.Errorf("committing action audit record: %w", err)
	}
	rec.CreatedAt = rec.CreatedAt.UTC()
	return rec, nil
}

// QueryActionAuditLogs implements ActionAuditStore on Postgres.
func (p *Postgres) QueryActionAuditLogs(ctx context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error) {
	if err := filter.Scope.validate(); err != nil {
		return nil, err
	}

	var (
		clauses []string
		args    []any
		argPos  = 1
	)
	addArg := func(clause string, val any) {
		clauses = append(clauses, fmt.Sprintf(clause, argPos))
		args = append(args, val)
		argPos++
	}

	if tenantID, ok := filter.Scope.TenantID(); ok {
		addArg("tenant_id = $%d", tenantID)
	} else if filter.TenantID != nil {
		addArg("tenant_id = $%d", *filter.TenantID)
	}
	if filter.Action != "" {
		addArg("action = $%d", filter.Action)
	}
	if filter.ResourceType != "" {
		addArg("resource_type = $%d", filter.ResourceType)
	}
	if filter.ResourceID != "" {
		addArg("resource_id = $%d", filter.ResourceID)
	}
	if filter.ActorID != "" {
		addArg("actor_id = $%d", filter.ActorID)
	}
	if filter.ActorType != "" {
		addArg("actor_type = $%d", filter.ActorType)
	}
	if filter.Status != "" {
		addArg("status = $%d", filter.Status)
	}
	if filter.RequestID != "" {
		addArg("request_id = $%d", filter.RequestID)
	}
	if !filter.Since.IsZero() {
		addArg("created_at >= $%d", filter.Since.UTC())
	}
	if !filter.Until.IsZero() {
		addArg("created_at <= $%d", filter.Until.UTC())
	}

	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}
	limit := effectiveActionAuditLimit(filter.Limit)
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT id, actor_id, actor_type, tenant_id, action, resource_type, resource_id,
		       method, route, remote_addr, request_id, status, http_status,
		       error_code, metadata, prev_hash, entry_hash, created_at
		FROM admin_action_audit
		%s
		ORDER BY id DESC
		LIMIT $%d`, whereSQL, argPos)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying action audit logs: %w", err)
	}
	defer rows.Close()

	out := make([]ActionAuditRecord, 0)
	for rows.Next() {
		var (
			rec     ActionAuditRecord
			metaRaw []byte
		)
		if err := rows.Scan(
			&rec.ID, &rec.ActorID, &rec.ActorType, &rec.TenantID, &rec.Action,
			&rec.ResourceType, &rec.ResourceID, &rec.Method, &rec.Route,
			&rec.RemoteAddr, &rec.RequestID, &rec.Status, &rec.HTTPStatus,
			&rec.ErrorCode, &metaRaw, &rec.PrevHash, &rec.EntryHash, &rec.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning action audit row: %w", err)
		}
		rec.CreatedAt = rec.CreatedAt.UTC()
		rec.Metadata = map[string]any{}
		if len(metaRaw) > 0 {
			_ = json.Unmarshal(metaRaw, &rec.Metadata)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating action audit rows: %w", err)
	}
	return out, nil
}

// PruneActionAuditLogsBefore implements ActionAuditStore on Postgres.
func (p *Postgres) PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultActionAuditPruneBatchSize
	}
	tag, err := p.pool.Exec(ctx, `
		DELETE FROM admin_action_audit
		WHERE id IN (
			SELECT id FROM admin_action_audit
			WHERE created_at < $1
			ORDER BY created_at ASC, id ASC
			LIMIT $2
		)`, cutoff.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("pruning action audit logs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- SQLite implementation ---

// RecordActionAudit implements ActionAuditStore on SQLite.
func (s *SQLite) RecordActionAudit(ctx context.Context, rec ActionAuditRecord) (ActionAuditRecord, error) {
	rec = NormalizeActionAuditRecord(rec)
	metaJSON := canonicalMetadataJSON(rec.Metadata)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ActionAuditRecord{}, fmt.Errorf("begin sqlite action audit tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var prevHash string
	err = tx.QueryRowContext(ctx, `SELECT entry_hash FROM admin_action_audit ORDER BY id DESC LIMIT 1`).Scan(&prevHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ActionAuditRecord{}, fmt.Errorf("reading sqlite previous action audit hash: %w", err)
	}
	rec.PrevHash = prevHash
	rec.EntryHash = ComputeActionAuditHash(rec.PrevHash, rec)

	var tenantArg any
	if rec.TenantID != nil {
		tenantArg = *rec.TenantID
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO admin_action_audit (
			actor_id, actor_type, tenant_id, action, resource_type, resource_id,
			method, route, remote_addr, request_id, status, http_status,
			error_code, metadata, prev_hash, entry_hash, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ActorID, rec.ActorType, tenantArg, rec.Action, rec.ResourceType, rec.ResourceID,
		rec.Method, rec.Route, rec.RemoteAddr, rec.RequestID, rec.Status, rec.HTTPStatus,
		rec.ErrorCode, metaJSON, rec.PrevHash, rec.EntryHash, formatTime(rec.CreatedAt),
	)
	if err != nil {
		return ActionAuditRecord{}, fmt.Errorf("inserting sqlite action audit record: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return ActionAuditRecord{}, fmt.Errorf("reading sqlite action audit id: %w", err)
	}
	rec.ID = id

	if err := tx.Commit(); err != nil {
		return ActionAuditRecord{}, fmt.Errorf("committing sqlite action audit record: %w", err)
	}
	return rec, nil
}

// QueryActionAuditLogs implements ActionAuditStore on SQLite.
func (s *SQLite) QueryActionAuditLogs(ctx context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error) {
	if err := filter.Scope.validate(); err != nil {
		return nil, err
	}

	var (
		clauses []string
		args    []any
	)
	addArg := func(clause string, val any) {
		clauses = append(clauses, clause)
		args = append(args, val)
	}

	if tenantID, ok := filter.Scope.TenantID(); ok {
		addArg("tenant_id = ?", tenantID)
	} else if filter.TenantID != nil {
		addArg("tenant_id = ?", *filter.TenantID)
	}
	if filter.Action != "" {
		addArg("action = ?", filter.Action)
	}
	if filter.ResourceType != "" {
		addArg("resource_type = ?", filter.ResourceType)
	}
	if filter.ResourceID != "" {
		addArg("resource_id = ?", filter.ResourceID)
	}
	if filter.ActorID != "" {
		addArg("actor_id = ?", filter.ActorID)
	}
	if filter.ActorType != "" {
		addArg("actor_type = ?", filter.ActorType)
	}
	if filter.Status != "" {
		addArg("status = ?", filter.Status)
	}
	if filter.RequestID != "" {
		addArg("request_id = ?", filter.RequestID)
	}
	if !filter.Since.IsZero() {
		addArg("created_at >= ?", formatTime(filter.Since.UTC()))
	}
	if !filter.Until.IsZero() {
		addArg("created_at <= ?", formatTime(filter.Until.UTC()))
	}

	whereSQL := ""
	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}
	limit := effectiveActionAuditLimit(filter.Limit)
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT id, actor_id, actor_type, tenant_id, action, resource_type, resource_id,
		       method, route, remote_addr, request_id, status, http_status,
		       error_code, metadata, prev_hash, entry_hash, created_at
		FROM admin_action_audit
		%s
		ORDER BY id DESC
		LIMIT ?`, whereSQL)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying sqlite action audit logs: %w", err)
	}
	defer rows.Close()

	out := make([]ActionAuditRecord, 0)
	for rows.Next() {
		var (
			rec          ActionAuditRecord
			tenantIDNull sql.NullInt64
			metaRaw      string
			createdAtRaw string
		)
		if err := rows.Scan(
			&rec.ID, &rec.ActorID, &rec.ActorType, &tenantIDNull, &rec.Action,
			&rec.ResourceType, &rec.ResourceID, &rec.Method, &rec.Route,
			&rec.RemoteAddr, &rec.RequestID, &rec.Status, &rec.HTTPStatus,
			&rec.ErrorCode, &metaRaw, &rec.PrevHash, &rec.EntryHash, &createdAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scanning sqlite action audit row: %w", err)
		}
		if tenantIDNull.Valid {
			tid := tenantIDNull.Int64
			rec.TenantID = &tid
		}
		rec.CreatedAt = parseTime(createdAtRaw).UTC().Truncate(time.Microsecond)
		rec.Metadata = map[string]any{}
		if metaRaw != "" {
			_ = json.Unmarshal([]byte(metaRaw), &rec.Metadata)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating sqlite action audit rows: %w", err)
	}
	return out, nil
}

// PruneActionAuditLogsBefore implements ActionAuditStore on SQLite.
func (s *SQLite) PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultActionAuditPruneBatchSize
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM admin_action_audit
		WHERE id IN (
			SELECT id FROM admin_action_audit
			WHERE created_at < ?
			ORDER BY created_at ASC, id ASC
			LIMIT ?
		)`, formatTime(cutoff.UTC()), limit)
	if err != nil {
		return 0, fmt.Errorf("pruning sqlite action audit logs: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// --- TracingStore & guardedStore pass-throughs ---

// RecordActionAudit delegates to the wrapped store if it implements ActionAuditStore.
func (s *TracingStore) RecordActionAudit(ctx context.Context, rec ActionAuditRecord) (ActionAuditRecord, error) {
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return ActionAuditRecord{}, ErrUnsupported
	}
	return inner.RecordActionAudit(ctx, rec)
}

// QueryActionAuditLogs delegates to the wrapped store if it implements ActionAuditStore.
func (s *TracingStore) QueryActionAuditLogs(ctx context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error) {
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return nil, ErrUnsupported
	}
	return inner.QueryActionAuditLogs(ctx, filter)
}

// PruneActionAuditLogsBefore delegates to the wrapped store if it implements ActionAuditStore.
func (s *TracingStore) PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return 0, ErrUnsupported
	}
	return inner.PruneActionAuditLogsBefore(ctx, cutoff, limit)
}

func (s *guardedStore) RecordActionAudit(ctx context.Context, rec ActionAuditRecord) (ActionAuditRecord, error) {
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return ActionAuditRecord{}, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.RecordActionAudit")
	defer cancel()
	start := time.Now()
	out, err := inner.RecordActionAudit(ctx, rec)
	s.logSlowQuery(ctx, "store.RecordActionAudit", start, err)
	return out, err
}

func (s *guardedStore) QueryActionAuditLogs(ctx context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error) {
	if err := filter.Scope.validate(); err != nil {
		return nil, err
	}
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return nil, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.QueryActionAuditLogs")
	defer cancel()
	start := time.Now()
	out, err := inner.QueryActionAuditLogs(ctx, filter)
	s.logSlowQuery(ctx, "store.QueryActionAuditLogs", start, err)
	return out, err
}

func (s *guardedStore) PruneActionAuditLogsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	inner, ok := s.Store.(ActionAuditStore)
	if !ok {
		return 0, ErrUnsupported
	}
	ctx, cancel := s.wrapContext(ctx, "store.PruneActionAuditLogsBefore")
	defer cancel()
	start := time.Now()
	out, err := inner.PruneActionAuditLogsBefore(ctx, cutoff, limit)
	s.logSlowQuery(ctx, "store.PruneActionAuditLogsBefore", start, err)
	return out, err
}

// --- In-memory implementation ---

// MemoryActionAuditStore is a concurrency-safe in-memory ActionAuditStore used
// by unit tests and lightweight servers whose underlying Store does not
// directly implement ActionAuditStore.
type MemoryActionAuditStore struct {
	mu      sync.Mutex
	nextID  int64
	records []ActionAuditRecord
}

// NewMemoryActionAuditStore returns an initialized in-memory ActionAuditStore.
func NewMemoryActionAuditStore() *MemoryActionAuditStore {
	return &MemoryActionAuditStore{}
}

// RecordActionAudit implements ActionAuditStore on MemoryActionAuditStore.
func (m *MemoryActionAuditStore) RecordActionAudit(_ context.Context, rec ActionAuditRecord) (ActionAuditRecord, error) {
	rec = NormalizeActionAuditRecord(rec)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	rec.ID = m.nextID
	if rec.TenantID != nil {
		tid := *rec.TenantID
		rec.TenantID = &tid
	}
	prevHash := ""
	if len(m.records) > 0 {
		prevHash = m.records[len(m.records)-1].EntryHash
	}
	rec.PrevHash = prevHash
	rec.EntryHash = ComputeActionAuditHash(rec.PrevHash, rec)

	m.records = append(m.records, rec)
	return rec, nil
}

// QueryActionAuditLogs implements ActionAuditStore on MemoryActionAuditStore.
func (m *MemoryActionAuditStore) QueryActionAuditLogs(_ context.Context, filter ActionAuditFilter) ([]ActionAuditRecord, error) {
	if err := filter.Scope.validate(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	limit := effectiveActionAuditLimit(filter.Limit)
	out := make([]ActionAuditRecord, 0)
	for i := len(m.records) - 1; i >= 0; i-- {
		rec := m.records[i]
		if tenantID, ok := filter.Scope.TenantID(); ok {
			if rec.TenantID == nil || *rec.TenantID != tenantID {
				continue
			}
		} else if filter.TenantID != nil {
			if rec.TenantID == nil || *rec.TenantID != *filter.TenantID {
				continue
			}
		}
		if filter.Action != "" && rec.Action != filter.Action {
			continue
		}
		if filter.ResourceType != "" && rec.ResourceType != filter.ResourceType {
			continue
		}
		if filter.ResourceID != "" && rec.ResourceID != filter.ResourceID {
			continue
		}
		if filter.ActorID != "" && rec.ActorID != filter.ActorID {
			continue
		}
		if filter.ActorType != "" && rec.ActorType != filter.ActorType {
			continue
		}
		if filter.Status != "" && rec.Status != filter.Status {
			continue
		}
		if filter.RequestID != "" && rec.RequestID != filter.RequestID {
			continue
		}
		if !filter.Since.IsZero() && rec.CreatedAt.Before(filter.Since.UTC()) {
			continue
		}
		if !filter.Until.IsZero() && rec.CreatedAt.After(filter.Until.UTC()) {
			continue
		}
		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// PruneActionAuditLogsBefore implements ActionAuditStore on MemoryActionAuditStore.
func (m *MemoryActionAuditStore) PruneActionAuditLogsBefore(_ context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultActionAuditPruneBatchSize
	}
	cutoff = cutoff.UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		pruned int64
		kept   = make([]ActionAuditRecord, 0, len(m.records))
	)
	for _, rec := range m.records {
		if int(pruned) < limit && rec.CreatedAt.Before(cutoff) {
			pruned++
			continue
		}
		kept = append(kept, rec)
	}
	m.records = kept
	return pruned, nil
}

// --- Separate Administrative Action Audit Retention Policy ---

// ActionAuditRetentionOptions configures the retention policy for
// administrative action audit records (#1277), strictly decoupled from event
// retention (RetentionOptions) so short event-retention windows never delete
// administrative audit history.
type ActionAuditRetentionOptions struct {
	// Age is the minimum age of action audit entries eligible for deletion.
	// Defaults to DefaultActionAuditRetentionAge (365 days) when <= 0.
	Age time.Duration
	// BatchSize bounds how many rows are deleted per prune step.
	BatchSize int
	// Now overrides time.Now in tests.
	Now func() time.Time
}

// PruneActionAuditLogs deletes administrative action audit records older than
// opts.Age in bounded batches until none remain or ctx is cancelled.
func PruneActionAuditLogs(ctx context.Context, st ActionAuditStore, opts ActionAuditRetentionOptions) (int64, error) {
	age := opts.Age
	if age <= 0 {
		age = DefaultActionAuditRetentionAge
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = DefaultActionAuditPruneBatchSize
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	cutoff := nowFn().UTC().Add(-age)

	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := st.PruneActionAuditLogsBefore(ctx, cutoff, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < int64(batch) {
			return total, nil
		}
	}
}
