package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// BackupFormatVersion identifies the JSON structure of BackupArchive.
const BackupFormatVersion = 1

// SecretMode controls how webhook signing secrets and API key hashes are
// serialized in a backup archive.
type SecretMode string

const (
	// SecretModeInclude writes secrets in plaintext JSON. WriteBackupFile enforces
	// 0600 file permissions and emits an operator warning.
	SecretModeInclude SecretMode = "include"
	// SecretModeRedact replaces webhook secrets and API key hashes with "REDACTED".
	// On restore, redacted webhook subscriptions are inserted with enabled=false
	// until an operator rotates their signing secret.
	SecretModeRedact SecretMode = "redact"
	// SecretModeEncrypt encrypts each secret field using AES-256-GCM with a key
	// derived from a caller-supplied passphrase and per-archive random salt.
	SecretModeEncrypt SecretMode = "encrypt"
)

const redactedSecretPlaceholder = "REDACTED"
const encryptedSecretPrefix = "enc:v1:"

var (
	// ErrRestoreTargetNotEmpty is returned when RestoreBackup targets a database
	// that already contains operator state or events and Force is false.
	ErrRestoreTargetNotEmpty = errors.New("restore target database is not empty (pass --force to overwrite)")
	// ErrCorruptBackup is returned when an archive fails JSON parsing, format
	// validation, or SHA-256 integrity verification.
	ErrCorruptBackup = errors.New("backup archive is corrupt or failed integrity check")
	// ErrBackupNewerSchema is returned when an archive was created from a newer
	// migration version than the target database currently has applied.
	ErrBackupNewerSchema = errors.New("backup schema version is newer than target database schema version; run 'sorotrail migrate up' first")
	// ErrSecretPassphraseRequired is returned when SecretModeEncrypt is selected
	// without a non-empty passphrase.
	ErrSecretPassphraseRequired = errors.New("passphrase is required when secret mode is 'encrypt'")
	// ErrSecretDecryptFailed is returned when decrypting an encrypted secret fails
	// (for example, wrong passphrase or tampered ciphertext).
	ErrSecretDecryptFailed = errors.New("failed to decrypt secret field in backup archive (wrong passphrase or corrupted ciphertext)")
)

// StateClassification documents what SoroTrail state is irreplaceable vs.
// reconstructible from the Stellar network / ledger archives.
type StateClassification struct {
	Table           string `json:"table"`
	Category        string `json:"category"` // "irreplaceable" | "reconstructible"
	BackedUpDefault bool   `json:"backed_up_by_default"`
	RecoveryMethod  string `json:"recovery_method"`
}

// IrreplaceableAndReconstructibleState returns the canonical inventory of
// database tables classified by whether they must be backed up or can be
// reconstructed from the chain.
func IrreplaceableAndReconstructibleState() []StateClassification {
	return []StateClassification{
		{Table: "watched_contracts", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; operator watch list does not exist on-chain."},
		{Table: "contract_cursors", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; per-contract ingestion resume checkpoints."},
		{Table: "ingestion_state", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; per-network global ingestion checkpoint."},
		{Table: "audit_state", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; gap-audit watermark."},
		{Table: "audit_findings", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; historical gap-audit findings and resolution state."},
		{Table: "replay_state", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; in-progress replay checkpoint."},
		{Table: "backfill_state", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; parallel backfill range checkpoints."},
		{Table: "subscriptions", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; webhook URLs, filters, and HMAC secrets."},
		{Table: "api_keys", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; operator API key prefixes, bcrypt hashes, and revocation timestamps."},
		{Table: "tenants", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; multi-tenant definitions and rate-limit quotas."},
		{Table: "tenant_contract_grants", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; tenant read-access ACLs."},
		{Table: "tenant_watched_contracts", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; tenant-scoped watched contract lists."},
		{Table: "tenant_api_keys", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; tenant API key SHA-256 hashes and metadata."},
		{Table: "contract_specs", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; cached contract WASM specs (avoids cold-start RPC fetches)."},
		{Table: "contract_spec_overrides", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; operator-uploaded custom contract event specs."},
		{Table: "contract_spec_override_history", Category: "irreplaceable", BackedUpDefault: true, RecoveryMethod: "Restored from backup; immutable audit trail of spec override changes."},
		{Table: "events", Category: "reconstructible", BackedUpDefault: false, RecoveryMethod: "Re-indexed from Stellar RPC / ledger archives via 'sorotrail backfill' or included in full snapshots via '--include-events'."},
		{Table: "delivery_attempts", Category: "reconstructible", BackedUpDefault: false, RecoveryMethod: "Transient webhook delivery telemetry; not required for resume correctness."},
	}
}

// BackupOptions configures CreateBackup.
type BackupOptions struct {
	IncludeEvents bool
	SecretMode    SecretMode
	Passphrase    string
}

// RestoreOptions configures RestoreBackup.
type RestoreOptions struct {
	Force              bool
	Verify             bool
	Passphrase         string
	RetentionMinLedger uint32
}

// EventSnapshotSummary records the shape of the events table at backup time,
// even when IncludeEvents is false, so post-restore verification can report the
// exact ledger window that needs chain backfill or retention alignment.
type EventSnapshotSummary struct {
	TotalCount        int64            `json:"total_count"`
	MinLedger         uint32           `json:"min_ledger"`
	MaxLedger         uint32           `json:"max_ledger"`
	PerContractCounts map[string]int64 `json:"per_contract_counts,omitempty"`
}

// BackupTables holds the serialized rows of each backed-up table.
type BackupTables struct {
	WatchedContracts            []WatchedContractBackupRow             `json:"watched_contracts"`
	ContractCursors             []ContractCursorBackupRow              `json:"contract_cursors"`
	IngestionState              []IngestionStateBackupRow              `json:"ingestion_state"`
	AuditState                  []AuditStateBackupRow                  `json:"audit_state"`
	AuditFindings               []AuditFindingBackupRow                `json:"audit_findings"`
	ReplayState                 []ReplayStateBackupRow                 `json:"replay_state"`
	BackfillState               []BackfillStateBackupRow               `json:"backfill_state,omitempty"`
	Subscriptions               []SubscriptionBackupRow                `json:"subscriptions"`
	APIKeys                     []APIKeyBackupRow                      `json:"api_keys,omitempty"`
	Tenants                     []TenantBackupRow                      `json:"tenants,omitempty"`
	TenantContractGrants        []TenantContractGrantBackupRow         `json:"tenant_contract_grants,omitempty"`
	TenantWatchedContracts      []TenantWatchedContractBackupRow       `json:"tenant_watched_contracts,omitempty"`
	TenantAPIKeys               []TenantAPIKeyBackupRow                `json:"tenant_api_keys,omitempty"`
	ContractSpecs               []ContractSpecBackupRow                `json:"contract_specs"`
	ContractSpecOverrides       []ContractSpecOverrideBackupRow        `json:"contract_spec_overrides"`
	ContractSpecOverrideHistory []ContractSpecOverrideHistoryBackupRow `json:"contract_spec_override_history"`
	Events                      []EventBackupRow                       `json:"events,omitempty"`
}

type WatchedContractBackupRow struct {
	Network    string `json:"network"`
	ContractID string `json:"contract_id"`
	AddedAt    string `json:"added_at"`
}

type ContractCursorBackupRow struct {
	ContractID         string `json:"contract_id"`
	LastIngestedLedger int64  `json:"last_ingested_ledger"`
	LastCursor         string `json:"last_cursor"`
	UpdatedAt          string `json:"updated_at"`
}

type IngestionStateBackupRow struct {
	Network            string  `json:"network"`
	LastIngestedLedger int64   `json:"last_ingested_ledger"`
	LastCursor         string  `json:"last_cursor"`
	UpdatedAt          string  `json:"updated_at"`
	LastSuccessfulPoll *string `json:"last_successful_poll,omitempty"`
}

type AuditStateBackupRow struct {
	Network               string `json:"network"`
	VerifiedThroughLedger int64  `json:"verified_through_ledger"`
	UpdatedAt             string `json:"updated_at"`
}

type AuditFindingBackupRow struct {
	ID              int64   `json:"id"`
	Network         string  `json:"network"`
	FromLedger      int64   `json:"from_ledger"`
	ToLedger        int64   `json:"to_ledger"`
	ExpectedCount   int64   `json:"expected_count"`
	ActualCount     int64   `json:"actual_count"`
	MissingIDs      string  `json:"missing_ids"`
	Status          string  `json:"status"`
	Attempts        int64   `json:"attempts"`
	LastAttemptedAt *string `json:"last_attempted_at,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
	CreatedAt       string  `json:"created_at"`
}

type ReplayStateBackupRow struct {
	ID          int64   `json:"id"`
	FromLedger  int64   `json:"from_ledger"`
	ToLedger    int64   `json:"to_ledger"`
	LastEventID string  `json:"last_event_id"`
	Processed   int64   `json:"processed"`
	Changed     int64   `json:"changed"`
	Skipped     int64   `json:"skipped"`
	StartedAt   string  `json:"started_at"`
	UpdatedAt   string  `json:"updated_at"`
	CompletedAt *string `json:"completed_at,omitempty"`
}

type BackfillStateBackupRow struct {
	ID          int64   `json:"id"`
	ContractID  string  `json:"contract_id"`
	FromLedger  int64   `json:"from_ledger"`
	ToLedger    int64   `json:"to_ledger"`
	LastLedger  int64   `json:"last_ledger"`
	StartedAt   string  `json:"started_at"`
	UpdatedAt   string  `json:"updated_at"`
	CompletedAt *string `json:"completed_at,omitempty"`
	RangesJSON  string  `json:"ranges_json"`
}

type SubscriptionBackupRow struct {
	ID           int64  `json:"id"`
	URL          string `json:"url"`
	Filters      string `json:"filters"`
	Secret       string `json:"secret"`
	Enabled      bool   `json:"enabled"`
	FailureCount int64  `json:"failure_count"`
	CreatedAt    string `json:"created_at"`
	TenantID     *int64 `json:"tenant_id,omitempty"`
}

type APIKeyBackupRow struct {
	ID        int64   `json:"id"`
	Name      string  `json:"name"`
	Prefix    string  `json:"prefix"`
	KeyHash   string  `json:"key_hash"`
	CreatedAt string  `json:"created_at"`
	RevokedAt *string `json:"revoked_at,omitempty"`
}

type TenantBackupRow struct {
	ID                  int64    `json:"id"`
	Name                string   `json:"name"`
	Wildcard            bool     `json:"wildcard"`
	IsAdmin             bool     `json:"is_admin"`
	Enabled             bool     `json:"enabled"`
	RateLimitRPS        *float64 `json:"rate_limit_rps,omitempty"`
	RateLimitBurst      *int64   `json:"rate_limit_burst,omitempty"`
	MaxWatchedContracts *int64   `json:"max_watched_contracts,omitempty"`
	CreatedAt           string   `json:"created_at"`
}

type TenantContractGrantBackupRow struct {
	TenantID   int64  `json:"tenant_id"`
	ContractID string `json:"contract_id"`
	GrantedAt  string `json:"granted_at"`
}

type TenantWatchedContractBackupRow struct {
	TenantID   int64  `json:"tenant_id"`
	Network    string `json:"network"`
	ContractID string `json:"contract_id"`
	AddedAt    string `json:"added_at"`
}

type TenantAPIKeyBackupRow struct {
	ID         int64   `json:"id"`
	TenantID   int64   `json:"tenant_id"`
	Name       string  `json:"name"`
	Prefix     string  `json:"prefix"`
	KeyHashHex string  `json:"key_hash_hex"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
	RevokedAt  *string `json:"revoked_at,omitempty"`
}

type ContractSpecBackupRow struct {
	WasmHash   string `json:"wasm_hash"`
	ContractID string `json:"contract_id"`
	SpecJSON   string `json:"spec_json"`
	FetchedAt  string `json:"fetched_at"`
}

type ContractSpecOverrideBackupRow struct {
	ContractID string `json:"contract_id"`
	SpecJSON   string `json:"spec_json"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

type ContractSpecOverrideHistoryBackupRow struct {
	ID           int64   `json:"id"`
	ContractID   string  `json:"contract_id"`
	SpecJSON     *string `json:"spec_json,omitempty"`
	APIKeyPrefix string  `json:"api_key_prefix"`
	CreatedAt    string  `json:"created_at"`
}

type EventBackupRow struct {
	Network          string  `json:"network"`
	ID               string  `json:"id"`
	ContractID       string  `json:"contract_id"`
	Ledger           int64   `json:"ledger"`
	Type             string  `json:"type"`
	TxHash           string  `json:"tx_hash"`
	TxIndex          int64   `json:"tx_index"`
	OpIndex          int64   `json:"op_index"`
	InSuccessfulCall bool    `json:"in_successful_call"`
	Topics           string  `json:"topics"`
	Value            *string `json:"value,omitempty"`
	CreatedAt        string  `json:"created_at"`
	TopicsXDR        *string `json:"topics_xdr,omitempty"`
	ValueXDR         *string `json:"value_xdr,omitempty"`
}

// BackupArchive is the self-describing, integrity-checked backup document.
type BackupArchive struct {
	FormatVersion   int                  `json:"format_version"`
	CreatedAt       time.Time            `json:"created_at"`
	Backend         string               `json:"backend"`
	SchemaVersion   uint                 `json:"schema_version"`
	SchemaDirty     bool                 `json:"schema_dirty"`
	IncludeEvents   bool                 `json:"include_events"`
	SecretMode      SecretMode           `json:"secret_mode"`
	EncryptionSalt  string               `json:"encryption_salt,omitempty"`
	EventSummary    EventSnapshotSummary `json:"event_summary"`
	Tables          BackupTables         `json:"tables"`
	IntegritySHA256 string               `json:"integrity_sha256"`
}

// RetentionConsistencyReport describes how restored checkpoints interact with
// event retention boundaries and whether chain backfill is required.
type RetentionConsistencyReport struct {
	LastIngestedLedger    uint32   `json:"last_ingested_ledger"`
	VerifiedThroughLedger uint32   `json:"verified_through_ledger"`
	StoredEventsCount     int64    `json:"stored_events_count"`
	StoredMinLedger       uint32   `json:"stored_min_ledger"`
	StoredMaxLedger       uint32   `json:"stored_max_ledger"`
	SnapshotEventsCount   int64    `json:"snapshot_events_count"`
	SnapshotMinLedger     uint32   `json:"snapshot_min_ledger"`
	SnapshotMaxLedger     uint32   `json:"snapshot_max_ledger"`
	BackfillRequired      bool     `json:"backfill_required"`
	Warnings              []string `json:"warnings,omitempty"`
}

// RestoreVerificationReport summarizes post-restore completeness and consistency checks.
type RestoreVerificationReport struct {
	VerifiedAt      time.Time                  `json:"verified_at"`
	TableCounts     map[string]int             `json:"table_counts"`
	Mismatches      []string                   `json:"mismatches,omitempty"`
	RetentionReport RetentionConsistencyReport `json:"retention_report"`
}

// RestoreResult is returned by RestoreBackup.
type RestoreResult struct {
	Backend            string                     `json:"backend"`
	TargetSchemaVer    uint                       `json:"target_schema_version"`
	ArchiveSchemaVer   uint                       `json:"archive_schema_version"`
	RestoredTables     map[string]int             `json:"restored_tables"`
	RedactedSubsLocked int                        `json:"redacted_subscriptions_disabled"`
	Duration           time.Duration              `json:"duration"`
	Verification       *RestoreVerificationReport `json:"verification,omitempty"`
}

type sqlDialect struct {
	backend string // "sqlite" or "postgres"
}

func (d sqlDialect) ph(n int) string {
	if d.backend == "postgres" {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func openSQLForBackup(databaseURL string) (*sql.DB, sqlDialect, error) {
	if isPostgresURL(databaseURL) {
		db, err := sql.Open("pgx", databaseURL)
		if err != nil {
			return nil, sqlDialect{}, fmt.Errorf("opening postgres connection: %w", err)
		}
		return db, sqlDialect{backend: "postgres"}, nil
	}
	if isSQLiteURL(databaseURL) {
		db, err := openSQLiteForMigration(databaseURL)
		if err != nil {
			return nil, sqlDialect{}, err
		}
		return db, sqlDialect{backend: "sqlite"}, nil
	}
	return nil, sqlDialect{}, fmt.Errorf("unsupported DATABASE_URL scheme for backup/restore (expected postgres:// or sqlite://)")
}

// ComputeIntegritySHA256 returns the hex-encoded SHA-256 digest of the canonical
// archive payload with IntegritySHA256 cleared.
func ComputeIntegritySHA256(a *BackupArchive) (string, error) {
	clone := *a
	clone.IntegritySHA256 = ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("marshaling archive for integrity hash: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// VerifyBackupIntegrity validates the format version and SHA-256 checksum of a
// BackupArchive.
func VerifyBackupIntegrity(a *BackupArchive) error {
	if a == nil {
		return fmt.Errorf("%w: nil archive", ErrCorruptBackup)
	}
	if a.FormatVersion != BackupFormatVersion {
		return fmt.Errorf("%w: unsupported format_version %d (expected %d)", ErrCorruptBackup, a.FormatVersion, BackupFormatVersion)
	}
	if strings.TrimSpace(a.IntegritySHA256) == "" {
		return fmt.Errorf("%w: missing integrity_sha256", ErrCorruptBackup)
	}
	expected, err := ComputeIntegritySHA256(a)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorruptBackup, err)
	}
	if !strings.EqualFold(a.IntegritySHA256, expected) {
		return fmt.Errorf("%w: integrity_sha256 mismatch (archive=%s computed=%s)", ErrCorruptBackup, a.IntegritySHA256, expected)
	}
	return nil
}

func deriveSecretKey(passphrase string, salt []byte) []byte {
	// Iterative SHA-256 key stretching (10,000 rounds) using stdlib crypto/sha256
	// so no external dependency is needed beyond the Go standard library.
	buf := make([]byte, 0, len(passphrase)+len(salt)+sha256.Size)
	buf = append(buf, []byte(passphrase)...)
	buf = append(buf, salt...)
	sum := sha256.Sum256(buf)
	for i := 0; i < 10000; i++ {
		h := sha256.New()
		h.Write(sum[:])
		h.Write([]byte(passphrase))
		h.Write(salt)
		sum = *(*[sha256.Size]byte)(h.Sum(nil))
	}
	return sum[:]
}

func encryptSecretValue(plaintext, passphrase string, salt []byte) (string, error) {
	key := deriveSecretKey(passphrase, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return encryptedSecretPrefix + base64.RawURLEncoding.EncodeToString(sealed), nil
}

func decryptSecretValue(encoded, passphrase, saltHex string) (string, error) {
	if !strings.HasPrefix(encoded, encryptedSecretPrefix) {
		return encoded, nil
	}
	if passphrase == "" {
		return "", ErrSecretPassphraseRequired
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil || len(salt) == 0 {
		return "", fmt.Errorf("%w: invalid encryption_salt", ErrSecretDecryptFailed)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, encryptedSecretPrefix))
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSecretDecryptFailed, err)
	}
	key := deriveSecretKey(passphrase, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("%w: ciphertext too short", ErrSecretDecryptFailed)
	}
	nonce := raw[:gcm.NonceSize()]
	ciphertext := raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSecretDecryptFailed, err)
	}
	return string(plain), nil
}

func normalizeSecretMode(mode SecretMode) (SecretMode, error) {
	switch mode {
	case "", SecretModeInclude:
		return SecretModeInclude, nil
	case SecretModeRedact:
		return SecretModeRedact, nil
	case SecretModeEncrypt:
		return SecretModeEncrypt, nil
	default:
		return "", fmt.Errorf("invalid secret mode %q (expected include, redact, or encrypt)", mode)
	}
}

func tableExists(ctx context.Context, db *sql.DB, d sqlDialect, tableName string) (bool, error) {
	if d.backend == "postgres" {
		var exists bool
		err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
			tableName,
		).Scan(&exists)
		return exists, err
	}
	var name string
	err := db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
		tableName,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && name == tableName, err
}

func tableHasColumn(ctx context.Context, db *sql.DB, d sqlDialect, tableName, colName string) (bool, error) {
	if d.backend == "postgres" {
		var exists bool
		err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`,
			tableName, colName,
		).Scan(&exists)
		return exists, err
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(name, colName) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// CreateBackup snapshots all irreplaceable operator configuration and checkpoint
// tables (and optionally the events table when opts.IncludeEvents is true) into
// an integrity-verified BackupArchive.
func CreateBackup(ctx context.Context, databaseURL string, opts BackupOptions) (*BackupArchive, error) {
	mode, err := normalizeSecretMode(opts.SecretMode)
	if err != nil {
		return nil, err
	}
	if mode == SecretModeEncrypt && strings.TrimSpace(opts.Passphrase) == "" {
		return nil, ErrSecretPassphraseRequired
	}

	migStatus, err := MigrateStatus(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("reading schema migration status: %w", err)
	}
	if migStatus.Dirty {
		return nil, fmt.Errorf("refusing to back up a database with a dirty migration state (version=%d)", migStatus.Version)
	}
	version := migStatus.Version
	dirty := migStatus.Dirty

	db, dialect, err := openSQLForBackup(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var salt []byte
	var saltHex string
	if mode == SecretModeEncrypt {
		salt = make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			return nil, fmt.Errorf("generating encryption salt: %w", err)
		}
		saltHex = hex.EncodeToString(salt)
	}

	transformSecret := func(raw string) (string, error) {
		switch mode {
		case SecretModeRedact:
			return redactedSecretPlaceholder, nil
		case SecretModeEncrypt:
			return encryptSecretValue(raw, opts.Passphrase, salt)
		default:
			return raw, nil
		}
	}

	archive := &BackupArchive{
		FormatVersion:  BackupFormatVersion,
		CreatedAt:      time.Now().UTC().Truncate(time.Millisecond),
		Backend:        dialect.backend,
		SchemaVersion:  version,
		SchemaDirty:    dirty,
		IncludeEvents:  opts.IncludeEvents,
		SecretMode:     mode,
		EncryptionSalt: saltHex,
		Tables: BackupTables{
			WatchedContracts:            []WatchedContractBackupRow{},
			ContractCursors:             []ContractCursorBackupRow{},
			IngestionState:              []IngestionStateBackupRow{},
			AuditState:                  []AuditStateBackupRow{},
			AuditFindings:               []AuditFindingBackupRow{},
			ReplayState:                 []ReplayStateBackupRow{},
			Subscriptions:               []SubscriptionBackupRow{},
			ContractSpecs:               []ContractSpecBackupRow{},
			ContractSpecOverrides:       []ContractSpecOverrideBackupRow{},
			ContractSpecOverrideHistory: []ContractSpecOverrideHistoryBackupRow{},
		},
	}

	if err := backupWatchedContracts(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupContractCursors(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupIngestionState(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupAuditState(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupAuditFindings(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupReplayState(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupBackfillState(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupSubscriptions(ctx, db, dialect, archive, transformSecret); err != nil {
		return nil, err
	}
	if err := backupAPIKeys(ctx, db, dialect, archive, transformSecret); err != nil {
		return nil, err
	}
	if err := backupTenantsAndGrants(ctx, db, dialect, archive, transformSecret); err != nil {
		return nil, err
	}
	if err := backupContractSpecs(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupContractSpecOverrides(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupContractSpecOverrideHistory(ctx, db, dialect, archive); err != nil {
		return nil, err
	}
	if err := backupEventSummaryAndRows(ctx, db, dialect, archive, opts.IncludeEvents); err != nil {
		return nil, err
	}

	digest, err := ComputeIntegritySHA256(archive)
	if err != nil {
		return nil, err
	}
	archive.IntegritySHA256 = digest
	return archive, nil
}

func formatTimestampVal(v any) string {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case string:
		return t
	case []byte:
		return string(t)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%v", v)
	}
}

func formatOptionalTimestampVal(v any) *string {
	if v == nil {
		return nil
	}
	s := formatTimestampVal(v)
	if s == "" {
		return nil
	}
	return &s
}

func backupWatchedContracts(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "watched_contracts")
	if err != nil || !ok {
		return err
	}
	hasNetwork, err := tableHasColumn(ctx, db, d, "watched_contracts", "network")
	if err != nil {
		return err
	}
	query := `SELECT network, contract_id, added_at FROM watched_contracts ORDER BY network, contract_id`
	if !hasNetwork {
		query = `SELECT 'default' AS network, contract_id, added_at FROM watched_contracts ORDER BY contract_id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up watched_contracts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r WatchedContractBackupRow
		var addedAt any
		if err := rows.Scan(&r.Network, &r.ContractID, &addedAt); err != nil {
			return err
		}
		r.AddedAt = formatTimestampVal(addedAt)
		a.Tables.WatchedContracts = append(a.Tables.WatchedContracts, r)
	}
	return rows.Err()
}

func backupContractCursors(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "contract_cursors")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT contract_id, last_ingested_ledger, last_cursor, updated_at FROM contract_cursors ORDER BY contract_id`)
	if err != nil {
		return fmt.Errorf("backing up contract_cursors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ContractCursorBackupRow
		var updatedAt any
		if err := rows.Scan(&r.ContractID, &r.LastIngestedLedger, &r.LastCursor, &updatedAt); err != nil {
			return err
		}
		r.UpdatedAt = formatTimestampVal(updatedAt)
		a.Tables.ContractCursors = append(a.Tables.ContractCursors, r)
	}
	return rows.Err()
}

func backupIngestionState(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "ingestion_state")
	if err != nil || !ok {
		return err
	}
	hasPoll, err := tableHasColumn(ctx, db, d, "ingestion_state", "last_successful_poll")
	if err != nil {
		return err
	}
	query := `SELECT network, last_ingested_ledger, last_cursor, updated_at, last_successful_poll FROM ingestion_state ORDER BY network`
	if !hasPoll {
		query = `SELECT network, last_ingested_ledger, last_cursor, updated_at, NULL FROM ingestion_state ORDER BY network`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up ingestion_state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r IngestionStateBackupRow
		var updatedAt, pollAt any
		if err := rows.Scan(&r.Network, &r.LastIngestedLedger, &r.LastCursor, &updatedAt, &pollAt); err != nil {
			return err
		}
		r.UpdatedAt = formatTimestampVal(updatedAt)
		r.LastSuccessfulPoll = formatOptionalTimestampVal(pollAt)
		a.Tables.IngestionState = append(a.Tables.IngestionState, r)
	}
	return rows.Err()
}

func backupAuditState(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "audit_state")
	if err != nil || !ok {
		return err
	}
	hasNetwork, err := tableHasColumn(ctx, db, d, "audit_state", "network")
	if err != nil {
		return err
	}
	query := `SELECT network, verified_through_ledger, updated_at FROM audit_state ORDER BY network`
	if !hasNetwork {
		query = `SELECT 'default' AS network, verified_through_ledger, updated_at FROM audit_state ORDER BY id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up audit_state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r AuditStateBackupRow
		var updatedAt any
		if err := rows.Scan(&r.Network, &r.VerifiedThroughLedger, &updatedAt); err != nil {
			return err
		}
		r.UpdatedAt = formatTimestampVal(updatedAt)
		a.Tables.AuditState = append(a.Tables.AuditState, r)
	}
	return rows.Err()
}

func backupAuditFindings(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "audit_findings")
	if err != nil || !ok {
		return err
	}
	hasNetwork, err := tableHasColumn(ctx, db, d, "audit_findings", "network")
	if err != nil {
		return err
	}
	query := `SELECT id, network, from_ledger, to_ledger, expected_count, actual_count, CAST(missing_ids AS TEXT), status, attempts, last_attempted_at, last_error, created_at FROM audit_findings ORDER BY id`
	if !hasNetwork {
		query = `SELECT id, 'default' AS network, from_ledger, to_ledger, expected_count, actual_count, CAST(missing_ids AS TEXT), status, attempts, last_attempted_at, last_error, created_at FROM audit_findings ORDER BY id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up audit_findings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r AuditFindingBackupRow
		var lastAttemptedAt, createdAt any
		var lastErr sql.NullString
		if err := rows.Scan(
			&r.ID, &r.Network, &r.FromLedger, &r.ToLedger,
			&r.ExpectedCount, &r.ActualCount, &r.MissingIDs, &r.Status,
			&r.Attempts, &lastAttemptedAt, &lastErr, &createdAt,
		); err != nil {
			return err
		}
		r.LastAttemptedAt = formatOptionalTimestampVal(lastAttemptedAt)
		if lastErr.Valid {
			val := lastErr.String
			r.LastError = &val
		}
		r.CreatedAt = formatTimestampVal(createdAt)
		a.Tables.AuditFindings = append(a.Tables.AuditFindings, r)
	}
	return rows.Err()
}

func backupReplayState(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "replay_state")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, from_ledger, to_ledger, last_event_id, processed, changed, skipped, started_at, updated_at, completed_at FROM replay_state ORDER BY id`)
	if err != nil {
		return fmt.Errorf("backing up replay_state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ReplayStateBackupRow
		var startedAt, updatedAt, completedAt any
		if err := rows.Scan(
			&r.ID, &r.FromLedger, &r.ToLedger, &r.LastEventID,
			&r.Processed, &r.Changed, &r.Skipped,
			&startedAt, &updatedAt, &completedAt,
		); err != nil {
			return err
		}
		r.StartedAt = formatTimestampVal(startedAt)
		r.UpdatedAt = formatTimestampVal(updatedAt)
		r.CompletedAt = formatOptionalTimestampVal(completedAt)
		a.Tables.ReplayState = append(a.Tables.ReplayState, r)
	}
	return rows.Err()
}

func backupBackfillState(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "backfill_state")
	if err != nil || !ok {
		return err
	}
	hasRanges, err := tableHasColumn(ctx, db, d, "backfill_state", "ranges_json")
	if err != nil {
		return err
	}
	query := `SELECT id, contract_id, from_ledger, to_ledger, last_ledger, started_at, updated_at, completed_at, CAST(ranges_json AS TEXT) FROM backfill_state ORDER BY id`
	if !hasRanges {
		query = `SELECT id, contract_id, from_ledger, to_ledger, last_ledger, started_at, updated_at, completed_at, '[]' FROM backfill_state ORDER BY id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up backfill_state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r BackfillStateBackupRow
		var startedAt, updatedAt, completedAt any
		if err := rows.Scan(
			&r.ID, &r.ContractID, &r.FromLedger, &r.ToLedger, &r.LastLedger,
			&startedAt, &updatedAt, &completedAt, &r.RangesJSON,
		); err != nil {
			return err
		}
		r.StartedAt = formatTimestampVal(startedAt)
		r.UpdatedAt = formatTimestampVal(updatedAt)
		r.CompletedAt = formatOptionalTimestampVal(completedAt)
		a.Tables.BackfillState = append(a.Tables.BackfillState, r)
	}
	return rows.Err()
}

func backupSubscriptions(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive, transformSecret func(string) (string, error)) error {
	ok, err := tableExists(ctx, db, d, "subscriptions")
	if err != nil || !ok {
		return err
	}
	hasTenantID, err := tableHasColumn(ctx, db, d, "subscriptions", "tenant_id")
	if err != nil {
		return err
	}
	query := `SELECT id, url, CAST(filters AS TEXT), secret, enabled, failure_count, created_at, tenant_id FROM subscriptions ORDER BY id`
	if !hasTenantID {
		query = `SELECT id, url, CAST(filters AS TEXT), secret, enabled, failure_count, created_at, NULL FROM subscriptions ORDER BY id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up subscriptions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r SubscriptionBackupRow
		var rawSecret string
		var enabledVal any
		var createdAt any
		var tenantID sql.NullInt64
		if err := rows.Scan(&r.ID, &r.URL, &r.Filters, &rawSecret, &enabledVal, &r.FailureCount, &createdAt, &tenantID); err != nil {
			return err
		}
		sec, err := transformSecret(rawSecret)
		if err != nil {
			return err
		}
		r.Secret = sec
		r.Enabled = parseBoolVal(enabledVal)
		r.CreatedAt = formatTimestampVal(createdAt)
		if tenantID.Valid {
			tid := tenantID.Int64
			r.TenantID = &tid
		}
		a.Tables.Subscriptions = append(a.Tables.Subscriptions, r)
	}
	return rows.Err()
}

func parseBoolVal(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case int:
		return b != 0
	case string:
		return b == "1" || strings.EqualFold(b, "true") || strings.EqualFold(b, "t")
	default:
		return false
	}
}

func backupAPIKeys(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive, transformSecret func(string) (string, error)) error {
	ok, err := tableExists(ctx, db, d, "api_keys")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, name, prefix, key_hash, created_at, revoked_at FROM api_keys ORDER BY id`)
	if err != nil {
		return fmt.Errorf("backing up api_keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r APIKeyBackupRow
		var rawHash string
		var createdAt, revokedAt any
		if err := rows.Scan(&r.ID, &r.Name, &r.Prefix, &rawHash, &createdAt, &revokedAt); err != nil {
			return err
		}
		h, err := transformSecret(rawHash)
		if err != nil {
			return err
		}
		r.KeyHash = h
		r.CreatedAt = formatTimestampVal(createdAt)
		r.RevokedAt = formatOptionalTimestampVal(revokedAt)
		a.Tables.APIKeys = append(a.Tables.APIKeys, r)
	}
	return rows.Err()
}

func backupTenantsAndGrants(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive, transformSecret func(string) (string, error)) error {
	ok, err := tableExists(ctx, db, d, "tenants")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, name, wildcard, is_admin, enabled, rate_limit_rps, rate_limit_burst, max_watched_contracts, created_at FROM tenants ORDER BY id`)
	if err != nil {
		return fmt.Errorf("backing up tenants: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r TenantBackupRow
		var wildcardVal, isAdminVal, enabledVal, createdAt any
		var rps sql.NullFloat64
		var burst, maxContracts sql.NullInt64
		if err := rows.Scan(&r.ID, &r.Name, &wildcardVal, &isAdminVal, &enabledVal, &rps, &burst, &maxContracts, &createdAt); err != nil {
			return err
		}
		r.Wildcard = parseBoolVal(wildcardVal)
		r.IsAdmin = parseBoolVal(isAdminVal)
		r.Enabled = parseBoolVal(enabledVal)
		if rps.Valid {
			v := rps.Float64
			r.RateLimitRPS = &v
		}
		if burst.Valid {
			v := burst.Int64
			r.RateLimitBurst = &v
		}
		if maxContracts.Valid {
			v := maxContracts.Int64
			r.MaxWatchedContracts = &v
		}
		r.CreatedAt = formatTimestampVal(createdAt)
		a.Tables.Tenants = append(a.Tables.Tenants, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if ok, err := tableExists(ctx, db, d, "tenant_contract_grants"); err != nil {
		return err
	} else if ok {
		gRows, err := db.QueryContext(ctx, `SELECT tenant_id, contract_id, granted_at FROM tenant_contract_grants ORDER BY tenant_id, contract_id`)
		if err != nil {
			return err
		}
		defer gRows.Close()
		for gRows.Next() {
			var r TenantContractGrantBackupRow
			var grantedAt any
			if err := gRows.Scan(&r.TenantID, &r.ContractID, &grantedAt); err != nil {
				return err
			}
			r.GrantedAt = formatTimestampVal(grantedAt)
			a.Tables.TenantContractGrants = append(a.Tables.TenantContractGrants, r)
		}
		if err := gRows.Err(); err != nil {
			return err
		}
	}

	if ok, err := tableExists(ctx, db, d, "tenant_watched_contracts"); err != nil {
		return err
	} else if ok {
		hasNet, err := tableHasColumn(ctx, db, d, "tenant_watched_contracts", "network")
		if err != nil {
			return err
		}
		q := `SELECT tenant_id, network, contract_id, added_at FROM tenant_watched_contracts ORDER BY tenant_id, network, contract_id`
		if !hasNet {
			q = `SELECT tenant_id, 'default' AS network, contract_id, added_at FROM tenant_watched_contracts ORDER BY tenant_id, contract_id`
		}
		wRows, err := db.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer wRows.Close()
		for wRows.Next() {
			var r TenantWatchedContractBackupRow
			var addedAt any
			if err := wRows.Scan(&r.TenantID, &r.Network, &r.ContractID, &addedAt); err != nil {
				return err
			}
			r.AddedAt = formatTimestampVal(addedAt)
			a.Tables.TenantWatchedContracts = append(a.Tables.TenantWatchedContracts, r)
		}
		if err := wRows.Err(); err != nil {
			return err
		}
	}

	if ok, err := tableExists(ctx, db, d, "tenant_api_keys"); err != nil {
		return err
	} else if ok {
		kRows, err := db.QueryContext(ctx, `SELECT id, tenant_id, name, prefix, key_hash, created_at, last_used_at, revoked_at FROM tenant_api_keys ORDER BY id`)
		if err != nil {
			return err
		}
		defer kRows.Close()
		for kRows.Next() {
			var r TenantAPIKeyBackupRow
			var rawHash []byte
			var createdAt, lastUsedAt, revokedAt any
			if err := kRows.Scan(&r.ID, &r.TenantID, &r.Name, &r.Prefix, &rawHash, &createdAt, &lastUsedAt, &revokedAt); err != nil {
				return err
			}
			sec, err := transformSecret(hex.EncodeToString(rawHash))
			if err != nil {
				return err
			}
			r.KeyHashHex = sec
			r.CreatedAt = formatTimestampVal(createdAt)
			r.LastUsedAt = formatOptionalTimestampVal(lastUsedAt)
			r.RevokedAt = formatOptionalTimestampVal(revokedAt)
			a.Tables.TenantAPIKeys = append(a.Tables.TenantAPIKeys, r)
		}
		if err := kRows.Err(); err != nil {
			return err
		}
	}
	return nil
}

func backupContractSpecs(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "contract_specs")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT wasm_hash, contract_id, CAST(spec_json AS TEXT), fetched_at FROM contract_specs ORDER BY wasm_hash`)
	if err != nil {
		return fmt.Errorf("backing up contract_specs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ContractSpecBackupRow
		var fetchedAt any
		if err := rows.Scan(&r.WasmHash, &r.ContractID, &r.SpecJSON, &fetchedAt); err != nil {
			return err
		}
		r.FetchedAt = formatTimestampVal(fetchedAt)
		a.Tables.ContractSpecs = append(a.Tables.ContractSpecs, r)
	}
	return rows.Err()
}

func backupContractSpecOverrides(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "contract_spec_overrides")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT contract_id, CAST(spec_json AS TEXT), created_at, updated_at FROM contract_spec_overrides ORDER BY contract_id`)
	if err != nil {
		return fmt.Errorf("backing up contract_spec_overrides: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ContractSpecOverrideBackupRow
		var createdAt, updatedAt any
		if err := rows.Scan(&r.ContractID, &r.SpecJSON, &createdAt, &updatedAt); err != nil {
			return err
		}
		r.CreatedAt = formatTimestampVal(createdAt)
		r.UpdatedAt = formatTimestampVal(updatedAt)
		a.Tables.ContractSpecOverrides = append(a.Tables.ContractSpecOverrides, r)
	}
	return rows.Err()
}

func backupContractSpecOverrideHistory(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive) error {
	ok, err := tableExists(ctx, db, d, "contract_spec_override_history")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, contract_id, CAST(spec_json AS TEXT), api_key_prefix, created_at FROM contract_spec_override_history ORDER BY id`)
	if err != nil {
		return fmt.Errorf("backing up contract_spec_override_history: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ContractSpecOverrideHistoryBackupRow
		var specJSON sql.NullString
		var createdAt any
		if err := rows.Scan(&r.ID, &r.ContractID, &specJSON, &r.APIKeyPrefix, &createdAt); err != nil {
			return err
		}
		if specJSON.Valid {
			val := specJSON.String
			r.SpecJSON = &val
		}
		r.CreatedAt = formatTimestampVal(createdAt)
		a.Tables.ContractSpecOverrideHistory = append(a.Tables.ContractSpecOverrideHistory, r)
	}
	return rows.Err()
}

func backupEventSummaryAndRows(ctx context.Context, db *sql.DB, d sqlDialect, a *BackupArchive, includeRows bool) error {
	ok, err := tableExists(ctx, db, d, "events")
	if err != nil || !ok {
		return err
	}
	var total int64
	var minL, maxL sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(ledger), MAX(ledger) FROM events`).Scan(&total, &minL, &maxL); err != nil {
		return fmt.Errorf("summarizing events table: %w", err)
	}
	summary := EventSnapshotSummary{
		TotalCount: total,
	}
	if minL.Valid && minL.Int64 > 0 {
		summary.MinLedger = uint32(minL.Int64)
	}
	if maxL.Valid && maxL.Int64 > 0 {
		summary.MaxLedger = uint32(maxL.Int64)
	}
	if total > 0 {
		summary.PerContractCounts = make(map[string]int64)
		cRows, err := db.QueryContext(ctx, `SELECT contract_id, COUNT(*) FROM events GROUP BY contract_id ORDER BY contract_id`)
		if err != nil {
			return err
		}
		defer cRows.Close()
		for cRows.Next() {
			var cid string
			var cnt int64
			if err := cRows.Scan(&cid, &cnt); err != nil {
				return err
			}
			summary.PerContractCounts[cid] = cnt
		}
		if err := cRows.Err(); err != nil {
			return err
		}
	}
	a.EventSummary = summary

	if !includeRows {
		return nil
	}
	hasNetwork, err := tableHasColumn(ctx, db, d, "events", "network")
	if err != nil {
		return err
	}
	query := `SELECT network, id, contract_id, ledger, type, tx_hash, tx_index, op_index, in_successful_call, CAST(topics AS TEXT), CAST(value AS TEXT), created_at, CAST(topics_xdr AS TEXT), value_xdr FROM events ORDER BY network, id`
	if !hasNetwork {
		query = `SELECT 'default' AS network, id, contract_id, ledger, type, tx_hash, tx_index, op_index, in_successful_call, CAST(topics AS TEXT), CAST(value AS TEXT), created_at, CAST(topics_xdr AS TEXT), value_xdr FROM events ORDER BY id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("backing up events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r EventBackupRow
		var inSucc any
		var val, topicsXDR, valueXDR sql.NullString
		var createdAt any
		if err := rows.Scan(
			&r.Network, &r.ID, &r.ContractID, &r.Ledger, &r.Type,
			&r.TxHash, &r.TxIndex, &r.OpIndex, &inSucc, &r.Topics,
			&val, &createdAt, &topicsXDR, &valueXDR,
		); err != nil {
			return err
		}
		r.InSuccessfulCall = parseBoolVal(inSucc)
		if val.Valid {
			v := val.String
			r.Value = &v
		}
		r.CreatedAt = formatTimestampVal(createdAt)
		if topicsXDR.Valid {
			v := topicsXDR.String
			r.TopicsXDR = &v
		}
		if valueXDR.Valid {
			v := valueXDR.String
			r.ValueXDR = &v
		}
		a.Tables.Events = append(a.Tables.Events, r)
	}
	return rows.Err()
}

// WriteBackupFile serializes a BackupArchive to disk. When archive.SecretMode
// is SecretModeInclude, the file is created with mode 0600 (owner read/write only);
// otherwise 0640 is used.
func WriteBackupFile(path string, archive *BackupArchive) error {
	if err := VerifyBackupIntegrity(archive); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(archive, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling backup archive: %w", err)
	}
	perm := os.FileMode(0640)
	if archive.SecretMode == SecretModeInclude {
		perm = 0600
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, raw, perm); err != nil {
		return fmt.Errorf("writing backup file: %w", err)
	}
	if err := os.Chmod(tmpPath, perm); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("setting backup file permissions: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("renaming temporary backup file: %w", err)
	}
	return nil
}

// ReadBackupFile loads and integrity-checks a BackupArchive from disk.
func ReadBackupFile(path string) (*BackupArchive, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading backup file: %w", err)
	}
	var archive BackupArchive
	if err := json.Unmarshal(raw, &archive); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrCorruptBackup, err)
	}
	if err := VerifyBackupIntegrity(&archive); err != nil {
		return nil, err
	}
	return &archive, nil
}

func checkDatabaseEmptyForRestore(ctx context.Context, db *sql.DB, d sqlDialect) error {
	tables := []string{
		"watched_contracts",
		"contract_cursors",
		"ingestion_state",
		"audit_state",
		"audit_findings",
		"replay_state",
		"backfill_state",
		"subscriptions",
		"api_keys",
		"tenant_contract_grants",
		"tenant_watched_contracts",
		"tenant_api_keys",
		"contract_specs",
		"contract_spec_overrides",
		"contract_spec_override_history",
		"events",
	}
	for _, tbl := range tables {
		ok, err := tableExists(ctx, db, d, tbl)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		var cnt int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)).Scan(&cnt); err != nil {
			return err
		}
		if cnt > 0 {
			return fmt.Errorf("%w: table %q contains %d row(s)", ErrRestoreTargetNotEmpty, tbl, cnt)
		}
	}
	// On Postgres, migration 0016 seeds a single 'default' row in tenants.
	if ok, err := tableExists(ctx, db, d, "tenants"); err != nil {
		return err
	} else if ok {
		var nonDefault int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenants WHERE name <> 'default'`).Scan(&nonDefault); err != nil {
			return err
		}
		if nonDefault > 0 {
			return fmt.Errorf("%w: table \"tenants\" contains %d non-default row(s)", ErrRestoreTargetNotEmpty, nonDefault)
		}
	}
	return nil
}

// RestoreBackup validates the archive integrity and schema compatibility,
// refuses to overwrite a populated database unless opts.Force is true, restores
// all archive tables inside a single atomic transaction, and optionally runs
// post-restore completeness and retention verification.
func RestoreBackup(ctx context.Context, databaseURL string, archive *BackupArchive, opts RestoreOptions) (*RestoreResult, error) {
	start := time.Now()
	if err := VerifyBackupIntegrity(archive); err != nil {
		return nil, err
	}
	if archive.SecretMode == SecretModeEncrypt && strings.TrimSpace(opts.Passphrase) == "" {
		return nil, ErrSecretPassphraseRequired
	}

	migStatus, err := MigrateStatus(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("checking target migration status: %w", err)
	}
	if migStatus.Dirty {
		return nil, fmt.Errorf("target database is in a dirty migration state (version=%d)", migStatus.Version)
	}
	targetVer := migStatus.Version
	if targetVer == 0 {
		return nil, fmt.Errorf("target database has no migrations applied (version=0); run 'sorotrail migrate up' before restoring")
	}
	if archive.SchemaVersion > targetVer {
		return nil, fmt.Errorf("%w: archive schema %d > target schema %d", ErrBackupNewerSchema, archive.SchemaVersion, targetVer)
	}

	db, dialect, err := openSQLForBackup(databaseURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if !opts.Force {
		if err := checkDatabaseEmptyForRestore(ctx, db, dialect); err != nil {
			return nil, err
		}
	}

	resolveSecret := func(stored string) (string, bool, error) {
		if archive.SecretMode == SecretModeRedact || stored == redactedSecretPlaceholder {
			return redactedSecretPlaceholder, true, nil
		}
		if archive.SecretMode == SecretModeEncrypt || strings.HasPrefix(stored, encryptedSecretPrefix) {
			plain, err := decryptSecretValue(stored, opts.Passphrase, archive.EncryptionSalt)
			return plain, false, err
		}
		return stored, false, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("beginning restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if opts.Force {
		clearOrder := []string{
			"delivery_attempts",
			"tenant_api_keys",
			"tenant_watched_contracts",
			"tenant_contract_grants",
			"subscriptions",
			"api_keys",
			"tenants",
			"contract_spec_override_history",
			"contract_spec_overrides",
			"contract_specs",
			"audit_findings",
			"audit_state",
			"replay_state",
			"backfill_state",
			"contract_cursors",
			"ingestion_state",
			"watched_contracts",
		}
		if archive.IncludeEvents {
			clearOrder = append(clearOrder, "events")
		}
		for _, tbl := range clearOrder {
			if ok, err := tableExists(ctx, db, dialect, tbl); err != nil {
				return nil, err
			} else if ok {
				if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", tbl)); err != nil {
					return nil, fmt.Errorf("clearing table %s: %w", tbl, err)
				}
			}
		}
	}

	restoredCounts := make(map[string]int)
	redactedSubsLocked := 0

	for _, r := range archive.Tables.WatchedContracts {
		net := r.Network
		if net == "" {
			net = defaultNetwork("")
		}
		q := fmt.Sprintf(`INSERT INTO watched_contracts (network, contract_id, added_at) VALUES (%s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3))
		if _, err := tx.ExecContext(ctx, q, net, r.ContractID, r.AddedAt); err != nil {
			return nil, fmt.Errorf("restoring watched_contracts (%s): %w", r.ContractID, err)
		}
		restoredCounts["watched_contracts"]++
	}

	for _, r := range archive.Tables.ContractCursors {
		q := fmt.Sprintf(`INSERT INTO contract_cursors (contract_id, last_ingested_ledger, last_cursor, updated_at) VALUES (%s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4))
		if _, err := tx.ExecContext(ctx, q, r.ContractID, r.LastIngestedLedger, r.LastCursor, r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("restoring contract_cursors (%s): %w", r.ContractID, err)
		}
		restoredCounts["contract_cursors"]++
	}

	for _, r := range archive.Tables.IngestionState {
		net := r.Network
		if net == "" {
			net = defaultNetwork("")
		}
		q := fmt.Sprintf(`INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, updated_at, last_successful_poll) VALUES (%s, %s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5))
		if _, err := tx.ExecContext(ctx, q, net, r.LastIngestedLedger, r.LastCursor, r.UpdatedAt, r.LastSuccessfulPoll); err != nil {
			return nil, fmt.Errorf("restoring ingestion_state (%s): %w", net, err)
		}
		restoredCounts["ingestion_state"]++
	}

	for _, r := range archive.Tables.AuditState {
		net := r.Network
		if net == "" {
			net = defaultNetwork("")
		}
		q := fmt.Sprintf(`INSERT INTO audit_state (network, verified_through_ledger, updated_at) VALUES (%s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3))
		if _, err := tx.ExecContext(ctx, q, net, r.VerifiedThroughLedger, r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("restoring audit_state (%s): %w", net, err)
		}
		restoredCounts["audit_state"]++
	}

	for _, r := range archive.Tables.AuditFindings {
		net := r.Network
		if net == "" {
			net = defaultNetwork("")
		}
		q := fmt.Sprintf(`INSERT INTO audit_findings (id, network, from_ledger, to_ledger, expected_count, actual_count, missing_ids, status, attempts, last_attempted_at, last_error, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5), dialect.ph(6),
			dialect.ph(7), dialect.ph(8), dialect.ph(9), dialect.ph(10), dialect.ph(11), dialect.ph(12))
		if _, err := tx.ExecContext(ctx, q,
			r.ID, net, r.FromLedger, r.ToLedger, r.ExpectedCount, r.ActualCount,
			r.MissingIDs, r.Status, r.Attempts, r.LastAttemptedAt, r.LastError, r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("restoring audit_findings (%d): %w", r.ID, err)
		}
		restoredCounts["audit_findings"]++
	}

	for _, r := range archive.Tables.ReplayState {
		q := fmt.Sprintf(`INSERT INTO replay_state (id, from_ledger, to_ledger, last_event_id, processed, changed, skipped, started_at, updated_at, completed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5),
			dialect.ph(6), dialect.ph(7), dialect.ph(8), dialect.ph(9), dialect.ph(10))
		if _, err := tx.ExecContext(ctx, q,
			r.ID, r.FromLedger, r.ToLedger, r.LastEventID,
			r.Processed, r.Changed, r.Skipped, r.StartedAt, r.UpdatedAt, r.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("restoring replay_state (%d): %w", r.ID, err)
		}
		restoredCounts["replay_state"]++
	}

	if ok, err := tableExists(ctx, db, dialect, "backfill_state"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.BackfillState {
			rangesJSON := r.RangesJSON
			if rangesJSON == "" {
				rangesJSON = "[]"
			}
			q := fmt.Sprintf(`INSERT INTO backfill_state (id, contract_id, from_ledger, to_ledger, last_ledger, started_at, updated_at, completed_at, ranges_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5),
				dialect.ph(6), dialect.ph(7), dialect.ph(8), dialect.ph(9))
			if _, err := tx.ExecContext(ctx, q,
				r.ID, r.ContractID, r.FromLedger, r.ToLedger, r.LastLedger,
				r.StartedAt, r.UpdatedAt, r.CompletedAt, rangesJSON,
			); err != nil {
				return nil, fmt.Errorf("restoring backfill_state (%d): %w", r.ID, err)
			}
			restoredCounts["backfill_state"]++
		}
	}

	if ok, err := tableExists(ctx, db, dialect, "tenants"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.Tenants {
			q := fmt.Sprintf(`INSERT INTO tenants (id, name, wildcard, is_admin, enabled, rate_limit_rps, rate_limit_burst, max_watched_contracts, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name) DO UPDATE SET wildcard = EXCLUDED.wildcard, is_admin = EXCLUDED.is_admin, enabled = EXCLUDED.enabled`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5),
				dialect.ph(6), dialect.ph(7), dialect.ph(8), dialect.ph(9))
			if _, err := tx.ExecContext(ctx, q,
				r.ID, r.Name, r.Wildcard, r.IsAdmin, r.Enabled,
				r.RateLimitRPS, r.RateLimitBurst, r.MaxWatchedContracts, r.CreatedAt,
			); err != nil {
				return nil, fmt.Errorf("restoring tenants (%s): %w", r.Name, err)
			}
			restoredCounts["tenants"]++
		}
	}

	if ok, err := tableExists(ctx, db, dialect, "tenant_contract_grants"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.TenantContractGrants {
			q := fmt.Sprintf(`INSERT INTO tenant_contract_grants (tenant_id, contract_id, granted_at) VALUES (%s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3))
			if _, err := tx.ExecContext(ctx, q, r.TenantID, r.ContractID, r.GrantedAt); err != nil {
				return nil, fmt.Errorf("restoring tenant_contract_grants: %w", err)
			}
			restoredCounts["tenant_contract_grants"]++
		}
	}

	if ok, err := tableExists(ctx, db, dialect, "tenant_watched_contracts"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.TenantWatchedContracts {
			net := r.Network
			if net == "" {
				net = defaultNetwork("")
			}
			q := fmt.Sprintf(`INSERT INTO tenant_watched_contracts (tenant_id, network, contract_id, added_at) VALUES (%s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4))
			if _, err := tx.ExecContext(ctx, q, r.TenantID, net, r.ContractID, r.AddedAt); err != nil {
				return nil, fmt.Errorf("restoring tenant_watched_contracts: %w", err)
			}
			restoredCounts["tenant_watched_contracts"]++
		}
	}

	if ok, err := tableExists(ctx, db, dialect, "tenant_api_keys"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.TenantAPIKeys {
			resolvedHex, isRedacted, err := resolveSecret(r.KeyHashHex)
			if err != nil {
				return nil, err
			}
			var hashBytes []byte
			if isRedacted {
				hashBytes = []byte(redactedSecretPlaceholder)
			} else {
				hashBytes, err = hex.DecodeString(resolvedHex)
				if err != nil {
					return nil, fmt.Errorf("decoding tenant_api_keys hash (%s): %w", r.Prefix, err)
				}
			}
			q := fmt.Sprintf(`INSERT INTO tenant_api_keys (id, tenant_id, name, prefix, key_hash, created_at, last_used_at, revoked_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4),
				dialect.ph(5), dialect.ph(6), dialect.ph(7), dialect.ph(8))
			if _, err := tx.ExecContext(ctx, q,
				r.ID, r.TenantID, r.Name, r.Prefix, hashBytes, r.CreatedAt, r.LastUsedAt, r.RevokedAt,
			); err != nil {
				return nil, fmt.Errorf("restoring tenant_api_keys (%s): %w", r.Prefix, err)
			}
			restoredCounts["tenant_api_keys"]++
		}
	}

	hasSubTenant, err := tableHasColumn(ctx, db, dialect, "subscriptions", "tenant_id")
	if err != nil {
		return nil, err
	}
	for _, r := range archive.Tables.Subscriptions {
		secret, isRedacted, err := resolveSecret(r.Secret)
		if err != nil {
			return nil, err
		}
		enabled := r.Enabled
		if isRedacted && enabled {
			enabled = false
			redactedSubsLocked++
		}
		var enabledArg any = enabled
		if dialect.backend == "sqlite" {
			if enabled {
				enabledArg = 1
			} else {
				enabledArg = 0
			}
		}
		if hasSubTenant {
			q := fmt.Sprintf(`INSERT INTO subscriptions (id, url, filters, secret, enabled, failure_count, created_at, tenant_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4),
				dialect.ph(5), dialect.ph(6), dialect.ph(7), dialect.ph(8))
			if _, err := tx.ExecContext(ctx, q, r.ID, r.URL, r.Filters, secret, enabledArg, r.FailureCount, r.CreatedAt, r.TenantID); err != nil {
				return nil, fmt.Errorf("restoring subscriptions (%d): %w", r.ID, err)
			}
		} else {
			q := fmt.Sprintf(`INSERT INTO subscriptions (id, url, filters, secret, enabled, failure_count, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4),
				dialect.ph(5), dialect.ph(6), dialect.ph(7))
			if _, err := tx.ExecContext(ctx, q, r.ID, r.URL, r.Filters, secret, enabledArg, r.FailureCount, r.CreatedAt); err != nil {
				return nil, fmt.Errorf("restoring subscriptions (%d): %w", r.ID, err)
			}
		}
		restoredCounts["subscriptions"]++
	}

	if ok, err := tableExists(ctx, db, dialect, "api_keys"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.APIKeys {
			keyHash, _, err := resolveSecret(r.KeyHash)
			if err != nil {
				return nil, err
			}
			q := fmt.Sprintf(`INSERT INTO api_keys (id, name, prefix, key_hash, created_at, revoked_at) VALUES (%s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5), dialect.ph(6))
			if _, err := tx.ExecContext(ctx, q, r.ID, r.Name, r.Prefix, keyHash, r.CreatedAt, r.RevokedAt); err != nil {
				return nil, fmt.Errorf("restoring api_keys (%s): %w", r.Prefix, err)
			}
			restoredCounts["api_keys"]++
		}
	}

	for _, r := range archive.Tables.ContractSpecs {
		q := fmt.Sprintf(`INSERT INTO contract_specs (wasm_hash, contract_id, spec_json, fetched_at) VALUES (%s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4))
		if _, err := tx.ExecContext(ctx, q, r.WasmHash, r.ContractID, r.SpecJSON, r.FetchedAt); err != nil {
			return nil, fmt.Errorf("restoring contract_specs (%s): %w", r.WasmHash, err)
		}
		restoredCounts["contract_specs"]++
	}

	for _, r := range archive.Tables.ContractSpecOverrides {
		q := fmt.Sprintf(`INSERT INTO contract_spec_overrides (contract_id, spec_json, created_at, updated_at) VALUES (%s, %s, %s, %s)`,
			dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4))
		if _, err := tx.ExecContext(ctx, q, r.ContractID, r.SpecJSON, r.CreatedAt, r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("restoring contract_spec_overrides (%s): %w", r.ContractID, err)
		}
		restoredCounts["contract_spec_overrides"]++
	}

	if ok, err := tableExists(ctx, db, dialect, "contract_spec_override_history"); err != nil {
		return nil, err
	} else if ok {
		for _, r := range archive.Tables.ContractSpecOverrideHistory {
			q := fmt.Sprintf(`INSERT INTO contract_spec_override_history (id, contract_id, spec_json, api_key_prefix, created_at) VALUES (%s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5))
			if _, err := tx.ExecContext(ctx, q, r.ID, r.ContractID, r.SpecJSON, r.APIKeyPrefix, r.CreatedAt); err != nil {
				return nil, fmt.Errorf("restoring contract_spec_override_history (%d): %w", r.ID, err)
			}
			restoredCounts["contract_spec_override_history"]++
		}
	}

	if dialect.backend == "postgres" {
		seqTables := []string{
			"audit_findings",
			"replay_state",
			"backfill_state",
			"tenants",
			"tenant_api_keys",
			"subscriptions",
			"api_keys",
			"contract_spec_override_history",
		}
		for _, tbl := range seqTables {
			if ok, err := tableExists(ctx, db, dialect, tbl); err != nil {
				return nil, err
			} else if ok {
				seqSQL := fmt.Sprintf(
					`SELECT CASE WHEN pg_get_serial_sequence('%s', 'id') IS NOT NULL THEN setval(pg_get_serial_sequence('%s', 'id'), COALESCE((SELECT MAX(id) FROM %s), 1), (SELECT COUNT(*) > 0 FROM %s)) ELSE 0 END`,
					tbl, tbl, tbl, tbl,
				)
				if _, err := tx.ExecContext(ctx, seqSQL); err != nil {
					return nil, fmt.Errorf("resetting sequence for %s: %w", tbl, err)
				}
			}
		}
	}

	if archive.IncludeEvents {
		for _, r := range archive.Tables.Events {
			net := r.Network
			if net == "" {
				net = defaultNetwork("")
			}
			var inSuccArg any = r.InSuccessfulCall
			var topicsArg any = r.Topics
			var valueArg any = r.Value
			if dialect.backend == "sqlite" {
				if r.InSuccessfulCall {
					inSuccArg = 1
				} else {
					inSuccArg = 0
				}
				topicsArg = []byte(r.Topics)
				if r.Value != nil {
					valueArg = []byte(*r.Value)
				} else {
					valueArg = nil
				}
			}
			q := fmt.Sprintf(`INSERT INTO events (network, id, contract_id, ledger, type, tx_hash, tx_index, op_index, in_successful_call, topics, value, created_at, topics_xdr, value_xdr) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
				dialect.ph(1), dialect.ph(2), dialect.ph(3), dialect.ph(4), dialect.ph(5), dialect.ph(6), dialect.ph(7),
				dialect.ph(8), dialect.ph(9), dialect.ph(10), dialect.ph(11), dialect.ph(12), dialect.ph(13), dialect.ph(14))
			if _, err := tx.ExecContext(ctx, q,
				net, r.ID, r.ContractID, r.Ledger, r.Type, r.TxHash, r.TxIndex, r.OpIndex,
				inSuccArg, topicsArg, valueArg, r.CreatedAt, r.TopicsXDR, r.ValueXDR,
			); err != nil {
				return nil, fmt.Errorf("restoring event (%s): %w", r.ID, err)
			}
			restoredCounts["events"]++
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing restore transaction: %w", err)
	}

	res := &RestoreResult{
		Backend:            dialect.backend,
		TargetSchemaVer:    targetVer,
		ArchiveSchemaVer:   archive.SchemaVersion,
		RestoredTables:     restoredCounts,
		RedactedSubsLocked: redactedSubsLocked,
		Duration:           time.Since(start),
	}

	if opts.Verify {
		vReport, err := VerifyBackupRestore(ctx, databaseURL, archive, opts)
		if err != nil {
			return res, err
		}
		res.Verification = vReport
	}
	return res, nil
}

// VerifyBackupRestore inspects the live database after a restore and asserts
// that every table's row count, key checkpoints, contract specs/overrides,
// subscriptions, and events match the BackupArchive. It also evaluates how the
// restored checkpoints interact with event retention boundaries.
func VerifyBackupRestore(ctx context.Context, databaseURL string, expected *BackupArchive, opts RestoreOptions) (*RestoreVerificationReport, error) {
	actual, err := CreateBackup(ctx, databaseURL, BackupOptions{
		IncludeEvents: expected.IncludeEvents,
		SecretMode:    SecretModeInclude,
	})
	if err != nil {
		return nil, fmt.Errorf("snapshotting restored database for verification: %w", err)
	}

	report := &RestoreVerificationReport{
		VerifiedAt:  time.Now().UTC(),
		TableCounts: make(map[string]int),
	}

	checkSliceCount := func(table string, got, want int) {
		report.TableCounts[table] = got
		if got != want {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("%s count mismatch: got %d, want %d", table, got, want))
		}
	}

	checkSliceCount("watched_contracts", len(actual.Tables.WatchedContracts), len(expected.Tables.WatchedContracts))
	checkSliceCount("contract_cursors", len(actual.Tables.ContractCursors), len(expected.Tables.ContractCursors))
	checkSliceCount("ingestion_state", len(actual.Tables.IngestionState), len(expected.Tables.IngestionState))
	checkSliceCount("audit_state", len(actual.Tables.AuditState), len(expected.Tables.AuditState))
	checkSliceCount("audit_findings", len(actual.Tables.AuditFindings), len(expected.Tables.AuditFindings))
	checkSliceCount("replay_state", len(actual.Tables.ReplayState), len(expected.Tables.ReplayState))
	checkSliceCount("backfill_state", len(actual.Tables.BackfillState), len(expected.Tables.BackfillState))
	checkSliceCount("subscriptions", len(actual.Tables.Subscriptions), len(expected.Tables.Subscriptions))
	checkSliceCount("api_keys", len(actual.Tables.APIKeys), len(expected.Tables.APIKeys))
	checkSliceCount("tenants", len(actual.Tables.Tenants), len(expected.Tables.Tenants))
	checkSliceCount("tenant_contract_grants", len(actual.Tables.TenantContractGrants), len(expected.Tables.TenantContractGrants))
	checkSliceCount("tenant_watched_contracts", len(actual.Tables.TenantWatchedContracts), len(expected.Tables.TenantWatchedContracts))
	checkSliceCount("tenant_api_keys", len(actual.Tables.TenantAPIKeys), len(expected.Tables.TenantAPIKeys))
	checkSliceCount("contract_specs", len(actual.Tables.ContractSpecs), len(expected.Tables.ContractSpecs))
	checkSliceCount("contract_spec_overrides", len(actual.Tables.ContractSpecOverrides), len(expected.Tables.ContractSpecOverrides))
	checkSliceCount("contract_spec_override_history", len(actual.Tables.ContractSpecOverrideHistory), len(expected.Tables.ContractSpecOverrideHistory))
	if expected.IncludeEvents {
		checkSliceCount("events", len(actual.Tables.Events), len(expected.Tables.Events))
	}

	for i := range expected.Tables.WatchedContracts {
		if i >= len(actual.Tables.WatchedContracts) {
			break
		}
		if actual.Tables.WatchedContracts[i].ContractID != expected.Tables.WatchedContracts[i].ContractID {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("watched_contracts[%d] contract_id: got %s, want %s",
				i, actual.Tables.WatchedContracts[i].ContractID, expected.Tables.WatchedContracts[i].ContractID))
		}
	}

	for i := range expected.Tables.ContractCursors {
		if i >= len(actual.Tables.ContractCursors) {
			break
		}
		got := actual.Tables.ContractCursors[i]
		want := expected.Tables.ContractCursors[i]
		if got.ContractID != want.ContractID || got.LastIngestedLedger != want.LastIngestedLedger || got.LastCursor != want.LastCursor {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("contract_cursors[%d] mismatch: got %+v, want %+v", i, got, want))
		}
	}

	var maxIngested uint32
	for i := range expected.Tables.IngestionState {
		if i >= len(actual.Tables.IngestionState) {
			break
		}
		got := actual.Tables.IngestionState[i]
		want := expected.Tables.IngestionState[i]
		if got.Network != want.Network || got.LastIngestedLedger != want.LastIngestedLedger || got.LastCursor != want.LastCursor {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("ingestion_state[%d] mismatch: got %+v, want %+v", i, got, want))
		}
		if uint32(got.LastIngestedLedger) > maxIngested {
			maxIngested = uint32(got.LastIngestedLedger)
		}
	}

	var maxVerified uint32
	for i := range expected.Tables.AuditState {
		if i >= len(actual.Tables.AuditState) {
			break
		}
		got := actual.Tables.AuditState[i]
		want := expected.Tables.AuditState[i]
		if got.Network != want.Network || got.VerifiedThroughLedger != want.VerifiedThroughLedger {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("audit_state[%d] mismatch: got %+v, want %+v", i, got, want))
		}
		if uint32(got.VerifiedThroughLedger) > maxVerified {
			maxVerified = uint32(got.VerifiedThroughLedger)
		}
	}

	for i := range expected.Tables.Subscriptions {
		if i >= len(actual.Tables.Subscriptions) {
			break
		}
		got := actual.Tables.Subscriptions[i]
		want := expected.Tables.Subscriptions[i]
		if got.URL != want.URL || got.FailureCount != want.FailureCount {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("subscriptions[%d] metadata mismatch: got url=%s failures=%d, want url=%s failures=%d",
				i, got.URL, got.FailureCount, want.URL, want.FailureCount))
		}
		if expected.SecretMode == SecretModeRedact {
			if got.Enabled {
				report.Mismatches = append(report.Mismatches, fmt.Sprintf("subscriptions[%d] should be disabled after redacted restore", i))
			}
		} else if expected.SecretMode == SecretModeEncrypt {
			dec, err := decryptSecretValue(want.Secret, opts.Passphrase, expected.EncryptionSalt)
			if err != nil || got.Secret != dec {
				report.Mismatches = append(report.Mismatches, fmt.Sprintf("subscriptions[%d] decrypted secret mismatch", i))
			}
		} else if got.Secret != want.Secret {
			report.Mismatches = append(report.Mismatches, fmt.Sprintf("subscriptions[%d] secret mismatch", i))
		}
	}

	ret := RetentionConsistencyReport{
		LastIngestedLedger:    maxIngested,
		VerifiedThroughLedger: maxVerified,
		StoredEventsCount:     actual.EventSummary.TotalCount,
		StoredMinLedger:       actual.EventSummary.MinLedger,
		StoredMaxLedger:       actual.EventSummary.MaxLedger,
		SnapshotEventsCount:   expected.EventSummary.TotalCount,
		SnapshotMinLedger:     expected.EventSummary.MinLedger,
		SnapshotMaxLedger:     expected.EventSummary.MaxLedger,
	}

	if !expected.IncludeEvents && expected.EventSummary.TotalCount > 0 {
		ret.BackfillRequired = true
		ret.Warnings = append(ret.Warnings, fmt.Sprintf(
			"config-only backup restored (events table has %d rows vs %d in snapshot covering ledgers [%d..%d]); run 'sorotrail backfill --from %d --to %d' to reconstruct historical events from chain, or start 'sorotrail serve' to resume forward from ledger %d",
			actual.EventSummary.TotalCount, expected.EventSummary.TotalCount,
			expected.EventSummary.MinLedger, expected.EventSummary.MaxLedger,
			expected.EventSummary.MinLedger, expected.EventSummary.MaxLedger,
			maxIngested,
		))
	}
	if opts.RetentionMinLedger > 0 {
		if expected.EventSummary.MinLedger > 0 && expected.EventSummary.MinLedger < opts.RetentionMinLedger {
			ret.Warnings = append(ret.Warnings, fmt.Sprintf(
				"retention floor ledger %d is newer than snapshot earliest event ledger %d; ledgers [%d..%d) fall outside the active retention window and will be pruned or dropped if partitions were already detached",
				opts.RetentionMinLedger, expected.EventSummary.MinLedger,
				expected.EventSummary.MinLedger, opts.RetentionMinLedger,
			))
		}
		if maxVerified > 0 && maxVerified < opts.RetentionMinLedger {
			ret.Warnings = append(ret.Warnings, fmt.Sprintf(
				"audit_state.verified_through_ledger (%d) precedes retention floor ledger %d; gap audit will fast-forward to the retention floor unless historical partitions are restored first",
				maxVerified, opts.RetentionMinLedger,
			))
		}
	}
	sort.Strings(ret.Warnings)
	report.RetentionReport = ret

	if len(report.Mismatches) > 0 {
		return report, fmt.Errorf("post-restore verification failed with %d mismatch(es): %s",
			len(report.Mismatches), strings.Join(report.Mismatches, "; "))
	}
	return report, nil
}
