package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
)

// ColumnProtectionKind classifies how a database column is protected at rest.
type ColumnProtectionKind string

const (
	// ProtectionEncryptedAEAD indicates the column is encrypted at rest using
	// AES-256-GCM with a per-write random 96-bit nonce and versioned key tag
	// (`enc:<version>:<base64url(nonce||ciphertext||tag)>`) because the
	// application must recover the plaintext at runtime.
	ProtectionEncryptedAEAD ColumnProtectionKind = "encrypted-aead-aes-256-gcm"

	// ProtectionOneWayHashed indicates the column stores a one-way cryptographic
	// digest (bcrypt or SHA-256) and never needs to be decrypted.
	ProtectionOneWayHashed ColumnProtectionKind = "hashed-one-way"

	// ProtectionPlaintextLookupOrPublic indicates the column is intentionally
	// stored in plaintext because it is either a non-secret indexed lookup
	// handle (API key prefix) or public on-chain data.
	ProtectionPlaintextLookupOrPublic ColumnProtectionKind = "plaintext"
)

// ColumnProtectionPolicy documents the at-rest protection posture and rationale
// for a security-relevant database column (#1225).
type ColumnProtectionPolicy struct {
	Table      string
	Column     string
	Protection ColumnProtectionKind
	Rationale  string
}

// SensitiveColumnInventory returns the authoritative classification of
// security-relevant columns across the SoroTrail schema (#1225).
func SensitiveColumnInventory() []ColumnProtectionPolicy {
	return []ColumnProtectionPolicy{
		{
			Table:      "subscriptions",
			Column:     "secret",
			Protection: ProtectionEncryptedAEAD,
			Rationale:  "Webhook HMAC-SHA256 signing secret must be recovered in plaintext by the delivery worker to sign outbound event payloads; encrypted at rest with AES-256-GCM.",
		},
		{
			Table:      "api_keys",
			Column:     "key_hash",
			Protection: ProtectionOneWayHashed,
			Rationale:  "Operator API key secret is stored as a one-way bcrypt hash and verified at authentication time; plaintext is never persisted.",
		},
		{
			Table:      "tenant_api_keys",
			Column:     "key_hash",
			Protection: ProtectionOneWayHashed,
			Rationale:  "Tenant API key secret is stored as a one-way SHA-256 digest and compared in constant time; plaintext is never persisted.",
		},
		{
			Table:      "idempotency_keys",
			Column:     "request_hash",
			Protection: ProtectionOneWayHashed,
			Rationale:  "Idempotent request payload is stored as a one-way SHA-256 digest rather than raw request bodies.",
		},
		{
			Table:      "api_keys",
			Column:     "prefix",
			Protection: ProtectionPlaintextLookupOrPublic,
			Rationale:  "Non-secret 16-char key prefix kept in plaintext so authentication can locate the candidate row via an indexed WHERE prefix = $1 lookup before verifying key_hash.",
		},
		{
			Table:      "tenant_api_keys",
			Column:     "prefix",
			Protection: ProtectionPlaintextLookupOrPublic,
			Rationale:  "Non-secret key prefix kept in plaintext for indexed candidate lookup before constant-time SHA-256 verification.",
		},
	}
}

var (
	// ErrInvalidEncryptionKeyConfig is returned when ENCRYPTION_KEYS or
	// ENCRYPTION_KEY is malformed, uses an invalid key version identifier,
	// duplicates a version ID, or decodes to a key length other than 32 bytes.
	ErrInvalidEncryptionKeyConfig = errors.New("invalid field encryption key configuration")

	// ErrEncryptionKeyRequired is returned when the store encounters an
	// encrypted column value (`enc:<version>:...`) while no FieldEncryptor is
	// configured.
	ErrEncryptionKeyRequired = errors.New("field encryption key is required to read encrypted column")

	// ErrUnknownEncryptionKeyVersion is returned when an encrypted column
	// references a key version ID that is not present in the configured keyring.
	ErrUnknownEncryptionKeyVersion = errors.New("unknown field encryption key version")

	// ErrEncryptionAuthFailed is returned when AES-256-GCM authentication tag
	// verification fails (corrupted ciphertext, tampered header, or wrong key).
	ErrEncryptionAuthFailed = errors.New("field encryption authentication failed")

	// ErrPlaintextNotAllowed is returned when plaintext fallback is disabled
	// and a column contains an unencrypted value.
	ErrPlaintextNotAllowed = errors.New("unencrypted plaintext value is not allowed")
)

const (
	encryptedValuePrefix = "enc:"
	defaultKeyVersion    = "v1"
	aes256KeyBytes       = 32
)

var validKeyVersionRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$`)

// FieldEncryptor performs versioned authenticated encryption and decryption
// (AES-256-GCM with per-write 96-bit CSPRNG nonces) for sensitive database
// columns at rest.
type FieldEncryptor struct {
	activeVersion          string
	versions               []string
	aeads                  map[string]cipher.AEAD
	allowPlaintextFallback bool
}

// ParseFieldEncryptor parses a keyring specification into a FieldEncryptor.
//
// Supported formats:
//   - Versioned keyring (`ENCRYPTION_KEYS`):
//     "v2:<32-byte-base64-or-hex-key>,v1:<32-byte-base64-or-hex-key>"
//     The first entry (`v2`) is the active key used for all new writes; all
//     entries remain active for decryption during key rotation.
//   - Single unversioned key (`ENCRYPTION_KEY`):
//     "<32-byte-base64-or-hex-key>" (assigned version "v1").
//
// Raw key material is never echoed in any returned error message.
func ParseFieldEncryptor(spec string) (*FieldEncryptor, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("%w: empty keyring specification", ErrInvalidEncryptionKeyConfig)
	}

	entries := strings.Split(spec, ",")
	aeads := make(map[string]cipher.AEAD, len(entries))
	var versions []string
	var activeVersion string

	for idx, rawEntry := range entries {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			return nil, fmt.Errorf("%w: empty key entry at position %d", ErrInvalidEncryptionKeyConfig, idx+1)
		}

		version := defaultKeyVersion
		encodedKey := entry
		if strings.Contains(entry, ":") {
			v, k, _ := strings.Cut(entry, ":")
			version = strings.TrimSpace(v)
			encodedKey = strings.TrimSpace(k)
		} else if len(entries) > 1 {
			return nil, fmt.Errorf("%w: multi-key entry at position %d must specify <version>:<key>", ErrInvalidEncryptionKeyConfig, idx+1)
		}

		if !validKeyVersionRe.MatchString(version) {
			return nil, fmt.Errorf("%w: invalid key version identifier at position %d (want 1..32 alphanumeric/hyphen/underscore chars)", ErrInvalidEncryptionKeyConfig, idx+1)
		}
		if _, dup := aeads[version]; dup {
			return nil, fmt.Errorf("%w: duplicate key version %q", ErrInvalidEncryptionKeyConfig, version)
		}

		keyBytes, err := decodeAES256Key(encodedKey)
		if err != nil {
			return nil, fmt.Errorf("%w: key version %q: %v", ErrInvalidEncryptionKeyConfig, version, err)
		}

		block, err := aes.NewCipher(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: key version %q: initializing AES cipher", ErrInvalidEncryptionKeyConfig, version)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("%w: key version %q: initializing GCM", ErrInvalidEncryptionKeyConfig, version)
		}

		if activeVersion == "" {
			activeVersion = version
		}
		versions = append(versions, version)
		aeads[version] = gcm
	}

	return &FieldEncryptor{
		activeVersion:          activeVersion,
		versions:               versions,
		aeads:                  aeads,
		allowPlaintextFallback: true,
	}, nil
}

func decodeAES256Key(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, errors.New("key material is empty")
	}
	// 64 hex characters -> 32 bytes.
	if len(encoded) == aes256KeyBytes*2 {
		if decoded, err := hex.DecodeString(encoded); err == nil && len(decoded) == aes256KeyBytes {
			return decoded, nil
		}
	}
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := enc.DecodeString(encoded); err == nil {
			if len(decoded) != aes256KeyBytes {
				return nil, fmt.Errorf("decoded key length is %d bytes (want %d bytes for AES-256)", len(decoded), aes256KeyBytes)
			}
			return decoded, nil
		}
	}
	return nil, errors.New("key must be a 32-byte value encoded as base64 or 64-char hex")
}

// ActiveVersion returns the key version ID used for new encryptions.
func (e *FieldEncryptor) ActiveVersion() string {
	if e == nil {
		return ""
	}
	return e.activeVersion
}

// KeyVersions returns all key version IDs loaded in the keyring in priority order.
func (e *FieldEncryptor) KeyVersions() []string {
	if e == nil {
		return nil
	}
	out := make([]string, len(e.versions))
	copy(out, e.versions)
	return out
}

// SetAllowPlaintextFallback controls whether DecryptString accepts legacy
// unencrypted values that lack the `enc:<version>:` prefix. Defaults to true
// so deployments upgrading in place can read pre-existing rows while
// RotateSubscriptionsEncryption migrates them.
func (e *FieldEncryptor) SetAllowPlaintextFallback(allow bool) {
	if e == nil {
		return
	}
	e.allowPlaintextFallback = allow
}

// IsEncryptedValue reports whether stored carries the `enc:<version>:<payload>`
// envelope written by FieldEncryptor.
func IsEncryptedValue(stored string) bool {
	if !strings.HasPrefix(stored, encryptedValuePrefix) {
		return false
	}
	rest := strings.TrimPrefix(stored, encryptedValuePrefix)
	ver, payload, ok := strings.Cut(rest, ":")
	return ok && validKeyVersionRe.MatchString(ver) && payload != ""
}

// EncryptedValueVersion extracts the key version ID from an encrypted column
// value (`enc:<version>:<payload>`), returning ("", false) if stored is not in
// encrypted envelope format.
func EncryptedValueVersion(stored string) (string, bool) {
	if !strings.HasPrefix(stored, encryptedValuePrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(stored, encryptedValuePrefix)
	ver, payload, ok := strings.Cut(rest, ":")
	if !ok || !validKeyVersionRe.MatchString(ver) || payload == "" {
		return "", false
	}
	return ver, true
}

func fieldAAD(version string) []byte {
	return []byte("sorotrail:field:" + version)
}

// EncryptString encrypts plaintext with the active AES-256-GCM key and a fresh
// 96-bit cryptographic random nonce. Empty strings are returned unchanged.
// If e is nil, plaintext is returned unchanged.
func (e *FieldEncryptor) EncryptString(plaintext string) (string, error) {
	if plaintext == "" || e == nil {
		return plaintext, nil
	}
	aead, ok := e.aeads[e.activeVersion]
	if !ok {
		return "", fmt.Errorf("%w: active key version %q not initialized", ErrInvalidEncryptionKeyConfig, e.activeVersion)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generating random nonce for field encryption: %w", err)
	}
	sealed := aead.Seal(nonce, nonce, []byte(plaintext), fieldAAD(e.activeVersion))
	encoded := base64.RawURLEncoding.EncodeToString(sealed)
	return encryptedValuePrefix + e.activeVersion + ":" + encoded, nil
}

// DecryptString decrypts a stored column value. If stored carries an
// `enc:<version>:<payload>` envelope, it is authenticated and decrypted with
// the matching key version from the keyring. Even when e is nil, an encrypted
// envelope fails closed with ErrEncryptionKeyRequired so ciphertext is never
// silently served as a plaintext secret.
func (e *FieldEncryptor) DecryptString(stored string) (string, error) {
	if stored == "" {
		return "", nil
	}
	if strings.HasPrefix(stored, encryptedValuePrefix) {
		rest := strings.TrimPrefix(stored, encryptedValuePrefix)
		version, encodedPayload, ok := strings.Cut(rest, ":")
		if !ok || !validKeyVersionRe.MatchString(version) || encodedPayload == "" {
			return "", fmt.Errorf("%w: malformed encrypted column envelope", ErrEncryptionAuthFailed)
		}
		if e == nil {
			return "", fmt.Errorf("%w: row requires key version %q", ErrEncryptionKeyRequired, version)
		}
		aead, exists := e.aeads[version]
		if !exists {
			return "", fmt.Errorf("%w: row requires key version %q", ErrUnknownEncryptionKeyVersion, version)
		}
		raw, err := base64.RawURLEncoding.DecodeString(encodedPayload)
		if err != nil {
			return "", fmt.Errorf("%w: key version %q invalid base64 payload", ErrEncryptionAuthFailed, version)
		}
		nonceSize := aead.NonceSize()
		if len(raw) < nonceSize+aead.Overhead() {
			return "", fmt.Errorf("%w: key version %q truncated ciphertext", ErrEncryptionAuthFailed, version)
		}
		nonce := raw[:nonceSize]
		ciphertext := raw[nonceSize:]
		plaintext, err := aead.Open(nil, nonce, ciphertext, fieldAAD(version))
		if err != nil {
			return "", fmt.Errorf("%w: key version %q authentication tag mismatch", ErrEncryptionAuthFailed, version)
		}
		return string(plaintext), nil
	}

	if e != nil && !e.allowPlaintextFallback {
		return "", ErrPlaintextNotAllowed
	}
	return stored, nil
}

// NeedsRotation reports whether stored should be encrypted or re-encrypted to
// the active key version (either because it is still unencrypted plaintext or
// because it is encrypted under a retired key version).
func (e *FieldEncryptor) NeedsRotation(stored string) bool {
	if e == nil || stored == "" {
		return false
	}
	ver, ok := EncryptedValueVersion(stored)
	if !ok {
		return true
	}
	return ver != e.activeVersion
}

func encryptSubscriptionSecret(enc *FieldEncryptor, secret string) (string, error) {
	if enc == nil {
		if IsEncryptedValue(secret) {
			return secret, nil
		}
		return secret, nil
	}
	return enc.EncryptString(secret)
}

func decryptSubscriptionSecret(enc *FieldEncryptor, stored string) (string, error) {
	return enc.DecryptString(stored)
}

// EncryptionRotationStats reports the outcome of a resumable in-place batch
// encryption / key-rotation pass over stored rows.
type EncryptionRotationStats struct {
	Scanned                  int
	EncryptedNew             int
	ReEncrypted              int
	AlreadyActive            int
	SkippedConcurrentUpdates int
}

// RotateSQLSubscriptionsEncryption scans `subscriptions.secret` in bounded
// batches of batchSize using keyset pagination (`WHERE id > ?`), encrypting
// legacy plaintext rows and re-encrypting rows on older key versions to
// enc.ActiveVersion(). Each row update uses an optimistic concurrency predicate
// (`WHERE id = ? AND secret = ?`) so a concurrent tenant secret update is never
// clobbered. Safe and idempotent to run repeatedly or resume after an
// interruption.
func RotateSQLSubscriptionsEncryption(ctx context.Context, db *sql.DB, enc *FieldEncryptor, batchSize int) (EncryptionRotationStats, error) {
	return rotateSQLSubscriptionsEncryptionWithHook(ctx, db, enc, batchSize, nil)
}

func rotateSQLSubscriptionsEncryptionWithHook(
	ctx context.Context,
	db *sql.DB,
	enc *FieldEncryptor,
	batchSize int,
	beforeBatchTx func(batchIDs []int64),
) (EncryptionRotationStats, error) {
	if enc == nil {
		return EncryptionRotationStats{}, ErrEncryptionKeyRequired
	}
	if batchSize <= 0 {
		batchSize = 250
	}

	var (
		stats  EncryptionRotationStats
		lastID int64
	)

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		rows, err := db.QueryContext(ctx, `
			SELECT id, secret
			FROM subscriptions
			WHERE id > ?
			ORDER BY id ASC
			LIMIT ?`,
			lastID, batchSize,
		)
		if err != nil {
			return stats, fmt.Errorf("scanning subscriptions for encryption rotation: %w", err)
		}

		type rowItem struct {
			id     int64
			secret string
		}
		var batch []rowItem
		for rows.Next() {
			var it rowItem
			if err := rows.Scan(&it.id, &it.secret); err != nil {
				_ = rows.Close()
				return stats, fmt.Errorf("reading subscription row for rotation: %w", err)
			}
			batch = append(batch, it)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return stats, fmt.Errorf("iterating subscriptions for rotation: %w", err)
		}
		_ = rows.Close()

		if len(batch) == 0 {
			break
		}

		if beforeBatchTx != nil {
			ids := make([]int64, len(batch))
			for i, it := range batch {
				ids[i] = it.id
			}
			beforeBatchTx(ids)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return stats, fmt.Errorf("beginning encryption rotation batch transaction: %w", err)
		}

		for _, it := range batch {
			stats.Scanned++
			lastID = it.id
			if it.secret == "" || !enc.NeedsRotation(it.secret) {
				stats.AlreadyActive++
				continue
			}

			wasEncrypted := IsEncryptedValue(it.secret)
			plaintext, err := enc.DecryptString(it.secret)
			if err != nil {
				_ = tx.Rollback()
				return stats, fmt.Errorf("decrypting subscription %d during rotation: %w", it.id, err)
			}
			reencrypted, err := enc.EncryptString(plaintext)
			if err != nil {
				_ = tx.Rollback()
				return stats, fmt.Errorf("encrypting subscription %d during rotation: %w", it.id, err)
			}
			res, err := tx.ExecContext(ctx, `UPDATE subscriptions SET secret = ? WHERE id = ? AND secret = ?`, reencrypted, it.id, it.secret)
			if err != nil {
				_ = tx.Rollback()
				return stats, fmt.Errorf("updating subscription %d during rotation: %w", it.id, err)
			}
			rowsAffected, err := res.RowsAffected()
			if err != nil {
				_ = tx.Rollback()
				return stats, fmt.Errorf("checking rows affected for subscription %d during rotation: %w", it.id, err)
			}
			if rowsAffected == 0 {
				stats.SkippedConcurrentUpdates++
				continue
			}
			if wasEncrypted {
				stats.ReEncrypted++
			} else {
				stats.EncryptedNew++
			}
		}

		if err := tx.Commit(); err != nil {
			return stats, fmt.Errorf("committing encryption rotation batch: %w", err)
		}

		slog.Default().Info("rotated subscription secrets encryption batch",
			"active_version", enc.ActiveVersion(),
			"scanned", stats.Scanned,
			"encrypted_new", stats.EncryptedNew,
			"re_encrypted", stats.ReEncrypted,
			"already_active", stats.AlreadyActive,
			"skipped_concurrent_updates", stats.SkippedConcurrentUpdates,
		)

		if len(batch) < batchSize {
			break
		}
	}

	return stats, nil
}

// VerifySQLStoreEncryptionReadiness checks stored `subscriptions.secret` rows
// at startup to verify that if any encrypted rows exist, a FieldEncryptor is
// configured, contains every key version present in the database, and can
// authenticate a sample ciphertext for each version.
func VerifySQLStoreEncryptionReadiness(ctx context.Context, db *sql.DB, enc *FieldEncryptor) error {
	rows, err := db.QueryContext(ctx, `SELECT id, secret FROM subscriptions WHERE secret LIKE 'enc:%'`)
	if err != nil {
		return fmt.Errorf("checking encrypted subscriptions readiness: %w", err)
	}
	defer rows.Close()

	verifiedVersions := make(map[string]bool)
	var missingVersions []string

	for rows.Next() {
		var (
			id     int64
			secret string
		)
		if err := rows.Scan(&id, &secret); err != nil {
			return fmt.Errorf("scanning encrypted subscription readiness row: %w", err)
		}
		ver, ok := EncryptedValueVersion(secret)
		if !ok {
			return fmt.Errorf("subscription %d: %w: malformed encrypted envelope", id, ErrEncryptionAuthFailed)
		}
		if enc == nil {
			return fmt.Errorf("subscription %d: %w (version %q)", id, ErrEncryptionKeyRequired, ver)
		}
		if _, exists := enc.aeads[ver]; !exists {
			missingVersions = append(missingVersions, ver)
			continue
		}
		if !verifiedVersions[ver] {
			if _, err := enc.DecryptString(secret); err != nil {
				return fmt.Errorf("subscription %d: %w", id, err)
			}
			verifiedVersions[ver] = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading encrypted subscriptions readiness: %w", err)
	}
	if len(missingVersions) > 0 {
		sort.Strings(missingVersions)
		return fmt.Errorf("%w: database contains rows encrypted with key version(s) %v not present in keyring", ErrUnknownEncryptionKeyVersion, compactUniqueStrings(missingVersions))
	}
	return nil
}

func compactUniqueStrings(in []string) []string {
	var out []string
	for _, s := range in {
		if len(out) == 0 || out[len(out)-1] != s {
			out = append(out, s)
		}
	}
	return out
}
