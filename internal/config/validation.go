package config

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// sensitiveEnvVars lists environment variable names whose values must be
// redacted in any log output to prevent credential leakage.
var sensitiveEnvVars = map[string]bool{
	"DATABASE_URL":               true,
	"RPC_URL":                    true,
	"RPC_URLS":                   true,
	"API_KEY":                    true,
	"ARCHIVE_SECRET_ACCESS_KEY":  true,
	"MULTI_TENANT_BOOTSTRAP_KEY": true,
	"ENCRYPTION_KEY":             true,
	"ENCRYPTION_KEYS":            true,
}

// redact returns value unchanged unless name is a known sensitive variable,
// in which case the credential portion is replaced with "***".
func redact(name, value string) string {
	if value == "" || !sensitiveEnvVars[name] {
		return value
	}
	if name == "DATABASE_URL" || name == "RPC_URL" || name == "RPC_URLS" {
		if u, err := url.Parse(value); err == nil && u.Scheme != "" && u.Host != "" {
			return redactURLPassword(value)
		}
		return "<redacted>"
	}
	return "***"
}

func redactURLPassword(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<redacted>"
	}
	if u.User != nil {
		if _, has := u.User.Password(); has {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

// redactURLCredentials drops the whole userinfo rather than masking only the
// password, keeping the scheme, host and path so the line still says which
// endpoint was configured. It is what LoggableFields uses: that line is
// emitted once per startup into whatever ships the logs onward, so it does not
// disclose a username either.
//
// Like redactURLPassword it fails closed. A valid credential is enough to
// defeat url.Parse — a "%", a space or a "[" in the userinfo each do it — and
// Load() does not reject an unparseable DATABASE_URL, so that value really can
// reach here.
func redactURLCredentials(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<redacted>"
	}
	u.User = nil
	return u.String()
}

// multiError aggregates several validation failures into a single error.
type multiError []string

func (m multiError) Error() string {
	var b strings.Builder
	b.WriteString("configuration validation failed:\n")
	for _, e := range m {
		b.WriteString("  - ")
		b.WriteString(e)
		b.WriteByte('\n')
	}
	return b.String()
}

// ValidateAll collects every configuration validation failure into a single
// aggregated error. It validates required fields, URL format, duration format,
// numeric ranges, allowed-value sets, contract ID format, and mutual
// dependency consistency.
func (c Config) ValidateAll() error {
	var errs []string

	// --- required -----------------------------------------------------------

	if c.DatabaseURL == "" {
		errs = append(errs, "DATABASE_URL: required but empty")
	}

	// --- URL format ---------------------------------------------------------

	// RPC_URLS takes priority when set: the multi-provider failover client
	// is used and RPC_URL may be left empty. Otherwise RPC_URL is the
	// single-provider endpoint and is required to be a valid URL.
	if len(c.RPCURLS) > 0 {
		for i, raw := range c.RPCURLS {
			if u, err := url.Parse(raw); err != nil || u.Scheme == "" || u.Host == "" {
				errs = append(errs, fmt.Sprintf("RPC_URLS[%d]: %q is not a valid absolute URL (want scheme://host)", i, raw))
			}
		}
	} else if c.RPCURL == "" {
		errs = append(errs, "RPC_URL: required but empty")
	} else if u, err := url.Parse(c.RPCURL); err != nil || u.Scheme == "" || u.Host == "" {
		errs = append(errs, fmt.Sprintf("RPC_URL: %q is not a valid absolute URL (want scheme://host)", c.RPCURL))
	}

	// --- duration format ----------------------------------------------------

	if c.PollInterval <= 0 {
		errs = append(errs, fmt.Sprintf("POLL_INTERVAL: %s must be a positive duration (e.g. 5s, 1m)",
			c.PollInterval))
	}
	if c.PollIntervalMin < 0 {
		errs = append(errs, fmt.Sprintf("POLL_INTERVAL_MIN: %s must be non-negative", c.PollIntervalMin))
	}
	if c.PollIntervalMax < 0 {
		errs = append(errs, fmt.Sprintf("POLL_INTERVAL_MAX: %s must be non-negative", c.PollIntervalMax))
	}
	if c.PollIntervalMin > 0 && c.PollIntervalMax > 0 && c.PollIntervalMin > c.PollIntervalMax {
		errs = append(errs, fmt.Sprintf("POLL_INTERVAL_MIN (%s) must be <= POLL_INTERVAL_MAX (%s)",
			c.PollIntervalMin, c.PollIntervalMax))
	}
	if c.AuditPollInterval <= 0 {
		errs = append(errs, fmt.Sprintf("AUDIT_POLL_INTERVAL: %s must be a positive duration (e.g. 30s)",
			c.AuditPollInterval))
	}
	if c.IngesterMinBackoff <= 0 {
		errs = append(errs, fmt.Sprintf("INGESTER_MIN_BACKOFF: %s must be positive", c.IngesterMinBackoff))
	}
	if c.IngesterMaxBackoff <= 0 {
		errs = append(errs, fmt.Sprintf("INGESTER_MAX_BACKOFF: %s must be positive", c.IngesterMaxBackoff))
	}
	if c.IngesterMinBackoff > c.IngesterMaxBackoff {
		errs = append(errs, "INGESTER_MIN_BACKOFF: must not exceed INGESTER_MAX_BACKOFF")
	}
	if c.IngesterJitterMin < 0 || c.IngesterJitterMax < 0 {
		errs = append(errs, "INGESTER jitter bounds must be non-negative")
	}
	if c.IngesterJitterMax > 0 && c.IngesterJitterMin > c.IngesterJitterMax {
		errs = append(errs, "INGESTER_JITTER_MIN: must not exceed INGESTER_JITTER_MAX")
	}

	// --- numeric ranges -----------------------------------------------------

	if c.RetentionLedgers == 0 {
		errs = append(errs, "RETENTION_LEDGERS: must be a positive integer (default 17280)")
	}
	if c.PartitionLedgerSpan == 0 {
		errs = append(errs, "PARTITION_LEDGER_SPAN: must be a positive integer (default 120960)")
	}
	if c.AuditBatchLedgers == 0 {
		errs = append(errs, "AUDIT_BATCH_LEDGERS: must be a positive integer (default 100)")
	}
	if c.AuditLagThreshold == 0 {
		errs = append(errs, "AUDIT_LAG_THRESHOLD: must be a positive integer (default 200)")
	}
	if c.AuditBudgetShare < 0 || c.AuditBudgetShare > 1 {
		errs = append(errs, fmt.Sprintf("AUDIT_BUDGET_SHARE: %v must be in [0, 1]",
			c.AuditBudgetShare))
	}
	if c.AuditMaxRPS <= 0 {
		errs = append(errs, fmt.Sprintf("AUDIT_MAX_RPS: %v must be positive", c.AuditMaxRPS))
	}
	if c.AuditMaxRepair <= 0 {
		errs = append(errs, fmt.Sprintf("AUDIT_MAX_REPAIR_ATTEMPTS: %d must be positive",
			c.AuditMaxRepair))
	}
	if c.AuditFindingMaxLgrs == 0 {
		errs = append(errs, "AUDIT_FINDING_MAX_LEDGERS: must be a positive integer (default 100)")
	}
	if c.RateLimitRPS < 0 {
		errs = append(errs, fmt.Sprintf("RATE_LIMIT_RPS: %v must be non-negative", c.RateLimitRPS))
	}
	if c.RateLimitBurst < 0 {
		errs = append(errs, fmt.Sprintf("RATE_LIMIT_BURST: %d must be non-negative", c.RateLimitBurst))
	}
	if c.HourlyQuota < 0 {
		errs = append(errs, fmt.Sprintf("HOURLY_QUOTA: %d must be non-negative", c.HourlyQuota))
	}
	if c.DailyQuota < 0 {
		errs = append(errs, fmt.Sprintf("DAILY_QUOTA: %d must be non-negative", c.DailyQuota))
	}

	// --- allowed values -----------------------------------------------------

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(c.LogLevel)] {
		errs = append(errs, fmt.Sprintf("LOG_LEVEL: %q must be one of debug|info|warn|error",
			redact("LOG_LEVEL", c.LogLevel)))
	}

	// --- logging format -----------------------------------------------------

	validFormats := map[string]bool{"json": true, "text": true}
	if !validFormats[strings.ToLower(c.LogFormat)] {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT: %q must be one of json|text",
			redact("LOG_FORMAT", c.LogFormat)))
	}

	// --- contract ID format -------------------------------------------------

	for _, id := range c.WatchedContracts {
		if !ValidContractID(id) {
			errs = append(errs, fmt.Sprintf("WATCHED_CONTRACTS entry %q is not a valid contract ID (want C... strkey, 56 chars)", id))
		}
	}

	// --- mutual dependency --------------------------------------------------

	if (c.RateLimitRPS > 0) != (c.RateLimitBurst > 0) {
		errs = append(errs, "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
	}
	errs = append(errs, c.collectLegacyValidateErrors()...)
	errs = append(errs, c.validateMTLS()...)
	errs = append(errs, c.validateEncryption()...)

	if len(errs) == 0 {
		return nil
	}
	return multiError(errs)
}

func (c Config) validateEncryption() []string {
	var errs []string
	if raw := strings.TrimSpace(c.EncryptionKeys); raw != "" {
		if err := validateEncryptionKeyringSpec("ENCRYPTION_KEYS", raw, true); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if raw := strings.TrimSpace(c.EncryptionKey); raw != "" {
		if err := validateEncryptionKeyringSpec("ENCRYPTION_KEY", raw, false); err != nil {
			errs = append(errs, err.Error())
		}
	}
	return errs
}

func validateEncryptionKeyringSpec(envName, raw string, requireVersioned bool) error {
	parts := strings.Split(raw, ",")
	seenVersions := make(map[string]struct{}, len(parts))
	validCount := 0
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		version := "v1"
		keyStr := part
		if v, k, hasColon := strings.Cut(part, ":"); hasColon {
			version = strings.TrimSpace(v)
			keyStr = strings.TrimSpace(k)
		} else if requireVersioned && len(parts) > 1 {
			return fmt.Errorf("%s: entry %d is missing a key version prefix (expected <version>:<key>)", envName, i+1)
		}
		if !validKeyVersionID(version) {
			return fmt.Errorf("%s: entry %d has invalid key version identifier %q", envName, i+1, version)
		}
		if _, dup := seenVersions[version]; dup {
			return fmt.Errorf("%s: duplicate key version %q", envName, version)
		}
		seenVersions[version] = struct{}{}
		keyLen, err := decodedAES256KeyLength(keyStr)
		if err != nil {
			return fmt.Errorf("%s: version %q: %w", envName, version, err)
		}
		if keyLen != 32 {
			return fmt.Errorf("%s: version %q decoded to %d bytes, want 32-byte AES-256 key", envName, version, keyLen)
		}
		validCount++
	}
	if validCount == 0 {
		return fmt.Errorf("%s: no valid encryption keys found", envName)
	}
	return nil
}

func validKeyVersionID(v string) bool {
	if v == "" || len(v) > 32 {
		return false
	}
	for i, r := range v {
		isLower := r >= 'a' && r <= 'z'
		isUpper := r >= 'A' && r <= 'Z'
		isDigit := r >= '0' && r <= '9'
		if i == 0 {
			if !isLower && !isUpper && !isDigit {
				return false
			}
			continue
		}
		if !isLower && !isUpper && !isDigit && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func decodedAES256KeyLength(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty key material")
	}
	if len(raw) == 64 {
		if decoded, err := hex.DecodeString(raw); err == nil {
			return len(decoded), nil
		}
	}
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := enc.DecodeString(raw); err == nil {
			return len(decoded), nil
		}
	}
	return 0, fmt.Errorf("key material is neither valid 64-character hex nor valid base64")
}
