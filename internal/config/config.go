// Package config loads and validates SoroTrail's configuration from
// environment variables.
package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all runtime configuration. Every field is settable via the
// environment variable named in its `env` tag; see .env.example for docs.
type Config struct {
	RPCURL                string        `env:"RPC_URL" envDefault:"https://soroban-testnet.stellar.org"`
	DatabaseURL           string        `env:"DATABASE_URL"`
	PollInterval          time.Duration `env:"POLL_INTERVAL" envDefault:"5s"`
	HTTPAddr              string        `env:"HTTP_ADDR" envDefault:":8080"`
	WatchedContracts      []string      `env:"WATCHED_CONTRACTS"`
	StartLedger           uint32        `env:"START_LEDGER"`
	RetentionLedgers      uint32        `env:"RETENTION_LEDGERS" envDefault:"17280"`
	PartitionLedgerSpan   uint32        `env:"PARTITION_LEDGER_SPAN" envDefault:"120960"`
	LogLevel              string        `env:"LOG_LEVEL" envDefault:"info"`
	APIQueryTimeout       time.Duration `env:"API_QUERY_TIMEOUT" envDefault:"25s"`
	APISlowQueryThreshold time.Duration `env:"API_SLOW_QUERY_THRESHOLD" envDefault:"2s"`

	// Horizon backfill configuration. HORIZON_URL is the REST endpoint
	// the backfill command reads; BACKFILL_RATE_RPS controls how many
	// requests per second the backfill command issues (env/v11 parses
	// the float directly). Both are used only by `sorotrail backfill`,
	// not by the live indexer. The defaults match the documented
	// public-testnet target and a safe ~10 req/s pace; private
	// deployments can point HORIZON_URL at themselves and allow a
	// tighter rate via the flag or env override.
	HorizonURL      string  `env:"HORIZON_URL" envDefault:"https://horizon-testnet.stellar.org"`
	BackfillRateRPS float64 `env:"BACKFILL_RATE_RPS" envDefault:"10"`

	// Audit config. AUDIT_ENABLED=false (default) disables the auditor
	// entirely; the binary behaves exactly like the pre-audit build.
	AuditEnabled        bool          `env:"AUDIT_ENABLED" envDefault:"false"`
	AuditPollInterval   time.Duration `env:"AUDIT_POLL_INTERVAL" envDefault:"30s"`
	AuditBatchLedgers   uint32        `env:"AUDIT_BATCH_LEDGERS" envDefault:"100"`
	AuditLagThreshold   uint32        `env:"AUDIT_LAG_THRESHOLD" envDefault:"200"`
	AuditBudgetShare    float64       `env:"AUDIT_BUDGET_SHARE" envDefault:"0.10"`
	AuditMaxRPS         float64       `env:"AUDIT_MAX_RPS" envDefault:"10"`
	AuditMaxRepair      int           `env:"AUDIT_MAX_REPAIR_ATTEMPTS" envDefault:"3"`
	AuditFindingMaxLgrs uint32        `env:"AUDIT_FINDING_MAX_LEDGERS" envDefault:"100"`

	// HTTP server timeouts. Zero means no timeout for that field.
	// HTTP_READ_TIMEOUT limits the time to read the full request
	// (including body); HTTP_READ_HEADER_TIMEOUT limits header reads
	// only and is the most important defence against slow-client attacks.
	HTTPReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"30s"`
	HTTPWriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"30s"`
	HTTPIdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"60s"`
	HTTPReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"10s"`

	// APIKey, when set, gates the watched-contracts management endpoints
	// via a constant-time comparison against the X-API-Key request header.
	// Empty means the watched-contracts surface starts up rejected (every
	// request gets a 503 with a "no API_KEY configured" message), so
	// writes are never open even when other auth would be off.
	APIKey string `env:"API_KEY"`
	// HTTP rate limiting (per client). RATE_LIMIT_RPS / RATE_LIMIT_BURST
	// are both unset (zero) by default, which disables the limiter
	// entirely — a no-op middleware — so deployments without this turned
	// on keep today's behavior bit-for-bit.
	//
	// RATE_LIMIT_TRUSTED_PROXY defaults to false because X-Forwarded-For
	// is set by the client itself; enabling it without an upstream proxy
	// that strips/rewrites the header would let any caller pick their own
	// rate-limit key and bypass arbitrary per-IP throttling.
	RateLimitRPS          float64 `env:"RATE_LIMIT_RPS"`
	RateLimitBurst        int     `env:"RATE_LIMIT_BURST"`
	RateLimitTrustedProxy bool    `env:"RATE_LIMIT_TRUSTED_PROXY" envDefault:"false"`

	// CompressMinSize is the response body size, in bytes, at or above which
	// responses are gzip/deflate encoded for clients that advertise support.
	// Negative disables compression entirely; 0 uses api.CompressMinSize.
	CompressMinSize int `env:"COMPRESS_MIN_SIZE" envDefault:"0"`

	// CachePrivate flips the cacheable endpoints from Cache-Control: public
	// to Cache-Control: private. Set this when the deployment serves
	// per-user data behind an auth layer (#17, not yet merged) so shared
	// caches (CDN/proxy) cannot leak responses across keys. Browsers can
	// still cache the response for the same authenticated user; CDNs and
	// intermediaries cannot. Defaults to false (the deployment does not
	// need request-scoped caching).
	CachePrivate bool `env:"CACHE_PRIVATE" envDefault:"false"`

	// ShutdownTimeout limits how long the graceful HTTP server drain and
	// component shutdown may take before the process is killed. Zero means
	// no timeout (wait indefinitely).
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
}

// Load reads configuration from the environment and validates it.
func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing environment: %w", err)
	}
	// env/v11 splits on "," but keeps empty entries and whitespace.
	cfg.WatchedContracts = cleanContractList(cfg.WatchedContracts)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the configuration for values that would fail at runtime.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	u, err := url.Parse(c.RPCURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("RPC_URL %q is not a valid URL", c.RPCURL)
	}
	if u, err := url.Parse(c.HorizonURL); err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("HORIZON_URL %q is not a valid URL", c.HorizonURL)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got %s", c.PollInterval)
	}
	if c.APIQueryTimeout <= 0 {
		return fmt.Errorf("API_QUERY_TIMEOUT must be positive, got %s", c.APIQueryTimeout)
	}
	if c.APISlowQueryThreshold <= 0 {
		return fmt.Errorf("API_SLOW_QUERY_THRESHOLD must be positive, got %s", c.APISlowQueryThreshold)
	}
	if c.RetentionLedgers == 0 {
		return fmt.Errorf("RETENTION_LEDGERS must be positive")
	}
	if c.PartitionLedgerSpan == 0 {
		return fmt.Errorf("PARTITION_LEDGER_SPAN must be positive")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("LOG_LEVEL %q is not one of debug|info|warn|error", c.LogLevel)
	}
	for _, id := range c.WatchedContracts {
		if !ValidContractID(id) {
			return fmt.Errorf("WATCHED_CONTRACTS entry %q is not a valid contract ID (want C... strkey, 56 chars)", id)
		}
	}
	if c.AuditPollInterval <= 0 {
		return fmt.Errorf("AUDIT_POLL_INTERVAL must be positive")
	}
	if c.AuditBatchLedgers == 0 {
		return fmt.Errorf("AUDIT_BATCH_LEDGERS must be positive")
	}
	if c.AuditLagThreshold == 0 {
		return fmt.Errorf("AUDIT_LAG_THRESHOLD must be positive")
	}
	if c.AuditBudgetShare < 0 || c.AuditBudgetShare > 1 {
		return fmt.Errorf("AUDIT_BUDGET_SHARE must be in [0,1]")
	}
	if c.AuditMaxRPS <= 0 {
		return fmt.Errorf("AUDIT_MAX_RPS must be positive")
	}
	if c.AuditMaxRepair <= 0 {
		return fmt.Errorf("AUDIT_MAX_REPAIR_ATTEMPTS must be positive")
	}
	if c.AuditFindingMaxLgrs == 0 {
		return fmt.Errorf("AUDIT_FINDING_MAX_LEDGERS must be positive")
	}
	if c.BackfillRateRPS <= 0 {
		return fmt.Errorf("BACKFILL_RATE_RPS must be positive, got %v", c.BackfillRateRPS)
	}
	if c.RateLimitRPS < 0 {
		return fmt.Errorf("RATE_LIMIT_RPS must be non-negative")
	}
	if c.RateLimitBurst < 0 {
		return fmt.Errorf("RATE_LIMIT_BURST must be non-negative")
	}
	if c.HTTPReadTimeout < 0 {
		return fmt.Errorf("HTTP_READ_TIMEOUT must be non-negative, got %s", c.HTTPReadTimeout)
	}
	if c.HTTPWriteTimeout < 0 {
		return fmt.Errorf("HTTP_WRITE_TIMEOUT must be non-negative, got %s", c.HTTPWriteTimeout)
	}
	if c.HTTPIdleTimeout < 0 {
		return fmt.Errorf("HTTP_IDLE_TIMEOUT must be non-negative, got %s", c.HTTPIdleTimeout)
	}
	if c.HTTPReadHeaderTimeout < 0 {
		return fmt.Errorf("HTTP_READ_HEADER_TIMEOUT must be non-negative, got %s", c.HTTPReadHeaderTimeout)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be non-negative, got %s", c.ShutdownTimeout)
	}
	// Both must be set together: half-configured limits would silently
	// behave like the disabled case (Enabled returns false when either is
	// non-positive), which would confuse operators who set one and
	// expected throttling to kick in.
	if (c.RateLimitRPS > 0) != (c.RateLimitBurst > 0) {
		return fmt.Errorf("RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
	}
	return nil
}

// ValidContractID reports whether s looks like a Soroban contract strkey.
// It checks shape only (C prefix, 56 base32 chars), not the checksum.
func ValidContractID(s string) bool {
	if len(s) != 56 || s[0] != 'C' {
		return false
	}
	for _, r := range s[1:] {
		if (r < 'A' || r > 'Z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}

// ValidCursor reports whether s is a valid pagination cursor.
// A cursor must be non-empty, at most 128 characters, and consist only of
// alphanumeric characters, hyphens, underscores, dots, or colons.
func ValidCursor(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') &&
			(r < 'A' || r > 'Z') &&
			(r < '0' || r > '9') &&
			r != '-' && r != '_' && r != '.' && r != ':' {
			return false
		}
	}
	return true
}

func cleanContractList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// LoggableFields returns the configuration as a map of fields suitable for logging,
// with credentials redacted (e.g., from DATABASE_URL).
func (c Config) LoggableFields() []any {
	dbURL := c.DatabaseURL
	if u, err := url.Parse(c.DatabaseURL); err == nil {
		u.User = nil
		dbURL = u.String()
	}

	return []any{
		"rpc_url", c.RPCURL,
		"database_url", dbURL,
		"poll_interval", c.PollInterval,
		"http_addr", c.HTTPAddr,
		"watched_contracts", len(c.WatchedContracts),
		"start_ledger", c.StartLedger,
		"retention_ledgers", c.RetentionLedgers,
		"log_level", c.LogLevel,
		"http_read_timeout", c.HTTPReadTimeout,
		"http_write_timeout", c.HTTPWriteTimeout,
		"http_idle_timeout", c.HTTPIdleTimeout,
		"http_read_header_timeout", c.HTTPReadHeaderTimeout,
		"shutdown_timeout", c.ShutdownTimeout,
		"audit_enabled", c.AuditEnabled,
	}
}
