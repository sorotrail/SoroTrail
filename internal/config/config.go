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
	RPCURL              string        `env:"RPC_URL" envDefault:"https://soroban-testnet.stellar.org"`
	DatabaseURL         string        `env:"DATABASE_URL"`
	PollInterval        time.Duration `env:"POLL_INTERVAL" envDefault:"5s"`
	HTTPAddr            string        `env:"HTTP_ADDR" envDefault:":8080"`
	WatchedContracts    []string      `env:"WATCHED_CONTRACTS"`
	StartLedger         uint32        `env:"START_LEDGER"`
	RetentionLedgers    uint32        `env:"RETENTION_LEDGERS" envDefault:"17280"`
	PartitionLedgerSpan uint32        `env:"PARTITION_LEDGER_SPAN" envDefault:"120960"`
	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info"`

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
	// CachePrivate flips the cacheable endpoints from Cache-Control: public
	// to Cache-Control: private. Set this when the deployment serves
	// per-user data behind an auth layer (#17, not yet merged) so shared
	// caches (CDN/proxy) cannot leak responses across keys. Browsers can
	// still cache the response for the same authenticated user; CDNs and
	// intermediaries cannot. Defaults to false (the deployment does not
	// need request-scoped caching).
	CachePrivate bool `env:"CACHE_PRIVATE" envDefault:"false"`
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
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got %s", c.PollInterval)
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
	if c.RateLimitRPS < 0 {
		return fmt.Errorf("RATE_LIMIT_RPS must be non-negative")
	}
	if c.RateLimitBurst < 0 {
		return fmt.Errorf("RATE_LIMIT_BURST must be non-negative")
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

func cleanContractList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
