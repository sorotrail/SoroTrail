// Package config loads and validates SoroTrail's configuration from
// environment variables.
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// NetworkConfig describes one Stellar network to index.
type NetworkConfig struct {
	Name   string `json:"name"`
	RPCURL string `json:"rpc_url"`
}

// Config holds all runtime configuration. Every field is settable via the
// environment variable named in its `env` tag; see .env.example for docs.
type Config struct {
	RPCURL              string        `env:"RPC_URL"` // deprecated — use NETWORKS
	DatabaseURL         string        `env:"DATABASE_URL"`
	PollInterval        time.Duration `env:"POLL_INTERVAL" envDefault:"5s"`
	HTTPAddr            string        `env:"HTTP_ADDR" envDefault:":8080"`
	WatchedContracts    []string      `env:"WATCHED_CONTRACTS"`
	StartLedger         uint32        `env:"START_LEDGER"`
	RetentionLedgers    uint32        `env:"RETENTION_LEDGERS" envDefault:"17280"`
	PartitionLedgerSpan uint32        `env:"PARTITION_LEDGER_SPAN" envDefault:"120960"`
	LogLevel            string        `env:"LOG_LEVEL" envDefault:"info"`

	// Networks configures one or more Stellar RPC endpoints.
	// JSON array of {name, rpc_url}. Parsed from NETWORKS env var as raw JSON string.
	Networks       []NetworkConfig `env:"-"`
	NetworksRaw    string          `env:"NETWORKS"`
	DefaultNetwork string          `env:"DEFAULT_NETWORK"`

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
	RateLimitRPS          float64 `env:"RATE_LIMIT_RPS"`
	RateLimitBurst        int     `env:"RATE_LIMIT_BURST"`
	RateLimitTrustedProxy bool    `env:"RATE_LIMIT_TRUSTED_PROXY" envDefault:"false"`
	CachePrivate          bool    `env:"CACHE_PRIVATE" envDefault:"false"`
}

// NetworksOrDefault returns the configured networks. When NETWORKS is empty
// but RPC_URL is set (legacy config), it returns a single network named
// "default" with that URL for backward compatibility.
func (c Config) NetworksOrDefault() []NetworkConfig {
	if len(c.Networks) > 0 {
		return c.Networks
	}
	if c.RPCURL != "" {
		return []NetworkConfig{{Name: "default", RPCURL: c.RPCURL}}
	}
	// Fallback to the default testnet URL.
	return []NetworkConfig{{Name: "default", RPCURL: "https://soroban-testnet.stellar.org"}}
}

// NetworkNames returns the list of configured network names.
func (c Config) NetworkNames() []string {
	networks := c.NetworksOrDefault()
	names := make([]string, len(networks))
	for i, n := range networks {
		names[i] = n.Name
	}
	return names
}

// DefaultNetworkName returns the configured default network name. When only
// one network exists and DEFAULT_NETWORK is unset, that single network is the
// default.
func (c Config) DefaultNetworkName() string {
	if c.DefaultNetwork != "" {
		return c.DefaultNetwork
	}
	networks := c.NetworksOrDefault()
	if len(networks) == 1 {
		return networks[0].Name
	}
	return ""
}

// Load reads configuration from the environment and validates it.
func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing environment: %w", err)
	}
	cfg.WatchedContracts = cleanContractList(cfg.WatchedContracts)
	// Parse NETWORKS from raw JSON string.
	if cfg.NetworksRaw != "" {
		networks, err := ParseNetworks(cfg.NetworksRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parsing NETWORKS: %w", err)
		}
		cfg.Networks = networks
	}
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

	if len(c.Networks) > 0 {
		names := map[string]bool{}
		for _, n := range c.Networks {
			u, err := url.Parse(n.RPCURL)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("network %q: RPC_URL %q is not a valid URL", n.Name, n.RPCURL)
			}
			if n.Name == "" {
				return fmt.Errorf("network name must not be empty for URL %q", n.RPCURL)
			}
			if names[n.Name] {
				return fmt.Errorf("duplicate network name %q", n.Name)
			}
			names[n.Name] = true
		}
		if len(c.Networks) > 1 && c.DefaultNetwork == "" {
			return fmt.Errorf("DEFAULT_NETWORK is required when multiple networks are configured")
		}
		if c.DefaultNetwork != "" && !names[c.DefaultNetwork] {
			return fmt.Errorf("DEFAULT_NETWORK %q is not in the configured NETWORKS list", c.DefaultNetwork)
		}
	} else {
		// Legacy: validate single RPC_URL.
		u, err := url.Parse(c.RPCURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			// If RPC_URL is unset, use the default testnet URL — that's valid.
			if c.RPCURL != "" {
				return fmt.Errorf("RPC_URL %q is not a valid URL", c.RPCURL)
			}
		}
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
	if (c.RateLimitRPS > 0) != (c.RateLimitBurst > 0) {
		return fmt.Errorf("RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
	}
	if (c.RPCURL != "") && len(c.Networks) > 0 {
		return fmt.Errorf("RPC_URL and NETWORKS cannot both be set; use NETWORKS only")
	}
	return nil
}

// ValidContractID reports whether s looks like a Soroban contract strkey.
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

// ParseNetworks parses a JSON string into a slice of NetworkConfig.
func ParseNetworks(raw string) ([]NetworkConfig, error) {
	if raw == "" {
		return nil, nil
	}
	var networks []NetworkConfig
	if err := json.Unmarshal([]byte(raw), &networks); err != nil {
		return nil, fmt.Errorf("parsing NETWORKS JSON: %w", err)
	}
	return networks, nil
}
