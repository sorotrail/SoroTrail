package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
)

// sensitiveEnvVars lists environment variable names whose values must be
// redacted in any log output to prevent credential leakage.
var sensitiveEnvVars = map[string]bool{
	"DATABASE_URL": true,
}

// redact returns value unchanged unless name is a known sensitive variable,
// in which case the credential portion is replaced with "***".
func redact(name, value string) string {
	if value == "" || !sensitiveEnvVars[name] {
		return value
	}
	return redactURLPassword(value)
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
		errs = append(errs, fmt.Sprintf("RPC_URL: %q is not a valid absolute URL (want scheme://host)",
			redact("RPC_URL", c.RPCURL)))
	}

	// --- duration format ----------------------------------------------------

	if c.PollInterval <= 0 {
		errs = append(errs, fmt.Sprintf("POLL_INTERVAL: %s must be a positive duration (e.g. 5s, 1m)",
			c.PollInterval))
	}
	if c.AuditPollInterval <= 0 {
		errs = append(errs, fmt.Sprintf("AUDIT_POLL_INTERVAL: %s must be a positive duration (e.g. 30s)",
			c.AuditPollInterval))
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

	// --- allowed values -----------------------------------------------------

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(c.LogLevel)] {
		errs = append(errs, fmt.Sprintf("LOG_LEVEL: %q must be one of debug|info|warn|error",
			redact("LOG_LEVEL", c.LogLevel)))
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

	if len(errs) == 0 {
		return nil
	}
	return multiError(errs)
}

// MustValidate validates the configuration and exits the process with a fatal
// log message when any check fails. It is intended for the top-level main
// function so that boot-time errors are surfaced immediately.
func (c Config) MustValidate() {
	if err := c.ValidateAll(); err != nil {
		log.Printf("FATAL: %s", err)
		os.Exit(1)
	}
}
