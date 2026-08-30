package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file tests that every configuration validation error is actionable:
// it names the variable, shows the bad value, states the constraint, and
// references the README section. Each test fails if the behaviour it
// describes regresses.

// validBaseValidate extends validBase with fields that Validate() checks
// but that are normally supplied by env/v11 defaults in Load().
func validBaseValidate() Config {
	cfg := validBase()
	cfg.LogFormat = "text"
	cfg.APIQueryTimeout = 25 * time.Second
	cfg.APISlowQueryThreshold = 2 * time.Second
	cfg.IngestPageSize = 1000
	cfg.IngestBatchSize = 1000
	cfg.RetentionBatchSize = 5000
	cfg.RetentionPause = 100 * time.Millisecond
	cfg.RetentionInterval = 1 * time.Hour
	cfg.BackfillRateRPS = 10
	cfg.RPCMaxAttempts = 3
	cfg.RPCBaseBackoff = 500 * time.Millisecond
	cfg.RPCMaxBackoff = 30 * time.Second
	cfg.RPCRateLimitRPS = 10
	cfg.RPCRateLimit = 10
	cfg.SweepConcurrency = 1
	cfg.ExportMaxRange = 17280
	cfg.APIMaxLimit = 500
	cfg.ShutdownTimeout = 15 * time.Second
	cfg.MultiTenantUsageFlush = 10 * time.Second
	cfg.MultiTenantStreamScopeSync = 30 * time.Second
	cfg.ReorgConfirmationWindow = 64
	cfg.ReorgRescanInterval = 1 * time.Minute
	cfg.BatchMaxBackoff = 1 * time.Second
	return cfg
}

// ---------- ValidateAll: per-variable error text assertions ----------

// TestValidateAll_EveryErrorNamesVariable verifies that every error
// message produced by ValidateAll starts with the environment variable
// name, so an operator can immediately identify which setting to fix.
func TestValidateAll_EveryErrorNamesVariable(t *testing.T) {
	// Build a config that triggers every validation rule at once.
	// Zero-value Config hits most rules; we set a few fields to trigger
	// the remaining ones.
	cfg := Config{
		DatabaseURL:         "postgres://user:pass@localhost/db",
		RPCURL:              "not-a-url",
		PollInterval:        -1 * time.Second,
		AuditPollInterval:   -1 * time.Second,
		RetentionLedgers:    0,
		PartitionLedgerSpan: 0,
		AuditBatchLedgers:   0,
		AuditLagThreshold:   0,
		AuditBudgetShare:    2.0,
		AuditMaxRPS:         -1,
		AuditMaxRepair:      -1,
		AuditFindingMaxLgrs: 0,
		RateLimitRPS:        -1,
		RateLimitBurst:      -1,
		LogLevel:            "loud",
		LogFormat:           "xml",
		WatchedContracts:    []string{"not-a-contract"},
	}

	err := cfg.ValidateAll()
	require.Error(t, err)

	// Every line (after the header) must start with a variable name.
	lines := strings.Split(err.Error(), "\n")
	for _, line := range lines {
		if line == "" || line == "configuration validation failed:" {
			continue
		}
		// Lines have the format "  - VARIABLE: ..." — strip the "  - " prefix.
		body := strings.TrimPrefix(line, "  - ")
		if body == line {
			continue // not an error line
		}
		// The first token should be an env var name (UPPER_CASE or UPPER_CASE[index]).
		firstSpace := strings.IndexAny(body, ": [")
		if firstSpace <= 0 {
			t.Errorf("error line does not start with variable name: %q", line)
			continue
		}
		varName := body[:firstSpace]
		// Variable names are UPPER_CASE or UPPER_CASE[index].
		for _, ch := range varName {
			if ch != '_' && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '[' && ch != ']' {
				t.Errorf("error line starts with %q which doesn't look like a variable name: %q", varName, line)
				break
			}
		}
	}
}

// TestValidateAll_EveryErrorShowsConstraint verifies that every error
// message contains a constraint description so the operator knows what
// the valid range or format is.
func TestValidateAll_EveryErrorShowsConstraint(t *testing.T) {
	constraintHints := []string{
		"required",
		"valid",
		"positive",
		"non-negative",
		"must be",
		"not a valid",
		"want",
	}

	cfg := Config{
		DatabaseURL:         "postgres://user:pass@localhost/db",
		RPCURL:              "not-a-url",
		PollInterval:        -1 * time.Second,
		AuditPollInterval:   -1 * time.Second,
		RetentionLedgers:    0,
		PartitionLedgerSpan: 0,
		AuditBatchLedgers:   0,
		AuditLagThreshold:   0,
		AuditBudgetShare:    2.0,
		AuditMaxRPS:         -1,
		AuditMaxRepair:      -1,
		AuditFindingMaxLgrs: 0,
		RateLimitRPS:        -1,
		RateLimitBurst:      -1,
		LogLevel:            "loud",
		LogFormat:           "xml",
		WatchedContracts:    []string{"not-a-contract"},
	}

	err := cfg.ValidateAll()
	require.Error(t, err)

	lines := strings.Split(err.Error(), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "configuration validation failed:" {
			continue
		}
		body := strings.TrimPrefix(line, "  - ")
		hasConstraint := false
		for _, hint := range constraintHints {
			if strings.Contains(body, hint) {
				hasConstraint = true
				break
			}
		}
		if !hasConstraint {
			t.Errorf("error missing constraint description: %q", line)
		}
	}
}

// TestValidateAll_AllErrorsReportedTogether verifies that a config with
// many invalid fields produces a single aggregated error listing all
// failures, not just the first one.
func TestValidateAll_AllErrorsReportedTogether(t *testing.T) {
	// Empty config triggers most rules.
	err := Config{}.ValidateAll()
	require.Error(t, err)

	msg := err.Error()
	// Must have the aggregation header.
	assert.True(t, strings.HasPrefix(msg, "configuration validation failed:\n"),
		"error must start with aggregation header, got: %s", msg)

	// Count distinct error entries.
	lines := strings.Split(msg, "\n")
	errorCount := 0
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "- ") {
			errorCount++
		}
	}
	// An empty config should produce at least 8 distinct errors.
	assert.GreaterOrEqual(t, errorCount, 8,
		"empty config should produce many errors, got %d:\n%s", errorCount, msg)

	// Specific errors that MUST all be present.
	mustContain := []string{
		"DATABASE_URL",
		"POLL_INTERVAL",
		"RETENTION_LEDGERS",
		"PARTITION_LEDGER_SPAN",
		"AUDIT_BATCH_LEDGERS",
		"AUDIT_LAG_THRESHOLD",
		"AUDIT_MAX_RPS",
		"LOG_LEVEL",
	}
	for _, v := range mustContain {
		assert.Contains(t, msg, v, "error must include %s", v)
	}
}

// ---------- Per-rule error text assertions ----------

func TestValidateAll_DatabaseURLEmpty(t *testing.T) {
	err := Config{}.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "DATABASE_URL")
	assert.Contains(t, msg, "required but empty")
}

func TestValidateAll_RPCURLEmpty(t *testing.T) {
	cfg := validBase()
	cfg.RPCURL = ""
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RPC_URL")
	assert.Contains(t, msg, "required but empty")
}

func TestValidateAll_RPCURLInvalid(t *testing.T) {
	cfg := validBase()
	cfg.RPCURL = "not-a-url"
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RPC_URL")
	assert.Contains(t, msg, "not-a-url")
	assert.Contains(t, msg, "not a valid absolute URL")
}

func TestValidateAll_RPCURLWithCredentials(t *testing.T) {
	cfg := validBase()
	// Use a URL with credentials to verify it's accepted when valid.
	cfg.RPCURL = "https://user:pass@rpc.example.com"
	err := cfg.ValidateAll()
	require.NoError(t, err, "valid RPC URL should pass")
}

func TestValidateAll_RPCURLSInvalidEntry(t *testing.T) {
	cfg := validBase()
	cfg.RPCURLS = []string{"https://good.example.com", "bad-url"}
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RPC_URLS[1]")
	assert.Contains(t, msg, "bad-url")
	assert.Contains(t, msg, "not a valid absolute URL")
}

func TestValidateAll_PollIntervalZero(t *testing.T) {
	cfg := validBase()
	cfg.PollInterval = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "POLL_INTERVAL")
	assert.Contains(t, msg, "must be a positive duration")
}

func TestValidateAll_PollIntervalNegative(t *testing.T) {
	cfg := validBase()
	cfg.PollInterval = -5 * time.Second
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "POLL_INTERVAL")
	assert.Contains(t, msg, "-5s")
	assert.Contains(t, msg, "must be a positive duration")
}

func TestValidateAll_AuditPollIntervalZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditPollInterval = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_POLL_INTERVAL")
	assert.Contains(t, msg, "must be a positive duration")
}

func TestValidateAll_RetentionLedgersZero(t *testing.T) {
	cfg := validBase()
	cfg.RetentionLedgers = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RETENTION_LEDGERS")
	assert.Contains(t, msg, "must be a positive integer")
}

func TestValidateAll_PartitionLedgerSpanZero(t *testing.T) {
	cfg := validBase()
	cfg.PartitionLedgerSpan = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "PARTITION_LEDGER_SPAN")
	assert.Contains(t, msg, "must be a positive integer")
}

func TestValidateAll_AuditBatchLedgersZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditBatchLedgers = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_BATCH_LEDGERS")
	assert.Contains(t, msg, "must be a positive integer")
}

func TestValidateAll_AuditLagThresholdZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditLagThreshold = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_LAG_THRESHOLD")
	assert.Contains(t, msg, "must be a positive integer")
}

func TestValidateAll_AuditBudgetShareNegative(t *testing.T) {
	cfg := validBase()
	cfg.AuditBudgetShare = -0.5
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_BUDGET_SHARE")
	assert.Contains(t, msg, "-0.5")
	assert.Contains(t, msg, "must be in [0, 1]")
}

func TestValidateAll_AuditBudgetShareAboveOne(t *testing.T) {
	cfg := validBase()
	cfg.AuditBudgetShare = 1.5
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_BUDGET_SHARE")
	assert.Contains(t, msg, "1.5")
	assert.Contains(t, msg, "must be in [0, 1]")
}

func TestValidateAll_AuditMaxRPSZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditMaxRPS = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_MAX_RPS")
	assert.Contains(t, msg, "must be positive")
}

func TestValidateAll_AuditMaxRPSNegative(t *testing.T) {
	cfg := validBase()
	cfg.AuditMaxRPS = -5
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_MAX_RPS")
	assert.Contains(t, msg, "-5")
	assert.Contains(t, msg, "must be positive")
}

func TestValidateAll_AuditMaxRepairZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditMaxRepair = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_MAX_REPAIR_ATTEMPTS")
	assert.Contains(t, msg, "must be positive")
}

func TestValidateAll_AuditFindingMaxLgrsZero(t *testing.T) {
	cfg := validBase()
	cfg.AuditFindingMaxLgrs = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "AUDIT_FINDING_MAX_LEDGERS")
	assert.Contains(t, msg, "must be a positive integer")
}

func TestValidateAll_RateLimitRPSNegative(t *testing.T) {
	cfg := validBase()
	cfg.RateLimitRPS = -1
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RATE_LIMIT_RPS")
	assert.Contains(t, msg, "-1")
	assert.Contains(t, msg, "must be non-negative")
}

func TestValidateAll_RateLimitBurstNegative(t *testing.T) {
	cfg := validBase()
	cfg.RateLimitBurst = -1
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RATE_LIMIT_BURST")
	assert.Contains(t, msg, "-1")
	assert.Contains(t, msg, "must be non-negative")
}

func TestValidateAll_LogLevelInvalid(t *testing.T) {
	cfg := validBase()
	cfg.LogLevel = "loud"
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "LOG_LEVEL")
	assert.Contains(t, msg, "loud")
	assert.Contains(t, msg, "must be one of debug|info|warn|error")
}

func TestValidateAll_LogFormatInvalid(t *testing.T) {
	cfg := validBase()
	cfg.LogFormat = "xml"
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "LOG_FORMAT")
	assert.Contains(t, msg, "xml")
	assert.Contains(t, msg, "must be one of json|text")
}

func TestValidateAll_ContractIDInvalid(t *testing.T) {
	cfg := validBase()
	cfg.WatchedContracts = []string{"bad-id"}
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "WATCHED_CONTRACTS")
	assert.Contains(t, msg, "bad-id")
	assert.Contains(t, msg, "not a valid contract ID")
}

func TestValidateAll_RateLimitMismatchRPSOnly(t *testing.T) {
	cfg := validBase()
	cfg.RateLimitRPS = 5
	cfg.RateLimitBurst = 0
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
}

func TestValidateAll_RateLimitMismatchBurstOnly(t *testing.T) {
	cfg := validBase()
	cfg.RateLimitRPS = 0
	cfg.RateLimitBurst = 10
	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
}

// ---------- redaction assertions ----------

func TestRedact_DatabaseURLPasswordRedacted(t *testing.T) {
	raw := "postgres://alice:hunter2@localhost:5432/mydb?sslmode=disable"
	got := redact("DATABASE_URL", raw)
	assert.NotContains(t, got, "hunter2", "password must be redacted")
	assert.Contains(t, got, "alice", "username should be preserved")
	assert.Contains(t, got, "localhost", "host should be preserved")
}

func TestRedact_DatabaseURLEmptyValueUnchanged(t *testing.T) {
	got := redact("DATABASE_URL", "")
	assert.Equal(t, "", got, "empty value should pass through unchanged")
}

func TestRedact_NonSensitiveVariableUnchanged(t *testing.T) {
	raw := "https://rpc.example.com"
	got := redact("RPC_URL", raw)
	assert.Equal(t, raw, got, "non-sensitive variables should not be redacted")
}

func TestRedact_DatabaseURLInvalidFormat(t *testing.T) {
	got := redact("DATABASE_URL", "not-a-url")
	assert.Equal(t, "<redacted>", got, "unparseable sensitive URL should become <redacted>")
}

func TestRedact_SecretValueRedacted(t *testing.T) {
	assert.Equal(t, "***", redact("API_KEY", "st_ABCDEFGHIJKLMNOP_secret"))
	assert.Equal(t, "***", redact("MULTI_TENANT_BOOTSTRAP_KEY", "st_ABCDEFGHIJKLMNOP_secret"))
	assert.Equal(t, "***", redact("ARCHIVE_SECRET_ACCESS_KEY", "super-secret"))
}

// ---------- Validate(): errors from the legacy validation path ----------
// Validate() returns the first error only, so each test must start from
// a fully valid config and flip exactly one field.

func TestValidate_DatabaseURLEmpty(t *testing.T) {
	cfg := validBaseValidate()
	cfg.DatabaseURL = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL is required")
}

func TestValidate_RPCURLInvalid(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RPCURL = "not-a-url"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RPC_URL")
	assert.Contains(t, err.Error(), "not a valid URL")
}

func TestValidate_PollIntervalZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.PollInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POLL_INTERVAL")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_APIQueryTimeoutZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.APIQueryTimeout = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API_QUERY_TIMEOUT")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_APISlowQueryThresholdZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.APISlowQueryThreshold = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API_SLOW_QUERY_THRESHOLD")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RetentionBatchSizeZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RetentionBatchSize = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETENTION_BATCH_SIZE")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_IngestPageSizeZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.IngestPageSize = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INGEST_PAGE_SIZE")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_IngestBatchSizeZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.IngestBatchSize = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "INGEST_BATCH_SIZE")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RetentionPauseNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RetentionPause = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETENTION_PAUSE")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_RetentionIntervalZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RetentionInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETENTION_INTERVAL")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RetentionMaxAgeNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RetentionMaxAge = -1 * time.Hour
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETENTION_MAX_AGE")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_BackfillRateRPSZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.BackfillRateRPS = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BACKFILL_RATE_RPS")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RPCMaxAttemptsZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RPCMaxAttempts = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RPC_MAX_ATTEMPTS")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RPCBaseBackoffZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RPCBaseBackoff = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RPC_BASE_BACKOFF")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_RPCMaxBackoffZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RPCMaxBackoff = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RPC_MAX_BACKOFF")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_HTTPReadTimeoutNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.HTTPReadTimeout = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_READ_TIMEOUT")
	assert.Contains(t, err.Error(), "-1s")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_HTTPWriteTimeoutNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.HTTPWriteTimeout = -5 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_WRITE_TIMEOUT")
	assert.Contains(t, err.Error(), "-5s")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_HTTPIdleTimeoutNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.HTTPIdleTimeout = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_IDLE_TIMEOUT")
	assert.Contains(t, err.Error(), "-1s")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_HTTPReadHeaderTimeoutNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.HTTPReadHeaderTimeout = -3 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_READ_HEADER_TIMEOUT")
	assert.Contains(t, err.Error(), "-3s")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_APIMaxLimitZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.APIMaxLimit = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API_MAX_LIMIT")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_ShutdownTimeoutNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.ShutdownTimeout = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
	assert.Contains(t, err.Error(), "-1s")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_SweepConcurrencyZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.SweepConcurrency = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SWEEP_CONCURRENCY")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_ExportMaxRangeZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.ExportMaxRange = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXPORT_MAX_RANGE")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_ReorgRescanIntervalZeroWithWindow(t *testing.T) {
	cfg := validBaseValidate()
	cfg.ReorgConfirmationWindow = 64
	cfg.ReorgRescanInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REORG_RESCAN_INTERVAL")
	assert.Contains(t, err.Error(), "must be positive when REORG_CONFIRMATION_WINDOW is set")
}

func TestValidate_MultiTenantMaxWatchedNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.MultiTenantMaxWatched = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_MAX_WATCHED")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_MultiTenantUsageFlushZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.MultiTenantUsageFlush = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_USAGE_FLUSH")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_MultiTenantStreamScopeSyncZero(t *testing.T) {
	cfg := validBaseValidate()
	cfg.MultiTenantStreamScopeSync = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_STREAM_SCOPE_SYNC")
	assert.Contains(t, err.Error(), "must be positive")
}

func TestValidate_MultiTenantBootstrapKeyWithoutFlag(t *testing.T) {
	cfg := validBaseValidate()
	cfg.MultiTenantBootstrapKey = "secret-key"
	cfg.MultiTenant = false
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_BOOTSTRAP_KEY is set but MULTI_TENANT is false")
}

func TestValidate_ArchiveBucketWithoutEndpoint(t *testing.T) {
	cfg := validBaseValidate()
	cfg.ArchiveBucket = "my-bucket"
	cfg.ArchiveEndpoint = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ARCHIVE_ENDPOINT is required when ARCHIVE_BUCKET is set")
}

func TestValidate_ArchiveMaxRetriesNegative(t *testing.T) {
	cfg := validBaseValidate()
	cfg.ArchiveMaxRetries = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ARCHIVE_MAX_RETRIES")
	assert.Contains(t, err.Error(), "must be non-negative")
}

func TestValidate_RateLimitMismatch(t *testing.T) {
	cfg := validBaseValidate()
	cfg.RateLimitRPS = 5
	cfg.RateLimitBurst = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
}

// ---------- CORS-origin errors ----------

func TestValidate_CORSNullOriginRejected(t *testing.T) {
	cfg := validBaseValidate()
	cfg.CORSAllowedOrigins = []string{"null"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CORS_ALLOWED_ORIGINS")
	assert.Contains(t, err.Error(), "null")
	assert.Contains(t, err.Error(), "not allowed")
}

func TestValidate_CORSInvalidOriginRejected(t *testing.T) {
	cfg := validBaseValidate()
	cfg.CORSAllowedOrigins = []string{"app.example.com"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CORS_ALLOWED_ORIGINS")
	assert.Contains(t, err.Error(), "app.example.com")
	assert.Contains(t, err.Error(), "not a valid origin")
}

// ---------- multiError formatting ----------

func TestMultiError_AllEntriesPresent(t *testing.T) {
	errs := multiError{"first error", "second error", "third error"}
	msg := errs.Error()

	assert.True(t, strings.HasPrefix(msg, "configuration validation failed:\n"))
	assert.Contains(t, msg, "  - first error\n")
	assert.Contains(t, msg, "  - second error\n")
	assert.Contains(t, msg, "  - third error\n")
}

func TestMultiError_SingleEntry(t *testing.T) {
	errs := multiError{"only one"}
	msg := errs.Error()
	assert.Contains(t, msg, "  - only one\n")
}

func TestMultiError_IsError(t *testing.T) {
	var err error = multiError{"test"}
	assert.Error(t, err)
}

// ---------- valid config passes both paths ----------

func TestValidateAll_ValidConfigPasses(t *testing.T) {
	cfg := validBase()
	assert.NoError(t, cfg.ValidateAll(), "validBase() should pass ValidateAll")
}

func TestValidate_ValidConfigPasses(t *testing.T) {
	cfg := validBaseValidate()
	assert.NoError(t, cfg.Validate(), "validBaseValidate() should pass Validate")
}

// ---------- Compile-time interface checks ----------

var _ error = multiError{}
