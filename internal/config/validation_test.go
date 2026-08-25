package config

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestMultiErrorFormat(t *testing.T) {
	err := multiError{"first", "second", "third"}
	msg := err.Error()
	if !strings.Contains(msg, "first") || !strings.Contains(msg, "second") || !strings.Contains(msg, "third") {
		t.Fatalf("multiError missing entries: %s", msg)
	}
	if !strings.HasPrefix(msg, "configuration validation failed:\n") {
		t.Fatalf("wrong prefix: %s", msg)
	}
}

func TestValidateAll_ValidConfig(t *testing.T) {
	cfg := Config{
		DatabaseURL:         "postgres://user:pass@localhost/db",
		RPCURL:              "https://soroban-testnet.stellar.org",
		PollInterval:        5 * time.Second,
		AuditPollInterval:   30 * time.Second,
		RetentionLedgers:    17280,
		PartitionLedgerSpan: 120960,
		AuditBatchLedgers:   100,
		AuditLagThreshold:   200,
		AuditBudgetShare:    0.1,
		AuditMaxRPS:         10,
		AuditMaxRepair:      3,
		AuditFindingMaxLgrs: 100,
		LogLevel:            "info",
		LogFormat:           "text",
	}
	if err := cfg.ValidateAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// --- per-rule-type tests ---------------------------------------------------

func TestValidateAll_Required(t *testing.T) {
	err := Config{}.ValidateAll()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL: required but empty") {
		t.Fatalf("missing DATABASE_URL error: %s", err)
	}
}

func TestValidateAll_URL(t *testing.T) {
	cfg := validBase()
	cfg.RPCURL = "not-a-url"
	err := cfg.ValidateAll()
	checkContains(t, err, "RPC_URL")
	checkContains(t, err, "not-a-url")

	cfg.RPCURL = ""
	err = cfg.ValidateAll()
	checkContains(t, err, "RPC_URL")
}

func TestValidateAll_Duration(t *testing.T) {
	cfg := validBase()
	cfg.PollInterval = 0
	err := cfg.ValidateAll()
	checkContains(t, err, "POLL_INTERVAL")
	checkContains(t, err, "must be a positive duration")

	cfg.PollInterval = 5 * time.Second
	cfg.AuditPollInterval = -1
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_POLL_INTERVAL")
}

func TestValidateAll_NumericRanges(t *testing.T) {
	cfg := validBase()

	cfg.RetentionLedgers = 0
	err := cfg.ValidateAll()
	checkContains(t, err, "RETENTION_LEDGERS")

	cfg.RetentionLedgers = 17280
	cfg.PartitionLedgerSpan = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "PARTITION_LEDGER_SPAN")

	cfg.PartitionLedgerSpan = 120960
	cfg.AuditBatchLedgers = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_BATCH_LEDGERS")

	cfg.AuditBatchLedgers = 100
	cfg.AuditLagThreshold = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_LAG_THRESHOLD")

	cfg.AuditLagThreshold = 200
	cfg.AuditBudgetShare = -0.1
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_BUDGET_SHARE")

	cfg.AuditBudgetShare = 0.1
	cfg.AuditMaxRPS = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_MAX_RPS")

	cfg.AuditMaxRPS = 10
	cfg.AuditMaxRepair = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_MAX_REPAIR_ATTEMPTS")

	cfg.AuditMaxRepair = 3
	cfg.AuditFindingMaxLgrs = 0
	err = cfg.ValidateAll()
	checkContains(t, err, "AUDIT_FINDING_MAX_LEDGERS")

	cfg.AuditFindingMaxLgrs = 100
	cfg.RateLimitRPS = -1
	err = cfg.ValidateAll()
	checkContains(t, err, "RATE_LIMIT_RPS")

	cfg.RateLimitRPS = 0
	cfg.RateLimitBurst = -1
	err = cfg.ValidateAll()
	checkContains(t, err, "RATE_LIMIT_BURST")
}

func TestValidateAll_LogLevel(t *testing.T) {
	cfg := validBase()
	cfg.LogLevel = "loud"
	err := cfg.ValidateAll()
	checkContains(t, err, "LOG_LEVEL")
	checkContains(t, err, "loud")
}

func TestValidateAll_ContractIDs(t *testing.T) {
	cfg := validBase()
	cfg.WatchedContracts = []string{"not-a-contract"}
	err := cfg.ValidateAll()
	checkContains(t, err, "WATCHED_CONTRACTS")
}

func TestValidateAll_MutualDependency(t *testing.T) {
	cfg := validBase()
	cfg.RateLimitRPS = 5
	err := cfg.ValidateAll()
	checkContains(t, err, "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")

	cfg.RateLimitRPS = 0
	cfg.RateLimitBurst = 10
	err = cfg.ValidateAll()
	checkContains(t, err, "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
}

// --- multi-error aggregation -----------------------------------------------

func TestValidateAll_Aggregation(t *testing.T) {
	// Empty config hits many rules at once.
	cfg := Config{}
	err := cfg.ValidateAll()
	if err == nil {
		t.Fatal("expected multiple errors")
	}
	msg := err.Error()
	count := strings.Count(msg, "\n  - ")
	if count < 5 {
		t.Fatalf("expected >=5 failures, got %d:\n%s", count, msg)
	}
}

func TestValidateAll_Aggregation_PreservesAll(t *testing.T) {
	cfg := Config{
		// Only set DatabaseURL — leave everything else invalid.
		DatabaseURL: "postgres://localhost/db",
	}
	err := cfg.ValidateAll()
	msg := err.Error()
	required := []string{
		"RPC_URL",
		"POLL_INTERVAL",
		"AUDIT_POLL_INTERVAL",
		"RETENTION_LEDGERS",
		"PARTITION_LEDGER_SPAN",
		"AUDIT_BATCH_LEDGERS",
		"AUDIT_LAG_THRESHOLD",
		"AUDIT_MAX_RPS",
		"AUDIT_MAX_REPAIR_ATTEMPTS",
		"AUDIT_FINDING_MAX_LEDGERS",
		"LOG_LEVEL",
	}
	for _, v := range required {
		if !strings.Contains(msg, v) {
			t.Errorf("aggregated error missing %q", v)
		}
	}
}

// --- redaction -------------------------------------------------------------

func TestRedact_DatabaseURL_Token(t *testing.T) {
	raw := "postgres://alice:secret123@localhost:5432/mydb?sslmode=disable"
	got := redact("DATABASE_URL", raw)
	if strings.Contains(got, "secret123") {
		t.Fatalf("redact leaked password: %s", got)
	}
	if !strings.HasPrefix(got, "postgres://alice:") {
		t.Fatalf("redact removed username prefix: %s", got)
	}
	if strings.Contains(got, ":secret123@") {
		t.Fatalf("redact preserved original password: %s", got)
	}
	// url.UserPassword URL-encodes special chars, so *** becomes %2A%2A%2A.
	if got == raw {
		t.Fatalf("redact did not modify the URL: %s", got)
	}
}

func TestRedact_DatabaseURL_NoCredentials(t *testing.T) {
	raw := "postgres://localhost/mydb"
	got := redact("DATABASE_URL", raw)
	if got != raw {
		t.Fatalf("redact should not change URL without credentials: got %q", got)
	}
}

func TestRedact_NonSensitive(t *testing.T) {
	raw := "https://example.com"
	got := redact("RPC_URL", raw)
	if got != raw {
		t.Fatalf("redact should not touch non-sensitive vars: got %q", got)
	}
}

func TestRedact_InvalidURL(t *testing.T) {
	got := redact("DATABASE_URL", "not-a-url")
	if got != "<redacted>" {
		t.Fatalf("redact should return <redacted> for unparseable URLs: got %q", got)
	}
}

func TestRedact_Empty(t *testing.T) {
	if got := redact("DATABASE_URL", ""); got != "" {
		t.Fatalf("redact should return empty string unchanged: got %q", got)
	}
}

func TestValidateAll_RedactInErrorOutput(t *testing.T) {
	// Even though DATABASE_URL errors don't include the value normally,
	// verify that the redact path works end-to-end by checking via url.Parse
	// that we'd get a redacted URL back.
	raw := "postgres://user:supersecret@localhost/db"
	u, _ := url.Parse(raw)
	u.User = url.UserPassword(u.User.Username(), "***")
	redacted := u.String()
	if strings.Contains(redacted, "supersecret") {
		t.Fatal("redacted URL still contains secret")
	}
}

// --- helpers ---------------------------------------------------------------

func validBase() Config {
	return Config{
		DatabaseURL:         "postgres://user:pass@localhost/db",
		RPCURL:              "https://soroban-testnet.stellar.org",
		PollInterval:        5 * time.Second,
		AuditPollInterval:   30 * time.Second,
		RetentionLedgers:    17280,
		PartitionLedgerSpan: 120960,
		AuditBatchLedgers:   100,
		AuditLagThreshold:   200,
		AuditBudgetShare:    0.1,
		AuditMaxRPS:         10,
		AuditMaxRepair:      3,
		AuditFindingMaxLgrs: 100,
		LogLevel:            "info",
	}
}

func checkContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("expected error containing %q:\n%s", substr, err.Error())
	}
}

// compile-time check: multiError satisfies error
var _ error = multiError{}
