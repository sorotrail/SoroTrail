package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// envKeys is the comprehensive list of env vars Load reads. Each test
// subtest clears them so leftover values from the host environment or a
// prior test don't leak across cases.
var envKeys = []string{
	"RPC_URL", "DATABASE_URL", "POLL_INTERVAL", "HTTP_ADDR",
	"WATCHED_CONTRACTS", "START_LEDGER", "RETENTION_LEDGERS", "LOG_LEVEL",
	"API_QUERY_TIMEOUT", "API_SLOW_QUERY_THRESHOLD",
	"HORIZON_URL", "BACKFILL_RATE_RPS",
	"AUDIT_ENABLED", "AUDIT_POLL_INTERVAL", "AUDIT_BATCH_LEDGERS",
	"AUDIT_LAG_THRESHOLD", "AUDIT_BUDGET_SHARE", "AUDIT_MAX_RPS",
	"AUDIT_MAX_REPAIR_ATTEMPTS", "AUDIT_FINDING_MAX_LEDGERS",
	"RATE_LIMIT_RPS", "RATE_LIMIT_BURST", "RATE_LIMIT_TRUSTED_PROXY",
	"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT",
	"HTTP_READ_HEADER_TIMEOUT",
	"SHUTDOWN_TIMEOUT",
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, c Config)
	}{
		{
			name: "defaults with only DATABASE_URL",
			env:  map[string]string{"DATABASE_URL": "postgres://localhost/db"},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "https://soroban-testnet.stellar.org", c.RPCURL)
				assert.Equal(t, 5*time.Second, c.PollInterval)
				assert.Equal(t, 25*time.Second, c.APIQueryTimeout)
				assert.Equal(t, 2*time.Second, c.APISlowQueryThreshold)
				assert.Equal(t, ":8080", c.HTTPAddr)
				assert.Equal(t, uint32(17280), c.RetentionLedgers)
				assert.Equal(t, uint32(120960), c.PartitionLedgerSpan)
				assert.Empty(t, c.WatchedContracts)
				assert.Zero(t, c.RateLimitRPS, "rate limiter disabled by default")
				assert.Zero(t, c.RateLimitBurst)
				assert.False(t, c.RateLimitTrustedProxy)

				assert.Equal(t, 30*time.Second, c.HTTPReadTimeout)
				assert.Equal(t, 30*time.Second, c.HTTPWriteTimeout)
				assert.Equal(t, 60*time.Second, c.HTTPIdleTimeout)
				assert.Equal(t, 10*time.Second, c.HTTPReadHeaderTimeout)
			},
		},
		{
			name:    "missing DATABASE_URL",
			env:     map[string]string{},
			wantErr: "DATABASE_URL is required",
		},
		{
			name: "watched contracts parsed and trimmed",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"WATCHED_CONTRACTS": validContract + ", " + validContract + " ,",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{validContract, validContract}, c.WatchedContracts)
			},
		},
		{
			name: "invalid watched contract rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"WATCHED_CONTRACTS": "not-a-contract",
			},
			wantErr: "not a valid contract ID",
		},
		{
			name: "bad poll interval",
			env: map[string]string{
				"DATABASE_URL":  "postgres://localhost/db",
				"POLL_INTERVAL": "-3s",
			},
			wantErr: "POLL_INTERVAL must be positive",
		},
		{
			name: "bad query timeout",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"API_QUERY_TIMEOUT": "0s",
			},
			wantErr: "API_QUERY_TIMEOUT",
		},
		{
			name: "bad log level",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_LEVEL":    "loud",
			},
			wantErr: "LOG_LEVEL",
		},
		{
			name: "bad rpc url",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URL":      "not a url",
			},
			wantErr: "RPC_URL",
		},
		{
			name: "http timeouts custom values accepted",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"HTTP_READ_TIMEOUT":        "15s",
				"HTTP_WRITE_TIMEOUT":       "20s",
				"HTTP_IDLE_TIMEOUT":        "90s",
				"HTTP_READ_HEADER_TIMEOUT": "5s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 15*time.Second, c.HTTPReadTimeout)
				assert.Equal(t, 20*time.Second, c.HTTPWriteTimeout)
				assert.Equal(t, 90*time.Second, c.HTTPIdleTimeout)
				assert.Equal(t, 5*time.Second, c.HTTPReadHeaderTimeout)
			},
		},
		{
			name: "http timeouts zero is accepted (disables timeout)",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"HTTP_READ_TIMEOUT":        "0s",
				"HTTP_WRITE_TIMEOUT":       "0s",
				"HTTP_IDLE_TIMEOUT":        "0s",
				"HTTP_READ_HEADER_TIMEOUT": "0s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, time.Duration(0), c.HTTPReadTimeout)
				assert.Equal(t, time.Duration(0), c.HTTPWriteTimeout)
				assert.Equal(t, time.Duration(0), c.HTTPIdleTimeout)
				assert.Equal(t, time.Duration(0), c.HTTPReadHeaderTimeout)
			},
		},
		{
			name: "negative http read timeout rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"HTTP_READ_TIMEOUT": "-1s",
			},
			wantErr: "HTTP_READ_TIMEOUT must be non-negative",
		},
		{
			name: "negative http write timeout rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"HTTP_WRITE_TIMEOUT": "-5s",
			},
			wantErr: "HTTP_WRITE_TIMEOUT must be non-negative",
		},
		{
			name: "negative http idle timeout rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"HTTP_IDLE_TIMEOUT": "-1s",
			},
			wantErr: "HTTP_IDLE_TIMEOUT must be non-negative",
		},
		{
			name: "negative http read header timeout rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"HTTP_READ_HEADER_TIMEOUT": "-3s",
			},
			wantErr: "HTTP_READ_HEADER_TIMEOUT must be non-negative",
		},
		{
			name: "shutdown timeout defaults to 15s",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 15*time.Second, c.ShutdownTimeout)
			},
		},
		{
			name: "shutdown timeout custom value accepted",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"SHUTDOWN_TIMEOUT": "30s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 30*time.Second, c.ShutdownTimeout)
			},
		},
		{
			name: "shutdown timeout zero accepted (no timeout)",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"SHUTDOWN_TIMEOUT": "0s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, time.Duration(0), c.ShutdownTimeout)
			},
		},
		{
			name: "negative shutdown timeout rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"SHUTDOWN_TIMEOUT": "-1s",
			},
			wantErr: "SHUTDOWN_TIMEOUT must be non-negative",
		},
		{
			name: "rate limit both set is accepted",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"RATE_LIMIT_RPS":           "5",
				"RATE_LIMIT_BURST":         "10",
				"RATE_LIMIT_TRUSTED_PROXY": "true",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(5), c.RateLimitRPS)
				assert.Equal(t, 10, c.RateLimitBurst)
				assert.True(t, c.RateLimitTrustedProxy)
			},
		},
		{
			name: "rate limit only RPS set is rejected",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/db",
				"RATE_LIMIT_RPS": "5",
			},
			wantErr: "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset",
		},
		{
			name: "rate limit only Burst set is rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RATE_LIMIT_BURST": "10",
			},
			wantErr: "RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset",
		},
		{
			name: "rate limit negative RPS rejected",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/db",
				"RATE_LIMIT_RPS": "-1",
			},
			wantErr: "RATE_LIMIT_RPS must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the variables Load reads, then apply the case's env.
			// t.Setenv registers restoration; Unsetenv makes defaults apply.
			for _, key := range envKeys {
				t.Setenv(key, "")
				os.Unsetenv(key)
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cfg, err := Load()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestValidContractID(t *testing.T) {
	assert.True(t, ValidContractID(validContract))
	assert.False(t, ValidContractID(""))
	assert.False(t, ValidContractID("G"+validContract[1:]), "account keys are not contracts")
	assert.False(t, ValidContractID(validContract[:55]), "too short")
	assert.False(t, ValidContractID(validContract[:55]+"a"), "lowercase is not base32")
}

func TestValidCursor(t *testing.T) {
	assert.True(t, ValidCursor("0001099511627776-0000000001"))
	assert.True(t, ValidCursor("00000000000000000102-00000"))
	assert.True(t, ValidCursor("e1"))
	assert.True(t, ValidCursor("cursor-42"))
	assert.True(t, ValidCursor("pt_1"))
	assert.True(t, ValidCursor("abc.123:45_67-89"))

	assert.False(t, ValidCursor(""), "empty string")
	assert.False(t, ValidCursor("invalid cursor"), "contains space")
	assert.False(t, ValidCursor("e1; DROP TABLE events;"), "contains semicolon and space")
	assert.False(t, ValidCursor("cursor'OR'1'='1"), "contains single quotes")
	assert.False(t, ValidCursor("<script>alert(1)</script>"), "contains angle brackets")
	assert.False(t, ValidCursor("e1\n"), "contains newline")
	assert.False(t, ValidCursor(string(make([]byte, 129))), "too long (>128 chars)")
}
