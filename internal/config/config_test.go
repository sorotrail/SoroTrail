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
	"AUDIT_ENABLED", "AUDIT_POLL_INTERVAL", "AUDIT_BATCH_LEDGERS",
	"AUDIT_LAG_THRESHOLD", "AUDIT_BUDGET_SHARE", "AUDIT_MAX_RPS",
	"AUDIT_MAX_REPAIR_ATTEMPTS", "AUDIT_FINDING_MAX_LEDGERS",
	"RATE_LIMIT_RPS", "RATE_LIMIT_BURST", "RATE_LIMIT_TRUSTED_PROXY",
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
				assert.Equal(t, ":8080", c.HTTPAddr)
				assert.Equal(t, uint32(17280), c.RetentionLedgers)
				assert.Empty(t, c.WatchedContracts)
				assert.Zero(t, c.RateLimitRPS, "rate limiter disabled by default")
				assert.Zero(t, c.RateLimitBurst)
				assert.False(t, c.RateLimitTrustedProxy)
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
			name: "retention max age enabled",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"RETENTION_MAX_AGE": "720h",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 720*time.Hour, c.RetentionMaxAge)
				assert.True(t, c.RetentionEnabled())
			},
		},
		{
			name: "retention min ledger enabled",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"RETENTION_MIN_LEDGER": "100000",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint64(100000), c.RetentionMinLedger)
				assert.True(t, c.RetentionEnabled())
			},
		},
		{
			name: "retention defaults when disabled",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, time.Duration(0), c.RetentionMaxAge)
				assert.Equal(t, uint64(0), c.RetentionMinLedger)
				assert.Equal(t, 5000, c.RetentionBatchSize)
				assert.Equal(t, 100*time.Millisecond, c.RetentionPause)
				assert.Equal(t, 1*time.Hour, c.RetentionInterval)
				assert.False(t, c.RetentionEnabled())
			},
		},
		{
			name: "bad retention batch size",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"RETENTION_BATCH_SIZE": "0",
			},
			wantErr: "RETENTION_BATCH_SIZE must be positive",
		},
		{
			name: "bad retention pause",
			env: map[string]string{
				"DATABASE_URL":    "postgres://localhost/db",
				"RETENTION_PAUSE": "-1s",
			},
			wantErr: "RETENTION_PAUSE must be non-negative",
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
			for _, key := range []string{"RPC_URL", "DATABASE_URL", "POLL_INTERVAL",
				"HTTP_ADDR", "WATCHED_CONTRACTS", "START_LEDGER", "RETENTION_LEDGERS", "LOG_LEVEL",
				"RETENTION_MAX_AGE", "RETENTION_MIN_LEDGER", "RETENTION_BATCH_SIZE",
				"RETENTION_PAUSE", "RETENTION_INTERVAL"} {
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
