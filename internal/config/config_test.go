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
	"RPC_URL", "RPC_URLS", "RPC_RATE_LIMIT_RPS", "DATABASE_URL",
	"POLL_INTERVAL", "HTTP_ADDR",
	"WATCHED_CONTRACTS", "START_LEDGER", "RETENTION_LEDGERS", "LOG_LEVEL",
	"AUDIT_ENABLED", "AUDIT_POLL_INTERVAL", "AUDIT_BATCH_LEDGERS",
	"AUDIT_LAG_THRESHOLD", "AUDIT_BUDGET_SHARE", "AUDIT_MAX_RPS",
	"AUDIT_MAX_REPAIR_ATTEMPTS", "AUDIT_FINDING_MAX_LEDGERS",
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
			name: "RPC_URLS with valid URLs accepted",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URLS":     "https://rpc1.example.com,https://rpc2.example.com",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{"https://rpc1.example.com", "https://rpc2.example.com"}, c.RPCURLS)
				assert.Equal(t, float64(10), c.RPCRateLimitRPS)
			},
		},
		{
			name: "RPC_URLS invalid URL rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URLS":     "https://good.example.com,not a url",
			},
			wantErr: "RPC_URLS[1]",
		},
		{
			name: "RPC_URLS empty entries trimmed",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URLS":     "https://rpc.example.com, ,",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{"https://rpc.example.com"}, c.RPCURLS)
			},
		},
		{
			name: "RPC_RATE_LIMIT_RPS custom value",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"RPC_RATE_LIMIT_RPS": "5",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(5), c.RPCRateLimitRPS)
			},
		},
		{
			name: "RPC_RATE_LIMIT_RPS zero rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"RPC_RATE_LIMIT_RPS": "0",
			},
			wantErr: "RPC_RATE_LIMIT_RPS must be positive",
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
