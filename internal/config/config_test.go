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
	"NETWORKS", "NETWORK_NAME", "CACHE_PRIVATE", "PARTITION_LEDGER_SPAN",
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
				require.Len(t, c.Networks, 1)
				assert.Equal(t, "default", c.Networks[0].Name)
				assert.Equal(t, "https://soroban-testnet.stellar.org", c.Networks[0].RPCURL)
				assert.Equal(t, 5*time.Second, c.PollInterval)
				assert.Equal(t, ":8080", c.HTTPAddr)
				assert.Equal(t, uint32(17280), c.RetentionLedgers)
				assert.Equal(t, uint32(120960), c.PartitionLedgerSpan)
				assert.Empty(t, c.WatchedContracts)
				assert.Zero(t, c.RateLimitRPS, "rate limiter disabled by default")
				assert.Zero(t, c.RateLimitBurst)
				assert.False(t, c.RateLimitTrustedProxy)
				assert.Equal(t, "default", c.DefaultNetwork())
			},
		},
		{
			name:    "missing DATABASE_URL",
			env:     map[string]string{},
			wantErr: "DATABASE_URL is required",
		},
		{
			name: "multi-network via NETWORKS",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"NETWORKS":     "testnet=https://soroban-testnet.stellar.org,mainnet=https://mainnet.soroban.org",
			},
			check: func(t *testing.T, c Config) {
				require.Len(t, c.Networks, 2)
				assert.Equal(t, "testnet", c.Networks[0].Name)
				assert.Equal(t, "https://soroban-testnet.stellar.org", c.Networks[0].RPCURL)
				assert.Equal(t, "mainnet", c.Networks[1].Name)
				assert.Equal(t, "https://mainnet.soroban.org", c.Networks[1].RPCURL)
				assert.Empty(t, c.DefaultNetwork(), "multiple networks: no default")
				assert.Equal(t, []string{"testnet", "mainnet"}, c.NetworkNames())
			},
		},
		{
			name: "custom network name",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URL":      "https://custom.example.com",
				"NETWORK_NAME": "staging",
			},
			check: func(t *testing.T, c Config) {
				require.Len(t, c.Networks, 1)
				assert.Equal(t, "staging", c.Networks[0].Name)
				assert.Equal(t, "https://custom.example.com", c.Networks[0].RPCURL)
				assert.Equal(t, "staging", c.DefaultNetwork())
			},
		},
		{
			name: "NETWORKS takes precedence over RPC_URL",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URL":      "https://old.example.com",
				"NETWORKS":     "testnet=https://soroban-testnet.stellar.org",
			},
			check: func(t *testing.T, c Config) {
				require.Len(t, c.Networks, 1)
				assert.Equal(t, "testnet", c.Networks[0].Name)
				assert.Equal(t, "https://soroban-testnet.stellar.org", c.Networks[0].RPCURL)
			},
		},
		{
			name: "NETWORKS with spaces and URL containing equals",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"NETWORKS":     "testnet=https://soroban-testnet.stellar.org?apikey=abc123,  mainnet=https://mainnet.soroban.org  ",
			},
			check: func(t *testing.T, c Config) {
				require.Len(t, c.Networks, 2)
				assert.Equal(t, "testnet", c.Networks[0].Name)
				assert.Contains(t, c.Networks[0].RPCURL, "apikey=abc123")
				assert.Equal(t, "mainnet", c.Networks[1].Name)
			},
		},
		{
			name: "NETWORKS with duplicate names rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"NETWORKS":     "dup=https://a.com,dup=https://b.com",
			},
			wantErr: "duplicate network name",
		},
		{
			name: "NETWORKS with invalid URL rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"NETWORKS":     "bad=not-a-valid-url",
			},
			wantErr: "not a valid URL",
		},
		{
			name: "NETWORKS with empty name rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"NETWORKS":     "=https://example.com",
			},
			wantErr: "invalid network entry",
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

func TestParseNetworks(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []NetworkConfig
		wantErr string
	}{
		{
			name: "single network",
			raw:  "testnet=https://testnet.example.com",
			want: []NetworkConfig{{Name: "testnet", RPCURL: "https://testnet.example.com"}},
		},
		{
			name: "two networks",
			raw:  "testnet=https://testnet.example.com,mainnet=https://mainnet.example.com",
			want: []NetworkConfig{
				{Name: "testnet", RPCURL: "https://testnet.example.com"},
				{Name: "mainnet", RPCURL: "https://mainnet.example.com"},
			},
		},
		{
			name: "trims whitespace",
			raw:  "  testnet  =  https://testnet.example.com  , mainnet = https://mainnet.example.com ",
			want: []NetworkConfig{
				{Name: "testnet", RPCURL: "https://testnet.example.com"},
				{Name: "mainnet", RPCURL: "https://mainnet.example.com"},
			},
		},
		{
			name:    "empty string returns error",
			raw:     "",
			wantErr: "parsed to zero networks",
		},
		{
			name:    "missing equals returns error",
			raw:     "testnet",
			wantErr: "invalid network entry",
		},
		{
			name:    "empty name returns error",
			raw:     "=https://example.com",
			wantErr: "invalid network entry",
		},
		{
			name:    "duplicate names returns error",
			raw:     "dup=https://a.com,dup=https://b.com",
			wantErr: "duplicate network name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNetworks(tt.raw)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNetworkByURL(t *testing.T) {
	cfg := Config{
		Networks: []NetworkConfig{
			{Name: "testnet", RPCURL: "https://testnet.example.com"},
			{Name: "mainnet", RPCURL: "https://mainnet.example.com"},
		},
	}
	assert.Equal(t, "testnet", cfg.NetworkByURL("https://testnet.example.com"))
	assert.Equal(t, "mainnet", cfg.NetworkByURL("https://mainnet.example.com"))
	assert.Empty(t, cfg.NetworkByURL("https://unknown.example.com"))
}
