package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

var envKeys = []string{
	"RPC_URL", "RPC_URLS", "RPC_RATE_LIMIT_RPS", "DATABASE_URL",
	"POLL_INTERVAL", "HTTP_ADDR",
	"WATCHED_CONTRACTS", "START_LEDGER", "RETENTION_LEDGERS", "LOG_LEVEL", "LOG_FORMAT",
	"API_QUERY_TIMEOUT", "API_SLOW_QUERY_THRESHOLD",
	"HORIZON_URL", "BACKFILL_RATE_RPS",
	"AUDIT_ENABLED", "AUDIT_POLL_INTERVAL", "AUDIT_BATCH_LEDGERS",
	"AUDIT_LAG_THRESHOLD", "AUDIT_BUDGET_SHARE", "AUDIT_MAX_RPS",
	"AUDIT_MAX_REPAIR_ATTEMPTS", "AUDIT_FINDING_MAX_LEDGERS",
	"RATE_LIMIT_RPS", "RATE_LIMIT_BURST", "RATE_LIMIT_TRUSTED_PROXY",
	"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT",
	"HTTP_READ_HEADER_TIMEOUT",
	"SHUTDOWN_TIMEOUT",
	"INGESTION_LOCK_ENABLED",
	"MAX_EVENTS_PER_CYCLE",
	"MULTI_TENANT", "MULTI_TENANT_MAX_WATCHED", "MULTI_TENANT_USAGE_FLUSH",
	"MULTI_TENANT_STREAM_SCOPE_SYNC", "MULTI_TENANT_BOOTSTRAP_KEY",
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
				assert.Equal(t, 5*time.Second, c.PollInterval)
				assert.Equal(t, ":8080", c.HTTPAddr)
				assert.Equal(t, uint32(17280), c.RetentionLedgers)
				assert.Equal(t, uint32(120960), c.PartitionLedgerSpan)
				assert.Empty(t, c.WatchedContracts)
				assert.Equal(t, uint32(100), c.LagWarnLedgers,
					"LagWarnLedgers default lets the lag alarm work out of the box")
			},
		},
		{
			name: "lag alarm threshold configurable",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"LAG_WARN_LEDGERS": "50",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint32(50), c.LagWarnLedgers)
			},
		},
		{
			name: "lag alarm threshold zero disables the alarm",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"LAG_WARN_LEDGERS": "0",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint32(0), c.LagWarnLedgers,
					"0 is the documented way to silence the alarm entirely")
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
			wantErr: "DATABASE_URL: required but empty",
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
			wantErr: "POLL_INTERVAL",
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
			name: "log level debug",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_LEVEL":    "debug",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "debug", c.LogLevel)
			},
		},
		{
			name: "log level info",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_LEVEL":    "info",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "info", c.LogLevel)
			},
		},
		{
			name: "log level warn",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_LEVEL":    "warn",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "warn", c.LogLevel)
			},
		},
		{
			name: "log level error",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_LEVEL":    "error",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "error", c.LogLevel)
			},
		},
		{
			name: "log level defaults to info",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "info", c.LogLevel)
			},
		},
		{
			name: "log format text",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_FORMAT":   "text",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "text", c.LogFormat)
			},
		},
		{
			name: "log format json",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_FORMAT":   "json",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "json", c.LogFormat)
			},
		},
		{
			name: "bad log format",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"LOG_FORMAT":   "xml",
			},
			wantErr: "LOG_FORMAT",
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
			wantErr: "RATE_LIMIT_RPS: -1 must be non-negative",
		},
		{
			name: "cors origins parsed, trimmed, and normalized",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "https://app.example.com, https://dashboard.example.com/ , *",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{
					"https://app.example.com",
					"https://dashboard.example.com",
					"*",
				}, c.CORSAllowedOrigins)
			},
		},
		{
			name: "cors wildcard accepted",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "*",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{"*"}, c.CORSAllowedOrigins)
			},
		},
		{
			name: "cors origin missing scheme rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "app.example.com",
			},
			wantErr: "CORS_ALLOWED_ORIGINS entry \"app.example.com\" is not a valid origin",
		},
		{
			name: "cors origin with non-http scheme rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "ftp://example.com",
			},
			wantErr: "CORS_ALLOWED_ORIGINS entry",
		},
		{
			name: "cors javascript scheme rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "javascript:alert(1)",
			},
			wantErr: "CORS_ALLOWED_ORIGINS entry",
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
		{
			name: "MAX_EVENTS_PER_CYCLE defaults to disabled",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint(0), c.MaxEventsPerCycle,
					"zero is the documented 'cap disabled' default")
			},
		},
		{
			name: "MAX_EVENTS_PER_CYCLE parsed",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"MAX_EVENTS_PER_CYCLE": "50000",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint(50000), c.MaxEventsPerCycle)
			},
		},
		{
			name: "negative MAX_EVENTS_PER_CYCLE rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"MAX_EVENTS_PER_CYCLE": "-1",
			},
			wantErr: "MaxEventsPerCycle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

func TestValidOrigin(t *testing.T) {
	assert.True(t, ValidOrigin("*"))
	assert.True(t, ValidOrigin("https://app.example.com"))
	assert.True(t, ValidOrigin("http://localhost:5173"))
	assert.True(t, ValidOrigin("https://a.example.com:8443"))

	assert.False(t, ValidOrigin(""), "empty string")
	assert.False(t, ValidOrigin("app.example.com"), "missing scheme")
	assert.False(t, ValidOrigin("ftp://example.com"), "non-http scheme")
	assert.False(t, ValidOrigin("javascript:alert(1)"), "javascript scheme")
	assert.False(t, ValidOrigin("https://"), "missing host")
	assert.False(t, ValidOrigin("https://example.com/path"), "origins cannot carry a path")
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

// Multi-tenancy is off unless asked for, and its knobs are validated.
func TestLoad_MultiTenancy(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, c Config)
	}{
		{
			name: "off by default",
			env:  map[string]string{"DATABASE_URL": "postgres://localhost/db"},
			check: func(t *testing.T, c Config) {
				assert.False(t, c.MultiTenant,
					"an upgraded deployment must not silently start requiring API keys")
				assert.Equal(t, 250, c.MultiTenantMaxWatched)
				assert.Equal(t, 10*time.Second, c.MultiTenantUsageFlush)
				assert.Equal(t, 30*time.Second, c.MultiTenantStreamScopeSync)
			},
		},
		{
			name: "enabled with overrides",
			env: map[string]string{
				"DATABASE_URL":                   "postgres://localhost/db",
				"MULTI_TENANT":                   "true",
				"MULTI_TENANT_MAX_WATCHED":       "50",
				"MULTI_TENANT_USAGE_FLUSH":       "5s",
				"MULTI_TENANT_STREAM_SCOPE_SYNC": "2s",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, c.MultiTenant)
				assert.Equal(t, 50, c.MultiTenantMaxWatched)
				assert.Equal(t, 5*time.Second, c.MultiTenantUsageFlush)
				assert.Equal(t, 2*time.Second, c.MultiTenantStreamScopeSync)
			},
		},
		{
			name: "zero disables the instance watch cap",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"MULTI_TENANT":             "true",
				"MULTI_TENANT_MAX_WATCHED": "0",
			},
			check: func(t *testing.T, c Config) { assert.Equal(t, 0, c.MultiTenantMaxWatched) },
		},
		{
			name: "a negative watch cap is rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"MULTI_TENANT_MAX_WATCHED": "-1",
			},
			wantErr: "MULTI_TENANT_MAX_WATCHED",
		},
		{
			name: "a non-positive scope sync is rejected",
			env: map[string]string{
				"DATABASE_URL":                   "postgres://localhost/db",
				"MULTI_TENANT_STREAM_SCOPE_SYNC": "0s",
			},
			wantErr: "MULTI_TENANT_STREAM_SCOPE_SYNC",
		},
		{
			// Silently ignoring this would leave an operator believing the
			// instance is protected when it is wide open.
			name: "a bootstrap key without multi-tenancy is rejected",
			env: map[string]string{
				"DATABASE_URL":               "postgres://localhost/db",
				"MULTI_TENANT_BOOTSTRAP_KEY": "st_ABCDEFGHIJKLMNOP_secret",
			},
			wantErr: "MULTI_TENANT_BOOTSTRAP_KEY is set but MULTI_TENANT is false",
		},
		{
			name: "a bootstrap key with multi-tenancy is accepted",
			env: map[string]string{
				"DATABASE_URL":               "postgres://localhost/db",
				"MULTI_TENANT":               "true",
				"MULTI_TENANT_BOOTSTRAP_KEY": "st_ABCDEFGHIJKLMNOP_secret",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "st_ABCDEFGHIJKLMNOP_secret", c.MultiTenantBootstrapKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
