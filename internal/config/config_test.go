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
	"HTTP_REQUEST_BODY_LIMIT", // body size limit
	"RPC_URL", "RPC_URLS", "RPC_RATE_LIMIT_RPS", "RPC_RATE_LIMIT", "DATABASE_URL",
	"POLL_INTERVAL", "HTTP_ADDR",
	"WATCHED_CONTRACTS", "START_LEDGER", "RETENTION_LEDGERS", "INGEST_PAGE_SIZE", "INGEST_BATCH_SIZE", "LOG_LEVEL", "LOG_FORMAT",
	"API_QUERY_TIMEOUT", "API_SLOW_QUERY_THRESHOLD",
	"HORIZON_URL", "BACKFILL_RATE_RPS",
	"AUDIT_ENABLED", "AUDIT_POLL_INTERVAL", "AUDIT_BATCH_LEDGERS",
	"AUDIT_LAG_THRESHOLD", "AUDIT_BUDGET_SHARE", "AUDIT_MAX_RPS",
	"AUDIT_MAX_REPAIR_ATTEMPTS", "AUDIT_FINDING_MAX_LEDGERS",
	"RATE_LIMIT_RPS", "RATE_LIMIT_BURST", "RATE_LIMIT_TRUSTED_PROXY",
	"API_KEY_AUTH_ENABLED",
	"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT",
	"HTTP_READ_HEADER_TIMEOUT",
	"SHUTDOWN_TIMEOUT",
	"INGESTION_LOCK_ENABLED",
	"MAX_EVENTS_PER_CYCLE",
	"BATCH_SIZE", "BATCH_TARGET_LATENCY", "BATCH_MAX_BACKOFF",
	"MULTI_TENANT", "MULTI_TENANT_MAX_WATCHED", "MULTI_TENANT_USAGE_FLUSH",
	"MULTI_TENANT_STREAM_SCOPE_SYNC", "MULTI_TENANT_BOOTSTRAP_KEY",
	"RETENTION_MAX_AGE", "RETENTION_MIN_LEDGER", "RETENTION_BATCH_SIZE",
	"RETENTION_PAUSE", "RETENTION_INTERVAL",
	"RPC_MAX_ATTEMPTS", "RPC_BASE_BACKOFF", "RPC_MAX_BACKOFF", "RPC_JITTER",
	"INGESTER_MIN_BACKOFF", "INGESTER_MAX_BACKOFF", "INGESTER_JITTER_MIN", "INGESTER_JITTER_MAX",
	"METRICS_ENABLED", "ENABLE_METRICS", "CACHE_PRIVATE", "COMPRESS_MIN_SIZE",
	"EXPORT_MAX_RANGE", "REORG_CONFIRMATION_WINDOW", "REORG_RESCAN_INTERVAL",
	"SWEEP_CONCURRENCY", "API_MAX_LIMIT",
	"STATS_CACHE_TTL",
	"CORS_ALLOWED_ORIGINS", "CORS_ALLOWED_METHODS", "CORS_ALLOWED_HEADERS",
	"CORS_EXPOSED_HEADERS", "GRAPHQL_PLAYGROUND",
}

func TestLoad_FileBackedSecrets(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/database_url.txt"
	require.NoError(t, os.WriteFile(dbPath, []byte("postgres://user:pass@localhost/db\n"), 0o600))
	rpcPath := dir + "/rpc_url.txt"
	require.NoError(t, os.WriteFile(rpcPath, []byte("https://user:pass@rpc.example.com\n"), 0o600))

	t.Setenv("DATABASE_URL_FILE", dbPath)
	t.Setenv("RPC_URL_FILE", rpcPath)

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "postgres://user:pass@localhost/db", cfg.DatabaseURL)
	assert.Equal(t, "https://user:pass@rpc.example.com", cfg.RPCURL)
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, c Config)
	}{
		{
			name: "HTTP_REQUEST_BODY_LIMIT env variable overrides default",
			env:  map[string]string{"DATABASE_URL": "postgres://localhost/db", "HTTP_REQUEST_BODY_LIMIT": "8192"},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, int64(8192), c.HTTPRequestBodyLimit, "Request body limit from env")
			},
		},
		{
			name: "defaults with only DATABASE_URL",
			env:  map[string]string{"DATABASE_URL": "postgres://localhost/db"},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 5*time.Second, c.PollInterval)
				assert.Equal(t, ":8080", c.HTTPAddr)
				assert.Equal(t, uint32(17280), c.RetentionLedgers)
				assert.Zero(t, c.RetentionAge)
				assert.Equal(t, time.Hour, c.RetentionPoll)
				assert.Equal(t, uint32(120960), c.PartitionLedgerSpan)
				assert.Equal(t, uint(1000), c.IngestPageSize)
				assert.Equal(t, uint(1000), c.IngestBatchSize)
				assert.Empty(t, c.WatchedContracts)
				assert.Equal(t, uint32(100), c.LagWarnLedgers,
					"LagWarnLedgers default lets the lag alarm work out of the box")
				assert.Equal(t, 5*time.Second, c.StatsCacheTTL,
					"StatsCacheTTL defaults to 5s")
				assert.Equal(t, int64(1048576), c.HTTPRequestBodyLimit, "Request body limit default")
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
				assert.False(t, c.APIKeyAuthEnabled, "API key auth off by default")
			},
		},
		{
			name: "API key auth can be enabled",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"API_KEY_AUTH_ENABLED": "true",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, c.APIKeyAuthEnabled)

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
			name: "ingest sizes configurable",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"INGEST_PAGE_SIZE":  "250",
				"INGEST_BATCH_SIZE": "75",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint(250), c.IngestPageSize)
				assert.Equal(t, uint(75), c.IngestBatchSize)
			},
		},
		{
			name: "zero ingest page size rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"INGEST_PAGE_SIZE": "0",
			},
			wantErr: "INGEST_PAGE_SIZE must be positive",
		},
		{
			name: "zero ingest batch size rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"INGEST_BATCH_SIZE": "0",
			},
			wantErr: "INGEST_BATCH_SIZE must be positive",
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
			name: "RPC_RATE_LIMIT defaults to 10",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(10), c.RPCRateLimit,
					"default keeps today's ~10 req/s public-endpoint pacing")
			},
		},
		{
			name: "RPC_RATE_LIMIT custom value",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/db",
				"RPC_RATE_LIMIT": "50",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(50), c.RPCRateLimit)
			},
		},
		{
			name: "RPC_RATE_LIMIT zero rejected",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/db",
				"RPC_RATE_LIMIT": "0",
			},
			wantErr: "RPC_RATE_LIMIT must be positive",
		},
		{
			name: "RPC_RATE_LIMIT negative rejected",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/db",
				"RPC_RATE_LIMIT": "-3",
			},
			wantErr: "RPC_RATE_LIMIT must be positive",
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

		// --- event batch sizing / backpressure -------------------------------------
		{
			name: "BATCH_SIZE defaults to disabled",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint(0), c.BatchSize,
					"zero is the documented 'batching disabled' default")
				assert.Equal(t, time.Duration(0), c.BatchTargetLatency)
				assert.Equal(t, time.Second, c.BatchMaxBackoff,
					"BatchMaxBackoff has a 1s default")
			},
		},
		{
			name: "BATCH_SIZE parsed with target latency and backoff",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"BATCH_SIZE":           "500",
				"BATCH_TARGET_LATENCY": "50ms",
				"BATCH_MAX_BACKOFF":    "2s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint(500), c.BatchSize)
				assert.Equal(t, 50*time.Millisecond, c.BatchTargetLatency)
				assert.Equal(t, 2*time.Second, c.BatchMaxBackoff)
			},
		},
		{
			name: "negative BATCH_MAX_BACKOFF rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"BATCH_MAX_BACKOFF": "-1s",
			},
			wantErr: "BATCH_MAX_BACKOFF must be non-negative",
		},

		// --- missing/invalid env combinations (gap coverage) -----------------------

		{
			name:    "empty DATABASE_URL rejected",
			env:     map[string]string{"DATABASE_URL": ""},
			wantErr: "DATABASE_URL",
		},
		{
			name: "RETENTION_LEDGERS zero rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"RETENTION_LEDGERS": "0",
			},
			wantErr: "RETENTION_LEDGERS",
		},
		{
			name: "PARTITION_LEDGER_SPAN zero rejected",
			env: map[string]string{
				"DATABASE_URL":          "postgres://localhost/db",
				"PARTITION_LEDGER_SPAN": "0",
			},
			wantErr: "PARTITION_LEDGER_SPAN",
		},
		{
			name: "API_QUERY_TIMEOUT zero rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"API_QUERY_TIMEOUT": "0s",
			},
			wantErr: "API_QUERY_TIMEOUT",
		},
		{
			name: "API_QUERY_TIMEOUT negative rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"API_QUERY_TIMEOUT": "-1s",
			},
			wantErr: "API_QUERY_TIMEOUT",
		},
		{
			name: "API_SLOW_QUERY_THRESHOLD zero rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"API_SLOW_QUERY_THRESHOLD": "0s",
			},
			wantErr: "API_SLOW_QUERY_THRESHOLD",
		},
		{
			name: "API_SLOW_QUERY_THRESHOLD negative rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"API_SLOW_QUERY_THRESHOLD": "-1s",
			},
			wantErr: "API_SLOW_QUERY_THRESHOLD",
		},
		{
			name: "RETENTION_MAX_AGE negative rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"RETENTION_MAX_AGE": "-1s",
			},
			wantErr: "RETENTION_MAX_AGE",
		},
		{
			name: "RETENTION_MAX_AGE zero accepted (disabled)",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"RETENTION_MAX_AGE": "0s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, time.Duration(0), c.RetentionMaxAge)
				assert.False(t, c.RetentionEnabled())
			},
		},
		{
			name: "RETENTION_INTERVAL zero rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"RETENTION_INTERVAL": "0s",
			},
			wantErr: "RETENTION_INTERVAL",
		},
		{
			name: "RETENTION_INTERVAL negative rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"RETENTION_INTERVAL": "-1s",
			},
			wantErr: "RETENTION_INTERVAL",
		},
		{
			name: "RETENTION_MIN_LEDGER zero accepted (disabled)",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"RETENTION_MIN_LEDGER": "0",
			},
			check: func(t *testing.T, c Config) {
				assert.False(t, c.RetentionEnabled())
			},
		},
		{
			name: "RETENTION_MAX_AGE positive enables retention",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"RETENTION_MAX_AGE": "24h",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, c.RetentionEnabled())
			},
		},
		{
			name: "BACKFILL_RATE_RPS zero rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"BACKFILL_RATE_RPS": "0",
			},
			wantErr: "BACKFILL_RATE_RPS",
		},
		{
			name: "BACKFILL_RATE_RPS negative rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"BACKFILL_RATE_RPS": "-1",
			},
			wantErr: "BACKFILL_RATE_RPS",
		},
		{
			name: "RPC_MAX_ATTEMPTS zero rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RPC_MAX_ATTEMPTS": "0",
			},
			wantErr: "RPC_MAX_ATTEMPTS",
		},
		{
			name: "RPC_MAX_ATTEMPTS negative rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RPC_MAX_ATTEMPTS": "-1",
			},
			wantErr: "RPC_MAX_ATTEMPTS",
		},
		{
			name: "RPC_BASE_BACKOFF zero rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RPC_BASE_BACKOFF": "0s",
			},
			wantErr: "RPC_BASE_BACKOFF",
		},
		{
			name: "RPC_BASE_BACKOFF negative rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RPC_BASE_BACKOFF": "-1s",
			},
			wantErr: "RPC_BASE_BACKOFF",
		},
		{
			name: "RPC_MAX_BACKOFF zero rejected",
			env: map[string]string{
				"DATABASE_URL":    "postgres://localhost/db",
				"RPC_MAX_BACKOFF": "0s",
			},
			wantErr: "RPC_MAX_BACKOFF",
		},
		{
			name: "RPC_MAX_BACKOFF negative rejected",
			env: map[string]string{
				"DATABASE_URL":    "postgres://localhost/db",
				"RPC_MAX_BACKOFF": "-1s",
			},
			wantErr: "RPC_MAX_BACKOFF",
		},
		{
			name: "RPC_RATE_LIMIT_RPS negative rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"RPC_RATE_LIMIT_RPS": "-5",
			},
			wantErr: "RPC_RATE_LIMIT_RPS must be positive",
		},
		{
			name: "RATE_LIMIT_BURST negative rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"RATE_LIMIT_RPS":   "5",
				"RATE_LIMIT_BURST": "-1",
			},
			wantErr: "RATE_LIMIT_BURST",
		},
		{
			name: "API_MAX_LIMIT zero rejected",
			env: map[string]string{
				"DATABASE_URL":  "postgres://localhost/db",
				"API_MAX_LIMIT": "0",
			},
			wantErr: "API_MAX_LIMIT",
		},
		{
			name: "API_MAX_LIMIT negative rejected",
			env: map[string]string{
				"DATABASE_URL":  "postgres://localhost/db",
				"API_MAX_LIMIT": "-1",
			},
			wantErr: "API_MAX_LIMIT",
		},
		{
			name: "STATS_CACHE_TTL configurable",
			env: map[string]string{
				"DATABASE_URL":    "postgres://localhost/db",
				"STATS_CACHE_TTL": "30s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, 30*time.Second, c.StatsCacheTTL)
			},
		},
		{
			name: "SWEEP_CONCURRENCY zero rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"SWEEP_CONCURRENCY": "0",
			},
			wantErr: "SWEEP_CONCURRENCY",
		},
		{
			name: "SWEEP_CONCURRENCY negative rejected",
			env: map[string]string{
				"DATABASE_URL":      "postgres://localhost/db",
				"SWEEP_CONCURRENCY": "-1",
			},
			wantErr: "SWEEP_CONCURRENCY",
		},
		{
			name: "REORG_CONFIRMATION_WINDOW with zero REORG_RESCAN_INTERVAL rejected",
			env: map[string]string{
				"DATABASE_URL":              "postgres://localhost/db",
				"REORG_CONFIRMATION_WINDOW": "64",
				"REORG_RESCAN_INTERVAL":     "0s",
			},
			wantErr: "REORG_RESCAN_INTERVAL must be positive when REORG_CONFIRMATION_WINDOW is set",
		},
		{
			name: "REORG_CONFIRMATION_WINDOW with negative REORG_RESCAN_INTERVAL rejected",
			env: map[string]string{
				"DATABASE_URL":              "postgres://localhost/db",
				"REORG_CONFIRMATION_WINDOW": "64",
				"REORG_RESCAN_INTERVAL":     "-1s",
			},
			wantErr: "REORG_RESCAN_INTERVAL must be positive when REORG_CONFIRMATION_WINDOW is set",
		},
		{
			name: "REORG_CONFIRMATION_WINDOW zero with zero REORG_RESCAN_INTERVAL accepted",
			env: map[string]string{
				"DATABASE_URL":              "postgres://localhost/db",
				"REORG_CONFIRMATION_WINDOW": "0",
				"REORG_RESCAN_INTERVAL":     "0s",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, uint32(0), c.ReorgConfirmationWindow)
			},
		},
		{
			name: "EXPORT_MAX_RANGE zero rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"EXPORT_MAX_RANGE": "0",
			},
			wantErr: "EXPORT_MAX_RANGE",
		},
		{
			name: "EXPORT_MAX_RANGE negative rejected",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"EXPORT_MAX_RANGE": "-1",
			},
			wantErr: "EXPORT_MAX_RANGE",
		},
		{
			name: "AUDIT_POLL_INTERVAL zero rejected",
			env: map[string]string{
				"DATABASE_URL":        "postgres://localhost/db",
				"AUDIT_POLL_INTERVAL": "0s",
			},
			wantErr: "AUDIT_POLL_INTERVAL",
		},
		{
			name: "AUDIT_POLL_INTERVAL negative rejected",
			env: map[string]string{
				"DATABASE_URL":        "postgres://localhost/db",
				"AUDIT_POLL_INTERVAL": "-1s",
			},
			wantErr: "AUDIT_POLL_INTERVAL",
		},
		{
			name: "AUDIT_BATCH_LEDGERS zero rejected",
			env: map[string]string{
				"DATABASE_URL":        "postgres://localhost/db",
				"AUDIT_BATCH_LEDGERS": "0",
			},
			wantErr: "AUDIT_BATCH_LEDGERS",
		},
		{
			name: "AUDIT_LAG_THRESHOLD zero rejected",
			env: map[string]string{
				"DATABASE_URL":        "postgres://localhost/db",
				"AUDIT_LAG_THRESHOLD": "0",
			},
			wantErr: "AUDIT_LAG_THRESHOLD",
		},
		{
			name: "AUDIT_BUDGET_SHARE negative rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"AUDIT_BUDGET_SHARE": "-0.1",
			},
			wantErr: "AUDIT_BUDGET_SHARE",
		},
		{
			name: "AUDIT_BUDGET_SHARE above one rejected",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"AUDIT_BUDGET_SHARE": "1.1",
			},
			wantErr: "AUDIT_BUDGET_SHARE",
		},
		{
			name: "AUDIT_BUDGET_SHARE zero accepted",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"AUDIT_BUDGET_SHARE": "0",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(0), c.AuditBudgetShare)
			},
		},
		{
			name: "AUDIT_BUDGET_SHARE one accepted",
			env: map[string]string{
				"DATABASE_URL":       "postgres://localhost/db",
				"AUDIT_BUDGET_SHARE": "1",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, float64(1), c.AuditBudgetShare)
			},
		},
		{
			name: "AUDIT_MAX_RPS zero rejected",
			env: map[string]string{
				"DATABASE_URL":  "postgres://localhost/db",
				"AUDIT_MAX_RPS": "0",
			},
			wantErr: "AUDIT_MAX_RPS",
		},
		{
			name: "AUDIT_MAX_REPAIR_ATTEMPTS zero rejected",
			env: map[string]string{
				"DATABASE_URL":              "postgres://localhost/db",
				"AUDIT_MAX_REPAIR_ATTEMPTS": "0",
			},
			wantErr: "AUDIT_MAX_REPAIR_ATTEMPTS",
		},
		{
			name: "AUDIT_FINDING_MAX_LEDGERS zero rejected",
			env: map[string]string{
				"DATABASE_URL":              "postgres://localhost/db",
				"AUDIT_FINDING_MAX_LEDGERS": "0",
			},
			wantErr: "AUDIT_FINDING_MAX_LEDGERS",
		},
		{
			name: "MULTI_TENANT_USAGE_FLUSH zero rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"MULTI_TENANT_USAGE_FLUSH": "0s",
			},
			wantErr: "MULTI_TENANT_USAGE_FLUSH",
		},
		{
			name: "MULTI_TENANT_USAGE_FLUSH negative rejected",
			env: map[string]string{
				"DATABASE_URL":             "postgres://localhost/db",
				"MULTI_TENANT_USAGE_FLUSH": "-1s",
			},
			wantErr: "MULTI_TENANT_USAGE_FLUSH",
		},
		{
			name: "CORS null origin rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "null",
			},
			wantErr: "CORS_ALLOWED_ORIGINS entry \"null\" is not allowed",
		},
		{
			name: "CORS null origin case-insensitive rejected",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"CORS_ALLOWED_ORIGINS": "NULL",
			},
			wantErr: "CORS_ALLOWED_ORIGINS entry \"NULL\" is not allowed",
		},
		{
			name: "HORIZON_URL invalid rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"HORIZON_URL":  "not-a-url",
			},
			wantErr: "HORIZON_URL",
		},
		{
			name: "HORIZON_URL unset keeps default",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, "https://horizon-testnet.stellar.org", c.HorizonURL,
					"envDefault provides a working public-testnet Horizon")
			},
		},
		{
			name: "SQLite DATABASE_URL relative path accepted",
			env: map[string]string{
				"DATABASE_URL": "sqlite:./local.db",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, IsSQLite(c.DatabaseURL))
			},
		},
		{
			name: "SQLite DATABASE_URL memory accepted",
			env: map[string]string{
				"DATABASE_URL": "sqlite::memory:",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, IsSQLite(c.DatabaseURL))
			},
		},
		{
			name: "SQLite DATABASE_URL bare name rejected",
			env: map[string]string{
				"DATABASE_URL": "sqlite:local.db",
			},
			wantErr: "sqlite DATABASE_URL",
		},
		{
			name: "SQLite DATABASE_URL with invalid subdir rejected",
			env: map[string]string{
				"DATABASE_URL": "sqlite:foo/bar/../baz.db",
			},
			wantErr: "sqlite DATABASE_URL",
		},
		{
			name: "RPC_URL missing host rejected",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URL":      "https://",
			},
			wantErr: "RPC_URL",
		},
		{
			name: "RPC_URLS all empty entries falls through to RPC_URL check",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URLS":     " , ,",
			},
			check: func(t *testing.T, c Config) {
				assert.Empty(t, c.RPCURLS, "empty entries should be cleaned")
				// RPC_URL has a valid default, so Load succeeds
				assert.Equal(t, "https://soroban-testnet.stellar.org", c.RPCURL)
			},
		},
		{
			name: "RPC_URLS override takes priority over RPC_URL",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
				"RPC_URL":      "https://custom.example.com",
				"RPC_URLS":     "https://failover1.example.com",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, []string{"https://failover1.example.com"}, c.RPCURLS)
			},
		},
		{
			name: "SQLite DATABASE_URL skips RPC_URL validation",
			env: map[string]string{
				"DATABASE_URL": "sqlite::memory:",
				"RPC_URL":      "",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, IsSQLite(c.DatabaseURL))
			},
		},
		{
			name: "rate limit neither set is accepted",
			env: map[string]string{
				"DATABASE_URL": "postgres://localhost/db",
			},
			check: func(t *testing.T, c Config) {
				assert.Zero(t, c.RateLimitRPS)
				assert.Zero(t, c.RateLimitBurst)
			},
		},
		{
			name: "SHUTDOWN_TIMEOUT zero accepted",
			env: map[string]string{
				"DATABASE_URL":     "postgres://localhost/db",
				"SHUTDOWN_TIMEOUT": "0",
			},
			check: func(t *testing.T, c Config) {
				assert.Equal(t, time.Duration(0), c.ShutdownTimeout)
			},
		},
		{
			name: "RETENTION_MAX_AGE and RETENTION_MIN_LEDGER both set enables retention",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"RETENTION_MAX_AGE":    "48h",
				"RETENTION_MIN_LEDGER": "1000",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, c.RetentionEnabled())
				assert.Equal(t, 48*time.Hour, c.RetentionMaxAge)
				assert.Equal(t, uint64(1000), c.RetentionMinLedger)
			},
		},
		{
			name: "RETENTION_MIN_LEDGER alone enables retention",
			env: map[string]string{
				"DATABASE_URL":         "postgres://localhost/db",
				"RETENTION_MIN_LEDGER": "500",
			},
			check: func(t *testing.T, c Config) {
				assert.True(t, c.RetentionEnabled())
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

func TestLoad_UsesProcessEnvironmentInsteadOfDotEnv(t *testing.T) {
	t.Chdir(t.TempDir())
	require.NoError(t, os.WriteFile(".env", []byte("LOG_LEVEL=warn\n"), 0o600))
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.LogLevel)
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

func TestParseStartLedger(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		latest  uint32
		want    uint32
		wantErr string
	}{
		{name: "absolute", raw: "12345", want: 12345},
		{name: "absolute min ledger 2", raw: "2", want: 2},
		{name: "absolute below min ledger rejected", raw: "1", wantErr: "must be >= 2"},
		{name: "relative offset", raw: "latest-1000", latest: 50000, want: 49000},
		{name: "relative offset clamps to 2", raw: "latest-100000", latest: 50000, want: 2},
		{name: "relative offset no latest", raw: "latest-1000", wantErr: "not available"},
		{name: "relative offset zero", raw: "latest-0", wantErr: "offset must be a positive"},
		{name: "empty", raw: "", want: 0},
		{name: "invalid", raw: "abc", wantErr: "not an absolute"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got uint32
			var err error
			if tt.latest > 0 {
				got, err = ParseStartLedger(tt.raw, tt.latest)
			} else {
				got, err = ParseStartLedger(tt.raw)
			}
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadStartLedgerRaw(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("START_LEDGER_RAW", "latest-500")
	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "latest-500", cfg.StartLedgerRaw)
}
