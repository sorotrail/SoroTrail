package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validBase() Config {
	return Config{
		DatabaseURL:                "postgres://user:pass@localhost:5432/db",
		RPCURL:                     "https://rpc.example.com",
		PollInterval:               5 * time.Second,
		AuditPollInterval:          30 * time.Second,
		IngesterMinBackoff:         1 * time.Second,
		IngesterMaxBackoff:         5 * time.Second,
		RetentionLedgers:           17280,
		PartitionLedgerSpan:        120960,
		AuditBatchLedgers:          100,
		AuditLagThreshold:          200,
		AuditBudgetShare:           0.5,
		AuditMaxRPS:                10,
		AuditMaxRepair:             3,
		AuditFindingMaxLgrs:        100,
		LogLevel:                   "info",
		LogFormat:                  "json",
		APIQueryTimeout:            25 * time.Second,
		APISlowQueryThreshold:      2 * time.Second,
		IngestPageSize:             1000,
		IngestBatchSize:            1000,
		RetentionBatchSize:         5000,
		RetentionPause:             100 * time.Millisecond,
		RetentionInterval:          1 * time.Hour,
		BackfillRateRPS:            10,
		RPCMaxAttempts:             3,
		RPCBaseBackoff:             500 * time.Millisecond,
		RPCMaxBackoff:              30 * time.Second,
		RPCRateLimitRPS:            10,
		RPCRateLimit:               10,
		SweepConcurrency:           1,
		ExportMaxRange:             17280,
		APIMaxLimit:                500,
		ShutdownTimeout:            15 * time.Second,
		MultiTenantUsageFlush:      10 * time.Second,
		MultiTenantStreamScopeSync: 30 * time.Second,
		ReorgConfirmationWindow:    64,
		ReorgRescanInterval:        1 * time.Minute,
		BatchMaxBackoff:            1 * time.Second,
	}
}

func TestValidateAll_Coverage(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*Config)
		expectError string
	}{
		{"empty DATABASE_URL", func(c *Config) { c.DatabaseURL = "" }, "DATABASE_URL: required but empty"},
		{"empty RPC_URL and RPC_URLS", func(c *Config) { c.RPCURL = ""; c.RPCURLS = nil }, "RPC_URL: required but empty"},
		{"invalid RPC_URL", func(c *Config) { c.RPCURL = "invalid-url" }, "is not a valid absolute URL"},
		{"invalid RPC_URLS element", func(c *Config) { c.RPCURLS = []string{"http://ok.com", "not-a-url"} }, "is not a valid absolute URL"},
		{"negative PollInterval", func(c *Config) { c.PollInterval = -1 * time.Second }, "POLL_INTERVAL"},
		{"negative PollIntervalMin", func(c *Config) { c.PollIntervalMin = -1 * time.Second }, "POLL_INTERVAL_MIN"},
		{"negative PollIntervalMax", func(c *Config) { c.PollIntervalMax = -1 * time.Second }, "POLL_INTERVAL_MAX"},
		{"PollIntervalMin > Max", func(c *Config) { c.PollIntervalMin = 5 * time.Second; c.PollIntervalMax = 1 * time.Second }, "must be <= POLL_INTERVAL_MAX"},
		{"negative AuditPollInterval", func(c *Config) { c.AuditPollInterval = -1 * time.Second }, "AUDIT_POLL_INTERVAL"},
		{"negative IngesterMinBackoff", func(c *Config) { c.IngesterMinBackoff = -1 * time.Second }, "INGESTER_MIN_BACKOFF"},
		{"negative IngesterMaxBackoff", func(c *Config) { c.IngesterMaxBackoff = -1 * time.Second }, "INGESTER_MAX_BACKOFF"},
		{"IngesterMinBackoff > Max", func(c *Config) { c.IngesterMinBackoff = 5 * time.Second; c.IngesterMaxBackoff = 1 * time.Second }, "must not exceed INGESTER_MAX_BACKOFF"},
		{"negative IngesterJitterMin", func(c *Config) { c.IngesterJitterMin = -1 }, "jitter bounds must be non-negative"},
		{"IngesterJitterMin > Max", func(c *Config) { c.IngesterJitterMin = 5; c.IngesterJitterMax = 1 }, "must not exceed INGESTER_JITTER_MAX"},
		{"zero RetentionLedgers", func(c *Config) { c.RetentionLedgers = 0 }, "RETENTION_LEDGERS"},
		{"zero PartitionLedgerSpan", func(c *Config) { c.PartitionLedgerSpan = 0 }, "PARTITION_LEDGER_SPAN"},
		{"zero AuditBatchLedgers", func(c *Config) { c.AuditBatchLedgers = 0 }, "AUDIT_BATCH_LEDGERS"},
		{"zero AuditLagThreshold", func(c *Config) { c.AuditLagThreshold = 0 }, "AUDIT_LAG_THRESHOLD"},
		{"AuditBudgetShare > 1", func(c *Config) { c.AuditBudgetShare = 1.5 }, "AUDIT_BUDGET_SHARE"},
		{"AuditMaxRPS zero", func(c *Config) { c.AuditMaxRPS = 0 }, "AUDIT_MAX_RPS"},
		{"AuditMaxRepair zero", func(c *Config) { c.AuditMaxRepair = 0 }, "AUDIT_MAX_REPAIR_ATTEMPTS"},
		{"AuditFindingMaxLgrs zero", func(c *Config) { c.AuditFindingMaxLgrs = 0 }, "AUDIT_FINDING_MAX_LEDGERS"},
		{"RateLimitRPS negative", func(c *Config) { c.RateLimitRPS = -1 }, "RATE_LIMIT_RPS"},
		{"RateLimitBurst negative", func(c *Config) { c.RateLimitBurst = -1 }, "RATE_LIMIT_BURST"},
		{"HourlyQuota negative", func(c *Config) { c.HourlyQuota = -1 }, "HOURLY_QUOTA"},
		{"DailyQuota negative", func(c *Config) { c.DailyQuota = -1 }, "DAILY_QUOTA"},
		{"invalid LogLevel", func(c *Config) { c.LogLevel = "invalid" }, "LOG_LEVEL"},
		{"invalid LogFormat", func(c *Config) { c.LogFormat = "invalid" }, "LOG_FORMAT"},
		{"invalid WatchedContracts", func(c *Config) { c.WatchedContracts = []string{"invalid"} }, "WATCHED_CONTRACTS"},
		// Former Validate()-only rules now enforced by ValidateAll() (#1143)
		{"sqlite DATABASE_URL bare name", func(c *Config) { c.DatabaseURL = "sqlite:barename.db" }, "sqlite DATABASE_URL"},
		{"negative DBMaxConns", func(c *Config) { c.DBMaxConns = -1 }, "DB_MAX_CONNS must be non-negative"},
		{"negative DBMinConns", func(c *Config) { c.DBMinConns = -1 }, "DB_MIN_CONNS must be non-negative"},
		{"DBMinConns > DBMaxConns", func(c *Config) { c.DBMaxConns = 4; c.DBMinConns = 8 }, "DB_MIN_CONNS (8) must be <= DB_MAX_CONNS (4)"},
		{"negative DBMaxConnLifetime", func(c *Config) { c.DBMaxConnLifetime = -1 * time.Second }, "DB_MAX_CONN_LIFETIME must be non-negative"},
		{"negative DBMaxConnIdleTime", func(c *Config) { c.DBMaxConnIdleTime = -1 * time.Second }, "DB_MAX_CONN_IDLE_TIME must be non-negative"},
		{"negative DBIngesterReservedConns", func(c *Config) { c.DBIngesterReservedConns = -1 }, "DB_INGESTER_RESERVED_CONNS must be non-negative"},
		{"DBIngesterReservedConns >= DBMaxConns", func(c *Config) { c.DBMaxConns = 4; c.DBIngesterReservedConns = 4 }, "DB_INGESTER_RESERVED_CONNS (4) must be < DB_MAX_CONNS (4)"},
		{"negative DBAPIReadMaxConns", func(c *Config) { c.DBAPIReadMaxConns = -1 }, "DB_API_READ_MAX_CONNS must be non-negative"},
		{"negative DBGraphQLMaxConns", func(c *Config) { c.DBGraphQLMaxConns = -1 }, "DB_GRAPHQL_MAX_CONNS must be non-negative"},
		{"negative DBSubscriptionMaxConns", func(c *Config) { c.DBSubscriptionMaxConns = -1 }, "DB_SUBSCRIPTION_MAX_CONNS must be non-negative"},
		{"negative DBMaintenanceMaxConns", func(c *Config) { c.DBMaintenanceMaxConns = -1 }, "DB_MAINTENANCE_MAX_CONNS must be non-negative"},
		{"negative DBPoolAcquireTimeout", func(c *Config) { c.DBPoolAcquireTimeout = -1 * time.Millisecond }, "DB_POOL_ACQUIRE_TIMEOUT must be non-negative"},
		{"DBPoolPressureThreshold > 1", func(c *Config) { c.DBPoolPressureThreshold = 1.5 }, "DB_POOL_PRESSURE_THRESHOLD must be in [0,1]"},
		{"invalid HorizonURL", func(c *Config) { c.HorizonURL = "not-a-url" }, "HORIZON_URL \"not-a-url\" is not a valid URL"},
		{"invalid HTTPAddr", func(c *Config) { c.HTTPAddr = "bad-addr" }, "HTTP_ADDR \"bad-addr\" is not a valid host:port"},
		{"zero APIQueryTimeout", func(c *Config) { c.APIQueryTimeout = 0 }, "API_QUERY_TIMEOUT must be positive"},
		{"zero APISlowQueryThreshold", func(c *Config) { c.APISlowQueryThreshold = 0 }, "API_SLOW_QUERY_THRESHOLD must be positive"},
		{"negative RetentionAge", func(c *Config) { c.RetentionAge = -1 * time.Hour }, "RETENTION_AGE must be non-negative"},
		{"negative RetentionPoll", func(c *Config) { c.RetentionPoll = -1 * time.Minute }, "RETENTION_POLL_INTERVAL must be non-negative"},
		{"invalid SkipContracts", func(c *Config) { c.SkipContracts = []string{"bad-contract"} }, "SKIP_CONTRACTS entry"},
		{"zero RPCRateLimitRPS", func(c *Config) { c.RPCRateLimitRPS = 0 }, "RPC_RATE_LIMIT_RPS must be positive"},
		{"zero RPCRateLimit", func(c *Config) { c.RPCRateLimit = 0 }, "RPC_RATE_LIMIT must be positive"},
		{"zero RetentionBatchSize", func(c *Config) { c.RetentionBatchSize = 0 }, "RETENTION_BATCH_SIZE must be positive"},
		{"zero IngestPageSize", func(c *Config) { c.IngestPageSize = 0 }, "INGEST_PAGE_SIZE must be positive"},
		{"zero IngestBatchSize", func(c *Config) { c.IngestBatchSize = 0 }, "INGEST_BATCH_SIZE must be positive"},
		{"negative RetentionPause", func(c *Config) { c.RetentionPause = -1 * time.Second }, "RETENTION_PAUSE must be non-negative"},
		{"zero RetentionInterval", func(c *Config) { c.RetentionInterval = 0 }, "RETENTION_INTERVAL must be positive"},
		{"negative RetentionMaxAge", func(c *Config) { c.RetentionMaxAge = -1 * time.Hour }, "RETENTION_MAX_AGE must be non-negative"},
		{"zero BackfillRateRPS", func(c *Config) { c.BackfillRateRPS = 0 }, "BACKFILL_RATE_RPS must be positive"},
		{"zero RPCMaxAttempts", func(c *Config) { c.RPCMaxAttempts = 0 }, "RPC_MAX_ATTEMPTS must be positive"},
		{"zero RPCBaseBackoff", func(c *Config) { c.RPCBaseBackoff = 0 }, "RPC_BASE_BACKOFF must be positive"},
		{"zero RPCMaxBackoff", func(c *Config) { c.RPCMaxBackoff = 0 }, "RPC_MAX_BACKOFF must be positive"},
		{"negative RPCHTTPTimeout", func(c *Config) { c.RPCHTTPTimeout = -1 * time.Second }, "RPC_HTTP_TIMEOUT must be non-negative"},
		{"negative HTTPReadTimeout", func(c *Config) { c.HTTPReadTimeout = -1 * time.Second }, "HTTP_READ_TIMEOUT must be non-negative"},
		{"negative HTTPWriteTimeout", func(c *Config) { c.HTTPWriteTimeout = -1 * time.Second }, "HTTP_WRITE_TIMEOUT must be non-negative"},
		{"negative HTTPIdleTimeout", func(c *Config) { c.HTTPIdleTimeout = -1 * time.Second }, "HTTP_IDLE_TIMEOUT must be non-negative"},
		{"negative HTTPReadHeaderTimeout", func(c *Config) { c.HTTPReadHeaderTimeout = -1 * time.Second }, "HTTP_READ_HEADER_TIMEOUT must be non-negative"},
		{"negative HTTPRequestBodyLimit", func(c *Config) { c.HTTPRequestBodyLimit = -1 }, "HTTP_REQUEST_BODY_LIMIT must be non-negative"},
		{"negative HTTPResponseBodyLimit", func(c *Config) { c.HTTPResponseBodyLimit = -1 }, "HTTP_RESPONSE_BODY_LIMIT must be non-negative"},
		{"negative HTTPDecompressedBodyLimit", func(c *Config) { c.HTTPDecompressedBodyLimit = -1 }, "HTTP_DECOMPRESSED_BODY_LIMIT must be non-negative"},
		{"invalid HTTPEndpointBodyLimits", func(c *Config) { c.HTTPEndpointBodyLimits = map[string]int64{"/events": 0} }, "HTTP_ENDPOINT_BODY_LIMITS entry"},
		{"zero APIMaxLimit", func(c *Config) { c.APIMaxLimit = 0 }, "API_MAX_LIMIT must be positive"},
		{"negative ShutdownTimeout", func(c *Config) { c.ShutdownTimeout = -1 * time.Second }, "SHUTDOWN_TIMEOUT must be non-negative"},
		{"zero SweepConcurrency", func(c *Config) { c.SweepConcurrency = 0 }, "SWEEP_CONCURRENCY must be positive"},
		{"negative BatchTargetLatency with BatchSize", func(c *Config) { c.BatchSize = 10; c.BatchTargetLatency = -1 * time.Millisecond }, "BATCH_TARGET_LATENCY must be non-negative"},
		{"negative BatchMaxBackoff", func(c *Config) { c.BatchMaxBackoff = -1 * time.Second }, "BATCH_MAX_BACKOFF must be non-negative"},
		{"zero ReorgRescanInterval with Window", func(c *Config) { c.ReorgConfirmationWindow = 10; c.ReorgRescanInterval = 0 }, "REORG_RESCAN_INTERVAL must be positive when REORG_CONFIRMATION_WINDOW is set"},
		{"zero ExportMaxRange", func(c *Config) { c.ExportMaxRange = 0 }, "EXPORT_MAX_RANGE must be positive"},
		{"null CORS origin", func(c *Config) { c.CORSAllowedOrigins = []string{"null"} }, "CORS_ALLOWED_ORIGINS entry \"null\" is not allowed"},
		{"invalid CORS origin", func(c *Config) { c.CORSAllowedOrigins = []string{"bad-origin"} }, "CORS_ALLOWED_ORIGINS entry \"bad-origin\" is not a valid origin"},
		{"negative MultiTenantMaxWatched", func(c *Config) { c.MultiTenantMaxWatched = -1 }, "MULTI_TENANT_MAX_WATCHED must be non-negative"},
		{"zero MultiTenantUsageFlush", func(c *Config) { c.MultiTenantUsageFlush = 0 }, "MULTI_TENANT_USAGE_FLUSH must be positive"},
		{"zero MultiTenantStreamScopeSync", func(c *Config) { c.MultiTenantStreamScopeSync = 0 }, "MULTI_TENANT_STREAM_SCOPE_SYNC must be positive"},
		{"MultiTenantBootstrapKey without MultiTenant", func(c *Config) { c.MultiTenantBootstrapKey = "key"; c.MultiTenant = false }, "MULTI_TENANT_BOOTSTRAP_KEY is set but MULTI_TENANT is false"},
		{"ArchiveBucket without ArchiveEndpoint", func(c *Config) { c.ArchiveBucket = "bucket"; c.ArchiveEndpoint = "" }, "ARCHIVE_ENDPOINT is required when ARCHIVE_BUCKET is set"},
		{"negative ArchiveMaxRetries", func(c *Config) { c.ArchiveMaxRetries = -1 }, "ARCHIVE_MAX_RETRIES must be non-negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validBaseValidate()
			tt.modify(&cfg)
			err := cfg.ValidateAll()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

func TestValidateAll_SinglePassCombinedErrorsAndSecretRedaction(t *testing.T) {
	// Feed a config with one original ValidateAll problem (POLL_INTERVAL <= 0)
	// and one former Validate-only problem (API_QUERY_TIMEOUT <= 0), plus
	// secret values in DATABASE_URL and MULTI_TENANT_BOOTSTRAP_KEY, and assert
	// both errors appear in a single pass with secrets redacted (#1143).
	cfg := validBaseValidate()
	cfg.DatabaseURL = "postgres://admin:super_secret_db_pass@localhost:5432/db"
	cfg.PollInterval = 0
	cfg.APIQueryTimeout = 0
	cfg.MultiTenantBootstrapKey = "st_top_secret_bootstrap_token"
	cfg.MultiTenant = false

	err := cfg.ValidateAll()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "POLL_INTERVAL:")
	assert.Contains(t, msg, "API_QUERY_TIMEOUT must be positive")
	assert.Contains(t, msg, "MULTI_TENANT_BOOTSTRAP_KEY is set but MULTI_TENANT is false")
	assert.NotContains(t, msg, "super_secret_db_pass")
	assert.NotContains(t, msg, "st_top_secret_bootstrap_token")
}

func TestRedactURLPassword_Table(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		want   string
		secret string
	}{
		{
			name:   "password is masked and URL components survive",
			in:     "https://alice:s3cr3t@rpc.example.com:8443/v1/events?network=test",
			want:   "https://alice:%2A%2A%2A@rpc.example.com:8443/v1/events?network=test",
			secret: "s3cr3t",
		},
		{
			name: "username without password is unchanged",
			in:   "https://alice@rpc.example.com/v1/events",
			want: "https://alice@rpc.example.com/v1/events",
		},
		{
			name: "URL without userinfo is unchanged",
			in:   "https://rpc.example.com/v1/events?network=test",
			want: "https://rpc.example.com/v1/events?network=test",
		},
		{
			name: "malformed URL fails closed without panicking",
			in:   "https://alice:secret%zz@rpc.example.com/v1/events",
			want: "<redacted>",
		},
		{name: "empty string remains empty", in: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactURLPassword(tt.in)
			assert.Equal(t, tt.want, got)
			if tt.secret != "" {
				assert.NotContains(t, got, tt.secret)
			}
		})
	}
}

// TestValidateCORSOrigins pins down the startup guard on CORS_ALLOWED_ORIGINS,
// which is the last point before a malformed entry becomes a permissive browser
// policy for the life of the process. Each case covers one rule the operator
// docs promise: an explicit scheme://host[:port] origin, the "*" wildcard, the
// rejected "null" origin, or an empty list meaning CORS is disabled.
func TestValidateCORSOrigins(t *testing.T) {
	tests := []struct {
		name    string
		origins []string
		// expectError is the substring the error must contain; empty means
		// the list is accepted.
		expectError string
	}{
		{
			// An empty list is the documented default: CORS stays off and no
			// browser request receives Access-Control-* headers.
			name: "empty list accepted (CORS disabled)",
		},
		{
			name:    "well-formed https origin accepted",
			origins: []string{"https://app.example.com"},
		},
		{
			name:    "http origin with an explicit port accepted",
			origins: []string{"http://localhost:3000"},
		},
		{
			name:    "wildcard accepted",
			origins: []string{"*"},
		},
		{
			// Load calls cleanOrigins first, so a pasted trailing slash is
			// stripped before validation. Called directly the entry is still
			// rejected: browsers never send a path in Origin, so comparing one
			// would silently match nothing.
			name:        "origin with a trailing slash rejected",
			origins:     []string{"https://app.example.com/"},
			expectError: `CORS_ALLOWED_ORIGINS entry "https://app.example.com/" is not a valid origin`,
		},
		{
			name:        "origin with a path rejected",
			origins:     []string{"https://app.example.com/api"},
			expectError: `CORS_ALLOWED_ORIGINS entry "https://app.example.com/api" is not a valid origin`,
		},
		{
			name:        "bare hostname with no scheme rejected",
			origins:     []string{"app.example.com"},
			expectError: `CORS_ALLOWED_ORIGINS entry "app.example.com" is not a valid origin`,
		},
		{
			// Origin: null is what sandboxed iframes and file:// pages send, so
			// allowing it would let any third-party page call the API.
			name:        "null origin rejected",
			origins:     []string{"null"},
			expectError: `CORS_ALLOWED_ORIGINS entry "null" is not allowed`,
		},
		{
			// The error has to name the offending entry, not just the variable,
			// so an operator with a long allowlist can find the bad one.
			name:        "error names the offending origin",
			origins:     []string{"https://app.example.com", "app.example.com"},
			expectError: `CORS_ALLOWED_ORIGINS entry "app.example.com" is not a valid origin`,
		},
		{
			// Whitespace-only entries are dropped by cleanOrigins before Load
			// validates, and the validator tolerates them directly rather than
			// failing startup over a stray comma.
			name:    "blank entries tolerated",
			origins: []string{"", "   "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCORSOrigins(tt.origins)
			if tt.expectError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}
