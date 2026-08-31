// Package config loads and validates SoroTrail's configuration from
// environment variables.
//
// The entry point is [Load], which parses the environment into a [Config]
// and calls [Config.Validate]. Every field is settable via the environment
// variable named in its `env` tag; see .env.example for documentation.
//
// Non-obvious contracts:
//   - DATABASE_URL is the only required variable; all others have safe
//     defaults.
//   - [Config.Validate] is idempotent and can be called again after
//     programmatic mutation.
//   - [ValidContractID] checks shape only (C prefix, 56 base32 chars),
//     not the checksum.
package config

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// NetworkConfig describes one Stellar network to index.
type NetworkConfig struct {
	Name       string `json:"name"`
	RPCURL     string `json:"rpc_url"`
	Passphrase string `json:"passphrase"`
}

var networks = map[string]NetworkConfig{
	"testnet":   {Name: "testnet", RPCURL: "https://soroban-testnet.stellar.org", Passphrase: "Test SDF Network ; September 2015"},
	"mainnet":   {Name: "mainnet", RPCURL: "https://mainnet.sorobanrpc.com", Passphrase: "Public Global Stellar Network ; September 2015"},
	"futurenet": {Name: "futurenet", RPCURL: "https://rpc-futurenet.stellar.org", Passphrase: "Test SDF Future Network ; October 2022"},
}

func NetworkConfigFor(name string) (NetworkConfig, bool) {
	c, ok := networks[strings.ToLower(strings.TrimSpace(name))]
	return c, ok
}

// Config holds all runtime configuration. Every field is settable via the
// environment variable named in its `env` tag; see .env.example for docs.
type Config struct {
	Network string `env:"NETWORK" envDefault:"testnet"`
	// RPCURL is the single-provider RPC endpoint. When RPC_URLS is set the
	// multi-provider failover client is used instead and RPC_URL is ignored.
	RPCURL string `env:"RPC_URL" envDefault:"https://soroban-testnet.stellar.org"`
	// RPCURLS, when set, enables the multi-provider failover client
	// (internal/rpc). List order is priority: index 0 is tried first.
	// RPC_URL is ignored while it is set; leaving RPC_URLS unset keeps the
	// single-provider behavior unchanged.
	RPCURLS []string `env:"RPC_URLS"`
	// RPCRateLimitRPS caps each provider's request rate (requests/second)
	// when the failover client is in use. Only read by the failover client.
	RPCRateLimitRPS float64 `env:"RPC_RATE_LIMIT_RPS" envDefault:"10"`
	// RPCRateLimit caps the single-provider client's request rate in
	// requests/second (RPC_RATE_LIMIT). The default of 10 matches the
	// public endpoint limit; raise it only against paid plans or
	// self-hosted RPCs whose allowance actually permits it. Ignored while
	// RPC_URLS is set (the failover path uses RPC_RATE_LIMIT_RPS).
	RPCRateLimit float64 `env:"RPC_RATE_LIMIT" envDefault:"10"`
	DatabaseURL  string  `env:"DATABASE_URL"`
	// DB pool sizing. Zero means "use the pgx default". These let an operator
	// bound the Postgres connection pool without a code redeploy.
	DBMaxConns        int32         `env:"DB_MAX_CONNS" envDefault:"0"`
	DBMinConns        int32         `env:"DB_MIN_CONNS" envDefault:"0"`
	DBMaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"0"`
	DBMaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"0"`
	PollInterval      time.Duration `env:"POLL_INTERVAL" envDefault:"5s"`
	// HTTPAddr is the address the HTTP server listens on (host:port), e.g.
	// ":8080" or "0.0.0.0:9090". See HTTP_ADDR in .env.example. It must be a
	// valid host:port pair.
	HTTPAddr              string        `env:"HTTP_ADDR" envDefault:":8080"`
	WatchedContracts      []string      `env:"WATCHED_CONTRACTS"`
	StartLedger           uint32        `env:"START_LEDGER"`
	StartLedgerRaw        string        `env:"START_LEDGER_RAW"`
	RetentionLedgers      uint32        `env:"RETENTION_LEDGERS" envDefault:"17280"`
	IngestPageSize        uint          `env:"INGEST_PAGE_SIZE" envDefault:"1000"`
	IngestBatchSize       uint          `env:"INGEST_BATCH_SIZE" envDefault:"1000"`
	PartitionLedgerSpan   uint32        `env:"PARTITION_LEDGER_SPAN" envDefault:"120960"`
	LogLevel              string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat             string        `env:"LOG_FORMAT" envDefault:"text"`
	APIQueryTimeout       time.Duration `env:"API_QUERY_TIMEOUT" envDefault:"25s"`
	APISlowQueryThreshold time.Duration `env:"API_SLOW_QUERY_THRESHOLD" envDefault:"2s"`

	// Retention pruning. RETENTION_MAX_AGE and RETENTION_MIN_LEDGER are the
	// two policy dimensions; when both are unset the pruner never runs (see
	// RetentionEnabled). The remaining knobs bound how aggressively it
	// deletes so a single sweep never holds a long lock.
	RetentionMaxAge    time.Duration `env:"RETENTION_MAX_AGE"`
	RetentionMinLedger uint64        `env:"RETENTION_MIN_LEDGER"`
	// RetentionAge / RetentionPoll: age-based retention job knobs
	// (RETENTION_AGE, RETENTION_POLL_INTERVAL). RetentionAge zero disables
	// the age-based pruner; RetentionPoll defaults to one hour.
	RetentionAge       time.Duration `env:"RETENTION_AGE"`
	RetentionPoll      time.Duration `env:"RETENTION_POLL_INTERVAL" envDefault:"1h"`
	RetentionBatchSize int           `env:"RETENTION_BATCH_SIZE" envDefault:"5000"`
	RetentionPause     time.Duration `env:"RETENTION_PAUSE" envDefault:"100ms"`
	RetentionInterval  time.Duration `env:"RETENTION_INTERVAL" envDefault:"1h"`
	// RetentionDryRun, when true, makes the pruner report what it
	// would delete without actually removing any rows.
	RetentionDryRun bool `env:"RETENTION_DRY_RUN"`

	// Archive configuration. When ARCHIVE_BUCKET is set, pruned events
	// are exported to S3-compatible object storage as compressed NDJSON
	// before deletion, ensuring pruned ranges are recoverable. All
	// archive_* fields are optional; without them the binary behaves
	// identically to the pre-archive build.
	ArchiveBucket          string `env:"ARCHIVE_BUCKET"`
	ArchivePrefix          string `env:"ARCHIVE_PREFIX"`
	ArchiveEndpoint        string `env:"ARCHIVE_ENDPOINT"`
	ArchiveRegion          string `env:"ARCHIVE_REGION"`
	ArchiveAccessKeyID     string `env:"ARCHIVE_ACCESS_KEY_ID"`
	ArchiveSecretAccessKey string `env:"ARCHIVE_SECRET_ACCESS_KEY"`
	ArchiveUseSSL          bool   `env:"ARCHIVE_USE_SSL"`
	ArchiveBeforePrune     bool   `env:"ARCHIVE_BEFORE_PRUNE" envDefault:"false"`
	ArchiveMaxRetries      int    `env:"ARCHIVE_MAX_RETRIES" envDefault:"3"`

	// LagWarnLedgers triggers a warn-level log when the ingester falls this
	// many ledgers behind the chain head. Zero disables the alarm.
	LagWarnLedgers uint32 `env:"LAG_WARN_LEDGERS" envDefault:"100"`

	// Ingester retry backoff. A zero jitter max preserves the ingester's
	// proportional jitter behavior; non-zero bounds make the random range
	// explicit.
	IngesterMinBackoff time.Duration `env:"INGESTER_MIN_BACKOFF" envDefault:"1s"`
	IngesterMaxBackoff time.Duration `env:"INGESTER_MAX_BACKOFF" envDefault:"1m"`
	IngesterJitterMin  time.Duration `env:"INGESTER_JITTER_MIN" envDefault:"0"`
	IngesterJitterMax  time.Duration `env:"INGESTER_JITTER_MAX" envDefault:"0"`

	// GraphQLPlayground gates the dev-mode GraphiQL UI at /graphiql.
	GraphQLPlayground bool `env:"GRAPHQL_PLAYGROUND"`

	// Horizon backfill configuration. HORIZON_URL is the REST endpoint
	// the backfill command reads; BACKFILL_RATE_RPS controls how many
	// requests per second the backfill command issues (env/v11 parses
	// the float directly). Both are used only by `sorotrail backfill`,
	// not by the live indexer. The defaults match the documented
	// public-testnet target and a safe ~10 req/s pace; private
	// deployments can point HORIZON_URL at themselves and allow a
	// tighter rate via the flag or env override.
	HorizonURL      string  `env:"HORIZON_URL" envDefault:"https://horizon-testnet.stellar.org"`
	BackfillRateRPS float64 `env:"BACKFILL_RATE_RPS" envDefault:"10"`

	MetricsEnabled bool `env:"METRICS_ENABLED" envDefault:"false"`
	// RPC retry/backoff configuration. These control how many times a
	// failing RPC call is retried, the base (exponential) backoff duration,
	// the maximum backoff cap, and whether random jitter is added between
	// attempts. Applied uniformly to every RPC call (getEvents,
	// getLatestLedger, getHealth, getLedgerEntries).
	RPCMaxAttempts int           `env:"RPC_MAX_ATTEMPTS" envDefault:"3"`
	RPCBaseBackoff time.Duration `env:"RPC_BASE_BACKOFF" envDefault:"500ms"`
	RPCMaxBackoff  time.Duration `env:"RPC_MAX_BACKOFF" envDefault:"30s"`
	RPCJitter      bool          `env:"RPC_JITTER" envDefault:"true"`
	// RPCHTTPTimeout is the timeout on the underlying HTTP client's RPC
	// requests (e.g. "30s"). Zero (default) keeps the client's 30s timeout;
	// operators can raise it for a slow private RPC endpoint.
	RPCHTTPTimeout time.Duration `env:"RPC_HTTP_TIMEOUT" envDefault:"30s"`

	// Audit config. AUDIT_ENABLED=false (default) disables the auditor
	// entirely; the binary behaves exactly like the pre-audit build.
	AuditEnabled        bool          `env:"AUDIT_ENABLED" envDefault:"false"`
	AuditPollInterval   time.Duration `env:"AUDIT_POLL_INTERVAL" envDefault:"30s"`
	AuditBatchLedgers   uint32        `env:"AUDIT_BATCH_LEDGERS" envDefault:"100"`
	AuditLagThreshold   uint32        `env:"AUDIT_LAG_THRESHOLD" envDefault:"200"`
	AuditBudgetShare    float64       `env:"AUDIT_BUDGET_SHARE" envDefault:"0.10"`
	AuditMaxRPS         float64       `env:"AUDIT_MAX_RPS" envDefault:"10"`
	AuditMaxRepair      int           `env:"AUDIT_MAX_REPAIR_ATTEMPTS" envDefault:"3"`
	AuditFindingMaxLgrs uint32        `env:"AUDIT_FINDING_MAX_LEDGERS" envDefault:"100"`

	// HTTP server timeouts. Zero means no timeout for that field.
	// HTTP_READ_TIMEOUT limits the time to read the full request
	// (including body); HTTP_READ_HEADER_TIMEOUT limits header reads
	// only and is the most important defence against slow-client attacks.
	HTTPReadTimeout       time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"30s"`
	HTTPWriteTimeout      time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"30s"`
	HTTPIdleTimeout       time.Duration `env:"HTTP_IDLE_TIMEOUT" envDefault:"60s"`
	HTTPReadHeaderTimeout time.Duration `env:"HTTP_READ_HEADER_TIMEOUT" envDefault:"10s"`
	// HTTPRequestBodyLimit caps the max size accepted for any request body (all endpoints).
	// Default 1048576 (1 MiB) protects against memory/resource exhaustion. Set higher if needed when trusting clients.
	HTTPRequestBodyLimit int64 `env:"HTTP_REQUEST_BODY_LIMIT" envDefault:"1048576"`

	// APIKey, when set, gates the watched-contracts management endpoints
	// via a constant-time comparison against the X-API-Key request header.
	// Empty means the watched-contracts surface starts up rejected (every
	// request gets a 503 with a "no API_KEY configured" message), so
	// writes are never open even when other auth would be off.
	APIKey string `env:"API_KEY"`
	// HTTP rate limiting.
	RateLimitRPS          float64 `env:"RATE_LIMIT_RPS"`
	RateLimitBurst        int     `env:"RATE_LIMIT_BURST"`
	RateLimitTrustedProxy bool    `env:"RATE_LIMIT_TRUSTED_PROXY" envDefault:"false"`
	HourlyQuota           int64   `env:"HOURLY_QUOTA"`
	DailyQuota            int64   `env:"DAILY_QUOTA"`

	// CompressMinSize is the response body size, in bytes, at or above which
	// responses are gzip/deflate encoded for clients that advertise support.
	// Negative disables compression entirely; 0 uses api.CompressMinSize.
	CompressMinSize int `env:"COMPRESS_MIN_SIZE" envDefault:"0"`

	// EnableMetrics exposes the Prometheus /metrics endpoint.
	EnableMetrics bool `env:"ENABLE_METRICS" envDefault:"false"`
	// APIMaxLimit is the maximum page size accepted by the API for list
	// endpoints (/events, /subscriptions/{id}/deliveries). Values above
	// this are rejected with 400; the store still clamps internally as a
	// safety net. Default 500 (up from the previous hardcoded 200).
	APIMaxLimit int `env:"API_MAX_LIMIT" envDefault:"500"`

	// StatsCacheTTL is how long GET /stats results are served from the
	// per-scope cache before being recomputed, short-circuiting the
	// expensive aggregation on busy endpoints. Zero disables caching.
	// Default 5s keeps a dial board roughly in pace with ingestion.
	StatsCacheTTL time.Duration `env:"STATS_CACHE_TTL" envDefault:"5s"`

	// CachePrivate flips the cacheable endpoints from Cache-Control: public
	// to Cache-Control: private. Set this when the deployment serves
	// per-user data behind an auth layer (#17, not yet merged) so shared
	// caches (CDN/proxy) cannot leak responses across keys. Browsers can
	// still cache the response for the same authenticated user; CDNs and
	// intermediaries cannot. Defaults to false (the deployment does not
	// need request-scoped caching).
	CachePrivate bool `env:"CACHE_PRIVATE" envDefault:"false"`

	// ShutdownTimeout limits how long the graceful HTTP server drain and
	// component shutdown may take before the process is killed. Zero means
	// no timeout (wait indefinitely).
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`

	// Multi-tenancy (#48). MULTI_TENANT=false (the default) means no
	// authentication, no tenant boundary, and no usage accounting — every
	// request runs with full access exactly as it did before. Turning it on
	// makes an API key mandatory on every endpoint except /health.
	MultiTenant bool `env:"MULTI_TENANT" envDefault:"false"`
	// MultiTenantMaxWatched caps the union of all tenants' watch lists, so
	// tenants collectively cannot push the ingester past the RPC budget the
	// operator planned for. 0 disables the cap.
	MultiTenantMaxWatched int `env:"MULTI_TENANT_MAX_WATCHED" envDefault:"250"`
	// MultiTenantUsageFlush is how often accumulated per-tenant usage
	// counters are written.
	MultiTenantUsageFlush time.Duration `env:"MULTI_TENANT_USAGE_FLUSH" envDefault:"10s"`
	// MultiTenantStreamScopeSync bounds how long an open stream can keep
	// serving a contract whose grant has been revoked.
	MultiTenantStreamScopeSync time.Duration `env:"MULTI_TENANT_STREAM_SCOPE_SYNC" envDefault:"30s"`
	// MultiTenantBootstrapKey solves the chicken-and-egg of a fresh
	// multi-tenant install: minting the first key requires an admin key,
	// which requires minting a key. When set, this value is installed as an
	// API key for the seeded "default" admin tenant at startup, so the
	// operator has a way in. Treat it as a secret; rotate it by revoking
	// through /admin once real keys exist.
	MultiTenantBootstrapKey string `env:"MULTI_TENANT_BOOTSTRAP_KEY"`

	// SweepConcurrency is the maximum number of filter batches that may be
	// fetched concurrently during a windowSweep pass. The Stellar RPC's
	// getEvents caps each request chain at 5 filters × 5 contracts = 25
	// contracts, so anything beyond that needs multiple request chains
	// paged through one ledger window — this knob lets us fan those out.
	//
	// Default is 1: the public RPC's interval limiter (HTTPClient's
	// ~10 req/s ceiling) already serializes parallel calls, so a higher
	// value only helps against private RPCs that allow more headroom.
	// Anything below 1 is invalid.
	SweepConcurrency int `env:"SWEEP_CONCURRENCY" envDefault:"1"`

	// MaxEventsPerCycle caps how many events a single ingestion cycle may
	// process, bounding per-cycle memory and latency on busy chains. When
	// the cap is hit mid-window the sweep stops fetching and the ingest
	// frontier stays put, so the next cycle resumes with idempotent
	// upserts covering the overlap; in single-page mode the pagination
	// limit is clamped down to the cap instead. Zero (the default)
	// disables the cap — identical to the pre-cap behavior.
	MaxEventsPerCycle uint `env:"MAX_EVENTS_PER_CYCLE"`

	// Event write batching and backpressure. BATCH_SIZE caps the number of
	// events in each store write (UpsertEvents), splitting a fetched page
	// into smaller chunks so high-volume chains don't land one giant batch
	// on the DB at once. Zero (the default) keeps the historical single-
	// write-per-page behavior bit-for-bit.
	//
	// When BATCH_SIZE is set AND BATCH_TARGET_LATENCY is set, the page's
	// chunk size becomes adaptive: writes measured over the latency budget
	// cause the chunk size to shrink and a backpressure sleep
	// (BATCH_MAX_BACKOFF) to be inserted between writes, so a strapped
	// database gets room to drain rather than being hammered at peak.
	// BATCH_TARGET_LATENCY zero (the default) means the chunk size is
	// fixed at BATCH_SIZE and no backpressure is applied.
	BatchSize          uint          `env:"BATCH_SIZE"`
	BatchTargetLatency time.Duration `env:"BATCH_TARGET_LATENCY"`
	BatchMaxBackoff    time.Duration `env:"BATCH_MAX_BACKOFF" envDefault:"1s"`

	// ReorgConfirmationWindow is the number of ledgers behind the ingest
	// frontier that get re-scanned on a periodic basis to detect and
	// repair RPC-side reorgs. Once a ledger is more than this many ledgers
	// behind the frontier it is considered finalized and never rewritten.
	// Zero means reorg detection is disabled.
	ReorgConfirmationWindow uint32 `env:"REORG_CONFIRMATION_WINDOW" envDefault:"64"`

	// ReorgRescanInterval is how often the ingester performs a reorg
	// re-scan over the recent finalized window. The re-scan is folded
	// into the existing ingest loop so it shares the RPC rate budget
	// and never races live ingestion; this knob controls how often the
	// window gets re-fetched, not how often the ingest loop polls.
	ReorgRescanInterval time.Duration `env:"REORG_RESCAN_INTERVAL" envDefault:"1m"`

	// ExportMaxRange caps the ledger span a /contracts/{id}/export call
	// may request. The handler streams events from the store with
	// chunked transfer encoding, but unbounded spans risk OOM and
	// uncooperative GC pauses on big results; the cap is configurable so
	// private deployments can opt for a larger analytical dump.
	ExportMaxRange int64 `env:"EXPORT_MAX_RANGE" envDefault:"17280"`

	// IngestionLockEnabled, when true, acquires a Postgres advisory lock
	// keyed by the RPC URL before starting the ingestion loop. A second
	// instance attempting to acquire the same lock will skip ingestion
	// (but continue serving the HTTP API), preventing double-processing.
	// Default false — single-instance deployments keep today's behavior.
	IngestionLockEnabled bool `env:"INGESTION_LOCK_ENABLED" envDefault:"false"`
	// CORS configuration. Default policy is deny-all: an empty
	// CORSAllowedOrigins list means no browser cross-origin request gets
	// CORS headers, so the browser blocks the response. Allowing a single
	// origin ("https://app.example.com") sets Access-Control-Allow-Origin
	// to that exact value on every request with a matching Origin header;
	// "*" is a special case that allows any origin (and intentionally
	// voids the Vary: Origin contract because the response is identical
	// regardless of origin — see parseAllowedOrigins).
	//
	// CORSAllowedMethods and CORSAllowedHeaders are returned on the
	// preflight (OPTIONS) response so a browser knowing the allowed
	// methods/headers can complete a non-simple cross-origin request.
	// The defaults cover the surface this API actually exposes; operators
	// adding custom routes (e.g. application/json PATCH) should extend
	// these.
	CORSAllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" envDefault:""`
	CORSAllowedMethods []string `env:"CORS_ALLOWED_METHODS" envDefault:"GET,POST,PUT,DELETE,OPTIONS"`
	CORSAllowedHeaders []string `env:"CORS_ALLOWED_HEADERS" envDefault:"Content-Type,X-API-Key,Accept"`
	// CORSExposedHeaders is returned as Access-Control-Expose-Headers on
	// responses to allowed origins so browser JavaScript can read those
	// response headers. X-Request-ID is set on every response by the API's
	// request logger (#29), so it is the default; an operator can extend
	// the list or empty it to suppress the header entirely.
	CORSExposedHeaders []string `env:"CORS_EXPOSED_HEADERS" envDefault:"X-Request-ID,X-RateLimit-Limit,X-RateLimit-Remaining,X-RateLimit-Reset"`
}

// Load reads configuration from the environment and validates it.
// All validation failures are aggregated into a single error.
func Load() (Config, error) {
	if err := resolveFileEnv(); err != nil {
		return Config{}, fmt.Errorf("resolving *_FILE environment values: %w", err)
	}
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing environment: %w", err)
	}
	cfg.WatchedContracts = cleanContractList(cfg.WatchedContracts)
	cfg.RPCURLS = cleanContractList(cfg.RPCURLS)
	cfg.CORSAllowedOrigins = cleanOrigins(cfg.CORSAllowedOrigins)
	cfg.Network = strings.ToLower(strings.TrimSpace(cfg.Network))
	if _, ok := NetworkConfigFor(cfg.Network); !ok {
		return Config{}, fmt.Errorf("NETWORK must be one of testnet, mainnet, futurenet; got %q", cfg.Network)
	}
	if _, explicitlySet := os.LookupEnv("RPC_URL"); !explicitlySet && len(cfg.RPCURLS) == 0 {
		cfg.RPCURL = networks[cfg.Network].RPCURL
	}
	if err := cfg.ValidateAll(); err != nil {
		return Config{}, err
	}
	// Validate() holds the per-field rules that predate ValidateAll
	// (API_QUERY_TIMEOUT, LOG_FORMAT, the HTTP_* timeouts, audit and
	// retention bounds). It is called here rather than from ValidateAll
	// because it assumes envDefaults have been applied, which is only true
	// on this path; ValidateAll also runs against hand-built Configs in
	// tests. Without this call those rules were silently unenforced.
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// IsSQLite reports whether the database URL points to a SQLite database.
func resolveFileEnv() error {
	for _, kv := range os.Environ() {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		name, filePath := parts[0], parts[1]
		if !strings.HasSuffix(name, "_FILE") {
			continue
		}
		baseName := strings.TrimSuffix(name, "_FILE")
		if _, set := os.LookupEnv(baseName); set {
			continue
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("%s: reading %q: %w", name, filePath, err)
		}
		if err := os.Setenv(baseName, strings.TrimRight(string(data), "\r\n")); err != nil {
			return fmt.Errorf("%s: setting %s: %w", name, baseName, err)
		}
	}
	return nil
}

func IsSQLite(databaseURL string) bool {
	return strings.HasPrefix(databaseURL, "sqlite:")
}

// ParseLogLevel maps a LOG_LEVEL string to a slog.Level. Unknown or empty
// values fall back to info rather than failing startup.
func ParseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogHandler returns the slog.Handler selected by a LOG_FORMAT value,
// writing to w with the given options. "json" selects the JSON handler;
// everything else — including unknown or empty values — falls back to the
// text handler rather than failing startup (mirrors ParseLogLevel).
func NewLogHandler(w io.Writer, format string, opts *slog.HandlerOptions) slog.Handler {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		return slog.NewJSONHandler(w, opts)
	default:
		return slog.NewTextHandler(w, opts)
	}
}

// Validate checks the configuration for values that would fail at runtime.
func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	// RPC_URLS takes priority when set; RPC_URL is the single-provider
	// fallback that works unchanged for existing deployments.
	if len(c.RPCURLS) > 0 {
		for i, raw := range c.RPCURLS {
			u, err := url.Parse(raw)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("RPC_URLS[%d] %q is not a valid URL", i, raw)
			}
		}
	} else if !IsSQLite(c.DatabaseURL) {
		u, err := url.Parse(c.RPCURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("RPC_URL %q is not a valid URL", c.RPCURL)
		}
	}
	if IsSQLite(c.DatabaseURL) {
		path := c.DatabaseURL[7:]
		if path == "" || path == ":memory:" {
		} else if path[0] != '/' && path[0] != '.' && path[0] != ':' {
			return fmt.Errorf("sqlite DATABASE_URL %q must be an absolute or relative path (or :memory:)", c.DatabaseURL)
		}
	}
	if c.DBMaxConns < 0 {
		return fmt.Errorf("DB_MAX_CONNS must be non-negative, got %d", c.DBMaxConns)
	}
	if c.DBMinConns < 0 {
		return fmt.Errorf("DB_MIN_CONNS must be non-negative, got %d", c.DBMinConns)
	}
	if c.DBMaxConnLifetime < 0 {
		return fmt.Errorf("DB_MAX_CONN_LIFETIME must be non-negative, got %s", c.DBMaxConnLifetime)
	}
	if c.DBMaxConnIdleTime < 0 {
		return fmt.Errorf("DB_MAX_CONN_IDLE_TIME must be non-negative, got %s", c.DBMaxConnIdleTime)
	}
	// Empty means "unused": HORIZON_URL is only read by `sorotrail backfill`
	// and Load supplies a default, so an unset value is not a misconfigured
	// indexer. Validate the shape only when someone actually set one.
	if u, err := url.Parse(c.HorizonURL); c.HorizonURL != "" && (err != nil || u.Scheme == "" || u.Host == "") {
		return fmt.Errorf("HORIZON_URL %q is not a valid URL", c.HorizonURL)
	}

	if c.HTTPAddr != "" {
		if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
			return fmt.Errorf("HTTP_ADDR %q is not a valid host:port (e.g. :8080)", c.HTTPAddr)
		}
	}

	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be positive, got %s", c.PollInterval)
	}
	if c.APIQueryTimeout <= 0 {
		return fmt.Errorf("API_QUERY_TIMEOUT must be positive, got %s", c.APIQueryTimeout)
	}
	if c.APISlowQueryThreshold <= 0 {
		return fmt.Errorf("API_SLOW_QUERY_THRESHOLD must be positive, got %s", c.APISlowQueryThreshold)
	}
	if c.RetentionLedgers == 0 {
		return fmt.Errorf("RETENTION_LEDGERS must be positive")
	}
	if c.RetentionAge < 0 {
		return fmt.Errorf("RETENTION_AGE must be non-negative")
	}
	// RetentionPoll mirrors RetentionAge: it only matters once age-based
	// pruning is enabled, and zero (the hand-built, non-default value) is a
	// valid "disabled" state rather than a misconfiguration.
	if c.RetentionPoll < 0 {
		return fmt.Errorf("RETENTION_POLL_INTERVAL must be non-negative")
	}
	if c.PartitionLedgerSpan == 0 {
		return fmt.Errorf("PARTITION_LEDGER_SPAN must be positive")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("LOG_LEVEL %q is not one of debug|info|warn|error", c.LogLevel)
	}
	switch strings.ToLower(c.LogFormat) {
	case "text", "json":
	default:
		return fmt.Errorf("LOG_FORMAT %q is not one of text|json", c.LogFormat)
	}
	for _, id := range c.WatchedContracts {
		if !ValidContractID(id) {
			return fmt.Errorf("WATCHED_CONTRACTS entry %q is not a valid contract ID (want C... strkey, 56 chars)", id)
		}
	}
	if c.AuditPollInterval <= 0 {
		return fmt.Errorf("AUDIT_POLL_INTERVAL must be positive")
	}
	if c.AuditBatchLedgers == 0 {
		return fmt.Errorf("AUDIT_BATCH_LEDGERS must be positive")
	}
	if c.AuditLagThreshold == 0 {
		return fmt.Errorf("AUDIT_LAG_THRESHOLD must be positive")
	}
	if c.AuditBudgetShare < 0 || c.AuditBudgetShare > 1 {
		return fmt.Errorf("AUDIT_BUDGET_SHARE must be in [0,1]")
	}
	if c.AuditMaxRPS <= 0 {
		return fmt.Errorf("AUDIT_MAX_RPS must be positive")
	}
	if c.AuditMaxRepair <= 0 {
		return fmt.Errorf("AUDIT_MAX_REPAIR_ATTEMPTS must be positive")
	}
	if c.AuditFindingMaxLgrs == 0 {
		return fmt.Errorf("AUDIT_FINDING_MAX_LEDGERS must be positive")
	}
	if c.RPCRateLimitRPS <= 0 {
		return fmt.Errorf("RPC_RATE_LIMIT_RPS must be positive")
	}
	if c.RPCRateLimit <= 0 {
		return fmt.Errorf("RPC_RATE_LIMIT must be positive, got %v", c.RPCRateLimit)
	}
	if c.RetentionBatchSize <= 0 {
		return fmt.Errorf("RETENTION_BATCH_SIZE must be positive")
	}
	if c.IngestPageSize == 0 {
		return fmt.Errorf("INGEST_PAGE_SIZE must be positive")
	}
	if c.IngestBatchSize == 0 {
		return fmt.Errorf("INGEST_BATCH_SIZE must be positive")
	}
	if c.RetentionPause < 0 {
		return fmt.Errorf("RETENTION_PAUSE must be non-negative")
	}
	if c.RetentionInterval <= 0 {
		return fmt.Errorf("RETENTION_INTERVAL must be positive")
	}
	if c.RetentionMaxAge < 0 {
		return fmt.Errorf("RETENTION_MAX_AGE must be non-negative")
	}
	if c.BackfillRateRPS <= 0 {
		return fmt.Errorf("BACKFILL_RATE_RPS must be positive, got %v", c.BackfillRateRPS)
	}
	if c.RPCMaxAttempts <= 0 {
		return fmt.Errorf("RPC_MAX_ATTEMPTS must be positive, got %d", c.RPCMaxAttempts)
	}
	if c.RPCBaseBackoff <= 0 {
		return fmt.Errorf("RPC_BASE_BACKOFF must be positive, got %s", c.RPCBaseBackoff)
	}
	if c.RPCMaxBackoff <= 0 {
		return fmt.Errorf("RPC_MAX_BACKOFF must be positive, got %s", c.RPCMaxBackoff)
	}
	if c.RPCHTTPTimeout < 0 {
		return fmt.Errorf("RPC_HTTP_TIMEOUT must be non-negative, got %s", c.RPCHTTPTimeout)
	}
	if c.RateLimitRPS < 0 {
		return fmt.Errorf("RATE_LIMIT_RPS must be non-negative")
	}
	if c.RateLimitBurst < 0 {
		return fmt.Errorf("RATE_LIMIT_BURST must be non-negative")
	}
	if c.HTTPReadTimeout < 0 {
		return fmt.Errorf("HTTP_READ_TIMEOUT must be non-negative, got %s", c.HTTPReadTimeout)
	}
	if c.HTTPWriteTimeout < 0 {
		return fmt.Errorf("HTTP_WRITE_TIMEOUT must be non-negative, got %s", c.HTTPWriteTimeout)
	}
	if c.HTTPIdleTimeout < 0 {
		return fmt.Errorf("HTTP_IDLE_TIMEOUT must be non-negative, got %s", c.HTTPIdleTimeout)
	}
	if c.HTTPReadHeaderTimeout < 0 {
		return fmt.Errorf("HTTP_READ_HEADER_TIMEOUT must be non-negative, got %s", c.HTTPReadHeaderTimeout)
	}
	if c.HTTPRequestBodyLimit < 0 {
		return fmt.Errorf("HTTP_REQUEST_BODY_LIMIT must be non-negative, got %d", c.HTTPRequestBodyLimit)
	}
	if c.APIMaxLimit < 1 {
		return fmt.Errorf("API_MAX_LIMIT must be positive, got %d", c.APIMaxLimit)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be non-negative, got %s", c.ShutdownTimeout)
	}
	if c.SweepConcurrency < 1 {
		return fmt.Errorf("SWEEP_CONCURRENCY must be positive, got %d", c.SweepConcurrency)
	}
	// MAX_EVENTS_PER_CYCLE needs no explicit rule: the env parser rejects
	// negatives (uint), and zero is the documented "disabled" value.
	// BATCH_TARGET_LATENCY and BATCH_MAX_BACKOFF tune the batching
	// behavior that only exists when BATCH_SIZE is set, but tolerating
	// them when batching is off (a config that sets a budget but forgets
	// the size) is safer than rejecting it — the operator sees a warning
	// at startup rather than a crash loop only during a deploy review.
	if c.BatchSize > 0 && c.BatchTargetLatency < 0 {
		return fmt.Errorf("BATCH_TARGET_LATENCY must be non-negative, got %s", c.BatchTargetLatency)
	}
	if c.BatchMaxBackoff < 0 {
		return fmt.Errorf("BATCH_MAX_BACKOFF must be non-negative, got %s", c.BatchMaxBackoff)
	}
	if c.ReorgConfirmationWindow > 0 && c.ReorgRescanInterval <= 0 {
		return fmt.Errorf("REORG_RESCAN_INTERVAL must be positive when REORG_CONFIRMATION_WINDOW is set")
	}
	if c.ExportMaxRange <= 0 {
		return fmt.Errorf("EXPORT_MAX_RANGE must be positive, got %d", c.ExportMaxRange)
	}
	if err := validateCORSOrigins(c.CORSAllowedOrigins); err != nil {
		return err
	}
	// Both must be set together: half-configured limits would silently
	// behave like the disabled case (Enabled returns false when either is
	// non-positive), which would confuse operators who set one and
	// expected throttling to kick in.
	if (c.RateLimitRPS > 0) != (c.RateLimitBurst > 0) {
		return fmt.Errorf("RATE_LIMIT_RPS and RATE_LIMIT_BURST must both be set or both unset")
	}
	if c.MultiTenantMaxWatched < 0 {
		return fmt.Errorf("MULTI_TENANT_MAX_WATCHED must be non-negative")
	}
	if c.MultiTenantUsageFlush <= 0 {
		return fmt.Errorf("MULTI_TENANT_USAGE_FLUSH must be positive")
	}
	if c.MultiTenantStreamScopeSync <= 0 {
		return fmt.Errorf("MULTI_TENANT_STREAM_SCOPE_SYNC must be positive")
	}
	// Rejected rather than ignored: an operator who sets a bootstrap key
	// without enabling multi-tenancy has configured a credential that
	// authenticates nothing, and would reasonably assume the instance is
	// protected when it is wide open.
	if c.MultiTenantBootstrapKey != "" && !c.MultiTenant {
		return fmt.Errorf("MULTI_TENANT_BOOTSTRAP_KEY is set but MULTI_TENANT is false")
	}
	// Archive configuration: ARCHIVE_BUCKET is the master switch. When
	// set, the endpoint must also be provided (S3-compatible stores
	// need an explicit endpoint).
	if c.ArchiveBucket != "" && c.ArchiveEndpoint == "" {
		return fmt.Errorf("ARCHIVE_ENDPOINT is required when ARCHIVE_BUCKET is set")
	}
	if c.ArchiveMaxRetries < 0 {
		return fmt.Errorf("ARCHIVE_MAX_RETRIES must be non-negative")
	}
	return nil
}

// RetentionEnabled reports whether at least one retention policy is
// configured — the pruner only runs when this is true.
func (c Config) RetentionEnabled() bool {
	return c.RetentionMaxAge > 0 || c.RetentionMinLedger > 0
}

// ArchiveEnabled reports whether the S3-compatible archive backend
// is configured. When true, pruned events can be exported before
// deletion.
func (c Config) ArchiveEnabled() bool {
	return c.ArchiveBucket != ""
}

// ValidContractID reports whether s looks like a Soroban contract strkey.
func ValidContractID(s string) bool {
	if len(s) != 56 || s[0] != 'C' {
		return false
	}
	for _, r := range s[1:] {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", r) {
			return false
		}
	}
	return true
}

// ValidCursor reports whether s is a valid pagination cursor.
// A cursor must be non-empty, at most 128 characters, and consist only of
// alphanumeric characters, hyphens, underscores, dots, or colons.
func ValidCursor(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') &&
			(r < 'A' || r > 'Z') &&
			(r < '0' || r > '9') &&
			r != '-' && r != '_' && r != '.' && r != ':' {
			return false
		}
	}
	return true
}

// ParseStartLedger parses a START_LEDGER value that may be an absolute
// ledger number or a relative offset like "latest-1000". Absolute values
// must be positive. Relative offsets subtract from latestLedger and must
// resolve to at least ledger 2.
func ParseStartLedger(raw string, latestLedger ...uint32) (uint32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if n, err := strconv.ParseUint(raw, 10, 32); err == nil {
		if n < 2 {
			return 0, fmt.Errorf("START_LEDGER %q: ledger must be >= 2", raw)
		}
		return uint32(n), nil
	}
	if !strings.HasPrefix(strings.ToLower(raw), "latest-") {
		return 0, fmt.Errorf("START_LEDGER %q: not an absolute ledger number or relative offset (expected N or latest-N)", raw)
	}
	offsetStr := raw[len("latest-"):]
	offset, err := strconv.ParseUint(offsetStr, 10, 32)
	if err != nil || offset == 0 {
		return 0, fmt.Errorf("START_LEDGER %q: offset must be a positive integer", raw)
	}
	if len(latestLedger) == 0 || latestLedger[0] == 0 {
		return 0, fmt.Errorf("START_LEDGER %q: relative offset requires RPC latest ledger (not available at config parse time)", raw)
	}
	latest := int64(latestLedger[0])
	start := latest - int64(offset)
	if start < 2 {
		start = 2
	}
	return uint32(start), nil
}

// ValidOrigin reports whether s is a valid CORS origin: either the "*"
// wildcard (allow any origin) or an absolute http/https URL whose host is
// present and which carries no path, query, or fragment (browsers never
// send those in the Origin header).
func ValidOrigin(s string) bool {
	if s == "*" {
		return true
	}
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return true
}

func cleanContractList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// validateCORSOrigins rejects origin entries that can't be safely compared
// to a browser-supplied Origin header. Rejecting "null" matters because
// browsers send Origin: null for sandboxed iframes / file:// pages /
// redirects — accepting it would mean any third-party page could call
// the API with credentials if X-API-Key happened to be known.
//
// "*" is a valid literal but documented separately as a special case the
// middleware recognizes (see API CORS handler).
// cleanOrigins normalizes CORS origin entries: env/v11 splits on commas but
// preserves whitespace, and operators commonly paste origins with a trailing
// slash, so each entry is trimmed and any trailing "/" removed.
func cleanOrigins(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, strings.TrimSuffix(s, "/"))
		}
	}
	return out
}

func validateCORSOrigins(in []string) error {
	for _, o := range in {
		o = strings.TrimSpace(o)
		if o == "" || o == "*" {
			continue
		}
		if strings.EqualFold(o, "null") {
			return fmt.Errorf("CORS_ALLOWED_ORIGINS entry %q is not allowed (sandboxed Origin: null is a credentialed-origin bypass)", o)
		}
		if !ValidOrigin(o) {
			return fmt.Errorf("CORS_ALLOWED_ORIGINS entry %q is not a valid origin (want scheme://host[:port])", o)
		}
	}
	return nil
}

// LoggableFields returns the configuration as a map of fields suitable for logging,
// with credentials redacted (e.g., from DATABASE_URL).
func (c Config) LoggableFields() []any {
	dbURL := c.DatabaseURL
	if u, err := url.Parse(c.DatabaseURL); err == nil {
		u.User = nil
		dbURL = u.String()
	}
	return []any{
		"network", c.Network,
		"rpc_url", c.RPCURL,
		"metrics_enabled", c.MetricsEnabled,
		"rpc_max_attempts", c.RPCMaxAttempts,
		"rpc_base_backoff", c.RPCBaseBackoff,
		"rpc_max_backoff", c.RPCMaxBackoff,
		"rpc_jitter", c.RPCJitter,
		"ingester_min_backoff", c.IngesterMinBackoff,
		"ingester_max_backoff", c.IngesterMaxBackoff,
		"ingester_jitter_min", c.IngesterJitterMin,
		"ingester_jitter_max", c.IngesterJitterMax,
		"rpc_rate_limit", c.RPCRateLimit,
		"database_url", dbURL,
		"poll_interval", c.PollInterval,
		"http_addr", c.HTTPAddr,
		"watched_contracts", len(c.WatchedContracts),
		"start_ledger", c.StartLedger,
		"start_ledger_raw", c.StartLedgerRaw,
		"retention_ledgers", c.RetentionLedgers,
		"log_level", c.LogLevel,
		"http_read_timeout", c.HTTPReadTimeout,
		"http_write_timeout", c.HTTPWriteTimeout,
		"http_idle_timeout", c.HTTPIdleTimeout,
		"http_read_header_timeout", c.HTTPReadHeaderTimeout,
		"shutdown_timeout", c.ShutdownTimeout,
		"sweep_concurrency", c.SweepConcurrency,
		"max_events_per_cycle", c.MaxEventsPerCycle,
		"batch_size", c.BatchSize,
		"batch_target_latency", c.BatchTargetLatency,
		"batch_max_backoff", c.BatchMaxBackoff,
		"reorg_confirmation_window", c.ReorgConfirmationWindow,
		"reorg_rescan_interval", c.ReorgRescanInterval,
		"export_max_range", c.ExportMaxRange,
		"ingestion_lock_enabled", c.IngestionLockEnabled,
		"cors_allowed_origins", len(c.CORSAllowedOrigins),
		"audit_enabled", c.AuditEnabled,
	}
}
