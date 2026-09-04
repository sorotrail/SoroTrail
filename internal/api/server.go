// Package api serves stored events over HTTP. Endpoints are documented in
// the OpenAPI spec and the README.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/pruner"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type ctxKey string

const loggerCtxKey ctxKey = "logger"

var (
	auditorMu sync.RWMutex
	auditor   *audit.Auditor
)

func SetAuditor(a *audit.Auditor) {
	auditorMu.Lock()
	auditor = a
	auditorMu.Unlock()
}

// SetPruner registers the binary's Pruner so /stats can surface its
// Metrics counters. There is exactly one Pruner per process; like the
// auditor, it is a no-op when retention is not configured.
//
// Like SetAuditor this MUST be called BEFORE the API starts serving
// requests (i.e. before http.Server.ListenAndServe), so the first
// /stats request observes a stable value rather than a nil pruner.
// cmd/sorotrail/main does this in the wiring phase before constructing
// the http.Server.
//
// The local variable name is `prn` (not `pruner`) because the pruner
// package shares the name and a same-named variable would shadow it
// inside this file. `prn` matches the shorthand already used in
// cmd/sorotrail/main.go.
var (
	prunerMu sync.RWMutex
	prn      *pruner.Pruner
)

func SetPruner(p *pruner.Pruner) {
	prunerMu.Lock()
	prn = p
	prunerMu.Unlock()
}

func getPruner() *pruner.Pruner {
	prunerMu.RLock()
	defer prunerMu.RUnlock()
	return prn
}

func getAuditor() *audit.Auditor {
	auditorMu.RLock()
	defer auditorMu.RUnlock()
	return auditor
}

// SpecCacheStatsSource supplies spec-cache metrics for /stats. Mirrors
// the SetAuditor pattern: one setter, called before ListenAndServe.
// nil (the default) leaves the /stats spec_cache field omitted.
type SpecCacheStatsSource interface {
	SpecCacheStats() store.SpecCacheStats
}

var (
	specCacheMu     sync.RWMutex
	specCacheSource SpecCacheStatsSource
)

func SetSpecCache(src SpecCacheStatsSource) {
	specCacheMu.Lock()
	specCacheSource = src
	specCacheMu.Unlock()
}

func getSpecCache() SpecCacheStatsSource {
	specCacheMu.RLock()
	defer specCacheMu.RUnlock()
	return specCacheSource
}

// Enricher is the spec-based event enrichment interface used by the API.
type Enricher interface {
	EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent
}

// SetRPCCounter registers the CountingClient so /stats can expose
// per-method RPC error totals. Call this before ListenAndServe.
// The setter is guarded by a RWMutex so concurrent /stats readers
// never observe a torn pointer.
var (
	rpcCounterMu sync.RWMutex
	rpcCounter   *rpc.CountingClient
)

func SetRPCCounter(c *rpc.CountingClient) {
	rpcCounterMu.Lock()
	rpcCounter = c
	rpcCounterMu.Unlock()
}

func getRPCCounter() *rpc.CountingClient {
	rpcCounterMu.RLock()
	defer rpcCounterMu.RUnlock()
	return rpcCounter
}

// SetIngester registers the binary's Ingester so /stats can surface its
// adaptive poll interval (issue #146). There is exactly one Ingester per
// process, always constructed (even when INGESTION_LOCK_ENABLED causes
// Run to be skipped on this instance) — main.go calls this unconditionally
// right after building it.
//
// Like SetPruner this MUST be called BEFORE the API starts serving
// requests, so the first /stats request observes a stable value rather
// than a nil ingester.
var (
	ingesterMu sync.RWMutex
	ing        *ingester.Ingester
)

func SetIngester(i *ingester.Ingester) {
	ingesterMu.Lock()
	ing = i
	ingesterMu.Unlock()
}

func getIngester() *ingester.Ingester {
	ingesterMu.RLock()
	defer ingesterMu.RUnlock()
	return ing
// SpecCacheStatsSource supplies spec-cache metrics for /stats. Mirrors
// the SetAuditor pattern: one setter, called before ListenAndServe.
// nil (the default) leaves the /stats spec_cache field omitted.
type SpecCacheStatsSource interface {
	SpecCacheStats() store.SpecCacheStats
}

var (
	specCacheMu     sync.RWMutex
	specCacheSource SpecCacheStatsSource
)

func SetSpecCache(src SpecCacheStatsSource) {
	specCacheMu.Lock()
	specCacheSource = src
	specCacheMu.Unlock()
}

func getSpecCache() SpecCacheStatsSource {
	specCacheMu.RLock()
	defer specCacheMu.RUnlock()
	return specCacheSource
}

// Enricher is the spec-based event enrichment interface used by the API.
// Defined here so the API package doesn't import internal/spec directly.
// DecodeStats lets /stats surface the enrichment decode failure rate when a
// concrete enricher is wired; nil servers (decoded=true unavailable) simply
// leave the stats field empty.
type Enricher interface {
	EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent
	DecodeStats() store.DecodeStats
}

// Server holds the API's dependencies.
type Server struct {
	store    store.Store
	rpc      rpc.Client
	enricher Enricher
	log      *slog.Logger
	limiter  *RateLimiter
	bcast    *broadcast.Broadcaster

	// apiKeyAuth turns on API key authentication for the write,
	// streaming, subscription-management, and key-management routes.
	// Off by default: the API behaves exactly as before.
	apiKeyAuth bool
}

// New builds the API server. rpcClient is only used by /health.
// enricher is optional — pass nil to disable spec decoding.
func New(st store.Store, rpcClient rpc.Client, log *slog.Logger, enricher ...Enricher) *Server {
	s := &Server{store: st, rpc: rpcClient, log: log}
	store            store.Store
	rpc              rpc.Client
	log              *slog.Logger
	apiKey           string
	limiter          *RateLimiter
	bcast            *broadcast.Broadcaster
	enricher         Enricher
	enableMetrics    bool
	recoverer        *Recoverer
	metrics          *metrics.HTTPMetrics
	tracer           trace.Tracer
	retentionLedgers uint32

	httpRequestBodyLimit int64 // max accepted request body size, in bytes

	// GraphQL transport, injected by main after the server is built.
	// internal/api/graphql imports this package for its ServerDeps, so
	// the dependency has to run in this direction to avoid an import
	// cycle. Both nil means the /graphql routes are simply not mounted.
	graphqlHandler    http.Handler
	graphqlPlayground http.Handler
	// compressMinSize is the body size at which responses start being
	// compressed. The zero value means CompressMinSize, so compression is on
	// by default; negative disables the middleware entirely.
	compressMinSize int

	// Multi-tenancy (#48). multiTenant is false by default, which makes
	// every request run as an untenanted wildcard principal — the exact
	// pre-#48 behavior. tenants is nil in that mode and must not be
	// dereferenced outside the multiTenant branches.
	multiTenant     bool
	tenants         store.TenantStore
	usage           *UsageRecorder
	maxWatched      int
	streamScopeSync time.Duration

	// statsTTL is how long a /stats result is served from cache before it
	// is recomputed. Zero disables caching (the pre-cache behavior).
	// config-driven wiring sets STATS_CACHE_TTL.
	statsTTL time.Duration
	// statsCache is the per-scope TTL cache used by /stats. Built lazily on
	// the first request so tests and hand-built Servers don't allocate it
	// until needed.
	statsCache *StatsCache

	// exportMaxRange caps the ledger span of /contracts/{id}/export.
	// Zero means unbounded (legacy behavior); config-driven wiring sets
	// EXPORT_MAX_RANGE so requests default to a sane ceiling.
	exportMaxRange int64
	// cors is the CORS middleware config. Wired via SetCORS from main so
	// the API does not import the config package.
	cors CORSConfig
}

// SetHTTPRequestBodyLimit sets the request body size limit, in bytes, for all handlers accepting a body.
func (s *Server) SetHTTPRequestBodyLimit(n int64) {
	s.httpRequestBodyLimit = n
}

// SetCompressMinSize overrides the body size at which responses are
// compressed. Pass a negative value to disable compression.
func (s *Server) SetCompressMinSize(n int) {
	s.compressMinSize = n
}

// SetMetricsEnabled enables or disables the /metrics endpoint.
func (s *Server) SetMetricsEnabled(enabled bool) {
	s.enableMetrics = enabled
}

// maxLimit is the API's upper bound for page-size parameters (limit and
// recent); values above it are rejected with 400 before the store is
// consulted. It is set once at startup via SetMaxLimit (driven by the
// API_MAX_LIMIT env var) before any requests are served so no mutex is
// needed. Default 500 — keep it at or below store.MaxQueryLimit: the store
// hard-clamps every query at that constant, so an API_MAX_LIMIT above 500
// accepts limits the store then silently truncates.
var maxLimit = 500

// SetMaxLimit configures the API's maximum page size for list endpoints.
// Call once at startup before ListenAndServe. Values ≤0 are ignored.
func SetMaxLimit(n int) {
	if n > 0 {
		maxLimit = n
	}
}

// SetStatsTTL configures how long GET /stats results are served from the
// per-scope cache before being recomputed. Zero disables caching (every
// request recomputes). Config-driven wiring sets STATS_CACHE_TTL; call
// before ListenAndServe so the first request observes it.
func (s *Server) SetStatsTTL(ttl time.Duration) {
	s.statsTTL = ttl
}

// New builds the API server. rpcClient is used by /health, /readyz, and /stats.
// apiKey gates the watched-contracts management endpoints; pass "" to
// fail closed (every request gets a 503 with "API_KEY not configured").
func New(st store.Store, rpcClient rpc.Client, log *slog.Logger, apiKey string, enricher ...Enricher) *Server {
	s := &Server{store: st, rpc: rpcClient, log: log, apiKey: apiKey, recoverer: NewRecoverer(log), metrics: metrics.New()}
	if len(enricher) > 0 {
		s.enricher = enricher[0]
	}
	if s.store != nil {
		s.store = store.NewTracingStore(s.store, s.tracer)
	}
	return s
}

func (s *Server) WithTracer(tracer trace.Tracer) *Server {
	if tracer != nil {
		s.tracer = tracer
		if s.store != nil {
			s.store = store.NewTracingStore(s.store, s.tracer)
		}
	}
	return s
}

// MultiTenantOptions configures the tenant boundary.
type MultiTenantOptions struct {
	// MaxWatchedContracts caps the union of every tenant's watch list, so
	// tenants collectively cannot drive the ingester's RPC cost past what
	// the operator budgeted. 0 means no instance cap.
	MaxWatchedContracts int
	// UsageFlushInterval controls how often accumulated usage counters are
	// persisted. Zero uses DefaultUsageFlushInterval.
	UsageFlushInterval time.Duration
	// StreamScopeSync is how often a live stream re-resolves its tenant's
	// grants. Zero uses DefaultStreamScopeSync.
	StreamScopeSync time.Duration
}

// WithMultiTenancy turns on tenant isolation. Without this call the server
// is single-tenant and behaves exactly as it did before #48.
//
// The usage recorder's Start/Stop lifecycle is owned by main, mirroring how
// the rate limiter is wired.
func (s *Server) WithMultiTenancy(ts store.TenantStore, opts MultiTenantOptions) *Server {
	s.multiTenant = true
	s.tenants = ts
	// Responses become tenant-specific from here on; the cache helpers are
	// package-level functions and read this flag rather than the server.
	SetTenantScopedCaching(true)
	s.maxWatched = opts.MaxWatchedContracts
	s.streamScopeSync = opts.StreamScopeSync
	s.usage = NewUsageRecorder(ts, s.log, opts.UsageFlushInterval)
	return s
}

// Usage exposes the recorder so main can run its flush loop.
func (s *Server) Usage() *UsageRecorder { return s.usage }

// SetRateLimiter wires a per-client rate limiter into the router. Pass
// nil to leave the limiter disabled (the default — no behavior change).
// The limiter's Start/Stop lifecycle is owned by main, not by the Server.
func (s *Server) SetRateLimiter(l *RateLimiter) {
	s.limiter = l
}

// WithBroadcaster attaches the live event broadcaster so streaming endpoints
// can deliver events as they arrive.
func (s *Server) WithBroadcaster(b *broadcast.Broadcaster) *Server {
	s.bcast = b
	return s
}

// WithAPIKeyAuth turns on optional API key authentication (see
// internal/api/auth.go). When enabled, requests to write, streaming,
// subscription-management, and key-management endpoints must present a
// valid API key; read-only endpoints stay public. The flag is off by
// default, so deployments that don't set API_KEY_AUTH_ENABLED see no
// behavior change.
func (s *Server) WithAPIKeyAuth(enabled bool) *Server {
	s.apiKeyAuth = enabled
	return s
}

// SetExportMaxRange caps the ledger span a /contracts/{id}/export call
// may request. Zero means no cap (the handler still validates range fits
// the requested bound, but won't reject on span alone). The config layer
// exposes EXPORT_MAX_RANGE; pass it through so an operator can tune
// analytical workloads without code changes.
func (s *Server) SetExportMaxRange(n int64) { s.exportMaxRange = n }

// SetRetentionLedgers records the cold-start reach-back (RETENTION_LEDGERS)
// used to compute history_from_ledger when a new watched contract is added.
// Zero falls back to the ingester's default of 17280 (~24h at 5s/ledger).
func (s *Server) SetRetentionLedgers(n uint32) { s.retentionLedgers = n }

// retentionLedgers reports the configured cold-start reach-back in ledgers.
func (s *Server) retentionWindow() uint32 {
	if s.retentionLedgers > 0 {
		return s.retentionLedgers
	}
	return 17280
}

// SetCORSConfig wires the CORS middleware. The default (zero-valued
// config) is deny-all: no cross-origin browser request receives CORS
// headers, so the browser blocks the response. Pass the
// CORSAllowedOrigins / CORSAllowedMethods / CORSAllowedHeaders lists the
// operator wants; an empty AllowedOrigins is still deny-all (the
// middleware short-circuits).
func (s *Server) SetCORSConfig(cfg CORSConfig) { s.cors = cfg }

// Router returns the HTTP handler with all routes mounted.
func (s *Server) Router() http.Handler {
	var h http.Handler = s.router()
	// With a tracer attached, every request gets an otelhttp server span
	// (named by method, e.g. "GET") that parents the store spans emitted
	// by TracingStore, so a single trace covers HTTP → handler → SQL.
	if s.tracer != nil {
		h = otelhttp.NewHandler(h, "")
	}
	return h
}

// router builds the chi router with middleware and all routes. Returned
// as chi.Router (not http.Handler) so tests can walk the route tree with
// chi.Walk to verify spec coverage.
func (s *Server) router() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s.requestLogger)
	// Metrics are optional: a hand-built Server (tests, embedded use)
	// may carry no collector, in which case latency recording and the
	// /metrics endpoint are simply absent.
	if s.metrics != nil {
		r.Use(s.metrics.Middleware)
	}
	// CORS runs before Recoverer/Timeout so a preflight never blocks
	// nor panics inside the recovery middleware, and so the same-origin
	// contract (no Origin header) is forwarded as-is. Mounted
	// unconditionally so an operator can flip the config on without
	// restarts; CORS() is a no-op when the allowlist is empty.
	r.Use(CORS(s.cors))
	// Limit request body size to prevent resource exhaustion.
	// Applied after CORS so preflight requests (OPTIONS) pass through
	// without body size restrictions.
	if s.httpRequestBodyLimit > 0 {
		r.Use(s.bodyLimitMiddleware)
	}
	r.Use(s.recoverer.Middleware)
	r.Use(middleware.Timeout(30 * time.Second))
	if s.limiter != nil {
		r.Use(s.limiter.Middleware)
	}
	// authenticate must run before any handler that reads events: it is what
	// puts a Principal (and therefore a Scope) on the request context. In
	// single-tenant mode it injects a wildcard principal, so behavior is
	// unchanged; without it mounted, scopeFrom returns the zero Scope and
	// every scoped query silently matches nothing.
	r.Use(s.authenticate)

	// usageMiddleware must run after authenticate: it reads the Principal
	// that authenticate installs, and counts one request per tenant.
	r.Use(s.usageMiddleware)

	// prettyMiddleware must be the innermost wrapper (closest to the handler)
	// so the type assertion in writeJSON sees the prettyWriter interface.
	// It reads ?pretty=true from the query and wraps the ResponseWriter.
	r.Use(prettyMiddleware)

	// Non-list routes: health, metrics, writes — responses are always
	// small, so compression is just overhead with no benefit.
	r.Get("/health", s.handleHealth)
	// Registered as GET, not Handle: Handle advertises every method (the
	// route-drift test then demands CONNECT/TRACE entries in the OpenAPI
	// spec), and scraping is a GET.
	// Always registered so the documented surface is stable; a nil
	// collector (hand-built Server) answers 503 rather than 404.
	r.Get("/metrics", func(w http.ResponseWriter, req *http.Request) {
		if s.metrics == nil {
			writeError(w, http.StatusServiceUnavailable, errors.New("metrics not enabled"))
			return
		}
		s.metrics.Handler().ServeHTTP(w, req)
	})
	r.Get("/livez", s.handleLivez)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/version", s.handleVersion)
	// API documentation: embedded OpenAPI 3.1 spec + self-hosted
	// Swagger UI (no CDN). Handlers serve compiled-in assets.
	r.Get("/openapi.json", s.handleOpenAPI)
	r.Get("/docs", s.handleDocs)

	r.Get("/events", s.handleListEvents)
	r.Get("/events/count", s.handleCountEvents)
	r.Get("/events/aggregate", s.handleAggregateEvents)
	r.Get("/events/{id}/raw", s.handleGetEventRaw)
	r.Get("/events/{id}/transaction", s.handleGetEventTransaction)
	r.Get("/events/{id}", s.handleGetEvent)
	r.Get("/events.csv", s.handleEventsCSV)
	r.Get("/contracts", s.handleListContracts)
	r.Get("/contracts/{id}", s.handleGetContract)
	r.Get("/contracts/{id}/events", s.handleContractEvents)
	r.Get("/contracts/{id}/export", s.handleContractExport)
	r.Get("/contracts/{id}/stats", s.handleContractStats)
	r.Get("/stats", s.handleStats)

	// Admin bulk delete: auth-gated endpoint to delete events by ledger range.
	adminMW := apiKeyAuth(s.apiKey)
	r.With(adminMW).Delete("/events", s.handleDeleteEvents)
	// contributors: new read endpoints go here. Every one of them must
	// obtain its scope from filterFromQuery (list-shaped reads) or
	// scopeFrom (single-object reads) and pass it to the store — see
	// docs/multi-tenancy.md. Endpoints that skip this return nothing
	// rather than everything, but "returns nothing" is still a bug.
	// GraphQL transport — read-only, mounts at /graphql and dev-mode
	// /graphiql. Built by the graphql package; the API server only
	// owns the route registration so a misconfigured GraphQL handler
	// shows up as a 404 instead of a confusing 500.
	if s.graphqlHandler != nil {
		r.Handle("/graphql", s.graphqlHandler)
	}
	if s.graphqlPlayground != nil {
		r.Get("/graphiql", func(w http.ResponseWriter, req *http.Request) {
			s.graphqlPlayground.ServeHTTP(w, req)
		})
	}

	// Watched-contracts management: writes and updates to the runtime
	// filter list. Always auth-gated, even when AUTH_ENABLED would be
	// false elsewhere — that asymmetry is intentional and part of the
	// "writes are never open" contract.
	// Routes are absolute (no sub-router) so callers don't need a
	// trailing slash or chi's RedirectSlashes middleware.
	//
	// This is the operator's own list, not a tenant's: it is keyed on
	// API_KEY rather than a tenant credential, and it edits the global
	// watched_contracts table that forms one side of the ingestion union.
	// Tenants manage their own claims through /tenant/watch below.
	watchedMW := apiKeyAuth(s.apiKey)
	r.With(watchedMW).Post("/watched-contracts", s.handleAddWatchedChain)
	r.With(watchedMW).Delete("/watched-contracts/{id}", s.handleRemoveWatchedChain)

	// Contract spec overrides: user-supplied spec JSON per contract_id.
	// A spec override silently changes how that contract's events decode,
	// so — like every other management surface — writes are never open.
	r.With(watchedMW).Put("/contracts/{id}/spec", s.handlePutContractSpecOverride)
	r.With(watchedMW).Get("/contracts/{id}/spec", s.handleGetContractSpecOverride)
	r.With(watchedMW).Delete("/contracts/{id}/spec", s.handleDeleteContractSpecOverride)

	r.With(watchedMW).Get("/dead-letters", s.handleListDeadLetters)
	r.With(watchedMW).Delete("/dead-letters/{id}", s.handleDeleteDeadLetter)

	// API key management is ALWAYS authenticated: an unauthenticated
	// create endpoint would let anyone mint keys and walk around auth
	// entirely. The CLI (`sorotrail apikey`) is the bootstrap path for
	// the first key.
	r.Group(func(r chi.Router) {
		r.Use(s.requireAPIKey)
		r.Post("/apikeys", s.handleCreateAPIKey)
		r.Get("/apikeys", s.handleListAPIKeys)
		r.Delete("/apikeys/{id}", s.handleRevokeAPIKey)
	})

	// The streaming and subscription routes are the surface that auth
	// gates. Subscriptions carry webhook HMAC signing secrets, so the
	// whole subtree (reads included) is protected once auth is on —
	// leaking a subscription's secret would let an attacker forge
	// webhook payloads.
	protect := func(next http.Handler) http.Handler { return next }
	if s.apiKeyAuth {
		protect = s.requireAPIKey
	}
	r.Group(func(r chi.Router) {
		r.Use(protect)
		r.Get("/events/ws", s.handleEventStreamWS)
		r.Post("/subscriptions", s.handleCreateSubscription)
		r.Get("/subscriptions", s.handleListSubscriptions)
		r.Get("/subscriptions/{id}", s.handleGetSubscription)
		r.Put("/subscriptions/{id}", s.handleUpdateSubscription)
		r.Delete("/subscriptions/{id}", s.handleDeleteSubscription)
		r.Get("/subscriptions/{id}/deliveries", s.handleListDeliveries)
	})
	// Subscription CRUD and delivery history.
	r.Post("/subscriptions", s.handleCreateSubscription)
	r.Put("/subscriptions/{id}", s.handleUpdateSubscription)
	r.Delete("/subscriptions/{id}", s.handleDeleteSubscription)

	// List endpoints: responses can be large (many events, many
	// subscriptions), so compression is negotiated per request.
	// Inside a Group so the middleware only touches routes worth
	// compressing and a panic mid-body can't leave a truncated gzip
	// stream as the last thing the client sees (Recoverer is above).
	r.Group(func(r chi.Router) {
		if s.compressMinSize >= 0 {
			r.Use(Compress(s.compressMinSize))
		}
		r.Get("/events", s.handleListEvents)
		r.Get("/events/count", s.handleCountEvents)
		r.Get("/events/{id}/raw", s.handleGetEventRaw)
		r.Get("/events/{id}", s.handleGetEvent)
		r.Get("/contracts/{id}", s.handleGetContract)
		r.Get("/contracts/{id}/events", s.handleContractEvents)
		r.Get("/contracts/{id}/export", s.handleContractExport)
		r.Get("/events.csv", s.handleEventsCSV)
		r.Get("/subscriptions", s.handleListSubscriptions)
		r.Get("/subscriptions/{id}", s.handleGetSubscription)
		r.Get("/subscriptions/{id}/deliveries", s.handleListDeliveries)
		r.With(watchedMW).Get("/watched-contracts", s.handleListWatchedChains)
	})

	// Tenant self-service: who am I, what am I using, what am I watching.
	r.Group(func(r chi.Router) {
		r.Use(s.requireTenant)
		r.Get("/tenant", s.handleWhoAmI)
		r.Get("/tenant/usage", s.handleOwnUsage)
		r.Get("/tenant/watch", s.handleListOwnWatched)
		r.Post("/tenant/watch", s.handleAddOwnWatched)
		r.Delete("/tenant/watch/{contract_id}", s.handleRemoveOwnWatched)
	})

	// Tenant administration.
	r.Group(func(r chi.Router) {
		r.Use(s.requireAdmin)
		r.Post("/admin/tenants", s.handleCreateTenant)
		r.Get("/admin/tenants", s.handleListTenants)
		r.Get("/admin/tenants/{id}", s.handleGetTenant)
		r.Patch("/admin/tenants/{id}", s.handleUpdateTenant)
		r.Delete("/admin/tenants/{id}", s.handleDeleteTenant)
		r.Get("/admin/tenants/{id}/usage", s.handleTenantUsage)

		r.Get("/admin/tenants/{id}/grants", s.handleListTenantGrants)
		r.Post("/admin/tenants/{id}/grants", s.handleGrantContract)
		r.Delete("/admin/tenants/{id}/grants/{contract_id}", s.handleRevokeContract)

		r.Get("/admin/tenants/{id}/keys", s.handleListTenantKeys)
		r.Post("/admin/tenants/{id}/keys", s.handleCreateTenantKey)
		r.Delete("/admin/keys/{key_id}", s.handleRevokeTenantKey)
	})

	// Address activity index (#46).
	r.Get("/addresses/{address}/events", s.handleAddressEvents)
	r.Get("/addresses/{address}/summary", s.handleAddressSummary)

	return r
}

// handleOpenAPI serves the embedded OpenAPI 3.1 specification.
// The spec is a compiled-in static asset, so it is cacheable and
// immutable for an hour. In multi-tenant mode the shared helper marks
// it private — conservative but harmless: the document contains no
// tenant data, and losing CDN pooling on a docs route is a fair price
// for a single cache policy.
func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeCacheHeaders(w, cacheImmutable, time.Hour, "")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openapiSpec)
}

// handleDocs serves the Swagger UI page that renders /openapi.json.
// Same cacheability as the spec it renders: a compiled-in static asset.
func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	writeCacheHeaders(w, cacheImmutable, time.Hour, "")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUI))
}

// routePattern reports the chi route pattern for a request ("/events/{id}"),
// falling back to the raw path when the router has not matched yet. Spans are
// tagged with the pattern, not the path, so per-id URLs don't explode
// cardinality.
func (s *Server) routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if len(rctx.RoutePatterns) > 0 {
			return rctx.RoutePatterns[0]
		}
		if rctx.RoutePath != "" {
			return rctx.RoutePath
		}
	}
	return r.URL.Path
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		reqID := middleware.GetReqID(r.Context())
		log := s.log.With("request_id", reqID, "route", r.Method+" "+r.URL.Path)
		ctx := context.WithValue(r.Context(), loggerCtxKey, log)
		ww.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(ww, r.WithContext(ctx))
		if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetAttributes(
				attribute.String("http.route", s.routePattern(r)),
				attribute.String("request.id", reqID),
				attribute.Int("http.status_code", ww.Status()),
			)
		}
		log.Info("http request",
			"status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// bodyLimitMiddleware wraps the request body with http.MaxBytesReader to
// enforce a maximum request body size. This prevents resource exhaustion
// from clients sending excessively large request bodies. A limit <= 0 means
// no limit is enforced.
func (s *Server) bodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.httpRequestBodyLimit > 0 {
			r.Body = http.MaxBytesReader(w, r.Body, s.httpRequestBodyLimit)
		}
		next.ServeHTTP(w, r)
	})
}

func loggerFromContext(ctx context.Context) *slog.Logger {
	log, ok := ctx.Value(loggerCtxKey).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return log
}

// SetGraphQLHandler mounts the GraphQL transport. handler serves /graphql;
// playground, when non-nil, serves GraphiQL at /graphiql. Call before
// Router(); passing nil for either leaves that route unmounted.
func (s *Server) SetGraphQLHandler(handler, playground http.Handler) *Server {
	s.graphqlHandler = handler
	s.graphqlPlayground = playground
	return s
}
