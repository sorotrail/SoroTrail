// Package api serves stored events over HTTP. Endpoints are documented in
// the README's API reference.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/khaylebfortune/sorotrail/internal/audit"
	"github.com/khaylebfortune/sorotrail/internal/broadcast"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// SetAuditor registers the binary's Auditor so /stats can surface its
// Metrics counters. There is exactly one Auditor per process; its
// lifetime is the lifetime of main(). SetAuditor must be called BEFORE
// ListenAndServe so the first /stats request observes a stable value.
// The setter is guarded by a RWMutex so concurrent reader goroutines in
// /stats handlers can never observe a torn pointer.
//
// When AUDIT_ENABLED=false the function is never called and /stats
// returns Stats with the embedded AuditStats struct zero-valued (and
// omitted from JSON, courtesy of its `omitempty` tag).
var (
	auditorMu sync.RWMutex
	auditor   *audit.Auditor
)

func SetAuditor(a *audit.Auditor) {
	auditorMu.Lock()
	auditor = a
	auditorMu.Unlock()
}

func getAuditor() *audit.Auditor {
	auditorMu.RLock()
	defer auditorMu.RUnlock()
	return auditor
}

// Enricher is the spec-based event enrichment interface used by the API.
// Defined here so the API package doesn't import internal/spec directly.
type Enricher interface {
	EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent
}

// Server holds the API's dependencies.
type Server struct {
	store    store.Store
	rpc      rpc.Client
	enricher Enricher
	log      *slog.Logger
	limiter  *RateLimiter
	bcast    *broadcast.Broadcaster

	// Multi-tenancy (#48). multiTenant is false by default, which makes
	// every request run as an untenanted wildcard principal — the exact
	// pre-#48 behavior. tenants is nil in that mode and must not be
	// dereferenced outside the multiTenant branches.
	multiTenant     bool
	tenants         store.TenantStore
	usage           *UsageRecorder
	maxWatched      int
	streamScopeSync time.Duration
}

// New builds the API server. rpcClient is only used by /health.
// enricher is optional — pass nil to disable spec decoding.
func New(st store.Store, rpcClient rpc.Client, log *slog.Logger, enricher ...Enricher) *Server {
	s := &Server{store: st, rpc: rpcClient, log: log}
	if len(enricher) > 0 {
		s.enricher = enricher[0]
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
// (SSE, WebSocket) can deliver events as they arrive.
func (s *Server) WithBroadcaster(b *broadcast.Broadcaster) *Server {
	s.bcast = b
	return s
}

// Router returns the HTTP handler with all routes mounted.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(s.requestLogger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// Authentication runs before the limiter so the limiter can key on the
	// resolved tenant rather than on a source IP that several tenants may
	// share (and that one tenant may spread across many of). It is
	// installed unconditionally: in single-tenant mode it injects the
	// wildcard principal, which is what guarantees every handler
	// downstream can obtain a scope instead of silently denying.
	r.Use(s.authenticate)
	if s.multiTenant {
		r.Use(s.usageMiddleware)
	}
	if s.limiter != nil {
		// Limiter sits inside Timeout and Recoverer so its instant 429
		// response always makes it back through the deadline cleanly, and
		// a panic inside the limiter can't take down the server.
		r.Use(s.limiter.Middleware)
	}

	r.Get("/health", s.handleHealth)
	r.Get("/events", s.handleListEvents)
	r.Get("/events/{id}", s.handleGetEvent)
	r.Get("/contracts/{id}/events", s.handleContractEvents)
	r.Get("/stats", s.handleStats)
	r.Get("/events/ws", s.handleEventStreamWS)

	// contributors: new read endpoints go here. Every one of them must
	// obtain its scope from filterFromQuery (list-shaped reads) or
	// scopeFrom (single-object reads) and pass it to the store — see
	// docs/multi-tenancy.md. Endpoints that skip this return nothing
	// rather than everything, but "returns nothing" is still a bug.

	// Subscription CRUD and delivery history.
	r.Post("/subscriptions", s.handleCreateSubscription)
	r.Get("/subscriptions", s.handleListSubscriptions)
	r.Get("/subscriptions/{id}", s.handleGetSubscription)
	r.Put("/subscriptions/{id}", s.handleUpdateSubscription)
	r.Delete("/subscriptions/{id}", s.handleDeleteSubscription)
	r.Get("/subscriptions/{id}/deliveries", s.handleListDeliveries)

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

	return r
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		s.log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"duration", time.Since(start),
			"remote", r.RemoteAddr,
		)
	})
}
