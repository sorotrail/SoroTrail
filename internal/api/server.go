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
	if len(enricher) > 0 {
		s.enricher = enricher[0]
	}
	return s
}

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

// Router returns the HTTP handler with all routes mounted.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(s.requestLogger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
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

	// contributors: new read endpoints go here. Anything that writes (e.g.
	// managing watched contracts at runtime) should come with auth first.

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
