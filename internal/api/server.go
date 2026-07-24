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

// Router returns the HTTP handler with all routes mounted.
//
// Middleware layering: requestLogger → Recoverer → Timeout → (Limiter) on
// the top-level router, then middleware.Compress on the JSON group only.
// Streaming endpoints (/events/ws) are mounted directly on the parent
// router so they pick up the deadline / limiter protection WITHOUT being
// wrapped by Compress — Compress buffers responses and would mangle the
// WebSocket upgrade (its writer does not implement http.Hijacker in
// every code path). SSE endpoints, when/if #3 lands, must likewise be
// registered outside the Compress group.
//
// Compress is mounted on the JSON group (not the top-level router) for
// the opposite reason on the error path: middleware.Timeout writes its
// 503 BEFORE Compress sees the response, so a stalled handler's 503 is
// served uncompressed. That path is rare (no in-flight request runs
// that long) and bandwidth on 503s is not the issue this middleware is
// solving; the pagination-history wins worth costlier work to keep
// are the 200 responses that the group covers.
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

	// Streaming endpoints — exempt from compression. Compress buffers
	// responses, which is incompatible with connection-hijacking
	// protocols (WebSocket today, SSE if/when #3 lands). Register the
	// route here so it picks up Timeout + Limiter but NOT Compress.
	r.Get("/events/ws", s.handleEventStreamWS)

	// JSON endpoints — compressed. middleware.Compress(5) negotiates
	// gzip with the client and sets Content-Encoding/Vary accordingly;
	// requests without Accept-Encoding: gzip get an identity response
	// unchanged. The default min-size threshold (~512 B) means tiny
	// error envelopes don't pay the compress cost — they fly through
	// as identity.
	//
	// Contributors: when you add a new route, add it inside this
	// group unless it is a streaming protocol that hijacks the
	// connection, in which case register it on `r` directly next to
	// /events/ws.
	r.Group(func(r chi.Router) {
		r.Use(middleware.Compress(5))

		r.Get("/health", s.handleHealth)
		r.Get("/events", s.handleListEvents)
		r.Get("/events/{id}", s.handleGetEvent)
		r.Get("/contracts/{id}/events", s.handleContractEvents)
		r.Get("/stats", s.handleStats)

		// Subscription CRUD and delivery history.
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
