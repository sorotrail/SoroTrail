// Package api serves stored events over HTTP. Endpoints are documented in
// the OpenAPI spec and the README.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type ctxKey string

const loggerCtxKey ctxKey = "logger"

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

// Enricher is the spec-based event enrichment interface used by the API.
// Defined here so the API package doesn't import internal/spec directly.
type Enricher interface {
	EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent
}

// Server holds the API's dependencies.
type Server struct {
	store     store.Store
	rpc       rpc.Client
	enricher  Enricher
	log       *slog.Logger
	apiKey    string
	limiter   *RateLimiter
	recoverer *Recoverer
	bcast     *broadcast.Broadcaster
	// compressMinSize is the body size at which responses start being
	// compressed. The zero value means CompressMinSize, so compression is on
	// by default; negative disables the middleware entirely.
	compressMinSize int
}

// SetCompressMinSize overrides the body size at which responses are
// compressed. Pass a negative value to disable compression.
func (s *Server) SetCompressMinSize(n int) {
	s.compressMinSize = n
}

// New builds the API server. rpcClient is only used by /health.
// apiKey gates the watched-contracts management endpoints; pass "" to
// fail closed (every request gets a 503 with "API_KEY not configured").
// See apiKeyAuth for the exact contract. The trailing enricher is optional —
// pass nil to disable spec decoding, or one Enricher to enable it.
func New(st store.Store, rpcClient rpc.Client, log *slog.Logger, apiKey string, enricher ...Enricher) *Server {
	s := &Server{store: st, rpc: rpcClient, log: log, apiKey: apiKey, recoverer: NewRecoverer(log)}
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
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s.requestLogger)
	r.Use(s.recoverer.Middleware)
	r.Use(middleware.Timeout(30 * time.Second))
	// Compression sits outside the limiter so a 429 is written through the
	// same encoding path as any other small response (i.e. sent as-is), and
	// inside Recoverer so a panic mid-body can't leave a truncated gzip
	// stream as the last thing the client sees.
	if s.compressMinSize >= 0 {
		r.Use(Compress(s.compressMinSize))
	}
	if s.limiter != nil {
		// Limiter sits inside Timeout and Recoverer so its instant 429
		// response always makes it back through the deadline cleanly, and
		// a panic inside the limiter can't take down the server.
		r.Use(s.limiter.Middleware)
	}

	r.Get("/health", s.handleHealth)
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/version", s.handleVersion)
	r.Get("/events", s.handleListEvents)
	r.Get("/events/{id}", s.handleGetEvent)
	r.Get("/contracts/{id}/events", s.handleContractEvents)
	r.Get("/stats", s.handleStats)
	r.Get("/events/ws", s.handleEventStreamWS)

	// Watched-contracts management: writes and updates to the runtime
	// filter list. Always auth-gated, even when AUTH_ENABLED would be
	// false elsewhere — that asymmetry is intentional and part of the
	// "writes are never open" contract. GET is gated too so an operator
	// with the key can confirm the current list without touching /stats.
	// Routes are absolute (no sub-router) so callers don't need a
	// trailing slash or chi's RedirectSlashes middleware.
	watchedMW := apiKeyAuth(s.apiKey)
	r.With(watchedMW).Get("/watched-contracts", s.handleListWatchedChains)
	r.With(watchedMW).Post("/watched-contracts", s.handleAddWatchedChain)
	r.With(watchedMW).Delete("/watched-contracts/{id}", s.handleRemoveWatchedChain)

	// Subscription CRUD and delivery history.
	r.Post("/subscriptions", s.handleCreateSubscription)
	r.Get("/subscriptions", s.handleListSubscriptions)
	r.Get("/subscriptions/{id}", s.handleGetSubscription)
	r.Put("/subscriptions/{id}", s.handleUpdateSubscription)
	r.Delete("/subscriptions/{id}", s.handleDeleteSubscription)
	r.Get("/subscriptions/{id}/deliveries", s.handleListDeliveries)

	return r
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
		log.Info("http request",
			"status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func loggerFromContext(ctx context.Context) *slog.Logger {
	log, ok := ctx.Value(loggerCtxKey).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return log
}
