// Package api serves stored events over HTTP. Endpoints are documented in
// the README's API reference.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/khaylebfortune/sorotrail/internal/audit"
	"github.com/khaylebfortune/sorotrail/internal/metrics"
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

// Server holds the API's dependencies.
type Server struct {
	store   store.Store
	rpc     rpc.Client
	log     *slog.Logger
	metrics *metrics.Metrics
	broker  *Broker
}

// New builds the API server. rpcClient is only used by /health.
func New(st store.Store, rpcClient rpc.Client, log *slog.Logger, m *metrics.Metrics, b *Broker) *Server {
	return &Server{store: st, rpc: rpcClient, log: log, metrics: m, broker: b}
}

// Router returns the HTTP handler with all routes mounted.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(s.requestLogger)
	r.Use(middleware.Recoverer)

	// Long-lived SSE connection — no timeout, registered before /events/{id}
	// so the literal "subscribe" path wins over the {id} parameter.
	r.Get("/events/subscribe", s.handleSubscribe)

	r.Group(func(r chi.Router) {
		r.Use(middleware.Timeout(30 * time.Second))
		r.Get("/health", s.handleHealth)
		r.Get("/events", s.handleListEvents)
		r.Get("/events/{id}", s.handleGetEvent)
		r.Get("/contracts/{id}/events", s.handleContractEvents)
		r.Get("/stats", s.handleStats)
		r.Get("/metrics", s.handleMetrics)
	})

	// contributors: new read endpoints go here. Anything that writes (e.g.
	// managing watched contracts at runtime) should come with auth first.
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
		if s.metrics != nil {
			path := chi.RouteContext(r.Context()).RoutePattern()
			if path == "" {
				path = r.URL.Path
			}
			s.metrics.RecordHTTPRequest(path, ww.Status(), time.Since(start).Seconds())
		}
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metrics.Handler().ServeHTTP(w, r)
}

// handleSubscribe streams newly ingested events to the client via
// Server-Sent Events (text/event-stream). Same filter semantics as GET
// /events: contract_id, type, topic, from_ledger, to_ledger.
//
// Delivery is best-effort: events are pushed after the store upsert
// succeeds, but a reconnecting client should catch up via the REST cursor.
// The per-subscriber buffer is bounded; when it overflows the connection
// is closed with an error event so the client knows to reconnect.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	ch, cancel := s.broker.Subscribe(filter)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.log.Error("streaming not supported")
		return
	}

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				// Buffer overflow: the broker closed the channel.
				errData, _ := json.Marshal(map[string]string{"message": "buffer overflow, please reconnect"})
				fmt.Fprintf(w, "event: error\ndata: %s\n\n", errData)
				flusher.Flush()
				return
			}
			fmt.Fprint(w, "event: event\ndata: ")
			_ = enc.Encode(event)
			fmt.Fprint(w, "\n")
			flusher.Flush()
		}
	}
}
