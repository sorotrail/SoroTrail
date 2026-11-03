package api

import (
	"math"
	"net/http"
	"strconv"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	defaultDegradedRetryAfterSeconds = 5
	degradedErrorMessage             = "database unavailable: service operating in degraded mode"
)

// WithStoreBreaker attaches a database circuit breaker to the Server. When the
// breaker is open (or the underlying store reports Degraded() == true),
// database-backed endpoints fast-fail with 503 Service Unavailable and a
// Retry-After header without issuing queries against the unavailable database.
func (s *Server) WithStoreBreaker(b *store.StoreBreaker) *Server {
	s.breaker = b
	return s
}

// StoreBreaker returns the database circuit breaker attached to the Server, or
// the one carried by the underlying store if none was set explicitly.
func (s *Server) StoreBreaker() *store.StoreBreaker {
	if s.breaker != nil {
		return s.breaker
	}
	if s.store != nil {
		if bp, ok := s.store.(interface{ Breaker() *store.StoreBreaker }); ok {
			return bp.Breaker()
		}
	}
	return nil
}

// isDegraded reports whether the server's store is currently known to be
// unavailable via either the attached StoreBreaker or a DegradationReporter on
// the store itself.
func (s *Server) isDegraded() bool {
	if b := s.StoreBreaker(); b != nil && b.Degraded() {
		return true
	}
	if s.store != nil {
		if rep, ok := s.store.(store.DegradationReporter); ok && rep.Degraded() {
			return true
		}
	}
	return false
}

// isDegradedExemptPath reports whether a request path is served without
// querying the database (or is a health/readiness probe that inspects and
// reports degraded state in its own structured envelope).
func isDegradedExemptPath(path string) bool {
	switch path {
	case "/livez", "/readyz", "/health", "/version", "/metrics", "/openapi.json", "/docs":
		return true
	default:
		return false
	}
}

func degradedRetryAfterHeader(b *store.StoreBreaker) string {
	seconds := defaultDegradedRetryAfterSeconds
	if b != nil {
		if rem := int(math.Ceil(b.CoolDownRemaining().Seconds())); rem > seconds {
			seconds = rem
		}
	}
	return strconv.Itoa(seconds)
}

func writeDegradedUnavailable(w http.ResponseWriter, b *store.StoreBreaker) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", degradedRetryAfterHeader(b))
	writeJSON(w, http.StatusServiceUnavailable, errorResponse{
		Error: degradedErrorMessage,
		Code:  CodeServiceUnavailable,
	})
}

// degradedMiddleware short-circuits database-backed endpoints with 503 Service
// Unavailable and a Retry-After header while the database is unavailable,
// keeping liveness (/livez), readiness (/readyz), health (/health), version
// (/version), metrics (/metrics), and documentation (/openapi.json, /docs)
// responsive.
func (s *Server) degradedMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isDegradedExemptPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if s.isDegraded() {
			writeDegradedUnavailable(w, s.StoreBreaker())
			return
		}
		next.ServeHTTP(w, r)
	})
}
