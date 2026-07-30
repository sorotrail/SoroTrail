// Package metrics exposes Prometheus metrics for the HTTP API, notably a
// per-route request-duration histogram served at GET /metrics.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// unmatchedRoute labels requests that never reached a registered chi route
// (404s from a totally unknown path). Falling back to r.URL.Path there
// would let clients probing random paths grow the histogram's cardinality
// without bound.
const unmatchedRoute = "unmatched"

// HTTPMetrics holds the HTTP request-duration histogram and the registry
// it's registered against. Each Server owns its own instance (rather than
// using the global Prometheus registry) so tests can construct one per
// case without collectors colliding across parallel tests.
type HTTPMetrics struct {
	registry *prometheus.Registry
	duration *prometheus.HistogramVec
}

// New creates an HTTPMetrics with a fresh registry and registers the
// request-duration histogram against it.
func New() *HTTPMetrics {
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds, labeled by route, method, and status code.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"route", "method", "status"},
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(duration)
	return &HTTPMetrics{registry: reg, duration: duration}
}

// Middleware records each request's duration in the http_request_duration_seconds
// histogram, labeled with the matched chi route pattern (e.g. "/events/{id}")
// rather than the raw URL path, so cardinality stays bounded regardless of
// how many distinct path parameter values are seen. It must be mounted via
// r.Use so it wraps chi's route matching: the route pattern is only
// resolved in the request context once next.ServeHTTP returns.
func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = unmatchedRoute
		}
		m.duration.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).
			Observe(time.Since(start).Seconds())
	})
}

// Handler serves the registered metrics in the Prometheus exposition format.
func (m *HTTPMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}
