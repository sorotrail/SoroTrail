package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddleware_RecordsMatchedRoute(t *testing.T) {
	m := New()

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/events/abc123", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	body := scrapeBody(t, m)
	assert.Contains(t, body, `route="/events/{id}"`,
		"histogram must be labeled with the chi route pattern, not the raw path")
	assert.Contains(t, body, `method="GET"`)
	assert.Contains(t, body, `status="200"`)
	assert.NotContains(t, body, "abc123",
		"the raw path parameter value must never leak into a label (cardinality)")
}

func TestMiddleware_UnmatchedRouteLabel(t *testing.T) {
	m := New()

	r := chi.NewRouter()
	r.Use(m.Middleware)
	// At least one route must be registered, otherwise chi never builds
	// mx.handler and a 404 skips the middleware chain entirely (see
	// chi's Mux.ServeHTTP: "if mx.handler == nil" short-circuits before
	// middlewares run) — that's a degenerate case a real router with
	// registered routes never hits.
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)

	body := scrapeBody(t, m)
	assert.Contains(t, body, `route="unmatched"`)
	assert.Contains(t, body, `status="404"`)
}

func TestMiddleware_DistinctRoutesRecordedSeparately(t *testing.T) {
	m := New()

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/events", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	for _, path := range []string{"/health", "/events", "/events"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
	}

	body := scrapeBody(t, m)
	assert.Contains(t, body, `http_request_duration_seconds_count{method="GET",route="/events",status="200"} 2`)
	assert.Contains(t, body, `http_request_duration_seconds_count{method="GET",route="/health",status="200"} 1`)
}

func TestHandler_ServesPrometheusExposition(t *testing.T) {
	m := New()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
}

func scrapeBody(t *testing.T, m *HTTPMetrics) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	return strings.ReplaceAll(w.Body.String(), "\n\n", "\n")
}
