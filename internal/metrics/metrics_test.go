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

// TestIngestionLagGauge_ExportedAtMetrics verifies the ingestion-lag gauge
// (#237) is registered on the global registry and exposed at /metrics with
// the exact chain-head-minus-last-ingested value.
func TestIngestionLagGauge_ExportedAtMetrics(t *testing.T) {
	tests := []struct {
		name         string
		chainHead    int64
		lastIngested int64
		want         string
	}{
		{name: "caught up", chainHead: 100, lastIngested: 100, want: "sorotrail_ingestion_lag_ledgers 0"},
		{name: "five ledgers behind", chainHead: 100, lastIngested: 95, want: "sorotrail_ingestion_lag_ledgers 5"},
		{name: "deep lag", chainHead: 1_000_000, lastIngested: 123_456, want: "sorotrail_ingestion_lag_ledgers 876544"},
		{name: "nothing ingested yet", chainHead: 100, lastIngested: 0, want: "sorotrail_ingestion_lag_ledgers 100"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			IngestionLag.Set(float64(tt.chainHead - tt.lastIngested))
			defer IngestionLag.Set(0)

			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			w := httptest.NewRecorder()
			Handler().ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)
			assert.Contains(t, w.Body.String(), tt.want)
		})
	}
}

func scrapeBody(t *testing.T, m *HTTPMetrics) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	return strings.ReplaceAll(w.Body.String(), "\n\n", "\n")
}

// TestBatchMetrics_ExportedAtMetrics verifies the batch/backpressure
// instrumentation is registered on the global registry and served at
// /metrics, so operators can observe how the ingester is sizing its writes
// and whether it is throttling a strapped database. Unobserved families
// still render with a zero value, so no mutation is needed and the test is
// safe to run against the shared global registry regardless of order.
func TestBatchMetrics_ExportedAtMetrics(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	assert.Contains(t, body, "sorotrail_event_batch_writes_total",
		"write-count counter must be exported")
	assert.Contains(t, body, "sorotrail_event_batch_size",
		"batch-size gauge must be exported")
	assert.Contains(t, body, "sorotrail_event_backpressure_total",
		"backpressure counter must be exported")
	assert.Contains(t, body, "sorotrail_event_backpressure_seconds_total",
		"backpressure-seconds counter must be exported")
}
