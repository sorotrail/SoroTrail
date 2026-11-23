package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

func TestMetrics_ServesRequestDurationHistogram(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// Generate a sample on a known route before scraping.
	resp, _ := doGet(t, s, "/health")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	text := string(body)
	assert.Contains(t, text, "http_request_duration_seconds")
	assert.Contains(t, text, `route="/health"`)
	assert.Contains(t, text, `method="GET"`)
	assert.Contains(t, text, `status="200"`)
}

func TestMetrics_ExemptFromRateLimit(t *testing.T) {
	assert.True(t, exemptPaths["/metrics"], "/metrics must stay exempt from the rate limiter")
}

// TestMetrics_ExposesIngestionLagGauge asserts the /metrics endpoint serves
// the ingestion-lag gauge (#237): latest RPC ledger minus last ingested.
func TestMetrics_ExposesIngestionLagGauge(t *testing.T) {
	metrics.IngestionLag.Set(21)
	defer metrics.IngestionLag.Set(0)

	s := newTestServer(&stubStore{}, nil)
	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "sorotrail_ingestion_lag_ledgers 21")
}

func TestServer_routePattern(t *testing.T) {
	s := &Server{}

	tests := []struct {
		name     string
		setup    func(*http.Request)
		path     string
		expected string
	}{
		{
			name:     "no chi context",
			path:     "/unknown",
			expected: "/unknown",
		},
		{
			name: "matched route returns chi pattern",
			path: "/events/123",
			setup: func(r *http.Request) {
				rctx := chi.NewRouteContext()
				rctx.RoutePatterns = []string{"/events/{id}"}
				*r = *r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			},
			expected: "/events/{id}",
		},
		{
			name: "unmatched request falls back to raw path",
			path: "/unmatched",
			setup: func(r *http.Request) {
				rctx := chi.NewRouteContext()
				*r = *r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			},
			expected: "/unmatched",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, tt.path, nil)
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(req)
			}

			assert.Equal(t, tt.expected, s.routePattern(req))
		})
	}
}

func TestBodyLimitMiddleware(t *testing.T) {
	tests := []struct {
		name        string
		bodyLimit   int64
		requestBody string
		// When body exceeds limit and is read, the handler gets an error.
		// The existing handlers return 400 for oversized bodies.
		expectBodyReadError bool
	}{
		{
			name:                "body under limit is accepted",
			bodyLimit:           1024,
			requestBody:         `{"name": "test"}`,
			expectBodyReadError: false,
		},
		{
			name:                "body exactly at limit is accepted",
			bodyLimit:           1024,
			requestBody:         string(bytes.Repeat([]byte("x"), 1024)),
			expectBodyReadError: false,
		},
		{
			name:                "body over limit returns error when read",
			bodyLimit:           1024,
			requestBody:         string(bytes.Repeat([]byte("x"), 1025)),
			expectBodyReadError: true,
		},
		{
			name:                "empty body is accepted",
			bodyLimit:           1024,
			requestBody:         "",
			expectBodyReadError: false,
		},
		{
			name:                "zero limit means no limit (default behavior)",
			bodyLimit:           0,
			requestBody:         string(bytes.Repeat([]byte("x"), 10000)),
			expectBodyReadError: false,
		},
		{
			name:                "negative limit means no limit",
			bodyLimit:           -1,
			requestBody:         string(bytes.Repeat([]byte("x"), 10000)),
			expectBodyReadError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test server with the body limit middleware
			s := newTestServer(&stubStore{}, nil)
			s.SetHTTPRequestBodyLimit(tt.bodyLimit)

			// Use a simple handler that reads the body
			var readErr error
			testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, readErr = io.ReadAll(r.Body)
				if readErr == nil {
					w.WriteHeader(http.StatusOK)
				}
			})

			// Wrap with the body limit middleware
			handler := s.bodyLimitMiddleware(testHandler)

			// Create a POST request with the test body
			req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(tt.requestBody))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if tt.expectBodyReadError {
				// When body exceeds limit, reading returns a MaxBytesError
				require.Error(t, readErr)
				var maxBytesErr *http.MaxBytesError
				assert.ErrorAs(t, readErr, &maxBytesErr, "expected MaxBytesError when body exceeds limit")
			} else {
				assert.NoError(t, readErr)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

// TestRouteMux_RejectsDuplicateMethodAndPattern covers the guard directly: a
// second registration of the same method and pattern must be a hard failure,
// while the same pattern under another method is legitimate.
func TestRouteMux_RejectsDuplicateMethodAndPattern(t *testing.T) {
	r := newRouteMux(chi.NewRouter())
	noop := func(http.ResponseWriter, *http.Request) {}

	r.Get("/events", noop)
	assert.Panics(t, func() {
		r.Get("/events", noop)
	}, "a duplicate GET /events must not silently replace the first handler")

	assert.NotPanics(t, func() {
		r.Delete("/events", noop)
	}, "GET and DELETE on the same pattern are distinct routes")
}

// TestRouter_RegistersEachRouteOnce pins the final router surface and fails if
// a duplicate registration is reintroduced. routeMux panics while the router
// is being built because chi would have already replaced the earlier handler
// by the time chi.Walk could inspect the tree; walking afterwards also catches
// a route that was removed or renamed.
func TestRouter_RegistersEachRouteOnce(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	var router chi.Router
	require.NotPanics(t, func() { router = s.router() },
		"router construction must reject duplicate method/pattern registrations")

	seen := make(map[string]struct{})
	require.NoError(t, chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		key := method + " " + route
		if _, exists := seen[key]; exists {
			t.Errorf("route %s is registered more than once", key)
		}
		seen[key] = struct{}{}
		return nil
	}))
	assert.NotEmpty(t, seen)
}

// TestEffectiveDecompressedBodyLimit_HappyPath asserts that when
// SetMaxDecompressedBodySize is configured, EffectiveDecompressedBodyLimit
// returns that configured value regardless of the request.
func TestEffectiveDecompressedBodyLimit_HappyPath(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	s.SetMaxDecompressedBodySize(2048)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	got := s.EffectiveDecompressedBodyLimit(req)
	require.Equal(t, int64(2048), got)
}

// TestEffectiveDecompressedBodyLimit_FallsBackToRequestBodyLimit asserts that
// when maxDecompressedBodySize is not set, EffectiveDecompressedBodyLimit
// falls back to the effective request body limit.
func TestEffectiveDecompressedBodyLimit_FallsBackToRequestBodyLimit(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	// Do not set maxDecompressedBodySize; fall through to path 2

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	// Set a per-endpoint body limit so path 2 is exercised
	s.SetEndpointRequestBodyLimit("/events", 4096)

	got := s.EffectiveDecompressedBodyLimit(req)
	require.Equal(t, int64(4096), got)
}

// TestEffectiveDecompressedBodyLimit_DefaultFallback asserts that when neither
// maxDecompressedBodySize nor a request body limit is configured, the function
// returns DefaultMaxDecompressedBodyBytes.
func TestEffectiveDecompressedBodyLimit_DefaultFallback(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	// Ensure no custom limits are set

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	got := s.EffectiveDecompressedBodyLimit(req)
	require.Equal(t, DefaultMaxDecompressedBodyBytes, got)
}
