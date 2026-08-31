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
