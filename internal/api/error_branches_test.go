package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestAPIErrorBranches_Table(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		setupStore     func() *stubStore
	}{
		// 400 branches
		{
			name:           "400: invalid limit param",
			method:         http.MethodGet,
			path:           "/events?limit=-1",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "400: invalid cursor format",
			method:         http.MethodGet,
			path:           "/events?cursor=bad!",
			expectedStatus: http.StatusBadRequest,
		},
		// 404 branches
		{
			name:           "404: unknown event ID",
			method:         http.MethodGet,
			path:           "/events/nonexistent",
			expectedStatus: http.StatusNotFound,
		},
		// 500 branches
		{
			name:           "500: store error on events query",
			method:         http.MethodGet,
			path:           "/events",
			expectedStatus: http.StatusInternalServerError,
			setupStore: func() *stubStore {
				return &stubStore{queryErr: errors.New("db timeout")}
			},
		},
		{
			name:           "500: store error on contract summary",
			method:         http.MethodGet,
			path:           "/contracts/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			expectedStatus: http.StatusInternalServerError,
			setupStore: func() *stubStore {
				return &stubStore{contractSummaryErr: errors.New("db timeout")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var st *stubStore
			if tt.setupStore != nil {
				st = tt.setupStore()
			} else {
				st = &stubStore{}
				st.eventByID = map[string]store.Event{} // Ensure 404 instead of nil panic
			}

			server := newTestServerWithKey(st, nil, "test-key")

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			server.Router().ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			// Verify shared error envelope
			var env map[string]interface{}
			err := json.Unmarshal(w.Body.Bytes(), &env)
			require.NoError(t, err, "Response must be valid JSON")

			_, hasError := env["error"]
			assert.True(t, hasError, "Error response must contain 'error' key")
		})
	}
}

func TestAPIErrorBranches_RateLimiter_Table(t *testing.T) {
	limiter := NewRateLimiter(1, 1, false)

	// Create a minimal handler to test the middleware
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request succeeds
	req1, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req1.RemoteAddr = "1.2.3.4:1234"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)
	require.Equal(t, http.StatusOK, w1.Code)

	// Second request fails with 429
	req2, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req2.RemoteAddr = "1.2.3.4:5678" // same IP, different port
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusTooManyRequests, w2.Code)
	assert.NotEmpty(t, w2.Header().Get("Retry-After"), "429 must include Retry-After header")
}
