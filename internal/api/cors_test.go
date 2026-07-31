package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doCORSRequest runs a single request against the server's router with an
// optional Origin header and, for preflights, Access-Control-Request-Method
// and Access-Control-Request-Headers.
func doCORSRequest(t *testing.T, s *Server, method, path, origin, reqMethod, reqHeaders string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if reqMethod != "" {
		req.Header.Set("Access-Control-Request-Method", reqMethod)
	}
	if reqHeaders != "" {
		req.Header.Set("Access-Control-Request-Headers", reqHeaders)
	}
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)
	return rec
}

func newCORSserver(origins []string) *Server {
	s := newTestServer(&stubStore{}, nil)
	s.SetCORS(origins)
	return s
}

func TestCORS_SimpleRequests(t *testing.T) {
	tests := []struct {
		name           string
		origins        []string
		requestOrigin  string
		wantAllow      string // "" = header must be absent
		wantVaryOrigin bool
	}{
		{
			name:          "disabled by default emits no CORS headers",
			origins:       nil,
			requestOrigin: "https://app.example.com",
			wantAllow:     "",
		},
		{
			name:           "matching origin is echoed back",
			origins:        []string{"https://app.example.com"},
			requestOrigin:  "https://app.example.com",
			wantAllow:      "https://app.example.com",
			wantVaryOrigin: true,
		},
		{
			name:          "unknown origin gets no CORS headers",
			origins:       []string{"https://app.example.com"},
			requestOrigin: "https://evil.example.com",
			wantAllow:     "",
		},
		{
			name:          "wildcard allows any origin",
			origins:       []string{"*"},
			requestOrigin: "https://anything.example.com",
			wantAllow:     "*",
		},
		{
			name:           "same-origin request from allowed list is echoed",
			origins:        []string{"http://localhost:5173"},
			requestOrigin:  "http://localhost:5173",
			wantAllow:      "http://localhost:5173",
			wantVaryOrigin: true,
		},
		{
			name:          "no Origin header means no CORS headers",
			origins:       []string{"*"},
			requestOrigin: "",
			wantAllow:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doCORSRequest(t, newCORSserver(tt.origins), http.MethodGet, "/health", tt.requestOrigin, "", "")
			assert.Equal(t, http.StatusOK, rec.Code)
			got := rec.Header().Get("Access-Control-Allow-Origin")
			if tt.wantAllow == "" {
				assert.Empty(t, got, "no CORS header expected")
			} else {
				assert.Equal(t, tt.wantAllow, got)
			}
			gotVary := strings.Join(rec.Header().Values("Vary"), ",")
			if tt.wantVaryOrigin {
				assert.Contains(t, gotVary, "Origin")
			} else {
				assert.NotContains(t, gotVary, "Origin")
			}
		})
	}
}

func TestCORS_Preflight(t *testing.T) {
	tests := []struct {
		name           string
		origins        []string
		requestOrigin  string
		requestMethod  string
		requestHeaders string
		wantStatus     int // 0 = assert fall-through (not 204, no allow header)
		wantAllow      string
		wantAllowMeths string
		wantAllowHdrs  string
	}{
		{
			name:           "allowed origin short-circuits with 204",
			origins:        []string{"https://app.example.com"},
			requestOrigin:  "https://app.example.com",
			requestMethod:  "POST",
			wantStatus:     http.StatusNoContent,
			wantAllow:      "https://app.example.com",
			wantAllowMeths: "GET, POST, PUT, DELETE, OPTIONS",
			wantAllowHdrs:  "Content-Type, X-API-Key, X-Request-ID",
		},
		{
			name:           "requested headers are echoed",
			origins:        []string{"https://app.example.com"},
			requestOrigin:  "https://app.example.com",
			requestMethod:  "DELETE",
			requestHeaders: "X-API-Key, X-Custom",
			wantStatus:     http.StatusNoContent,
			wantAllow:      "https://app.example.com",
			wantAllowMeths: "GET, POST, PUT, DELETE, OPTIONS",
			wantAllowHdrs:  "X-API-Key, X-Custom",
		},
		{
			name:           "wildcard preflight allows any origin",
			origins:        []string{"*"},
			requestOrigin:  "https://anything.example.com",
			requestMethod:  "GET",
			wantStatus:     http.StatusNoContent,
			wantAllow:      "*",
			wantAllowMeths: "GET, POST, PUT, DELETE, OPTIONS",
		},
		{
			name:          "disallowed origin falls through to routing",
			origins:       []string{"https://app.example.com"},
			requestOrigin: "https://evil.example.com",
			requestMethod: "POST",
			wantStatus:    0,
			wantAllow:     "",
		},
		{
			name:          "cors disabled falls through to routing",
			origins:       nil,
			requestOrigin: "https://app.example.com",
			requestMethod: "GET",
			wantStatus:    0,
			wantAllow:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doCORSRequest(t, newCORSserver(tt.origins), http.MethodOptions, "/health", tt.requestOrigin, tt.requestMethod, tt.requestHeaders)
			if tt.wantStatus == 0 {
				assert.NotEqual(t, http.StatusNoContent, rec.Code, "unconfigured/denied preflight must not short-circuit")
				assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
				return
			}
			require.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantAllow, rec.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tt.wantAllowMeths, rec.Header().Get("Access-Control-Allow-Methods"))
			if tt.wantAllowHdrs != "" {
				assert.Equal(t, tt.wantAllowHdrs, rec.Header().Get("Access-Control-Allow-Headers"))
			}
			assert.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
		})
	}
}

func TestCORS_HeadersPresentOnErrorResponses(t *testing.T) {
	// CORS headers must survive responses written by later middleware/handlers,
	// including 4xx/5xx paths (404 here).
	rec := doCORSRequest(t, newCORSserver([]string{"https://app.example.com"}), http.MethodGet, "/does-not-exist", "https://app.example.com", "", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "https://app.example.com", rec.Header().Get("Access-Control-Allow-Origin"))
}
