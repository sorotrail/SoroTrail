package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
)

func newCORSTestServer(origins []string) *Server {
	st := &stubStore{}
	rc := &stubRPC{health: rpc.Health{Status: "healthy"}}
	s := New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetCORSOrigins(origins)
	return s
}

func TestCORS_AllowedOriginGetsHeaders(t *testing.T) {
	s := newCORSTestServer([]string{"https://app.example.com", "https://staging.example.com"})
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("GET", srv.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://app.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, resp.Header.Get("Vary"), "Origin")
}

func TestCORS_DisallowedOriginNoHeaders(t *testing.T) {
	s := newCORSTestServer([]string{"https://app.example.com"})
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("GET", srv.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://evil.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"),
		"disallowed origin must not receive CORS headers")
}

func TestCORS_PreflightOptionsHandled(t *testing.T) {
	s := newCORSTestServer([]string{"https://app.example.com"})
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("OPTIONS", srv.URL+"/events", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET")
}

func TestCORS_WildcardAllowsAnyOrigin(t *testing.T) {
	s := newCORSTestServer([]string{"*"})
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("GET", srv.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://anything.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestCORS_UnsetConfigEmitsNothing(t *testing.T) {
	// No origins = no CORS middleware mounted.
	s := newCORSTestServer(nil)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("GET", srv.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://app.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"),
		"no CORS middleware means no CORS headers")
}

func TestCORS_ExposesXRequestIDHeader(t *testing.T) {
	s := newCORSTestServer([]string{"https://app.example.com"})
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	req, err := http.NewRequest("GET", srv.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://app.example.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Contains(t, resp.Header.Get("Access-Control-Expose-Headers"), "X-Request-Id")
}
