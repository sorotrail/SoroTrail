package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/config"
)

func newTestServerWithConfig(st *stubStore, cfg config.Config) *Server {
	return New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
}

func doRequest(t *testing.T, s *Server, method, path string, origin string) *http.Response {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, nil)
	require.NoError(t, err)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if method == http.MethodOptions {
		req.Header.Set("Access-Control-Request-Method", "GET")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestCORS_UnsetConfig_NoHeaders(t *testing.T) {
	st := &stubStore{}
	s := newTestServerWithConfig(st, config.Config{})
	resp := doRequest(t, s, http.MethodGet, "/events", "https://example.com")
	defer resp.Body.Close()
	// No Access-Control-Allow-Origin header present
	require.Equal(t, "", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestCORS_AllowedOrigin_GetHasHeader(t *testing.T) {
	st := &stubStore{}
	cfg := config.Config{CORSAllowedOrigins: []string{"https://app.example.com"}}
	s := newTestServerWithConfig(st, cfg)
	resp := doRequest(t, s, http.MethodGet, "/events", "https://app.example.com")
	defer resp.Body.Close()
	require.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestCORS_DisallowedOrigin_NoHeader(t *testing.T) {
	st := &stubStore{}
	cfg := config.Config{CORSAllowedOrigins: []string{"https://app.example.com"}}
	s := newTestServerWithConfig(st, cfg)
	resp := doRequest(t, s, http.MethodGet, "/events", "https://evil.example.com")
	defer resp.Body.Close()
	require.Equal(t, "", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestCORS_Preflight_Works(t *testing.T) {
	st := &stubStore{}
	cfg := config.Config{CORSAllowedOrigins: []string{"https://app.example.com"}}
	s := newTestServerWithConfig(st, cfg)
	resp := doRequest(t, s, http.MethodOptions, "/events", "https://app.example.com")
	defer resp.Body.Close()
	// Should include allow origin and methods
	require.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	allowMethods := resp.Header.Get("Access-Control-Allow-Methods")
	require.True(t, strings.Contains(allowMethods, "GET") || strings.Contains(allowMethods, "GET"))
}
