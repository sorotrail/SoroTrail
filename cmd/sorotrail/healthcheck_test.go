package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheckReturnsZeroOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("expected /health, got %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/health",
	}); got != 0 {
		t.Fatalf("expected exit code 0 for 200 response, got %d", got)
	}
}

func TestHealthcheckReturnsOneOn503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"degraded"}`))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/health",
	}); got != 1 {
		t.Fatalf("expected exit code 1 for 503 response, got %d", got)
	}
}

func TestHealthcheckReturnsOneOnConnectRefused(t *testing.T) {
	// Pick a free TCP port, then close the listener — the kernel
	// returns RST on connect so dialing fails immediately rather
	// than hanging. "Endpoint not listening yet" must surface
	// as unhealthy (exit 1), not as a stuck probe.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/health", "--timeout", "200ms",
	}); got != 1 {
		t.Fatalf("expected exit code 1 for connection refused, got %d", got)
	}
}

func TestHealthcheckReturnsTwoOnUsageError(t *testing.T) {
	cases := map[string][]string{
		"missing --endpoint slash": {"--endpoint", "health"},
		"empty --endpoint":         {"--endpoint", ""},
		"negative --timeout":       {"--timeout", "-1s"},
		"zero --timeout":           {"--timeout", "0s"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			if got := runHealthcheck(args); got != 2 {
				t.Fatalf("expected exit code 2 for usage error %q, got %d", name, got)
			}
		})
	}
}

func TestHealthcheckFlagParsingErrorReturnsTwo(t *testing.T) {
	// An unknown flag exercises flag.ContinueOnError's error path
	// inside runHealthcheck; it must still surface as exit code 2.
	if got := runHealthcheck([]string{"--this-flag-does-not-exist"}); got != 2 {
		t.Fatalf("expected exit code 2 for unknown flag, got %d", got)
	}
}

func TestHealthcheckHonoursHTTPAddr(t *testing.T) {
	// Pin HTTP_ADDR to the test server and confirm --addr=""
	// (the default) routes through it. Without the HTTP_ADDR
	// fallback the probe would hit 127.0.0.1:8080 and fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	hostPort := strings.TrimPrefix(srv.URL, "http://")
	t.Setenv("HTTP_ADDR", hostPort)

	if got := runHealthcheck(nil); got != 0 {
		t.Fatalf("expected exit code 0 with HTTP_ADDR=%s, got %d", hostPort, got)
	}
}

func TestHealthcheckColonAddrReformattedToLoopback(t *testing.T) {
	// Spin up the test server, set HTTP_ADDR=":PORT" so
	// resolveHealthcheckAddr rewrites it to "127.0.0.1:PORT",
	// and confirm the probe still reaches the server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}
	t.Setenv("HTTP_ADDR", ":"+port)

	if got := runHealthcheck(nil); got != 0 {
		t.Fatalf("expected exit code 0 with HTTP_ADDR=:%s, got %d", port, got)
	}
}

func TestHealthcheckCustomEndpoint(t *testing.T) {
	// Operators who want process-only liveness can point the
	// probe at /livez instead of /health.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/livez" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/livez",
	}); got != 0 {
		t.Fatalf("expected exit code 0 for /livez 200, got %d", got)
	}
}

func TestHealthcheckDoesNotFollowRedirects(t *testing.T) {
	// A 3xx must not be silently absorbed as "healthy" — the
	// indexer is misconfigured if /health redirects, and the
	// probe must surface that as exit 1.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/health",
	}); got != 1 {
		t.Fatalf("expected exit code 1 for 3xx response, got %d", got)
	}
}

func TestHealthcheckIgnoresLargeResponseBodyAfterDecision(t *testing.T) {
	// Whatever else the server returns, the exit code is decided
	// by the status line. A response body of tens of kilobytes
	// must not pin the process open once the status is read.
	big := strings.Repeat("x", 64*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, big)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := runHealthcheck([]string{
		"--addr", addr, "--endpoint", "/health", "--timeout", "2s",
	}); got != 0 {
		t.Fatalf("expected exit code 0 even with large body, got %d", got)
	}
}
