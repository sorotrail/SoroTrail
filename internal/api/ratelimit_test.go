package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func mkReq(method, path string) *http.Request {
	return httptest.NewRequest(method, path, nil)
}

func startLimiter(t *testing.T, lim *RateLimiter) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lim.Start(ctx)
	return func() {
		cancel()
		lim.Stop()
	}
}

func TestClientIPTrustedXFF(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.9:5432"
	r.Header.Set("X-Forwarded-For", "198.51.100.7, 10.0.0.1")
	if got := clientIP(r, true); got != "198.51.100.7" {
		t.Fatalf("clientIP(trusted) = %q, want 198.51.100.7", got)
	}
	if got := clientIP(r, false); got != "203.0.113.9" {
		t.Fatalf("clientIP(untrusted) = %q, want 203.0.113.9", got)
	}
}

func TestClientIPFallbackToRemote(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.9:5432"
	if got := clientIP(r, true); got != "203.0.113.9" {
		t.Fatalf("clientIP(no XFF) = %q, want 203.0.113.9", got)
	}
}

func TestClientKeyUsesRemoteAddrWhenNoCredential(t *testing.T) {
	l := NewRateLimiter(1, 1, false)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.9:1234"
	if got := l.clientKey(r); got != "203.0.113.9" {
		t.Fatalf("clientKey() = %q, want 203.0.113.9", got)
	}
}

func TestCeilSeconds(t *testing.T) {
	cases := []struct{ in, want time.Duration }{
		{0, time.Second},
		{1, time.Second},
		{999 * time.Millisecond, time.Second},
		{time.Second, time.Second},
		{time.Second + time.Millisecond, 2 * time.Second},
		{2 * time.Second, 2 * time.Second},
	}
	for _, c := range cases {
		if got := ceilSeconds(c.in); got != c.want {
			t.Fatalf("ceilSeconds(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestBucketEntryForReturnsSameInstance(t *testing.T) {
	l := NewRateLimiter(1, 1, false)
	e1 := l.bucketEntryFor("k1", 1, 1)
	if e1 == nil {
		t.Fatal("bucketEntryFor() returned nil")
	}
	if e2 := l.bucketEntryFor("k1", 1, 1); e2 != e1 {
		t.Fatal("bucketEntryFor() did not return the same bucket instance")
	}
}
