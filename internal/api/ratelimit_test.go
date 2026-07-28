package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// drive sends req through s.Router() via httptest and returns the result.
// Using a recorder (rather than httptest.NewServer) keeps RemoteAddr under
// test control — IP-keying and XFF tests depend on deterministic peers.
func drive(t *testing.T, s *Server, req *http.Request) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)
	return rec.Result()
}

// mkReq builds a request with a known RemoteAddr so callers can drive the
// IP-key behavior without a TCP socket.
func mkReq(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = "198.51.100.5:54321"
	return req
}

func startLimiter(t *testing.T, lim *RateLimiter) func() {
	t.Helper()
	if !lim.Enabled() {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	lim.Start(ctx)
	return func() {
		cancel()
		lim.Stop()
	}
}

func TestRateLimit_DisabledByDefault(t *testing.T) {
	// No SetRateLimiter call — router should pass through indefinitely,
	// matching the documented "off by default" posture.
	s := newTestServer(&stubStore{}, nil)
	for i := 0; i < 100; i++ {
		resp := drive(t, s, mkReq(http.MethodGet, "/events"))
		require.Equal(t, http.StatusOK, resp.StatusCode, "request %d must pass with no limiter wired", i)
		resp.Body.Close()
	}
}

func TestRateLimit_DisabledByNewLimiter(t *testing.T) {
	// A limiter constructed with rps=0 or burst=0 must be a no-op even
	// when explicitly wired — protects against half-configured env vars.
	lim := NewRateLimiter(0, 0, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	for i := 0; i < 50; i++ {
		resp := drive(t, s, mkReq(http.MethodGet, "/events"))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}
}

func TestRateLimit_UnderLimitPasses(t *testing.T) {
	lim := NewRateLimiter(1, 3, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	for i := 0; i < 3; i++ {
		resp := drive(t, s, mkReq(http.MethodGet, "/events"))
		require.Equalf(t, http.StatusOK, resp.StatusCode,
			"req %d should be admitted by burst", i+1)
		resp.Body.Close()
	}
}

func TestRateLimit_OverLimitReturns429(t *testing.T) {
	// burst=1, rps=0.5 — second request within ~2s should 429 immediately.
	lim := NewRateLimiter(0.5, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	r1 := drive(t, s, mkReq(http.MethodGet, "/events"))
	require.Equal(t, http.StatusOK, r1.StatusCode)
	r1.Body.Close()

	r2 := drive(t, s, mkReq(http.MethodGet, "/events"))
	defer r2.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r2.StatusCode)

	// Standard error envelope.
	body, err := io.ReadAll(r2.Body)
	require.NoError(t, err)
	var env map[string]string
	require.NoError(t, json.Unmarshal(body, &env))
	assert.NotEmpty(t, env["error"], "429 must include error envelope")

	// Retry-After is a positive integer in seconds (RFC 7231 §7.1.3).
	ra := r2.Header.Get("Retry-After")
	require.NotEmpty(t, ra, "Retry-After must be set")
	secs, err := strconv.Atoi(ra)
	require.NoError(t, err, "Retry-After must be an integer delta-seconds")
	assert.GreaterOrEqual(t, secs, 1)
	assert.LessOrEqual(t, secs, 5, "0.5 RPS gives ~2s, allow generous slack")
}

func TestRateLimit_HealthIsExempt(t *testing.T) {
	// Tight limit so /events trips after one call; /health must keep passing.
	lim := NewRateLimiter(0.001, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	// Drain the bucket.
	r0 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r0.Body.Close()
	r1 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r1.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r1.StatusCode, "sanity: bucket must be empty")

	// /health must not 429 even under heavy pressure.
	for i := 0; i < 10; i++ {
		rh := drive(t, s, mkReq(http.MethodGet, "/health"))
		require.Equalf(t, http.StatusOK, rh.StatusCode,
			"/health call %d must be exempt", i)
		rh.Body.Close()
	}
}

func TestRateLimit_HealthzIsExempt(t *testing.T) {
	lim := NewRateLimiter(0.001, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	r0 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r0.Body.Close()
	r1 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r1.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r1.StatusCode, "sanity: bucket must be empty")

	for i := 0; i < 10; i++ {
		rh := drive(t, s, mkReq(http.MethodGet, "/healthz"))
		require.Equalf(t, http.StatusOK, rh.StatusCode,
			"/healthz call %d must be exempt", i)
		rh.Body.Close()
	}
}

func TestRateLimit_ReadyzIsExempt(t *testing.T) {
	lim := NewRateLimiter(0.001, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	// /readyz checks DB + recent ingest, so provide a valid ingestion state.
	s := newTestServer(&stubStore{ingestion: store.IngestionState{LastIngestedLedger: 1000}}, nil)
	s.SetRateLimiter(lim)

	r0 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r0.Body.Close()
	r1 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r1.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r1.StatusCode, "sanity: bucket must be empty")

	for i := 0; i < 10; i++ {
		rh := drive(t, s, mkReq(http.MethodGet, "/readyz"))
		require.Equalf(t, http.StatusOK, rh.StatusCode,
			"/readyz call %d must be exempt", i)
		rh.Body.Close()
	}
}

func TestRateLimit_MetricsIsExempt(t *testing.T) {
	// /metrics isn't routed today, but the exemption is forward-looking
	// (Prometheus scrapers shouldn't trip their own rate limit). Verify
	// it stays a non-429 response — 404 is acceptable from chi.
	lim := NewRateLimiter(0.001, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	r0 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r0.Body.Close()
	r1 := drive(t, s, mkReq(http.MethodGet, "/events"))
	r1.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r1.StatusCode, "sanity")

	rm := drive(t, s, mkReq(http.MethodGet, "/metrics"))
	defer rm.Body.Close()
	require.NotEqual(t, http.StatusTooManyRequests, rm.StatusCode,
		"/metrics must not be rate-limited even though it's not routed today")
}

func TestRateLimit_XForwardedForIgnoredWhenUntrusted(t *testing.T) {
	// Without the trusted-proxy flag, XFF must not affect the bucket key.
	// Two requests through the same TCP peer with different XFF values
	// must share one bucket — otherwise unsanitized XFF lets clients
	// dodge per-IP throttling.
	lim := NewRateLimiter(0.5, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	req1 := mkReq(http.MethodGet, "/events")
	req1.Header.Set("X-Forwarded-For", "203.0.113.1")
	r1 := drive(t, s, req1)
	require.Equal(t, http.StatusOK, r1.StatusCode, "first request from this RemoteAddr should pass")
	r1.Body.Close()

	// Same RemoteAddr, different XFF — XFF must be ignored.
	req2 := mkReq(http.MethodGet, "/events")
	req2.Header.Set("X-Forwarded-For", "203.0.113.99")
	r2 := drive(t, s, req2)
	defer r2.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r2.StatusCode,
		"spoofed XFF must not bypass the per-IP bucket")
}

func TestRateLimit_XForwardedForTrustedHonorsLeftmost(t *testing.T) {
	// With trusted-proxy mode, the leftmost XFF entry selects the bucket.
	// Different upstream IPs (XFF) behind the same proxy must get fresh
	// buckets; same upstream client must 429.
	lim := NewRateLimiter(0.5, 1, true)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	req1 := mkReq(http.MethodGet, "/events")
	req1.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1") // leftmost = 203.0.113.7
	r1 := drive(t, s, req1)
	require.Equal(t, http.StatusOK, r1.StatusCode)
	r1.Body.Close()

	req2 := mkReq(http.MethodGet, "/events")
	req2.Header.Set("X-Forwarded-For", "203.0.113.8, 10.0.0.1") // different leftmost
	r2 := drive(t, s, req2)
	require.Equal(t, http.StatusOK, r2.StatusCode,
		"different upstream client via XFF must get a fresh bucket")
	r2.Body.Close()

	// Same upstream client — must 429.
	req3 := mkReq(http.MethodGet, "/events")
	req3.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	r3 := drive(t, s, req3)
	defer r3.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r3.StatusCode,
		"returning client should hit their own bucket")
}

func TestRateLimit_CleanupEvictsIdle(t *testing.T) {
	// Tight TTL + frequent sweep so eviction is observable in milliseconds
	// rather than minutes; tests don't want to wait minutes for a sweep tick.
	lim := NewRateLimiter(1, 1, false,
		WithIdleTTL(30*time.Millisecond),
		WithSweepInterval(10*time.Millisecond),
	)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	req := mkReq(http.MethodGet, "/events")
	req.RemoteAddr = "198.51.100.99:1111"
	r := drive(t, s, req)
	r.Body.Close()
	require.Equal(t, 1, lim.Size(), "one bucket after first observation")

	// Once the bucket's lastSeen is older than the TTL it is eligible
	// for eviction. Drive the eviction directly inside the Eventually
	// predicate so the test is deterministic regardless of whether the
	// background sweep goroutine has been scheduled by the runtime yet
	// (heavily-loaded CI runners can starve background goroutines).
	require.Eventually(t, func() bool {
		evicted := lim.evictIdle(time.Now())
		t.Logf("evictIdle removed %d bucket(s); size=%d", evicted, lim.Size())
		return lim.Size() == 0
	}, 500*time.Millisecond, 5*time.Millisecond,
		"idle bucket must be evicted once TTL has expired")
}

func TestRateLimit_DifferentRemoteAddrsIsolated(t *testing.T) {
	// Defensive sanity: distinct TCP peers must get independent buckets.
	lim := NewRateLimiter(0.5, 1, false)
	stop := startLimiter(t, lim)
	t.Cleanup(stop)

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)

	a1 := mkReq(http.MethodGet, "/events")
	a1.RemoteAddr = "198.51.100.1:1000"
	ra := drive(t, s, a1)
	require.Equal(t, http.StatusOK, ra.StatusCode)
	ra.Body.Close()

	b1 := mkReq(http.MethodGet, "/events")
	b1.RemoteAddr = "198.51.100.2:1000"
	rb := drive(t, s, b1)
	require.Equal(t, http.StatusOK, rb.StatusCode,
		"second IP must NOT share bucket with first IP")
	rb.Body.Close()
}
