package api

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// middlewareFor wraps a 200-OK endpoint with the limiter under test so the
// request path — not some internal shortcut — is what every assertion below
// exercises.
func middlewareFor(lim *RateLimiter) http.Handler {
	return lim.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

// sendFrom sends a GET as if it came from addr and returns the response
// recorder. Distinct addresses are what separate buckets, so every helper
// below pins its client identity through this parameter.
func sendFrom(lim *RateLimiter, addr string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := mkReq(http.MethodGet, "/events")
	req.RemoteAddr = addr
	middlewareFor(lim).ServeHTTP(rec, req)
	return rec
}

// TestClientIP pins the extraction rules the rate limiter keys on.
// They matter for security: trusting XFF blindly lets a caller mint
// unlimited identities, and skipping a valid hop silently regroups
// traffic under the wrong bucket.
func TestClientIP(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		trustXFF   bool
		want       string
	}{
		{
			name:       "direct connection uses the remote address",
			remoteAddr: "203.0.113.9:5432",
			want:       "203.0.113.9",
		},
		{
			name:       "port is stripped from the remote address",
			remoteAddr: "198.51.100.1:65535",
			want:       "198.51.100.1",
		},
		{
			name:       "trusted proxy honors the leftmost XFF entry",
			remoteAddr: "10.0.0.1:443",
			xff:        "198.51.100.7, 10.0.0.1",
			trustXFF:   true,
			want:       "198.51.100.7",
		},
		{
			name:       "untrusted proxy ignores XFF entirely",
			remoteAddr: "203.0.113.9:5432",
			xff:        "198.51.100.7, 10.0.0.1",
			want:       "203.0.113.9",
		},
		{
			name:       "trusted with no XFF falls back to remote",
			remoteAddr: "203.0.113.9:5432",
			trustXFF:   true,
			want:       "203.0.113.9",
		},
		{
			name:       "multi-hop chain picks the first valid entry",
			remoteAddr: "10.0.0.1:443",
			xff:        "not-an-ip, 198.51.100.7, 172.16.0.9",
			trustXFF:   true,
			want:       "198.51.100.7",
		},
		{
			name:       "trusted with all-invalid XFF falls back to remote",
			remoteAddr: "203.0.113.9:5432",
			xff:        "not-an-ip, , also-bad",
			trustXFF:   true,
			want:       "203.0.113.9",
		},
		{
			name:       "IPv6 remote keeps brackets off the key",
			remoteAddr: "[2001:db8::1]:443",
			want:       "2001:db8::1",
		},
		{
			name:       "bare IPv6 remote without port",
			remoteAddr: "2001:db8::1",
			want:       "2001:db8::1",
		},
		{
			name:       "bracketed IPv6 remote without port",
			remoteAddr: "[2001:db8::1]",
			want:       "2001:db8::1",
		},
		{
			name:       "unbracketed IPv6 in XFF",
			remoteAddr: "10.0.0.1:443",
			xff:        "2001:db8::1, 10.0.0.1",
			trustXFF:   true,
			want:       "2001:db8::1",
		},
		{
			name:       "bracketed IPv6 with port in XFF",
			remoteAddr: "10.0.0.1:443",
			xff:        "[2001:db8::1]:4711, 10.0.0.1",
			trustXFF:   true,
			want:       "2001:db8::1",
		},
		{
			name:       "bracketed IPv6 without port in XFF",
			remoteAddr: "10.0.0.1:443",
			xff:        "[2001:db8::1], 10.0.0.1",
			trustXFF:   true,
			want:       "2001:db8::1",
		},
		{
			// A malformed address must produce the documented empty
			// result so clientKey can substitute its stable
			// "unknown" key instead of bucketing on junk.
			name:       "malformed remote yields empty result",
			remoteAddr: "garbage-without-a-port",
			want:       "",
		},
		{
			// host:port syntax alone is not enough: "foo:bar"
			// splits cleanly but names no IP, and returning it raw
			// would give every typo its own bucket.
			name:       "non-IP host in remote yields empty result",
			remoteAddr: "foo:bar",
			want:       "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = c.remoteAddr
			if c.xff != "" {
				r.Header.Set("X-Forwarded-For", c.xff)
			}
			got := clientIP(r, c.trustXFF)
			assert.Equal(t, c.want, got,
				"clientIP(trustXFF=%v, RemoteAddr=%q, XFF=%q)",
				c.trustXFF, c.remoteAddr, c.xff)
		})
	}
}

// TestClientKeyStableFallbackForMalformedAddress covers the hand-off
// the issue is about: a malformed address must not become an empty
// rate-limit key, because an empty key would merge every broken client
// into one bucket (or, worse, if treated as "no key", let them through
// unthrottled).
func TestClientKeyStableFallbackForMalformedAddress(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{name: "malformed remote maps to unknown", remoteAddr: "garbage-without-a-port", want: "unknown"},
		{name: "empty remote maps to unknown", remoteAddr: "", want: "unknown"},
		{name: "valid remote keeps its IP key", remoteAddr: "203.0.113.9:1234", want: "203.0.113.9"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := NewRateLimiter(1, 1, false)
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = c.remoteAddr
			assert.Equal(t, c.want, l.clientKey(r))
		})
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

// TestCeilSeconds pins the Retry-After rounding. Rounding down would
// tell a throttled client to retry before its budget refills, which
// produces a second 429 and looks like the limiter is broken; a
// negative value would violate RFC 7231's delta-seconds contract
// outright.
func TestCeilSeconds(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero returns the documented minimum", in: 0, want: time.Second},
		{name: "sub-second never rounds down to zero", in: time.Millisecond, want: time.Second},
		{name: "one nanosecond rounds up to a full second", in: 1, want: time.Second},
		{name: "sub-second rounds up to one", in: 500 * time.Millisecond, want: time.Second},
		{name: "exact whole second stays as is", in: time.Second, want: time.Second},
		{name: "larger exact multiple stays as is", in: 3 * time.Second, want: 3 * time.Second},
		{name: "fractional duration rounds up", in: 1500 * time.Millisecond, want: 2 * time.Second},
		{name: "just over a second rounds up to two", in: time.Second + time.Nanosecond, want: 2 * time.Second},
		{name: "999 milliseconds round up to one", in: 999 * time.Millisecond, want: time.Second},
		{name: "negative duration clamps to the minimum", in: -500 * time.Millisecond, want: time.Second},
		{name: "large negative never yields a negative header", in: -time.Hour, want: time.Second},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ceilSeconds(c.in)
			assert.Equal(t, c.want, got, "ceilSeconds(%s)", c.in)
			assert.Greater(t, got, time.Duration(0),
				"Retry-After must never be zero or negative (input %s)", c.in)
		})
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

// TestBucketEntryForReconfiguresLimits pins the branch that lets an operator
// lower a tenant's quota and have it bind on the next lookup instead of
// whenever the bucket happens to age out.
func TestBucketEntryForReconfiguresLimits(t *testing.T) {
	l := NewRateLimiter(1, 1, false)
	entry := l.bucketEntryFor("tenant-a", 2, 4)
	require.Equal(t, rate.Limit(2), entry.limiter.Limit())
	require.Equal(t, 4, entry.limiter.Burst())

	again := l.bucketEntryFor("tenant-a", 0.5, 1)
	require.Same(t, entry, again, "reconfiguring must reuse the existing bucket")
	assert.Equal(t, rate.Limit(0.5), again.limiter.Limit(),
		"a lowered limit must bind immediately, not when the bucket ages out")
	assert.Equal(t, 1, again.limiter.Burst())
	assert.Equal(t, 1, l.Size(), "reconfiguration must not fork a second bucket")
}

// TestRateLimiterAdmissionTable drives the middleware with distinct source
// addresses. burst=2 at 0.5 rps means a client may spend its two-token bucket
// immediately and is refused from the third request on — the plain
// token-bucket contract the limiter exists to enforce.
func TestRateLimiterAdmissionTable(t *testing.T) {
	cases := []struct {
		name         string
		remoteAddr   string
		requests     int
		wantAdmitted int
		wantRejected int
	}{
		{
			name:         "client under its budget is admitted",
			remoteAddr:   "203.0.113.10:1000",
			requests:     2,
			wantAdmitted: 2,
		},
		{
			name:         "client over budget receives 429 with Retry-After",
			remoteAddr:   "203.0.113.11:2000",
			requests:     3,
			wantAdmitted: 2,
			wantRejected: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lim := NewRateLimiter(0.5, 2, false)

			admitted, rejected := 0, 0
			for i := 0; i < tc.requests; i++ {
				rec := sendFrom(lim, tc.remoteAddr)
				if rec.Code == http.StatusOK {
					admitted++
					continue
				}
				// Only a genuine over-budget rejection may land here;
				// anything else means the limiter is refusing for the
				// wrong reason.
				require.Equal(t, http.StatusTooManyRequests, rec.Code)
				assert.NotEmpty(t, rec.Header().Get("Retry-After"),
					"every 429 must tell the caller when to come back")
				assert.Equal(t, "0", rec.Header().Get("X-RateLimit-Remaining"))
				rejected++
			}

			assert.Equal(t, tc.wantAdmitted, admitted,
				"admitted count drifted from the token-bucket contract")
			assert.Equal(t, tc.wantRejected, rejected,
				"rejected count drifted from the token-bucket contract")
			assert.Equal(t, 1, lim.Size(),
				"one source address must map to exactly one bucket")
		})
	}
}

// TestRateLimiterPerClientBuckets pins the per-key isolation behind the
// middleware: one client's spent bucket must never throttle another client.
func TestRateLimiterPerClientBuckets(t *testing.T) {
	lim := NewRateLimiter(0.5, 1, false)

	first := sendFrom(lim, "203.0.113.21:4000")
	require.Equal(t, http.StatusOK, first.Code, "first client's initial request must pass")

	// A different source address gets its own bucket, not what is left of
	// client A's.
	second := sendFrom(lim, "203.0.113.22:5000")
	assert.Equal(t, http.StatusOK, second.Code,
		"a second client must not be throttled by the first client's spend")
	assert.Equal(t, 2, lim.Size(), "two clients must map to two buckets")

	// And client A really is done for now: their own bucket ran dry, not
	// some shared pool.
	third := sendFrom(lim, "203.0.113.21:4000")
	assert.Equal(t, http.StatusTooManyRequests, third.Code,
		"the first client's own budget must still be exhausted")
}

// TestRateLimiterUnauthenticatedFallback pins the fallback branch for callers
// without a credential: they are keyed by source IP and share the
// instance-wide bucket rather than being rejected outright.
func TestRateLimiterUnauthenticatedFallback(t *testing.T) {
	lim := NewRateLimiter(0.5, 1, false)
	addr := "203.0.113.31:6000"

	for i, want := range []int{http.StatusOK, http.StatusTooManyRequests} {
		rec := sendFrom(lim, addr)
		assert.Equal(t, want, rec.Code,
			"request %d from an unauthenticated address must hit the shared IP bucket", i+1)
	}
}

// TestRateLimiterRefillsOverTime covers the refill path. The limiter's one
// seam for moving time without sleeping is the idle sweep, which takes an
// explicit `now`: once a quiet bucket ages past the TTL it is evicted, and
// the client's next request starts from a freshly created, full bucket —
// which is exactly how a long-quiet client is refilled in production.
func TestRateLimiterRefillsOverTime(t *testing.T) {
	lim := NewRateLimiter(1, 2, false)
	addr := "203.0.113.41:8000"

	start := time.Now()
	require.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)
	require.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)
	require.Equal(t, http.StatusTooManyRequests, sendFrom(lim, addr).Code,
		"the bucket must be empty once the burst is spent")

	// An hour later the untouched bucket is both refill-eligible (at 1 token
	// per second it would have long topped up) and past the 5-minute idle
	// TTL, so the sweep retires it.
	require.Equal(t, 1, lim.evictIdle(start.Add(time.Hour)),
		"a bucket untouched past the idle TTL must be evicted")
	assert.Equal(t, 0, lim.Size(), "eviction must remove the bucket from the map")

	assert.Equal(t, http.StatusOK, sendFrom(lim, addr).Code,
		"after the refill window the client must be admitted again")
	assert.Equal(t, http.StatusOK, sendFrom(lim, addr).Code,
		"the refilled bucket must carry the full burst again")
}

// TestRateLimiterRefillDoesNotLeakSpendAcrossEviction is the negative half
// of the refill story: eviction refills a quiet client, but a client seen
// inside the TTL window keeps its spent bucket even when the sweep runs.
func TestRateLimiterRefillDoesNotLeakSpendAcrossEviction(t *testing.T) {
	lim := NewRateLimiter(1, 1, false)
	addr := "203.0.113.42:8100"

	now := time.Now()
	require.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)

	// A sweep a second later must not evict a just-seen bucket, and the
	// client's spend must survive it.
	assert.Equal(t, 0, lim.evictIdle(now.Add(time.Second)),
		"a bucket seen inside the TTL window must survive the sweep")
	assert.Equal(t, http.StatusTooManyRequests, sendFrom(lim, addr).Code,
		"the sweep must not refill a bucket that is still in use")
}

// TestRateLimiterRetryAfterIsCeiled covers the Retry-After calculation end
// to end. With burst=3 at 0.5 rps the fourth immediate request waits just
// under two seconds for its next token, so delta-seconds rounding must ceil
// to 2 — truncating to 1 would invite the caller to hammer the endpoint
// back-to-back and earn a second 429.
func TestRateLimiterRetryAfterIsCeiled(t *testing.T) {
	lim := NewRateLimiter(0.5, 3, false)
	addr := "203.0.113.51:9000"

	for i := 0; i < 3; i++ {
		rec := sendFrom(lim, addr)
		require.Equal(t, http.StatusOK, rec.Code, "request %d must be admitted", i+1)
	}

	rec := sendFrom(lim, addr)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	raw := rec.Header().Get("Retry-After")
	require.NotEmpty(t, raw, "a 429 must carry Retry-After")
	secs, err := strconv.Atoi(raw)
	require.NoError(t, err, "Retry-After must be integer delta-seconds (RFC 7231 §7.1.3)")
	assert.Equal(t, 2, secs,
		"ceil(2s - elapsed) must stay 2s; truncating to 1 would let the caller retry too early")

	assert.Equal(t, "0", rec.Header().Get("X-RateLimit-Remaining"))
	assert.Equal(t, "3", rec.Header().Get("X-RateLimit-Limit"), "the burst must be advertised")

	reset, err := strconv.ParseInt(rec.Header().Get("X-RateLimit-Reset"), 10, 64)
	require.NoError(t, err, "X-RateLimit-Reset must be a unix timestamp")
	assert.InDelta(t, time.Now().Add(2*time.Second).Unix(), reset, 1,
		"X-RateLimit-Reset must sit one refill period out")
}

// TestRateLimiterResolvedBucketsArePerKey pins the resolver seam itself.
// Once a resolver supplies the key, two callers sharing a source address must
// still get separate budgets — the property per-tenant limits are built on —
// and the burst the resolver returns, not the instance-wide one, is what gets
// enforced and advertised.
func TestRateLimiterResolvedBucketsArePerKey(t *testing.T) {
	// The header stands in for whatever identity the resolver keys on; the
	// tenant path uses the principal, which authenticate installs.
	resolve := func(r *http.Request) (string, float64, int, bool) {
		key := r.Header.Get("X-Test-Bucket")
		if key == "" {
			return "", 0, 0, false
		}
		return key, 0.5, 2, true
	}
	lim := NewRateLimiter(1, 5, false, WithLimitResolver(resolve))

	send := func(bucket, addr string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := mkReq(http.MethodGet, "/events")
		req.RemoteAddr = addr
		if bucket != "" {
			req.Header.Set("X-Test-Bucket", bucket)
		}
		middlewareFor(lim).ServeHTTP(rec, req)
		return rec
	}

	// Both keys ride the same source address. Only the resolved key differs;
	// a bucket chosen by address here would throttle one caller for the
	// other's spend.
	const sharedAddr = "203.0.113.72:6000"

	for i := 0; i < 2; i++ {
		rec := send("tenant:1", sharedAddr)
		require.Equal(t, http.StatusOK, rec.Code,
			"request %d within the resolved burst must be admitted", i+1)
		assert.Equal(t, "2", rec.Header().Get("X-RateLimit-Limit"),
			"the resolver's burst must be advertised, not the instance-wide burst")
	}
	rec := send("tenant:1", sharedAddr)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))

	assert.Equal(t, http.StatusOK, send("tenant:2", sharedAddr).Code,
		"a second resolved key must keep its own budget despite sharing an address")
	assert.Equal(t, 2, lim.Size(), "each resolved key must own exactly one bucket")

	// A caller the resolver declines keeps the instance-wide IP bucket, which
	// is a third key rather than a share of either resolved one.
	assert.Equal(t, http.StatusOK, send("", sharedAddr).Code)
	assert.Equal(t, 3, lim.Size())
}

// TestRateLimiterDisabledIsPassthrough guards the off-by-default posture: a
// limiter with no limits configured must not add or reject anything, even
// though the wiring stays in place.
func TestRateLimiterDisabledIsPassthrough(t *testing.T) {
	lim := NewRateLimiter(0, 0, false)
	require.False(t, lim.Enabled())

	for i := 0; i < 5; i++ {
		rec := sendFrom(lim, "203.0.113.61:10000")
		assert.Equal(t, http.StatusOK, rec.Code, "a disabled limiter must never reject")
	}
	assert.Equal(t, 0, lim.Size(), "a disabled limiter must not track buckets")
	assert.Equal(t, 0, lim.quotaSize(), "a disabled limiter must not track quotas")
}

// limiterClock moves the limiter's clock forward without sleeping. The
// token bucket itself still refills on the real clock, so quota tests use a
// high RPS to keep bucket admission out of the way.
type limiterClock struct {
	now time.Time
}

func (c *limiterClock) Now() time.Time { return c.now }

func (c *limiterClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func retryAfterSeconds(t *testing.T, rec *httptest.ResponseRecorder) int {
	t.Helper()
	raw := rec.Header().Get("Retry-After")
	require.NotEmpty(t, raw, "a quota rejection must carry Retry-After")
	secs, err := strconv.Atoi(raw)
	require.NoError(t, err, "Retry-After must be integer delta-seconds")
	return secs
}

// TestRateLimiterDailyQuotaSurvivesIdleBucketEviction is the regression test
// for quotas resetting when a client pauses: after the daily budget is spent
// and the idle sweep drops the token bucket, the next request must still be
// refused until the 24-hour window closes. In particular the sweep itself
// must not take the quota counters with it.
func TestRateLimiterDailyQuotaSurvivesIdleBucketEviction(t *testing.T) {
	const (
		limit = int64(5)
		addr  = "203.0.113.81:11000"
	)
	clock := &limiterClock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	lim := NewRateLimiter(1000, 1000, false,
		WithDailyQuota(limit),
		WithIdleTTL(time.Minute),
		WithClock(clock.Now),
	)

	// Spend the whole budget, then keep hammering: the quota, not the token
	// bucket, must be what refuses the excess.
	for i := int64(0); i < limit; i++ {
		rec := sendFrom(lim, addr)
		require.Equal(t, http.StatusOK, rec.Code, "request %d must fit in the daily quota", i+1)
	}
	for i := 0; i < 3; i++ {
		rec := sendFrom(lim, addr)
		require.Equal(t, http.StatusTooManyRequests, rec.Code,
			"usage above DAILY_QUOTA must be refused")
		assert.Equal(t, "0", rec.Header().Get("X-RateLimit-Remaining"))
		assert.Equal(t, int((24 * time.Hour).Seconds()), retryAfterSeconds(t, rec))
	}
	require.Equal(t, 1, lim.quotaSize(), "the quota counters must be tracked")

	// Idle past the bucket TTL. The bucket is reclaimed, but the quota is
	// still inside its window and has to keep binding.
	clock.Advance(2 * time.Minute)
	require.Equal(t, 1, lim.evictIdle(clock.Now()),
		"the idle token bucket must still be evicted")
	assert.Equal(t, 0, lim.Size(), "bucket eviction must reclaim the bucket")
	assert.Equal(t, 1, lim.quotaSize(),
		"bucket eviction must not discard a live quota window")

	rec := sendFrom(lim, addr)
	require.Equal(t, http.StatusTooManyRequests, rec.Code,
		"a client returning inside the same day must still be over quota")
	assert.Equal(t, int((24*time.Hour - 2*time.Minute).Seconds()), retryAfterSeconds(t, rec),
		"the retry hint must reflect the remaining daily window")
	assert.Equal(t, "0", rec.Header().Get("X-RateLimit-Remaining"))

	// Once both the idle bucket and the quota window have expired, the
	// state is reclaimed and the next request starts a fresh day.
	clock.Advance(24 * time.Hour)
	require.Equal(t, 1, lim.evictIdle(clock.Now()),
		"the bucket recreated by the refused request must also age out")
	assert.Equal(t, 0, lim.Size())
	assert.Equal(t, 0, lim.quotaSize(), "an expired quota window must be reclaimed")
	assert.Equal(t, http.StatusOK, sendFrom(lim, addr).Code,
		"after the daily window closes the client must be admitted again")
}

// TestRateLimiterHourlyQuotaSurvivesIdleBucketEviction covers the shorter
// window too, and proves the retry hint is the time left in that window
// rather than the idle TTL.
func TestRateLimiterHourlyQuotaSurvivesIdleBucketEviction(t *testing.T) {
	const (
		limit = int64(2)
		addr  = "203.0.113.82:12000"
	)
	clock := &limiterClock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	lim := NewRateLimiter(1000, 1000, false,
		WithHourlyQuota(limit),
		WithIdleTTL(time.Minute),
		WithClock(clock.Now),
	)

	for i := int64(0); i < limit; i++ {
		require.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)
	}
	rec := sendFrom(lim, addr)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, int(time.Hour.Seconds()), retryAfterSeconds(t, rec))

	clock.Advance(2 * time.Minute)
	require.Equal(t, 1, lim.evictIdle(clock.Now()))
	assert.Equal(t, 0, lim.Size())
	assert.Equal(t, 1, lim.quotaSize())

	rec = sendFrom(lim, addr)
	require.Equal(t, http.StatusTooManyRequests, rec.Code,
		"the hourly quota must survive idle-bucket eviction")
	assert.Equal(t, int((time.Hour - 2*time.Minute).Seconds()), retryAfterSeconds(t, rec))

	// Crossing the hour boundary resets the hourly counters even though the
	// daily quota is disabled and no new state remains to retain.
	clock.Advance(time.Hour)
	require.Equal(t, 1, lim.evictIdle(clock.Now()))
	assert.Equal(t, 0, lim.quotaSize())
	assert.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)
}

// TestRateLimiterQuotaCountersSurviveBucketRecreation pins the accounting
// boundary: a denied request may recreate the client's token bucket, but it
// must not mint fresh quota counters.
func TestRateLimiterQuotaCountersSurviveBucketRecreation(t *testing.T) {
	const addr = "203.0.113.83:13000"
	clock := &limiterClock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	lim := NewRateLimiter(1000, 1000, false,
		WithDailyQuota(1),
		WithIdleTTL(time.Minute),
		WithClock(clock.Now),
	)

	require.Equal(t, http.StatusOK, sendFrom(lim, addr).Code)
	clock.Advance(2 * time.Minute)
	require.Equal(t, 1, lim.evictIdle(clock.Now()))
	require.Equal(t, 0, lim.Size())
	require.Equal(t, 1, lim.quotaSize())

	for i := 0; i < 3; i++ {
		rec := sendFrom(lim, addr)
		require.Equal(t, http.StatusTooManyRequests, rec.Code,
			"each refused request must be refused by quota, not by a reset counter")
	}
	// The refusals touched the bucket (lastSeen), not the quota counters.
	assert.Equal(t, int64(1), lim.quotas["203.0.113.83"].dayUsed,
		"a refused request must not consume quota or reset the window")
}

type mockQuotaStore struct {
	hourUsed, dayUsed int64
}

func (m *mockQuotaStore) IncrementQuota(ctx context.Context, clientKey string, now time.Time, batchSize int) (int64, int64, error) {
	m.hourUsed += int64(batchSize)
	m.dayUsed += int64(batchSize)
	return m.hourUsed, m.dayUsed, nil
}

func TestMultiInstanceRateLimiterQuotaShared(t *testing.T) {
	store := &mockQuotaStore{}
	limiter1 := NewRateLimiter(10, 100, false, WithQuotaStore(store), WithDailyQuota(10))
	limiter2 := NewRateLimiter(10, 100, false, WithQuotaStore(store), WithDailyQuota(10))

	for i := 0; i < 5; i++ {
		rec := sendFrom(limiter1, "203.0.113.99:1234")
		require.Equal(t, http.StatusOK, rec.Code)
	}
	for i := 0; i < 5; i++ {
		rec := sendFrom(limiter2, "203.0.113.99:1234")
		require.Equal(t, http.StatusOK, rec.Code)
	}
	rec := sendFrom(limiter1, "203.0.113.99:1234")
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
}

// TestClearBuckets drops all cached token buckets.
func TestClearBuckets(t *testing.T) {
	lim := NewRateLimiter(1, 1, false)

	// Populate some buckets first.
	sendFrom(lim, "203.0.113.1:1")
	sendFrom(lim, "203.0.113.2:2")

	assert.Equal(t, 2, lim.Size(), "buckets must exist before ClearBuckets")

	lim.ClearBuckets()

	assert.Equal(t, 0, lim.Size(), "ClearBuckets must remove all buckets")
}
