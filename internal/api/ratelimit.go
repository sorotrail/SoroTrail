// Package api — rate-limiter middleware.
//
// RateLimiter is a per-client token-bucket gate for the HTTP API. It is
// wired into the API router; an enabled limiter rejects requests over the
// per-key RPS *with a burst window* using golang.org/x/time/rate.
//
// Design notes:
//
//   - When the operator does not configure RATE_LIMIT_RPS / RATE_LIMIT_BURST
//     the limiter is a no-op. This preserves the "off by default" posture;
//     existing deployments see no behavior change.
//   - Keys are derived from the client's source IP from r.RemoteAddr by
//     default. If the trusted-proxy flag is set we honor
//     X-Forwarded-For (leftmost entry), which assumes there is an upstream
//     proxy that strips/rewrites the header. The flag defaults to false
//     because clients control X-Forwarded-For — accepting it
//     unconditionally would let any caller pick their own key and skirt
//     per-IP throttling.
//   - Each unique key gets its own *rate.Limiter on first sight. Idle
//     buckets are evicted by a background sweep to bound memory against
//     churning source addresses (NAT exits, scraper sweeps, scanners).
//   - 429 responses carry a Retry-After header and the standard
//     {"error": "..."} envelope.

package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// exemptPaths are HTTP paths that bypass the limiter. /health is polled
// by orchestrators and /metrics is scraped by Prometheus — throttling
// either of those degrades observability without slowing an abuser.
// Prometheus hammering the scrape target would otherwise generate
// rate-limit errors against the very metrics operators rely on to see
// the rate-limit pressure.
var exemptPaths = map[string]bool{
	"/health":  true,
	"/metrics": true,
}

// RateLimiter is a per-client token-bucket HTTP middleware.
type RateLimiter struct {
	rps     float64
	burst   int
	trusted bool

	// idleTTL is the wall-clock age beyond which an untouched bucket is
	// evicted. sweepEvery is the cleanup tick. Both are tunable for tests
	// via WithIdleTTL and WithSweepInterval.
	idleTTL    time.Duration
	sweepEvery time.Duration

	mu      sync.Mutex
	buckets map[string]*bucketEntry

	once sync.Once
	stop chan struct{}
	wg   sync.WaitGroup
}

// bucketEntry tracks one client's rate.Limiter and the wall-clock time of
// its most recent observation. lastSeen uses atomic stores so the cleanup
// sweeper can read it without holding the bucket-map mutex.
type bucketEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64 // unix nanos
}

// LimiterOption configures optional tuning knobs. Defaults are tuned for
// production (5min TTL, 1min sweep); tests override both via these
// options to make eviction observable in milliseconds rather than minutes.
type LimiterOption func(*RateLimiter)

// WithIdleTTL sets how long a bucket survives without traffic before
// sweep evicts it. Values <= 0 are ignored.
func WithIdleTTL(d time.Duration) LimiterOption {
	return func(l *RateLimiter) {
		if d > 0 {
			l.idleTTL = d
		}
	}
}

// WithSweepInterval sets how often the cleanup goroutine ticks.
// Values <= 0 are ignored.
func WithSweepInterval(d time.Duration) LimiterOption {
	return func(l *RateLimiter) {
		if d > 0 {
			l.sweepEvery = d
		}
	}
}

// NewRateLimiter returns a middleware that admits up to `burst` requests
// per client instantaneously, refilling at `rps` per second.
//
// trustedProxy enables honoring X-Forwarded-For. When rps <= 0 or
// burst <= 0 the returned limiter is disabled (Enabled returns false);
// callers may still wire it unconditionally because Middleware becomes a
// pass-through in that case.
func NewRateLimiter(rps float64, burst int, trustedProxy bool, opts ...LimiterOption) *RateLimiter {
	l := &RateLimiter{
		rps:        rps,
		burst:      burst,
		trusted:    trustedProxy,
		idleTTL:    5 * time.Minute,
		sweepEvery: 1 * time.Minute,
		buckets:    make(map[string]*bucketEntry),
		stop:       make(chan struct{}),
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Enabled reports whether the limiter actually rejects anything. When
// false, Middleware passes requests through unchanged.
func (l *RateLimiter) Enabled() bool {
	return l != nil && l.rps > 0 && l.burst > 0
}

// Start runs the idle-bucket cleanup goroutine. No-op when disabled.
// Cancel ctx OR call Stop to terminate.
func (l *RateLimiter) Start(ctx context.Context) {
	if !l.Enabled() {
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		ticker := time.NewTicker(l.sweepEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.stop:
				return
			case <-ticker.C:
				l.evictIdle(time.Now())
			}
		}
	}()
}

// Stop terminates the cleanup goroutine. Safe to call multiple times.
// No-op when disabled.
func (l *RateLimiter) Stop() {
	if !l.Enabled() {
		return
	}
	l.once.Do(func() { close(l.stop) })
	l.wg.Wait()
}

// evictIdle drops buckets whose lastSeen is older than now-idleTTL.
// Returns the number of buckets evicted (used by tests).
func (l *RateLimiter) evictIdle(now time.Time) int {
	cutoff := now.Add(-l.idleTTL).UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for k, e := range l.buckets {
		if e.lastSeen.Load() < cutoff {
			delete(l.buckets, k)
			removed++
		}
	}
	return removed
}

// Size reports the number of buckets currently tracked. Useful for
// tests that verify eviction happened.
func (l *RateLimiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// Middleware wraps next with the limiter. Returns next unchanged when
// the limiter is disabled, so wiring can stay in place regardless of
// configuration.
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	if !l.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exemptPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		key := l.clientKey(r)
		lim := l.bucketFor(key)

		// Reserve so we can read the actual wait-to-1-token time and use
		// it for an accurate Retry-After. Cancel undoes the reservation
		// when Delay > 0 (the request is being rejected). This is the
		// standard golang.org/x/time/rate idiom for "peek without burn".
		rsv := lim.Reserve()
		if !rsv.OK() {
			// We use burst >= 1 in our validator so n>max tokens is not
			// reachable; report a conservative retry-after just in case.
			l.writeLimited(w, l.idleTTL)
			return
		}
		delay := rsv.Delay()
		if delay > 0 {
			rsv.Cancel()
			l.writeLimited(w, ceilSeconds(delay))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeLimited sends a 429 with the standard error envelope and a
// Retry-After header rounded up to whole seconds (RFC 7231 §7.1.3
// accepts delta-seconds as a non-negative integer).
func (l *RateLimiter) writeLimited(w http.ResponseWriter, retryAfter time.Duration) {
	secs := int(retryAfter.Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeError(w, http.StatusTooManyRequests,
		errors.New("rate limit exceeded; retry later"))
}

// bucketFor returns the per-key limiter, creating one on first sight and
// refreshing lastSeen so cleanup is debounced.
func (l *RateLimiter) bucketFor(key string) *rate.Limiter {
	now := time.Now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.buckets[key]; ok {
		e.lastSeen.Store(now)
		return e.limiter
	}
	e := &bucketEntry{limiter: rate.NewLimiter(rate.Limit(l.rps), l.burst)}
	e.lastSeen.Store(now)
	l.buckets[key] = e
	return e.limiter
}

// clientKey returns the rate-limit key for this request.
//
// Trusted-proxy mode: take the leftmost entry of X-Forwarded-For (per
// RFC 7239, the original client). Untrusted: use r.RemoteAddr.
//
// An empty/invalid XFF falls back to RemoteAddr even when trusted, so a
// misbehaving upstream proxy that strips the header still produces a
// usable key instead of silently grouping all such traffic together.
func (l *RateLimiter) clientKey(r *http.Request) string {
	if ip := clientIP(r, l.trusted); ip != "" {
		return ip
	}
	return "unknown"
}

// clientIP returns the parsed source IP, optionally honoring XFF.
func clientIP(r *http.Request, trustXFF bool) string {
	if trustXFF {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			for _, part := range strings.Split(xff, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				if host, _, err := net.SplitHostPort(part); err == nil {
					if net.ParseIP(host) != nil {
						return host
					}
					continue
				}
				if net.ParseIP(part) != nil {
					return part
				}
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// RemoteAddr may already be just an IP for some transports; treat
		// anything net.ParseIP accepts as the host.
		if net.ParseIP(r.RemoteAddr) != nil {
			return r.RemoteAddr
		}
		return ""
	}
	return host
}

// ceilSeconds rounds d up to the next whole second. RFC 7231 specifies
// Retry-After as delta-seconds (integer), so any sub-second precision is
// rounded up rather than truncated — telling a throttled client they may
// retry "0 seconds" would let them hammer the endpoint back-to-back.
func ceilSeconds(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Second
	}
	secs := d / time.Second
	if d%time.Second != 0 {
		secs++
	}
	return secs * time.Second
}
