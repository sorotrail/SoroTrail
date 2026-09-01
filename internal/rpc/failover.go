package rpc

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// ErrAllProvidersDown is returned when every provider is unhealthy and the
// failover client is backing off.
var ErrAllProvidersDown = errors.New("rpc: all providers down, backing off")

// ErrFailoverReanchor is returned by a FailoverClient when a provider switch
// occurs mid-pagination (cursor-based request). Callers that persist a
// cursor should discard it and re-anchor from the last known ledger position
// so a foreign cursor from the old provider is not replayed against the new
// one. Idempotent upserts absorb any overlap from re-scanning.
var ErrFailoverReanchor = errors.New("rpc: failover occurred, discard cursor and re-anchor from ledger position")

// IsFailoverReanchor reports whether err is ErrFailoverReanchor.
func IsFailoverReanchor(err error) bool {
	return errors.Is(err, ErrFailoverReanchor)
}

// ProviderState describes the health state of one RPC provider.
type ProviderState int32

const (
	// StateActive is receiving live traffic.
	StateActive ProviderState = 0
	// StateDegraded has hit the consecutive-error threshold and is being
	// probed passively until it recovers.
	StateDegraded ProviderState = 1
	// StateDown failed probation; excluded from traffic and probed
	// actively on a slow timer.
	StateDown ProviderState = 2
)

func (s ProviderState) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateDegraded:
		return "degraded"
	case StateDown:
		return "down"
	default:
		return "unknown"
	}
}

// provider wraps one concrete rpc.Client with health tracking.
type provider struct {
	client Client
	url    string
	// label is the Prometheus-safe, credential-free identifier for this
	// provider (its hostname). RPC URLs may carry basic-auth credentials,
	// and raw URLs must never appear as metric labels.
	label string

	state    atomic.Int32 // ProviderState
	errCount atomic.Int32 // consecutive errors (resets on success)
	okCount  atomic.Int32 // consecutive successes during probation
	limiter  *rate.Limiter
}

// FailoverClient implements rpc.Client by routing to the highest-priority
// healthy provider. It is a decorator: the concrete HTTPClient doesn't learn
// about failover.
//
// Health scoring is passive-first: real request outcomes (network/5xx errors)
// drive demotion. Active getHealth probes are only sent to demoted providers
// awaiting recovery — healthy providers are never probed, so rate limit is
// reserved for real work.
//
// Error classification:
//   - Network errors (DNS, connection refused, timeout) and HTTP 5xx → demote
//   - Semantic errors (IsLedgerOutOfRange, HTTP 4xx) → do NOT demote
type FailoverClient struct {
	providers []*provider
	log       *slog.Logger

	maxConsecutiveErrors int           // demote after this many (default 3)
	probationSuccesses   int           // promote after this many (default 2)
	probeInterval        time.Duration // getHealth interval for StateDown (default 30s)
	headSkewTolerance    uint32        // ignore small chain-head differences (default 3)

	// Which provider handled the last successful call (index). Used to
	// detect provider switches so we can signal cursor invalidation.
	lastActive atomic.Int32 // provider index, or -1 if never succeeded

	// All-down backoff state.
	allDownCount atomic.Int32 // consecutive all-down episodes for exponential backoff
	allDownUntil atomic.Int64 // unix nanos; zero when not in backoff

	// Per-provider rate limit RPS for constructing child clients.
	rateLimitRPS float64
}

// FailoverOption customizes a FailoverClient.
type FailoverOption func(*FailoverClient)

// WithFailoverMaxErrors sets the consecutive error count that triggers
// demotion. Default 3.
func WithFailoverMaxErrors(n int) FailoverOption {
	return func(fc *FailoverClient) { fc.maxConsecutiveErrors = n }
}

// WithFailoverProbationSuccesses sets the number of consecutive successful
// probes required for promotion. Default 2.
func WithFailoverProbationSuccesses(n int) FailoverOption {
	return func(fc *FailoverClient) { fc.probationSuccesses = n }
}

// WithFailoverProbeInterval sets the getHealth probe interval for
// StateDown providers. Default 30s.
func WithFailoverProbeInterval(d time.Duration) FailoverOption {
	return func(fc *FailoverClient) { fc.probeInterval = d }
}

// WithFailoverHeadSkew sets the maximum acceptable chain-head difference
// between providers. Default 3 ledgers.
func WithFailoverHeadSkew(n uint32) FailoverOption {
	return func(fc *FailoverClient) { fc.headSkewTolerance = n }
}

// WithFailoverLogger sets the logger. Default slog.Default().
func WithFailoverLogger(log *slog.Logger) FailoverOption {
	return func(fc *FailoverClient) { fc.log = log }
}

// NewFailoverClient creates a failover wrapper around one concrete client per
// URL. The list order is priority: index 0 is tried first. rateLimitRPS is
// applied per provider (each gets its own token bucket).
//
// Each provider's concrete client is created via newClient(url, rateLimitRPS).
// Pass this as a constructor function so tests can inject mocks.
func NewFailoverClient(urls []string, rateLimitRPS float64, newClient func(url string, rps float64) Client, opts ...FailoverOption) *FailoverClient {
	fc := &FailoverClient{
		log:                  slog.Default(),
		maxConsecutiveErrors: 3,
		probationSuccesses:   2,
		probeInterval:        30 * time.Second,
		headSkewTolerance:    3,
		rateLimitRPS:         rateLimitRPS,
	}
	fc.lastActive.Store(-1)
	for _, opt := range opts {
		opt(fc)
	}

	fc.providers = make([]*provider, len(urls))
	for i, u := range urls {
		bucketRPS := rateLimitRPS
		burst := int(math.Ceil(bucketRPS))
		if burst < 1 {
			burst = 1
		}
		fc.providers[i] = &provider{
			client:  newClient(u, rateLimitRPS),
			url:     u,
			label:   providerLabel(u),
			limiter: rate.NewLimiter(rate.Limit(bucketRPS), burst),
		}
		// Start active.
		fc.setProviderState(fc.providers[i], StateActive)
	}
	return fc
}

// providerLabel derives a bounded, credential-free Prometheus label from a
// provider URL: scheme, userinfo (basic-auth credentials), path, and query
// are stripped — only the hostname remains. Malformed or empty URLs fall
// back to "unknown" so a label is always present and never leaks secrets.
func providerLabel(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return "unknown"
	}
	return u.Hostname()
}

// setProviderState records a provider's health state and republishes the
// per-provider state gauge (1 on the new state, 0 on every other) so
// /metrics always reflects the live provider set.
func (fc *FailoverClient) setProviderState(p *provider, s ProviderState) {
	p.state.Store(int32(s))
	for _, st := range []ProviderState{StateActive, StateDegraded, StateDown} {
		v := 0.0
		if st == s {
			v = 1
		}
		metrics.RPCProviderState.WithLabelValues(p.label, st.String()).Set(v)
	}
}

// getActive returns the highest-priority active provider, or -1 if none.
// Degraded providers are only returned when no active provider exists.
func (fc *FailoverClient) getActive() int {
	// Prefer active providers first.
	for i, p := range fc.providers {
		if ProviderState(p.state.Load()) == StateActive {
			return i
		}
	}
	// Fall back to degraded providers.
	for i, p := range fc.providers {
		if ProviderState(p.state.Load()) == StateDegraded {
			return i
		}
	}
	return -1
}

// ProviderStates returns a snapshot of each provider's URL and state,
// suitable for metrics or diagnostics.
func (fc *FailoverClient) ProviderStates() []struct {
	URL   string
	State ProviderState
} {
	out := make([]struct {
		URL   string
		State ProviderState
	}, len(fc.providers))
	for i, p := range fc.providers {
		out[i].URL = p.url
		out[i].State = ProviderState(p.state.Load())
	}
	return out
}

// pickProvider selects the provider for the next call, respecting priority
// order and health state. It handles the all-down backoff case.
func (fc *FailoverClient) pickProvider(ctx context.Context) (*provider, int, error) {
	// Check all-down backoff.
	if until := fc.allDownUntil.Load(); until != 0 {
		if d := time.Until(time.Unix(0, until)); d > 0 {
			return nil, -1, ErrAllProvidersDown
		}
		fc.allDownUntil.Store(0)
	}

	idx := fc.getActive()
	if idx >= 0 {
		return fc.providers[idx], idx, nil
	}

	// No active or degraded providers — all are StateDown. Set
	// exponential backoff (capped at 30s) with jitter.
	fc.allDownCount.Add(1)
	fc.allDownUntil.Store(time.Now().Add(fc.allDownBackoff()).UnixNano())
	fc.log.Error("all RPC providers are down", "backoff", fc.allDownBackoff())
	return nil, -1, ErrAllProvidersDown
}

// allDownBackoff returns an exponentially-growing backoff duration with
// jitter, capped at 30s. It uses the allDownCount to scale.
func (fc *FailoverClient) allDownBackoff() time.Duration {
	c := fc.allDownCount.Load()
	base := time.Second
	for range c {
		base *= 2
		if base > 30*time.Second {
			base = 30 * time.Second
			break
		}
	}
	// ±25% jitter.
	jitter := time.Duration(rand.N(int64(base) / 4))
	return base - base/4 + jitter
}

func (fc *FailoverClient) recordSuccess(idx int) {
	p := fc.providers[idx]
	p.errCount.Store(0)

	// A successful call means at least one provider is available, so
	// reset the all-down backoff counter.
	fc.allDownCount.Store(0)
	fc.allDownUntil.Store(0)

	prev := fc.lastActive.Swap(int32(idx))
	if prev != int32(idx) && prev != -1 {
		metrics.RPCFailoversTotal.WithLabelValues("switch").Inc()
		fc.log.Info("failover provider switched",
			"from", fc.providers[prev].url,
			"to", p.url)
	}

	// If degraded/down and probation passes, promote.
	s := ProviderState(p.state.Load())
	if s == StateDegraded || s == StateDown {
		if p.okCount.Add(1) >= int32(fc.probationSuccesses) {
			p.okCount.Store(0)
			fc.setProviderState(p, StateActive)
			p.errCount.Store(0)
			fc.log.Info("provider promoted", "url", p.url, "state", "active")
		}
	}
}

func (fc *FailoverClient) recordError(idx int, err error) {
	p := fc.providers[idx]

	// Semantic errors do NOT demote. IsLedgerOutOfRange means the provider
	// is healthy but our query is outside its retention window.
	if IsLedgerOutOfRange(err) {
		// Don't count as an error.
		return
	}

	// Classify: network errors and 5xx responses demote.
	if !isDemotableError(err) {
		// 4xx, or other non-demotable error.
		return
	}

	p.okCount.Store(0)
	c := p.errCount.Add(1)
	oldState := ProviderState(p.state.Load())

	if oldState == StateActive && c >= int32(fc.maxConsecutiveErrors) {
		fc.setProviderState(p, StateDegraded)
		fc.log.Warn("provider degraded", "url", p.url,
			"consecutive_errors", c, "state", "degraded")
	} else if oldState == StateDegraded && c >= int32(fc.maxConsecutiveErrors*2) {
		fc.setProviderState(p, StateDown)
		fc.log.Error("provider down", "url", p.url,
			"consecutive_errors", c, "state", "down")
	}
}

// isDemotableError reports whether err should count toward provider demotion.
// Network errors (DNS, refused, timeout, reset) and HTTP 5xx responses are
// demotable. Semantic errors (4xx, ledger-out-of-range) are not.
func isDemotableError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()

	// Network-level errors.
	if isNetworkError(err) {
		return true
	}

	// HTTP 5xx. The HTTPClient wraps these as "method returned HTTP 5xx".
	if strings.Contains(msg, "HTTP 5") {
		return true
	}

	// JSON-RPC server errors are in the -32000 to -32099 range.
	// -32600 (Invalid Request), -32601 (Method not found), -32602 (Invalid
	// params) are client errors and must NOT demote. -32603 (Internal error)
	// is a server error and SHOULD demote.
	if strings.Contains(msg, "rpc error -320") {
		return true
	}
	if strings.Contains(msg, "rpc error -32603") {
		return true
	}

	return false
}

// isNetworkError checks for common net.OpError patterns without importing
// every OS-specific package.
func isNetworkError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// Also catch wrapped network errors and context cancellations/timeouts.
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "TLS handshake timeout") ||
		strings.Contains(msg, "connect: connection refused")
}

// waitLimiter blocks until the provider's per-provider rate limiter allows
// one request.
func (p *provider) waitLimiter(ctx context.Context) error {
	if p.limiter == nil {
		return nil
	}
	return p.limiter.Wait(ctx)
}

// ---------------------------------------------------------------------------
// rpc.Client implementation — each method picks a provider, delegates, and
// records the outcome.
// ---------------------------------------------------------------------------

func (fc *FailoverClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""

	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return GetEventsResponse{}, err
	}

	// Detect provider switch mid-pagination: if the request has a cursor
	// and we're about to use a different provider than the one that set
	// the cursor, signal the caller to re-anchor from ledger position.
	if hasCursor && fc.lastActive.Load() != -1 && fc.lastActive.Load() != int32(idx) {
		metrics.RPCFailoversTotal.WithLabelValues("reanchor").Inc()
		fc.log.Warn("provider switch with active cursor — re-anchor required",
			"new_provider", p.url)
		return GetEventsResponse{}, ErrFailoverReanchor
	}

	if err := p.waitLimiter(ctx); err != nil {
		return GetEventsResponse{}, err
	}
	resp, err := p.client.GetEvents(ctx, req)
	if err != nil {
		fc.recordError(idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

func (fc *FailoverClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return LatestLedger{}, err
	}
	if err := p.waitLimiter(ctx); err != nil {
		return LatestLedger{}, err
	}
	resp, err := p.client.GetLatestLedger(ctx)
	if err != nil {
		fc.recordError(idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

func (fc *FailoverClient) GetHealth(ctx context.Context) (Health, error) {
	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return Health{}, err
	}
	if err := p.waitLimiter(ctx); err != nil {
		return Health{}, err
	}
	resp, err := p.client.GetHealth(ctx)
	if err != nil {
		fc.recordError(idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

func (fc *FailoverClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return GetLedgerEntriesResponse{}, err
	}
	if err := p.waitLimiter(ctx); err != nil {
		return GetLedgerEntriesResponse{}, err
	}
	resp, err := p.client.GetLedgerEntries(ctx, req)
	if err != nil {
		fc.recordError(idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

func (fc *FailoverClient) SimulateTransaction(ctx context.Context, req SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return SimulateTransactionResponse{}, err
	}
	if err := p.waitLimiter(ctx); err != nil {
		return SimulateTransactionResponse{}, err
	}
	resp, err := p.client.SimulateTransaction(ctx, req)
	if err != nil {
		fc.recordError(idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

// Compile-time check.
var _ Client = (*FailoverClient)(nil)

// ---------------------------------------------------------------------------
// Background probing for demoted providers.
// ---------------------------------------------------------------------------

// RunProbes starts a background goroutine that periodically probes
// StateDown providers with getHealth until ctx is done. This should be
// launched by the caller after creating the FailoverClient.
func (fc *FailoverClient) RunProbes(ctx context.Context) {
	go fc.runProbes(ctx)
}

func (fc *FailoverClient) runProbes(ctx context.Context) {
	ticker := time.NewTicker(fc.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fc.probeDownProviders(ctx)
		}
	}
}

func (fc *FailoverClient) probeDownProviders(ctx context.Context) {
	for _, p := range fc.providers {
		if ProviderState(p.state.Load()) != StateDown {
			continue
		}
		if err := p.waitLimiter(ctx); err != nil {
			continue
		}
		_, err := p.client.GetHealth(ctx)
		if err != nil {
			fc.log.Debug("provider probe failed", "url", p.url, "error", err)
			continue
		}
		// Successful probe: advance probation counter.
		if p.okCount.Add(1) >= int32(fc.probationSuccesses) {
			p.okCount.Store(0)
			p.errCount.Store(0)
			fc.setProviderState(p, StateActive)
			fc.log.Info("provider promoted after probe", "url", p.url, "state", "active")
		} else {
			fc.log.Debug("provider probation progress", "url", p.url,
				"successes", p.okCount.Load(), "needed", fc.probationSuccesses)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// NewHTTPClientForFailover creates a concrete HTTPClient for use as a
// failover provider. Exported so tests can supply their own constructor.
func NewHTTPClientForFailover(rawURL string, rps float64) Client {
	// Convert RPS to minimum interval: 1/rps seconds.
	interval := time.Duration(float64(time.Second) / rps)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return NewHTTPClient(rawURL, WithMinRequestInterval(interval))
}
