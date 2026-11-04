package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/requestid"
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

// Correctness-gate sentinel errors returned when no candidate provider can
// safely serve the request without introducing gaps or cross-network corruption.
var (
	// ErrProviderBehindCursor is returned when every candidate provider has a
	// LatestLedger behind the current ingestion cursor or requested ledger.
	ErrProviderBehindCursor = errors.New("rpc: provider latest ledger is behind cursor")

	// ErrProviderRetentionGap is returned when a provider's retention window
	// starts after the requested ledger and no fallback provider covers the gap.
	ErrProviderRetentionGap = errors.New("rpc: provider retention window does not cover requested ledger")

	// ErrNetworkPassphraseMismatch is returned when a provider reports a
	// Stellar network passphrase that does not match the configured network.
	ErrNetworkPassphraseMismatch = errors.New("rpc: provider network passphrase mismatch")

	// ErrProviderDivergence is returned when a candidate provider returns
	// event data that diverges from canonical events already observed for
	// the same ledger range.
	ErrProviderDivergence = errors.New("rpc: provider returned divergent event data")
)

// Canonical Stellar network passphrases for WithFailoverExpectedNetwork.
const (
	MainnetNetworkPassphrase   = "Public Global Stellar Network ; September 2015"
	TestnetNetworkPassphrase   = "Test SDF Network ; September 2015"
	FuturenetNetworkPassphrase = "Test SDF Future Network ; October 2022"
)

// Bounded Prometheus reason labels for sorotrail_rpc_provider_demotions_total.
const (
	DemoteReasonConsecutiveErrors = "consecutive_errors"
	DemoteReasonBehindCursor      = "behind_cursor"
	DemoteReasonRetentionGap      = "retention_gap"
	DemoteReasonNetworkMismatch   = "network_mismatch"
	DemoteReasonDivergence        = "divergence"
)

// RetentionGapError is returned when a candidate provider's OldestLedger is
// strictly greater than the requested StartLedger (hiding a retention gap as
// an empty or truncated page) and no fallback provider can cover the gap. It
// unwraps to both ErrProviderRetentionGap and an *Error that satisfies
// IsLedgerOutOfRange so callers re-clamp rather than silently skipping ledgers.
type RetentionGapError struct {
	ProviderURL     string
	RequestedLedger uint32
	OldestLedger    uint32
}

func (e *RetentionGapError) Error() string {
	return fmt.Sprintf("rpc: provider %s retention window starts at ledger %d, outside of retention window for requested ledger %d",
		e.ProviderURL, e.OldestLedger, e.RequestedLedger)
}

func (e *RetentionGapError) Unwrap() []error {
	return []error{
		ErrProviderRetentionGap,
		&Error{
			Code: -32602,
			Message: fmt.Sprintf("startLedger %d is outside of retention window (must be within [%d, ...])",
				e.RequestedLedger, e.OldestLedger),
		},
	}
}

// ProviderDivergence records a cross-provider data discrepancy detected when
// a secondary provider returns different event data for a ledger range that
// was already observed from another provider.
type ProviderDivergence struct {
	ProviderURL string
	Ledger      uint32
	EventID     string
	Description string
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

// provider wraps one concrete rpc.Client with health and correctness tracking.
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

	// Per-provider ledger bounds learned from getHealth, getLatestLedger,
	// and getEvents responses.
	latestLedger atomic.Uint32
	oldestLedger atomic.Uint32

	// Per-provider network passphrase verification state.
	networkVerified atomic.Bool
	networkInvalid  atomic.Bool

	// Per-provider XDR format capability tracking (true once this specific
	// provider rejects xdrFormat: "json").
	xdrJSONUnsupported atomic.Bool
}

type observedEvent struct {
	providerIdx int
	id          string
	ledger      uint32
	contractID  string
	eventType   string
	txHash      string
	topicDigest string
	value       string
	topicJSON   string
	valueJSON   string
}

type observedLedgerPrefix struct {
	providerIdx int
	eventIDs    []string
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
// Correctness invariants enforced on provider selection and responses:
//   - Network passphrase verification: when configured via
//     [WithFailoverExpectedNetworkPassphrase] or [WithFailoverExpectedNetwork],
//     each provider's getNetwork passphrase is checked before traffic is
//     routed to it; mismatched providers are permanently rejected.
//   - Cursor height monotonicity: a candidate provider whose LatestLedger is
//     behind the current cursor (or lags the highest observed head beyond
//     headSkewTolerance) is demoted and skipped on the selection path.
//   - Retention-window gap protection: a provider whose OldestLedger exceeds
//     the requested StartLedger is demoted and skipped rather than returning
//     an empty page that would silently advance the cursor past unread data.
//   - Per-provider XDR-format fallback: xdrFormat="json" rejection is tracked
//     per provider so one legacy provider never downgrades sibling providers.
//   - Cross-provider divergence detection: overlapping ledger ranges re-served
//     after failover are checked against already-observed event digests.
type FailoverClient struct {
	providers []*provider
	log       *slog.Logger

	maxConsecutiveErrors int           // demote after this many (default 3)
	probationSuccesses   int           // promote after this many (default 2)
	probeInterval        time.Duration // getHealth interval for StateDown (default 30s)
	headSkewTolerance    uint32        // ignore small chain-head differences (default 3)
	expectedPassphrase   string        // expected Stellar network passphrase

	// Which provider handled the last successful call (index). Used to
	// detect provider switches so we can signal cursor invalidation.
	lastActive atomic.Int32 // provider index, or -1 if never succeeded

	// Highest ledger reached by the event cursor stream and highest chain
	// head observed across providers.
	cursorLedger    atomic.Uint32
	maxObservedHead atomic.Uint32
	reanchorLedger  atomic.Uint32

	// All-down backoff state.
	allDownCount atomic.Int32 // consecutive all-down episodes for exponential backoff
	allDownUntil atomic.Int64 // unix nanos; zero when not in backoff

	// Per-provider rate limit RPS for constructing child clients.
	rateLimitRPS float64

	// Cross-provider event fingerprint history for divergence detection.
	divMu         sync.Mutex
	observedByID  map[string]observedEvent
	observedByLgr map[uint32]observedLedgerPrefix
	divergences   []ProviderDivergence
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

// WithFailoverExpectedNetworkPassphrase requires every provider to report the
// given Stellar network passphrase via getNetwork before it can be selected
// or promoted.
func WithFailoverExpectedNetworkPassphrase(passphrase string) FailoverOption {
	return func(fc *FailoverClient) {
		fc.expectedPassphrase = strings.TrimSpace(passphrase)
	}
}

// WithFailoverExpectedNetwork resolves a logical network name ("mainnet",
// "pubnet", "testnet", "futurenet") or raw passphrase string and configures
// passphrase verification across all providers.
func WithFailoverExpectedNetwork(network string) FailoverOption {
	return func(fc *FailoverClient) {
		fc.expectedPassphrase = resolveExpectedPassphrase(network)
	}
}

func resolveExpectedPassphrase(network string) string {
	trimmed := strings.TrimSpace(network)
	switch strings.ToLower(trimmed) {
	case "mainnet", "pubnet", "public":
		return MainnetNetworkPassphrase
	case "testnet", "test":
		return TestnetNetworkPassphrase
	case "futurenet":
		return FuturenetNetworkPassphrase
	default:
		return trimmed
	}
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
		observedByID:         make(map[string]observedEvent),
		observedByLgr:        make(map[uint32]observedLedgerPrefix),
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

// demoteProvider transitions p to targetState, resets probation okCount, and
// increments sorotrail_rpc_provider_demotions_total with the bounded reason.
func (fc *FailoverClient) demoteProvider(p *provider, targetState ProviderState, reason string) {
	p.okCount.Store(0)
	prev := ProviderState(p.state.Load())
	if prev == targetState && targetState == StateDegraded {
		targetState = StateDown
	}
	fc.setProviderState(p, targetState)
	metrics.RPCProviderDemotionsTotal.WithLabelValues(p.label, reason).Inc()
}

// promoteProvider transitions p back to StateActive and increments
// sorotrail_rpc_provider_promotions_total.
func (fc *FailoverClient) promoteProvider(p *provider) {
	p.okCount.Store(0)
	p.errCount.Store(0)
	fc.setProviderState(p, StateActive)
	metrics.RPCProviderPromotionsTotal.WithLabelValues(p.label).Inc()
}

// SetCursorLedger updates the FailoverClient's tracked cursor ledger height.
// It never regresses the stored cursor height unless the current height is 0.
func (fc *FailoverClient) SetCursorLedger(ledger uint32) {
	for {
		cur := fc.cursorLedger.Load()
		if ledger <= cur {
			return
		}
		if fc.cursorLedger.CompareAndSwap(cur, ledger) {
			return
		}
	}
}

// CursorLedger returns the highest ledger reached by the event cursor stream.
func (fc *FailoverClient) CursorLedger() uint32 {
	return fc.cursorLedger.Load()
}

// SetProviderLedgerRange records a known [oldestLedger, latestLedger] range
// for provider idx.
func (fc *FailoverClient) SetProviderLedgerRange(idx int, oldest, latest uint32) {
	if idx < 0 || idx >= len(fc.providers) {
		return
	}
	fc.updateProviderRange(fc.providers[idx], oldest, latest)
}

// ProviderSupportsXDRJSON reports whether provider idx currently sends
// xdrFormat: "json" on GetEvents requests.
func (fc *FailoverClient) ProviderSupportsXDRJSON(idx int) bool {
	if idx < 0 || idx >= len(fc.providers) {
		return false
	}
	p := fc.providers[idx]
	if p.xdrJSONUnsupported.Load() {
		return false
	}
	if hc, ok := p.client.(*HTTPClient); ok {
		return hc.SupportsXDRJSON()
	}
	return true
}

// Divergences returns a snapshot of all cross-provider event divergences
// recorded by this FailoverClient.
func (fc *FailoverClient) Divergences() []ProviderDivergence {
	fc.divMu.Lock()
	defer fc.divMu.Unlock()
	out := make([]ProviderDivergence, len(fc.divergences))
	copy(out, fc.divergences)
	return out
}

func (fc *FailoverClient) updateProviderRange(p *provider, oldest, latest uint32) {
	if oldest > 0 {
		p.oldestLedger.Store(oldest)
	}
	if latest > 0 {
		p.latestLedger.Store(latest)
		for {
			curMax := fc.maxObservedHead.Load()
			if latest <= curMax {
				break
			}
			if fc.maxObservedHead.CompareAndSwap(curMax, latest) {
				break
			}
		}
	}
}

// getActive returns the highest-priority active provider, or -1 if none.
// Degraded providers are only returned when no active provider exists.
func (fc *FailoverClient) getActive() int {
	return fc.getActiveExcluding(nil)
}

func (fc *FailoverClient) getActiveExcluding(excluded map[int]bool) int {
	// Prefer active providers first.
	for i, p := range fc.providers {
		if excluded != nil && excluded[i] {
			continue
		}
		if p.networkInvalid.Load() {
			continue
		}
		if ProviderState(p.state.Load()) == StateActive {
			return i
		}
	}
	// Fall back to degraded providers.
	for i, p := range fc.providers {
		if excluded != nil && excluded[i] {
			continue
		}
		if p.networkInvalid.Load() {
			continue
		}
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

// verifyProviderNetwork validates p's network passphrase via getNetwork when
// expectedPassphrase is set. A provider with a mismatched passphrase is
// permanently marked networkInvalid and demoted to StateDown.
func (fc *FailoverClient) verifyProviderNetwork(ctx context.Context, idx int) error {
	p := fc.providers[idx]
	if p.networkInvalid.Load() {
		return ErrNetworkPassphraseMismatch
	}
	if fc.expectedPassphrase == "" || p.networkVerified.Load() {
		return nil
	}
	inspector, ok := p.client.(NetworkInspector)
	if !ok {
		p.networkVerified.Store(true)
		return nil
	}
	if err := p.waitLimiter(ctx); err != nil {
		return err
	}
	info, err := inspector.GetNetwork(ctx)
	if err != nil {
		fc.recordError(ctx, idx, err)
		return err
	}
	actual := strings.TrimSpace(info.Passphrase)
	if actual != fc.expectedPassphrase {
		p.networkInvalid.Store(true)
		fc.demoteProvider(p, StateDown, DemoteReasonNetworkMismatch)
		fc.log.Error("provider network passphrase mismatch — rejecting provider",
			append(requestid.Attrs(ctx),
				"url", p.url,
				"expected_passphrase", fc.expectedPassphrase,
				"actual_passphrase", actual,
				"state", "down",
			)...)
		return fmt.Errorf("%w: provider %s reported %q, want %q",
			ErrNetworkPassphraseMismatch, p.url, actual, fc.expectedPassphrase)
	}
	p.networkVerified.Store(true)
	return nil
}

// pickProvider selects the provider for the next call, respecting priority
// order, network passphrase verification, and health state. It handles the
// all-down backoff case.
func (fc *FailoverClient) pickProvider(ctx context.Context) (*provider, int, error) {
	return fc.pickProviderExcluding(ctx, nil)
}

func (fc *FailoverClient) pickProviderExcluding(ctx context.Context, excluded map[int]bool) (*provider, int, error) {
	// Check all-down backoff.
	if until := fc.allDownUntil.Load(); until != 0 {
		if d := time.Until(time.Unix(0, until)); d > 0 {
			return nil, -1, ErrAllProvidersDown
		}
		fc.allDownUntil.Store(0)
	}

	var lastMismatchErr error
	for {
		idx := fc.getActiveExcluding(excluded)
		if idx < 0 {
			break
		}
		p := fc.providers[idx]
		if err := fc.verifyProviderNetwork(ctx, idx); err != nil {
			if errors.Is(err, ErrNetworkPassphraseMismatch) {
				lastMismatchErr = err
				if excluded == nil {
					excluded = make(map[int]bool)
				}
				excluded[idx] = true
				continue
			}
			return nil, -1, err
		}
		return p, idx, nil
	}

	if len(excluded) > 0 && fc.getActiveExcluding(nil) >= 0 {
		// Some providers are still active/degraded globally, just excluded for this call.
		if lastMismatchErr != nil {
			return nil, -1, lastMismatchErr
		}
		return nil, -1, ErrAllProvidersDown
	}

	// No active or degraded providers — all are StateDown. Set
	// exponential backoff (capped at 30s) with jitter.
	fc.allDownCount.Add(1)
	fc.allDownUntil.Store(time.Now().Add(fc.allDownBackoff()).UnixNano())
	fc.log.Error("all RPC providers are down",
		append(requestid.Attrs(ctx), "backoff", fc.allDownBackoff())...)
	if lastMismatchErr != nil {
		return nil, -1, fmt.Errorf("%w (%w)", ErrAllProvidersDown, lastMismatchErr)
	}
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
	if p.networkInvalid.Load() {
		return
	}
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
			fc.promoteProvider(p)
			fc.log.Info("provider promoted", "url", p.url, "state", "active")
		}
	}
}

func (fc *FailoverClient) recordError(ctx context.Context, idx int, err error) {
	p := fc.providers[idx]

	// Semantic errors do NOT demote on consecutive-error counting.
	// IsLedgerOutOfRange means the provider is healthy at the transport layer
	// but our query is outside its retention window.
	if IsLedgerOutOfRange(err) {
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

	// A demotion is an RPC failure, so the line carries the correlation id
	// of whichever request or job drove the call.
	if oldState == StateActive && c >= int32(fc.maxConsecutiveErrors) {
		fc.demoteProvider(p, StateDegraded, DemoteReasonConsecutiveErrors)
		fc.log.Warn("provider degraded",
			append(requestid.Attrs(ctx), "url", p.url, "consecutive_errors", c, "reason", DemoteReasonConsecutiveErrors, "state", "degraded")...)
	} else if oldState == StateDegraded && c >= int32(fc.maxConsecutiveErrors*2) {
		fc.demoteProvider(p, StateDown, DemoteReasonConsecutiveErrors)
		fc.log.Error("provider down",
			append(requestid.Attrs(ctx), "url", p.url, "consecutive_errors", c, "reason", DemoteReasonConsecutiveErrors, "state", "down")...)
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

func parseLedgerFromCursor(cursor string) uint32 {
	if cursor == "" {
		return 0
	}
	dash := strings.IndexByte(cursor, '-')
	if dash <= 0 {
		return 0
	}
	n, err := strconv.ParseUint(cursor[:dash], 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

func (fc *FailoverClient) requiredLedgerForRequest(req GetEventsRequest) uint32 {
	reqLedger := fc.cursorLedger.Load()
	if req.StartLedger > reqLedger {
		reqLedger = req.StartLedger
	}
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		if cLedger := parseLedgerFromCursor(req.Pagination.Cursor); cLedger > reqLedger {
			reqLedger = cLedger
		}
	}
	return reqLedger
}

func (fc *FailoverClient) hasCandidateWithHeadAtLeast(excluded map[int]bool, selfIdx int, minHead uint32) bool {
	for i, p := range fc.providers {
		if i == selfIdx || (excluded != nil && excluded[i]) || p.networkInvalid.Load() {
			continue
		}
		if ProviderState(p.state.Load()) == StateDown {
			continue
		}
		latest := p.latestLedger.Load()
		if latest == 0 || latest >= minHead {
			return true
		}
	}
	return false
}

func (fc *FailoverClient) callProviderGetEvents(ctx context.Context, p *provider, req GetEventsRequest) (GetEventsResponse, error) {
	callReq := req
	if p.xdrJSONUnsupported.Load() {
		callReq.XDRFormat = ""
	} else if callReq.XDRFormat == "" {
		callReq.XDRFormat = XDRFormatJSON
	}

	resp, err := p.client.GetEvents(ctx, callReq)
	if err != nil && isXDRFormatRejected(err) {
		p.xdrJSONUnsupported.Store(true)
		callReq.XDRFormat = ""
		resp, err = p.client.GetEvents(ctx, callReq)
	}
	if hc, ok := p.client.(*HTTPClient); ok && !hc.SupportsXDRJSON() {
		p.xdrJSONUnsupported.Store(true)
	}
	return resp, err
}

func (fc *FailoverClient) checkAndRecordEvents(providerIdx int, startLedger uint32, hasCursor bool, resp GetEventsResponse) (*ProviderDivergence, bool) {
	if len(resp.Events) == 0 {
		return nil, true
	}

	fc.divMu.Lock()
	defer fc.divMu.Unlock()

	pURL := fc.providers[providerIdx].url

	// 1. Check per-event content digest against any previously observed event
	// with the same ID from a different provider.
	for _, ev := range resp.Events {
		obs := buildObservedEvent(providerIdx, ev)
		if prev, exists := fc.observedByID[ev.ID]; exists && prev.providerIdx != providerIdx {
			if reason := compareObservedEvents(prev, obs); reason != "" {
				div := ProviderDivergence{
					ProviderURL: pURL,
					Ledger:      ev.Ledger,
					EventID:     ev.ID,
					Description: reason,
				}
				fc.divergences = append(fc.divergences, div)
				return &div, false
			}
		}
	}

	// 2. When re-scanning from a startLedger (no cursor), check that any ledger
	// already observed from another provider produces the identical event ID
	// prefix in the same order.
	if !hasCursor {
		byLedger := make(map[uint32][]string)
		for _, ev := range resp.Events {
			byLedger[ev.Ledger] = append(byLedger[ev.Ledger], ev.ID)
		}
		maxLedgerInPage := resp.Events[len(resp.Events)-1].Ledger
		for ledger, gotIDs := range byLedger {
			if startLedger > 0 && ledger < startLedger {
				continue
			}
			prev, exists := fc.observedByLgr[ledger]
			if !exists || prev.providerIdx == providerIdx || len(prev.eventIDs) == 0 {
				continue
			}
			common := min(len(gotIDs), len(prev.eventIDs))
			for i := range common {
				if gotIDs[i] != prev.eventIDs[i] {
					div := ProviderDivergence{
						ProviderURL: pURL,
						Ledger:      ledger,
						EventID:     gotIDs[i],
						Description: fmt.Sprintf("ledger %d event index %d ID mismatch: got %q, want %q", ledger, i, gotIDs[i], prev.eventIDs[i]),
					}
					fc.divergences = append(fc.divergences, div)
					return &div, false
				}
			}
			// If this response already moved past `ledger` (or returned no
			// continuation cursor) with fewer events at `ledger` than were
			// previously observed, the provider omitted events at `ledger`.
			if len(gotIDs) < len(prev.eventIDs) && (maxLedgerInPage > ledger || resp.Cursor == "") {
				missingID := prev.eventIDs[len(gotIDs)]
				div := ProviderDivergence{
					ProviderURL: pURL,
					Ledger:      ledger,
					EventID:     missingID,
					Description: fmt.Sprintf("ledger %d missing previously observed event %q (got %d events, want >= %d)", ledger, missingID, len(gotIDs), len(prev.eventIDs)),
				}
				fc.divergences = append(fc.divergences, div)
				return &div, false
			}
		}
	}

	// All checks passed — record observed events and per-ledger ordered IDs.
	byLedger := make(map[uint32][]string)
	for _, ev := range resp.Events {
		fc.observedByID[ev.ID] = buildObservedEvent(providerIdx, ev)
		byLedger[ev.Ledger] = append(byLedger[ev.Ledger], ev.ID)
	}
	for ledger, ids := range byLedger {
		existing, ok := fc.observedByLgr[ledger]
		if !ok {
			fc.observedByLgr[ledger] = observedLedgerPrefix{
				providerIdx: providerIdx,
				eventIDs:    ids,
			}
			continue
		}
		// Append any newly discovered event IDs at the end of this ledger.
		seen := make(map[string]bool, len(existing.eventIDs))
		for _, id := range existing.eventIDs {
			seen[id] = true
		}
		for _, id := range ids {
			if !seen[id] {
				existing.eventIDs = append(existing.eventIDs, id)
				seen[id] = true
			}
		}
		fc.observedByLgr[ledger] = existing
	}

	return nil, true
}

func buildObservedEvent(providerIdx int, ev Event) observedEvent {
	var topicDigest string
	if len(ev.Topic) > 0 {
		h := sha256.New()
		for _, t := range ev.Topic {
			_, _ = io.WriteString(h, t)
			_, _ = io.WriteString(h, "|")
		}
		topicDigest = hex.EncodeToString(h.Sum(nil))
	}
	var topicJSON string
	if len(ev.TopicJSON) > 0 {
		b, _ := json.Marshal(ev.TopicJSON)
		topicJSON = string(b)
	}
	return observedEvent{
		providerIdx: providerIdx,
		id:          ev.ID,
		ledger:      ev.Ledger,
		contractID:  ev.ContractID,
		eventType:   ev.Type,
		txHash:      ev.TxHash,
		topicDigest: topicDigest,
		value:       ev.Value,
		topicJSON:   topicJSON,
		valueJSON:   string(ev.ValueJSON),
	}
}

func compareObservedEvents(prev, cur observedEvent) string {
	if prev.ledger != cur.ledger {
		return fmt.Sprintf("event %s ledger mismatch: got %d, want %d", cur.id, cur.ledger, prev.ledger)
	}
	if prev.contractID != cur.contractID {
		return fmt.Sprintf("event %s contractId mismatch: got %q, want %q", cur.id, cur.contractID, prev.contractID)
	}
	if prev.eventType != cur.eventType {
		return fmt.Sprintf("event %s type mismatch: got %q, want %q", cur.id, cur.eventType, prev.eventType)
	}
	if prev.txHash != cur.txHash {
		return fmt.Sprintf("event %s txHash mismatch: got %q, want %q", cur.id, cur.txHash, prev.txHash)
	}
	if prev.value != "" && cur.value != "" && prev.value != cur.value {
		return fmt.Sprintf("event %s value XDR mismatch: got %q, want %q", cur.id, cur.value, prev.value)
	}
	if prev.topicDigest != "" && cur.topicDigest != "" && prev.topicDigest != cur.topicDigest {
		return fmt.Sprintf("event %s topic XDR mismatch", cur.id)
	}
	if prev.valueJSON != "" && cur.valueJSON != "" && prev.valueJSON != cur.valueJSON {
		return fmt.Sprintf("event %s valueJson mismatch: got %s, want %s", cur.id, cur.valueJSON, prev.valueJSON)
	}
	if prev.topicJSON != "" && cur.topicJSON != "" && prev.topicJSON != cur.topicJSON {
		return fmt.Sprintf("event %s topicJson mismatch: got %s, want %s", cur.id, cur.topicJSON, prev.topicJSON)
	}
	return ""
}

// ---------------------------------------------------------------------------
// rpc.Client implementation — each method picks a provider, delegates, and
// records the outcome.
// ---------------------------------------------------------------------------

func (fc *FailoverClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""

	// If a prior call returned ErrFailoverReanchor mid-ledger and the caller
	// discarded its cursor (causing resolvePosition to request
	// LastIngestedLedger+1), re-anchor StartLedger back to the interrupted
	// ledger so remaining events in that ledger are not skipped.
	if !hasCursor {
		if reanchor := fc.reanchorLedger.Load(); reanchor > 0 && req.StartLedger == reanchor+1 {
			req.StartLedger = reanchor
		}
	}

	requiredLedger := fc.requiredLedgerForRequest(req)
	excluded := make(map[int]bool)

	var (
		lastBehindErr       error
		lastRetentionGapErr error
		lastDivergenceErr   error
	)

	for {
		p, idx, err := fc.pickProviderExcluding(ctx, excluded)
		if err != nil {
			if lastRetentionGapErr != nil {
				return GetEventsResponse{}, lastRetentionGapErr
			}
			if lastBehindErr != nil {
				return GetEventsResponse{}, lastBehindErr
			}
			if lastDivergenceErr != nil {
				return GetEventsResponse{}, lastDivergenceErr
			}
			return GetEventsResponse{}, err
		}

		// Selection-path check 1: provider LatestLedger must not be behind
		// the current cursor/request ledger, nor lag the highest observed
		// chain head beyond headSkewTolerance when a fresher provider exists.
		pLatest := p.latestLedger.Load()
		if requiredLedger > 0 && pLatest > 0 && pLatest < requiredLedger {
			fc.demoteProvider(p, StateDegraded, DemoteReasonBehindCursor)
			fc.log.Warn("provider behind cursor on selection path — skipping",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"provider_latest_ledger", pLatest,
					"required_ledger", requiredLedger,
					"reason", DemoteReasonBehindCursor,
				)...)
			lastBehindErr = fmt.Errorf("%w: provider %s latestLedger %d < cursor %d",
				ErrProviderBehindCursor, p.url, pLatest, requiredLedger)
			excluded[idx] = true
			continue
		}
		if maxHead := fc.maxObservedHead.Load(); maxHead > 0 && pLatest > 0 &&
			pLatest+fc.headSkewTolerance < maxHead &&
			fc.hasCandidateWithHeadAtLeast(excluded, idx, maxHead-fc.headSkewTolerance) {
			fc.demoteProvider(p, StateDegraded, DemoteReasonBehindCursor)
			fc.log.Warn("provider lags max observed head beyond tolerance — skipping",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"provider_latest_ledger", pLatest,
					"max_observed_head", maxHead,
					"head_skew_tolerance", fc.headSkewTolerance,
					"reason", DemoteReasonBehindCursor,
				)...)
			lastBehindErr = fmt.Errorf("%w: provider %s latestLedger %d lags head %d",
				ErrProviderBehindCursor, p.url, pLatest, maxHead)
			excluded[idx] = true
			continue
		}

		// Selection-path check 2: provider OldestLedger must not start after
		// the requested StartLedger.
		pOldest := p.oldestLedger.Load()
		if !hasCursor && req.StartLedger > 0 && pOldest > req.StartLedger {
			fc.demoteProvider(p, StateDegraded, DemoteReasonRetentionGap)
			fc.log.Warn("provider retention window does not cover start ledger on selection path — skipping",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"provider_oldest_ledger", pOldest,
					"start_ledger", req.StartLedger,
					"reason", DemoteReasonRetentionGap,
				)...)
			lastRetentionGapErr = &RetentionGapError{
				ProviderURL:     p.url,
				RequestedLedger: req.StartLedger,
				OldestLedger:    pOldest,
			}
			excluded[idx] = true
			continue
		}

		// Detect provider switch mid-pagination: if the request has a cursor
		// and we're about to use a different provider than the one that set
		// the cursor, signal the caller to re-anchor from ledger position.
		if hasCursor && fc.lastActive.Load() != -1 && fc.lastActive.Load() != int32(idx) {
			if cur := fc.cursorLedger.Load(); cur > 0 {
				fc.reanchorLedger.Store(cur)
			}
			metrics.RPCFailoversTotal.WithLabelValues("reanchor").Inc()
			fc.log.Warn("provider switch with active cursor — re-anchor required",
				"new_provider", p.url)
			return GetEventsResponse{}, ErrFailoverReanchor
		}

		if err := p.waitLimiter(ctx); err != nil {
			return GetEventsResponse{}, err
		}
		resp, err := fc.callProviderGetEvents(ctx, p, req)
		if err != nil {
			if IsLedgerOutOfRange(err) {
				if len(fc.providers) == 1 {
					fc.recordError(ctx, idx, err)
					return resp, err
				}
				fc.demoteProvider(p, StateDegraded, DemoteReasonRetentionGap)
				fc.log.Warn("provider returned ledger out of range — failing over",
					append(requestid.Attrs(ctx),
						"url", p.url,
						"start_ledger", req.StartLedger,
						"reason", DemoteReasonRetentionGap,
						"error", err,
					)...)
				lastRetentionGapErr = err
				excluded[idx] = true
				continue
			}
			fc.recordError(ctx, idx, err)
			return resp, err
		}

		fc.updateProviderRange(p, resp.OldestLedger, resp.LatestLedger)

		// Response-path check 1: provider LatestLedger must not be behind
		// the cursor / required ledger. Never advance the cursor onto a
		// behind provider.
		if requiredLedger > 0 && resp.LatestLedger > 0 && resp.LatestLedger < requiredLedger {
			fc.demoteProvider(p, StateDegraded, DemoteReasonBehindCursor)
			fc.log.Warn("provider response latestLedger is behind cursor — skipping",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"provider_latest_ledger", resp.LatestLedger,
					"required_ledger", requiredLedger,
					"reason", DemoteReasonBehindCursor,
				)...)
			lastBehindErr = fmt.Errorf("%w: provider %s latestLedger %d < cursor %d",
				ErrProviderBehindCursor, p.url, resp.LatestLedger, requiredLedger)
			excluded[idx] = true
			continue
		}

		// Response-path check 2: distinguish a retention-window gap
		// (resp.OldestLedger > req.StartLedger) from a legitimate empty page
		// of matching events.
		if !hasCursor && req.StartLedger > 0 && resp.OldestLedger > req.StartLedger {
			fc.demoteProvider(p, StateDegraded, DemoteReasonRetentionGap)
			fc.log.Warn("provider retention gap detected in getEvents response — skipping",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"provider_oldest_ledger", resp.OldestLedger,
					"start_ledger", req.StartLedger,
					"reason", DemoteReasonRetentionGap,
				)...)
			lastRetentionGapErr = &RetentionGapError{
				ProviderURL:     p.url,
				RequestedLedger: req.StartLedger,
				OldestLedger:    resp.OldestLedger,
			}
			excluded[idx] = true
			continue
		}

		// Response-path check 3: cross-provider divergence check on
		// overlapping ledger ranges.
		if div, ok := fc.checkAndRecordEvents(idx, req.StartLedger, hasCursor, resp); !ok {
			fc.demoteProvider(p, StateDown, DemoteReasonDivergence)
			fc.log.Error("provider returned divergent event data — demoting to down",
				append(requestid.Attrs(ctx),
					"url", p.url,
					"ledger", div.Ledger,
					"event_id", div.EventID,
					"description", div.Description,
					"reason", DemoteReasonDivergence,
				)...)
			lastDivergenceErr = fmt.Errorf("%w: %s", ErrProviderDivergence, div.Description)
			excluded[idx] = true
			continue
		}

		// Advance cursorLedger monotonically.
		var maxLedger uint32
		for _, ev := range resp.Events {
			if ev.Ledger > maxLedger {
				maxLedger = ev.Ledger
			}
		}
		if maxLedger == 0 && resp.Cursor != "" {
			maxLedger = parseLedgerFromCursor(resp.Cursor)
		}
		if maxLedger > 0 {
			fc.SetCursorLedger(maxLedger)
		}
		if !hasCursor {
			fc.reanchorLedger.Store(0)
		}

		fc.recordSuccess(idx)
		return resp, nil
	}
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
		fc.recordError(ctx, idx, err)
		return resp, err
	}
	fc.updateProviderRange(p, 0, resp.Sequence)
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
		fc.recordError(ctx, idx, err)
		return resp, err
	}
	fc.updateProviderRange(p, resp.OldestLedger, resp.LatestLedger)
	fc.recordSuccess(idx)
	return resp, nil
}

func (fc *FailoverClient) GetNetwork(ctx context.Context) (NetworkInfo, error) {
	p, idx, err := fc.pickProvider(ctx)
	if err != nil {
		return NetworkInfo{}, err
	}
	inspector, ok := p.client.(NetworkInspector)
	if !ok {
		return NetworkInfo{Passphrase: fc.expectedPassphrase}, nil
	}
	if err := p.waitLimiter(ctx); err != nil {
		return NetworkInfo{}, err
	}
	resp, err := inspector.GetNetwork(ctx)
	if err != nil {
		fc.recordError(ctx, idx, err)
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
		fc.recordError(ctx, idx, err)
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
		fc.recordError(ctx, idx, err)
		return resp, err
	}
	fc.recordSuccess(idx)
	return resp, nil
}

// Compile-time checks.
var (
	_ Client           = (*FailoverClient)(nil)
	_ NetworkInspector = (*FailoverClient)(nil)
)

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
	for idx, p := range fc.providers {
		if ProviderState(p.state.Load()) != StateDown {
			continue
		}
		// Never probe or promote a provider rejected for a network passphrase
		// mismatch.
		if p.networkInvalid.Load() {
			continue
		}
		if err := fc.verifyProviderNetwork(ctx, idx); err != nil {
			continue
		}
		if err := p.waitLimiter(ctx); err != nil {
			continue
		}
		health, err := p.client.GetHealth(ctx)
		if err != nil {
			fc.log.Debug("provider probe failed",
				append(requestid.Attrs(ctx), "url", p.url, "error", err)...)
			continue
		}
		fc.updateProviderRange(p, health.OldestLedger, health.LatestLedger)
		if reqLedger := fc.cursorLedger.Load(); reqLedger > 0 && health.LatestLedger > 0 && health.LatestLedger < reqLedger {
			fc.log.Debug("provider probe still behind cursor",
				"url", p.url, "latest_ledger", health.LatestLedger, "cursor_ledger", reqLedger)
			continue
		}
		// Successful probe: advance probation counter.
		if p.okCount.Add(1) >= int32(fc.probationSuccesses) {
			fc.promoteProvider(p)
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
