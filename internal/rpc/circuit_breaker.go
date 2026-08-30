// Package rpc is a minimal JSON-RPC 2.0 client for the Stellar RPC (Soroban)
// methods SoroTrail needs: getEvents, getLatestLedger, getHealth.
package rpc

import (
	"log/slog"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// BreakerState represents the state of a circuit breaker.
type BreakerState int

const (
	// BreakerClosed is the normal state: requests flow through.
	BreakerClosed BreakerState = iota
	// BreakerOpen rejects all requests until the probe timer expires.
	BreakerOpen
	// BreakerHalfOpen allows exactly one probe request to test recovery.
	BreakerHalfOpen
)

// String returns the human-readable name of the breaker state.
func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig holds tunables for a CircuitBreaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures that trips
	// the breaker from closed to open. Must be > 0.
	FailureThreshold int
	// ProbeTimeout is how long the breaker stays open before allowing a
	// single probe request (half-open state). Must be > 0.
	ProbeTimeout time.Duration
}

// CircuitBreaker tracks consecutive RPC failures and stops hammering the
// endpoint when it is clearly unhealthy. After ProbeTimeout it transitions
// to half-open and allows one probe request; success closes the breaker,
// failure reopens it.
//
// A nil *CircuitBreaker is a pass-through (always allows requests).
type CircuitBreaker struct {
	mu     sync.Mutex
	config CircuitBreakerConfig
	log    *slog.Logger

	state           BreakerState
	consecutiveFail int
	lastOpenTime    time.Time
}

// NewCircuitBreaker creates a circuit breaker with the given config. A nil
// logger defaults to slog.Default().
func NewCircuitBreaker(config CircuitBreakerConfig, log *slog.Logger) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.ProbeTimeout <= 0 {
		config.ProbeTimeout = 30 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	cb := &CircuitBreaker{
		config: config,
		log:    log,
		state:  BreakerClosed,
	}
	// Seed the gauge so a freshly-constructed breaker is immediately
	// visible as closed before the first request decides anything.
	metrics.RPCCircuitBreakerState.WithLabelValues(BreakerClosed.String()).Set(1)
	return cb
}

// Allow reports whether a request should be attempted. When the breaker is
// open and the probe timeout has elapsed it transitions to half-open and
// returns true (allowing the probe). When half-open it returns true for
// exactly one caller (the probe). Otherwise it returns false.
//
// A nil *CircuitBreaker always returns true.
func (cb *CircuitBreaker) Allow() bool {
	if cb == nil {
		return true
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if time.Since(cb.lastOpenTime) >= cb.config.ProbeTimeout {
			cb.setState(BreakerHalfOpen)
			cb.log.Warn("circuit breaker half-open: allowing probe request",
				"consecutive_failures", cb.consecutiveFail)
			return true
		}
		return false
	case BreakerHalfOpen:
		// Only one probe allowed; subsequent callers are rejected until
		// the probe completes.
		return false
	default:
		return true
	}
}

// RecordSuccess records a successful RPC call. If the breaker is in
// half-open state this closes it (recovery confirmed).
//
// A nil *CircuitBreaker is a no-op.
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == BreakerHalfOpen {
		cb.log.Info("circuit breaker closed: RPC recovered",
			"previous_failures", cb.consecutiveFail)
	}
	cb.consecutiveFail = 0
	cb.setState(BreakerClosed)
}

// RecordFailure records a failed RPC call. If consecutive failures reach
// the threshold the breaker opens. If the breaker is half-open it reopens
// (probe failed).
//
// A nil *CircuitBreaker is a no-op.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFail++
	if cb.state == BreakerHalfOpen {
		cb.log.Error("circuit breaker reopened: probe request failed",
			"consecutive_failures", cb.consecutiveFail)
		cb.setState(BreakerOpen)
		return
	}
	if cb.consecutiveFail >= cb.config.FailureThreshold {
		cb.log.Error("circuit breaker opened: too many consecutive failures",
			"consecutive_failures", cb.consecutiveFail,
			"threshold", cb.config.FailureThreshold,
			"probe_timeout", cb.config.ProbeTimeout)
		cb.setState(BreakerOpen)
	}
}

// State returns the current breaker state.
//
// A nil *CircuitBreaker returns BreakerClosed.
func (cb *CircuitBreaker) State() BreakerState {
	if cb == nil {
		return BreakerClosed
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// ConsecutiveFailures returns the current count of consecutive failures.
//
// A nil *CircuitBreaker returns 0.
func (cb *CircuitBreaker) ConsecutiveFailures() int {
	if cb == nil {
		return 0
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFail
}

// BreakerStats exposes breaker state for metrics and logging.
type BreakerStats struct {
	State               BreakerState
	ConsecutiveFailures int
	FailureThreshold    int
	ProbeTimeout        time.Duration
}

// Stats returns a snapshot of the breaker state for observability.
//
// A nil *CircuitBreaker returns zero-value stats.
func (cb *CircuitBreaker) Stats() BreakerStats {
	if cb == nil {
		return BreakerStats{}
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return BreakerStats{
		State:               cb.state,
		ConsecutiveFailures: cb.consecutiveFail,
		FailureThreshold:    cb.config.FailureThreshold,
		ProbeTimeout:        cb.config.ProbeTimeout,
	}
}

// setState transitions the breaker and records when open began. Every
// transition also republishes the state gauge (1 on the new state, 0 on
// the old), so /metrics always reflects the live breaker.
func (cb *CircuitBreaker) setState(s BreakerState) {
	if cb.state != s {
		old := cb.state
		cb.state = s
		if s == BreakerOpen {
			cb.lastOpenTime = time.Now()
		}
		metrics.RPCCircuitBreakerState.WithLabelValues(old.String()).Set(0)
		metrics.RPCCircuitBreakerState.WithLabelValues(s.String()).Set(1)
		cb.log.Debug("circuit breaker state transition",
			"from", old, "to", s,
			"consecutive_failures", cb.consecutiveFail)
	}
}
