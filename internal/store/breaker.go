package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// ErrUnavailable is returned when the store cannot be reached or the
// database circuit breaker is open. Callers can test for it with
// errors.Is(err, ErrUnavailable) or IsUnavailable(err).
var ErrUnavailable = errors.New("database unavailable")

// DegradationReporter is implemented by Store wrappers and backends that can
// report whether the database is currently unreachable or degraded.
type DegradationReporter interface {
	Degraded() bool
}

// IsUnavailable reports whether err represents a database connectivity or
// availability failure (unreachable host, refused or reset connection, broken
// pipe, closed pool, Postgres connection-class 08* / shutdown 57P01..57P03 /
// too-many-connections 53300, or ErrUnavailable).
//
// Caller-side cancellations, request timeouts, and domain lookup errors such
// as ErrNotFound or ErrInvalidCursor return false so normal application
// traffic never trips the circuit breaker.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrInvalidCursor) {
		return false
	}
	if errors.Is(err, ErrUnavailable) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", "53300":
			return true
		}
		return false
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"use of closed network connection",
		"no such host",
		"failed to connect",
		"server closed the connection",
		"conn closed",
		"closed pool",
		"pool unhealthy",
		"database unavailable",
		"terminating connection due to administrator command",
		"the database system is shutting down",
		"the database system is starting up",
		"the database system is in recovery mode",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// DatabaseHealth tracks database reachability and deduplicates outage logs so
// a prolonged database outage logs once on transition into degraded mode and
// once on recovery instead of flooding logs on every query or poll tick.
type DatabaseHealth struct {
	mu         sync.Mutex
	logger     *slog.Logger
	now        func() time.Time
	degraded   bool
	since      time.Time
	lastErr    error
	suppressed uint64
}

// NewDatabaseHealth returns a DatabaseHealth tracker that logs state
// transitions to logger (when non-nil).
func NewDatabaseHealth(logger *slog.Logger) *DatabaseHealth {
	return &DatabaseHealth{
		logger: logger,
		now:    time.Now,
	}
}

// SetLogger updates the logger used for state transition messages.
func (h *DatabaseHealth) SetLogger(logger *slog.Logger) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.logger = logger
	h.mu.Unlock()
}

// Observe records the outcome of a store operation. Availability failures
// transition the tracker into degraded mode and log a single warning; the
// first subsequent nil error clears degraded mode and logs recovery with the
// outage duration and count of suppressed errors.
func (h *DatabaseHealth) Observe(err error) {
	if h == nil {
		return
	}
	if err == nil {
		h.mu.Lock()
		wasDegraded := h.degraded
		since := h.since
		suppressed := h.suppressed
		logger := h.logger
		now := h.now()
		h.degraded = false
		h.since = time.Time{}
		h.lastErr = nil
		h.suppressed = 0
		h.mu.Unlock()

		if wasDegraded && logger != nil {
			outage := max(now.Sub(since), time.Duration(0))
			logger.Info("database reachable again",
				"outage", outage.Round(time.Millisecond),
				"suppressed_errors", suppressed,
			)
		}
		return
	}

	if !IsUnavailable(err) {
		return
	}

	h.mu.Lock()
	alreadyDegraded := h.degraded
	logger := h.logger
	if !alreadyDegraded {
		h.degraded = true
		h.since = h.now()
		h.suppressed = 0
	} else {
		h.suppressed++
	}
	h.lastErr = err
	h.mu.Unlock()

	if !alreadyDegraded && logger != nil {
		logger.Warn("database unavailable; entering degraded mode", "error", err)
	}
}

// Degraded reports whether the database is currently marked unavailable.
func (h *DatabaseHealth) Degraded() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.degraded
}

// LastError returns the most recent availability error observed while degraded.
func (h *DatabaseHealth) LastError() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastErr
}

// SuppressedCount returns the number of duplicate availability errors
// suppressed since the current outage began.
func (h *DatabaseHealth) SuppressedCount() uint64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.suppressed
}

// BreakerState represents the state of a StoreBreaker.
type BreakerState int

const (
	// BreakerClosed is normal operation: requests pass through to the store.
	BreakerClosed BreakerState = iota
	// BreakerOpen short-circuits requests until the cool-down expires.
	BreakerOpen
	// BreakerHalfOpen admits a single probe request after the cool-down
	// expires to test whether the store has recovered.
	BreakerHalfOpen
)

// String returns the Prometheus label and log representation of the state.
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

const (
	defaultBreakerFailureThreshold = 3
	defaultBreakerCoolDown         = 5 * time.Second
	defaultBreakerMaxCoolDown      = 60 * time.Second
)

// DefaultJitter returns a jittered duration in [d/2, d] so recovery probes and
// background workers do not thundering-herd a recovering database.
func DefaultJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}

// BreakerOption configures a StoreBreaker.
type BreakerOption func(*StoreBreaker)

// WithFailureThreshold sets the number of consecutive failures required to
// trip the breaker from closed to open. Values <= 0 are ignored.
func WithFailureThreshold(n int) BreakerOption {
	return func(b *StoreBreaker) {
		if n > 0 {
			b.failureThreshold = n
		}
	}
}

// WithCoolDown sets the initial cool-down duration when the breaker opens.
// Values <= 0 are ignored.
func WithCoolDown(d time.Duration) BreakerOption {
	return func(b *StoreBreaker) {
		if d > 0 {
			b.baseCoolDown = d
			if b.maxCoolDown < d {
				b.maxCoolDown = d
			}
		}
	}
}

// WithMaxCoolDown caps the exponential cool-down growth across consecutive
// failed recovery probes. Values <= 0 are ignored.
func WithMaxCoolDown(d time.Duration) BreakerOption {
	return func(b *StoreBreaker) {
		if d > 0 {
			b.maxCoolDown = d
		}
	}
}

// WithBreakerClock injects a deterministic clock for tests.
func WithBreakerClock(now func() time.Time) BreakerOption {
	return func(b *StoreBreaker) {
		if now != nil {
			b.now = now
		}
	}
}

// WithBreakerJitter sets a custom jitter function applied to the cool-down
// window when the breaker opens or re-opens.
func WithBreakerJitter(jitter func(time.Duration) time.Duration) BreakerOption {
	return func(b *StoreBreaker) {
		b.jitter = jitter
	}
}

// WithBreakerLogger configures a logger that records circuit breaker state
// transitions once per transition.
func WithBreakerLogger(logger *slog.Logger) BreakerOption {
	return func(b *StoreBreaker) {
		b.logger = logger
	}
}

// StoreBreaker is a three-state circuit breaker for database operations.
type StoreBreaker struct {
	mu                  sync.Mutex
	state               BreakerState
	failureThreshold    int
	baseCoolDown        time.Duration
	maxCoolDown         time.Duration
	now                 func() time.Time
	jitter              func(time.Duration) time.Duration
	logger              *slog.Logger
	consecutiveFailures int
	reopenCount         int
	openUntil           time.Time
	probeInFlight       bool
	lastErr             error
}

// NewStoreBreaker constructs a StoreBreaker in the closed state.
func NewStoreBreaker(opts ...BreakerOption) *StoreBreaker {
	b := &StoreBreaker{
		state:            BreakerClosed,
		failureThreshold: defaultBreakerFailureThreshold,
		baseCoolDown:     defaultBreakerCoolDown,
		maxCoolDown:      defaultBreakerMaxCoolDown,
		now:              time.Now,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.maxCoolDown < b.baseCoolDown {
		b.maxCoolDown = b.baseCoolDown
	}
	metrics.SetDBCircuitBreakerState(BreakerClosed.String())
	return b
}

// SetLogger updates the logger used for circuit breaker state transitions.
func (b *StoreBreaker) SetLogger(logger *slog.Logger) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.logger = logger
	b.mu.Unlock()
}

// Allow reports whether a store operation may proceed. When the breaker is
// open and its cool-down has elapsed, Allow transitions the breaker to
// half-open and admits a single probe request while rejecting concurrent
// callers until the probe finishes.
func (b *StoreBreaker) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if b.now().Before(b.openUntil) {
			return false
		}
		b.state = BreakerHalfOpen
		b.probeInFlight = true
		metrics.SetDBCircuitBreakerState(BreakerHalfOpen.String())
		return true
	case BreakerHalfOpen:
		if !b.probeInFlight {
			b.probeInFlight = true
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess records a successful store operation, closing the circuit
// breaker if it was open or half-open.
func (b *StoreBreaker) RecordSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	prev := b.state
	logger := b.logger
	b.state = BreakerClosed
	b.consecutiveFailures = 0
	b.reopenCount = 0
	b.openUntil = time.Time{}
	b.probeInFlight = false
	b.lastErr = nil
	b.mu.Unlock()

	if prev != BreakerClosed {
		metrics.SetDBCircuitBreakerState(BreakerClosed.String())
		if logger != nil {
			logger.Info("database circuit breaker closed", "previous_state", prev.String())
		}
	}
}

// RecordFailure records a failed store operation. Once consecutive failures
// reach the threshold (or immediately if a half-open recovery probe fails),
// the breaker opens for a cool-down window that grows exponentially up to
// maxCoolDown.
func (b *StoreBreaker) RecordFailure(err error) {
	if b == nil || err == nil {
		return
	}
	b.mu.Lock()
	prev := b.state
	logger := b.logger
	b.lastErr = err
	b.consecutiveFailures++
	b.probeInFlight = false

	var opened bool
	var coolDown time.Duration
	switch b.state {
	case BreakerHalfOpen:
		b.reopenCount++
		coolDown = b.nextCoolDownLocked()
		b.state = BreakerOpen
		b.openUntil = b.now().Add(coolDown)
		opened = true
	case BreakerClosed:
		if b.consecutiveFailures >= b.failureThreshold {
			b.reopenCount = 0
			coolDown = b.nextCoolDownLocked()
			b.state = BreakerOpen
			b.openUntil = b.now().Add(coolDown)
			opened = true
		}
	case BreakerOpen:
		// Already open; keep existing cool-down window unless expired.
		if !b.now().Before(b.openUntil) {
			b.reopenCount++
			coolDown = b.nextCoolDownLocked()
			b.openUntil = b.now().Add(coolDown)
		}
	}
	failures := b.consecutiveFailures
	b.mu.Unlock()

	if opened {
		metrics.SetDBCircuitBreakerState(BreakerOpen.String())
		if logger != nil {
			logger.Warn("database circuit breaker opened",
				"previous_state", prev.String(),
				"consecutive_failures", failures,
				"cooldown", coolDown.Round(time.Millisecond),
				"error", err,
			)
		}
	}
}

// Observe inspects err and records either a success (when err == nil) or a
// failure (when IsUnavailable(err) is true). Non-availability application
// errors are ignored so they do not trip the breaker.
func (b *StoreBreaker) Observe(err error) {
	if b == nil {
		return
	}
	if err == nil {
		b.RecordSuccess()
		return
	}
	if IsUnavailable(err) {
		b.RecordFailure(err)
	}
}

func (b *StoreBreaker) nextCoolDownLocked() time.Duration {
	cd := b.baseCoolDown
	for i := 0; i < b.reopenCount; i++ {
		if cd >= b.maxCoolDown/2 {
			cd = b.maxCoolDown
			break
		}
		cd *= 2
	}
	if cd > b.maxCoolDown {
		cd = b.maxCoolDown
	}
	if b.jitter != nil {
		cd = b.jitter(cd)
		if cd <= 0 {
			cd = b.baseCoolDown
		}
		if cd > b.maxCoolDown {
			cd = b.maxCoolDown
		}
	}
	return cd
}

// State returns the current breaker state. If the breaker was open and its
// cool-down window has elapsed, State reports BreakerHalfOpen.
func (b *StoreBreaker) State() BreakerState {
	if b == nil {
		return BreakerClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen && !b.now().Before(b.openUntil) {
		return BreakerHalfOpen
	}
	return b.state
}

// Degraded reports whether the circuit breaker is currently open or half-open.
func (b *StoreBreaker) Degraded() bool {
	return b.State() != BreakerClosed
}

// ConsecutiveFailures returns the current streak of consecutive failures.
func (b *StoreBreaker) ConsecutiveFailures() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutiveFailures
}

// LastError returns the most recent error recorded on the breaker.
func (b *StoreBreaker) LastError() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErr
}

// CoolDownRemaining returns the remaining cool-down time while the breaker is
// open, or 0 otherwise.
func (b *StoreBreaker) CoolDownRemaining() time.Duration {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BreakerOpen {
		return 0
	}
	rem := b.openUntil.Sub(b.now())
	if rem < 0 {
		return 0
	}
	return rem
}

// Reset returns the breaker to its initial closed state.
func (b *StoreBreaker) Reset() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.state = BreakerClosed
	b.consecutiveFailures = 0
	b.reopenCount = 0
	b.openUntil = time.Time{}
	b.probeInFlight = false
	b.lastErr = nil
	b.mu.Unlock()
	metrics.SetDBCircuitBreakerState(BreakerClosed.String())
}
