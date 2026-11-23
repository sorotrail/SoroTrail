package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// ErrStoreUnavailable is returned immediately by a StoreBreaker when the
// database circuit is open, allowing callers to fail fast with a retryable 503
// and a jittered Retry-After rather than queueing until their request timeout.
var ErrStoreUnavailable = errors.New("database is temporarily unavailable")

// StoreUnavailableError wraps ErrStoreUnavailable with a jittered RetryAfter
// duration so HTTP handlers can populate Retry-After without synchronising
// client retries into a thundering herd.
type StoreUnavailableError struct {
	RetryAfter time.Duration
	Cause      error
}

func (e *StoreUnavailableError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", ErrStoreUnavailable.Error(), e.Cause)
	}
	return ErrStoreUnavailable.Error()
}

func (e *StoreUnavailableError) Unwrap() error {
	return ErrStoreUnavailable
}

// BreakerState represents the circuit state of a StoreBreaker.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half-open"
)

// BreakerOptions configures a StoreBreaker.
type BreakerOptions struct {
	// FailureThreshold is the number of consecutive connectivity/store
	// failures that trip the breaker from closed to open. Default 3.
	FailureThreshold int
	// BaseCooldown is the initial cooldown before a half-open probe is
	// allowed. Doubles on each failed probe up to MaxCooldown, with jitter.
	// Default 2s.
	BaseCooldown time.Duration
	// MaxCooldown caps the exponential recovery backoff. Default 30s.
	MaxCooldown time.Duration
	// Now overrides the clock in tests.
	Now func() time.Time
	// Jitter returns a random duration in [0, max). Defaults to math/rand/v2.
	Jitter func(max time.Duration) time.Duration
	// OnStateChange is an optional callback invoked only on state transitions
	// (closed -> open, open -> half-open, half-open -> closed) so logging is
	// bounded to transitions rather than emitted per request.
	OnStateChange func(from, to BreakerState, err error)
}

// StoreBreaker guards a Store with an explicit circuit breaker and jittered
// exponential recovery backoff (#1282).
type StoreBreaker struct {
	mu               sync.Mutex
	state            BreakerState
	consecutiveFails int
	openAttempts     int
	nextProbeAt      time.Time
	currentCooldown  time.Duration
	lastErr          error
	probeInFlight    bool
	opts             BreakerOptions
}

// NewStoreBreaker constructs a StoreBreaker with sensible defaults.
func NewStoreBreaker(opts BreakerOptions) *StoreBreaker {
	if opts.FailureThreshold < 1 {
		opts.FailureThreshold = 3
	}
	if opts.BaseCooldown <= 0 {
		opts.BaseCooldown = 2 * time.Second
	}
	if opts.MaxCooldown <= 0 {
		opts.MaxCooldown = 30 * time.Second
	}
	if opts.MaxCooldown < opts.BaseCooldown {
		opts.MaxCooldown = opts.BaseCooldown
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Jitter == nil {
		opts.Jitter = rand.N[time.Duration]
	}
	b := &StoreBreaker{
		state: BreakerClosed,
		opts:  opts,
	}
	b.publishStateMetricLocked()
	return b
}

// State returns the breaker's current state.
func (b *StoreBreaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow checks whether a store operation may proceed. When the circuit is open
// and the cooldown has not elapsed (or a half-open probe is already in flight),
// it fails immediately with *StoreUnavailableError carrying a jittered
// RetryAfter duration.
func (b *StoreBreaker) Allow() (probe bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.opts.Now()
	switch b.state {
	case BreakerClosed:
		return false, nil

	case BreakerOpen:
		if !now.Before(b.nextProbeAt) && !b.probeInFlight {
			b.transitionLocked(BreakerHalfOpen, b.lastErr)
			b.probeInFlight = true
			return true, nil
		}
		metrics.DBDegradedRejectionsTotal.Inc()
		return false, &StoreUnavailableError{
			RetryAfter: b.jitteredRetryAfterLocked(now),
			Cause:      b.lastErr,
		}

	case BreakerHalfOpen:
		if !b.probeInFlight {
			b.probeInFlight = true
			return true, nil
		}
		metrics.DBDegradedRejectionsTotal.Inc()
		return false, &StoreUnavailableError{
			RetryAfter: b.jitteredRetryAfterLocked(now),
			Cause:      b.lastErr,
		}
	default:
		return false, nil
	}
}

// Record updates the breaker with the outcome of a store operation. Not-found
// and caller validation errors do not count as store availability failures.
func (b *StoreBreaker) Record(probe bool, err error) {
	if isBenignStoreError(err) {
		err = nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if probe {
		b.probeInFlight = false
	}

	if err == nil {
		b.consecutiveFails = 0
		b.openAttempts = 0
		b.currentCooldown = 0
		b.lastErr = nil
		if b.state != BreakerClosed {
			b.transitionLocked(BreakerClosed, nil)
		}
		return
	}

	b.lastErr = err
	b.consecutiveFails++
	if b.state == BreakerHalfOpen || b.consecutiveFails >= b.opts.FailureThreshold {
		b.openAttempts++
		cooldown := b.opts.BaseCooldown
		for i := 1; i < b.openAttempts && cooldown < b.opts.MaxCooldown; i++ {
			cooldown *= 2
			if cooldown > b.opts.MaxCooldown {
				cooldown = b.opts.MaxCooldown
			}
		}
		jitterWindow := cooldown / 2
		if jitterWindow > 0 {
			cooldown = cooldown/2 + b.opts.Jitter(jitterWindow)
		}
		if cooldown < time.Millisecond {
			cooldown = time.Millisecond
		}
		b.currentCooldown = cooldown
		b.nextProbeAt = b.opts.Now().Add(cooldown)
		if b.state != BreakerOpen {
			b.transitionLocked(BreakerOpen, err)
		}
	}
}

func (b *StoreBreaker) transitionLocked(to BreakerState, err error) {
	from := b.state
	b.state = to
	b.publishStateMetricLocked()
	if b.opts.OnStateChange != nil && from != to {
		b.opts.OnStateChange(from, to, err)
	}
}

func (b *StoreBreaker) publishStateMetricLocked() {
	for _, s := range []BreakerState{BreakerClosed, BreakerOpen, BreakerHalfOpen} {
		val := 0.0
		if b.state == s {
			val = 1.0
		}
		metrics.DBCircuitBreakerState.WithLabelValues(string(s)).Set(val)
	}
}

func (b *StoreBreaker) jitteredRetryAfterLocked(now time.Time) time.Duration {
	rem := b.nextProbeAt.Sub(now)
	if rem <= 0 {
		rem = b.opts.BaseCooldown
	}
	if jitter := rem / 2; jitter > 0 {
		rem += b.opts.Jitter(jitter)
	}
	if rem < time.Second {
		rem = time.Second
	}
	return rem
}

func isBenignStoreError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidCursor) || errors.Is(err, context.Canceled) {
		return true
	}
	return false
}
