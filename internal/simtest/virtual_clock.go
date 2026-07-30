// Package simtest provides a deterministic simulation harness for the
// SoroTrail ingester. It combines a virtual clock, a scripted fake RPC, a
// fault-injection scheduler, and an oracle that verifies every fetchable
// event reached the store.
//
// # Determinism
//
// Virtual time, seeded randomness, and a single goroutine make every run
// exactly reproducible. The ingester's Clock and Jitter seams are injected
// with VirtualClock and DeterministicJitter so time passes instantly and
// backoff jitter is deterministic.
//
// # Oracle predicate
//
// After a scenario, the oracle asserts:
//
//	StoredEvents == ChainEvents - LegitimatelyLostEvents
//
// where:
//   - ChainEvents: every event the VirtualChain generated for the scenario.
//   - LegitimatelyLostEvents: events whose ledger fell outside the RPC's
//     retention window during all ingester up-periods (tracked via
//     reclampToOldest calls) — these are the only events the ingester is
//     allowed to miss, and they MUST be accompanied by a warning log.
//   - No events are duplicated in the store.
//   - No spurious events (not in ChainEvents at all) appear.
//
// # Writing a new scenario
//
//  1. Create a Scenario value (see Scenario struct docs).
//  2. Add it to CuratedScenarios in scenarios.go.
//  3. Register it in TestCuratedScenarios in simtest_test.go.
//
// # Reproducing a failed randomized seed
//
// The randomized mode prints the seed on failure. Copy the seed and call
// RandomScenario(seed) to replay the exact fault schedule.
package simtest

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/ingester"
)

// VirtualClock implements ingester.Clock so the ingester's timing seams
// advance instantly. The simulation advances Now() explicitly by calling
// Advance.
type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewVirtualClock creates a clock at the given time.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start}
}

// Now returns the current virtual time.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// SleepCtx advances the clock by d instantly and reports true (the \"sleep\"
// always completes in simulation unless ctx is already done).
func (c *VirtualClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return ctx.Err() == nil
}

// Advance moves the virtual clock forward by d. Unlike SleepCtx, this does
// not check ctx and is used by the simulation driver to advance time between
// steps.
func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// SetNow sets the virtual clock to t.
func (c *VirtualClock) SetNow(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// DeterministicJitter creates a jitter function backed by a seeded RNG.
// Each simulation step gets a deterministic "random" jitter value.
func DeterministicJitter(rng *rand.Rand) ingester.JitterFunc {
	return func(max time.Duration) time.Duration {
		if max <= 0 {
			return 0
		}
		return time.Duration(rng.IntN(int(max)))
	}
}
