package ingester

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClampDuration covers the bound-enforcement primitive adjustPollInterval
// relies on: a computed interval is always clipped back into [min, max]
// regardless of how far outside the range it lands.
func TestClampDuration(t *testing.T) {
	tests := []struct {
		name        string
		d, min, max time.Duration
		want        time.Duration
	}{
		{"within range unchanged", 5 * time.Second, time.Second, 30 * time.Second, 5 * time.Second},
		{"below min clamps up", 500 * time.Millisecond, time.Second, 30 * time.Second, time.Second},
		{"above max clamps down", time.Minute, time.Second, 30 * time.Second, 30 * time.Second},
		{"equal to min", time.Second, time.Second, 30 * time.Second, time.Second},
		{"equal to max", 30 * time.Second, time.Second, 30 * time.Second, 30 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, clampDuration(tc.d, tc.min, tc.max))
		})
	}
}

// TestAdjustPollInterval_ShrinksOnBacklog covers the first half of issue
// #146's acceptance criteria: a cycle that observes backlog (caughtUp ==
// false) must shrink the effective interval, and the shrink must move
// toward PollIntervalMin, not just any smaller value.
func TestAdjustPollInterval_ShrinksOnBacklog(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval:    8 * time.Second,
		PollIntervalMin: time.Second,
		PollIntervalMax: 32 * time.Second,
	})
	require.Equal(t, 8*time.Second, ing.EffectivePollInterval())

	ing.adjustPollInterval(false)
	assert.Equal(t, 4*time.Second, ing.EffectivePollInterval(), "backlog halves the interval")

	ing.adjustPollInterval(false)
	assert.Equal(t, 2*time.Second, ing.EffectivePollInterval())
}

// TestAdjustPollInterval_GrowsOnIdle covers the second half: an idle
// cycle (caughtUp == true) must grow the effective interval toward
// PollIntervalMax.
func TestAdjustPollInterval_GrowsOnIdle(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval:    8 * time.Second,
		PollIntervalMin: time.Second,
		PollIntervalMax: 32 * time.Second,
	})

	ing.adjustPollInterval(true)
	assert.Equal(t, 10*time.Second, ing.EffectivePollInterval(), "idle grows the interval by 25%")

	ing.adjustPollInterval(true)
	assert.Equal(t, 12500*time.Millisecond, ing.EffectivePollInterval())
}

// TestAdjustPollInterval_NeverBreachesMin drives many consecutive backlog
// cycles and asserts the interval never drops below PollIntervalMin, no
// matter how many times it halves.
func TestAdjustPollInterval_NeverBreachesMin(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval:    8 * time.Second,
		PollIntervalMin: 3 * time.Second,
		PollIntervalMax: 32 * time.Second,
	})
	for i := 0; i < 20; i++ {
		ing.adjustPollInterval(false)
		assert.GreaterOrEqual(t, ing.EffectivePollInterval(), 3*time.Second,
			"cycle %d: interval must never drop below PollIntervalMin", i)
	}
	assert.Equal(t, 3*time.Second, ing.EffectivePollInterval(), "settles exactly at the floor")
}

// TestAdjustPollInterval_NeverBreachesMax drives many consecutive idle
// cycles and asserts the interval never exceeds PollIntervalMax.
func TestAdjustPollInterval_NeverBreachesMax(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval:    8 * time.Second,
		PollIntervalMin: time.Second,
		PollIntervalMax: 15 * time.Second,
	})
	for i := 0; i < 20; i++ {
		ing.adjustPollInterval(true)
		assert.LessOrEqual(t, ing.EffectivePollInterval(), 15*time.Second,
			"cycle %d: interval must never exceed PollIntervalMax", i)
	}
	assert.Equal(t, 15*time.Second, ing.EffectivePollInterval(), "settles exactly at the ceiling")
}

// TestAdjustPollInterval_RecoversAfterBacklogClears proves the adaptive
// behavior actually adapts both ways in sequence: shrink under backlog,
// then grow back out once cycles go idle again.
func TestAdjustPollInterval_RecoversAfterBacklogClears(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval:    8 * time.Second,
		PollIntervalMin: time.Second,
		PollIntervalMax: 32 * time.Second,
	})
	ing.adjustPollInterval(false)
	ing.adjustPollInterval(false)
	shrunk := ing.EffectivePollInterval()
	require.Less(t, shrunk, 8*time.Second)

	ing.adjustPollInterval(true)
	assert.Greater(t, ing.EffectivePollInterval(), shrunk, "interval grows back once cycles go idle")
}

// TestEffectivePollInterval_FixedWhenBoundsUnset covers the backward-
// compatibility guarantee: a caller that only sets PollInterval (leaving
// PollIntervalMin/Max at their zero value) gets byte-for-byte the
// pre-#146 fixed-interval behavior — adjustPollInterval becomes a no-op
// because Min == Max == PollInterval.
func TestEffectivePollInterval_FixedWhenBoundsUnset(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{
		PollInterval: 5 * time.Second,
	})
	require.Equal(t, 5*time.Second, ing.EffectivePollInterval())

	ing.adjustPollInterval(true)
	assert.Equal(t, 5*time.Second, ing.EffectivePollInterval(), "idle must not grow past the collapsed range")

	ing.adjustPollInterval(false)
	assert.Equal(t, 5*time.Second, ing.EffectivePollInterval(), "backlog must not shrink below the collapsed range")
}

// TestOptionsApplyDefaults_PollIntervalBoundsCollapseWhenUnset pins
// applyDefaults' documented behavior: PollIntervalMin/Max default to
// PollInterval when left at zero (or negative), and an inverted pair
// (Min > Max, only reachable via hand-built Options) is clamped rather
// than left inconsistent.
func TestOptionsApplyDefaults_PollIntervalBoundsCollapseWhenUnset(t *testing.T) {
	o := Options{PollInterval: 5 * time.Second}
	o.applyDefaults()
	assert.Equal(t, 5*time.Second, o.PollIntervalMin)
	assert.Equal(t, 5*time.Second, o.PollIntervalMax)

	o2 := Options{PollInterval: 5 * time.Second, PollIntervalMin: 10 * time.Second}
	o2.applyDefaults()
	assert.Equal(t, 10*time.Second, o2.PollIntervalMin)
	assert.Equal(t, 10*time.Second, o2.PollIntervalMax, "inverted Min > Max is clamped by raising Max")
}
