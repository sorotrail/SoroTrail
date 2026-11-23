package ingester

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLagCoordinator_HysteresisBandPreventsOscillation(t *testing.T) {
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	downstream := &recordingLagMetrics{}

	coord := NewLagCoordinator(LagBackpressureOptions{
		Enabled:         true,
		EnterLagLedgers: 100, // default RecoverLagLedgers = 75
		Downstream:      downstream,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:             func() time.Time { return now },
	})

	// Below enter threshold (100 ledgers) -> stays false.
	assert.False(t, coord.ObserveLag(1100, 1000))
	assert.False(t, coord.IsLagging())
	assert.Equal(t, 1, coord.EffectiveSweepMultiplier())

	// Cross above 100 (lag = 120) -> enters lagging once.
	now = now.Add(5 * time.Second)
	assert.True(t, coord.ObserveLag(1120, 1000))
	assert.True(t, coord.IsLagging())
	assert.Equal(t, DefaultCatchUpSweepMultiplier, coord.EffectiveSweepMultiplier())

	// Fluctuate inside the hysteresis band (76..100]: must NOT oscillate!
	for _, lag := range []int64{100, 95, 82, 99, 76} {
		now = now.Add(5 * time.Second)
		assert.True(t, coord.ObserveLag(1000+lag, 1000),
			"lag %d inside hysteresis band (75, 100] must stay lagging", lag)
	}

	statsMid := coord.Stats()
	assert.Equal(t, uint64(1), statsMid.TransitionsToLagging, "must enter lagging only once")
	assert.Equal(t, uint64(0), statsMid.TransitionsToRecovered, "must not recover inside hysteresis band")

	// Drop to 75 (<= RecoverLagLedgers) -> exits lagging cleanly.
	now = now.Add(5 * time.Second)
	assert.False(t, coord.ObserveLag(1075, 1000))
	assert.False(t, coord.IsLagging())
	assert.Equal(t, 1, coord.EffectiveSweepMultiplier())

	// Re-enter hysteresis band from below (lag = 90 <= 100) -> stays recovered!
	now = now.Add(5 * time.Second)
	assert.False(t, coord.ObserveLag(1090, 1000))

	statsFinal := coord.Stats()
	assert.Equal(t, uint64(1), statsFinal.TransitionsToLagging)
	assert.Equal(t, uint64(1), statsFinal.TransitionsToRecovered)
	assert.Equal(t, []bool{false, true, true, true, true, true, true, false, false}, downstream.history())
}

func TestLagCoordinator_MaxDeferralForcesPassAndResets(t *testing.T) {
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)

	coord := NewLagCoordinator(LagBackpressureOptions{
		Enabled:         true,
		EnterLagLedgers: 100,
		MaxDeferral:     10 * time.Minute,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:             func() time.Time { return now },
	})

	// Enter lagging state (lag = 500).
	require.True(t, coord.ObserveLag(1500, 1000))

	// At t=0 and t=5m, pruner and auditor both yield.
	yield, reason := coord.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "ingest_lagging", reason)

	now = now.Add(5 * time.Minute)
	yield, reason = coord.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "ingest_lagging", reason)

	yield, reason = coord.ShouldYield("auditor")
	assert.True(t, yield)
	assert.Equal(t, "ingest_lagging", reason)

	// At t=10m, pruner has been deferred for 10m (>= MaxDeferral) -> forced pass!
	// Auditor has only been deferred for 5m -> still yields!
	now = now.Add(5 * time.Minute)
	yield, reason = coord.ShouldYield("pruner")
	assert.False(t, yield, "pruner must be allowed to run once MaxDeferral elapses")
	assert.Equal(t, "max_deferral_reached", reason)

	yield, reason = coord.ShouldYield("auditor")
	assert.True(t, yield, "auditor deferral started 5m later, so it still yields")
	assert.Equal(t, "ingest_lagging", reason)

	// Immediately after the forced pass, pruner's deferral clock was reset, so
	// the next check yields again for another MaxDeferral window.
	now = now.Add(time.Minute)
	yield, reason = coord.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "ingest_lagging", reason)

	stats := coord.Stats()
	assert.Equal(t, uint64(5), stats.TotalYields)
	assert.Equal(t, uint64(1), stats.MaxDeferralOverrides)
	assert.Equal(t, uint64(3), stats.YieldsByJob["pruner"])
	assert.Equal(t, uint64(2), stats.YieldsByJob["auditor"])
	assert.Equal(t, uint64(1), stats.OverridesByJob["pruner"])
}

func TestLagCoordinator_DisabledIsPassthrough(t *testing.T) {
	downstream := &recordingLagMetrics{}
	coord := NewLagCoordinator(LagBackpressureOptions{
		Enabled:         false,
		EnterLagLedgers: 100,
		Downstream:      downstream,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	assert.True(t, coord.ObserveLag(2000, 1000))
	assert.True(t, coord.IsLagging())
	assert.Equal(t, 1, coord.EffectiveSweepMultiplier(), "disabled backpressure must not widen sweep window")

	yield, reason := coord.ShouldYield("pruner")
	assert.False(t, yield, "disabled backpressure must never defer background jobs")
	assert.Empty(t, reason)
	assert.Equal(t, []bool{true}, downstream.history(), "downstream LagMetrics must still receive signals")
}

func TestLagCoordinator_CatchUpClosingRateAndIngesterIntegration(t *testing.T) {
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	downstream := &recordingLagMetrics{}

	coord := NewLagCoordinator(LagBackpressureOptions{
		Enabled:                true,
		EnterLagLedgers:        100,
		RecoverLagLedgers:      70,
		CatchUpSweepMultiplier: 3,
		Downstream:             downstream,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:                    func() time.Time { return now },
	})

	// Lag drops from 500 to 300 over 10 seconds -> closing rate = +20 ledgers/sec.
	coord.ObserveLag(1500, 1000)
	now = now.Add(10 * time.Second)
	coord.ObserveLag(1500, 1200)

	stats := coord.Stats()
	assert.InDelta(t, 20.0, stats.ClosingRateLedgersPerSec, 0.001)
	assert.Equal(t, int64(300), stats.CurrentLagLedgers)
	assert.Equal(t, 3, stats.EffectiveSweepMultiplier)

	// Wire into Ingester and verify effectiveSweepWindow and adjustPollInterval
	// react to the lagging state.
	ing, _, _ := makeIngester(t, Options{
		PollInterval:           4 * time.Second,
		PollIntervalMin:        1 * time.Second,
		PollIntervalMax:        8 * time.Second,
		SweepWindow:            500,
		LagWarnLedgers:         100,
		LagBackpressureEnabled: true,
		LagCoordinator:         coord,
	})

	assert.Equal(t, uint32(1500), ing.effectiveSweepWindow(),
		"while lagging, effectiveSweepWindow must scale by CatchUpSweepMultiplier (500 * 3)")

	// Even if a single pass reports caughtUp=true on a batch, adjustPollInterval
	// keeps shrinking toward PollIntervalMin while the coordinator is lagging.
	ing.adjustPollInterval(true)
	assert.Equal(t, 2*time.Second, ing.EffectivePollInterval())
	ing.adjustPollInterval(true)
	assert.Equal(t, 1*time.Second, ing.EffectivePollInterval())

	// Once lag recovers (lag = 50 <= 70), sweep window returns to 500 and
	// adjustPollInterval grows toward PollIntervalMax on caught-up cycles.
	now = now.Add(10 * time.Second)
	driveLagCycles(t, ing, []mockCycle{{latest: 1500, ingested: 1450}})

	assert.False(t, coord.IsLagging())
	assert.Equal(t, uint32(500), ing.effectiveSweepWindow())
	ing.adjustPollInterval(true)
	assert.Greater(t, ing.EffectivePollInterval(), 1*time.Second)
}
