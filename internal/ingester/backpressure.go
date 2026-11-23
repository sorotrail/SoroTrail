package ingester

import (
	"log/slog"
	"sync"
	"time"
)

// DefaultMaxLagDeferral bounds how long background maintenance jobs (pruner,
// archiver, auditor) may continuously yield to a lagging ingester before one
// maintenance pass is forced through so retention and verification never stall
// indefinitely.
const DefaultMaxLagDeferral = 15 * time.Minute

// DefaultCatchUpSweepMultiplier is the default factor by which windowSweep
// widens its ledger scan window while the ingester is in lag catch-up mode.
const (
	DefaultCatchUpSweepMultiplier = 2
	MaxCatchUpSweepMultiplier     = 4
)

// LagBackpressureOptions configures a LagCoordinator.
type LagBackpressureOptions struct {
	// Enabled turns on active lag backpressure. When false, the coordinator
	// still forwards SetLagging to Downstream (if set) and records telemetry,
	// but ShouldYield always returns false so background jobs run on their
	// normal schedule.
	Enabled bool

	// EnterLagLedgers is the lag threshold (latest_ledger - last_ingested_ledger)
	// above which the coordinator enters the lagging state.
	EnterLagLedgers uint32

	// RecoverLagLedgers is the lower hysteresis bound at or below which the
	// coordinator exits the lagging state. When zero and EnterLagLedgers > 0,
	// it defaults to 75% of EnterLagLedgers (EnterLagLedgers * 3 / 4) so a
	// fluctuating lag hovering near the threshold does not oscillate between
	// lagging and recovered on adjacent poll cycles.
	RecoverLagLedgers uint32

	// MaxDeferral is the maximum continuous wall-clock time a background job
	// may be deferred while ingestion is lagging. Once a job has yielded for
	// at least MaxDeferral, ShouldYield returns false ("max_deferral_reached")
	// for one pass and resets that job's deferral timer. Default 15m.
	MaxDeferral time.Duration

	// CatchUpSweepMultiplier scales the ingester's SweepWindow while lagging
	// (clamped to [1, MaxCatchUpSweepMultiplier]). Default 2 when Enabled.
	CatchUpSweepMultiplier int

	// Downstream receives every hysteresis-smoothed SetLagging call so
	// existing LagMetrics consumers continue to work unchanged.
	Downstream LagMetrics

	// Logger receives structured yield, max-deferral override, and catch-up
	// rate log lines. Optional (defaults to slog.Default()).
	Logger *slog.Logger

	// Now is the time source used for deferral windows and closing-rate math.
	// Defaults to time.Now.
	Now func() time.Time
}

func (o *LagBackpressureOptions) applyDefaults() {
	if o.EnterLagLedgers > 0 && o.RecoverLagLedgers == 0 {
		o.RecoverLagLedgers = (o.EnterLagLedgers * 3) / 4
	}
	if o.RecoverLagLedgers > o.EnterLagLedgers {
		o.RecoverLagLedgers = o.EnterLagLedgers
	}
	if o.MaxDeferral <= 0 {
		o.MaxDeferral = DefaultMaxLagDeferral
	}
	if o.CatchUpSweepMultiplier <= 0 {
		if o.Enabled {
			o.CatchUpSweepMultiplier = DefaultCatchUpSweepMultiplier
		} else {
			o.CatchUpSweepMultiplier = 1
		}
	}
	if o.CatchUpSweepMultiplier > MaxCatchUpSweepMultiplier {
		o.CatchUpSweepMultiplier = MaxCatchUpSweepMultiplier
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// LagBackpressureStats is a concurrency-safe snapshot of the coordinator's
// state, catch-up rate, and per-job yield/override counters.
type LagBackpressureStats struct {
	Enabled                  bool              `json:"enabled"`
	Lagging                  bool              `json:"lagging"`
	CurrentLagLedgers        int64             `json:"current_lag_ledgers"`
	ClosingRateLedgersPerSec float64           `json:"closing_rate_ledgers_per_sec"`
	EffectiveSweepMultiplier int               `json:"effective_sweep_multiplier"`
	TransitionsToLagging     uint64            `json:"transitions_to_lagging"`
	TransitionsToRecovered   uint64            `json:"transitions_to_recovered"`
	TotalYields              uint64            `json:"total_yields"`
	MaxDeferralOverrides     uint64            `json:"max_deferral_overrides"`
	YieldsByJob              map[string]uint64 `json:"yields_by_job"`
	OverridesByJob           map[string]uint64 `json:"overrides_by_job"`
}

// LagCoordinator couples the ingester's hysteresis-smoothed lag signal to
// adaptive ingester catch-up sizing and bounded deferral of background jobs
// (pruner, archiver, auditor).
//
// It implements LagMetrics so it can be passed directly as Options.LagMetrics,
// and it implements the ShouldYield(job string) contract consulted by
// background tasks.
type LagCoordinator struct {
	mu   sync.Mutex
	opts LagBackpressureOptions

	lagging                  bool
	laggingSince             time.Time
	currentLag               int64
	prevLag                  int64
	prevSampleAt             time.Time
	closingRateLedgersPerSec float64

	deferredSince map[string]time.Time
	yieldsByJob   map[string]uint64
	overrideByJob map[string]uint64

	transitionsToLagging   uint64
	transitionsToRecovered uint64
	totalYields            uint64
	maxDeferralOverrides   uint64
}

// NewLagCoordinator constructs a LagCoordinator with the supplied options.
func NewLagCoordinator(opts LagBackpressureOptions) *LagCoordinator {
	opts.applyDefaults()
	return &LagCoordinator{
		opts:          opts,
		deferredSince: make(map[string]time.Time),
		yieldsByJob:   make(map[string]uint64),
		overrideByJob: make(map[string]uint64),
	}
}

func (c *LagCoordinator) logger() *slog.Logger {
	if c.opts.Logger != nil {
		return c.opts.Logger
	}
	return slog.Default()
}

// SetEnabled toggles active backpressure at runtime or in configuration.
func (c *LagCoordinator) SetEnabled(enabled bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.opts.Enabled = enabled
	if !enabled {
		clear(c.deferredSince)
	}
	c.mu.Unlock()
}

// Enabled reports whether active lag backpressure is turned on.
func (c *LagCoordinator) Enabled() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opts.Enabled
}

// SetLagging satisfies LagMetrics. It updates the coordinator's lagging state
// and forwards the signal to any configured Downstream LagMetrics sink.
func (c *LagCoordinator) SetLagging(lagging bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	now := c.opts.Now()
	c.applyStateLocked(lagging, now)
	downstream := c.opts.Downstream
	c.mu.Unlock()

	if downstream != nil {
		downstream.SetLagging(lagging)
	}
}

// ObserveLag evaluates rawLag (latestLedger - lastIngestedLedger) against the
// coordinator's hysteresis band [RecoverLagLedgers, EnterLagLedgers], updates
// the measured catch-up rate (ledgers closed per second), and forwards the
// resulting state to Downstream.
//
// Negative rawLag values (RPC reorg or stale cache) preserve the current
// hysteresis state without resetting deferral timers.
func (c *LagCoordinator) ObserveLag(latestLedger, lastIngestedLedger int64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	now := c.opts.Now()

	if lastIngestedLedger <= 0 {
		c.currentLag = 0
		c.prevSampleAt = time.Time{}
		c.closingRateLedgersPerSec = 0
		c.applyStateLocked(false, now)
		downstream := c.opts.Downstream
		c.mu.Unlock()
		if downstream != nil {
			downstream.SetLagging(false)
		}
		return false
	}

	rawLag := latestLedger - lastIngestedLedger
	if rawLag < 0 {
		lagging := c.lagging
		downstream := c.opts.Downstream
		c.mu.Unlock()
		if downstream != nil {
			downstream.SetLagging(lagging)
		}
		return lagging
	}

	if !c.prevSampleAt.IsZero() {
		if dt := now.Sub(c.prevSampleAt).Seconds(); dt > 0 {
			c.closingRateLedgersPerSec = float64(c.prevLag-rawLag) / dt
		}
	}
	c.prevLag = rawLag
	c.prevSampleAt = now
	c.currentLag = rawLag

	lagging := c.lagging
	if c.opts.EnterLagLedgers > 0 {
		if !c.lagging && rawLag > int64(c.opts.EnterLagLedgers) {
			lagging = true
		} else if c.lagging && rawLag <= int64(c.opts.RecoverLagLedgers) {
			lagging = false
		}
	}

	transitioned := lagging != c.lagging
	closingRate := c.closingRateLedgersPerSec
	c.applyStateLocked(lagging, now)
	downstream := c.opts.Downstream
	enabled := c.opts.Enabled
	mult := c.effectiveSweepMultiplierLocked()
	c.mu.Unlock()

	if downstream != nil {
		downstream.SetLagging(lagging)
	}
	if enabled && transitioned {
		if lagging {
			c.logger().Warn("ingest lag backpressure engaged",
				"lag_ledgers", rawLag,
				"enter_threshold_ledgers", c.opts.EnterLagLedgers,
				"recover_threshold_ledgers", c.opts.RecoverLagLedgers,
				"catchup_sweep_multiplier", mult,
			)
		} else {
			c.logger().Info("ingest lag backpressure cleared",
				"lag_ledgers", rawLag,
				"recover_threshold_ledgers", c.opts.RecoverLagLedgers,
				"closing_rate_ledgers_per_sec", closingRate,
			)
		}
	}
	return lagging
}

func (c *LagCoordinator) applyStateLocked(lagging bool, now time.Time) {
	if lagging && !c.lagging {
		c.lagging = true
		c.laggingSince = now
		c.transitionsToLagging++
	} else if !lagging && c.lagging {
		c.lagging = false
		c.laggingSince = time.Time{}
		c.closingRateLedgersPerSec = 0
		clear(c.deferredSince)
		c.transitionsToRecovered++
	}
}

// IsLagging reports whether the coordinator is currently in the lagging state.
func (c *LagCoordinator) IsLagging() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lagging
}

// EffectiveSweepMultiplier returns the sweep-window multiplier to apply on the
// current ingestion cycle (1 when caught up or when backpressure is disabled;
// CatchUpSweepMultiplier when lagging and backpressure is enabled).
func (c *LagCoordinator) EffectiveSweepMultiplier() int {
	if c == nil {
		return 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.effectiveSweepMultiplierLocked()
}

func (c *LagCoordinator) effectiveSweepMultiplierLocked() int {
	if !c.opts.Enabled || !c.lagging || c.opts.CatchUpSweepMultiplier <= 1 {
		return 1
	}
	return c.opts.CatchUpSweepMultiplier
}

// ShouldYield is consulted by background maintenance tasks (such as "pruner",
// "archiver", and "auditor") before starting a pass or batch.
//
// When backpressure is enabled and the ingester is lagging, ShouldYield
// returns (true, "ingest_lagging") until the job has been continuously
// deferred for MaxDeferral. Once MaxDeferral elapses, ShouldYield returns
// (false, "max_deferral_reached") for one pass and resets that job's deferral
// clock so retention and auditing never fall permanently behind.
func (c *LagCoordinator) ShouldYield(job string) (yield bool, reason string) {
	if c == nil {
		return false, ""
	}
	if job == "" {
		job = "background"
	}

	c.mu.Lock()
	if !c.opts.Enabled || !c.lagging {
		delete(c.deferredSince, job)
		c.mu.Unlock()
		return false, ""
	}

	now := c.opts.Now()
	since, ok := c.deferredSince[job]
	if !ok {
		since = now
		c.deferredSince[job] = since
	}
	deferredFor := now.Sub(since)
	maxDeferral := c.opts.MaxDeferral
	lagLedgers := c.currentLag
	closingRate := c.closingRateLedgersPerSec

	if maxDeferral > 0 && deferredFor >= maxDeferral {
		c.deferredSince[job] = now
		c.maxDeferralOverrides++
		c.overrideByJob[job]++
		c.mu.Unlock()

		c.logger().Warn("background job max deferral reached during ingest lag; allowing pass",
			"job", job,
			"deferred_for", deferredFor,
			"max_deferral", maxDeferral,
			"lag_ledgers", lagLedgers,
			"closing_rate_ledgers_per_sec", closingRate,
		)
		return false, "max_deferral_reached"
	}

	c.totalYields++
	c.yieldsByJob[job]++
	c.mu.Unlock()

	c.logger().Info("background job yielding to lagging ingester",
		"job", job,
		"deferred_for", deferredFor,
		"max_deferral", maxDeferral,
		"lag_ledgers", lagLedgers,
		"closing_rate_ledgers_per_sec", closingRate,
	)
	return true, "ingest_lagging"
}

// Stats returns a point-in-time snapshot of the coordinator's state and
// counters.
func (c *LagCoordinator) Stats() LagBackpressureStats {
	if c == nil {
		return LagBackpressureStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	yieldsCopy := make(map[string]uint64, len(c.yieldsByJob))
	for k, v := range c.yieldsByJob {
		yieldsCopy[k] = v
	}
	overridesCopy := make(map[string]uint64, len(c.overrideByJob))
	for k, v := range c.overrideByJob {
		overridesCopy[k] = v
	}

	return LagBackpressureStats{
		Enabled:                  c.opts.Enabled,
		Lagging:                  c.lagging,
		CurrentLagLedgers:        c.currentLag,
		ClosingRateLedgersPerSec: c.closingRateLedgersPerSec,
		EffectiveSweepMultiplier: c.effectiveSweepMultiplierLocked(),
		TransitionsToLagging:     c.transitionsToLagging,
		TransitionsToRecovered:   c.transitionsToRecovered,
		TotalYields:              c.totalYields,
		MaxDeferralOverrides:     c.maxDeferralOverrides,
		YieldsByJob:              yieldsCopy,
		OverridesByJob:           overridesCopy,
	}
}
