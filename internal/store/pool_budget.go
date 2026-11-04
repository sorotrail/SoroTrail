package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics for database connection pool budgeting and backpressure (#1231).
var (
	// DBPoolBudgetInUse reports the number of connection pool budget permits
	// currently held by each subsystem (ingester, api_read, graphql,
	// subscription, maintenance).
	DBPoolBudgetInUse = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_db_pool_budget_in_use",
			Help: "Current number of database connection budget permits held per subsystem.",
		},
		[]string{"subsystem"},
	)

	// DBPoolBudgetLimit reports the configured maximum connection pool budget
	// permits for each subsystem.
	DBPoolBudgetLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_db_pool_budget_limit",
			Help: "Configured maximum database connection budget permits per subsystem.",
		},
		[]string{"subsystem"},
	)

	// DBPoolBudgetUtilizationRatio reports the current utilization ratio
	// (in_use / limit) of each subsystem's connection budget share.
	DBPoolBudgetUtilizationRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_db_pool_budget_utilization_ratio",
			Help: "Current utilization ratio (0.0-1.0) of each subsystem's database connection budget.",
		},
		[]string{"subsystem"},
	)

	// DBPoolBudgetWaitDuration records the wall-clock time spent waiting to
	// acquire a connection pool budget permit, labelled by subsystem and
	// outcome (acquired | rejected | canceled).
	DBPoolBudgetWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sorotrail_db_pool_budget_wait_duration_seconds",
			Help:    "Wall-clock time spent waiting for a database connection pool budget permit, labelled by subsystem and outcome.",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"subsystem", "outcome"},
	)

	// DBPoolBudgetRejectionsTotal counts fast-fail rejections when a subsystem
	// exhausts its connection budget share or the shared non-ingester pool cap,
	// labelled by subsystem and reason (subsystem_cap | shared_cap).
	DBPoolBudgetRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_db_pool_budget_rejections_total",
			Help: "Total number of database operations rejected due to connection pool budget exhaustion, labelled by subsystem and reason.",
		},
		[]string{"subsystem", "reason"},
	)

	// DBPoolMaintenanceYieldsTotal counts how many times a background
	// maintenance job yielded early because the database connection pool was
	// under pressure.
	DBPoolMaintenanceYieldsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_db_pool_maintenance_yields_total",
			Help: "Total number of times background maintenance jobs yielded due to connection pool pressure, labelled by job and reason.",
		},
		[]string{"job", "reason"},
	)

	// DBPoolConnections reports the live connection counts by state
	// (acquired, idle, total, max, ingester_reserved, shared_cap).
	DBPoolConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_db_pool_connections",
			Help: "Database connection pool counts by state (acquired | idle | total | max | ingester_reserved | shared_cap).",
		},
		[]string{"state"},
	)
)

func init() {
	prometheus.MustRegister(
		DBPoolBudgetInUse,
		DBPoolBudgetLimit,
		DBPoolBudgetUtilizationRatio,
		DBPoolBudgetWaitDuration,
		DBPoolBudgetRejectionsTotal,
		DBPoolMaintenanceYieldsTotal,
		DBPoolConnections,
	)
}

// ErrPoolBudgetExhausted is returned when a subsystem's connection pool budget
// share (or the shared non-ingester pool ceiling) is exhausted and cannot be
// acquired within the configured fast-fail timeout (#1231).
//
// Callers (such as HTTP handlers and GraphQL resolvers) map this error to a
// retriable 503 Service Unavailable with a Retry-After header rather than
// blocking until the request context deadline expires.
var ErrPoolBudgetExhausted = errors.New("database connection pool budget exhausted")

// Subsystem identifies a logical database consumer for connection pool
// budgeting, priority isolation, and per-subsystem observability (#1231).
type Subsystem string

const (
	SubsystemIngester     Subsystem = "ingester"
	SubsystemAPIRead      Subsystem = "api_read"
	SubsystemGraphQL      Subsystem = "graphql"
	SubsystemSubscription Subsystem = "subscription"
	SubsystemMaintenance  Subsystem = "maintenance"
)

// AllSubsystems returns every canonical database consumer subsystem.
func AllSubsystems() []Subsystem {
	return []Subsystem{
		SubsystemIngester,
		SubsystemAPIRead,
		SubsystemGraphQL,
		SubsystemSubscription,
		SubsystemMaintenance,
	}
}

// Default pool budget constants (#1231).
const (
	DefaultPoolAcquireTimeout        = 50 * time.Millisecond
	DefaultIngesterAcquireTimeout    = 5 * time.Second
	DefaultPoolPressureThreshold     = 0.75
	DefaultMaxMaintenancePoolDefer   = 15 * time.Minute
	DefaultBudgetExhaustedRetryAfter = 1 * time.Second
)

// PoolBudgetExhaustedError wraps ErrPoolBudgetExhausted with the saturated
// subsystem, rejection reason, and recommended Retry-After duration.
type PoolBudgetExhaustedError struct {
	Subsystem  Subsystem
	Reason     string
	InUse      int
	Limit      int
	RetryAfter time.Duration
}

func (e *PoolBudgetExhaustedError) Error() string {
	return fmt.Sprintf(
		"%s: subsystem=%s reason=%s (in_use=%d limit=%d retry_after=%s)",
		ErrPoolBudgetExhausted.Error(),
		e.Subsystem,
		e.Reason,
		e.InUse,
		e.Limit,
		e.RetryAfter,
	)
}

func (e *PoolBudgetExhaustedError) Unwrap() error {
	return ErrPoolBudgetExhausted
}

type subsystemContextKey struct{}
type budgetPermitContextKey struct{}

// WithSubsystem tags ctx with the database consumer subsystem that owns
// subsequent Store operations on that context.
func WithSubsystem(ctx context.Context, sub Subsystem) context.Context {
	if sub == "" {
		return ctx
	}
	return context.WithValue(ctx, subsystemContextKey{}, sub)
}

// SubsystemFromContext extracts the Subsystem tag from ctx, if present.
func SubsystemFromContext(ctx context.Context) (Subsystem, bool) {
	if ctx == nil {
		return "", false
	}
	sub, ok := ctx.Value(subsystemContextKey{}).(Subsystem)
	return sub, ok && sub != ""
}

// PoolConnectionSnapshot reports live connection counts from an underlying
// database pool (such as *pgxpool.Pool).
type PoolConnectionSnapshot struct {
	AcquiredConns int32
	IdleConns     int32
	TotalConns    int32
	MaxConns      int32
}

// PoolStatProvider supplies live connection pool statistics.
type PoolStatProvider interface {
	PoolStatsSnapshot() PoolConnectionSnapshot
}

// PoolStatsSnapshot returns a point-in-time snapshot of the underlying pgxpool
// connection counts when p.pool is non-nil.
func (p *Postgres) PoolStatsSnapshot() PoolConnectionSnapshot {
	if p == nil || p.pool == nil {
		return PoolConnectionSnapshot{}
	}
	st := p.pool.Stat()
	return PoolConnectionSnapshot{
		AcquiredConns: st.AcquiredConns(),
		IdleConns:     st.IdleConns(),
		TotalConns:    st.TotalConns(),
		MaxConns:      st.MaxConns(),
	}
}

// PoolBudgetConfig configures per-subsystem connection pool partitioning,
// reserved ingester floor, fast-fail acquisition timeout, and maintenance
// backpressure thresholds (#1231).
type PoolBudgetConfig struct {
	// TotalConns is the total connection pool capacity (defaults to 20).
	TotalConns int

	// IngesterReserved is the minimum number of connections strictly reserved
	// for SubsystemIngester. Non-ingester subsystems can never consume these
	// slots even under a read flood. Defaults to max(2, 25% of TotalConns).
	IngesterReserved int

	// Per-subsystem caps within the shared non-ingester pool.
	// Defaults: APIRead=50%, GraphQL=30%, Subscription=20%, Maintenance=15% of TotalConns
	// (each clamped to TotalConns - IngesterReserved).
	APIReadMax      int
	GraphQLMax      int
	SubscriptionMax int
	MaintenanceMax  int

	// AcquireTimeout bounds how long a non-ingester caller waits for a budget
	// permit before failing fast with ErrPoolBudgetExhausted. Defaults to 50ms.
	AcquireTimeout time.Duration

	// IngesterAcquireTimeout bounds how long SubsystemIngester waits when
	// borrowing above its reserved floor. Defaults to 5s.
	IngesterAcquireTimeout time.Duration

	// PressureThreshold is the utilization ratio (0.0-1.0) at or above which
	// background maintenance yields to preserve headroom. Defaults to 0.75.
	PressureThreshold float64

	// MaxMaintenanceDeferral bounds how long a maintenance job can be
	// continuously deferred by pool pressure before one pass is allowed through.
	// Defaults to 15m.
	MaxMaintenanceDeferral time.Duration

	// RetryAfter is the Retry-After hint attached to PoolBudgetExhaustedError.
	// Defaults to 1s.
	RetryAfter time.Duration

	// Now is an optional clock for deterministic testing.
	Now func() time.Time
}

// SubsystemBudgetStats summarizes one subsystem's runtime pool budget state.
type SubsystemBudgetStats struct {
	Subsystem   Subsystem     `json:"subsystem"`
	InUse       int           `json:"in_use"`
	Limit       int           `json:"limit"`
	Utilization float64       `json:"utilization"`
	Acquired    uint64        `json:"acquired"`
	Rejected    uint64        `json:"rejected"`
	WaitTotal   time.Duration `json:"wait_total"`
}

// PoolBudgetSnapshot summarizes the entire PoolBudget state at a point in time.
type PoolBudgetSnapshot struct {
	TotalConns        int                                `json:"total_conns"`
	IngesterReserved  int                                `json:"ingester_reserved"`
	SharedCap         int                                `json:"shared_cap"`
	TotalInUse        int                                `json:"total_in_use"`
	SharedInUse       int                                `json:"shared_in_use"`
	IngesterWaiters   int                                `json:"ingester_waiters"`
	UnderPressure     bool                               `json:"under_pressure"`
	MaintenanceYields uint64                             `json:"maintenance_yields"`
	Subsystems        map[Subsystem]SubsystemBudgetStats `json:"subsystems"`
}

type subsystemTracker struct {
	sem       chan struct{}
	limit     int
	inUse     atomic.Int64
	acquired  atomic.Uint64
	rejected  atomic.Uint64
	waitNanos atomic.Int64
}

// PoolBudget partitions database connection capacity across subsystems,
// guarantees a reserved connection floor for the ingester, fails fast when a
// read/maintenance subsystem exhausts its share, and signals background
// maintenance jobs to yield when the pool is under pressure (#1231).
type PoolBudget struct {
	totalConns             int
	ingesterReserved       int
	sharedCap              int
	acquireTimeout         time.Duration
	ingesterAcquireTimeout time.Duration
	pressureThreshold      float64
	maxMaintenanceDeferral time.Duration
	retryAfter             time.Duration
	now                    func() time.Time

	totalSem  chan struct{}
	sharedSem chan struct{}

	subsystems map[Subsystem]*subsystemTracker

	totalInUse        atomic.Int64
	sharedInUse       atomic.Int64
	ingesterWaiters   atomic.Int64
	maintenanceYields atomic.Uint64

	mu            sync.Mutex
	deferredSince map[string]time.Time
	statProvider  PoolStatProvider
}

// NewPoolBudget constructs a PoolBudget with resolved defaults and initializes
// per-subsystem Prometheus limit gauges.
func NewPoolBudget(cfg PoolBudgetConfig) *PoolBudget {
	total := cfg.TotalConns
	if total <= 0 {
		total = int(DefaultPoolMaxConns)
	}
	if total < 2 {
		total = 2
	}

	reserved := cfg.IngesterReserved
	if reserved <= 0 {
		reserved = max(2, (total*25)/100)
	}
	if reserved >= total {
		reserved = total - 1
	}
	sharedCap := total - reserved

	resolveCap := func(explicit, def int) int {
		v := explicit
		if v <= 0 {
			v = max(1, def)
		}
		if v > sharedCap {
			v = sharedCap
		}
		return v
	}

	apiMax := resolveCap(cfg.APIReadMax, (total*50)/100)
	gqlMax := resolveCap(cfg.GraphQLMax, (total*30)/100)
	subMax := resolveCap(cfg.SubscriptionMax, (total*20)/100)
	maintMax := resolveCap(cfg.MaintenanceMax, (total*15)/100)

	acqTimeout := cfg.AcquireTimeout
	if acqTimeout <= 0 {
		acqTimeout = DefaultPoolAcquireTimeout
	}
	ingTimeout := cfg.IngesterAcquireTimeout
	if ingTimeout <= 0 {
		ingTimeout = DefaultIngesterAcquireTimeout
	}
	pressure := cfg.PressureThreshold
	if pressure <= 0 || pressure > 1 {
		pressure = DefaultPoolPressureThreshold
	}
	maxDefer := cfg.MaxMaintenanceDeferral
	if maxDefer <= 0 {
		maxDefer = DefaultMaxMaintenancePoolDefer
	}
	retryAfter := cfg.RetryAfter
	if retryAfter <= 0 {
		retryAfter = DefaultBudgetExhaustedRetryAfter
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	b := &PoolBudget{
		totalConns:             total,
		ingesterReserved:       reserved,
		sharedCap:              sharedCap,
		acquireTimeout:         acqTimeout,
		ingesterAcquireTimeout: ingTimeout,
		pressureThreshold:      pressure,
		maxMaintenanceDeferral: maxDefer,
		retryAfter:             retryAfter,
		now:                    nowFn,
		totalSem:               make(chan struct{}, total),
		sharedSem:              make(chan struct{}, sharedCap),
		subsystems: map[Subsystem]*subsystemTracker{
			SubsystemIngester: {
				sem:   make(chan struct{}, total),
				limit: total,
			},
			SubsystemAPIRead: {
				sem:   make(chan struct{}, apiMax),
				limit: apiMax,
			},
			SubsystemGraphQL: {
				sem:   make(chan struct{}, gqlMax),
				limit: gqlMax,
			},
			SubsystemSubscription: {
				sem:   make(chan struct{}, subMax),
				limit: subMax,
			},
			SubsystemMaintenance: {
				sem:   make(chan struct{}, maintMax),
				limit: maintMax,
			},
		},
		deferredSince: make(map[string]time.Time),
	}

	for sub, tr := range b.subsystems {
		DBPoolBudgetLimit.WithLabelValues(string(sub)).Set(float64(tr.limit))
		DBPoolBudgetInUse.WithLabelValues(string(sub)).Set(0)
		DBPoolBudgetUtilizationRatio.WithLabelValues(string(sub)).Set(0)
	}
	DBPoolConnections.WithLabelValues("max").Set(float64(total))
	DBPoolConnections.WithLabelValues("ingester_reserved").Set(float64(reserved))
	DBPoolConnections.WithLabelValues("shared_cap").Set(float64(sharedCap))
	DBPoolConnections.WithLabelValues("acquired").Set(0)
	return b
}

// SetPoolStatProvider attaches an optional PoolStatProvider (such as *Postgres)
// so PoolBudget can incorporate live pgxpool stats into pressure decisions and
// Prometheus gauges.
func (b *PoolBudget) SetPoolStatProvider(p PoolStatProvider) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.statProvider = p
	b.mu.Unlock()
}

func (b *PoolBudget) trackerFor(sub Subsystem) (Subsystem, *subsystemTracker) {
	if tr, ok := b.subsystems[sub]; ok {
		return sub, tr
	}
	return SubsystemAPIRead, b.subsystems[SubsystemAPIRead]
}

// Acquire obtains a connection budget permit for sub (or returns a no-op
// release immediately if ctx already holds an active permit on this PoolBudget).
//
// Ingester operations bypass the shared non-ingester semaphore so at least
// IngesterReserved permits in totalSem are always available to the ingester
// regardless of read/GraphQL/subscription/maintenance load.
func (b *PoolBudget) Acquire(ctx context.Context, sub Subsystem) (context.Context, func(), error) {
	if b == nil {
		return ctx, func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if existing, ok := ctx.Value(budgetPermitContextKey{}).(*PoolBudget); ok && existing == b {
		return ctx, func() {}, nil
	}
	if ctxSub, ok := SubsystemFromContext(ctx); ok {
		sub = ctxSub
	}
	sub, tr := b.trackerFor(sub)
	start := b.now()

	if sub == SubsystemIngester {
		return b.acquireIngester(ctx, tr, start)
	}
	return b.acquireNonIngester(ctx, sub, tr, start)
}

func (b *PoolBudget) acquireIngester(ctx context.Context, tr *subsystemTracker, start time.Time) (context.Context, func(), error) {
	// Fast path: non-blocking acquire from totalSem.
	select {
	case b.totalSem <- struct{}{}:
		tr.sem <- struct{}{}
		return b.finishAcquire(ctx, SubsystemIngester, tr, false, start)
	default:
	}

	b.ingesterWaiters.Add(1)
	defer b.ingesterWaiters.Add(-1)

	timer := time.NewTimer(b.ingesterAcquireTimeout)
	defer timer.Stop()

	select {
	case b.totalSem <- struct{}{}:
		tr.sem <- struct{}{}
		return b.finishAcquire(ctx, SubsystemIngester, tr, false, start)
	case <-ctx.Done():
		wait := b.now().Sub(start)
		tr.waitNanos.Add(wait.Nanoseconds())
		DBPoolBudgetWaitDuration.WithLabelValues(string(SubsystemIngester), "canceled").Observe(wait.Seconds())
		return ctx, nil, ctx.Err()
	case <-timer.C:
		wait := b.now().Sub(start)
		tr.waitNanos.Add(wait.Nanoseconds())
		tr.rejected.Add(1)
		DBPoolBudgetWaitDuration.WithLabelValues(string(SubsystemIngester), "rejected").Observe(wait.Seconds())
		DBPoolBudgetRejectionsTotal.WithLabelValues(string(SubsystemIngester), "subsystem_cap").Inc()
		return ctx, nil, &PoolBudgetExhaustedError{
			Subsystem:  SubsystemIngester,
			Reason:     "subsystem_cap",
			InUse:      int(tr.inUse.Load()),
			Limit:      tr.limit,
			RetryAfter: b.retryAfter,
		}
	}
}

func (b *PoolBudget) acquireNonIngester(ctx context.Context, sub Subsystem, tr *subsystemTracker, start time.Time) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}

	// Step 1: Acquire from the subsystem's own semaphore (non-blocking first, then bounded wait).
	var timer *time.Timer
	select {
	case tr.sem <- struct{}{}:
	default:
		timer = time.NewTimer(b.acquireTimeout)
		defer timer.Stop()
		select {
		case tr.sem <- struct{}{}:
		case <-ctx.Done():
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "canceled").Observe(wait.Seconds())
			return ctx, nil, ctx.Err()
		case <-timer.C:
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			tr.rejected.Add(1)
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "rejected").Observe(wait.Seconds())
			DBPoolBudgetRejectionsTotal.WithLabelValues(string(sub), "subsystem_cap").Inc()
			return ctx, nil, &PoolBudgetExhaustedError{
				Subsystem:  sub,
				Reason:     "subsystem_cap",
				InUse:      int(tr.inUse.Load()),
				Limit:      tr.limit,
				RetryAfter: b.retryAfter,
			}
		}
	}

	// Step 2: Acquire from the shared non-ingester semaphore (TotalConns - IngesterReserved).
	select {
	case b.sharedSem <- struct{}{}:
	default:
		elapsed := b.now().Sub(start)
		rem := b.acquireTimeout - elapsed
		if rem <= 0 {
			<-tr.sem
			tr.waitNanos.Add(elapsed.Nanoseconds())
			tr.rejected.Add(1)
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "rejected").Observe(elapsed.Seconds())
			DBPoolBudgetRejectionsTotal.WithLabelValues(string(sub), "shared_cap").Inc()
			return ctx, nil, &PoolBudgetExhaustedError{
				Subsystem:  sub,
				Reason:     "shared_cap",
				InUse:      int(b.sharedInUse.Load()),
				Limit:      b.sharedCap,
				RetryAfter: b.retryAfter,
			}
		}
		if timer == nil {
			timer = time.NewTimer(rem)
			defer timer.Stop()
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(rem)
		}
		select {
		case b.sharedSem <- struct{}{}:
		case <-ctx.Done():
			<-tr.sem
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "canceled").Observe(wait.Seconds())
			return ctx, nil, ctx.Err()
		case <-timer.C:
			<-tr.sem
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			tr.rejected.Add(1)
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "rejected").Observe(wait.Seconds())
			DBPoolBudgetRejectionsTotal.WithLabelValues(string(sub), "shared_cap").Inc()
			return ctx, nil, &PoolBudgetExhaustedError{
				Subsystem:  sub,
				Reason:     "shared_cap",
				InUse:      int(b.sharedInUse.Load()),
				Limit:      b.sharedCap,
				RetryAfter: b.retryAfter,
			}
		}
	}

	// Step 3: Acquire from totalSem (may briefly wait only if ingester borrowed into shared slots).
	select {
	case b.totalSem <- struct{}{}:
	default:
		elapsed := b.now().Sub(start)
		rem := max(b.acquireTimeout-elapsed, 5*time.Millisecond)
		if timer == nil {
			timer = time.NewTimer(rem)
			defer timer.Stop()
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(rem)
		}
		select {
		case b.totalSem <- struct{}{}:
		case <-ctx.Done():
			<-b.sharedSem
			<-tr.sem
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "canceled").Observe(wait.Seconds())
			return ctx, nil, ctx.Err()
		case <-timer.C:
			<-b.sharedSem
			<-tr.sem
			wait := b.now().Sub(start)
			tr.waitNanos.Add(wait.Nanoseconds())
			tr.rejected.Add(1)
			DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "rejected").Observe(wait.Seconds())
			DBPoolBudgetRejectionsTotal.WithLabelValues(string(sub), "shared_cap").Inc()
			return ctx, nil, &PoolBudgetExhaustedError{
				Subsystem:  sub,
				Reason:     "shared_cap",
				InUse:      int(b.totalInUse.Load()),
				Limit:      b.totalConns,
				RetryAfter: b.retryAfter,
			}
		}
	}

	return b.finishAcquire(ctx, sub, tr, true, start)
}

func (b *PoolBudget) finishAcquire(ctx context.Context, sub Subsystem, tr *subsystemTracker, usedShared bool, start time.Time) (context.Context, func(), error) {
	wait := b.now().Sub(start)
	tr.waitNanos.Add(wait.Nanoseconds())
	tr.acquired.Add(1)
	inUse := tr.inUse.Add(1)
	totalInUse := b.totalInUse.Add(1)
	if usedShared {
		b.sharedInUse.Add(1)
	}

	b.updateSubsystemGauges(sub, inUse, tr.limit, totalInUse)
	DBPoolBudgetWaitDuration.WithLabelValues(string(sub), "acquired").Observe(wait.Seconds())

	var once sync.Once
	release := func() {
		once.Do(func() {
			if usedShared {
				b.sharedInUse.Add(-1)
				<-b.sharedSem
			}
			newInUse := tr.inUse.Add(-1)
			newTotal := b.totalInUse.Add(-1)
			<-tr.sem
			<-b.totalSem
			b.updateSubsystemGauges(sub, newInUse, tr.limit, newTotal)
		})
	}

	permitCtx := context.WithValue(ctx, budgetPermitContextKey{}, b)
	return permitCtx, release, nil
}

func (b *PoolBudget) updateSubsystemGauges(sub Subsystem, inUse int64, limit int, totalInUse int64) {
	DBPoolBudgetInUse.WithLabelValues(string(sub)).Set(float64(inUse))
	if limit > 0 {
		DBPoolBudgetUtilizationRatio.WithLabelValues(string(sub)).Set(float64(inUse) / float64(limit))
	}
	DBPoolConnections.WithLabelValues("acquired").Set(float64(totalInUse))

	b.mu.Lock()
	sp := b.statProvider
	b.mu.Unlock()
	if sp != nil {
		snap := sp.PoolStatsSnapshot()
		if snap.MaxConns > 0 {
			DBPoolConnections.WithLabelValues("idle").Set(float64(snap.IdleConns))
			DBPoolConnections.WithLabelValues("total").Set(float64(snap.TotalConns))
		}
	}
}

// UnderPressure reports whether the connection pool is currently under
// pressure: either the ingester has waiting acquisitions, shared non-ingester
// utilization has reached PressureThreshold, total pool utilization has reached
// PressureThreshold, or the underlying pgxpool is at/above PressureThreshold.
func (b *PoolBudget) UnderPressure() bool {
	if b == nil {
		return false
	}
	if b.ingesterWaiters.Load() > 0 {
		return true
	}
	if b.sharedCap > 0 && float64(b.sharedInUse.Load())/float64(b.sharedCap) >= b.pressureThreshold {
		return true
	}
	if b.totalConns > 0 && float64(b.totalInUse.Load())/float64(b.totalConns) >= b.pressureThreshold {
		return true
	}
	b.mu.Lock()
	sp := b.statProvider
	b.mu.Unlock()
	if sp != nil {
		snap := sp.PoolStatsSnapshot()
		if snap.MaxConns > 0 && float64(snap.AcquiredConns)/float64(snap.MaxConns) >= b.pressureThreshold {
			return true
		}
	}
	return false
}

// ShouldYield implements the background maintenance yield controller interface
// consumed by pruner.Pruner, archive.Archiver, and audit.Auditor (#1231).
//
// When the connection pool is under pressure, ShouldYield returns
// (true, "pool_pressure") unless the named job has been continuously deferred
// for >= MaxMaintenanceDeferral, in which case one maintenance pass is allowed
// through with (false, "max_deferral_reached").
func (b *PoolBudget) ShouldYield(job string) (bool, string) {
	if b == nil {
		return false, ""
	}
	if job == "" {
		job = "maintenance"
	}
	pressure := b.UnderPressure()
	now := b.now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if !pressure {
		delete(b.deferredSince, job)
		return false, ""
	}

	since, exists := b.deferredSince[job]
	if !exists {
		b.deferredSince[job] = now
		b.maintenanceYields.Add(1)
		DBPoolMaintenanceYieldsTotal.WithLabelValues(job, "pool_pressure").Inc()
		return true, "pool_pressure"
	}
	if now.Sub(since) >= b.maxMaintenanceDeferral {
		b.deferredSince[job] = now
		return false, "max_deferral_reached"
	}
	b.maintenanceYields.Add(1)
	DBPoolMaintenanceYieldsTotal.WithLabelValues(job, "pool_pressure").Inc()
	return true, "pool_pressure"
}

// YieldController is implemented by PoolBudget and ingester.LagCoordinator to
// signal when background maintenance jobs should defer work.
type YieldController interface {
	ShouldYield(job string) (bool, string)
}

type combinedYieldController struct {
	controllers []YieldController
}

// CombineYieldControllers returns a YieldController that yields whenever any
// non-nil child controller signals (true, reason).
func CombineYieldControllers(controllers ...YieldController) YieldController {
	return &combinedYieldController{controllers: controllers}
}

func (c *combinedYieldController) ShouldYield(job string) (bool, string) {
	if c == nil {
		return false, ""
	}
	for _, ctl := range c.controllers {
		if ctl == nil {
			continue
		}
		if yield, reason := ctl.ShouldYield(job); yield {
			return true, reason
		}
	}
	return false, ""
}

// Snapshot returns a point-in-time PoolBudgetSnapshot for observability and tests.
func (b *PoolBudget) Snapshot() PoolBudgetSnapshot {
	if b == nil {
		return PoolBudgetSnapshot{}
	}
	subs := make(map[Subsystem]SubsystemBudgetStats, len(b.subsystems))
	for sub, tr := range b.subsystems {
		inUse := int(tr.inUse.Load())
		util := 0.0
		if tr.limit > 0 {
			util = float64(inUse) / float64(tr.limit)
		}
		subs[sub] = SubsystemBudgetStats{
			Subsystem:   sub,
			InUse:       inUse,
			Limit:       tr.limit,
			Utilization: util,
			Acquired:    tr.acquired.Load(),
			Rejected:    tr.rejected.Load(),
			WaitTotal:   time.Duration(tr.waitNanos.Load()),
		}
	}
	return PoolBudgetSnapshot{
		TotalConns:        b.totalConns,
		IngesterReserved:  b.ingesterReserved,
		SharedCap:         b.sharedCap,
		TotalInUse:        int(b.totalInUse.Load()),
		SharedInUse:       int(b.sharedInUse.Load()),
		IngesterWaiters:   int(b.ingesterWaiters.Load()),
		UnderPressure:     b.UnderPressure(),
		MaintenanceYields: b.maintenanceYields.Load(),
		Subsystems:        subs,
	}
}

// BudgetedStore wraps a Store with a PoolBudget, attributing every operation to
// the context's Subsystem tag (or the wrapper's defaultSubsystem when the
// context is untagged) and enforcing connection pool budgets (#1231).
type BudgetedStore struct {
	inner            Store
	budget           *PoolBudget
	defaultSubsystem Subsystem
}

var _ Store = (*BudgetedStore)(nil)

// NewBudgetedStore wraps inner with budget. Operations on contexts without an
// explicit WithSubsystem tag are attributed to defaultSubsystem (which defaults
// to SubsystemAPIRead when empty).
func NewBudgetedStore(inner Store, budget *PoolBudget, defaultSubsystem Subsystem) *BudgetedStore {
	if defaultSubsystem == "" {
		defaultSubsystem = SubsystemAPIRead
	}
	if sp, ok := inner.(PoolStatProvider); ok && budget != nil {
		budget.SetPoolStatProvider(sp)
	}
	return &BudgetedStore{
		inner:            inner,
		budget:           budget,
		defaultSubsystem: defaultSubsystem,
	}
}

// ForSubsystem returns a lightweight view of s sharing the same underlying
// Store and PoolBudget but defaulting untagged calls to sub.
func (s *BudgetedStore) ForSubsystem(sub Subsystem) *BudgetedStore {
	if s == nil {
		return nil
	}
	if sub == "" {
		sub = s.defaultSubsystem
	}
	return &BudgetedStore{
		inner:            s.inner,
		budget:           s.budget,
		defaultSubsystem: sub,
	}
}

// Budget returns the underlying *PoolBudget.
func (s *BudgetedStore) Budget() *PoolBudget {
	if s == nil {
		return nil
	}
	return s.budget
}

// Unwrap returns the wrapped Store.
func (s *BudgetedStore) Unwrap() Store {
	if s == nil {
		return nil
	}
	return s.inner
}

func (s *BudgetedStore) acquire(ctx context.Context) (context.Context, func(), error) {
	if s == nil || s.budget == nil {
		return ctx, func() {}, nil
	}
	return s.budget.Acquire(ctx, s.defaultSubsystem)
}

func (s *BudgetedStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.UpsertEvents(ctx, events)
}

func (s *BudgetedStore) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.ReplaceEventsInRange(ctx, events, fromLedger, toLedger)
}

func (s *BudgetedStore) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return Event{}, err
	}
	defer release()
	return s.inner.GetEvent(ctx, id, sc)
}

func (s *BudgetedStore) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.GetEventsByTxHash(ctx, txHash, excludeID)
}

func (s *BudgetedStore) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return s.inner.EventExists(ctx, id, sc)
}

func (s *BudgetedStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	defer release()
	return s.inner.QueryEvents(ctx, f)
}

func (s *BudgetedStore) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountEvents(ctx, f)
}

func (s *BudgetedStore) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.AggregateEvents(ctx, f, bucket)
}

func (s *BudgetedStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.LedgerRangeCensus(ctx, fromLedger, toLedger, idsOnly)
}

func (s *BudgetedStore) ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	defer release()
	return s.inner.ListContracts(ctx, f)
}

func (s *BudgetedStore) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountContracts(ctx, f)
}

func (s *BudgetedStore) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return ContractSummary{}, err
	}
	defer release()
	return s.inner.GetContractSummary(ctx, contractID)
}

func (s *BudgetedStore) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ContractEventTypeCounts(ctx, contractID)
}

func (s *BudgetedStore) DeadLetterEvent(ctx context.Context, ev DeadLetterInput) (DeadLetter, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return DeadLetter{}, err
	}
	defer release()
	return s.inner.DeadLetterEvent(ctx, ev)
}

func (s *BudgetedStore) ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	defer release()
	return s.inner.ListDeadLetters(ctx, contractID, limit, cursor)
}

func (s *BudgetedStore) CountDeadLetters(ctx context.Context, contractID string) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountDeadLetters(ctx, contractID)
}

func (s *BudgetedStore) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return DeadLetter{}, err
	}
	defer release()
	return s.inner.GetDeadLetter(ctx, id)
}

func (s *BudgetedStore) DeleteDeadLetter(ctx context.Context, id int64) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.DeleteDeadLetter(ctx, id)
}

func (s *BudgetedStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return IngestionState{}, err
	}
	defer release()
	return s.inner.GetIngestionState(ctx)
}

func (s *BudgetedStore) SaveIngestionState(ctx context.Context, st IngestionState) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SaveIngestionState(ctx, st)
}

func (s *BudgetedStore) GetAuditState(ctx context.Context, network string) (AuditState, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return AuditState{}, err
	}
	defer release()
	return s.inner.GetAuditState(ctx, network)
}

func (s *BudgetedStore) SaveAuditState(ctx context.Context, st AuditState) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SaveAuditState(ctx, st)
}

func (s *BudgetedStore) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return AuditState{}, err
	}
	defer release()
	return s.inner.SaveAuditStateIfGreater(ctx, network, ledger)
}

func (s *BudgetedStore) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListWatchedContracts(ctx)
}

func (s *BudgetedStore) ListWatchedContractsScoped(ctx context.Context, sc Scope) ([]WatchedContract, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListWatchedContractsScoped(ctx, sc)
}

func (s *BudgetedStore) AddWatchedContract(ctx context.Context, w WatchedContract) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.AddWatchedContract(ctx, w)
}

func (s *BudgetedStore) RemoveWatchedContract(ctx context.Context, w WatchedContract) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.RemoveWatchedContract(ctx, w)
}

func (s *BudgetedStore) GetContractCursor(ctx context.Context, contractID string) (ContractCursor, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return ContractCursor{}, err
	}
	defer release()
	return s.inner.GetContractCursor(ctx, contractID)
}

func (s *BudgetedStore) SaveContractCursor(ctx context.Context, c ContractCursor) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SaveContractCursor(ctx, c)
}

func (s *BudgetedStore) DeleteContractCursor(ctx context.Context, contractID string) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.DeleteContractCursor(ctx, contractID)
}

func (s *BudgetedStore) ListContractCursors(ctx context.Context) ([]ContractCursor, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListContractCursors(ctx)
}

func (s *BudgetedStore) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return AuditFinding{}, err
	}
	defer release()
	return s.inner.RecordAuditFinding(ctx, f)
}

func (s *BudgetedStore) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.UpdateAuditFinding(ctx, f)
}

func (s *BudgetedStore) ListOpenFindingsByRange(ctx context.Context, network string, fromLedger, toLedger int64) (AuditFinding, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return AuditFinding{}, err
	}
	defer release()
	return s.inner.ListOpenFindingsByRange(ctx, network, fromLedger, toLedger)
}

func (s *BudgetedStore) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer release()
	return s.inner.CreateSubscription(ctx, sub)
}

func (s *BudgetedStore) GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer release()
	return s.inner.GetSubscription(ctx, id, owner)
}

func (s *BudgetedStore) ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListSubscriptions(ctx, owner)
}

func (s *BudgetedStore) UpdateSubscription(ctx context.Context, sub Subscription, owner SubscriptionOwner) (Subscription, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer release()
	return s.inner.UpdateSubscription(ctx, sub, owner)
}

func (s *BudgetedStore) DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.DeleteSubscription(ctx, id, owner)
}

func (s *BudgetedStore) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListEnabledSubscriptions(ctx)
}

func (s *BudgetedStore) CreateAPIKey(ctx context.Context, k APIKey) (APIKey, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return APIKey{}, err
	}
	defer release()
	return s.inner.CreateAPIKey(ctx, k)
}

func (s *BudgetedStore) GetAPIKey(ctx context.Context, id int64) (APIKey, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return APIKey{}, err
	}
	defer release()
	return s.inner.GetAPIKey(ctx, id)
}

func (s *BudgetedStore) LookupAPIKeyByPrefix(ctx context.Context, prefix string) (APIKey, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return APIKey{}, err
	}
	defer release()
	return s.inner.LookupAPIKeyByPrefix(ctx, prefix)
}

func (s *BudgetedStore) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListAPIKeys(ctx)
}

func (s *BudgetedStore) RevokeAPIKey(ctx context.Context, id int64) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.RevokeAPIKey(ctx, id)
}

func (s *BudgetedStore) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, false, err
	}
	defer release()
	return s.inner.IncrementSubscriptionFailures(ctx, id, maxFailures)
}

func (s *BudgetedStore) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.ResetSubscriptionFailures(ctx, id)
}

func (s *BudgetedStore) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return DeliveryAttempt{}, err
	}
	defer release()
	return s.inner.RecordDeliveryAttempt(ctx, a)
}

func (s *BudgetedStore) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListDeliveryAttempts(ctx, subscriptionID, limit, owner)
}

func (s *BudgetedStore) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, owner SubscriptionOwner) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountDeliveryAttempts(ctx, subscriptionID, owner)
}

func (s *BudgetedStore) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.GetContractSpec(ctx, wasmHash)
}

func (s *BudgetedStore) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SetContractSpec(ctx, wasmHash, contractID, specJSON)
}

func (s *BudgetedStore) GetContractSpecOverride(ctx context.Context, contractID string) ([]byte, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.GetContractSpecOverride(ctx, contractID)
}

func (s *BudgetedStore) SetContractSpecOverride(ctx context.Context, contractID string, specJSON []byte) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SetContractSpecOverride(ctx, contractID, specJSON)
}

func (s *BudgetedStore) DeleteContractSpecOverride(ctx context.Context, contractID string) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.DeleteContractSpecOverride(ctx, contractID)
}

func (s *BudgetedStore) SetContractSpecOverrideWithHistory(ctx context.Context, contractID string, specJSON []byte, apiKeyPrefix string) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.SetContractSpecOverrideWithHistory(ctx, contractID, specJSON, apiKeyPrefix)
}

func (s *BudgetedStore) DeleteContractSpecOverrideWithHistory(ctx context.Context, contractID string, apiKeyPrefix string) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.DeleteContractSpecOverrideWithHistory(ctx, contractID, apiKeyPrefix)
}

func (s *BudgetedStore) ListContractSpecOverrideHistory(ctx context.Context, contractID string, limit int, cursor string) ([]ContractSpecOverrideVersion, string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	defer release()
	return s.inner.ListContractSpecOverrideHistory(ctx, contractID, limit, cursor)
}

func (s *BudgetedStore) DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.DeleteEventsBeforeLedger(ctx, beforeLedger)
}

func (s *BudgetedStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.DeleteEventsBefore(ctx, maxLedger, beforeTime, limit)
}

func (s *BudgetedStore) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountEventsBefore(ctx, maxLedger, beforeTime, limit)
}

func (s *BudgetedStore) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.UpsertAddressRefs(ctx, refs)
}

func (s *BudgetedStore) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, "", err
	}
	defer release()
	return s.inner.QueryAddressEvents(ctx, address, f)
}

func (s *BudgetedStore) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountAddressEvents(ctx, address)
}

func (s *BudgetedStore) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return AddressSummary{}, err
	}
	defer release()
	return s.inner.GetAddressSummary(ctx, address)
}

func (s *BudgetedStore) MigrationVersion(ctx context.Context) (int, bool, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, false, err
	}
	defer release()
	return s.inner.MigrationVersion(ctx)
}

func (s *BudgetedStore) Stats(ctx context.Context, sc Scope) (Stats, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return Stats{}, err
	}
	defer release()
	return s.inner.Stats(ctx, sc)
}

func (s *BudgetedStore) Ping(ctx context.Context) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.Ping(ctx)
}

func (s *BudgetedStore) ListContractIDs(ctx context.Context) ([]string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListContractIDs(ctx)
}

func (s *BudgetedStore) GetContractMeta(ctx context.Context, contractID string) (ContractMeta, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return ContractMeta{}, err
	}
	defer release()
	return s.inner.GetContractMeta(ctx, contractID)
}

func (s *BudgetedStore) UpsertContractMeta(ctx context.Context, m ContractMeta) error {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.inner.UpsertContractMeta(ctx, m)
}

func (s *BudgetedStore) CountContractEvents(ctx context.Context, contractID string) (int64, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return s.inner.CountContractEvents(ctx, contractID)
}

func (s *BudgetedStore) ListContractsNeedingRefresh(ctx context.Context, olderThan time.Time) ([]string, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.inner.ListContractsNeedingRefresh(ctx, olderThan)
}

func (s *BudgetedStore) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return IngestionState{}, err
	}
	defer release()
	if r, ok := s.inner.(interface {
		GetIngestionStateForNetwork(context.Context, string) (IngestionState, error)
	}); ok {
		return r.GetIngestionStateForNetwork(ctx, network)
	}
	if defaultNetwork(network) == "default" {
		return s.inner.GetIngestionState(ctx)
	}
	return IngestionState{}, ErrNotFound
}

func (s *BudgetedStore) PruneEventsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	inner, ok := s.inner.(RetentionStore)
	if !ok {
		return 0, ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return inner.PruneEventsBefore(ctx, cutoff)
}

func (s *BudgetedStore) ClaimIdempotencyKey(ctx context.Context, rec IdempotencyRecord, now time.Time) (IdempotencyOutcome, IdempotencyRecord, error) {
	inner, ok := s.inner.(IdempotencyStore)
	if !ok {
		return 0, IdempotencyRecord{}, ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, IdempotencyRecord{}, err
	}
	defer release()
	return inner.ClaimIdempotencyKey(ctx, rec, now)
}

func (s *BudgetedStore) CompleteIdempotencyKey(ctx context.Context, rec IdempotencyRecord) error {
	inner, ok := s.inner.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return inner.CompleteIdempotencyKey(ctx, rec)
}

func (s *BudgetedStore) DeleteIdempotencyKey(ctx context.Context, tenantID, key string) error {
	inner, ok := s.inner.(IdempotencyStore)
	if !ok {
		return ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return inner.DeleteIdempotencyKey(ctx, tenantID, key)
}

func (s *BudgetedStore) GetIdempotencyKey(ctx context.Context, tenantID, key string, now time.Time) (IdempotencyRecord, error) {
	inner, ok := s.inner.(IdempotencyStore)
	if !ok {
		return IdempotencyRecord{}, ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return IdempotencyRecord{}, err
	}
	defer release()
	return inner.GetIdempotencyKey(ctx, tenantID, key, now)
}

func (s *BudgetedStore) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	inner, ok := s.inner.(IdempotencyStore)
	if !ok {
		return 0, ErrUnsupported
	}
	ctx, release, err := s.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	return inner.DeleteExpiredIdempotencyKeys(ctx, now, limit)
}
