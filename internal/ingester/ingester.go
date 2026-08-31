// Package ingester runs the polling loop that pulls contract events from
// Stellar RPC and persists them.
//
// The entry point is [New], which wires an [Ingester] from an [rpc.Client],
// a [store.Store], a [decode.Decoder], a logger, and [Options]. Call
// [Ingester.Run] to start the loop; it blocks until the context is
// canceled.
//
// Non-obvious contracts:
//   - [Ingester.Run] never returns nil on success; the only terminal
//     condition is context cancellation (returns ctx.Err()).
//   - Errors are retried with jittered exponential backoff, capped at
//     [Options.MaxBackoff].
//   - [Ingester.BuildFilterBatches] is exported so the auditor can fetch
//     with the exact same filter set as ingest — events outside this
//     filter set were intentionally not stored and must not be flagged as
//     audit discrepancies.
//   - [Ingester.ReingestRange] does NOT advance the ingester's persisted
//     cursor; it is for auditor repairs of specific ledger ranges.
//   - [Ingester.PageLimit] is exported so the auditor can reuse it and
//     never silently disagree on page size.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	noop "go.opentelemetry.io/otel/trace/noop"

	"golang.org/x/sync/errgroup"

	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Clock abstracts time operations so tests and simulations can supply a
// deterministic virtual clock instead of wall time.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// SleepCtx sleeps for d or until ctx is done; it reports whether the
	// full sleep completed. This is the only sleep seam in the ingester.
	SleepCtx(ctx context.Context, d time.Duration) bool
}

// RealClock is the wall-clock implementation of Clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// JitterFunc returns a random duration in [0, max). The default uses
// math/rand/v2; simulations inject a seeded generator for determinism.
type JitterFunc func(max time.Duration) time.Duration

// RealJitter is the wall-clock jitter function.
func RealJitter(max time.Duration) time.Duration { return rand.N(max) }

// Options configure an Ingester.
type Options struct {
	// Clock supplies time and sleeping. Defaults to RealClock; simulations
	// inject a virtual clock so a test can cover hours of ingestion without
	// waiting for them.
	Clock Clock
	// Jitter randomises backoff so restarts don't thundering-herd a shared
	// RPC endpoint. Defaults to RealJitter; simulations inject a seeded
	// generator for determinism.
	Jitter JitterFunc
	// PollInterval is how long to sleep once caught up. Default 5s.
	PollInterval time.Duration
	// StartLedger, when non-zero, overrides the cold-start position.
	StartLedger uint32
	// StartLedgerRaw holds the raw START_LEDGER value from the
	// environment so the ingester can parse relative offsets (e.g.
	// "latest-1000") at runtime when the RPC client is available.
	StartLedgerRaw string
	// RetentionLedgers is how far behind the latest ledger a cold start
	// reaches when StartLedger is unset. Default 17280 (~24h).
	RetentionLedgers uint32
	// PageLimit is the getEvents pagination limit per request. Default 1000.
	PageLimit uint
	// WriteBatchSize is the maximum number of events written in one store
	// operation. Default 1000.
	WriteBatchSize uint
	// MaxEventsPerCycle caps the number of events a single runOnce cycle
	// may process, bounding memory and per-cycle latency on busy chains.
	// When the cap is hit mid-window the sweep stops issuing further
	// requests and the ingest frontier is left where it was, so the next
	// cycle re-scans the remainder of the window (idempotent upserts make
	// the overlap harmless). In singlePage mode the pagination limit is
	// clamped down to the cap so the page itself cannot exceed it.
	//
	// Zero (the default) disables the cap — behavior identical to builds
	// before this option existed.
	MaxEventsPerCycle uint
	// BatchSize caps how many events a single store write (UpsertEvents
	// call) may carry, splitting a fetched page into smaller chunks. This
	// keeps high-volume chains from landing one giant batch on the DB at
	// once: many small writes are kinder to Postgres than one huge one
	// when events are dense.
	//
	// Zero (the default) disables splitting entirely — each page is
	// persisted in a single write exactly as before this option existed
	// (bit-for-bit backward compatible).
	//
	// When BatchSize > 0 AND BatchTargetLatency > 0 the controller becomes
	// adaptive: it shrinks the chunk size below BatchSize and inserts
	// backpressure sleeps whenever write latency exceeds the budget, then
	// grows back once the store catches up. See BatchTargetLatency.
	BatchSize uint
	// BatchTargetLatency is the per-WRITE latency budget at the maximum
	// batch size (BatchSize). The controller derives a per-event budget
	// from it and compares against an EWMA of measured per-event write
	// latency: when writes run over budget it halves the chunk size and
	// sleeps between writes (backpressure); when they comfortably beat
	// the budget it grows the chunk size back toward BatchSize.
	//
	// Zero disables the adaptive/backpressure machinery (the chunk size
	// stays fixed at BatchSize). Only meaningful when BatchSize > 0.
	BatchTargetLatency time.Duration
	// BatchMaxBackoff caps the backpressure sleep the controller inserts
	// between writes when the store is running over the latency budget,
	// so a severely degraded database cannot stretch a single ingestion
	// cycle for minutes. Default 1s when batching is enabled.
	BatchMaxBackoff time.Duration
	// MaxBackoff caps the error backoff. Default 1m.
	MaxBackoff time.Duration
	// JitterMin and JitterMax bound the random jitter added to error
	// backoffs. When JitterMax is zero, jitter remains proportional to the
	// current backoff as it was before these options existed.
	JitterMin time.Duration
	JitterMax time.Duration
	// SweepWindow bounds the ledger range scanned per pass when the watched
	// list needs more than one getEvents request (see buildFilterBatches).
	// Default 1000 ledgers.
	SweepWindow uint32
	// LagWarnLedgers triggers a warn-level log when (latest ledger − last
	// ingested ledger) exceeds this threshold on a poll cycle. The check
	// uses hysteresis so the warn fires once on crossing and an info
	// "recovered" line fires once when the gap closes.
	//
	// Zero means the alarm is disabled (no log lines, no metric updates);
	// intended use is for tests and operators with their own monitoring in
	// place. The env-config layer (internal/config) supplies the
	// operational default of 100, so this struct does NOT re-default a
	// zero value — that's deliberate so tests/MSG-built Ingester instances
	// honor an explicit "0 = off" without surprise.
	LagWarnLedgers uint32
	// LagMetrics is the optional sink for ingest-lag signals. The default
	// is a no-op so the alarm works end-to-end as log-only today, and a
	// future Prometheus /metrics endpoint can opt in by passing a real
	// implementation here — main.go wires the no-op until that endpoint
	// lands. SetLagging is called on every poll cycle so the gauge
	// reflects the current state even when it doesn't change, so stale
	// values cannot accumulate across restarts.
	LagMetrics LagMetrics
	// SweepConcurrency is the maximum number of filter batches a
	// windowSweep will fetch in parallel. Default 1 (sequential), so
	// deployment with >25 watched contracts keeps the existing linear
	// latency profile unless an operator opts into concurrency. The
	// per-client RPC interval limiter still governs total request rate,
	// so a higher value helps only against private RPCs that have
	// headroom past the public ceiling.
	SweepConcurrency int
	// ReorgConfirmationWindow is the number of ledgers behind the ingest
	// frontier that the Run loop periodically re-scans for RPC-side
	// reorgs. Zero disables reorg repair entirely. Default 64.
	ReorgConfirmationWindow uint32
	// ReorgRescanInterval is how often the Run loop re-scans the recent
	// finalized window for reorg repair. Default 1 minute. Smaller
	// values mean replays-of-truth are caught faster at the cost of
	// extra RPC requests per idle cycle.
	ReorgRescanInterval time.Duration
	// Network is the logical network name this ingester is responsible for
	// (e.g. "mainnet", "testnet"). Empty means callers should treat it as
	// the store default ("default").
	Network string
}

// LagMetrics is the optional sink for ingest-lag signals. The Ingester
// calls SetLagging on every poll cycle with the current hysteresis
// state; the implementation can publish the value however it sees fit
// (Prometheus gauge, /stats JSON field, etc.). A no-op default lives
// in noopLagMetrics.
type LagMetrics interface {
	SetLagging(lagging bool)
}

// noopLagMetrics is the default LagMetrics implementation; it discards
// every signal so callers don't need to think about it.
type noopLagMetrics struct{}

// SetLagging discards the lag alarm state. Part of the LagMetrics
// interface; the no-op variant is the default in Options.applyDefaults.
func (noopLagMetrics) SetLagging(bool) {}

func (o *Options) applyDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.RetentionLedgers == 0 {
		o.RetentionLedgers = 17280
	}
	if o.PageLimit == 0 {
		o.PageLimit = 1000
	}
	if o.WriteBatchSize == 0 {
		o.WriteBatchSize = 1000
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Minute
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MinBackoff > o.MaxBackoff {
		o.MinBackoff = o.MaxBackoff
	}
	if o.JitterMin < 0 {
		o.JitterMin = 0
	}
	if o.JitterMax < 0 {
		o.JitterMax = 0
	}
	if o.JitterMax > 0 && o.JitterMin > o.JitterMax {
		o.JitterMin = o.JitterMax
	}
	if o.SweepWindow == 0 {
		o.SweepWindow = 1000
	}
	// BatchMaxBackoff is only meaningful when batching is on; the default
	// (1s) caps how long a single backpressure sleep can stretch a cycle
	// when the DB is badly behind. When batching is off the field is
	// simply never read.
	if o.BatchMaxBackoff <= 0 {
		o.BatchMaxBackoff = time.Second
	}
	if o.LagMetrics == nil {
		o.LagMetrics = noopLagMetrics{}
	}
	if o.Clock == nil {
		o.Clock = RealClock{}
	}
	if o.Jitter == nil {
		o.Jitter = RealJitter
	}
	// LagWarnLedgers intentionally NOT defaulted to 100 here: see
	// Options.LagWarnLedgers doc comment. The env-config layer applies
	// the 100 default; Ingester callers who want any other behavior
	// (including disabled=0) get exactly what they ask for.
	if o.SweepConcurrency < 1 {
		o.SweepConcurrency = 1
	}
	// 0 is the documented "disabled" sentinel for the reorg window, so we
	// only fill in the default rescan interval when the feature is on.
	if o.ReorgConfirmationWindow > 0 && o.ReorgRescanInterval <= 0 {
		o.ReorgRescanInterval = time.Minute
	}
}

// EventNotifier is notified after events are persisted so external
// systems (webhooks, SSE, etc.) can react without blocking ingestion.
type EventNotifier interface {
	NotifyEvents(ctx context.Context, events []store.Event)
}

// Ingester pages events out of the RPC and into the store.
type Ingester struct {
	client  rpc.Client
	store   store.Store
	decoder decode.Decoder
	log     *slog.Logger
	opts    Options
	// tracer emits OpenTelemetry spans around each ingest cycle. It is
	// always non-nil (noop by default) so call sites never need a guard.
	tracer trace.Tracer

	// lagging is the hysteresis state for the ingest-lag alarm. It is
	// mutated only from the Run goroutine. Crossing the threshold flips
	// it from false→true and emits a single warn-level log on the
	// transition; crossing back flips true→false with an info-level
	// "recovered" log; in-between cycles on either side stay quiet.
	//
	// Invariant: read/written only from the Run() loop body. A future
	// /admin or Signals() accessor MUST either move the alarm into a
	// mutex/atomic or document that the new caller is also single-
	// threaded relative to Run().
	lagging bool
	bcast   *broadcast.Broadcaster
	// notifier fans ingested events out to subscribers; optional, nil
	// means no notification.
	notifier EventNotifier
	// deadLetterStore receives events that fail to decode or persist so
	// a poison event no longer stalls the loop. nil means no
	// dead-lettering — the cycle aborts on the first error as before.
	deadLetterStore DeadLetterSink
	// pollInterval is the live-adjustable poll interval, stored as
	// nanoseconds so SetPollInterval can update it from any goroutine
	// (e.g. a SIGHUP config-reload handler) without a lock, while Run's
	// idle sleep reads the current value fresh on every cycle via
	// PollInterval. Seeded from opts.PollInterval in New; opts.PollInterval
	// itself is left untouched and only reflects the value the Ingester
	// was constructed with.
	pollInterval atomic.Int64
}

type networkStateStore interface {
	GetIngestionStateForNetwork(context.Context, string) (store.IngestionState, error)
}

func (ing *Ingester) getIngestionState(ctx context.Context) (store.IngestionState, error) {
	if scoped, ok := ing.store.(networkStateStore); ok {
		return scoped.GetIngestionStateForNetwork(ctx, ing.opts.Network)
	}
	return ing.store.GetIngestionState(ctx)
}

// New wires an Ingester.
func New(client rpc.Client, st store.Store, dec decode.Decoder, log *slog.Logger, opts Options) *Ingester {
	opts.applyDefaults()
	ing := &Ingester{
		client:  client,
		store:   st,
		decoder: dec,
		log:     log,
		opts:    opts,
		tracer:  noop.NewTracerProvider().Tracer("github.com/sorotrail/sorotrail/internal/ingester"),
	}
	ing.pollInterval.Store(int64(opts.PollInterval))
	return ing
}

// PollInterval returns the poll interval Run currently sleeps for between
// caught-up cycles. It reflects the live value: the construction-time
// default until SetPollInterval is called, and the most recently applied
// value afterward.
func (ing *Ingester) PollInterval() time.Duration {
	return time.Duration(ing.pollInterval.Load())
}

// SetPollInterval updates the poll interval Run uses for its idle sleep,
// effective on the next cycle boundary — no restart required. It validates
// d and leaves the current interval unchanged if d is not positive, so a
// rejected config-reload attempt (e.g. via SIGHUP) never puts the ingester
// into a busy-loop or a permanently-stalled state.
func (ing *Ingester) SetPollInterval(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("poll interval must be positive, got %s", d)
	}
	ing.pollInterval.Store(int64(d))
	return nil
}

// WithTracer attaches an OpenTelemetry tracer. Each runOnce cycle emits
// an ingester.poll_cycle span with ingester.fetch_page and
// ingester.persist_events children; without a tracer the noop provider
// makes every span a zero-cost no-op.
func (ing *Ingester) WithTracer(t trace.Tracer) *Ingester {
	if t != nil {
		ing.tracer = t
	}
	return ing
}

// WithBroadcaster attaches a live event broadcaster so ingested events are
// pushed to streaming subscribers.
func (ing *Ingester) WithBroadcaster(b *broadcast.Broadcaster) *Ingester {
	ing.bcast = b
	return ing
}

// SetNotifier attaches an optional EventNotifier that is called after
// every successful event persistence. When nil (the default) no
// notification is sent — the ingester behaves exactly as before.
func (ing *Ingester) SetNotifier(n EventNotifier) {
	ing.notifier = n
}

// SetDeadLetterSink wires the store that receives events the ingester
// fails to persist. nil means dead-lettering is disabled and the
// pre-issue behavior (fail the cycle) is preserved.
func (ing *Ingester) SetDeadLetterSink(s DeadLetterSink) { ing.deadLetterStore = s }

// DeadLetterSink is the minimal interface the ingester uses to record
// dead-letter rows. Decoupled from store.Store because tests need a
// simpler in-memory implementation.
type DeadLetterSink interface {
	DeadLetterEvent(ctx context.Context, ev store.DeadLetterInput) (store.DeadLetter, error)
}

// logAttrs renders the effective Options as structured log fields. It is
// called after applyDefaults, so the values shown are what the ingester
// will actually run with — an operator reading the startup line sees the
// applied defaults (5s poll, 1000 page limit, …) rather than the raw env.
func (o *Options) logAttrs() []any {
	return []any{
		"poll_interval", o.PollInterval,
		"page_limit", o.PageLimit,
		"max_events_per_cycle", o.MaxEventsPerCycle,
		"batch_size", o.BatchSize,
		"batch_target_latency", o.BatchTargetLatency,
		"batch_max_backoff", o.BatchMaxBackoff,
		"start_ledger", o.StartLedger,
		"retention_ledgers", o.RetentionLedgers,
		"sweep_window", o.SweepWindow,
		"sweep_concurrency", o.SweepConcurrency,
		"max_backoff", o.MaxBackoff,
		"min_backoff", o.MinBackoff,
		"jitter_min", o.JitterMin,
		"jitter_max", o.JitterMax,
		"lag_warn_ledgers", o.LagWarnLedgers,
		"reorg_confirmation_window", o.ReorgConfirmationWindow,
	}
}

func (ing *Ingester) backoffSleep(backoff time.Duration) time.Duration {
	jitterMin := ing.opts.JitterMin
	jitterMax := ing.opts.JitterMax
	if jitterMax == 0 {
		jitterMax = backoff / 2
	}
	if jitterMax <= jitterMin {
		return backoff/2 + jitterMin
	}
	return backoff/2 + jitterMin + ing.opts.Jitter(jitterMax-jitterMin)
}

// Run polls until ctx is canceled. Errors are logged and retried with
// exponential backoff; the only terminal condition is context cancellation.
//
// On a clean cycle the loop also performs an optional reorg re-scan over
// the ledger range [frontier-confWindow, frontier-1] using the existing
// ReingestRange plumbing, so RPC-side revisions in that window are
// upsert-replaced before they can drift away from a long cwd.
//
// Graceful shutdown: ctx cancellation propagates into the RPC and store
// layers via context-aware calls, so an in-flight page finishes its
// round trip or fails cleanly. The ingester's Run loop returns
// ctx.Err() at the next iteration boundary; ingestion_state is only
// advanced at the end of a successful runOnce, so a cancelled cycle
// leaves the frontier at the last fully-persisted ledger — restart
// resumes from there with idempotent upserts covering any half-done
// batch. There is no place in the loop where a partial state lands in
// the store, so a tranquil Ctrl-C / SIGTERM never truncates a write.
// Startup/shutdown logging: Run emits one "ingester started" line carrying
// the effective (post-defaults) configuration, and one "ingester stopped"
// line on every exit path — clean cancellation, RPC failure backoff exit,
// or error return — so an operator correlating logs can see exactly when
// the loop was live and with what knobs, without grepping config dumps.
func (ing *Ingester) Run(ctx context.Context) (err error) {
	ing.log.Info("ingester started", ing.opts.logAttrs()...)
	defer func() {
		if err != nil {
			ing.log.Info("ingester stopped", "reason", err.Error())
			return
		}
		ing.log.Info("ingester stopped")
	}()

	backoff := ing.opts.MinBackoff
	lastReorgRescanAt := time.Time{}
	for {
		caughtUp, err := ing.runOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			// Lag alarm runs BEFORE the backoff so a stuck indexer
			// doesn't wait out MaxBackoff before the operator sees
			// it.
			ing.checkLag(ctx)
			// Jittered exponential backoff so restarts don't thundering-herd
			// a shared endpoint.
			sleep := ing.backoffSleep(backoff)
			ing.log.Error("ingestion pass failed", "error", err, "retry_in", sleep)
			if !ing.opts.Clock.SleepCtx(ctx, sleep) {
				return ctx.Err()
			}
			if backoff *= 2; backoff > ing.opts.MaxBackoff {
				backoff = ing.opts.MaxBackoff
			}
		default:
			// Lag alarm runs BEFORE the sleep so the operator sees it
			// on the cycle that noticed the gap, not PollInterval
			// later.
			ing.checkLag(ctx)
			backoff = ing.opts.MinBackoff
			if caughtUp {
				// PollInterval (not opts.PollInterval) so a live update via
				// SetPollInterval — e.g. a SIGHUP config reload — takes
				// effect starting with this sleep, no restart required.
				if !ing.opts.Clock.SleepCtx(ctx, ing.PollInterval()) {
					return ctx.Err()
				}
			}
			// Reorg rescan: only on successful cycles, and gated by the
			// configured cadence. The timestamp advances on every attempt
			// (success or fail) so a flapping RPC doesn't make every
			// subsequent cycle retry the rescan — the next interval
			// decides. Runs synchronously because reorg repair is bounded
			// by the confirmation window (small by default) and trivial
			// overlapping with ingest is acceptable. Cancelled mid-rescan
			// is just an early return; partial ReplaceEventsInRange is
			// undone by its transactional delete+insert pair.
			if ing.opts.ReorgConfirmationWindow > 0 &&
				time.Since(lastReorgRescanAt) >= ing.opts.ReorgRescanInterval {
				lastReorgRescanAt = time.Now()
				if rerr := ing.rescanForReorg(ctx); rerr != nil {
					if ctx.Err() == nil {
						ing.log.Warn("reorg rescan failed; will retry next interval", "error", rerr)
					}
				}
			}
		}
	}
}

// rescanForReorg performs one reorg-detection pass over the recent
// finalized window. It uses ReingestRange, which fans out across the
// ingester's filter batches, re-fetches events for the closed range
// [frontier-confWindow, frontier-1], then calls ReplaceEventsInRange
// so orphans are deleted and same-ID rows are updated. Returns the
// number of events the RPC reported for the range as the "evidence
// the RPC has something to say"; the actual row mutation is opaque to
// the caller. A no-op return is fine: it means there's not yet enough
// history to have a finalized window.
func (ing *Ingester) rescanForReorg(ctx context.Context) error {
	state, err := ing.getIngestionState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("loading ingestion state for reorg rescan: %w", err)
	}
	if state.LastIngestedLedger <= 0 {
		return nil
	}
	// Frontier must be at least confWindow+1 ledgers in so we have a
	// non-empty finalized window behind it.
	frontier := uint32(state.LastIngestedLedger)
	if frontier <= ing.opts.ReorgConfirmationWindow {
		return nil
	}
	to := frontier - 1
	from := to - ing.opts.ReorgConfirmationWindow + 1
	ing.log.Debug("reorg rescan starting", "from", from, "to", to)
	n, err := ing.ReingestRange(ctx, ing.client, from, to)
	if err != nil {
		return fmt.Errorf("ReingestRange [%d,%d]: %w", from, to, err)
	}
	if n > 0 {
		ing.log.Info("reorg rescan rewrote", "from", from, "to", to, "events_observed", n)
	}
	return nil
}

// runOnce advances ingestion by one step and reports whether we are caught
// up with the chain. With at most one request's worth of filters it ingests
// a single page (resumable via the persisted cursor); with more watched
// contracts than one request allows it sweeps a bounded ledger window once
// per filter batch.
//
// Unwatched mode (empty WATCHED_CONTRACTS) uses the single global
// ingestion_state row — backward-compatible behavior unchanged.
// Watched mode uses per-contract cursors in the contract_cursors table.
func (ing *Ingester) runOnce(ctx context.Context) (caughtUp bool, err error) {
	ctx, span := ing.tracer.Start(ctx, "ingester.poll_cycle")
	defer span.End()
	startLedger, cursor, err := ing.resolvePosition(ctx)
	if err != nil {
		return false, err
	}
	batches, err := ing.buildFilterBatches(ctx)
	if err != nil {
		return false, err
	}
	if len(batches) == 1 {
		return ing.singlePage(ctx, startLedger, cursor, batches[0])
	}
	return ing.windowSweep(ctx, startLedger, batches)
}

func (ing *Ingester) singlePage(ctx context.Context, startLedger uint32, cursor string, filters []rpc.EventFilter) (bool, error) {
	limit := ing.effectivePageLimit()
	fetchCtx, fetchSpan := ing.tracer.Start(ctx, "ingester.fetch_page")
	resp, err := ing.client.GetEvents(fetchCtx, rpc.GetEventsRequest{
		StartLedger: startLedger,
		Filters:     filters,
		Pagination:  &rpc.Pagination{Cursor: cursor, Limit: limit},
	})
	fetchSpan.End()
	if rpc.IsFailoverReanchor(err) {
		ing.log.Warn("failover re-anchor: discarding cursor, re-scanning from last ingested ledger")
		ing.discardCursor(ctx)
		return false, err
	}
	if rpc.IsLedgerOutOfRange(err) {
		return false, ing.reclampToOldest(ctx, startLedger)
	}
	if err != nil {
		return false, fmt.Errorf("getEvents from ledger %d: %w", startLedger, err)
	}

	if err := ing.persistEvents(ctx, resp.Events, resp.LatestLedger); err != nil {
		return false, err
	}

	state, caughtUp := nextState(resp, limit)
	if state.LastCursor == "" && state.LastIngestedLedger <= 0 {
		state.LastIngestedLedger = int64(startLedger) - 1
	}
	state.Network = ing.opts.Network
	if err := ing.store.SaveIngestionState(ctx, state); err != nil {
		return false, err
	}
	ing.setIngestionLag(int64(resp.LatestLedger), state.LastIngestedLedger)
	return caughtUp, nil
}

// ReingestRange re-fetches events for the closed range [fromLedger, toLedger].
func (ing *Ingester) ReingestRange(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32) (int, error) {
	if fromLedger > toLedger {
		return 0, fmt.Errorf("ReingestRange: from %d > %d", fromLedger, toLedger)
	}
	if client == nil {
		client = ing.client
	}
	filters, err := ing.BuildFilterBatches(ctx)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, batch := range filters {
		inserted, err := ing.reingestBatch(ctx, client, fromLedger, toLedger, batch)
		if err != nil {
			return repaired, err
		}
		repaired += inserted
	}
	return repaired, nil
}

func (ing *Ingester) reingestBatch(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32, batch []rpc.EventFilter) (int, error) {
	endLedgerExcl := toLedger + 1
	var collected []rpc.Event
	cursor := ""
	for {
		resp, err := client.GetEvents(ctx, rpc.GetEventsRequest{
			StartLedger: fromLedger,
			EndLedger:   endLedgerExcl,
			Filters:     batch,
			Pagination:  &rpc.Pagination{Cursor: cursor, Limit: ing.opts.PageLimit},
		})
		if rpc.IsLedgerOutOfRange(err) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("ReingestRange getEvents [%d,%d]: %w", fromLedger, toLedger, err)
		}
		for _, e := range resp.Events {
			if e.Ledger <= toLedger {
				collected = append(collected, e)
			}
		}
		if len(resp.Events) == 0 {
			break
		}
		last := resp.Events[len(resp.Events)-1]
		// A short page is only terminal when the RPC supplies no
		// continuation cursor: some endpoints return fewer results than
		// requested while more data remains, and stopping here would make
		// ReplaceEventsInRange delete rows that were never re-fetched.
		if uint(len(resp.Events)) < ing.opts.PageLimit && resp.Cursor == "" {
			break
		}
		if last.Ledger > toLedger {
			break
		}
		next := resp.Cursor
		if next == "" {
			next = last.CursorValue()
		}
		if next == cursor {
			break // non-advancing cursor: stop rather than spin forever
		}
		cursor = next
	}
	storeEvents := make([]store.Event, 0, len(collected))
	for _, re := range collected {
		ev, err := ing.toStoreEvent(re)
		if err != nil {
			return 0, err
		}
		ev.Network = ing.opts.Network
		storeEvents = append(storeEvents, ev)
	}
	if err := ing.store.ReplaceEventsInRange(ctx, storeEvents, int64(fromLedger), int64(toLedger)); err != nil {
		return 0, fmt.Errorf("ReplaceEventsInRange [%d,%d]: %w", fromLedger, toLedger, err)
	}
	return len(storeEvents), nil
}

// BuildFilterBatches converts the watched-contract list into getEvents
// filter batches respecting the RPC caps.
func (ing *Ingester) BuildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error) {
	return ing.buildFilterBatches(ctx)
}

// PageLimit returns the getEvents pagination cap.
func (ing *Ingester) PageLimit() uint { return ing.opts.PageLimit }

// WriteBatchSize returns the maximum number of events written in one store
// operation.
func (ing *Ingester) WriteBatchSize() uint { return ing.opts.WriteBatchSize }

// MaxEventsPerCycle returns the per-cycle event cap; 0 means disabled.
func (ing *Ingester) MaxEventsPerCycle() uint { return ing.opts.MaxEventsPerCycle }

// effectivePageLimit is the pagination limit for the first getEvents
// request of a cycle: PageLimit clamped down to the per-cycle event cap
// when one is set and smaller. Without a cap this is exactly PageLimit,
// so behavior is unchanged for deployments that don't opt in.
func (ing *Ingester) effectivePageLimit() uint {
	if ing.opts.MaxEventsPerCycle > 0 && ing.opts.MaxEventsPerCycle < ing.opts.PageLimit {
		return ing.opts.MaxEventsPerCycle
	}
	return ing.opts.PageLimit
}

// newCycleBudget allocates the shared per-cycle event budget used by the
// window-sweep request chains. A nil return means no cap is configured
// and every batch pages to completion as before.
func (ing *Ingester) newCycleBudget() *atomic.Int64 {
	if ing.opts.MaxEventsPerCycle == 0 {
		return nil
	}
	var budget atomic.Int64
	budget.Store(int64(ing.opts.MaxEventsPerCycle))
	return &budget
}

// Network returns the network this ingester is responsible for.
func (ing *Ingester) Network() string { return ing.opts.Network }

func nextState(resp rpc.GetEventsResponse, pageLimit uint) (store.IngestionState, bool) {
	caughtUp := uint(len(resp.Events)) < pageLimit

	cursor := resp.Cursor
	if cursor == "" && len(resp.Events) > 0 {
		cursor = resp.Events[len(resp.Events)-1].CursorValue()
	}

	lastLedger := int64(resp.LatestLedger)
	if len(resp.Events) > 0 {
		lastLedger = int64(resp.Events[len(resp.Events)-1].Ledger)
	}

	now := time.Now().UTC()
	if cursor != "" {
		return store.IngestionState{LastIngestedLedger: lastLedger, LastCursor: cursor, LastSuccessfulPoll: &now}, caughtUp
	}
	return store.IngestionState{LastIngestedLedger: int64(resp.LatestLedger) - 1, LastSuccessfulPoll: &now}, caughtUp
}

// windowSweep ingests the ledger window [start, end] by paging each filter
// batch through it separately, then advances state past the window. One
// shared cursor can't track several concurrent request chains, so within a
// window each batch pages to completion in memory; idempotent upserts make
// re-scanning after a crash harmless.
//
// Concurrency: when SweepConcurrency > 1 the batches are fanned out via
// errgroup.SetLimit so a deployment watching 100 contracts no longer pays
// linear latency in the number of request chains. The HTTPClient's
// interval limiter still serializes the actual RPC round trips, so total
// request rate is unchanged — a higher value helps only when the RPC has
// headroom past the public ceiling. SweepConcurrency=1 keeps the
// historical sequential behavior bit-for-bit.
//
// Failure modes:
//   - one batch failing fails the whole sweep (errgroup returns the first
//     error); the window is NOT marked complete so the next runOnce retries
//     the entire [start, end] window. Events already persisted by
//     completed batches are rewritten idempotently.
//   - one batch hitting IsLedgerOutOfRange fails the sweep the same way:
//     we can't trust the prefix of the window without the rest, and the
//     retry-after-reclamp behavior belongs at the parent level. Cancelled
//     mid-sweep leaves the frontier at the last fully-completed window so
//     restart resumes from there (idempotent upserts cover the in-flight
//     rows on the next pass).
func (ing *Ingester) windowSweep(ctx context.Context, start uint32, batches [][]rpc.EventFilter) (bool, error) {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return false, fmt.Errorf("getHealth for sweep window: %w", err)
	}
	if start > health.LatestLedger {
		return true, nil
	}
	end := min(start+ing.opts.SweepWindow-1, health.LatestLedger)

	// Per-cycle event cap: all batches share one budget. capHit is set
	// out-of-band (same pattern as ledgerOutOfRange) because errgroup
	// cancellation can mask which batch observed the exhausted budget.
	remaining := ing.newCycleBudget()
	var capHit atomic.Bool
	// When SweepConcurrency > 1, two batches run in parallel. If one
	// returns IsLedgerOutOfRange, errgroup cancels the others and the
	// first goroutine to surface an error to g.Wait() wins — which means
	// a still-in-flight batch's ctx.Canceled can mask a sibling's
	// IsLedgerOutOfRange signal and we'd skip the reclamp. Flag the
	// condition out-of-band so the post-Wait check sees it regardless
	// of which error races out first.
	var ledgerOutOfRange atomic.Bool
	g, gctx := errgroup.WithContext(ctx)
	// SweepConcurrency also doubles as the DB backpressure knob: each
	// sweepBatch goroutine calls persistEvents (a synchronous UpsertEvents)
	// before fetching its next page, so this same SetLimit caps how many
	// UpsertEvents calls can be in flight at once — a goroutine can't start
	// a new RPC fetch (and so can't queue a new write) until its slot frees
	// up, which requires its current write to finish. No separate DB
	// semaphore is needed; a slow store just makes each goroutine's cycle
	// take longer, which throttles the RPC fetch rate to match.
	g.SetLimit(ing.opts.SweepConcurrency)
	for _, filters := range batches {
		filters := filters
		g.Go(func() error {
			return ing.sweepBatch(gctx, &ledgerOutOfRange, remaining, &capHit, start, end, filters)
		})
	}
	if err := g.Wait(); err != nil {
		if ledgerOutOfRange.Load() || rpc.IsLedgerOutOfRange(err) {
			return false, ing.reclampToOldest(ctx, start)
		}
		return false, err
	}
	if capHit.Load() {
		// The event budget ran out mid-window. Leave the frontier where
		// it was: events persisted so far are safe (idempotent upserts),
		// and the next cycle re-scans [start,end] to cover the rest.
		ing.log.Info("per-cycle event cap reached; window incomplete",
			"window_start", start, "window_end", end,
			"max_events_per_cycle", ing.opts.MaxEventsPerCycle)
		return false, nil
	}

	lastIngested := int64(end)
	if end >= health.LatestLedger {
		lastIngested = int64(end) - 1
	}
	now := time.Now().UTC()
	err = ing.store.SaveIngestionState(ctx, store.IngestionState{Network: ing.opts.Network, LastIngestedLedger: lastIngested, LastSuccessfulPoll: &now})
	if err != nil {
		return false, err
	}
	ing.setIngestionLag(int64(health.LatestLedger), lastIngested)
	return end >= health.LatestLedger, nil
}

// sweepBatch pages one filter batch through [start, end]. Errors are
// returned to the errgroup; ctx cancellation aborts the inner pagination
// loop (GetEvents respects ctx) so a graceful shutdown doesn't strand
// half-paged batches.
//
// remaining is the shared per-cycle event budget (nil = unlimited). Before
// each request the batch claims min(PageLimit, remaining) events; when the
// budget is exhausted it stops issuing requests and sets capHit so the
// caller leaves the window incomplete rather than advancing the frontier
// past data that was never fetched.
//
// ledgerOutOfRange is a side-channel for IsLedgerOutOfRange that survives
// the errgroup canceling sibling goroutines — see windowSweep's comment.
//
// Short pages: a page with fewer events than requested is treated as
// terminal only when it carries no top-level cursor. An RPC that returns
// short-but-cursored pages (internal caps, filtering, load shedding) is
// paged to completion instead, so the window is never marked complete over
// unfetched data; a non-advancing cursor terminates the loop defensively.
func (ing *Ingester) sweepBatch(ctx context.Context, ledgerOutOfRange *atomic.Bool, remaining *atomic.Int64, capHit *atomic.Bool, start, end uint32, filters []rpc.EventFilter) error {
	cursor := ""
	for {
		limit := int64(ing.opts.PageLimit)
		if remaining != nil {
			r := remaining.Load()
			if r <= 0 {
				capHit.Store(true)
				return nil
			}
			limit = min(limit, r)
		}
		fetchCtx, fetchSpan := ing.tracer.Start(ctx, "ingester.fetch_page")
		resp, err := ing.client.GetEvents(fetchCtx, rpc.GetEventsRequest{
			StartLedger: start,
			EndLedger:   end + 1, // endLedger is exclusive
			Filters:     filters,
			Pagination:  &rpc.Pagination{Cursor: cursor, Limit: uint(limit)},
		})
		fetchSpan.End()
		if rpc.IsLedgerOutOfRange(err) {
			ledgerOutOfRange.Store(true)
			return err
		}
		if rpc.IsFailoverReanchor(err) {
			ing.log.Warn("failover re-anchor during window sweep: discarding cursor")
			ing.discardCursor(ctx)
			return err
		}
		if err != nil {
			return fmt.Errorf("getEvents sweep [%d,%d]: %w", start, end, err)
		}
		if err := ing.persistEvents(ctx, resp.Events, resp.LatestLedger); err != nil {
			return err
		}
		if remaining != nil {
			remaining.Add(-int64(len(resp.Events)))
		}
		if len(resp.Events) == 0 {
			return nil
		}
		// Cursor requests can't carry the ledger range, so enforce the
		// window bound here once a page crosses it.
		if resp.Events[len(resp.Events)-1].Ledger > end {
			return nil
		}
		next := resp.Cursor
		if next == "" {
			next = resp.Events[len(resp.Events)-1].CursorValue()
		}
		short := uint(len(resp.Events)) < uint(limit)
		if short && resp.Cursor == "" {
			// A short page with no continuation hint really is the end
			// of this batch's data.
			return nil
		}
		if next == cursor {
			// The RPC keeps handing back the same cursor; stopping beats
			// spinning forever on a page that never advances.
			return nil
		}
		if short {
			// Short page but the RPC says more data follows: tolerate the
			// shortfall and keep paging. Declaring the batch done here
			// would let windowSweep advance the frontier past events that
			// were never fetched.
			ing.log.Debug("short getEvents page carried a cursor; continuing",
				"got", len(resp.Events), "requested", limit)
		}
		cursor = next
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// removeDuplicateEvents returns a slice of events with duplicate IDs removed,
// keeping the first occurrence of each ID. This is needed so that when the
// RPC returns the same event ID multiple times within a single page (e.g.
// due to RPC quirks or re-fetches), the ingester does not write duplicates
// in a single batch before the store's own idempotent upserts can absorb them.
func removeDuplicateEvents(events []store.Event) []store.Event {
	seen := make(map[string]bool, len(events))
	result := events[:0]
	for _, ev := range events {
		if !seen[ev.ID] {
			seen[ev.ID] = true
			result = append(result, ev)
		}
	}
	return result
}

func (ing *Ingester) persistEvents(ctx context.Context, rpcEvents []rpc.Event, latestLedger uint32) error {
	if len(rpcEvents) == 0 {
		return nil
	}
	events := make([]store.Event, 0, len(rpcEvents))
	for _, re := range rpcEvents {
		ev, err := ing.toStoreEvent(re)
		if err != nil {
			// Issue #131: a poison event must not stall the cycle. If a
			// dead-letter sink is wired, route the failing event + error
			// through it and continue with the rest of the page; otherwise
			// fall back to the legacy "abort the cycle" behavior so
			// unconfigured deployments catch the bug instead of silently
			// dropping events.
			if ing.deadLetterStore == nil {
				return err
			}
			dlCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if _, derr := ing.deadLetterStore.DeadLetterEvent(dlCtx, store.DeadLetterInput{
				EventID:    re.ID,
				ContractID: re.ContractID,
				Ledger:     int64(re.Ledger),
				Type:       re.Type,
				TxHash:     re.TxHash,
				TopicXDR:   re.Topic,
				ValueXDR:   re.Value,
				Err:        err,
			}); derr != nil {
				ing.log.Warn("dead-lettering failed", "event_id", re.ID, "error", derr)
			}
			cancel()
			continue
		}
		events = append(events, ev)
	}
	// Drop duplicate event IDs within this batch before writing, keeping the
	// first occurrence of each ID. This prevents a single batch from
	// containing duplicates before the store's idempotent upserts can absorb
	// the overlap on re-scans or restarts.
	events = removeDuplicateEvents(events)
	for start := 0; start < len(events); start += int(ing.opts.WriteBatchSize) {
		end := min(start+int(ing.opts.WriteBatchSize), len(events))
		if err := ing.persistEventBatch(ctx, events[start:end], rpcEvents[len(rpcEvents)-1].Ledger, latestLedger); err != nil {
			return err
		}
	}
	return nil
}

func (ing *Ingester) persistEventBatch(ctx context.Context, events []store.Event, throughLedger, latestLedger uint32) error {
	persistCtx, persistSpan := ing.tracer.Start(ctx, "ingester.persist_events")
	inserted, err := ing.writeEventsPersist(persistCtx, events)
	persistSpan.End()
	if err != nil {
		return err
	}

	if err := ing.indexEventAddresses(ctx, events); err != nil {
		ing.log.Error("indexing event addresses", "error", err)
	}

	ing.log.Info("ingested events",
		"count", len(events), "new", inserted,
		"through_ledger", throughLedger,
		"latest_ledger", latestLedger)

	if ing.bcast != nil {
		ing.bcast.Publish(ctx, events)
	}
	if ing.notifier != nil {
		ing.notifier.NotifyEvents(ctx, events)
	}
	return nil
}

// writeEventsPersist flushes decoded events to the store. With batching
// disabled (BatchSize == 0) it issues a single UpsertEvents for the whole
// slice — the exact behavior the ingester has always had. With batching
// enabled it walks the slice in chunks of up to the current adaptive batch
// size, and under latency pressure shrinks the chunk size and sleeps
// between chunks to give the DB room to drain.
//
// It returns the number of rows reported inserted (net-new) across all
// writes; the caller handles the post-persist work (address indexing,
// broadcast, notify, logging) that runs once regardless of how many writes
// a page split into.
func (ing *Ingester) writeEventsPersist(ctx context.Context, events []store.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	// Batching disabled: single write of the whole page, exactly as the
	// ingester has always done.
	if ing.opts.BatchSize == 0 {
		timer := prometheus.NewTimer(metrics.DBWriteLatency)
		inserted, err := ing.store.UpsertEvents(ctx, events)
		timer.ObserveDuration()
		if err != nil {
			return 0, err
		}
		metrics.EventsIngested.Add(float64(len(events)))
		metrics.EventBatchWrites.Inc()
		metrics.EventBatchSize.Set(float64(len(events)))
		return inserted, nil
	}

	bc := ing.newBatchController()
	metrics.EventBatchSize.Set(float64(bc.cur))
	var inserted int64
	for start := 0; start < len(events); {
		// Chunk no larger than the controller's current (possibly
		// shrunken) batch size.
		n := bc.cur
		end := start + n
		if end > len(events) {
			end = len(events)
		}
		chunk := events[start:end]
		start = end

		begin := ing.opts.Clock.Now()
		timer := prometheus.NewTimer(metrics.DBWriteLatency)
		got, err := ing.store.UpsertEvents(ctx, chunk)
		timer.ObserveDuration()
		latency := ing.opts.Clock.Now().Sub(begin)
		if err != nil {
			// A failed write aborts the page: the offending batch was not
			// committed, and the caller surfaces the error so runOnce
			// retries the whole page idempotently. Events already written
			// by earlier chunks are safe (idempotent upserts cover them).
			return inserted, err
		}
		inserted += got
		metrics.EventsIngested.Add(float64(len(chunk)))
		metrics.EventBatchWrites.Inc()

		// Backpressure: if this write ran over the latency budget, sleep
		// (and shrink the next chunk) before issuing the next write, so a
		// slow store gets a moment to drain instead of being hammered.
		backoff := bc.recordAndBackoff(len(chunk), latency)
		if backoff > 0 && start < len(events) {
			if !ing.opts.Clock.SleepCtx(ctx, backoff) {
				return inserted, ctx.Err()
			}
			metrics.EventBackpressure.Inc()
			metrics.EventBackpressureSeconds.Add(backoff.Seconds())
		}
	}
	return inserted, nil
}

// newBatchController builds a fresh batch controller from Options. It is
// created per persist call (never shared between goroutines) so concurrent
// windowSweep batches under SweepConcurrency > 1 each adapt independently
// of one another and no mutable state is written from two goroutines at
// once (the -race suite depends on that). Adaptation therefore carries
// across writes WITHIN a page — exactly the burst horizon the DB pressure
// problem lives on — not across pages.
func (ing *Ingester) newBatchController() *batchController {
	bc := &batchController{
		max:   int(ing.opts.BatchSize),
		cur:   int(ing.opts.BatchSize),
		maxBk: ing.opts.BatchMaxBackoff,
	}
	if ing.opts.BatchTargetLatency > 0 {
		bc.adaptive = true
		bc.targetPerEvent = ing.opts.BatchTargetLatency / time.Duration(ing.opts.BatchSize)
		if bc.targetPerEvent <= 0 {
			// Guard against a budget so small it rounds to zero: fall back
			// to a microsecond so a legitimate (tiny) budget never collapses
			// the controller into permanent immediate-backoff.
			bc.targetPerEvent = time.Microsecond
		}
		bc.ewma = bc.targetPerEvent
	}
	return bc
}

// batchController sizes the UpsertEvents chunks written for one fetched
// page and applies backpressure. Its state lives only for the duration of
// a single writeEventsPersist call, so it is not safe for cross-goroutine
// use by design — see newBatchController.
//
// Two knobs do different jobs:
//   - recordAndBackoff shrinks bc.cur below the configured max when the
//     EWMA of measured per-event write latency exceeds the derived budget,
//     so subsequent writes carry fewer rows.
//   - It returns a sleep duration so the caller can pause between writes
//     (backpressure), giving a slow store time to drain before the next
//     (already smaller) chunk.
//
// The controller grows bc.cur back toward max once writes are comfortably
// inside budget, so a store that recovers first reclaims its throughput.
type batchController struct {
	// max is the configured maximum chunk size; cur is the current
	// adaptive one (starts at max).
	max   int
	cur   int
	maxBk time.Duration
	// adaptive is true only when BatchTargetLatency > 0; when false the
	// controller still chunks at a fixed BatchSize but never shrinks or
	// sleeps.
	adaptive       bool
	targetPerEvent time.Duration
	ewma           time.Duration
}

// recordAndBackoff feeds one write's measured latency into the controller
// and returns how long to sleep before the next write (0 = none). With
// adaptation disabled it returns 0 always and the chunk size stays fixed at
// the configured maximum.
func (bc *batchController) recordAndBackoff(rows int, latency time.Duration) time.Duration {
	if !bc.adaptive || rows <= 0 || bc.cur <= 0 {
		return 0
	}

	perEvent := latency / time.Duration(rows)
	if bc.ewma <= 0 {
		bc.ewma = perEvent
	} else {
		// A fast EWMA (alpha 0.5) so the controller reacts to a burst
		// within a handful of writes instead of smoothing it away.
		bc.ewma = time.Duration(float64(perEvent)*0.5 + float64(bc.ewma)*0.5)
	}

	overBudget := bc.ewma > bc.targetPerEvent
	comfortablyUnder := bc.ewma < bc.targetPerEvent/2

	switch {
	case overBudget && bc.cur > 1:
		bc.cur /= 2
		metrics.EventBatchSize.Set(float64(bc.cur))
		return bc.maxBk
	case comfortablyUnder && bc.cur < bc.max:
		bc.cur *= 2
		if bc.cur > bc.max {
			bc.cur = bc.max
		}
		metrics.EventBatchSize.Set(float64(bc.cur))
		return 0
	case overBudget:
		// Already at the minimum chunk size: cannot shrink further, but
		// still back off before the next write.
		return bc.maxBk
	default:
		return 0
	}
}

func (ing *Ingester) resolvePosition(ctx context.Context) (startLedger uint32, cursor string, err error) {
	state, err := ing.getIngestionState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, "", err
	}
	if err == nil && state.LastCursor != "" {
		return 0, state.LastCursor, nil
	}
	if err == nil && state.LastIngestedLedger > 0 {
		return uint32(state.LastIngestedLedger) + 1, "", nil
	}

	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("getHealth for cold start: %w", err)
	}

	var resolved uint32
	if ing.opts.StartLedger > 0 {
		resolved = ing.opts.StartLedger
	} else if ing.opts.StartLedgerRaw != "" {
		resolved, err = resolveStartLedgerRaw(ing.opts.StartLedgerRaw, health)
		if err != nil {
			return 0, "", err
		}
	} else {
		start := int64(health.LatestLedger) - int64(ing.opts.RetentionLedgers)
		if oldest := int64(health.OldestLedger); start < oldest {
			start = oldest
		}
		if start < 2 {
			start = 2
		}
		resolved = uint32(start)
	}

	if (ing.opts.StartLedger > 0 || ing.opts.StartLedgerRaw != "") &&
		health.OldestLedger > 0 && resolved < health.OldestLedger {
		return 0, "", fmt.Errorf(
			"START_LEDGER %d is below the RPC's oldest retained ledger %d; events in the gap are unrecoverable",
			resolved, health.OldestLedger)
	}
	if resolved < 2 {
		resolved = 2
	}
	ing.log.Info("cold start", "start_ledger", resolved, "latest_ledger", health.LatestLedger)
	return resolved, "", nil
}

func resolveStartLedgerRaw(raw string, health rpc.Health) (uint32, error) {
	raw = strings.TrimSpace(raw)
	if n, err := strconv.ParseUint(raw, 10, 32); err == nil {
		return uint32(n), nil
	}
	if !strings.HasPrefix(strings.ToLower(raw), "latest-") {
		return 0, fmt.Errorf("START_LEDGER %q: not an absolute ledger number or relative offset", raw)
	}
	offsetStr := raw[len("latest-"):]
	offset, err := strconv.ParseUint(offsetStr, 10, 32)
	if err != nil || offset == 0 {
		return 0, fmt.Errorf("START_LEDGER %q: offset must be a positive integer", raw)
	}
	start := int64(health.LatestLedger) - int64(offset)
	if start < 2 {
		start = 2
	}
	return uint32(start), nil
}

func (ing *Ingester) reclampToOldest(ctx context.Context, requested uint32) error {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return fmt.Errorf("getHealth while re-clamping: %w", err)
	}
	ing.log.Warn("resume ledger fell outside RPC retention window; skipping ahead — events in the gap are lost",
		"requested_ledger", requested, "oldest_retained", health.OldestLedger)
	return ing.store.SaveIngestionState(ctx, store.IngestionState{
		Network:            ing.opts.Network,
		LastIngestedLedger: int64(health.OldestLedger) - 1,
	})
}

// discardCursor reads the persisted ingestion state and re-saves it without
// the cursor, so the next resolvePosition falls through to the
// ledger-based path. Idempotent upserts absorb the overlap from re-scanning.
//
// In windowSweep the internal cursor is never persisted (only
// LastIngestedLedger is saved at sweep end), so this is a defensive no-op
// in that path — it guards against any future change that might persist a
// cursor mid-sweep.
func (ing *Ingester) discardCursor(ctx context.Context) {
	state, err := ing.getIngestionState(ctx)
	if err != nil {
		ing.log.Warn("discardCursor: could not read state", "error", err)
		return
	}
	if state.LastCursor == "" {
		return
	}
	if err := ing.store.SaveIngestionState(ctx, store.IngestionState{
		Network:            ing.opts.Network,
		LastIngestedLedger: state.LastIngestedLedger,
	}); err != nil {
		ing.log.Warn("discardCursor: could not save state", "error", err)
	}
}

// checkLag evaluates the ingest-lag alarm against the chain head and
// the persisted ingestion state, with hysteresis so the warn fires
// exactly once on crossing and an info "recovered" line fires exactly
// once when the gap closes. Run calls it every poll cycle.
//
// The hysteresis state lives on the Ingester struct and is mutated only
// from the Run goroutine, so no synchronization is required. Failures
// to fetch the chain head or the ingestion state preserve the current
// state silently — a transient RPC blip should not flip the alarm or
// emit a spurious recovery line, and the runOnce path already logs
// real ingestion failures at error level.
//
// Cold-start caveat: when LastIngestedLedger is non-positive we have no
// baseline yet, so any "lag" would compare against a meaningless zero
// and we deliberately settle the gauge at false rather than firing.
//
// Reorg caveat: a raw lag below zero (chain head reported as behind
// what the store already has — RPC reorg or stale cache) is ambiguous
// data we can't interpret; we leave hysteresis alone and republish the
// current value so the gauge stays consistent.
func (ing *Ingester) checkLag(ctx context.Context) {
	if ing.opts.LagWarnLedgers == 0 {
		return // alarm disabled (operators opted out or test fixture)
	}

	latest, err := ing.client.GetLatestLedger(ctx)
	if err != nil {
		// The runOnce path already surfaces RPC failures at error
		// level; emitting another log line every cycle here would be
		// noise without adding information.
		return
	}
	state, err := ing.getIngestionState(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Cold start: no baseline yet, but still publish the
			// default so the downstream gauge isn't unknown. The
			// alarm is intentionally silent here so a fresh deploy
			// doesn't warn about a chain head that's just "big".
			//
			// Asymmetry: this branch publishes SetLagging(false) but
			// doesn't touch ing.lagging — the load-bearing invariant
			// is "ingestion_state row never gets deleted", so any
			// prior ing.lagging=true flips to false on the next
			// non-ErrNotFound cycle, realigning gauge and memory.
			ing.opts.LagMetrics.SetLagging(false)
			return
		}
		// Other store errors leave the alarm silent too: gaps in
		// evidence shouldn't be confused with recovery.
		return
	}

	// Compute the new hysteresis state. Guard above means we never get
	// here with LastIngestedLedger == 0; rawLag is hoisted so it can
	// also feed the log line (no need to recompute the subtraction).
	var (
		lagging bool
		rawLag  int64
	)
	if state.LastIngestedLedger > 0 {
		rawLag = int64(latest.Sequence) - state.LastIngestedLedger
		if rawLag < 0 {
			// RPC reorg / stale cache: we don't know what the new
			// chain head looks like vs what we have. Preserve
			// hysteresis (don't flip to "recovered" on data we
			// can't trust) and republish the gauge so scrapers
			// see a consistent value.
			ing.opts.LagMetrics.SetLagging(ing.lagging)
			return
		}
		lagging = rawLag > int64(ing.opts.LagWarnLedgers)
	}

	// Publish BEFORE the early-return below so the gauge stays fresh
	// on every cycle (a freshly-attached scraper sees 0 right away
	// and a restart never leaks a stale value). The hysteresis
	// transition check still gates the log line.
	ing.opts.LagMetrics.SetLagging(lagging)
	if lagging == ing.lagging {
		return // no edge → no log (this is the spam-suppression rule)
	}
	ing.lagging = lagging

	// Both branches print the same key set so downstream log scrapers
	// can rely on it; reuse rawLag so the math isn't recomputed.
	if lagging {
		ing.log.Warn("ingest lag exceeded threshold",
			"latest_ledger", latest.Sequence,
			"last_ingested_ledger", state.LastIngestedLedger,
			"lag_ledgers", rawLag,
			"threshold_ledgers", ing.opts.LagWarnLedgers,
		)
		return
	}
	ing.log.Info("ingest lag recovered",
		"latest_ledger", latest.Sequence,
		"last_ingested_ledger", state.LastIngestedLedger,
		"lag_ledgers", rawLag,
		"threshold_ledgers", ing.opts.LagWarnLedgers,
	)
}

// buildFilterBatches converts the watched-contract list into getEvents
// filter batches respecting the RPC caps: ≤5 contractIds per filter and ≤5
// filters per request, so ≤25 watched contracts fit in one request. Larger
// lists spill into additional batches, each requiring its own request chain.
// An empty watch list means one type-only filter, i.e. all contract events.
func (ing *Ingester) buildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error) {
	watched, err := ing.store.ListWatchedContracts(ctx)
	if err != nil {
		return nil, err
	}
	if len(watched) == 0 {
		return [][]rpc.EventFilter{{{Type: "contract"}}}, nil
	}
	// Copy IDs into a local slice: the watched list can grow between
	// passes (a runtime POST that lands mid-process is picked up by the
	// next runOnce without restart — see BuildFilterBatches' caller in
	// runOnce), and we don't want to capture a slice that an inserter
	// could mutate under us.
	ids := make([]string, len(watched))
	for i, wc := range watched {
		ids[i] = wc.ContractID
	}
	var filters []rpc.EventFilter
	for start := 0; start < len(ids); start += rpc.MaxContractIDsPerFilter {
		end := min(start+rpc.MaxContractIDsPerFilter, len(ids))
		filters = append(filters, rpc.EventFilter{Type: "contract", ContractIDs: ids[start:end]})
	}
	var batches [][]rpc.EventFilter
	for start := 0; start < len(filters); start += rpc.MaxFiltersPerRequest {
		end := min(start+rpc.MaxFiltersPerRequest, len(filters))
		batches = append(batches, filters[start:end])
	}
	return batches, nil
}

func (ing *Ingester) toStoreEvent(re rpc.Event) (store.Event, error) {
	topics, value, err := decode.EventTopicsValue(ing.decoder, re)
	if err != nil {
		return store.Event{}, fmt.Errorf("decoding event %s: %w", re.ID, err)
	}
	var createdAt time.Time
	if re.LedgerClosedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, re.LedgerClosedAt)
	}
	return store.Event{
		ID:               re.ID,
		ContractID:       re.ContractID,
		Ledger:           int64(re.Ledger),
		Type:             re.Type,
		TxHash:           re.TxHash,
		TxIndex:          re.TransactionIndex,
		OpIndex:          re.OperationIndex,
		InSuccessfulCall: re.InSuccessfulContractCall,
		Topics:           topics,
		Value:            value,
		CreatedAt:        createdAt,
		// Keep the raw XDR so `sorotrail replay` can re-decode this event
		// with a future decoder. Empty when the RPC delivered JSON directly
		// (xdrFormat "json") — there is no XDR to keep in that case, and
		// replay skips such rows.
		RawTopicXDR: re.Topic,
		RawValueXDR: re.Value,
	}, nil
}

// setIngestionLag updates the Prometheus gauge for ingestion lag.
// chainHead can be 0 when unknown (no-op in that case).
func (ing *Ingester) setIngestionLag(chainHead, lastIngested int64) {
	if chainHead <= 0 || lastIngested <= 0 {
		return
	}
	metrics.IngestionLag.Set(float64(chainHead - lastIngested))
}

// sleepCtx sleeps for d or until ctx is done; it reports whether the full
// sleep completed.
// indexEventAddresses extracts G.../C... addresses from each event's
// decoded topics and value JSON, then persists them to the event_addresses
// inverted index. Extraction is a best-effort derived index: errors are
// logged but do not fail the ingest pass, because the index can be rebuilt
// from stored events via the index-addresses backfill command.
func (ing *Ingester) indexEventAddresses(ctx context.Context, events []store.Event) error {
	if len(events) == 0 {
		return nil
	}
	var refs []store.AddressRef
	for _, ev := range events {
		decoded := decode.ExtractAddresses(ev.Topics, ev.Value)
		for _, r := range decoded {
			refs = append(refs, store.AddressRef{
				Address: r.Address,
				EventID: ev.ID,
				Role:    r.Role,
			})
		}
	}
	if len(refs) == 0 {
		return nil
	}
	return ing.store.UpsertAddressRefs(ctx, refs)
}

// RunOnceForTest executes exactly one ingestion pass and reports whether the
// indexer is caught up. It lets the simulation harness drive ingestion
// deterministically, one cycle at a time, rather than starting Run and racing
// its internal sleeps. Run owns the backoff, lag alarm and reorg cadence that
// this deliberately skips, so it is not a production entry point.
func (ing *Ingester) RunOnceForTest(ctx context.Context) (bool, error) {
	return ing.runOnce(ctx)
}
