// Package pruner runs an optional background job that deletes events older
// than a configured age or below a configured ledger, with safe batching
// so it never takes long locks or starves ingestion.
package pruner

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Options configure the pruner. Zero values disable the corresponding
// policy dimension (e.g. MaxAge == 0 means "no age-based pruning").
type Options struct {
	// MaxAge is the maximum age of events to keep. Events older than this
	// are deleted. Zero means no age-based pruning.
	MaxAge time.Duration
	// MinLedger is the lowest ledger to retain. All events strictly below
	// this ledger are deleted. Zero means no ledger-based pruning.
	MinLedger uint64
	// BatchSize is the number of rows to delete per batch. Default 5000.
	BatchSize int
	// Pause is the sleep between batches to avoid long locks. Default 100ms.
	Pause time.Duration
	// Interval is the sleep between full sweep attempts when there is
	// nothing left to prune. Default 1h.
	Interval time.Duration
}

// DefaultValues used when Options fields are zero.
const (
	DefaultBatchSize = 5000
	DefaultPause     = 100 * time.Millisecond
	DefaultInterval  = 1 * time.Hour
)

func (o *Options) applyDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.Pause <= 0 {
		o.Pause = DefaultPause
	}
	if o.Interval <= 0 {
		o.Interval = DefaultInterval
	}
}

// Metrics is a value-copy snapshot of counters the pruner has accumulated.
// It is what callers see via /stats; the underlying counters are stored as
// atomics on the Pruner so the JSON serialization sees plain values without
// touching any internals.
type Metrics struct {
	RunsCompleted   uint64 `json:"runs_completed"`
	TotalRowsPurged int64  `json:"total_rows_purged"`
}

// Pruner is the background retention-policy job.
//
// PRUNERS RUN ON A SINGLE GOROUTINE. Run mutates the atomic counters;
// HTTP handlers call Metrics() concurrently to surface the latest values.
// The value-copy semantics of Metrics() make that read race-free; running
// two Pruner instances concurrently is still unsupported (each would only
// see its own counters).
type Pruner struct {
	store store.Store
	log   *slog.Logger
	opts  Options

	// Atomics: written by Run, read by HTTP handlers via Metrics().
	runsCompleted atomic.Uint64
	rowsPurged    atomic.Int64
}

// Metrics returns a value-copy snapshot of the pruner's counters.
// Snapshotting atomics under the hood makes the call race-free without
// forcing callers to take a lock.
func (p *Pruner) Metrics() Metrics {
	return Metrics{
		RunsCompleted:   p.runsCompleted.Load(),
		TotalRowsPurged: p.rowsPurged.Load(),
	}
}

// New wires a Pruner. When both MaxAge and MinLedger are zero the pruner
// is effectively a no-op (Run returns immediately).
func New(st store.Store, log *slog.Logger, opts Options) *Pruner {
	opts.applyDefaults()
	return &Pruner{
		store: st,
		log:   log.With("component", "pruner"),
		opts:  opts,
	}
}

// Enabled reports whether at least one retention policy is configured.
func (p *Pruner) Enabled() bool {
	return p.opts.MaxAge > 0 || p.opts.MinLedger > 0
}

// Run loops until ctx is canceled, performing one retention sweep per
// interval. Each sweep deletes events in batches with a pause between
// batches. If neither retention policy is configured Run returns
// immediately.
func (p *Pruner) Run(ctx context.Context) error {
	if !p.Enabled() {
		p.log.Debug("pruner disabled; nothing to do")
		return nil
	}

	backoff := time.Second
	for {
		deleted, err := p.pruneOnce(ctx)
		if err != nil {
			sleep := backoff/2 + rand.N(backoff/2)
			p.log.Error("prune sweep failed", "error", err, "retry_in", sleep)
			if !sleepCtx(ctx, sleep) {
				return ctx.Err()
			}
			if backoff *= 2; backoff > p.opts.Interval {
				backoff = p.opts.Interval
			}
			continue
		}
		backoff = time.Second
		p.runsCompleted.Add(1)

		if deleted > 0 {
			// More to prune — try again immediately (with batch pauses).
			continue
		}

		// Nothing to prune — sleep for the full interval.
		if !sleepCtx(ctx, p.opts.Interval) {
			return ctx.Err()
		}
	}
}

// pruneOnce performs one full sweep: deletes batches until fewer than
// BatchSize rows are returned, then logs a summary. Returns the total
// number of rows deleted in the sweep.
func (p *Pruner) pruneOnce(ctx context.Context) (int64, error) {
	ing, err := p.store.GetIngestionState(ctx)
	if err != nil {
		if err == store.ErrNotFound {
			p.log.Debug("no ingestion state yet; skipping prune")
			return 0, nil
		}
		return 0, fmt.Errorf("loading ingestion state: %w", err)
	}

	// Never delete at or above the last ingested ledger. This is the
	// primary safety guard: even if the operator misconfigures
	// RETENTION_MIN_LEDGER, recent unverified events are preserved.
	maxLedger := ing.LastIngestedLedger
	if maxLedger <= 0 {
		p.log.Debug("nothing ingested yet; skipping prune")
		return 0, nil
	}

	// Compute the time cutoff. BeforeTime is set to now - MaxAge. Events
	// strictly older than this are eligible for deletion.
	var beforeTime time.Time
	if p.opts.MaxAge > 0 {
		beforeTime = time.Now().Add(-p.opts.MaxAge)
	}

	// If MinLedger is set and is lower than maxLedger, tighten the bound
	// so we never delete above the last ingested ledger. A MinLedger that
	// is >= last_ingested naturally falls through unchanged — the bound
	// can only ever shrink, never grow, which is the safety contract.
	if p.opts.MinLedger > 0 && uint64(maxLedger) > p.opts.MinLedger {
		maxLedger = int64(p.opts.MinLedger)
	}

	start := time.Now()
	var total int64
	for {
		affected, err := p.store.DeleteEventsBefore(ctx, maxLedger, beforeTime, p.opts.BatchSize)
		if err != nil {
			return total, fmt.Errorf("delete batch: %w", err)
		}
		total += affected
		p.rowsPurged.Add(affected)

		if affected < int64(p.opts.BatchSize) {
			break
		}

		// Pause between batches to let ingestion make progress and avoid
		// long-running transactions.
		if !sleepCtx(ctx, p.opts.Pause) {
			return total, ctx.Err()
		}
	}

	dur := time.Since(start)
	if total > 0 {
		p.log.Info("prune sweep complete",
			"rows_deleted", total,
			"duration", dur,
			"max_ledger", maxLedger,
			"before_time", beforeTime,
		)
	} else {
		p.log.Debug("prune sweep: nothing to delete",
			"max_ledger", maxLedger,
			"before_time", beforeTime,
		)
	}
	return total, nil
}

// sleepCtx sleeps for d or until ctx is cancelled, returning true if the
// full duration elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
