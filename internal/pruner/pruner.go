// Package pruner runs an optional background job that deletes events older
// than a configured age or below a configured ledger, with safe batching
// so it never takes long locks or starves ingestion.
package pruner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/archive"
	"github.com/sorotrail/sorotrail/internal/requestid"
	"github.com/sorotrail/sorotrail/internal/store"
)

// LagYieldController reports whether the pruner should defer its retention
// sweep (or yield between batches) while ingestion is lagging behind the
// chain head.
type LagYieldController interface {
	ShouldYield(job string) (yield bool, reason string)
}

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
	// ArchiveBeforePrune, when true, instructs the pruner to export
	// events to object storage before deleting them. Requires that the
	// Archiver was configured. When false (the default), events are
	// deleted directly without archival.
	ArchiveBeforePrune bool
	// DryRun, when true, reports what pruneOnce would delete without
	// removing any rows. The dry-run sweep populates DryRunEligibleRows
	// in the Metrics snapshot so operators can validate retention policy
	// before enabling actual deletion.
	DryRun bool
	// LagController is an optional lag backpressure coordinator. When set,
	// pruneOnce defers sweeps (and yields between batches) while ingestion
	// is lagging, up to the coordinator's maximum deferral window.
	LagController LagYieldController
	// Policies, when set, applies per-tenant retention (#1249): each sweep
	// plans the tenants' horizons with MaxAge as the instance default and
	// deletes each contract's events only past the longest horizon of the
	// tenants entitled to it. Nil keeps the single instance-wide sweep.
	Policies store.RetentionPolicyStore
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
	RunsCompleted        uint64 `json:"runs_completed"`
	TotalRowsPurged      int64  `json:"total_rows_purged"`
	ArchivedRanges       int64  `json:"archived_ranges,omitempty"`
	DryRunEligibleRows   int64  `json:"dry_run_eligible_rows,omitempty"`
	LagYields            uint64 `json:"lag_yields,omitempty"`
	MaxDeferralOverrides uint64 `json:"max_deferral_overrides,omitempty"`
	// RetentionPolicyGroups is how many distinct retention horizons the
	// last sweep applied; 1 means only the instance default was in force.
	RetentionPolicyGroups int64 `json:"retention_policy_groups,omitempty"`
	// TenantPolicyRowsPurged counts rows deleted under a tenant-set or
	// wildcard-tenant horizon rather than the instance default.
	TenantPolicyRowsPurged int64 `json:"tenant_policy_rows_purged,omitempty"`
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
	arch  *archive.Archiver // nil when archival is disabled

	// Atomics: written by Run, read by HTTP handlers via Metrics().
	runsCompleted        atomic.Uint64
	rowsPurged           atomic.Int64
	archivedRanges       atomic.Int64
	dryRunEligible       atomic.Int64
	lagYields            atomic.Uint64
	maxDeferralOverrides atomic.Uint64
	policyGroups         atomic.Int64
	tenantPolicyPurged   atomic.Int64
}

// Metrics returns a value-copy snapshot of the pruner's counters.
// Snapshotting atomics under the hood makes the call race-free without
// forcing callers to take a lock.
func (p *Pruner) Metrics() Metrics {
	return Metrics{
		RunsCompleted:          p.runsCompleted.Load(),
		TotalRowsPurged:        p.rowsPurged.Load(),
		ArchivedRanges:         p.archivedRanges.Load(),
		DryRunEligibleRows:     p.dryRunEligible.Load(),
		LagYields:              p.lagYields.Load(),
		MaxDeferralOverrides:   p.maxDeferralOverrides.Load(),
		RetentionPolicyGroups:  p.policyGroups.Load(),
		TenantPolicyRowsPurged: p.tenantPolicyPurged.Load(),
	}
}

// WithLagController attaches a LagYieldController to the Pruner (and its
// Archiver when present).
func (p *Pruner) WithLagController(c LagYieldController) *Pruner {
	p.opts.LagController = c
	if p.arch != nil {
		p.arch.WithLagController(c)
	}
	return p
}

// New wires a Pruner. When both MaxAge and MinLedger are zero the pruner
// is effectively a no-op (Run returns immediately).
func New(st store.Store, log *slog.Logger, opts Options) *Pruner {
	return NewWithArchiver(st, log, opts, nil)
}

// NewWithArchiver wires a Pruner with an optional archiver. When arch
// is non-nil and ArchiveBeforePrune is set, events are exported to
// object storage before each batch deletion. When arch is nil the
// pruner behaves identically to New.
func NewWithArchiver(st store.Store, log *slog.Logger, opts Options, arch *archive.Archiver) *Pruner {
	opts.applyDefaults()
	return &Pruner{
		store: st,
		// The same correlation field HTTP requests use carries the pruner's
		// stable job id, so one field always identifies the originating
		// work.
		log:  log.With("component", "pruner", requestid.Field, requestid.JobPruner),
		opts: opts,
		arch: arch,
	}
}

// Enabled reports whether at least one retention policy is configured.
// Per-tenant policies count: a tenant may set a horizon on an instance
// whose own default is "keep forever".
func (p *Pruner) Enabled() bool {
	return p.opts.MaxAge > 0 || p.opts.MinLedger > 0 || p.opts.Policies != nil
}

// Run loops until ctx is canceled, performing one retention sweep per
// interval. Each sweep deletes events in batches with a pause between
// batches. If neither retention policy is configured Run returns
// immediately.
func (p *Pruner) Run(ctx context.Context) error {
	// Carry the job id on the context so the store decorator tags this
	// loop's slow-query logs with "pruner".
	ctx = requestid.WithJob(ctx, requestid.JobPruner)
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
//
// When archival is configured and ArchiveBeforePrune is set, events are
// exported to object storage as compressed NDJSON before each batch
// deletion. Archival failure aborts the sweep — events are never
// deleted without a successful archive, ensuring pruned ranges are
// always recoverable.
func (p *Pruner) pruneOnce(ctx context.Context) (int64, error) {
	if p.opts.LagController != nil {
		if yield, reason := p.opts.LagController.ShouldYield("pruner"); yield {
			p.lagYields.Add(1)
			p.log.Info("prune sweep deferred due to ingestion lag", "reason", reason)
			return 0, nil
		} else if reason == "max_deferral_reached" {
			p.maxDeferralOverrides.Add(1)
		}
	}

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

	// If MinLedger is set and is lower than maxLedger, tighten the bound
	// so we never delete above the last ingested ledger. A MinLedger that
	// is >= last_ingested naturally falls through unchanged — the bound
	// can only ever shrink, never grow, which is the safety contract.
	if p.opts.MinLedger > 0 && uint64(maxLedger) > p.opts.MinLedger {
		maxLedger = int64(p.opts.MinLedger)
	}

	// The plan is reloaded every sweep, so a policy change takes effect at
	// the next sweep and never mid-sweep. Without Policies it is the single
	// instance-wide group, which is exactly the pre-#1249 sweep.
	plan, err := store.LoadRetentionPlan(ctx, p.opts.Policies, p.opts.MaxAge)
	if err != nil {
		return 0, fmt.Errorf("loading retention policies: %w", err)
	}
	p.policyGroups.Store(int64(len(plan.Groups)))

	// Archive events before deleting them when archival is enabled.
	// The archive covers the full range [0, maxLedger) that will be
	// pruned. This is a single export call; the archiver pages through
	// events using QueryEvents and uploads compressed NDJSON to S3.
	//
	// Archival is per row, not per tenant: the range holds every
	// contract's events, and every retention group below deletes only
	// inside it. So whichever group eventually deletes a row, that row is
	// already in the archive — no tenant's deletion relies on an archive
	// made "for" another tenant.
	if p.opts.ArchiveBeforePrune && p.arch != nil {
		archiveStart := time.Now()
		_, _, err := p.arch.ArchiveRange(ctx, 0, maxLedger)
		if errors.Is(err, archive.ErrDeferredToLag) {
			p.lagYields.Add(1)
			p.log.Info("prune sweep deferred because archiver yielded to ingestion lag",
				"max_ledger", maxLedger)
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("archiving events before prune: %w", err)
		}
		p.archivedRanges.Add(1)
		p.log.Info("archive before prune complete",
			"max_ledger", maxLedger,
			"duration", time.Since(archiveStart),
		)
	}

	var total int64
	for _, g := range plan.Groups {
		// A group with no age bound is pruned by ledger alone, and only
		// when RETENTION_MIN_LEDGER asks for that. Without either bound
		// the delete would cover everything below the last ingested
		// ledger, so such a group is skipped outright.
		if g.MaxAge <= 0 && p.opts.MinLedger == 0 {
			continue
		}
		// BeforeTime is now - MaxAge. Events strictly older than this are
		// eligible for deletion.
		var beforeTime time.Time
		if g.MaxAge > 0 {
			beforeTime = time.Now().Add(-g.MaxAge)
		}
		n, err := p.sweepGroup(ctx, g, maxLedger, beforeTime, len(plan.Groups) > 1)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// sweepGroup deletes (or, in dry-run mode, counts) one retention group's
// eligible events in batches. logGroup adds the group's policy to the
// sweep log; it is false for the single-group plan so an instance without
// tenant policies logs exactly what it did before.
func (p *Pruner) sweepGroup(ctx context.Context, g store.RetentionGroup, maxLedger int64, beforeTime time.Time, logGroup bool) (int64, error) {
	attrs := []any{"max_ledger", maxLedger, "before_time", beforeTime}
	if logGroup {
		attrs = append(attrs,
			"policy", g.Policy,
			"max_age", g.MaxAge,
			"contracts_listed", len(g.Contracts.IDs),
			"contracts_excluded", g.Contracts.Exclude,
		)
	}
	if p.opts.DryRun {
		return p.dryRunSweep(ctx, g, maxLedger, beforeTime, attrs)
	}

	start := time.Now()
	var total int64
	for {
		if total > 0 && p.opts.LagController != nil {
			if yield, reason := p.opts.LagController.ShouldYield("pruner"); yield {
				p.lagYields.Add(1)
				p.log.Info("prune batch loop yielding to ingestion lag",
					"rows_deleted_so_far", total,
					"reason", reason,
				)
				break
			} else if reason == "max_deferral_reached" {
				p.maxDeferralOverrides.Add(1)
			}
		}
		var (
			affected int64
			err      error
		)
		if g.Contracts.All() {
			affected, err = p.store.DeleteEventsBefore(ctx, maxLedger, beforeTime, p.opts.BatchSize)
		} else {
			affected, err = p.opts.Policies.DeleteEventsBeforeIn(ctx, maxLedger, beforeTime, p.opts.BatchSize, g.Contracts)
		}
		if err != nil {
			return total, fmt.Errorf("delete batch: %w", err)
		}
		total += affected
		p.rowsPurged.Add(affected)
		if g.Policy != store.RetentionPolicyDefault {
			p.tenantPolicyPurged.Add(affected)
		}

		if affected < int64(p.opts.BatchSize) {
			break
		}

		// Pause between batches to let ingestion make progress and avoid
		// long-running transactions.
		if !sleepCtx(ctx, p.opts.Pause) {
			return total, ctx.Err()
		}
	}

	if total > 0 {
		p.log.Info("prune sweep complete",
			append([]any{"rows_deleted", total, "duration", time.Since(start)}, attrs...)...)
	} else {
		p.log.Debug("prune sweep: nothing to delete", attrs...)
	}
	return total, nil
}

// dryRunSweep reports what sweepGroup would delete without removing any
// rows. It uses CountEventsBefore (or its contract-scoped form) to count
// eligible rows in batches.
func (p *Pruner) dryRunSweep(ctx context.Context, g store.RetentionGroup, maxLedger int64, beforeTime time.Time, attrs []any) (int64, error) {
	var total int64
	for {
		var (
			affected int64
			err      error
		)
		if g.Contracts.All() {
			affected, err = p.store.CountEventsBefore(ctx, maxLedger, beforeTime, p.opts.BatchSize)
		} else {
			affected, err = p.opts.Policies.CountEventsBeforeIn(ctx, maxLedger, beforeTime, p.opts.BatchSize, g.Contracts)
		}
		if err != nil {
			return total, fmt.Errorf("dry-run count batch: %w", err)
		}
		total += affected

		if affected < int64(p.opts.BatchSize) {
			break
		}

		if !sleepCtx(ctx, p.opts.Pause) {
			return total, ctx.Err()
		}
	}
	p.dryRunEligible.Add(total)

	if total > 0 {
		p.log.Info("dry-run prune sweep: would delete", append([]any{"rows_eligible", total}, attrs...)...)
	} else {
		p.log.Debug("dry-run prune sweep: nothing to delete", attrs...)
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
