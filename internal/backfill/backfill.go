// Package backfill ingests historical contract events from a Horizon
// instance that are older than the RPC's retention window, filling the
// gap between the earliest RPC-retained ledger and the contract's
// deployment.
//
// How it works:
//   - Page through /accounts/{contract_id}/transactions in ascending
//     order, decode each row's result_meta_xdr.
//   - Extract every Soroban ContractEvent from V3 / V4 meta and upsert
//     into the events table.
//   - Persist progress (lastLedger and per-range checkpoints) after each
//     committed batch so Ctrl-C + re-run resumes cleanly with idempotent
//     upserts handling any page overlap.
//
// Parallel execution, ordering guarantees, and live-ingestion coexistence (#1237):
//   - When Options.Workers > 1 (or Options.RangeSize > 0), the target
//     [FromLedger, ToLedger] span is partitioned into contiguous,
//     whole-ledger sub-ranges [rangeFrom, rangeTo] and processed by a
//     bounded worker pool.
//   - Ordering guarantees: because sub-range boundaries never split a
//     single ledger across two workers, per-ledger transaction indices
//     (tx_index) and deterministic event IDs (<tx_hash>-<op_index>-<event_index>)
//     are identical to a serial run. Across sub-ranges, workers commit batches
//     independently so rows may land in Postgres out of global wall-clock
//     order; all read paths order by (ledger, tx_index, op_index, id) so
//     query results are unaffected by commit arrival order.
//   - Coexistence with live ingestion: UpsertEvents uses ON CONFLICT
//     (ledger, id) DO NOTHING and partition-level advisory locking, so
//     concurrent backfill workers and the live ingester can write to the
//     events table simultaneously without deadlocking or duplicating rows.
//   - Rate limiting: all workers share a single aggregate RateLimiter
//     so N workers never exceed the configured Horizon/RPC request rate.
//
// Limitations (call them out in docs and operator guidance; we don't
// crash on them):
//
//   - Horizon must retain full historical transaction meta for the
//     target network. Public Stellar testnet Horizon retains everything
//     from protocol 17 onward; mainnet Horizon instances vary — some
//     prune old meta. Operators should check their target Horizon's
//     retention policy before backfilling long ranges.
//   - Speed is bounded by Horizon pagination (~200 txs per page) and
//     per-tx XDR decoding. Expect several-hundred events/second on a
//     small public instance; private deployments with burst quotas
//     can do much higher.
//   - Only Soroban transactions with V3/V4 meta carry events. Classic
//     Stellar (V1/V2) and failed Soroban txs without meta are counted
//     as Skipped.
//   - Soroban fee-bump txs emit events in their inner tx's operations;
//     we recurse into V4.InnerTransactions so those events capture
//     against the inner op indices, not the outer wrap.
//   - There's no dedupe against the live ingester at the row level:
//     for any range where both overlap (≤ the RPC retention window),
//     live writes a TOID-format ID and backfill writes a tx-hash-format
//     ID for the same on-chain emission. See docs/backfill.md.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// DefaultBatchSize is the default Horizon pagination limit. 200 matches
// Horizon's per-page cap and gives ~256KB-ish JSON payloads.
const DefaultBatchSize = 200

// DefaultMaxBackoff caps the per-page HTTP error backoff.
const DefaultMaxBackoff = 30 * time.Second

// DefaultMinRequestInterval is ~10 req/s — safe for public Horizon
// instances. Private deployments can lower this via Options.MinInterval.
const DefaultMinRequestInterval = 100 * time.Millisecond

// DefaultWorkers preserves single-worker serial behavior unless configured.
const DefaultWorkers = 1

// DefaultRangeMaxRetries is the default number of per-range retry attempts
// for transient Horizon/RPC errors in parallel mode.
const DefaultRangeMaxRetries = 3

// DefaultRetryInitialBackoff is the starting backoff for per-range retries.
const DefaultRetryInitialBackoff = 10 * time.Millisecond

// RateLimiter coordinates request pacing across all concurrent backfill
// workers so the pool stays under the configured RPC/Horizon rate limit in
// aggregate (#1237).
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// IntervalRateLimiter enforces a minimum spacing between Wait calls across
// all concurrent workers sharing the limiter.
type IntervalRateLimiter struct {
	mu          sync.Mutex
	minInterval time.Duration
	lastAllowed time.Time
	waits       int64
}

// NewIntervalRateLimiter constructs a concurrency-safe aggregate rate limiter
// that spaces requests by at least minInterval across all callers.
func NewIntervalRateLimiter(minInterval time.Duration) *IntervalRateLimiter {
	return &IntervalRateLimiter{minInterval: minInterval}
}

// Wait blocks until the next request slot is available or ctx is cancelled.
func (l *IntervalRateLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.minInterval <= 0 {
		l.waits++
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	next := l.lastAllowed.Add(l.minInterval)
	var waitDur time.Duration
	if l.lastAllowed.IsZero() || !now.Before(next) {
		l.lastAllowed = now
	} else {
		waitDur = next.Sub(now)
		l.lastAllowed = next
	}
	l.waits++
	l.mu.Unlock()

	if waitDur <= 0 {
		return nil
	}
	timer := time.NewTimer(waitDur)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WaitCount returns the total number of Wait calls granted by the limiter.
func (l *IntervalRateLimiter) WaitCount() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waits
}

// RangeTransactionLister is an optional extension of horizon.Client that
// fetches transactions scoped to a specific [fromLedger, toLedger] sub-range.
// When not implemented, workers call ListContractTransactions and filter
// records to [fromLedger, toLedger].
type RangeTransactionLister interface {
	ListContractTransactionsInRange(ctx context.Context, contractID string, fromLedger, toLedger int64, cursor string, limit int, includeFailed bool) (horizon.TransactionsResponse, error)
}

// FailedRange describes a ledger sub-range that failed after exhausting
// retries or hitting a non-retryable error.
type FailedRange struct {
	FromLedger int64
	ToLedger   int64
	LastLedger int64
	Err        error
}

// String formats the failed range bounds and cause for operator diagnostics.
func (f FailedRange) String() string {
	return fmt.Sprintf("[%d..%d] (last_ledger=%d): %v", f.FromLedger, f.ToLedger, f.LastLedger, f.Err)
}

// ProgressSnapshot reports observable progress across sub-ranges for
// long-running backfills (#1237).
type ProgressSnapshot struct {
	TotalRanges                int
	CompletedRanges            int
	InFlightRanges             int
	FailedRanges               int
	PendingRanges              int
	CompletedLedgers           int64
	TotalLedgers               int64
	EstimatedRemainingLedgers  int64
	EstimatedRemainingDuration time.Duration
}

// Options configure a Backfiller. Zero values produce a usable Backfiller
// after applyDefaults.
type Options struct {
	// ContractID selects which contract to backfill. Required — there
	// is no "all contracts" scan path because the page walker assumes
	// every Horizon row's events are filtered down to one contract.
	ContractID string

	// FromLedger is the inclusive start ledger for new or fresh runs.
	FromLedger int64
	// ToLedger is the inclusive upper bound; 0 = no upper bound (keep
	// going until Horizon returns an empty page).
	ToLedger int64

	BatchSize int
	// HorizonURL is a full URL (no trailing slash). Defaults to
	// https://horizon-testnet.stellar.org.
	HorizonURL string
	// MinInterval is the minimum spacing between Horizon requests.
	MinInterval time.Duration
	// IncludeFailed controls whether failed txs are walked. Failed
	// txs whose contract calls succeeded still emit events; this
	// mirrors the live ingester's default-past-and-future behavior.
	IncludeFailed bool
	// MaxBackoff caps retry backoff for HTTP errors.
	MaxBackoff time.Duration
	// DryRun reports what would change without writing anything.
	DryRun bool

	// Workers is the number of concurrent range workers (#1237).
	// Values <= 1 run serially (preserving legacy behavior).
	Workers int
	// RangeSize is the fixed ledger span per sub-range (#1237).
	// Values <= 0 split [FromLedger, ToLedger] adaptively across Workers.
	RangeSize int64
	// MaxRetries is the number of per-range retry attempts for transient
	// errors before marking a sub-range failed.
	MaxRetries int
	// RetryInitialBackoff is the starting backoff for per-range retries.
	RetryInitialBackoff time.Duration
	// Limiter is an optional shared RateLimiter across all workers.
	Limiter RateLimiter

	// Progress, when non-nil, is called after each page is processed
	// with the number of events extracted. The callback must be safe
	// for concurrent use and should not block.
	Progress func(extracted int64)

	// RangeProgress, when non-nil, is called whenever per-range progress
	// advances (#1237).
	RangeProgress func(snapshot ProgressSnapshot)
}

// applyDefaults fills in zero-value options so a partially-populated
// Options struct still produces a workable Backfiller.
func (o *Options) applyDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	if o.MinInterval <= 0 {
		o.MinInterval = DefaultMinRequestInterval
	}
	if o.HorizonURL == "" {
		o.HorizonURL = "https://horizon-testnet.stellar.org"
	}
	if o.Workers <= 0 {
		o.Workers = DefaultWorkers
	}
	if o.RetryInitialBackoff <= 0 {
		o.RetryInitialBackoff = DefaultRetryInitialBackoff
	}
}

// Store is the persistence surface the backfill package needs.
// *store.Postgres implements it. Kept narrow so the package stays
// import-light and tests can supply fakes.
type Store interface {
	UpsertEvents(ctx context.Context, events []store.Event) (int64, error)
	GetBackfillState(ctx context.Context) (store.BackfillState, error)
	StartBackfillState(ctx context.Context, contractID string, fromLedger, toLedger int64) error
	UpdateBackfillState(ctx context.Context, lastLedger int64) error
	CompleteBackfillState(ctx context.Context) error
}

// RangeStore is an optional extension of Store for persisting per-range
// progress checkpoints (#1237). *store.Postgres implements it.
type RangeStore interface {
	Store
	SaveBackfillRanges(ctx context.Context, ranges []store.BackfillRange) error
}

// Summary reports what a backfill run did. The counters are independent
// so an operator can read off throughput at a glance.
type Summary struct {
	PagesFetched       int64
	Transactions       int64
	Skipped            int64
	Failed             int64
	Extracted          int64
	Inserted           int64
	DryRun             bool
	Completed          bool
	Resumed            bool
	ThroughLedger      int64
	Duration           time.Duration
	TotalRanges        int
	CompletedRanges    int
	InFlightRanges     int
	FailedRanges       []FailedRange
	Ranges             []store.BackfillRange
	EstimatedRemaining time.Duration
}

// Backfiller ingests historical contract events from Horizon.
type Backfiller struct {
	client  horizon.Client
	store   Store
	decoder decode.Decoder
	log     *slog.Logger
	opts    Options
	limiter RateLimiter

	mu        sync.Mutex
	startedAt time.Time
	ranges    []store.BackfillRange
}

// New wires a Backfiller. ApplyDefaults runs once so callers can pass a
// partially-populated Options struct.
func New(h horizon.Client, st Store, dec decode.Decoder, log *slog.Logger, opts Options) *Backfiller {
	explicitInterval := opts.MinInterval > 0
	opts.applyDefaults()
	limiter := opts.Limiter
	if limiter == nil && explicitInterval {
		limiter = NewIntervalRateLimiter(opts.MinInterval)
	}
	return &Backfiller{
		client:  h,
		store:   st,
		decoder: dec,
		log:     log,
		opts:    opts,
		limiter: limiter,
	}
}

// SplitLedgerRanges partitions [fromLedger, toLedger] into contiguous
// whole-ledger sub-ranges (#1237). When rangeSize > 0, each sub-range covers
// at most rangeSize ledgers; otherwise the range is divided adaptively across
// workers. When toLedger <= 0 or <= fromLedger, a single sub-range is returned.
func SplitLedgerRanges(fromLedger, toLedger, rangeSize int64, workers int) []store.BackfillRange {
	if toLedger <= 0 || toLedger <= fromLedger || (workers <= 1 && rangeSize <= 0) {
		return []store.BackfillRange{{
			FromLedger: fromLedger,
			ToLedger:   toLedger,
			Status:     store.BackfillRangePending,
		}}
	}

	totalLedgers := toLedger - fromLedger + 1
	step := rangeSize
	if step <= 0 {
		if workers <= 0 {
			workers = 1
		}
		w := int64(workers)
		if w > totalLedgers {
			w = totalLedgers
		}
		step = (totalLedgers + w - 1) / w
	}
	if step <= 0 {
		step = 1
	}

	var out []store.BackfillRange
	for cur := fromLedger; cur <= toLedger; cur += step {
		end := cur + step - 1
		if end > toLedger {
			end = toLedger
		}
		out = append(out, store.BackfillRange{
			FromLedger: cur,
			ToLedger:   end,
			Status:     store.BackfillRangePending,
		})
	}
	return out
}

// ProgressSnapshot returns a point-in-time snapshot of sub-range progress
// and estimated remaining duration (#1237).
func (b *Backfiller) ProgressSnapshot() ProgressSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.progressSnapshotLocked()
}

func (b *Backfiller) progressSnapshotLocked() ProgressSnapshot {
	snap := ProgressSnapshot{
		TotalRanges: len(b.ranges),
	}
	for _, r := range b.ranges {
		var span int64
		if r.ToLedger >= r.FromLedger && r.ToLedger > 0 {
			span = r.ToLedger - r.FromLedger + 1
			snap.TotalLedgers += span
		}
		switch r.Status {
		case store.BackfillRangeCompleted:
			snap.CompletedRanges++
			snap.CompletedLedgers += span
		case store.BackfillRangeInFlight:
			snap.InFlightRanges++
			if r.LastLedger >= r.FromLedger && r.ToLedger >= r.FromLedger {
				done := r.LastLedger - r.FromLedger + 1
				if done > span {
					done = span
				}
				snap.CompletedLedgers += done
			}
		case store.BackfillRangeFailed:
			snap.FailedRanges++
			if r.LastLedger >= r.FromLedger && r.ToLedger >= r.FromLedger {
				done := r.LastLedger - r.FromLedger + 1
				if done > span {
					done = span
				}
				snap.CompletedLedgers += done
			}
		default:
			snap.PendingRanges++
		}
	}
	if snap.TotalLedgers > snap.CompletedLedgers {
		snap.EstimatedRemainingLedgers = snap.TotalLedgers - snap.CompletedLedgers
	}
	if !b.startedAt.IsZero() && snap.CompletedLedgers > 0 && snap.EstimatedRemainingLedgers > 0 {
		elapsed := time.Since(b.startedAt)
		snap.EstimatedRemainingDuration = time.Duration(
			float64(elapsed) * (float64(snap.EstimatedRemainingLedgers) / float64(snap.CompletedLedgers)),
		)
	}
	return snap
}

// Run executes the backfill. Pressing Ctrl-C between pages is safe:
// progress is committed per page via UpdateBackfillState (and per-range
// via SaveBackfillRanges when running across sub-ranges), and resume
// re-fetches only incomplete ranges with idempotent upserts handling
// any page overlap.
func (b *Backfiller) Run(ctx context.Context) (Summary, error) {
	if b.opts.ContractID == "" {
		return Summary{}, errors.New("backfill: --contract is required")
	}
	if b.opts.FromLedger <= 0 {
		return Summary{}, errors.New("backfill: --from-ledger must be positive")
	}
	started := time.Now()
	b.mu.Lock()
	b.startedAt = started
	b.mu.Unlock()

	if b.shouldRunParallelOrRanged(ctx) {
		return b.runRanged(ctx, started)
	}

	startLedger, sum, err := b.resume(ctx)
	if err != nil {
		return Summary{}, err
	}

	b.log.Info("backfill starting",
		"contract_id", b.opts.ContractID,
		"from_ledger", b.opts.FromLedger,
		"to_ledger", b.opts.ToLedger,
		"start_ledger", startLedger,
		"batch_size", b.opts.BatchSize,
		"include_failed", b.opts.IncludeFailed,
		"dry_run", b.opts.DryRun,
		"resumed", sum.Resumed)

	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			sum.Duration = time.Since(started)
			err := b.markTerminal(ctx, sum)
			return sum, err // interrupted: report progress, not an error
		}

		if err := b.waitRateLimit(ctx); err != nil {
			if ctx.Err() != nil {
				sum.Duration = time.Since(started)
				err := b.markTerminal(ctx, sum)
				return sum, err
			}
			sum.Duration = time.Since(started)
			return sum, fmt.Errorf("rate limiting horizon page: %w", err)
		}

		resp, err := b.fetch(ctx, cursor)
		if err != nil {
			sum.Duration = time.Since(started)
			return sum, fmt.Errorf("fetching horizon page: %w", err)
		}
		if len(resp.Embedded.Records) == 0 {
			sum.Completed = true
			break
		}
		sum.PagesFetched++

		events, lastProcessedLedger, perPage := b.extractPage(resp.Embedded.Records, startLedger)
		sum.Transactions += perPage.Transactions
		sum.Skipped += perPage.Skipped
		sum.Failed += perPage.Failed
		sum.Extracted += int64(len(events))
		if b.opts.Progress != nil {
			b.opts.Progress(int64(len(events)))
		}
		if lastProcessedLedger > sum.ThroughLedger {
			sum.ThroughLedger = lastProcessedLedger
		}

		// --to-ledger honored: we've seen at least one tx past the
		// bound. Keep what we already extracted from in-range txs,
		// commit, exit.
		if b.opts.ToLedger > 0 && lastProcessedLedger > b.opts.ToLedger {
			if len(events) > 0 && !b.opts.DryRun {
				ins, err := b.commitPage(ctx, events, sum.ThroughLedger)
				if err != nil {
					sum.Duration = time.Since(started)
					return sum, err
				}
				sum.Inserted += ins
			}
			sum.Completed = true
			break
		}

		if len(events) > 0 && !b.opts.DryRun {
			ins, err := b.commitPage(ctx, events, lastProcessedLedger)
			if err != nil {
				sum.Duration = time.Since(started)
				return sum, err
			}
			sum.Inserted += ins
		}

		// Short page = horizon is exhausted.
		if len(resp.Embedded.Records) < b.opts.BatchSize {
			sum.Completed = true
			break
		}

		last := resp.Embedded.Records[len(resp.Embedded.Records)-1]
		cursor = last.PagingToken
		if cursor == "" {
			// Some older Horizon deployments omit paging_token on
			// /accounts/{id}/transactions; falling back to the tx
			// hash is stable for that page's purposes.
			cursor = last.ID
		}
	}

	sum.TotalRanges = 1
	if sum.Completed {
		sum.CompletedRanges = 1
	}
	sum.Duration = time.Since(started)
	if err := b.markTerminal(ctx, sum); err != nil {
		return sum, err
	}
	return sum, nil
}

// shouldRunParallelOrRanged reports whether Run should execute using sub-range
// partitioning and per-range state tracking.
func (b *Backfiller) shouldRunParallelOrRanged(ctx context.Context) bool {
	if b.opts.Workers > 1 || b.opts.RangeSize > 0 || b.opts.MaxRetries > 0 {
		return true
	}
	if b.opts.DryRun {
		return false
	}
	state, err := b.store.GetBackfillState(ctx)
	if err == nil && !state.Done() &&
		state.ContractID == b.opts.ContractID &&
		state.FromLedger == b.opts.FromLedger &&
		state.ToLedger == b.opts.ToLedger &&
		len(state.Ranges) > 0 {
		return true
	}
	return false
}

// runRanged executes a sub-range partitioned backfill across a bounded
// worker pool (#1237).
func (b *Backfiller) runRanged(ctx context.Context, started time.Time) (Summary, error) {
	ranges, sum, err := b.resumeRanges(ctx)
	if err != nil {
		return Summary{}, err
	}

	b.mu.Lock()
	b.ranges = cloneRanges(ranges)
	b.mu.Unlock()
	b.notifyRangeProgress()

	workers := b.opts.Workers
	if workers <= 0 {
		workers = 1
	}
	maxRetries := b.opts.MaxRetries
	if maxRetries <= 0 && workers > 1 {
		maxRetries = DefaultRangeMaxRetries
	}

	b.log.Info("backfill starting ranged run",
		"contract_id", b.opts.ContractID,
		"from_ledger", b.opts.FromLedger,
		"to_ledger", b.opts.ToLedger,
		"workers", workers,
		"ranges", len(ranges),
		"batch_size", b.opts.BatchSize,
		"include_failed", b.opts.IncludeFailed,
		"dry_run", b.opts.DryRun,
		"resumed", sum.Resumed)

	var g errgroup.Group
	g.SetLimit(workers)

	for idx := range ranges {
		idx := idx
		if ranges[idx].Completed() {
			continue
		}
		g.Go(func() error {
			b.executeSubRangeWithRetry(ctx, idx, maxRetries, &sum)
			return nil
		})
	}
	_ = g.Wait()

	b.mu.Lock()
	finalRanges := cloneRanges(b.ranges)
	snap := b.progressSnapshotLocked()
	b.mu.Unlock()

	sum.TotalRanges = snap.TotalRanges
	sum.CompletedRanges = snap.CompletedRanges
	sum.InFlightRanges = snap.InFlightRanges
	sum.Ranges = finalRanges
	sum.EstimatedRemaining = snap.EstimatedRemainingDuration
	sum.Duration = time.Since(started)

	if ctx.Err() != nil {
		sum.Completed = false
		return sum, b.markTerminal(ctx, sum)
	}

	if len(sum.FailedRanges) > 0 {
		sum.Completed = false
		parts := make([]string, 0, len(sum.FailedRanges))
		for _, fr := range sum.FailedRanges {
			parts = append(parts, fr.String())
		}
		return sum, fmt.Errorf("backfill failed on %d/%d range(s): %s",
			len(sum.FailedRanges), sum.TotalRanges, strings.Join(parts, "; "))
	}

	sum.Completed = snap.CompletedRanges == snap.TotalRanges
	if err := b.markTerminal(ctx, sum); err != nil {
		return sum, err
	}
	return sum, nil
}

// resumeRanges initializes or resumes the sub-range table for a ranged run.
func (b *Backfiller) resumeRanges(ctx context.Context) ([]store.BackfillRange, Summary, error) {
	if b.opts.DryRun {
		ranges := SplitLedgerRanges(b.opts.FromLedger, b.opts.ToLedger, b.opts.RangeSize, b.opts.Workers)
		return ranges, Summary{DryRun: true}, nil
	}

	state, err := b.store.GetBackfillState(ctx)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// fall through to fresh start
	case err != nil:
		return nil, Summary{}, err
	case !state.Done() &&
		state.ContractID == b.opts.ContractID &&
		state.FromLedger == b.opts.FromLedger &&
		state.ToLedger == b.opts.ToLedger:
		if len(state.Ranges) > 0 {
			resumed := cloneRanges(state.Ranges)
			var through int64
			for i := range resumed {
				if resumed[i].LastLedger > through {
					through = resumed[i].LastLedger
				}
				if resumed[i].Status != store.BackfillRangeCompleted {
					resumed[i].Status = store.BackfillRangePending
					resumed[i].Error = ""
				}
			}
			return resumed, Summary{Resumed: true, ThroughLedger: through}, nil
		}
		start := state.LastLedger + 1
		if start < b.opts.FromLedger {
			start = b.opts.FromLedger
		}
		ranges := SplitLedgerRanges(start, b.opts.ToLedger, b.opts.RangeSize, b.opts.Workers)
		if err := b.persistRanges(ctx, ranges); err != nil {
			return nil, Summary{}, err
		}
		return ranges, Summary{Resumed: true, ThroughLedger: state.LastLedger}, nil
	}

	if err := b.store.StartBackfillState(ctx, b.opts.ContractID, b.opts.FromLedger, b.opts.ToLedger); err != nil {
		return nil, Summary{}, err
	}
	ranges := SplitLedgerRanges(b.opts.FromLedger, b.opts.ToLedger, b.opts.RangeSize, b.opts.Workers)
	if err := b.persistRanges(ctx, ranges); err != nil {
		return nil, Summary{}, err
	}
	return ranges, Summary{}, nil
}

// executeSubRangeWithRetry runs one sub-range and retries transient errors up
// to maxRetries times with exponential backoff capped at MaxBackoff.
func (b *Backfiller) executeSubRangeWithRetry(ctx context.Context, idx, maxRetries int, sum *Summary) {
	backoff := b.opts.RetryInitialBackoff
	if backoff <= 0 {
		backoff = DefaultRetryInitialBackoff
	}

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		err := b.runSingleSubRange(ctx, idx, sum)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if attempt >= maxRetries || !isRetryableSubRangeErr(err) {
			b.mu.Lock()
			r := b.ranges[idx]
			r.Status = store.BackfillRangeFailed
			r.Error = err.Error()
			r.UpdatedAt = time.Now()
			b.ranges[idx] = r
			sum.FailedRanges = append(sum.FailedRanges, FailedRange{
				FromLedger: r.FromLedger,
				ToLedger:   r.ToLedger,
				LastLedger: r.LastLedger,
				Err:        err,
			})
			snapshotRanges := cloneRanges(b.ranges)
			b.mu.Unlock()

			if !b.opts.DryRun {
				_ = b.persistRanges(ctx, snapshotRanges)
			}
			b.notifyRangeProgress()
			return
		}

		sleep := backoff
		if sleep > b.opts.MaxBackoff {
			sleep = b.opts.MaxBackoff
		}
		b.log.Warn("backfill sub-range transient error; retrying",
			"from_ledger", b.ranges[idx].FromLedger,
			"to_ledger", b.ranges[idx].ToLedger,
			"attempt", attempt+1,
			"retry_in", sleep,
			"error", err)

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
	}
}

func (b *Backfiller) runSingleSubRange(ctx context.Context, idx int, sum *Summary) error {
	b.mu.Lock()
	rng := b.ranges[idx]
	startLedger := rng.ResumeStartLedger()
	rangeTo := rng.ToLedger
	rng.Status = store.BackfillRangeInFlight
	rng.Error = ""
	rng.UpdatedAt = time.Now()
	b.ranges[idx] = rng
	b.mu.Unlock()
	b.notifyRangeProgress()

	if rangeTo > 0 && startLedger > rangeTo {
		return b.completeSubRange(ctx, idx, rangeTo)
	}

	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.waitRateLimit(ctx); err != nil {
			return err
		}

		resp, err := b.fetchSubRange(ctx, startLedger, rangeTo, cursor)
		if err != nil {
			return fmt.Errorf("fetching horizon page for range [%d..%d]: %w", rng.FromLedger, rangeTo, err)
		}
		if len(resp.Embedded.Records) == 0 {
			return b.completeSubRange(ctx, idx, rangeTo)
		}

		events, lastInRangeLedger, reachedPastEnd, perPage := b.extractSubRangePage(resp.Embedded.Records, startLedger, rangeTo)

		var inserted int64
		if len(events) > 0 && !b.opts.DryRun {
			ins, err := b.store.UpsertEvents(ctx, events)
			if err != nil {
				return fmt.Errorf("upserting %d backfilled events for range [%d..%d]: %w", len(events), rng.FromLedger, rangeTo, err)
			}
			inserted = ins
		}

		b.mu.Lock()
		sum.PagesFetched++
		sum.Transactions += perPage.Transactions
		sum.Skipped += perPage.Skipped
		sum.Failed += perPage.Failed
		sum.Extracted += int64(len(events))
		sum.Inserted += inserted
		if lastInRangeLedger > sum.ThroughLedger {
			sum.ThroughLedger = lastInRangeLedger
		}
		if lastInRangeLedger > b.ranges[idx].LastLedger {
			b.ranges[idx].LastLedger = lastInRangeLedger
			b.ranges[idx].UpdatedAt = time.Now()
		}
		snapshotRanges := cloneRanges(b.ranges)
		b.mu.Unlock()

		if b.opts.Progress != nil && len(events) > 0 {
			b.opts.Progress(int64(len(events)))
		}

		if len(events) > 0 && !b.opts.DryRun {
			if err := b.store.UpdateBackfillState(ctx, lastInRangeLedger); err != nil {
				return fmt.Errorf("saving backfill progress: %w", err)
			}
			if err := b.persistRanges(ctx, snapshotRanges); err != nil {
				return fmt.Errorf("saving backfill range progress: %w", err)
			}
		}
		b.notifyRangeProgress()

		if reachedPastEnd || len(resp.Embedded.Records) < b.opts.BatchSize {
			return b.completeSubRange(ctx, idx, rangeTo)
		}

		last := resp.Embedded.Records[len(resp.Embedded.Records)-1]
		cursor = last.PagingToken
		if cursor == "" {
			cursor = last.ID
		}
	}
}

func (b *Backfiller) completeSubRange(ctx context.Context, idx int, rangeTo int64) error {
	b.mu.Lock()
	r := b.ranges[idx]
	r.Status = store.BackfillRangeCompleted
	r.Error = ""
	if rangeTo > r.LastLedger {
		r.LastLedger = rangeTo
	}
	r.UpdatedAt = time.Now()
	b.ranges[idx] = r
	snapshotRanges := cloneRanges(b.ranges)
	b.mu.Unlock()

	if !b.opts.DryRun {
		if err := b.persistRanges(ctx, snapshotRanges); err != nil {
			return fmt.Errorf("saving completed range [%d..%d]: %w", r.FromLedger, r.ToLedger, err)
		}
	}
	b.notifyRangeProgress()
	return nil
}

func (b *Backfiller) persistRanges(ctx context.Context, ranges []store.BackfillRange) error {
	if rs, ok := b.store.(RangeStore); ok {
		return rs.SaveBackfillRanges(ctx, ranges)
	}
	return nil
}

func (b *Backfiller) notifyRangeProgress() {
	if b.opts.RangeProgress == nil {
		return
	}
	b.opts.RangeProgress(b.ProgressSnapshot())
}

func (b *Backfiller) waitRateLimit(ctx context.Context) error {
	if b.limiter == nil {
		return nil
	}
	return b.limiter.Wait(ctx)
}

func (b *Backfiller) fetchSubRange(ctx context.Context, fromLedger, toLedger int64, cursor string) (horizon.TransactionsResponse, error) {
	if rl, ok := b.client.(RangeTransactionLister); ok {
		return rl.ListContractTransactionsInRange(ctx, b.opts.ContractID, fromLedger, toLedger, cursor, b.opts.BatchSize, b.opts.IncludeFailed)
	}
	return b.fetch(ctx, cursor)
}

func isRetryableSubRangeErr(err error) bool {
	if errors.Is(err, horizon.ErrRateLimited) {
		return true
	}
	if errors.Is(err, horizon.ErrNotFound) {
		return false
	}
	return rpc.IsTransientHTTP(err)
}

func cloneRanges(in []store.BackfillRange) []store.BackfillRange {
	if len(in) == 0 {
		return nil
	}
	out := make([]store.BackfillRange, len(in))
	copy(out, in)
	return out
}

// fetch is a small wrapper that keeps the inner loop editable if we
// later add auth or a gRPC transport.
func (b *Backfiller) fetch(ctx context.Context, cursor string) (horizon.TransactionsResponse, error) {
	return b.client.ListContractTransactions(ctx, b.opts.ContractID, cursor, b.opts.BatchSize, b.opts.IncludeFailed)
}

// pageCounters is what extractPage returns alongside the events: the
// running tally the Summary needs.
type pageCounters struct {
	Transactions int64
	Skipped      int64
	Failed       int64
}

// extractSubRangePage filters a Horizon page strictly to [startLedger, rangeTo]
// so disjoint parallel workers never double-count or double-extract rows
// belonging to a sibling sub-range.
func (b *Backfiller) extractSubRangePage(rows []horizon.Transaction, startLedger, rangeTo int64) (events []store.Event, lastInRange int64, reachedPastEnd bool, perPage pageCounters) {
	currentLedger := int64(-1)
	txiInLedger := int32(0)

	for _, tx := range rows {
		if tx.Ledger < startLedger {
			continue
		}
		if rangeTo > 0 && tx.Ledger > rangeTo {
			reachedPastEnd = true
			continue
		}
		if tx.Ledger != currentLedger {
			currentLedger = tx.Ledger
			txiInLedger = 0
		} else {
			txiInLedger++
		}
		perPage.Transactions++
		lastInRange = tx.Ledger

		ex, err := horizon.ExtractContractEvents(b.decoder, b.opts.ContractID, horizon.TxHint{
			Hash:            tx.Hash,
			Ledger:          tx.Ledger,
			CreatedAt:       tx.CreatedAt,
			ResultCode:      tx.ResultCode,
			ResultMetaXDR:   tx.ResultMetaXDR,
			TxIndexInLedger: txiInLedger,
		})
		if err != nil {
			perPage.Failed++
			b.log.Warn("failed to extract events from horizon tx",
				"hash", tx.Hash, "ledger", tx.Ledger, "error", err)
			continue
		}
		if !ex.HadMeta || !ex.HadEvents {
			perPage.Skipped++
			continue
		}
		events = append(events, ex.Events...)
	}
	return events, lastInRange, reachedPastEnd, perPage
}

// extractPage walks one Horizon page, partition each tx's events based
// on the start/to-ledger bounds. Ledger sequencing: Horizon returns
// transactions in ledger/transaction order, so we reset the per-ledger
// tx_index counter when our running ledger changes — this gives every
// event a stable ordering within its ledger without depending on
// tx_index-from-the-RPC (Horizon doesn't expose it).
func (b *Backfiller) extractPage(rows []horizon.Transaction, startLedger int64) (events []store.Event, lastProcessed int64, perPage pageCounters) {
	currentLedger := int64(-1)
	txiInLedger := int32(0)

	for _, tx := range rows {
		// Discard rows strictly below the resume ledger. We re-fetch
		// from the start so progress is portable (we persist
		// lastLedger, not the opaque cursor).
		if tx.Ledger < startLedger {
			continue
		}
		if tx.Ledger != currentLedger {
			currentLedger = tx.Ledger
			txiInLedger = 0
		} else {
			txiInLedger++
		}
		perPage.Transactions++
		lastProcessed = tx.Ledger

		// Out-of-range txs are still tallied so ThroughLedger is
		// accurate; we just don't commit their events.
		if b.opts.ToLedger > 0 && tx.Ledger > b.opts.ToLedger {
			continue
		}

		ex, err := horizon.ExtractContractEvents(b.decoder, b.opts.ContractID, horizon.TxHint{
			Hash:            tx.Hash,
			Ledger:          tx.Ledger,
			CreatedAt:       tx.CreatedAt,
			ResultCode:      tx.ResultCode,
			ResultMetaXDR:   tx.ResultMetaXDR,
			TxIndexInLedger: txiInLedger,
		})
		if err != nil {
			perPage.Failed++
			b.log.Warn("failed to extract events from horizon tx",
				"hash", tx.Hash, "ledger", tx.Ledger, "error", err)
			continue
		}
		if !ex.HadMeta || !ex.HadEvents {
			perPage.Skipped++
			continue
		}
		events = append(events, ex.Events...)
	}
	return events, lastProcessed, perPage
}

// commitPage writes one page's events and advances the persisted
// progress. We deliberately call them in sequence (upsert first, then
// progress) so a partial failure below the upsert never claims
// progress on rows we didn't actually save.
func (b *Backfiller) commitPage(ctx context.Context, events []store.Event, lastLedger int64) (int64, error) {
	inserted, err := b.store.UpsertEvents(ctx, events)
	if err != nil {
		return inserted, fmt.Errorf("upserting %d backfilled events: %w", len(events), err)
	}
	if err := b.store.UpdateBackfillState(ctx, lastLedger); err != nil {
		return inserted, fmt.Errorf("saving backfill progress: %w", err)
	}
	return inserted, nil
}

// markTerminal cleans up the persisted row when the run finishes. In
// dry-run mode we leave the row untouched — operators expect the
// "nothing happened" signal for dry runs.
func (b *Backfiller) markTerminal(ctx context.Context, sum Summary) error {
	if b.opts.DryRun {
		// Reset any partial progress we may have set: dry runs
		// shouldn't leave a "last_ledger=42" impression that the
		// next non-dry run would inherit.
		if err := b.store.StartBackfillState(ctx, b.opts.ContractID, b.opts.FromLedger, b.opts.ToLedger); err != nil {
			return fmt.Errorf("resetting dry-run state: %w", err)
		}
		return nil
	}
	if !sum.Completed {
		// Interrupted run — leave progress intact so resume picks up.
		return nil
	}
	if err := b.store.CompleteBackfillState(ctx); err != nil {
		return fmt.Errorf("completing backfill state: %w", err)
	}
	return nil
}

// resume picks up saved progress for an identical in-progress run.
// Any divergence in (contract_id, from_ledger, to_ledger) starts fresh
// because resuming into a row whose bounds moved would silently drop
// events from the gap.
func (b *Backfiller) resume(ctx context.Context) (startLedger int64, sum Summary, err error) {
	if b.opts.DryRun {
		return b.opts.FromLedger, Summary{DryRun: true}, nil
	}

	state, err := b.store.GetBackfillState(ctx)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// fall through to Start.
	case err != nil:
		return 0, Summary{}, err
	case !state.Done() &&
		state.ContractID == b.opts.ContractID &&
		state.FromLedger == b.opts.FromLedger &&
		state.ToLedger == b.opts.ToLedger:
		start := state.LastLedger + 1
		if start < b.opts.FromLedger {
			start = b.opts.FromLedger
		}
		b.log.Info("backfill resuming",
			"contract_id", state.ContractID,
			"from_ledger", state.FromLedger,
			"to_ledger", state.ToLedger,
			"last_ledger", state.LastLedger,
			"resume_from", start)
		return start, Summary{Resumed: true}, nil
	}

	if err := b.store.StartBackfillState(ctx, b.opts.ContractID, b.opts.FromLedger, b.opts.ToLedger); err != nil {
		return 0, Summary{}, err
	}
	return b.opts.FromLedger, Summary{}, nil
}
