// Package ingester runs the polling loop that pulls contract events from
// Stellar RPC and persists them.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Options configure an Ingester.
type Options struct {
	// PollInterval is how long to sleep once caught up. Default 5s.
	PollInterval time.Duration
	// StartLedger, when non-zero, overrides the cold-start position.
	StartLedger uint32
	// RetentionLedgers is how far behind the latest ledger a cold start
	// reaches when StartLedger is unset. Default 17280 (~24h).
	RetentionLedgers uint32
	// PageLimit is the getEvents pagination limit per request. Default 1000.
	PageLimit uint
	// MaxBackoff caps the error backoff. Default 1m.
	MaxBackoff time.Duration
	// SweepWindow bounds the ledger range scanned per pass when the watched
	// list needs more than one getEvents request (see buildFilterBatches).
	// Default 1000 ledgers.
	SweepWindow uint32
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
}

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
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Minute
	}
	if o.SweepWindow == 0 {
		o.SweepWindow = 1000
	}
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
	client   rpc.Client
	store    store.Store
	decoder  decode.Decoder
	log      *slog.Logger
	opts     Options
	bcast    *broadcast.Broadcaster
	notifier EventNotifier // optional; nil means no notification
}

// New wires an Ingester. All dependencies are interfaces so tests can supply
// mocks.
func New(client rpc.Client, st store.Store, dec decode.Decoder, log *slog.Logger, opts Options) *Ingester {
	opts.applyDefaults()
	return &Ingester{client: client, store: st, decoder: dec, log: log, opts: opts}
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
func (ing *Ingester) Run(ctx context.Context) error {
	backoff := time.Second
	lastReorgRescanAt := time.Time{}
	for {
		caughtUp, err := ing.runOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			// Jittered exponential backoff so restarts don't thundering-herd
			// a shared endpoint.
			sleep := backoff/2 + rand.N(backoff/2)
			ing.log.Error("ingestion pass failed", "error", err, "retry_in", sleep)
			if !sleepCtx(ctx, sleep) {
				return ctx.Err()
			}
			if backoff *= 2; backoff > ing.opts.MaxBackoff {
				backoff = ing.opts.MaxBackoff
			}
		default:
			backoff = time.Second
			if caughtUp {
				if !sleepCtx(ctx, ing.opts.PollInterval) {
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
	state, err := ing.store.GetIngestionState(ctx)
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
func (ing *Ingester) runOnce(ctx context.Context) (caughtUp bool, err error) {
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

// singlePage issues one getEvents call and persists the page plus the resume
// cursor, so even huge backfills make durable progress request by request.
func (ing *Ingester) singlePage(ctx context.Context, startLedger uint32, cursor string, filters []rpc.EventFilter) (bool, error) {
	resp, err := ing.client.GetEvents(ctx, rpc.GetEventsRequest{
		StartLedger: startLedger,
		Filters:     filters,
		Pagination:  &rpc.Pagination{Cursor: cursor, Limit: ing.opts.PageLimit},
	})
	if rpc.IsLedgerOutOfRange(err) {
		return false, ing.reclampToOldest(ctx, startLedger)
	}
	if err != nil {
		return false, fmt.Errorf("getEvents from ledger %d: %w", startLedger, err)
	}

	if err := ing.persistEvents(ctx, resp.Events, resp.LatestLedger); err != nil {
		return false, err
	}

	state, caughtUp := nextState(resp, ing.opts.PageLimit)
	if state.LastCursor == "" && state.LastIngestedLedger <= 0 {
		// Degenerate response (no events, no cursor, no latestLedger):
		// hold position rather than regressing to a cold start.
		state.LastIngestedLedger = int64(startLedger) - 1
	}
	if err := ing.store.SaveIngestionState(ctx, state); err != nil {
		return false, err
	}
	return caughtUp, nil
}

// ReingestRange re-fetches events for the closed range [fromLedger, toLedger]
// using the caller-supplied client (the auditor passes its budget-paced
// client so repair traffic is accounted against the audit pool), then
// calls the store's replace-in-range so orphans (rows the RPC no longer
// reports) are deleted and same-ID rows are updated (topic/value drift on
// the RPC side is corrected). It does NOT advance the ingester's
// persisted cursor — the auditor uses this to repair specific ledger
// ranges without disturbing the ongoing ingestion frontier.
//
// All events the RPC returns with ledger > toLedger are dropped: the RPC's
// cursor pagination strips endLedger from the request, so the last page
// may legitimately spill past the requested end.
func (ing *Ingester) ReingestRange(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32) (int, error) {
	if fromLedger > toLedger {
		return 0, fmt.Errorf("ReingestRange: from %d > to %d", fromLedger, toLedger)
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

// reingestBatch pages one filter batch over [fromLedger, toLedger] in
// memory, then hands the post-filtered result to the store.
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
			// Aged out during repair — not an error, just an empty answer.
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("ReingestRange getEvents [%d,%d]: %w", fromLedger, toLedger, err)
		}
		// Defensive post-filter: cursor pagination strips endLedger from
		// the request, so the tail of a full page may legitimately include
		// events past our toLedger bound.
		for _, e := range resp.Events {
			if e.Ledger <= toLedger {
				collected = append(collected, e)
			}
		}
		if uint(len(resp.Events)) < ing.opts.PageLimit {
			break
		}
		last := resp.Events[len(resp.Events)-1]
		if last.Ledger > toLedger {
			break
		}
		cursor = resp.Cursor
		if cursor == "" {
			cursor = last.CursorValue()
		}
	}
	storeEvents := make([]store.Event, 0, len(collected))
	for _, re := range collected {
		ev, err := ing.toStoreEvent(re)
		if err != nil {
			return 0, err
		}
		storeEvents = append(storeEvents, ev)
	}
	if err := ing.store.ReplaceEventsInRange(ctx, storeEvents, int64(fromLedger), int64(toLedger)); err != nil {
		return 0, fmt.Errorf("ReplaceEventsInRange [%d,%d]: %w", fromLedger, toLedger, err)
	}
	return len(storeEvents), nil
}

// BuildFilterBatches converts the watched-contract list into getEvents
// filter batches respecting the RPC caps (≤5 contractIds per filter,
// ≤5 filters per request, ≤25 watched contracts per request chain).
// Exported so the auditor can fetch with the exact same filter set as
// ingest — events outside this filter set were intentionally not stored
// and must not be flagged as audit discrepancies.
func (ing *Ingester) BuildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error) {
	return ing.buildFilterBatches(ctx)
}

// PageLimit returns the getEvents pagination cap the ingester uses for
// its own loop. The auditor reuses this so it can never silently
// disagree on page size for an RPC round trip.
func (ing *Ingester) PageLimit() uint { return ing.opts.PageLimit }

// nextState derives the resume position after a page. A full page means more
// data is likely waiting: keep paging via cursor immediately. A short page
// means the RPC gave us everything it has, so we are caught up — but we
// still prefer resuming by cursor, because startLedger must stay within the
// server's retained range and "latest + 1" is rejected. Only when no cursor
// is available (old server, empty page) do we fall back to re-scanning the
// latest ledger; idempotent upserts make the overlap harmless.
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

	if cursor != "" {
		return store.IngestionState{LastIngestedLedger: lastLedger, LastCursor: cursor}, caughtUp
	}
	return store.IngestionState{LastIngestedLedger: int64(resp.LatestLedger) - 1}, caughtUp
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
		return true, nil // nothing new yet
	}
	end := min(start+ing.opts.SweepWindow-1, health.LatestLedger)

	// When SweepConcurrency > 1, two batches run in parallel. If one
	// returns IsLedgerOutOfRange, errgroup cancels the others and the
	// first goroutine to surface an error to g.Wait() wins — which means
	// a still-in-flight batch's ctx.Canceled can mask a sibling's
	// IsLedgerOutOfRange signal and we'd skip the reclamp. Flag the
	// condition out-of-band so the post-Wait check sees it regardless
	// of which error races out first.
	var ledgerOutOfRange atomic.Bool
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(ing.opts.SweepConcurrency)
	for _, filters := range batches {
		filters := filters
		g.Go(func() error {
			return ing.sweepBatch(gctx, &ledgerOutOfRange, start, end, filters)
		})
	}
	if err := g.Wait(); err != nil {
		if ledgerOutOfRange.Load() || rpc.IsLedgerOutOfRange(err) {
			return false, ing.reclampToOldest(ctx, start)
		}
		return false, err
	}

	// Resume from end+1 next pass — unless the window reached the chain
	// head, where end-1 keeps the next startLedger within the server's
	// retained range (a startLedger past latest is rejected). The one
	// re-scanned ledger is deduplicated by the idempotent upserts.
	lastIngested := int64(end)
	if end >= health.LatestLedger {
		lastIngested = int64(end) - 1
	}
	err = ing.store.SaveIngestionState(ctx, store.IngestionState{LastIngestedLedger: lastIngested})
	if err != nil {
		return false, err
	}
	return end >= health.LatestLedger, nil
}

// sweepBatch pages one filter batch through [start, end]. Errors are
// returned to the errgroup; ctx cancellation aborts the inner pagination
// loop (GetEvents respects ctx) so a graceful shutdown doesn't strand
// half-paged batches.
//
// ledgerOutOfRange is a side-channel for IsLedgerOutOfRange that survives
// the errgroup canceling sibling goroutines — see windowSweep's comment.
func (ing *Ingester) sweepBatch(ctx context.Context, ledgerOutOfRange *atomic.Bool, start, end uint32, filters []rpc.EventFilter) error {
	cursor := ""
	for {
		resp, err := ing.client.GetEvents(ctx, rpc.GetEventsRequest{
			StartLedger: start,
			EndLedger:   end + 1, // endLedger is exclusive
			Filters:     filters,
			Pagination:  &rpc.Pagination{Cursor: cursor, Limit: ing.opts.PageLimit},
		})
		if rpc.IsLedgerOutOfRange(err) {
			ledgerOutOfRange.Store(true)
			return err
		}
		if err != nil {
			return fmt.Errorf("getEvents sweep [%d,%d]: %w", start, end, err)
		}
		if err := ing.persistEvents(ctx, resp.Events, resp.LatestLedger); err != nil {
			return err
		}
		if uint(len(resp.Events)) < ing.opts.PageLimit {
			return nil
		}
		// Cursor requests can't carry the ledger range, so enforce the
		// window bound here once a page crosses it.
		if resp.Events[len(resp.Events)-1].Ledger > end {
			return nil
		}
		if cursor = resp.Cursor; cursor == "" {
			cursor = resp.Events[len(resp.Events)-1].CursorValue()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (ing *Ingester) persistEvents(ctx context.Context, rpcEvents []rpc.Event, latestLedger uint32) error {
	if len(rpcEvents) == 0 {
		return nil
	}
	events := make([]store.Event, 0, len(rpcEvents))
	for _, re := range rpcEvents {
		ev, err := ing.toStoreEvent(re)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}
	inserted, err := ing.store.UpsertEvents(ctx, events)
	if err != nil {
		return err
	}
	ing.log.Info("ingested events",
		"count", len(events), "new", inserted,
		"through_ledger", rpcEvents[len(rpcEvents)-1].Ledger,
		"latest_ledger", latestLedger)

	if ing.bcast != nil {
		ing.bcast.Publish(ctx, events)
	}
	// Notify webhooks (or other listeners) after successful persistence.
	// This is a fire-and-forget call — it must never block ingestion.
	if ing.notifier != nil {
		ing.notifier.NotifyEvents(ctx, events)
	}
	return nil
}

// resolvePosition decides where the next pass starts: the saved cursor
// (mid-pagination), the ledger after the last ingested one (warm start), or
// latest-minus-retention (cold start).
func (ing *Ingester) resolvePosition(ctx context.Context) (startLedger uint32, cursor string, err error) {
	state, err := ing.store.GetIngestionState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, "", err
	}
	if err == nil && state.LastCursor != "" {
		return 0, state.LastCursor, nil
	}
	if err == nil && state.LastIngestedLedger > 0 {
		return uint32(state.LastIngestedLedger) + 1, "", nil
	}

	// Cold start.
	if ing.opts.StartLedger > 0 {
		return ing.opts.StartLedger, "", nil
	}
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("getHealth for cold start: %w", err)
	}
	start := int64(health.LatestLedger) - int64(ing.opts.RetentionLedgers)
	if oldest := int64(health.OldestLedger); start < oldest {
		start = oldest
	}
	if start < 2 {
		start = 2 // ledger 1 predates any events; the RPC rejects 0
	}
	ing.log.Info("cold start", "start_ledger", start, "latest_ledger", health.LatestLedger)
	return uint32(start), "", nil
}

// reclampToOldest handles a resume point that aged out of the RPC's
// retention window (e.g. the indexer was down for days): skip ahead to the
// oldest retained ledger and accept the gap.
func (ing *Ingester) reclampToOldest(ctx context.Context, requested uint32) error {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return fmt.Errorf("getHealth while re-clamping: %w", err)
	}
	ing.log.Warn("resume ledger fell outside RPC retention window; skipping ahead — events in the gap are lost",
		"requested_ledger", requested, "oldest_retained", health.OldestLedger)
	return ing.store.SaveIngestionState(ctx, store.IngestionState{
		LastIngestedLedger: int64(health.OldestLedger) - 1,
	})
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

// sleepCtx sleeps for d or until ctx is done; it reports whether the full
// sleep completed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
