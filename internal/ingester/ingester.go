// Package ingester runs the polling loop that pulls contract events from
// Stellar RPC and persists them.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/khaylebfortune/sorotrail/internal/broadcast"
	"github.com/khaylebfortune/sorotrail/internal/decode"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
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
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Minute
	}
	if o.SweepWindow == 0 {
		o.SweepWindow = 1000
	}
	if o.LagMetrics == nil {
		o.LagMetrics = noopLagMetrics{}
	}
	// LagWarnLedgers intentionally NOT defaulted to 100 here: see
	// Options.LagWarnLedgers doc comment. The env-config layer applies
	// the 100 default; Ingester callers who want any other behavior
	// (including disabled=0) get exactly what they ask for.
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
func (ing *Ingester) Run(ctx context.Context) error {
	backoff := time.Second
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
			sleep := backoff/2 + rand.N(backoff/2)
			ing.log.Error("ingestion pass failed", "error", err, "retry_in", sleep)
			if !sleepCtx(ctx, sleep) {
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
			backoff = time.Second
			if caughtUp {
				if !sleepCtx(ctx, ing.opts.PollInterval) {
					return ctx.Err()
				}
			}
		}
	}
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
func (ing *Ingester) windowSweep(ctx context.Context, start uint32, batches [][]rpc.EventFilter) (bool, error) {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return false, fmt.Errorf("getHealth for sweep window: %w", err)
	}
	if start > health.LatestLedger {
		return true, nil // nothing new yet
	}
	end := min(start+ing.opts.SweepWindow-1, health.LatestLedger)

	for _, filters := range batches {
		cursor := ""
		for {
			resp, err := ing.client.GetEvents(ctx, rpc.GetEventsRequest{
				StartLedger: start,
				EndLedger:   end + 1, // endLedger is exclusive
				Filters:     filters,
				Pagination:  &rpc.Pagination{Cursor: cursor, Limit: ing.opts.PageLimit},
			})
			if rpc.IsLedgerOutOfRange(err) {
				return false, ing.reclampToOldest(ctx, start)
			}
			if err != nil {
				return false, fmt.Errorf("getEvents sweep [%d,%d]: %w", start, end, err)
			}
			if err := ing.persistEvents(ctx, resp.Events, resp.LatestLedger); err != nil {
				return false, err
			}
			if uint(len(resp.Events)) < ing.opts.PageLimit {
				break
			}
			// Cursor requests can't carry the ledger range, so enforce the
			// window bound here once a page crosses it.
			if resp.Events[len(resp.Events)-1].Ledger > end {
				break
			}
			if cursor = resp.Cursor; cursor == "" {
				cursor = resp.Events[len(resp.Events)-1].CursorValue()
			}
		}
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
	state, err := ing.store.GetIngestionState(ctx)
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
	var filters []rpc.EventFilter
	for start := 0; start < len(watched); start += rpc.MaxContractIDsPerFilter {
		end := min(start+rpc.MaxContractIDsPerFilter, len(watched))
		filters = append(filters, rpc.EventFilter{Type: "contract", ContractIDs: watched[start:end]})
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
