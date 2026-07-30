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
	PollInterval     time.Duration
	StartLedger      uint32
	RetentionLedgers uint32
	PageLimit        uint
	MaxBackoff       time.Duration
	SweepWindow      uint32
	Network          string
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
	if o.Network == "" {
		o.Network = "default"
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
	notifier EventNotifier
	bcast    *broadcast.Broadcaster
}

// New wires an Ingester.
func New(client rpc.Client, st store.Store, dec decode.Decoder, log *slog.Logger, opts Options) *Ingester {
	opts.applyDefaults()
	return &Ingester{client: client, store: st, decoder: dec, log: log, opts: opts}
}

// SetNotifier attaches an optional EventNotifier.
func (ing *Ingester) SetNotifier(n EventNotifier) {
	ing.notifier = n
}

// WithBroadcaster attaches a live event broadcaster.
func (ing *Ingester) WithBroadcaster(b *broadcast.Broadcaster) *Ingester {
	ing.bcast = b
	return ing
}

// Run polls until ctx is canceled.
func (ing *Ingester) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		caughtUp, err := ing.runOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			sleep := backoff/2 + rand.N(backoff/2)
			ing.log.Error("ingestion pass failed", "network", ing.opts.Network, "error", err, "retry_in", sleep)
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
		}
	}
}

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
		state.LastIngestedLedger = int64(startLedger) - 1
	}
	state.Network = ing.opts.Network
	if err := ing.store.SaveIngestionState(ctx, state); err != nil {
		return false, err
	}
	return caughtUp, nil
}

// ReingestRange re-fetches events for the closed range [fromLedger, toLedger].
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
// filter batches respecting the RPC caps.
func (ing *Ingester) BuildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error) {
	return ing.buildFilterBatches(ctx)
}

// PageLimit returns the getEvents pagination cap.
func (ing *Ingester) PageLimit() uint { return ing.opts.PageLimit }

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

	if cursor != "" {
		return store.IngestionState{LastIngestedLedger: lastLedger, LastCursor: cursor}, caughtUp
	}
	return store.IngestionState{LastIngestedLedger: int64(resp.LatestLedger) - 1}, caughtUp
}

func (ing *Ingester) windowSweep(ctx context.Context, start uint32, batches [][]rpc.EventFilter) (bool, error) {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return false, fmt.Errorf("getHealth for sweep window: %w", err)
	}
	if start > health.LatestLedger {
		return true, nil
	}
	end := min(start+ing.opts.SweepWindow-1, health.LatestLedger)

	for _, filters := range batches {
		cursor := ""
		for {
			resp, err := ing.client.GetEvents(ctx, rpc.GetEventsRequest{
				StartLedger: start,
				EndLedger:   end + 1,
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
			if resp.Events[len(resp.Events)-1].Ledger > end {
				break
			}
			if cursor = resp.Cursor; cursor == "" {
				cursor = resp.Events[len(resp.Events)-1].CursorValue()
			}
		}
	}

	lastIngested := int64(end)
	if end >= health.LatestLedger {
		lastIngested = int64(end) - 1
	}
	err = ing.store.SaveIngestionState(ctx, store.IngestionState{
		Network:            ing.opts.Network,
		LastIngestedLedger: lastIngested,
	})
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
		"network", ing.opts.Network,
		"count", len(events), "new", inserted,
		"through_ledger", rpcEvents[len(rpcEvents)-1].Ledger,
		"latest_ledger", latestLedger)

	if ing.notifier != nil {
		ing.notifier.NotifyEvents(ctx, events)
	}
	if ing.bcast != nil {
		ing.bcast.Publish(ctx, events)
	}
	return nil
}

func (ing *Ingester) resolvePosition(ctx context.Context) (startLedger uint32, cursor string, err error) {
	state, err := ing.store.GetIngestionState(ctx, ing.opts.Network)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, "", err
	}
	if err == nil && state.LastCursor != "" {
		return 0, state.LastCursor, nil
	}
	if err == nil && state.LastIngestedLedger > 0 {
		return uint32(state.LastIngestedLedger) + 1, "", nil
	}

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
		start = 2
	}
	ing.log.Info("cold start", "network", ing.opts.Network, "start_ledger", start, "latest_ledger", health.LatestLedger)
	return uint32(start), "", nil
}

func (ing *Ingester) reclampToOldest(ctx context.Context, requested uint32) error {
	health, err := ing.client.GetHealth(ctx)
	if err != nil {
		return fmt.Errorf("getHealth while re-clamping: %w", err)
	}
	ing.log.Warn("resume ledger fell outside RPC retention window; skipping ahead",
		"network", ing.opts.Network,
		"requested_ledger", requested, "oldest_retained", health.OldestLedger)
	return ing.store.SaveIngestionState(ctx, store.IngestionState{
		Network:            ing.opts.Network,
		LastIngestedLedger: int64(health.OldestLedger) - 1,
	})
}

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
		Network:          ing.opts.Network,
		RawTopicXDR:      re.Topic,
		RawValueXDR:      re.Value,
	}, nil
}

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
