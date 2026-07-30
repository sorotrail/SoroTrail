package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"

	"github.com/khaylebfortune/sorotrail/internal/decode"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// TokenBalanceProcessor derives per-address token balances from SEP-41 token
// events and persists them atomically. It implements EventNotifier so it can
// be wired directly into the ingester's notification chain.
//
// Correctness under re-ingest:
//   - Idempotency is guaranteed by the last_applied_event watermark: events
//     whose ID is <= the stored watermark are skipped. Re-ingested events
//     with IDs below the watermark do not double-count.
//   - Derive-from-scratch semantics (e.g. via the decoder replay tool) can be
//     built on top of NextReplayBatch + UpsertTokenBalanceState for a clean
//     slate.
//
// Honest limitation:
//   - Balances are derived only from events SoroTrail has stored. Contracts
//     with pre-ingestion history (events that were emitted before SoroTrail
//     started indexing) will show artificially low balances. Use
//     `sorotrail backfill` to close the gap, or the `earliest_ledger` field
//     exposed on the holders endpoint to judge coverage.
type TokenBalanceProcessor struct {
	store store.Store
	log   *slog.Logger
}

// NewTokenBalanceProcessor creates a processor that persists token balance
// updates derived from incoming events.
func NewTokenBalanceProcessor(st store.Store, log *slog.Logger) *TokenBalanceProcessor {
	return &TokenBalanceProcessor{store: st, log: log}
}

// NotifyEvents implements EventNotifier. It is called after events are
// persisted and must not block ingestion for long.
func (p *TokenBalanceProcessor) NotifyEvents(ctx context.Context, events []store.Event) {
	if ctx.Err() != nil {
		return
	}
	if err := p.processEvents(ctx, events); err != nil {
		p.log.Error("token balance processor", "error", err)
	}
}

// processEvents extracts SEP-41 token events, groups them by contract,
// and applies balance changes atomically per contract.
func (p *TokenBalanceProcessor) processEvents(ctx context.Context, events []store.Event) error {
	// Parse all token events from the batch.
	var tokenEvents []*decode.TokenEvent
	for _, ev := range events {
		te := decode.ParseTokenEvent(ev.ContractID, ev.ID, ev.Ledger, ev.Network, ev.Topics, ev.Value)
		if te == nil {
			continue
		}
		tokenEvents = append(tokenEvents, te)
	}
	if len(tokenEvents) == 0 {
		return nil
	}

	// Group by (network, contract_id).
	type contractKey struct {
		network    string
		contractID string
	}
	groups := make(map[contractKey][]*decode.TokenEvent)
	for _, te := range tokenEvents {
		key := contractKey{network: te.Network, contractID: te.ContractID}
		groups[key] = append(groups[key], te)
	}

	// Process each contract group.
	for key, group := range groups {
		if err := p.processContractGroup(ctx, key.network, key.contractID, group); err != nil {
			return fmt.Errorf("processing token balances for %s/%s: %w", key.network, key.contractID, err)
		}
	}
	return nil
}

// processContractGroup applies a batch of token events for a single contract.
func (p *TokenBalanceProcessor) processContractGroup(ctx context.Context, network, contractID string, events []*decode.TokenEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Sort events by event ID (which sorts by ledger) to ensure deterministic processing.
	sort.Slice(events, func(i, j int) bool {
		return events[i].EventID < events[j].EventID
	})

	// Determine the last event ID in this batch for the watermark.
	lastEventID := events[len(events)-1].EventID
	lastLedger := events[len(events)-1].Ledger

	// Get current state to skip already-applied events.
	currentState, err := p.store.GetTokenBalanceState(ctx, network, contractID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("getting token balance state: %w", err)
	}

	// Filter out events that have already been applied.
	var newEvents []*decode.TokenEvent
	if err == nil && currentState.LastAppliedEventID != "" {
		for _, te := range events {
			if te.EventID > currentState.LastAppliedEventID {
				newEvents = append(newEvents, te)
			}
		}
	} else {
		newEvents = events
	}

	if len(newEvents) == 0 {
		return nil
	}

	// Collect all changes and unique addresses.
	type addrChange struct {
		address string
		delta   *big.Int // positive for credit, negative for debit
		ledger  int64
	}
	changes := make([]addrChange, 0, len(newEvents)*2)
	addrSet := make(map[string]struct{})

	for _, te := range newEvents {
		switch te.Kind {
		case decode.TokenTransfer:
			changes = append(changes,
				addrChange{address: te.From, delta: new(big.Int).Neg(te.Amount), ledger: te.Ledger},
				addrChange{address: te.To, delta: new(big.Int).Set(te.Amount), ledger: te.Ledger},
			)
			addrSet[te.From] = struct{}{}
			addrSet[te.To] = struct{}{}

		case decode.TokenMint:
			changes = append(changes, addrChange{address: te.To, delta: new(big.Int).Set(te.Amount), ledger: te.Ledger})
			addrSet[te.To] = struct{}{}

		case decode.TokenBurn:
			changes = append(changes, addrChange{address: te.From, delta: new(big.Int).Neg(te.Amount), ledger: te.Ledger})
			addrSet[te.From] = struct{}{}

		case decode.TokenClawback:
			changes = append(changes, addrChange{address: te.From, delta: new(big.Int).Neg(te.Amount), ledger: te.Ledger})
			addrSet[te.From] = struct{}{}
		}
	}

	// Read all current balances for this contract in one query.
	// TODO: For contracts with many holders (10k+), consider targeted queries
	// or batching to avoid fetching every row.
	currentBalances, err := p.readAllBalances(ctx, network, contractID)
	if err != nil {
		return fmt.Errorf("reading current balances: %w", err)
	}

	// Apply all changes.
	updateMap := make(map[string]*store.TokenBalanceUpdate)
	for _, ch := range changes {
		current := currentBalances[ch.address]
		if current == nil {
			current = new(big.Int)
		}
		newBalance := new(big.Int).Add(current, ch.delta)
		if newBalance.Sign() < 0 {
			p.log.Warn("negative balance clamped to zero",
				"contract_id", contractID,
				"address", ch.address,
				"current", current.String(),
				"delta", ch.delta.String(),
				"network", network,
			)
			newBalance = new(big.Int)
		}
		currentBalances[ch.address] = newBalance

		if existing, ok := updateMap[ch.address]; ok {
			existing.Balance = newBalance
			if ch.ledger > existing.LastLedger {
				existing.LastLedger = ch.ledger
			}
		} else {
			updateMap[ch.address] = &store.TokenBalanceUpdate{
				Address:    ch.address,
				Balance:    new(big.Int).Set(newBalance),
				LastLedger: ch.ledger,
			}
		}
	}

	// Build the final update list.
	updates := make([]store.TokenBalanceUpdate, 0, len(updateMap))
	for _, u := range updateMap {
		updates = append(updates, *u)
	}

	// Persist atomically.
	state := store.TokenBalanceState{
		Network:            network,
		ContractID:         contractID,
		LastAppliedEventID: lastEventID,
		LastLedger:         lastLedger,
	}
	return p.store.UpsertTokenBalances(ctx, network, state, updates)
}

// readAllBalances reads all current token balances for a contract into a map.
func (p *TokenBalanceProcessor) readAllBalances(ctx context.Context, network, contractID string) (map[string]*big.Int, error) {
	balances, _, err := p.store.GetTokenBalances(ctx, contractID, network, "0", "", 100000)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*big.Int, len(balances))
	for _, tb := range balances {
		n := new(big.Int)
		n, ok := n.SetString(tb.Balance, 10)
		if !ok {
			return nil, fmt.Errorf("parsing balance for %s: %q", tb.Address, tb.Balance)
		}
		result[tb.Address] = n
	}
	return result, nil
}
