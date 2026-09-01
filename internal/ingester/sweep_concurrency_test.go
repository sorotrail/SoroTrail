package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Issue #145: "Parallelize Contract Filter Sweeps with Bounded
// Concurrency". Bounded concurrent sweeping already exists on main
// (Options.SweepConcurrency, windowSweep's errgroup.SetLimit fan-out —
// see ingester.go). This file adds the two tests that directly prove the
// issue's stated acceptance criteria rather than just exercising the
// mechanics (which TestWindowSweep_ParallelBatchesComplete and friends in
// ingester_test.go already cover):
//
//   - cycle time drops for >25 contracts with concurrency > 1
//   - no events are lost or duplicated vs the serial path
//
// Both tests drive windowSweep directly (the level at which
// SweepConcurrency fans batches out) against hand-built batches, one
// single-contract filter per batch — this mirrors the shape
// buildFilterBatches produces once a watch list spills past 25 contracts,
// without needing 750+ fixture contracts to observe >25 independent
// request chains.

// sweepBatches builds n single-contract filter batches: batch i watches
// exactly contract i. Each batch is therefore its own independent
// getEvents request chain, which is the unit windowSweep fans out over
// SweepConcurrency.
func sweepBatches(n int) [][]rpc.EventFilter {
	batches := make([][]rpc.EventFilter, n)
	for i := 0; i < n; i++ {
		batches[i] = []rpc.EventFilter{{
			Type:        "contract",
			ContractIDs: []string{fmt.Sprintf("C%055d", i)},
		}}
	}
	return batches
}

// contractEvent builds a decodable rpc.Event for a specific contract,
// event ID and ledger. Unlike the package's rpcEvent helper (fixed
// contract ID), this lets the parity test give every batch a distinct,
// attributable fixture event.
func contractEvent(contractID, id string, ledger uint32) rpc.Event {
	return rpc.Event{
		ID:         id,
		Type:       "contract",
		Ledger:     ledger,
		ContractID: contractID,
		TxHash:     "abc123",
		ValueJSON:  json.RawMessage(`{"u64":1}`),
		TopicJSON:  []json.RawMessage{json.RawMessage(`{"symbol":"transfer"}`)},
	}
}

// latencyRPC simulates an RPC endpoint whose GetEvents call takes a fixed
// amount of wall-clock time to answer, independent of any other in-flight
// call (time.After doesn't hold a lock). It always answers with zero
// events, so each batch completes in exactly one round trip — that makes
// the sweep's total elapsed time a direct, predictable function of
// ceil(numBatches/concurrency) * latency, which is what
// TestSweepConcurrency_FasterThanSerial measures.
type latencyRPC struct {
	health  rpc.Health
	latency time.Duration
	calls   atomic.Int64
}

func (c *latencyRPC) GetEvents(ctx context.Context, _ rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	select {
	case <-time.After(c.latency):
	case <-ctx.Done():
		return rpc.GetEventsResponse{}, ctx.Err()
	}
	c.calls.Add(1)
	return rpc.GetEventsResponse{LatestLedger: c.health.LatestLedger}, nil
}

func (c *latencyRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: c.health.LatestLedger}, nil
}

func (c *latencyRPC) GetHealth(context.Context) (rpc.Health, error) { return c.health, nil }

func (c *latencyRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

func (c *latencyRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

// TestSweepConcurrency_FasterThanSerial proves acceptance criterion #1:
// "Cycle time drops for >25 contracts with concurrency > 1". It sweeps 32
// single-contract batches (>25) against an RPC double whose GetEvents
// call has a fixed artificial latency, once with SweepConcurrency:1 and
// once with SweepConcurrency:8, and asserts the concurrent run's
// wall-clock time is well under half the serial run's — generous slack
// (2x rather than the ~8x theoretical speedup) to keep the test robust on
// loaded CI machines. Latency is kept in single-digit milliseconds so the
// whole test runs in well under a second.
func TestSweepConcurrency_FasterThanSerial(t *testing.T) {
	const (
		numBatches = 32
		latency    = 8 * time.Millisecond
	)
	batches := sweepBatches(numBatches)
	health := rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10}

	run := func(t *testing.T, concurrency int) time.Duration {
		t.Helper()
		client := &latencyRPC{health: health, latency: latency}
		ing := newTestIngester(client, newMockStore(), Options{
			SweepWindow:      1_000,
			PageLimit:        100,
			SweepConcurrency: concurrency,
		})
		start := time.Now()
		_, err := ing.windowSweep(context.Background(), 100, batches)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.EqualValues(t, numBatches, client.calls.Load(),
			"every batch must issue exactly one GetEvents call")
		return elapsed
	}

	serial := run(t, 1)
	concurrent := run(t, 8)

	t.Logf("serial=%s concurrent=%s (batches=%d, per-call latency=%s)",
		serial, concurrent, numBatches, latency)
	assert.Less(t, concurrent, serial/2,
		"SweepConcurrency>1 must meaningfully cut wall-clock time for >25 batches")
}

// keyedRPC answers GetEvents based on the single contract ID carried by
// the request's filter, so concurrent goroutines calling it in any order
// each get exactly the fixture event that belongs to their own batch —
// order-independent by construction, which is what lets the test compare
// a serial and a concurrent run over IDENTICAL input batches.
type keyedRPC struct {
	mu         sync.Mutex
	health     rpc.Health
	byContract map[string]rpc.GetEventsResponse
	calls      []string // contract IDs seen, in call order
}

func (c *keyedRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	if len(req.Filters) != 1 || len(req.Filters[0].ContractIDs) != 1 {
		return rpc.GetEventsResponse{}, fmt.Errorf("keyedRPC: expected exactly one single-contract filter, got %+v", req.Filters)
	}
	contractID := req.Filters[0].ContractIDs[0]
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, contractID)
	resp, ok := c.byContract[contractID]
	if !ok {
		return rpc.GetEventsResponse{}, fmt.Errorf("keyedRPC: no fixture scripted for contract %q", contractID)
	}
	return resp, nil
}

func (c *keyedRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: c.health.LatestLedger}, nil
}

func (c *keyedRPC) GetHealth(context.Context) (rpc.Health, error) { return c.health, nil }

func (c *keyedRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

func (c *keyedRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

// TestSweepConcurrency_NoLostOrDuplicatedEvents proves acceptance
// criterion #2: "No events are lost or duplicated vs the serial path". It
// runs 30 single-contract batches, each with its own distinct fixture
// event, through windowSweep twice against identical fixture data — once
// serially (SweepConcurrency:1) and once concurrently
// (SweepConcurrency:4) — and asserts the two runs persist exactly the
// same set of events: same IDs, same count, no batch re-persisted.
func TestSweepConcurrency_NoLostOrDuplicatedEvents(t *testing.T) {
	const numBatches = 30
	batches := sweepBatches(numBatches)
	health := rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10}

	byContract := make(map[string]rpc.GetEventsResponse, numBatches)
	for i := 0; i < numBatches; i++ {
		contractID := fmt.Sprintf("C%055d", i)
		byContract[contractID] = rpc.GetEventsResponse{
			Events:       []rpc.Event{contractEvent(contractID, fmt.Sprintf("e%d", i), 200+uint32(i))},
			LatestLedger: health.LatestLedger,
		}
	}

	run := func(t *testing.T, concurrency int) *mockStore {
		t.Helper()
		client := &keyedRPC{health: health, byContract: byContract}
		st := newMockStore()
		ing := newTestIngester(client, st, Options{
			SweepWindow:      1_000,
			PageLimit:        100,
			SweepConcurrency: concurrency,
		})
		_, err := ing.windowSweep(context.Background(), 100, batches)
		require.NoError(t, err)
		require.Len(t, client.calls, numBatches, "every batch fires exactly one GetEvents call")
		return st
	}

	serial := run(t, 1)
	concurrent := run(t, 4)

	require.Len(t, serial.events, numBatches, "serial run persists exactly one event per batch")
	require.Len(t, concurrent.events, numBatches, "concurrent run persists exactly one event per batch — no duplicates from overlapping goroutines")

	assert.Equal(t, serial.events, concurrent.events,
		"the persisted event set must be identical between the serial and concurrent sweep paths — no lost or duplicated events")

	// Flattened UpsertEvents call history: total events written across
	// every call must equal the distinct count for each run, proving no
	// batch was retried or re-persisted under concurrency.
	assertNoDuplicateUpserts(t, serial.upserted, numBatches)
	assertNoDuplicateUpserts(t, concurrent.upserted, numBatches)
}

func assertNoDuplicateUpserts(t *testing.T, upserted [][]store.Event, want int) {
	t.Helper()
	seen := make(map[string]bool, want)
	var total int
	for _, batch := range upserted {
		for _, ev := range batch {
			require.False(t, seen[ev.ID], "event %s upserted more than once", ev.ID)
			seen[ev.ID] = true
			total++
		}
	}
	assert.Equal(t, want, total, "total persisted events must equal the number of batches")
}
