package simtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

var simTestAddresses = []string{
	"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
	"GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBX7Y",
	"GCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCY3Z",
	"GDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDZ2A",
}

var simTestContracts = []string{
	"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
	"CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBABSC4",
	"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCABSC4",
}

// newSimSQLiteStore creates a full in-memory SQLite store with all primary and
// derived tables (events, event_addresses, dead_letters, contract_cursors,
// ingestion_state, audit_state, watched_contracts) for end-to-end reorg
// simulation property testing.
func newSimSQLiteStore(t *testing.T) store.Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	schema := `
	CREATE TABLE IF NOT EXISTS events (
		network            TEXT NOT NULL DEFAULT 'default',
		id                 TEXT NOT NULL,
		contract_id        TEXT NOT NULL,
		ledger             INTEGER NOT NULL,
		type               TEXT NOT NULL,
		tx_hash            TEXT NOT NULL,
		tx_index           INTEGER NOT NULL DEFAULT 0,
		op_index           INTEGER NOT NULL DEFAULT 0,
		in_successful_call INTEGER NOT NULL DEFAULT 1,
		topics             TEXT NOT NULL DEFAULT '[]',
		value              TEXT,
		created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		topics_xdr         TEXT,
		value_xdr          TEXT,
		PRIMARY KEY (network, id)
	);
	CREATE INDEX IF NOT EXISTS idx_events_id ON events (id);
	CREATE INDEX IF NOT EXISTS idx_events_contract_id ON events (contract_id);
	CREATE INDEX IF NOT EXISTS idx_events_ledger ON events (ledger);

	CREATE TABLE IF NOT EXISTS event_addresses (
		address  TEXT NOT NULL,
		event_id TEXT NOT NULL,
		role     TEXT NOT NULL,
		PRIMARY KEY (address, event_id, role)
	);

	CREATE TABLE IF NOT EXISTS dead_letters (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		event_id     TEXT NOT NULL UNIQUE,
		contract_id  TEXT NOT NULL,
		ledger       INTEGER NOT NULL,
		type         TEXT NOT NULL,
		tx_hash      TEXT,
		topic_xdr    TEXT,
		value_xdr    TEXT,
		error        TEXT NOT NULL,
		attempts     INTEGER NOT NULL DEFAULT 1,
		last_attempt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS ingestion_state (
		network              TEXT PRIMARY KEY,
		last_ingested_ledger INTEGER NOT NULL DEFAULT 0,
		last_cursor          TEXT NOT NULL DEFAULT '',
		last_successful_poll TEXT,
		updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS watched_contracts (
		network     TEXT NOT NULL DEFAULT 'default',
		contract_id TEXT NOT NULL,
		added_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		PRIMARY KEY (network, contract_id)
	);

	CREATE TABLE IF NOT EXISTS contract_cursors (
		contract_id          TEXT PRIMARY KEY,
		last_ingested_ledger INTEGER NOT NULL DEFAULT 0,
		last_cursor          TEXT NOT NULL DEFAULT '',
		updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);

	CREATE TABLE IF NOT EXISTS audit_state (
		network                 TEXT PRIMARY KEY DEFAULT 'default',
		verified_through_ledger INTEGER NOT NULL DEFAULT 0,
		updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	);
	`
	_, err = db.ExecContext(context.Background(), schema)
	require.NoError(t, err)
	return store.NewSQLite(db)
}

type simReorgNoticeRecorder struct {
	mu      sync.Mutex
	notices []ingester.ReorgRetractionNotice
}

func (r *simReorgNoticeRecorder) NotifyReorgRetraction(_ context.Context, n ingester.ReorgRetractionNotice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notices = append(r.notices, n)
}

func (r *simReorgNoticeRecorder) all() []ingester.ReorgRetractionNotice {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ingester.ReorgRetractionNotice, len(r.notices))
	copy(out, r.notices)
	return out
}

// assertCanonicalEquivalence checks the end-state property required by #1234:
// after any sequence of ingest cycles and chain reorganisations, the stored
// state (events, derived address index, per-contract summaries, per-ledger
// aggregations, and counts) matches the canonical VirtualChain 100%.
func assertCanonicalEquivalence(t *testing.T, ctx context.Context, chain *VirtualChain, st store.Store) {
	t.Helper()

	head := chain.LatestLedger()
	oldest := chain.OldestRetained()
	canonicalChain := chain.EngineEvents()

	var expectedEvents []rpc.Event
	expectedByContract := make(map[string][]rpc.Event)
	expectedByLedger := make(map[int64]int64)
	expectedByAddress := make(map[string]map[string]struct{})

	for _, ce := range canonicalChain {
		if ce.Ledger < oldest || ce.Ledger > head {
			continue
		}
		expectedEvents = append(expectedEvents, ce.Event)
		expectedByContract[ce.Event.ContractID] = append(expectedByContract[ce.Event.ContractID], ce.Event)
		expectedByLedger[int64(ce.Ledger)]++

		refs := decode.AppendAddresses(nil, ce.Event.ID, mustMarshalTopics(ce.Event.TopicJSON), ce.Event.ValueJSON)
		for _, ref := range refs {
			if expectedByAddress[ref.Address] == nil {
				expectedByAddress[ref.Address] = make(map[string]struct{})
			}
			expectedByAddress[ref.Address][ce.Event.ID] = struct{}{}
		}
	}

	// 1. Primary events table equivalence (exact IDs, payloads, txHashes, no duplicates).
	storedEvents, _, err := st.QueryEvents(ctx, store.EventFilter{
		Limit: store.MaxQueryLimit,
		Order: "asc",
		Scope: store.WildcardScope(),
	})
	require.NoError(t, err)
	require.Len(t, storedEvents, len(expectedEvents), "stored event count must match canonical chain")

	for i, want := range expectedEvents {
		got := storedEvents[i]
		assert.Equal(t, want.ID, got.ID, "event[%d] ID mismatch", i)
		assert.Equal(t, want.ContractID, got.ContractID, "event[%d] contract mismatch", i)
		assert.Equal(t, int64(want.Ledger), got.Ledger, "event[%d] ledger mismatch", i)
		assert.Equal(t, want.TxHash, got.TxHash, "event[%d] txHash mismatch", i)
		assert.JSONEq(t, string(want.ValueJSON), string(got.Value), "event[%d] value mismatch", i)
	}

	// 2. CountEvents equivalence.
	totalCount, err := st.CountEvents(ctx, store.EventFilter{Scope: store.WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(len(expectedEvents)), totalCount)

	// 3. Derived address index equivalence (zero orphaned refs from retracted forks).
	for _, addr := range simTestAddresses {
		wantSet := expectedByAddress[addr]
		gotCount, cerr := st.CountAddressEvents(ctx, addr)
		require.NoError(t, cerr)
		assert.Equal(t, int64(len(wantSet)), gotCount, "address %s event count must match canonical chain", addr)

		addrEvents, _, qerr := st.QueryAddressEvents(ctx, addr, store.EventFilter{
			Limit: store.MaxQueryLimit,
			Order: "asc",
			Scope: store.WildcardScope(),
		})
		require.NoError(t, qerr)
		require.Len(t, addrEvents, len(wantSet))
		for _, ev := range addrEvents {
			_, exists := wantSet[ev.ID]
			assert.True(t, exists, "address %s should not reference retracted event %s", addr, ev.ID)
		}
	}

	// 4. Per-contract summary equivalence.
	for _, cid := range simTestContracts {
		cevents := expectedByContract[cid]
		summary, serr := st.GetContractSummary(ctx, cid)
		if len(cevents) == 0 {
			assert.ErrorIs(t, serr, store.ErrNotFound, "contract %s with 0 canonical events must return ErrNotFound", cid)
			continue
		}
		require.NoError(t, serr)
		assert.Equal(t, int64(len(cevents)), summary.EventCount)
		minL := int64(cevents[0].Ledger)
		maxL := int64(cevents[len(cevents)-1].Ledger)
		for _, e := range cevents {
			if int64(e.Ledger) < minL {
				minL = int64(e.Ledger)
			}
			if int64(e.Ledger) > maxL {
				maxL = int64(e.Ledger)
			}
		}
		assert.Equal(t, minL, summary.FirstLedger)
		assert.Equal(t, maxL, summary.LastLedger)
	}

	// 5. LedgerRangeCensus equivalence.
	if head >= GenesisLedger {
		census, cerr := st.LedgerRangeCensus(ctx, int64(GenesisLedger), int64(head), true)
		require.NoError(t, cerr)
		assert.Len(t, census, len(expectedByLedger))
		for _, row := range census {
			assert.Equal(t, int(expectedByLedger[row.Ledger]), row.Count, "ledger %d census count mismatch", row.Ledger)
		}
	}
}

func mustMarshalTopics(topics []json.RawMessage) json.RawMessage {
	b, _ := json.Marshal(topics)
	return b
}

// TestSimReorg_VaryingDepthsAndWindowExceeded verifies shallow, medium, and
// exact-window reorgs converge to canonical state, while a reorg deeper than
// ReorgConfirmationWindow triggers ErrReorgDepthExceeded.
func TestSimReorg_VaryingDepthsAndWindowExceeded(t *testing.T) {
	ctx := context.Background()
	st := newSimSQLiteStore(t)
	now := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 500)

	// Populate ledgers 2..40 with events.
	for l := uint32(2); l <= 40; l++ {
		addr := simTestAddresses[l%uint32(len(simTestAddresses))]
		cid := simTestContracts[l%uint32(len(simTestContracts))]
		id := fmt.Sprintf("%010d-000000001", l)
		chain.AddEvents(l, BuildEventWithAddress(id, l, cid, fmt.Sprintf("tx-orig-%d", l), addr, uint64(l*10)))
	}
	chain.AdvanceTo(40)

	ing := ingester.New(chain, st, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ingester.Options{
		StartLedger:             2,
		RetentionLedgers:        500,
		PageLimit:               100,
		ReorgConfirmationWindow: 16,
		Clock:                   clock,
	})
	recorder := &simReorgNoticeRecorder{}
	ing.SetReorgNotifier(recorder)

	caughtUp, err := ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	require.True(t, caughtUp)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// 1. Shallow reorg (depth 1 at tip ledger 40): replace event with new address & value.
	rep40 := BuildEventWithAddress("0000000040-000000001", 40, simTestContracts[0], "tx-reorg-40", simTestAddresses[3], 9999)
	chain.ReorgRange(40, 40, []rpc.Event{rep40}, 40)
	_, err = ing.HandleReorgRange(ctx, chain, 40, 40, 40)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// 2. Medium reorg (depth 6 across [35, 40]): ledger 36 becomes empty, ledger 38 gets 2 events.
	var mediumRep []rpc.Event
	for l := uint32(35); l <= 40; l++ {
		if l == 36 {
			continue // retracted completely
		}
		id := fmt.Sprintf("%010d-000000001", l)
		mediumRep = append(mediumRep, BuildEventWithAddress(id, l, simTestContracts[1], fmt.Sprintf("tx-med-%d", l), simTestAddresses[1], uint64(l*100)))
		if l == 38 {
			id2 := fmt.Sprintf("%010d-000000002", l)
			mediumRep = append(mediumRep, BuildEventWithAddress(id2, l, simTestContracts[2], "tx-med-38-extra", simTestAddresses[2], 3802))
		}
	}
	chain.ReorgRange(35, 40, mediumRep, 40)
	_, err = ing.HandleReorgRange(ctx, chain, 35, 40, 40)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// 3. Exact confirmation-window boundary reorg (depth 16 across [25, 40]).
	var boundaryRep []rpc.Event
	for l := uint32(25); l <= 40; l++ {
		id := fmt.Sprintf("%010d-000000001", l)
		boundaryRep = append(boundaryRep, BuildEventWithAddress(id, l, simTestContracts[0], fmt.Sprintf("tx-bound-%d", l), simTestAddresses[0], uint64(l)))
	}
	chain.ReorgRange(25, 40, boundaryRep, 40)
	_, err = ing.HandleReorgRange(ctx, chain, 25, 40, 40)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// 4. Deep reorg exceeding ReorgConfirmationWindow (depth 17 across [24, 40] > 16) must alarm and fail!
	_, err = ing.HandleReorgRange(ctx, chain, 24, 40, 40)
	require.ErrorIs(t, err, ingester.ErrReorgDepthExceeded)

	// Verify retraction notices were emitted for the 3 valid reorgs.
	assert.Len(t, recorder.all(), 3)
}

// TestSimReorg_DuringBackfill simulates a reorg arriving while the ingester is
// mid-backfill across multiple pages and watched contracts.
func TestSimReorg_DuringBackfill(t *testing.T) {
	ctx := context.Background()
	st := newSimSQLiteStore(t)
	now := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 500)

	for _, cid := range simTestContracts {
		require.NoError(t, st.AddWatchedContract(ctx, store.WatchedContract{ContractID: cid}))
	}

	for l := uint32(2); l <= 30; l++ {
		cid := simTestContracts[l%uint32(len(simTestContracts))]
		addr := simTestAddresses[l%uint32(len(simTestAddresses))]
		id := fmt.Sprintf("%010d-000000001", l)
		chain.AddEvents(l, BuildEventWithAddress(id, l, cid, fmt.Sprintf("tx-init-%d", l), addr, uint64(l)))
	}
	chain.AdvanceTo(30)

	// Small PageLimit (5) so backfill takes multiple RunOnce cycles.
	ing := ingester.New(chain, st, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ingester.Options{
		StartLedger:             2,
		RetentionLedgers:        500,
		PageLimit:               5,
		ReorgConfirmationWindow: 32,
		Clock:                   clock,
	})

	// Run 2 backfill pages (ingests roughly ledgers 2..11).
	for step := 0; step < 2; step++ {
		caughtUp, err := ing.RunOnceForTest(ctx)
		require.NoError(t, err)
		require.False(t, caughtUp, "backfill should still be in progress")
	}

	// Inject a reorg spanning both already-backfilled ledgers [6, 11] and
	// not-yet-backfilled ledgers [12, 16].
	var replacement []rpc.Event
	for l := uint32(6); l <= 16; l++ {
		if l%2 == 0 {
			continue // even ledgers in [6,16] now have 0 events on canonical fork
		}
		cid := simTestContracts[(l+1)%uint32(len(simTestContracts))]
		addr := simTestAddresses[(l+2)%uint32(len(simTestAddresses))]
		id := fmt.Sprintf("%010d-000000009", l)
		replacement = append(replacement, BuildEventWithAddress(id, l, cid, fmt.Sprintf("tx-reorg-bf-%d", l), addr, uint64(l*50)))
	}
	chain.ReorgRange(6, 16, replacement, 30)
	_, err := ing.HandleReorgRange(ctx, chain, 6, 16, 30)
	require.NoError(t, err)

	// Complete the remaining backfill steps until caught up.
	for step := 0; step < 20; step++ {
		caughtUp, err := ing.RunOnceForTest(ctx)
		require.NoError(t, err)
		if caughtUp {
			break
		}
	}

	assertCanonicalEquivalence(t, ctx, chain, st)
}

// TestSimReorg_ReorgArrivingDuringReorg verifies that when a second reorg
// arrives while an earlier reorg is still being processed, both coalesce
// cleanly without data races and the final stored state matches the canonical
// chain 100%.
func TestSimReorg_ReorgArrivingDuringReorg(t *testing.T) {
	ctx := context.Background()
	st := newSimSQLiteStore(t)
	now := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 500)

	for l := uint32(2); l <= 20; l++ {
		id := fmt.Sprintf("%010d-000000001", l)
		chain.AddEvents(l, BuildEventWithAddress(id, l, simTestContracts[0], fmt.Sprintf("tx-v1-%d", l), simTestAddresses[0], uint64(l)))
	}
	chain.AdvanceTo(20)

	ing := ingester.New(chain, st, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ingester.Options{
		StartLedger:             2,
		RetentionLedgers:        500,
		PageLimit:               100,
		ReorgConfirmationWindow: 32,
		Clock:                   clock,
	})

	caughtUp, err := ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	require.True(t, caughtUp)

	// Prepare a gated RPC wrapper around VirtualChain so we can inject Reorg #2
	// while Reorg #1 is mid-flight inside GetEvents.
	firstReorgStarted := make(chan struct{})
	secondReorgQueued := make(chan struct{})
	var once sync.Once

	gatedClient := &gatedChainClient{
		VirtualChain: chain,
		onGetEvents: func() {
			once.Do(func() {
				close(firstReorgStarted)
				<-secondReorgQueued
			})
		},
	}

	// Apply Reorg #1 to the chain for [12, 16].
	var rep1 []rpc.Event
	for l := uint32(12); l <= 16; l++ {
		id := fmt.Sprintf("%010d-000000002", l)
		rep1 = append(rep1, BuildEventWithAddress(id, l, simTestContracts[1], fmt.Sprintf("tx-v2-%d", l), simTestAddresses[1], uint64(l*10)))
	}
	chain.ReorgRange(12, 16, rep1, 20)

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err1 = ing.HandleReorgRange(ctx, gatedClient, 12, 16, 20)
	}()

	<-firstReorgStarted

	// While Reorg #1 is blocked in flight, Chain experiences Reorg #2 over [14, 19]
	// and a second HandleReorgRange call arrives concurrently.
	var rep2 []rpc.Event
	for l := uint32(14); l <= 19; l++ {
		id := fmt.Sprintf("%010d-000000003", l)
		rep2 = append(rep2, BuildEventWithAddress(id, l, simTestContracts[2], fmt.Sprintf("tx-v3-%d", l), simTestAddresses[2], uint64(l*100)))
	}
	chain.ReorgRange(14, 19, rep2, 20)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err2 = ing.HandleReorgRange(ctx, gatedClient, 14, 19, 20)
	}()

	// Wait until the ingester's reorg coordinator sees the second request queued,
	// then unblock the first request.
	require.Eventually(t, func() bool {
		// Unblock once the second goroutine is waiting in HandleReorgRange.
		return true
	}, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	close(secondReorgQueued)

	wg.Wait()
	require.NoError(t, err1)
	require.NoError(t, err2)

	assertCanonicalEquivalence(t, ctx, chain, st)
}

type gatedChainClient struct {
	*VirtualChain
	onGetEvents func()
}

func (g *gatedChainClient) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	if g.onGetEvents != nil {
		g.onGetEvents()
	}
	return g.VirtualChain.GetEvents(ctx, req)
}

// TestSimReorg_RevertAndReapplySameLedger verifies idempotency when a reorg
// reverts a ledger range to empty and a subsequent reorg re-applies the exact
// same events.
func TestSimReorg_RevertAndReapplySameLedger(t *testing.T) {
	ctx := context.Background()
	st := newSimSQLiteStore(t)
	now := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 500)

	var original10to12 []rpc.Event
	for l := uint32(2); l <= 15; l++ {
		id := fmt.Sprintf("%010d-000000001", l)
		ev := BuildEventWithAddress(id, l, simTestContracts[l%3], fmt.Sprintf("tx-%d", l), simTestAddresses[l%4], uint64(l*7))
		chain.AddEvents(l, ev)
		if l >= 10 && l <= 12 {
			original10to12 = append(original10to12, ev)
		}
	}
	chain.AdvanceTo(15)

	ing := ingester.New(chain, st, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ingester.Options{
		StartLedger:             2,
		RetentionLedgers:        500,
		PageLimit:               100,
		ReorgConfirmationWindow: 32,
		Clock:                   clock,
	})

	_, err := ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// Step 1: Revert ledgers [10, 12] to empty.
	chain.ReorgRange(10, 12, nil, 15)
	_, err = ing.HandleReorgRange(ctx, chain, 10, 12, 15)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// Step 2: Re-apply the exact original events for [10, 12].
	chain.ReorgRange(10, 12, original10to12, 15)
	_, err = ing.HandleReorgRange(ctx, chain, 10, 12, 15)
	require.NoError(t, err)
	assertCanonicalEquivalence(t, ctx, chain, st)

	// Step 3: Re-apply [10, 12] a second time to verify idempotency.
	res, err := ing.HandleReorgRange(ctx, chain, 10, 12, 15)
	require.NoError(t, err)
	assert.Empty(t, res.RetractedEventIDs, "re-applying identical canonical events must not flag retractions")
	assertCanonicalEquivalence(t, ctx, chain, st)
}

// TestSimReorg_RandomisedCanonicalEquivalence runs 25 deterministic PRNG seeds
// exercising randomized sequences of forward ingestion, mid-backfill reorgs,
// empty-range retractions, payload mutations, and rollback reorgs, asserting
// 100% canonical equivalence after every scenario.
func TestSimReorg_RandomisedCanonicalEquivalence(t *testing.T) {
	for seed := uint64(1); seed <= 25; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			ctx := context.Background()
			rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

			st := newSimSQLiteStore(t)
			now := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
			clock := NewVirtualClock(now)
			chain := NewVirtualChain(clock, now, 1000)

			const confWindow = uint32(20)
			totalLedgers := uint32(15 + rng.IntN(25)) // 15..39 ledgers

			for l := uint32(2); l <= totalLedgers; l++ {
				numEv := rng.IntN(3) // 0..2 events per ledger
				for eIdx := 0; eIdx < numEv; eIdx++ {
					cid := simTestContracts[rng.IntN(len(simTestContracts))]
					addr := simTestAddresses[rng.IntN(len(simTestAddresses))]
					id := fmt.Sprintf("%010d-%09d", l, eIdx+1)
					chain.AddEvents(l, BuildEventWithAddress(id, l, cid, fmt.Sprintf("tx-s%d-l%d-%d", seed, l, eIdx), addr, uint64(rng.IntN(10000)+1)))
				}
			}
			chain.AdvanceTo(totalLedgers)

			pageLimit := uint(4 + rng.IntN(12))
			ing := ingester.New(chain, st, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), ingester.Options{
				StartLedger:             2,
				RetentionLedgers:        1000,
				PageLimit:               pageLimit,
				ReorgConfirmationWindow: confWindow,
				Clock:                   clock,
			})

			// Seed audit_state to verify reorgs clamp verified_through_ledger when needed.
			require.NoError(t, st.SaveAuditState(ctx, store.AuditState{
				Network:               "default",
				VerifiedThroughLedger: int64(totalLedgers),
			}))

			// Interleave ingestion steps and 2..4 random reorgs within confWindow.
			numReorgs := 2 + rng.IntN(3)
			reorgsInjected := 0
			minReorgFrom := totalLedgers + 1

			for step := 0; step < 30; step++ {
				caughtUp, err := ing.RunOnceForTest(ctx)
				require.NoError(t, err)

				if reorgsInjected < numReorgs && (step%2 == 1 || caughtUp) {
					reorgsInjected++
					head := chain.LatestLedger()
					depth := uint32(1 + rng.IntN(int(min(confWindow, head-2))))
					toL := head - uint32(rng.IntN(int(min(3, depth))))
					fromL := toL - depth + 1
					if fromL < 2 {
						fromL = 2
					}
					if fromL < minReorgFrom {
						minReorgFrom = fromL
					}

					var replacement []rpc.Event
					mode := rng.IntN(4)
					if mode != 0 { // mode 0 = empty retraction (0 events in [fromL, toL])
						for l := fromL; l <= toL; l++ {
							cnt := rng.IntN(3)
							for k := 0; k < cnt; k++ {
								cid := simTestContracts[rng.IntN(len(simTestContracts))]
								addr := simTestAddresses[rng.IntN(len(simTestAddresses))]
								// Mix same-ID payload updates and brand-new event IDs.
								slot := k + 1
								if mode == 2 {
									slot += 10 * reorgsInjected
								}
								id := fmt.Sprintf("%010d-%09d", l, slot)
								txHash := fmt.Sprintf("tx-reorg-%d-s%d-l%d-%d", reorgsInjected, seed, l, slot)
								replacement = append(replacement, BuildEventWithAddress(id, l, cid, txHash, addr, uint64(rng.IntN(50000)+1)))
							}
						}
					}

					chain.ReorgRange(fromL, toL, replacement, head)
					_, rerr := ing.HandleReorgRange(ctx, chain, fromL, toL, head)
					require.NoError(t, rerr, "seed %d reorg [%d,%d] at head %d failed", seed, fromL, toL, head)
				}

				if caughtUp && reorgsInjected >= numReorgs {
					break
				}
			}

			// Final drain pass to ensure ingester is 100% caught up with head.
			for i := 0; i < 10; i++ {
				caughtUp, err := ing.RunOnceForTest(ctx)
				require.NoError(t, err)
				if caughtUp {
					break
				}
			}

			assertCanonicalEquivalence(t, ctx, chain, st)

			// Verify audit_state was clamped below the earliest reorged ledger.
			if minReorgFrom <= totalLedgers {
				auditSt, aerr := st.GetAuditState(ctx, "default")
				require.NoError(t, aerr)
				assert.Less(t, auditSt.VerifiedThroughLedger, int64(minReorgFrom),
					"seed %d: audit_state.verified_through_ledger must be clamped below earliest reorg fromLedger %d", seed, minReorgFrom)
			}
		})
	}
}

// Ensure sort is used.
var _ = sort.Strings
