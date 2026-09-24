package audit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	testContract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	testNetwork  = "testnet"
)

// fixedRPCRange teaches the RPC mock to answer every getEvents call with
// the same n events per ledger for each ledger in [from, to].
func fixedRPCRange(cli *mockRPC, from, to uint32, n int, contract string) {
	cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
		var evs []rpc.Event
		for l := from; l <= to; l++ {
			evs = append(evs, mkEvents(l, n, contract)...)
		}
		return rpc.GetEventsResponse{Events: evs, LatestLedger: 1_000}, nil
	}
}

// auditOptions returns Options for tests that call the helpers directly
// rather than through PassOnce.
func auditOptions() Options {
	return Options{
		Network:           testNetwork,
		BatchLedgers:      1_000,
		LagThreshold:      10,
		MaxRepairAttempts: 3,
		FindingMaxLedgers: 10,
		PollInterval:      100 * time.Millisecond,
	}
}

// reconcileSetup wires a complete auditor plus its mocks for direct
// helper tests. Unless a test overrides it, the RPC mirrors the store's
// contents, which makes any repair converge immediately.
func reconcileSetup(t *testing.T, opts Options) (*Auditor, *mockRPC, *mockStore, *stubReingest) {
	t.Helper()
	a, cli, st, r := setup(t, opts)
	a.opts.Network = testNetwork
	cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		var evs []rpc.Event
		for _, e := range st.events {
			evs = append(evs, rpc.Event{
				ID:         e.ID,
				ContractID: e.ContractID,
				Ledger:     uint32(e.Ledger),
				Type:       e.Type,
			})
		}
		return rpc.GetEventsResponse{Events: evs, LatestLedger: 1_000}, nil
	}
	return a, cli, st, r
}

// TestReconcileRange_CleanRange covers the happy path: when the stored
// per-ledger counts match the RPC for the whole range, the high-water
// mark advances through it and no finding is opened.
func TestReconcileRange_CleanRange(t *testing.T) {
	a, _, st, _ := reconcileSetup(t, auditOptions())
	ctx := context.Background()

	for l := uint32(100); l <= 105; l++ {
		st.seedLedgers([]int{int(l)}, testContract)
	}

	worked, err := a.reconcileRange(ctx, testNetwork, 100, 105)
	require.NoError(t, err)
	assert.True(t, worked)

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(105), state.VerifiedThroughLedger,
		"a fully matching range advances the high-water mark through its end")
	assert.Empty(t, st.findings, "a clean range opens no finding")
	assert.Equal(t, uint64(6), a.metrics.LedgersChecked, "every ledger in the range is checked")
}

// TestReconcileRange_AdvancesOnlyThroughCleanPrefix covers the case where
// the mismatch sits in the middle: the HWM must never move past an
// unrepaired gap — it only advances once the repair has been verified.
func TestReconcileRange_AdvancesOnlyThroughCleanPrefix(t *testing.T) {
	a, cli, st, r := reconcileSetup(t, auditOptions())
	ctx := context.Background()

	// Ledgers 100..105 stored; 103 is missing, so the clean prefix is
	// 100..102. The repair succeeds, and only then may the HWM move on.
	for l := uint32(100); l <= 105; l++ {
		if l != 103 {
			st.seedLedgers([]int{int(l)}, testContract)
		}
	}
	fixedRPCRange(cli, 100, 105, 1, testContract)
	r.reingest = func(ctx context.Context, _ rpc.Client, from, to uint32) (int, error) {
		var evs []store.Event
		for l := from; l <= to; l++ {
			for _, e := range mkEvents(l, 1, testContract) {
				evs = append(evs, store.Event{ID: e.ID, ContractID: e.ContractID, Ledger: int64(e.Ledger), Type: e.Type})
			}
		}
		return len(evs), st.ReplaceEventsInRange(ctx, evs, int64(from), int64(to))
	}

	worked, err := a.reconcileRange(ctx, testNetwork, 100, 105)
	require.NoError(t, err)
	assert.True(t, worked)

	require.Len(t, st.findings, 1)
	assert.Equal(t, int64(103), st.findings[0].FromLedger,
		"the finding cluster opens at the first dirty ledger")

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(105), state.VerifiedThroughLedger,
		"the HWM advanced past the gap only after the repair was verified")
}

// TestReconcileRange_HWMStallsOnUnrepairedGap pins the stall behaviour:
// when the repair never converges, the high-water mark stays put and the
// mismatch keeps the auditor from declaring the range verified.
func TestReconcileRange_HWMStallsOnUnrepairedGap(t *testing.T) {
	a, cli, st, _ := reconcileSetup(t, auditOptions())
	ctx := context.Background()

	// Ledger 103 exists in the RPC but never in the store; repair does
	// nothing (the stub's nil reingest is a no-op), so the gap stays open.
	for l := uint32(100); l <= 105; l++ {
		if l != 103 {
			st.seedLedgers([]int{int(l)}, testContract)
		}
	}
	fixedRPCRange(cli, 100, 105, 1, testContract)

	worked, err := a.reconcileRange(ctx, testNetwork, 100, 105)
	require.NoError(t, err)
	assert.True(t, worked, "opening a finding counts as work done")

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(102), state.VerifiedThroughLedger,
		"the high-water mark stops at the clean prefix and never crosses the unrepaired gap")
	require.NotEmpty(t, st.findings, "the unrepaired gap keeps its finding")
}

// TestReconcileRange_RepairsBeforeAdvancing verifies that a fully-dirty
// range — nothing matches on arrival — is repaired and then marked
// verified, with the finding ending in the repaired state.
func TestReconcileRange_RepairsBeforeAdvancing(t *testing.T) {
	a, cli, st, r := reconcileSetup(t, auditOptions())
	ctx := context.Background()

	// Store has nothing; the RPC reports one event per ledger. Repair
	// re-ingests and fills the store, so the post-repair check succeeds.
	fixedRPCRange(cli, 200, 202, 1, testContract)
	r.reingest = func(ctx context.Context, _ rpc.Client, from, to uint32) (int, error) {
		var evs []store.Event
		for l := from; l <= to; l++ {
			for _, e := range mkEvents(l, 1, testContract) {
				evs = append(evs, store.Event{ID: e.ID, ContractID: e.ContractID, Ledger: int64(e.Ledger), Type: e.Type})
			}
		}
		return len(evs), st.ReplaceEventsInRange(ctx, evs, int64(from), int64(to))
	}

	worked, err := a.reconcileRange(ctx, testNetwork, 200, 202)
	require.NoError(t, err)
	assert.True(t, worked)

	require.Len(t, st.findings, 1)
	assert.Equal(t, store.FindingRepaired, st.findings[0].Status,
		"a range repaired by re-ingestion is marked repaired")

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(202), state.VerifiedThroughLedger,
		"a repaired range advances the high-water mark")
}

// TestReconcileRange_FindingBoundedByFindingMaxLedgers pins the bounding
// behaviour: a mismatch wider than FindingMaxLedgers produces one finding
// covering at most FindingMaxLedgers ledgers.
func TestReconcileRange_FindingBoundedByFindingMaxLedgers(t *testing.T) {
	opts := auditOptions()
	opts.FindingMaxLedgers = 5
	a, cli, st, _ := reconcileSetup(t, opts)
	ctx := context.Background()

	// The store has nothing for 300..309 (10 dirty ledgers) while the RPC
	// reports one event per ledger. Repair does nothing, so the cluster
	// stays dirty and gets bounded.
	fixedRPCRange(cli, 300, 309, 1, testContract)

	worked, err := a.reconcileRange(ctx, testNetwork, 300, 309)
	require.NoError(t, err)
	assert.True(t, worked)

	require.Len(t, st.findings, 1)
	f := st.findings[0]
	assert.Equal(t, int64(300), f.FromLedger)
	assert.Equal(t, int64(304), f.ToLedger,
		"the finding spans at most FindingMaxLedgers ledgers")

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(0), state.VerifiedThroughLedger,
		"the high-water mark never moves past an unrepaired gap")
}

// TestReconcileRange_EmptyRangeIsClean verifies that a range where both
// the store and the RPC agree there is nothing is clean, and advances the
// HWM — the auditor does not stall on quiet stretches of the chain.
func TestReconcileRange_EmptyRangeIsClean(t *testing.T) {
	a, _, st, _ := reconcileSetup(t, auditOptions())
	ctx := context.Background()

	worked, err := a.reconcileRange(ctx, testNetwork, 500, 505)
	require.NoError(t, err)
	assert.True(t, worked)

	state, err := st.GetAuditState(ctx, testNetwork)
	require.NoError(t, err)
	assert.Equal(t, int64(505), state.VerifiedThroughLedger,
		"an empty range where both sides agree is clean and advances the HWM")
	assert.Empty(t, st.findings)
}

// TestFetchRange covers the RPC paging helper: one walk per filter batch,
// pages followed until the RPC signals a short page, events past the
// range end dropped, and an out-of-retention error treated as end of data.
func TestFetchRange(t *testing.T) {
	t.Run("returns the events the RPC reports inside the range", func(t *testing.T) {
		a, cli, _, _ := reconcileSetup(t, auditOptions())
		fixedRPCRange(cli, 100, 102, 2, testContract)

		evs, err := a.fetchRange(context.Background(), 100, 102)
		require.NoError(t, err)
		assert.Len(t, evs, 6, "2 events per ledger across 3 ledgers")
		for _, e := range evs {
			assert.GreaterOrEqual(t, e.Ledger, uint32(100))
			assert.LessOrEqual(t, e.Ledger, uint32(102))
		}
	})

	t.Run("drops events past the end of the requested range", func(t *testing.T) {
		a, cli, _, _ := reconcileSetup(t, auditOptions())
		// The RPC answers with 100..110 regardless of what was asked; the
		// helper must keep only what falls inside [100,102].
		fixedRPCRange(cli, 100, 110, 1, testContract)

		evs, err := a.fetchRange(context.Background(), 100, 102)
		require.NoError(t, err)
		assert.Len(t, evs, 3, "events past the range end are filtered out")
	})

	t.Run("pages until the RPC returns a short page", func(t *testing.T) {
		a, cli, _, r := reconcileSetup(t, auditOptions())
		r.pageLimit = 2

		// First page: 2 events (full). Second page: 1 event (short → stop).
		var calls int
		cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
			calls++
			if calls == 1 {
				return rpc.GetEventsResponse{
					Events:       []rpc.Event{mkEvents(100, 1, testContract)[0], mkEvents(101, 1, testContract)[0]},
					LatestLedger: 1_000,
					Cursor:       "cursor-1",
				}, nil
			}
			return rpc.GetEventsResponse{
				Events:       []rpc.Event{mkEvents(102, 1, testContract)[0]},
				LatestLedger: 1_000,
			}, nil
		}

		evs, err := a.fetchRange(context.Background(), 100, 102)
		require.NoError(t, err)
		assert.Len(t, evs, 3, "events from both pages are collected")
		assert.Equal(t, 2, calls, "a short page ends the paging loop")

		require.Len(t, cli.eventsRequests, 2)
		assert.Equal(t, "cursor-1", cli.eventsRequests[1].Pagination.Cursor,
			"the second page resumes from the first page's cursor")
	})

	t.Run("stops when the last event of a full page passes the range end", func(t *testing.T) {
		a, cli, _, r := reconcileSetup(t, auditOptions())
		r.pageLimit = 2

		// One full page whose last event (ledger 105) already sits past
		// the requested end (102): asking for more pages is pointless.
		var calls int
		cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
			calls++
			return rpc.GetEventsResponse{
				Events:       []rpc.Event{mkEvents(100, 1, testContract)[0], mkEvents(105, 1, testContract)[0]},
				LatestLedger: 1_000,
			}, nil
		}

		evs, err := a.fetchRange(context.Background(), 100, 102)
		require.NoError(t, err)
		assert.Len(t, evs, 1, "only the in-range event is kept")
		assert.Equal(t, 1, calls, "paging stops once a page crosses the range end")
	})

	t.Run("a page limit of zero falls back to 1000", func(t *testing.T) {
		a, cli, _, _ := reconcileSetup(t, auditOptions())
		// r.pageLimit is the zero value here: PageLimit() reports 0 and
		// fetchRange must substitute its own default before calling the RPC.
		fixedRPCRange(cli, 100, 100, 1, testContract)

		_, err := a.fetchRange(context.Background(), 100, 100)
		require.NoError(t, err)
		require.NotEmpty(t, cli.eventsRequests)
		assert.Equal(t, uint(1000), cli.eventsRequests[0].Pagination.Limit,
			"a zero page limit must not be sent to the RPC")
	})

	t.Run("an out-of-retention range returns what was fetched so far", func(t *testing.T) {
		a, cli, _, _ := reconcileSetup(t, auditOptions())
		cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
			return rpc.GetEventsResponse{}, &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}
		}

		evs, err := a.fetchRange(context.Background(), 100, 102)
		require.NoError(t, err, "a ledger aging out of retention is end-of-data, not an error")
		assert.Empty(t, evs)
	})

	t.Run("other RPC errors are surfaced", func(t *testing.T) {
		a, cli, _, _ := reconcileSetup(t, auditOptions())
		cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
			return rpc.GetEventsResponse{}, errors.New("connection refused")
		}

		_, err := a.fetchRange(context.Background(), 100, 102)
		require.Error(t, err)
	})

	t.Run("filter batch construction errors are surfaced", func(t *testing.T) {
		a, _, _, r := reconcileSetup(t, auditOptions())
		r.filtersErr = errors.New("no watched contracts")

		_, err := a.fetchRange(context.Background(), 100, 102)
		require.Error(t, err)
	})
}

// TestHandleMismatch covers the finding-recording helper directly: what it
// records, which ids it counts as missing, and how repair attempts end.
func TestHandleMismatch(t *testing.T) {
	t.Run("records a finding carrying the missing ids", func(t *testing.T) {
		a, cli, st, _ := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		// Ledger 301: the RPC has two events, the store one of them, so
		// the other id is missing. Ledger 302 matches. Repair never
		// converges (the nil reingest is a no-op), which is fine — the
		// recorded shape is what this test pins.
		rpcByLedger := map[uint32][]string{
			301: mkIDs(301, 2),
			302: mkIDs(302, 1),
		}
		storedByLedger := map[uint32]int{301: 1, 302: 1}
		st.seedLedgers([]int{301, 302}, testContract)
		fixedRPCRange(cli, 301, 302, 1, testContract)
		cli.extraResponses = func(int) (rpc.GetEventsResponse, error) {
			return rpc.GetEventsResponse{
				Events:       append(mkEvents(301, 2, testContract), mkEvents(302, 1, testContract)...),
				LatestLedger: 1_000,
			}, nil
		}

		err := a.handleMismatch(ctx, testNetwork, rpcByLedger, storedByLedger, 301, 302, 302)
		require.NoError(t, err)

		require.Len(t, st.findings, 1)
		f := st.findings[0]
		assert.Equal(t, int64(301), f.FromLedger)
		assert.Equal(t, int64(302), f.ToLedger)
		assert.Equal(t, 3, f.ExpectedCount, "expected counts every RPC event in the cluster")
		assert.Equal(t, 2, f.ActualCount, "actual counts every stored event in the cluster")
		require.Len(t, f.MissingIDs, 1)
		assert.Equal(t, mkIDs(301, 2)[1], f.MissingIDs[0],
			"the missing id is the RPC event absent from the store")
		assert.Equal(t, uint64(1), a.metrics.FindingsOpened)
	})

	t.Run("a repaired range is marked repaired", func(t *testing.T) {
		a, _, st, r := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		// Repair re-ingests exactly what the RPC reports, so the
		// post-repair re-check (which mirrors the store) finds it clean.
		id := mkIDs(400, 1)[0]
		rpcByLedger := map[uint32][]string{400: {id}}
		storedByLedger := map[uint32]int{400: 0}
		r.reingest = func(ctx context.Context, _ rpc.Client, from, to uint32) (int, error) {
			evs := []store.Event{{ID: id, Ledger: 400, ContractID: testContract, Type: "contract"}}
			return len(evs), st.ReplaceEventsInRange(ctx, evs, int64(from), int64(to))
		}

		err := a.handleMismatch(ctx, testNetwork, rpcByLedger, storedByLedger, 400, 400, 400)
		require.NoError(t, err)

		require.Len(t, st.findings, 1)
		assert.Equal(t, store.FindingRepaired, st.findings[0].Status,
			"when re-ingestion fills the gap the finding is marked repaired")
	})

	t.Run("a range exceeding max repair attempts is marked unrecoverable", func(t *testing.T) {
		a, _, st, r := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		// Repair always fails with a generic error and produces no rows,
		// so the cluster stays dirty until the attempt cap.
		r.reingest = func(context.Context, rpc.Client, uint32, uint32) (int, error) {
			return 0, errors.New("rpc unavailable")
		}
		rpcByLedger := map[uint32][]string{410: mkIDs(410, 1)}
		storedByLedger := map[uint32]int{410: 0}

		err := a.handleMismatch(ctx, testNetwork, rpcByLedger, storedByLedger, 410, 410, 410)
		require.NoError(t, err)

		require.Len(t, st.findings, 1)
		f := st.findings[0]
		assert.Equal(t, store.FindingUnrecoverable, f.Status,
			"after MaxRepairAttempts the finding is marked unrecoverable, not retried forever")
		assert.Equal(t, a.opts.MaxRepairAttempts, f.Attempts)
		assert.NotEmpty(t, f.LastError)
		assert.Equal(t, uint64(1), a.metrics.FindingsUnrecoverable)
	})

	t.Run("a range aged out of rpc retention is marked unverifiable", func(t *testing.T) {
		a, _, st, r := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		// The repair reingest hits the retention error: the range is gone
		// from the RPC before it could be fixed.
		r.reingest = func(context.Context, rpc.Client, uint32, uint32) (int, error) {
			return 0, &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}
		}
		rpcByLedger := map[uint32][]string{420: mkIDs(420, 1)}
		storedByLedger := map[uint32]int{420: 0}

		err := a.handleMismatch(ctx, testNetwork, rpcByLedger, storedByLedger, 420, 420, 420)
		require.NoError(t, err)

		require.Len(t, st.findings, 1)
		f := st.findings[0]
		assert.Equal(t, store.FindingUnverifiable, f.Status,
			"a range that aged out of RPC retention cannot be verified or repaired")
		assert.Equal(t, uint64(1), a.metrics.FindingsUnverifiable)
	})

	t.Run("no finding when both sides agree the ledgers are empty", func(t *testing.T) {
		a, _, st, _ := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		err := a.handleMismatch(ctx, testNetwork, map[uint32][]string{}, map[uint32]int{}, 430, 431, 431)
		require.NoError(t, err)
		assert.Empty(t, st.findings, "zero expected and zero actual opens no finding")
	})

	t.Run("orphan events are counted but produce no missing ids", func(t *testing.T) {
		a, _, st, r := reconcileSetup(t, auditOptions())
		ctx := context.Background()
		r.reingest = func(context.Context, rpc.Client, uint32, uint32) (int, error) {
			return 0, nil
		}

		// The store has an event for a ledger the RPC reports nothing for:
		// expected 0, actual 2 by the census maps, and there is no RPC id
		// to list as missing.
		rpcByLedger := map[uint32][]string{}
		storedByLedger := map[uint32]int{440: 2}
		st.seedLedgers([]int{440}, testContract)

		err := a.handleMismatch(ctx, testNetwork, rpcByLedger, storedByLedger, 440, 440, 440)
		require.NoError(t, err)

		require.Len(t, st.findings, 1)
		f := st.findings[0]
		assert.Zero(t, f.ExpectedCount)
		assert.Equal(t, 2, f.ActualCount)
		assert.Empty(t, f.MissingIDs, "orphans are extra rows, not missing ones")
	})
}

// mkIDs mirrors mkEvents' id scheme without carrying the full events, so
// tests can build rpcByLedger maps by hand.
func mkIDs(ledger uint32, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%020d-%05d", ledger, i)
	}
	return ids
}

// TestAdvanceHWM covers the high-water-mark helper: it persists the new
// mark, never regresses an existing one, and surfaces store errors.
func TestAdvanceHWM(t *testing.T) {
	t.Run("persists a greater ledger", func(t *testing.T) {
		a, _, st, _ := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		require.NoError(t, st.SaveAuditState(ctx, store.AuditState{VerifiedThroughLedger: 50}))
		require.NoError(t, a.advanceHWM(ctx, testNetwork, 100))

		state, err := st.GetAuditState(ctx, testNetwork)
		require.NoError(t, err)
		assert.Equal(t, int64(100), state.VerifiedThroughLedger)
	})

	t.Run("does not regress an existing mark", func(t *testing.T) {
		a, _, st, _ := reconcileSetup(t, auditOptions())
		ctx := context.Background()

		require.NoError(t, st.SaveAuditState(ctx, store.AuditState{VerifiedThroughLedger: 100}))
		require.NoError(t, a.advanceHWM(ctx, testNetwork, 50))

		state, err := st.GetAuditState(ctx, testNetwork)
		require.NoError(t, err)
		assert.Equal(t, int64(100), state.VerifiedThroughLedger,
			"the conditional write never moves the HWM backwards")
	})
}
