package audit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
) // stubReingest is a stand-in for ingester.Ingester. It records filter-batches
// and reingest-range calls so tests can verify the auditor's path.
type stubReingest struct {
	mu       sync.Mutex
	ranges   []struct{ From, To uint32 }
	filters  []rpc.EventFilter
	reingest func(ctx context.Context, client rpc.Client, from, to uint32) (int, error)
}

func (s *stubReingest) BuildFilterBatches(context.Context) ([][]rpc.EventFilter, error) {
	return [][]rpc.EventFilter{s.filters}, nil
}

func (s *stubReingest) ReingestRange(ctx context.Context, client rpc.Client, from, to uint32) (int, error) {
	s.mu.Lock()
	s.ranges = append(s.ranges, struct{ From, To uint32 }{from, to})
	s.mu.Unlock()
	if s.reingest != nil {
		return s.reingest(ctx, client, from, to)
	}
	return 0, nil
}

// PageLimit satisfies the audit.Reingester interface; tests don't
// observe it directly.
func (s *stubReingest) PageLimit() uint { return 1000 }

// setup returns (auditor, mockRPC, mockStore, stubReingest) wired together.
func setup(t *testing.T, opts Options) (*Auditor, *mockRPC, *mockStore, *stubReingest) {
	t.Helper()
	if opts.PollInterval == 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	if opts.BatchLedgers == 0 {
		// Generous default so tests that seed ledgers 1..200 are actually
		// audited; tests that want to exercise the cap pass BatchLedgers
		// explicitly.
		opts.BatchLedgers = 1000
	}
	if opts.LagThreshold == 0 {
		opts.LagThreshold = 10
	}
	if opts.MaxRepairAttempts == 0 {
		opts.MaxRepairAttempts = 3
	}
	if opts.FindingMaxLedgers == 0 {
		opts.FindingMaxLedgers = 10
	}
	cli := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1_000, OldestLedger: 0},
	}
	st := newMockStore()
	r := &stubReingest{filters: []rpc.EventFilter{{Type: "contract"}}}
	a := New(cli, st, r, testLogger(), opts)
	return a, cli, st, r
}

// primeIngest teaches the mock store that ingest has reached `ledger`.
func primeIngest(st *mockStore, ledger int64) {
	st.SaveIngestionState(context.Background(), store.IngestionState{
		LastIngestedLedger: ledger,
	})
}

// TestPassOnce_CleanAdvance_HWM advances the audit high-water mark all the
// way through a range whose stored state matches the RPC.
func TestPassOnce_CleanAdvance_HWM(t *testing.T) {
	a, cli, st, _ := setup(t, Options{FindingMaxLedgers: 100})
	ctx := context.Background()

	const contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// Pre-seed 5 ledgers with one event each.
	for l := uint32(100); l <= 104; l++ {
		st.seedLedgers([]int{int(l)}, contract)
	}
	// The mock RPC returns exactly the events the store already has,
	// mirroring production reality.
	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		st.mu.Lock()
		var evs []rpc.Event
		for _, e := range st.events {
			evs = append(evs, rpc.Event{
				ID:         e.ID,
				ContractID: e.ContractID,
				Ledger:     uint32(e.Ledger),
				Type:       e.Type,
				TxHash:     "deadbeef",
			})
		}
		st.mu.Unlock()
		return rpc.GetEventsResponse{Events: evs, LatestLedger: 1_000}, nil
	}

	primeIngest(st, 104)

	_, err := a.PassOnce(ctx)
	require.NoError(t, err)

	state, err := st.GetAuditState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(104), state.VerifiedThroughLedger,
		"a clean audit pass advances HWM through the whole range")

	findings := len(st.findings)
	assert.Zero(t, findings, "no findings should be opened on a clean pass")
	m := a.Metrics()
	assert.GreaterOrEqual(t, m.LedgersChecked, uint64(5))
}

// TestPassOnce_DetectsMissingEvent_Repairs_AndVerifies seeds store with the
// right events for ledgers 100..102, but ledger 102 is missing in the store;
// the RPC has all three; after PassOnce the auditor repairs, advances HWM
// past the whole range, and marks the finding repaired.
func TestPassOnce_DetectsMissingEvent_Repairs_AndVerifies(t *testing.T) {
	a, cli, st, r := setup(t, Options{
		FindingMaxLedgers: 100,
		MaxRepairAttempts: 3,
	})
	ctx := context.Background()

	const contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// 100..104 — but we'll DELIBERATELY NOT seed ledger 102.
	for l := uint32(100); l <= 104; l++ {
		if l != 102 {
			st.seedLedgers([]int{int(l)}, contract)
		}
	}
	// The RPC returns all five events for every audit call. Repair
	// (ReingestRange) also returns them.
	primeIngest(st, 104)

	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		// Synthesize a 100..104 RPC response on the fly.
		var evs []rpc.Event
		for l := uint32(100); l <= 104; l++ {
			evs = append(evs, mkEvents(l, 1, contract)...)
		}
		return rpc.GetEventsResponse{Events: evs, LatestLedger: 1_000}, nil
	}
	r.reingest = func(ctx context.Context, client rpc.Client, from, to uint32) (int, error) {
		var evs []rpc.Event
		for l := from; l <= to; l++ {
			evs = append(evs, mkEvents(l, 1, contract)...)
		}
		// Decode using the stubDecoder logic from the audit's auditor is not exposed,
		// so we just call the store's ReplaceEventsInRange via a side-channel:
		// the stubReingest bypasses decoder; the auditor's toStoreEvent path
		// requires a real decoder; we sidestep that by writing raw rows.
		storeEvents := make([]store.Event, len(evs))
		for i, e := range evs {
			storeEvents[i] = store.Event{
				ID:         e.ID,
				ContractID: e.ContractID,
				Ledger:     int64(e.Ledger),
				Type:       e.Type,
			}
		}
		if err := st.ReplaceEventsInRange(ctx, storeEvents, int64(from), int64(to)); err != nil {
			return 0, err
		}
		return len(storeEvents), nil
	}

	worked, err := a.PassOnce(ctx)
	require.NoError(t, err)
	assert.True(t, worked)

	// The auditor should have opened then repaired exactly one finding.
	require.Len(t, st.findings, 1, "the auditor should open a finding for the missing ledger")
	f := st.findings[0]
	assert.Equal(t, int64(102), f.FromLedger, "cluster opens at the first dirty ledger")
	assert.Equal(t, int64(104), f.ToLedger)
	assert.Equal(t, store.FindingRepaired, f.Status, "finding should reach the repaired terminal state")
	assert.Empty(t, f.LastError)
	require.NotEmpty(t, f.MissingIDs)
	// 102 was missing; the missing id is the one we expect.
	assert.Contains(t, f.MissingIDs, "00000000000000000102-00000")

	// HWM should now be past the cluster.
	state, _ := st.GetAuditState(ctx)
	assert.Equal(t, int64(104), state.VerifiedThroughLedger, "post-repair HWM advanced to cluster end")

	// Sanity: events count.
	assert.Len(t, st.events, 5, "audit repair filled in the missing event")
}

// TestPassOnce_LagPaused does not advance HWM when ingest has not yet
// cleared LagThreshold ledgers past it.
func TestPassOnce_LagPaused(t *testing.T) {
	a, _, st, _ := setup(t, Options{LagThreshold: 50})
	ctx := context.Background()

	primeIngest(st, 100)
	st.SaveAuditState(ctx, store.AuditState{VerifiedThroughLedger: 90}) // lag = 10, < 50

	worked, err := a.PassOnce(ctx)
	require.NoError(t, err)
	assert.False(t, worked, "lag below threshold → audit does no work")

	state, _ := st.GetAuditState(ctx)
	assert.Equal(t, int64(90), state.VerifiedThroughLedger, "HWM unchanged on lag pause")
}

// TestPassOnce_FilterParity verifies the auditor does NOT flag events the
// RPC has for a contract we don't watch. The auditor goes through the
// same filter batches the ingester does, so events matching
// unwatched contracts are not in the audit's "RPC IDs" map.
func TestPassOnce_FilterParity(t *testing.T) {
	a, cli, st, r := setup(t, Options{FindingMaxLedgers: 100})
	ctx := context.Background()

	// Configure the auditor's filter set to a single contract — same as
	// if WATCHED_CONTRACTS had been set.
	r.filters = []rpc.EventFilter{{Type: "contract", ContractIDs: []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}}

	const watched = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// Pre-seed 100 with the watched event.
	st.seedLedgers([]int{100}, watched)
	primeIngest(st, 100)

	// RPC returns the watched event + an extra event for a different
	// contract that the auditor's filters exclude.
	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		return rpc.GetEventsResponse{
			Events: []rpc.Event{
				mkEvents(100, 1, watched)[0],
				{
					ID: "00000000000000000100-extra", Ledger: 100,
					ContractID: "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				},
			},
			LatestLedger: 1_000,
		}, nil
	}

	_, err := a.PassOnce(ctx)
	require.NoError(t, err)

	// No finding should be opened: the unwatched event was intentionally
	// not ingested.
	assert.Empty(t, st.findings, "watched-mode filter parity must hold")
	state, _ := st.GetAuditState(ctx)
	assert.Equal(t, int64(100), state.VerifiedThroughLedger, "HWM advances through the watched event")
}

// TestRepairFinding_RPCCap_Unrecoverable tests that a finding whose repair
// can't converge (the RPC keeps returning the wrong events) flips to
// "unrecoverable" after MaxRepairAttempts.
func TestRepairFinding_RPCCap_Unrecoverable(t *testing.T) {
	a, cli, st, r := setup(t, Options{
		MaxRepairAttempts: 2,
		FindingMaxLedgers: 10,
	})
	ctx := context.Background()

	// Set up a single ledger.
	primeIngest(st, 105)
	st.seedLedgers([]int{104}, "CA")

	// RPC keeps returning 0 events for ledger 104. Repair re-fetches but
	// repair never produces new rows. After MaxRepairAttempts the
	// finding flips to unrecoverable.
	var calls int
	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		calls++
		return rpc.GetEventsResponse{LatestLedger: 1_000}, nil
	}
	r.reingest = func(ctx context.Context, client rpc.Client, from, to uint32) (int, error) {
		// Repair can't find anything new; existing orphan stays put.
		return 0, nil
	}

	worked, err := a.PassOnce(ctx)
	require.NoError(t, err)
	assert.True(t, worked)

	require.Len(t, st.findings, 1)
	f := st.findings[0]
	assert.Equal(t, store.FindingUnrecoverable, f.Status, "after MaxRepairAttempts the finding is unrecoverable")
	assert.Equal(t, a.opts.MaxRepairAttempts, f.Attempts)
}

// TestPassOnce_BelowRetention_NoFalseFindings verifies the auditor does
// NOT open spurious findings when the entire audit window is below the
// RPC's oldest retained ledger — those ledgers have aged out of the
// RPC, so auditing them would always flag every stored row as orphan.
func TestPassOnce_BelowRetention_NoFalseFindings(t *testing.T) {
	a, cli, st, _ := setup(t, Options{FindingMaxLedgers: 50})
	ctx := context.Background()

	primeIngest(st, 100)
	cli.health = rpc.Health{Status: "healthy", LatestLedger: 1_000, OldestLedger: 500}
	st.seedLedgers([]int{100}, "CA")

	worked, err := a.PassOnce(ctx)
	require.NoError(t, err)
	assert.False(t, worked, "audit window below retention → no work")
	assert.Empty(t, st.findings, "no spurious findings for ledger below retention")
}

// TestAuditStatePersistence ensures HWM persists across PassOnce calls and
// is not regressed by a partial pass.
func TestAuditStatePersistence(t *testing.T) {
	a, cli, st, _ := setup(t, Options{BatchLedgers: 5, LagThreshold: 1, FindingMaxLedgers: 5})
	ctx := context.Background()

	primeIngest(st, 50)
	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		// Mirror whatever is stored; auditor walks ledgers 1..5 and we'd
		// loop externally to walk further. For this test we just want HWM
		// to advance and persist.
		st.mu.Lock()
		var evs []rpc.Event
		for _, e := range st.events {
			evs = append(evs, rpc.Event{
				ID:         e.ID,
				Ledger:     uint32(e.Ledger),
				ContractID: e.ContractID,
				Type:       e.Type,
			})
		}
		st.mu.Unlock()
		return rpc.GetEventsResponse{Events: evs, LatestLedger: 1_000}, nil
	}

	// Seed the first 5 ledgers.
	for l := int64(1); l <= 5; l++ {
		st.seedLedgers([]int{int(l)}, "CA")
	}
	_, err := a.PassOnce(ctx)
	require.NoError(t, err)
	state1, _ := st.GetAuditState(ctx)
	assert.EqualValues(t, 5, state1.VerifiedThroughLedger)

	_, err = a.PassOnce(ctx)
	require.NoError(t, err)
	state2, _ := st.GetAuditState(ctx)
	// Second pass should not regress even though BatchLedgers caps to 5
	// ledgers per pass.
	assert.GreaterOrEqual(t, state2.VerifiedThroughLedger, state1.VerifiedThroughLedger)
}

// TestPassOnce_PartialAgingMidPass verifies that when fetchRange's later
// pages start returning IsLedgerOutOfRange mid-pass — the RPC dropped
// part of the range during the audit — the auditor surfaces a
// FindingUnverifiable rather than crashing or false-alarming.
// Setup: primeIngest=100 places the auditor inside the RPC retention;
// the store has an event at ledger 95 that the RPC no longer reports.
// The first fetchRange call returns IsLedgerOutOfRange (the RPC
// already forgot the range, simulating mid-pass aging), and the repair
// attempt also returns IsLedgerOutOfRange. The finding should reach
// FindingUnverifiable terminal status.
func TestPassOnce_PartialAgingMidPass(t *testing.T) {
	a, cli, st, r := setup(t, Options{FindingMaxLedgers: 50, MaxRepairAttempts: 2})
	ctx := context.Background()

	primeIngest(st, 100)
	cli.health = rpc.Health{Status: "healthy", LatestLedger: 200, OldestLedger: 0}
	st.SaveAuditState(ctx, store.AuditState{VerifiedThroughLedger: 90})
	st.seedLedgers([]int{95}, "CA")

	// All RPC calls return IsLedgerOutOfRange (the range has fully
	// aged out from the RPC's perspective). A production auditor would
	// see this on the second page; we model it as the first response
	// to keep the test deterministic.
	cli.extraResponses = func(callIdx int) (rpc.GetEventsResponse, error) {
		return rpc.GetEventsResponse{}, &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}
	}
	r.reingest = func(ctx context.Context, client rpc.Client, from, to uint32) (int, error) {
		return 0, &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}
	}

	_, err := a.PassOnce(ctx)
	require.NoError(t, err, "partial aging must not error out the audit pass")

	require.NotEmpty(t, st.findings, "an orphan range that aged out of RPC retention opens a finding")
	found := false
	for _, f := range st.findings {
		if f.Status == store.FindingUnverifiable {
			found = true
			break
		}
	}
	assert.True(t, found, "the finding ends in FindingUnverifiable when the range ages out of RPC retention mid-pass")
}

// TestSaveAuditStateIfGreater_RaceConditionFree is the spec's canonical
// stress for the HWM race: many goroutines call SaveAuditStateIfGreater
// concurrently with non-contiguous values, and the final HWM must equal
// the largest value that was passed in — never any other.
func TestSaveAuditStateIfGreater_RaceConditionFree(t *testing.T) {
	st := newMockStore()
	// Non-contiguous values, both smaller and larger, to ensure the
	// update path isn't biased by sequence.
	candidates := []int64{42, 99, 7, 123, 50, 13, 200, 75, 1, 88}
	var wg sync.WaitGroup
	for _, n := range candidates {
		wg.Add(1)
		go func(ledger int64) {
			defer wg.Done()
			_, err := st.SaveAuditStateIfGreater(context.Background(), ledger)
			require.NoError(t, err)
		}(n)
	}
	wg.Wait()
	final, err := st.GetAuditState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(200), final.VerifiedThroughLedger,
		"concurrent SaveAuditStateIfGreater must converge on max(candidates)")
}
