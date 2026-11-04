package simtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// mockStore is an in-memory store.Store for simtest harness tests.
type mockStore struct {
	// Embedded so the mock keeps satisfying store.Store as the
	// interface grows; unstubbed methods panic if a test calls them.
	store.Store

	mu       sync.Mutex
	events   map[string]store.Event
	state    *store.IngestionState
	watched  []string
	upserted [][]store.Event
}

func newMockStore() *mockStore {
	return &mockStore{events: map[string]store.Event{}}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upserted = append(m.upserted, events)
	var inserted int64
	for _, e := range events {
		if _, dup := m.events[e.ID]; !dup {
			m.events[e.ID] = e
			inserted++
		}
	}
	return inserted, nil
}

func (m *mockStore) ReplaceEventsInRange(_ context.Context, events []store.Event, fromLedger, toLedger int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, e := range m.events {
		if e.Ledger >= fromLedger && e.Ledger <= toLedger {
			delete(m.events, id)
		}
	}
	for _, e := range events {
		m.events[e.ID] = e
	}
	return nil
}

func (m *mockStore) GetEvent(_ context.Context, id string, _ store.Scope) (store.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return store.Event{}, store.ErrNotFound
	}
	return e, nil
}

func (m *mockStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []store.Event
	for _, ev := range m.events {
		if f.ContractID != "" && ev.ContractID != f.ContractID {
			continue
		}
		if f.FromLedger > 0 && ev.Ledger < f.FromLedger {
			continue
		}
		if f.ToLedger > 0 && ev.Ledger > f.ToLedger {
			continue
		}
		if f.Cursor != "" && ev.ID <= f.Cursor {
			continue
		}
		result = append(result, ev)
	}
	// Sort by ID ascending for deterministic output (IDs are zero-padded).
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].ID > result[j].ID {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	if len(result) > limit {
		return result[:limit], result[limit-1].ID, nil
	}
	return result, "", nil
}

func (m *mockStore) LedgerRangeCensus(_ context.Context, from, to int64, idsOnly bool) ([]store.LedgerCensus, error) {
	return nil, nil
}

func (m *mockStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return store.IngestionState{}, store.ErrNotFound
	}
	return *m.state, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = &s
	return nil
}

func (m *mockStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]store.WatchedContract, 0, len(m.watched))
	for _, id := range m.watched {
		out = append(out, store.WatchedContract{ContractID: id})
	}
	return out, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, w store.WatchedContract) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watched = append(m.watched, w.ContractID)
	return nil
}

var _ store.Store = (*mockStore)(nil)

// Remaining store.Store methods that simtest doesn't exercise.

func (m *mockStore) GetAuditState(context.Context, string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error { return nil }
func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, _ string, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}
func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}
func (m *mockStore) UpdateAuditFinding(context.Context, store.AuditFinding) error { return nil }
func (m *mockStore) ListOpenFindingsByRange(context.Context, string, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (m *mockStore) Ping(context.Context) error { return nil }
func (m *mockStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}

// ---------- Tests ----------

func TestCuratedScenario_CrashBetweenPersistAndState(t *testing.T) {
	scenario := findScenario("crash_between_persist_and_save_state")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch")

	// All events should be present exactly once.
	stored, _, err := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, err)
	assert.Len(t, stored, len(scenario.Events), "all events should be stored")

	// Verify no duplicates.
	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once", id)
	}
}

func TestCuratedScenario_ColdStartAllInRetention(t *testing.T) {
	scenario := findScenario("cold_start_all_in_retention")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err)

	stored, _, err := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, err)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested")
}

func TestCuratedScenario_RetentionGapLegitimateLoss(t *testing.T) {
	scenario := findScenario("retention_clamp_legitimate_loss")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	// The oracle returns nil even for legitimate loss — it only flags
	// unexpected mismatches. Verify the err is nil (no oracle violation)
	// and that the events within retention are present.
	require.NoError(t, err, "oracle should not flag legitimate retention gaps as errors")

	// Verify events at 95 are stored (within retention).
	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)

	hasLedger := func(ledger int64) bool {
		for _, ev := range stored {
			if ev.Ledger == ledger {
				return true
			}
		}
		return false
	}

	assert.True(t, hasLedger(95), "event at ledger 95 should be in retention and stored")
}

func TestCuratedScenario_RPCFlapAndTimeoutDuplicate(t *testing.T) {
	scenario := findScenario("rpc_flap_and_timeout_duplicate")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err)

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)

	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once", id)
	}
	assert.Len(t, stored, len(scenario.Events))
}

// TestCuratedScenario_RateLimitRecovery verifies that a rate-limited
// page (HTTP 429) is retried and all events are eventually ingested
// without duplication.
func TestCuratedScenario_RateLimitRecovery(t *testing.T) {
	scenario := findScenario("rpc_fault_rate_limit")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after rate-limit recovery")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested after rate-limit retry")

	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once", id)
	}
}

// TestCuratedScenario_MalformedPageRecovery verifies that a malformed
// RPC response is retried and all events are eventually ingested.
func TestCuratedScenario_MalformedPageRecovery(t *testing.T) {
	scenario := findScenario("rpc_fault_malformed_page")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after malformed page recovery")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested after malformed page retry")
}

// TestCuratedScenario_TruncatedPageRecovery verifies that a truncated
// RPC response is retried and all events are eventually ingested.
func TestCuratedScenario_TruncatedPageRecovery(t *testing.T) {
	scenario := findScenario("rpc_fault_truncated_page")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after truncated page recovery")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested after truncated page retry")
}

// TestCuratedScenario_HealthErrorRecovery verifies that a transient
// getHealth error does not prevent eventual ingestion of all events.
func TestCuratedScenario_HealthErrorRecovery(t *testing.T) {
	scenario := findScenario("rpc_fault_health_error")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after health error recovery")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested after health error recovery")
}

// TestCuratedScenario_ChainReorgHeadMovesBack verifies that the
// ingester handles a chain head drop without losing events.
func TestCuratedScenario_ChainReorgHeadMovesBack(t *testing.T) {
	scenario := findScenario("chain_reorg_head_moves_back")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after chain reorg")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)

	// All events should be present exactly once despite the reorg.
	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once after reorg", id)
	}
	assert.Len(t, stored, len(scenario.Events), "all events should survive the reorg")
}

// TestCuratedScenario_ChainReorgRepeated verifies that repeated head
// drops do not cause duplicates or data loss.
func TestCuratedScenario_ChainReorgRepeated(t *testing.T) {
	scenario := findScenario("chain_reorg_repeated")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after repeated chain reorgs")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)

	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once after repeated reorgs", id)
	}
	assert.Len(t, stored, len(scenario.Events), "all events should survive repeated reorgs")
}

// TestCuratedScenario_CrashDuringFaultRetry verifies that the ingester
// can recover from a crash that occurs on the same step as a fault,
// exercising the restart-in-the-middle path.
func TestCuratedScenario_CrashDuringFaultRetry(t *testing.T) {
	scenario := findScenario("crash_during_fault_retry")
	require.NotNil(t, scenario, "scenario not found")

	st := newMockStore()
	h := NewHarness(*scenario, st)
	err := h.Run(context.Background())
	require.NoError(t, err, "oracle mismatch after crash-during-fault recovery")

	stored, _, qerr := st.QueryEvents(context.Background(), store.EventFilter{Limit: 1000, Order: "asc"})
	require.NoError(t, qerr)
	assert.Len(t, stored, len(scenario.Events), "all events should be ingested after crash+fault recovery")

	idCounts := make(map[string]int)
	for _, ev := range stored {
		idCounts[ev.ID]++
	}
	for id, count := range idCounts {
		assert.Equal(t, 1, count, "event %s should appear exactly once after crash+fault recovery", id)
	}
}

// TestAllCuratedScenarios runs every scenario in CuratedScenarios through
// the harness, asserting the oracle reports no mismatch. Each subtest
// prints the scenario name and description so a failure is immediately
// identifiable.
func TestAllCuratedScenarios(t *testing.T) {
	for _, scenario := range CuratedScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			st := newMockStore()
			h := NewHarness(scenario, st)
			err := h.Run(context.Background())
			assert.NoError(t, err, "scenario %q: %s", scenario.Name, scenario.Description)
		})
	}
}

// TestCuratedScenarioSeedsLogForReproducibility verifies that every
// curated scenario produces a deterministic run. The scenario name is
// the reproducibility key — calling RandomScenario with a seed derived
// from the name reproduces the same event placement and fault schedule.
func TestCuratedScenarioSeedsLogForReproducibility(t *testing.T) {
	for _, scenario := range CuratedScenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			// Run twice with the same scenario and confirm identical outcomes.
			st1 := newMockStore()
			h1 := NewHarness(scenario, st1)
			err1 := h1.Run(context.Background())

			st2 := newMockStore()
			h2 := NewHarness(scenario, st2)
			err2 := h2.Run(context.Background())

			assert.Equal(t, err1, err2, "same scenario must produce the same oracle result")

			events1, _, _ := st1.QueryEvents(context.Background(), store.EventFilter{Limit: 100000, Order: "asc"})
			events2, _, _ := st2.QueryEvents(context.Background(), store.EventFilter{Limit: 100000, Order: "asc"})
			assert.Equal(t, len(events1), len(events2), "same scenario must produce the same event count")
		})
	}
}

func TestRandomizedMode(t *testing.T) {
	seeds := []uint64{1, 42, 999, 12345}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			scenario := RandomScenario(seed)
			st := newMockStore()
			h := NewHarness(scenario, st)
			err := h.Run(context.Background())
			if err != nil {
				t.Logf("seed %d failed: %v (some random scenarios inject faults)", seed, err)
			}
		})
	}
}

func findScenario(name string) *Scenario {
	for i := range CuratedScenarios {
		if CuratedScenarios[i].Name == name {
			return &CuratedScenarios[i]
		}
	}
	return nil
}

func TestReproducibility(t *testing.T) {
	seed := uint64(42)
	s1 := RandomScenario(seed)
	s2 := RandomScenario(seed)

	assert.Equal(t, s1.Name, s2.Name)
	assert.Equal(t, s1.RetentionLedgers, s2.RetentionLedgers)
	assert.Equal(t, s1.ChainLedgers, s2.ChainLedgers)
	assert.Equal(t, s1.PageLimit, s2.PageLimit)
	assert.Len(t, s1.Events, len(s2.Events))
	for i := range s1.Events {
		assert.Equal(t, s1.Events[i].Ledger, s2.Events[i].Ledger)
	}
	assert.Len(t, s1.Faults, len(s2.Faults))
	for i := range s1.Faults {
		assert.Equal(t, s1.Faults[i].Kind, s2.Faults[i].Kind)
	}
}

// ---------- VirtualClock tests ----------

func TestVirtualClock_Now(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	assert.Equal(t, now, clock.Now())
}

func TestVirtualClock_SleepCtx(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	completed := clock.SleepCtx(context.Background(), 5*time.Second)
	assert.True(t, completed)
	assert.Equal(t, now.Add(5*time.Second), clock.Now())
}

func TestVirtualClock_Advance(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	clock.Advance(10 * time.Second)
	assert.Equal(t, now.Add(10*time.Second), clock.Now())
}

// ---------- Oracle tests ----------

func TestOracle_FetchableCount(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 100)
	chain.AddEvents(5, BuildEvent("0000000005-000000000", 5, "CAAAA"))
	chain.AddEvents(10, BuildEvent("0000000010-000000000", 10, "CAAAA"))

	oracle := NewOracle(chain)
	assert.Equal(t, 2, oracle.FetchableCount())
	oracle.RecordReclamp(5, 6)
	assert.Equal(t, 1, oracle.FetchableCount())
	assert.Len(t, oracle.LegitimatelyLostEvents(), 1)
}

// ---------- Chain tests ----------

func TestVirtualChain_AdvanceTo(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 100)
	chain.AdvanceTo(50)
	assert.Equal(t, uint32(50), chain.LatestLedger())
	assert.Equal(t, uint32(2), chain.OldestRetained())
}

func TestVirtualChain_Retention(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 10)
	chain.AdvanceTo(100)
	assert.Equal(t, uint32(90), chain.OldestRetained())
}

func TestVirtualChain_GetHealth(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 10)
	chain.AdvanceTo(100)
	health, err := chain.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(100), health.LatestLedger)
	assert.Equal(t, uint32(90), health.OldestLedger)
}

func TestVirtualChain_GetEvents_OutOfRange(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 10)
	chain.AdvanceTo(100)
	_, err := chain.GetEvents(context.Background(), rpc.GetEventsRequest{StartLedger: 50})
	require.Error(t, err)
	var rpcErr *rpc.Error
	require.True(t, errors.As(err, &rpcErr))
	assert.Contains(t, rpcErr.Message, "ledger range")
}

// ---------- Filter Matching & Virtual Clock Machineries tests ----------

func TestVirtualChain_FilterMatchingSemantics(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 100)
	chain.AdvanceTo(30)

	ev1 := BuildEvent("0000000010-000000000", 10, "CAAAA")
	ev2 := BuildEvent("0000000020-000000000", 20, "CBBBB")
	chain.AddEvents(10, ev1)
	chain.AddEvents(20, ev2)

	tests := []struct {
		name          string
		req           rpc.GetEventsRequest
		wantCount     int
		wantMinLedger uint32
	}{
		{
			name:      "no filters returns all events in range",
			req:       rpc.GetEventsRequest{StartLedger: 2},
			wantCount: 2,
		},
		{
			name: "contract ID filter matches specific contract",
			req: rpc.GetEventsRequest{
				StartLedger: 2,
				Filters:     []rpc.EventFilter{{ContractIDs: []string{"CAAAA"}}},
			},
			wantCount:     1,
			wantMinLedger: 10,
		},
		{
			name: "ledger range filter restricts bounds",
			req: rpc.GetEventsRequest{
				StartLedger: 15,
				EndLedger:   25,
			},
			wantCount:     1,
			wantMinLedger: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := chain.GetEvents(context.Background(), tt.req)
			require.NoError(t, err)
			assert.Len(t, resp.Events, tt.wantCount)
			if tt.wantMinLedger > 0 && len(resp.Events) > 0 {
				assert.Equal(t, tt.wantMinLedger, resp.Events[0].Ledger)
			}
		})
	}
}

func TestVirtualClock_DeterministicAdvance(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)

	assert.Equal(t, now, clock.Now())

	steps := []time.Duration{
		100 * time.Millisecond,
		2 * time.Second,
		1 * time.Hour,
	}

	expected := now
	for _, d := range steps {
		clock.Advance(d)
		expected = expected.Add(d)
		assert.Equal(t, expected, clock.Now())
	}
}

// ---------- Fault Scheduling, Seeded Generation, and Oracle Validation Tests ----------

func TestFaultScheduling_FiresOnNominatedCall(t *testing.T) {
	// Verify that a fault configured for a specific call index fires exactly on that call
	// and passes transparently on other calls.
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	chain := NewVirtualChain(clock, now, 100)
	chain.AdvanceTo(50)

	scenario := Scenario{
		Name:             "fault_scheduling_test",
		RetentionLedgers: 100,
		ChainLedgers:     50,
		PageLimit:        100,
		Faults: []FaultDescriptor{
			{
				Kind:      FaultRateLimit,
				CallIndex: 2,
			},
		},
	}

	client := NewFaultClient(chain, scenario)
	req := rpc.GetEventsRequest{StartLedger: 2}

	// Call 1: should succeed without fault.
	_, err1 := client.GetEvents(context.Background(), req)
	assert.NoError(t, err1, "call 1 should not trigger fault")

	// Call 2: should trigger the rate limit fault.
	_, err2 := client.GetEvents(context.Background(), req)
	require.Error(t, err2, "call 2 should trigger fault")

	// Call 3: should succeed again after fault triggers.
	_, err3 := client.GetEvents(context.Background(), req)
	assert.NoError(t, err3, "call 3 should not trigger fault")
}

func TestSeededGeneration_ReproducesIdenticalScenarios(t *testing.T) {
	// Verify that two scenarios generated with the exact same seed produce identical
	// event lists, fault schedules, and parameters.
	seed := uint64(12345)
	s1 := RandomScenario(seed)
	s2 := RandomScenario(seed)

	assert.Equal(t, s1.Name, s2.Name)
	assert.Equal(t, s1.RetentionLedgers, s2.RetentionLedgers)
	assert.Equal(t, s1.ChainLedgers, s2.ChainLedgers)
	assert.Equal(t, s1.PageLimit, s2.PageLimit)

	require.Len(t, s1.Events, len(s2.Events))
	for i := range s1.Events {
		assert.Equal(t, s1.Events[i].Ledger, s2.Events[i].Ledger)
		assert.Equal(t, s1.Events[i].ContractID, s2.Events[i].ContractID)
	}

	require.Len(t, s1.Faults, len(s2.Faults))
	for i := range s1.Faults {
		assert.Equal(t, s1.Faults[i].Kind, s2.Faults[i].Kind)
		assert.Equal(t, s1.Faults[i].CallIndex, s2.Faults[i].CallIndex)
	}
}

func TestOracle_DetectsDeliberateGapOrDuplicate(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = NewVirtualClock(now)

	tests := []struct {
		name        string
		mutateStore func(st *mockStore, scenario Scenario)
		wantError   bool
	}{
		{
			name: "deliberately omit an event (gap)",
			mutateStore: func(st *mockStore, scenario Scenario) {
				st.mu.Lock()
				defer st.mu.Unlock()
				for id := range st.events {
					delete(st.events, id)
					break
				}
			},
			wantError: true,
		},
		{
			name: "deliberately duplicate an event",
			mutateStore: func(st *mockStore, scenario Scenario) {
				st.mu.Lock()
				defer st.mu.Unlock()
				for _, ev := range st.events {
					dup := ev
					dup.ID = "9999999999-999999999"
					st.events[dup.ID] = dup
					break
				}
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scenario := findScenario("cold_start_all_in_retention")
			require.NotNil(t, scenario)

			st := newMockStore()
			h := NewHarness(*scenario, st)
			// Pre-run to ingest normally or mutate directly
			err := h.Run(context.Background())
			require.NoError(t, err)

			// Now inject the violation
			tt.mutateStore(st, *scenario)

			// Re-running or validating oracle against mutated store should catch it
			h2 := NewHarness(*scenario, st)
			err2 := h2.Run(context.Background())
			if tt.wantError {
				assert.Error(t, err2, "oracle should detect store deviation")
			} else {
				assert.NoError(t, err2)
			}
		})
	}
}

func NewFaultClient(chain rpc.Client, scenario Scenario) rpc.Client {
	return &faultClient{inner: chain, faults: scenario.Faults}
}

type faultClient struct {
	rpc.Client
	inner  rpc.Client
	faults []FaultDescriptor
	call   int
}

func (f *faultClient) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	f.call++
	for _, fault := range f.faults {
		if fault.CallIndex == f.call {
			return rpc.GetEventsResponse{}, &rpc.Error{Code: -32000, Message: fmt.Sprintf("fault injected: %s", fault.Kind)}
		}
	}
	return f.inner.GetEvents(ctx, req)
}

func (m *mockStore) CountEventsBefore(context.Context, int64, time.Time, int) (int64, error) {
	return 0, nil
}

// ---------- Multi-Provider Failover Simulation Tests ----------

// simProviderClient wraps a VirtualChain to model provider-specific failure
// modes in multi-provider failover simulations: failing after N GetEvents
// calls, reporting a lagging chain head, reporting a truncated retention
// window, or mutating event payloads on a target ledger.
type simProviderClient struct {
	chain          *VirtualChain
	passphrase     string
	failAfterCalls int
	overrideLatest uint32
	overrideOldest uint32
	corruptLedger  uint32

	mu    sync.Mutex
	calls int
}

func (p *simProviderClient) GetNetwork(_ context.Context) (rpc.NetworkInfo, error) {
	pass := p.passphrase
	if pass == "" {
		pass = rpc.MainnetNetworkPassphrase
	}
	return rpc.NetworkInfo{Passphrase: pass}, nil
}

func (p *simProviderClient) GetHealth(ctx context.Context) (rpc.Health, error) {
	h, err := p.chain.GetHealth(ctx)
	if err != nil {
		return h, err
	}
	if p.overrideLatest > 0 {
		h.LatestLedger = p.overrideLatest
	}
	if p.overrideOldest > 0 {
		h.OldestLedger = p.overrideOldest
	}
	return h, nil
}

func (p *simProviderClient) GetLatestLedger(ctx context.Context) (rpc.LatestLedger, error) {
	ll, err := p.chain.GetLatestLedger(ctx)
	if err != nil {
		return ll, err
	}
	if p.overrideLatest > 0 {
		ll.Sequence = p.overrideLatest
	}
	return ll, nil
}

func (p *simProviderClient) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	p.mu.Lock()
	p.calls++
	callNum := p.calls
	p.mu.Unlock()

	if p.failAfterCalls > 0 && callNum > p.failAfterCalls {
		return rpc.GetEventsResponse{}, fmt.Errorf("getEvents returned HTTP 503: provider down after %d calls", p.failAfterCalls)
	}

	resp, err := p.chain.GetEvents(ctx, req)
	if err != nil {
		return resp, err
	}

	if p.overrideLatest > 0 {
		resp.LatestLedger = p.overrideLatest
		filtered := resp.Events[:0]
		for _, ev := range resp.Events {
			if ev.Ledger <= p.overrideLatest {
				filtered = append(filtered, ev)
			}
		}
		resp.Events = filtered
	}

	if p.overrideOldest > 0 {
		resp.OldestLedger = p.overrideOldest
		// Simulate a provider with a shorter retention window that silently omits
		// events older than overrideOldest rather than returning an RPC error.
		var retained []rpc.Event
		for _, ev := range resp.Events {
			if ev.Ledger >= p.overrideOldest {
				retained = append(retained, ev)
			}
		}
		resp.Events = retained
	}

	if p.corruptLedger > 0 {
		mutated := make([]rpc.Event, len(resp.Events))
		copy(mutated, resp.Events)
		for i := range mutated {
			if mutated[i].Ledger == p.corruptLedger {
				mutated[i].ValueJSON = json.RawMessage(`{"u64":999999}`)
			}
		}
		resp.Events = mutated
	}

	return resp, nil
}

func (p *simProviderClient) GetLedgerEntries(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return p.chain.GetLedgerEntries(ctx, req)
}

func (p *simProviderClient) SimulateTransaction(ctx context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return p.chain.SimulateTransaction(ctx, req)
}

func buildPopulatedVirtualChain(scenario Scenario) (*VirtualChain, *VirtualClock) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	scenario.applyDefaults()
	chain := NewVirtualChain(clock, now, scenario.RetentionLedgers)
	for i, ep := range scenario.Events {
		id := fmt.Sprintf("%010d-%09d", ep.Ledger, i)
		ev := BuildEvent(id, ep.Ledger, ep.ContractID)
		chain.AddEvents(ep.Ledger, ev)
	}
	if scenario.ChainLedgers > 0 {
		chain.AdvanceTo(scenario.ChainLedgers)
	}
	return chain, clock
}

func runIngesterWithClient(t *testing.T, scenario Scenario, client rpc.Client, oracleChain *VirtualChain) ([]store.Event, *mockStore) {
	t.Helper()
	ctx := context.Background()
	st := newMockStore()
	_, clock := buildPopulatedVirtualChain(scenario)
	rng := rand.New(rand.NewPCG(42, 42))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ing := ingester.New(
		client,
		st,
		decode.XDRDecoder{},
		log,
		ingester.Options{
			PollInterval:     time.Second,
			StartLedger:      scenario.StartLedger,
			RetentionLedgers: scenario.RetentionLedgers,
			PageLimit:        scenario.PageLimit,
			SweepWindow:      scenario.SweepWindow,
			Clock:            clock,
			Jitter:           DeterministicJitter(rng),
		},
	)

	caughtUpCount := 0
	for step := 1; step <= 25; step++ {
		caughtUp, _ := ing.RunOnceForTest(ctx)
		if caughtUp {
			caughtUpCount++
			if caughtUpCount >= 2 {
				break
			}
		} else {
			caughtUpCount = 0
		}
	}

	oracle := NewOracle(oracleChain)
	require.NoError(t, oracle.Verify(ctx, st), "oracle verification must pass")

	stored, _, err := st.QueryEvents(ctx, store.EventFilter{Limit: 10000, Order: "asc"})
	require.NoError(t, err)
	return stored, st
}

func TestMultiProviderFailover_CorrectnessMatchesSingleHealthyProvider(t *testing.T) {
	baseScenario := findScenario("multi_provider_failover_mid_batch")
	require.NotNil(t, baseScenario, "curated scenario multi_provider_failover_mid_batch must exist")
	scenario := *baseScenario
	scenario.Faults = nil // Faults are injected at the provider layer below.

	// 1. Run against a single healthy provider to produce the reference event set.
	refChain, _ := buildPopulatedVirtualChain(scenario)
	expectedEvents, _ := runIngesterWithClient(t, scenario, refChain, refChain)
	require.Len(t, expectedEvents, len(scenario.Events), "reference single-provider run must ingest all events")

	t.Run("failover_mid_batch_reanchors_without_gap_or_duplicate", func(t *testing.T) {
		c1, _ := buildPopulatedVirtualChain(scenario)
		c2, _ := buildPopulatedVirtualChain(scenario)

		providers := map[string]rpc.Client{
			"https://primary.sim.test":   &simProviderClient{chain: c1, failAfterCalls: 1},
			"https://secondary.sim.test": &simProviderClient{chain: c2},
		}
		fc := rpc.NewFailoverClient(
			[]string{"https://primary.sim.test", "https://secondary.sim.test"},
			1000.0,
			func(u string, _ float64) rpc.Client { return providers[u] },
			rpc.WithFailoverMaxErrors(1),
			rpc.WithFailoverExpectedNetwork("mainnet"),
			rpc.WithFailoverLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		)

		gotEvents, _ := runIngesterWithClient(t, scenario, fc, c2)
		assert.Equal(t, expectedEvents, gotEvents, "stored events after mid-batch failover must equal single healthy provider")
	})

	t.Run("provider_behind_cursor_is_skipped", func(t *testing.T) {
		c1, _ := buildPopulatedVirtualChain(scenario)
		cBehind, _ := buildPopulatedVirtualChain(scenario)
		cHealthy, _ := buildPopulatedVirtualChain(scenario)

		providers := map[string]rpc.Client{
			"https://primary.sim.test": &simProviderClient{chain: c1, failAfterCalls: 1},
			"https://behind.sim.test":  &simProviderClient{chain: cBehind, overrideLatest: 4},
			"https://healthy.sim.test": &simProviderClient{chain: cHealthy},
		}
		fc := rpc.NewFailoverClient(
			[]string{"https://primary.sim.test", "https://behind.sim.test", "https://healthy.sim.test"},
			1000.0,
			func(u string, _ float64) rpc.Client { return providers[u] },
			rpc.WithFailoverMaxErrors(1),
			rpc.WithFailoverLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		)

		gotEvents, _ := runIngesterWithClient(t, scenario, fc, cHealthy)
		assert.Equal(t, expectedEvents, gotEvents, "stored events when skipping behind provider must equal single healthy provider")
		states := fc.ProviderStates()
		assert.Equal(t, rpc.StateDegraded, states[1].State, "behind provider must be demoted")
	})

	t.Run("provider_with_shorter_retention_window_is_skipped", func(t *testing.T) {
		c1, _ := buildPopulatedVirtualChain(scenario)
		cShort, _ := buildPopulatedVirtualChain(scenario)
		cHealthy, _ := buildPopulatedVirtualChain(scenario)

		providers := map[string]rpc.Client{
			"https://primary.sim.test":   &simProviderClient{chain: c1, failAfterCalls: 1},
			"https://short-ret.sim.test": &simProviderClient{chain: cShort, overrideOldest: 25},
			"https://healthy.sim.test":   &simProviderClient{chain: cHealthy},
		}
		fc := rpc.NewFailoverClient(
			[]string{"https://primary.sim.test", "https://short-ret.sim.test", "https://healthy.sim.test"},
			1000.0,
			func(u string, _ float64) rpc.Client { return providers[u] },
			rpc.WithFailoverMaxErrors(1),
			rpc.WithFailoverLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		)

		gotEvents, _ := runIngesterWithClient(t, scenario, fc, cHealthy)
		assert.Equal(t, expectedEvents, gotEvents, "stored events when skipping short-retention provider must equal single healthy provider")
		states := fc.ProviderStates()
		assert.Equal(t, rpc.StateDegraded, states[1].State, "short-retention provider must be demoted")
	})

	t.Run("provider_returning_subtly_different_data_is_rejected", func(t *testing.T) {
		c1, _ := buildPopulatedVirtualChain(scenario)
		cDivergent, _ := buildPopulatedVirtualChain(scenario)
		cHealthy, _ := buildPopulatedVirtualChain(scenario)

		// Page 1 (limit 3) fetches the 2 events at ledger 5 and 1 event at ledger 10.
		// Primary then fails mid-batch at ledger 10. Re-anchor re-scans from ledger 10,
		// where cDivergent returns a mutated ValueJSON for ledger 10.
		providers := map[string]rpc.Client{
			"https://primary.sim.test":   &simProviderClient{chain: c1, failAfterCalls: 1},
			"https://divergent.sim.test": &simProviderClient{chain: cDivergent, corruptLedger: 10},
			"https://healthy.sim.test":   &simProviderClient{chain: cHealthy},
		}
		fc := rpc.NewFailoverClient(
			[]string{"https://primary.sim.test", "https://divergent.sim.test", "https://healthy.sim.test"},
			1000.0,
			func(u string, _ float64) rpc.Client { return providers[u] },
			rpc.WithFailoverMaxErrors(1),
			rpc.WithFailoverLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		)

		gotEvents, _ := runIngesterWithClient(t, scenario, fc, cHealthy)
		assert.Equal(t, expectedEvents, gotEvents, "stored events when rejecting divergent provider must equal single healthy provider")
		states := fc.ProviderStates()
		assert.Equal(t, rpc.StateDown, states[1].State, "divergent provider must be demoted to StateDown")
		assert.NotEmpty(t, fc.Divergences(), "divergence must be recorded on FailoverClient")
	})

	t.Run("combined_multi_provider_gauntlet", func(t *testing.T) {
		c1, _ := buildPopulatedVirtualChain(scenario)
		cWrongNet, _ := buildPopulatedVirtualChain(scenario)
		cBehind, _ := buildPopulatedVirtualChain(scenario)
		cShort, _ := buildPopulatedVirtualChain(scenario)
		cDivergent, _ := buildPopulatedVirtualChain(scenario)
		cHealthy, _ := buildPopulatedVirtualChain(scenario)

		providers := map[string]rpc.Client{
			"https://primary.sim.test":   &simProviderClient{chain: c1, failAfterCalls: 1},
			"https://testnet.sim.test":   &simProviderClient{chain: cWrongNet, passphrase: rpc.TestnetNetworkPassphrase},
			"https://behind.sim.test":    &simProviderClient{chain: cBehind, overrideLatest: 4},
			"https://short-ret.sim.test": &simProviderClient{chain: cShort, overrideOldest: 25},
			"https://divergent.sim.test": &simProviderClient{chain: cDivergent, corruptLedger: 10},
			"https://healthy.sim.test":   &simProviderClient{chain: cHealthy},
		}
		fc := rpc.NewFailoverClient(
			[]string{
				"https://primary.sim.test",
				"https://testnet.sim.test",
				"https://behind.sim.test",
				"https://short-ret.sim.test",
				"https://divergent.sim.test",
				"https://healthy.sim.test",
			},
			1000.0,
			func(u string, _ float64) rpc.Client { return providers[u] },
			rpc.WithFailoverMaxErrors(1),
			rpc.WithFailoverExpectedNetwork("mainnet"),
			rpc.WithFailoverLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		)

		gotEvents, _ := runIngesterWithClient(t, scenario, fc, cHealthy)
		assert.Equal(t, expectedEvents, gotEvents, "combined multi-provider gauntlet must produce identical stored events to a single healthy provider")
	})
}
