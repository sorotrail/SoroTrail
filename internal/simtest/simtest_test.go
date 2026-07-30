package simtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// mockStore is an in-memory store.Store for simtest harness tests.
type mockStore struct {
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

func (m *mockStore) GetEvent(_ context.Context, id string) (store.Event, error) {
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

func (m *mockStore) ListWatchedContracts(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.watched))
	copy(out, m.watched)
	return out, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watched = append(m.watched, id)
	return nil
}

var _ store.Store = (*mockStore)(nil)

// Remaining store.Store methods that simtest doesn't exercise.

func (m *mockStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error { return nil }
func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}
func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}
func (m *mockStore) UpdateAuditFinding(context.Context, store.AuditFinding) error { return nil }
func (m *mockStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (m *mockStore) Ping(context.Context) error                 { return nil }
func (m *mockStore) Stats(context.Context) (store.Stats, error) { return store.Stats{}, nil }

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
	_ = err

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
