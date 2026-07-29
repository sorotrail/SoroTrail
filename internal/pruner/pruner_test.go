package pruner

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/store"
)

// mockStore implements store.Store for testing the pruner.
type mockStore struct {
	mu          sync.Mutex
	events      map[string]store.Event
	ingState    store.IngestionState
	ingErr      error
	deleteErr   error
	deleteCalls int
}

func newMockStore() *mockStore {
	return &mockStore{events: map[string]store.Event{}}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *mockStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) LedgerRangeCensus(context.Context, int64, int64, bool) ([]store.LedgerCensus, error) {
	return nil, nil
}

func (m *mockStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}

func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error {
	return nil
}

func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}

func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}

func (m *mockStore) UpdateAuditFinding(context.Context, store.AuditFinding) error {
	return nil
}

func (m *mockStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}

func (m *mockStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ingErr != nil {
		return store.IngestionState{}, m.ingErr
	}
	return m.ingState, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingState = s
	return nil
}

func (m *mockStore) ListWatchedContracts(context.Context) ([]string, error) {
	return nil, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, id string) error {
	return nil
}

func (m *mockStore) DeleteEventsBefore(_ context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls++
	if m.deleteErr != nil {
		return 0, m.deleteErr
	}
	var deleted int64
	for id, e := range m.events {
		if e.Ledger < maxLedger {
			if !beforeTime.IsZero() && !e.CreatedAt.Before(beforeTime) {
				continue
			}
			delete(m.events, id)
			deleted++
			if deleted >= int64(limit) {
				break
			}
		}
	}
	return deleted, nil
}

func (m *mockStore) Stats(context.Context) (store.Stats, error) { return store.Stats{}, nil }
func (m *mockStore) Ping(context.Context) error                 { return nil }

func (m *mockStore) setIngestionState(ledger int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingState = store.IngestionState{LastIngestedLedger: ledger}
}

func (m *mockStore) addEvent(id string, ledger int64, createdAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[id] = store.Event{
		ID:        id,
		Ledger:    ledger,
		CreatedAt: createdAt,
	}
}

func (m *mockStore) eventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func TestPrunerDisabledByDefault(t *testing.T) {
	// When both MaxAge and MinLedger are zero, the pruner is disabled.
	st := newMockStore()
	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{})

	assert.False(t, prn.Enabled())

	// Run should return immediately without error.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := prn.Run(ctx)
	assert.NoError(t, err)
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestPrunerNeverDeletesRecent(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)
	// Add events at ledger 50 and 90 — both below last ingested (100).
	st.addEvent("e1", 50, time.Now().Add(-24*time.Hour))
	st.addEvent("e2", 90, time.Now().Add(-24*time.Hour))

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 80,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	// e1 (ledger 50) is below min 80 → deleted.
	// e2 (ledger 90) is at or above min 80 → kept.
	assert.Equal(t, int64(1), total)
	assert.Equal(t, 1, st.eventCount())
}

func TestPrunerGuardNeverDeletesAboveLastIngested(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)
	// Events at ledger 50, 90, and 150.
	st.addEvent("e1", 50, time.Now().Add(-24*time.Hour))
	st.addEvent("e2", 90, time.Now().Add(-24*time.Hour))
	st.addEvent("e3", 150, time.Now().Add(-24*time.Hour)) // above last ingested!

	// MinLedger would delete everything below 200 — but 150 > 100 (last ingested).
	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 200,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	// maxLedger is min(100, 200) = 100, so only e1 (50) and e2 (90) are eligible.
	// e3 (150) is above the guard and kept.
	assert.Equal(t, int64(2), total)
	assert.Equal(t, 1, st.eventCount())
	_, err = st.GetEvent(context.Background(), "e3")
	assert.NoError(t, err) // e3 should still exist
}

func TestPrunerNoIngestionState(t *testing.T) {
	// No ingestion state yet — pruner should skip gracefully.
	st := newMockStore()
	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 100,
	})

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
}

func TestPrunerBatching(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)
	// Add 10 events, all below ledger 100.
	for i := range 10 {
		st.addEvent(
			string(rune('a'+i)),
			int64(50+i),
			time.Now().Add(-24*time.Hour),
		)
	}

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 60,
		BatchSize: 3,
		Pause:     1 * time.Millisecond,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	// Events with ledger < 60: 10 events starting at ledger 50-59. Only
	// ledger 50-59 (< 60) should be deleted, that's 10 events. But batch
	// size is 3, so it'll take 4 batches (3+3+3+1).
	assert.Equal(t, int64(10), total)
	assert.Equal(t, 0, st.eventCount())
	// 10 events / 3 per batch = 4 calls (3+3+3+1). Asserting the call
	// count catches an off-by-one in the loop's exit condition that a
	// pure total-count check could miss.
	assert.GreaterOrEqual(t, st.deleteCalls, 2,
		"multiple batches should be issued for a backlog the size of MaxBatchSize")
	assert.Less(t, st.deleteCalls, 10,
		"the loop must terminate once a partial batch signals exhaustion")
}

func TestPrunerMaxAge(t *testing.T) {
	now := time.Now()
	st := newMockStore()
	st.setIngestionState(100)

	// Old event (older than 1h)
	st.addEvent("old", 50, now.Add(-2*time.Hour))
	// Recent event (within 1h)
	st.addEvent("recent", 55, now.Add(-30*time.Minute))

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MaxAge:    1 * time.Hour,
		BatchSize: 10,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, 1, st.eventCount())

	_, err = st.GetEvent(context.Background(), "old")
	assert.ErrorIs(t, err, store.ErrNotFound)
	_, err = st.GetEvent(context.Background(), "recent")
	assert.NoError(t, err)
}

func TestPrunerMaxAgeAndMinLedgerCombined(t *testing.T) {
	now := time.Now()
	st := newMockStore()
	st.setIngestionState(100)

	// Old event, low ledger — should be deleted by both criteria
	st.addEvent("e1", 30, now.Add(-2*time.Hour))
	// Old event, high ledger — age says delete, but ledger >= min → keep
	st.addEvent("e2", 80, now.Add(-2*time.Hour))
	// Recent event, low ledger — ledger says delete, but age recent → keep
	st.addEvent("e3", 30, now.Add(-30*time.Minute))

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MaxAge:    1 * time.Hour,
		MinLedger: 50,
		BatchSize: 10,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	// e1: ledger 30 < both guards AND old → deleted
	// e2: ledger 80 >= maxLedger (50, from MinLedger) → kept (the ledger guard stops it)
	// e3: ledger 30 < 50 but created_at is recent (within MaxAge) → kept (time guard stops it)
	// Actually wait, let me reconsider. The logic is:
	// DeleteEventsBefore deletes events WHERE ledger < maxLedger AND (if beforeTime non-zero) created_at < beforeTime.
	// So both conditions must be met (AND logic).
	// maxLedger = min(last_ingested=100, minLedger=50) = 50
	// beforeTime = now - 1h
	// e1: ledger 30 < 50 ✓, created_at -2h < -1h ✓ → delete
	// e2: ledger 80 < 50 ✗ → keep (ledger too high)
	// e3: ledger 30 < 50 ✓, created_at -30min < -1h ✗ → keep (too recent)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, 2, st.eventCount())
}

func TestPrunerMetrics(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)

	for i := range 5 {
		st.addEvent(
			string(rune('a'+i)),
			int64(50+i),
			time.Now().Add(-24*time.Hour),
		)
	}

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 55,
		BatchSize: 10,
	})

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(5), total)

	m := prn.Metrics()
	assert.Equal(t, int64(5), m.TotalRowsPurged)
}

func TestPrunerEmptyStore(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 80,
	})

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
}
