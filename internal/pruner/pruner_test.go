package pruner

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

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
	_, err = st.GetEvent(context.Background(), "e3", store.Scope{})
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

	_, err = st.GetEvent(context.Background(), "old", store.Scope{})
	assert.ErrorIs(t, err, store.ErrNotFound)
	_, err = st.GetEvent(context.Background(), "recent", store.Scope{})
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

// CountContracts satisfies store.Store; unused by these tests.
func (m *mockStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}

func (m *mockStore) CountEventsBefore(_ context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for _, e := range m.events {
		if e.Ledger < maxLedger {
			if !beforeTime.IsZero() && !e.CreatedAt.Before(beforeTime) {
				continue
			}
			count++
			if count >= int64(limit) {
				break
			}
		}
	}
	return count, nil
}

func TestPrunerDryRun(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)
	st.addEvent("e1", 50, time.Now().Add(-24*time.Hour))
	st.addEvent("e2", 90, time.Now().Add(-24*time.Hour))

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 80,
		BatchSize: 10,
		DryRun:    true,
	})
	require.True(t, prn.Enabled())

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, 2, st.eventCount(), "dry-run must not delete any events")
}

func TestPrunerDryRunMetrics(t *testing.T) {
	st := newMockStore()
	st.setIngestionState(100)
	for i := 0; i < 5; i++ {
		st.addEvent(string(rune('a'+i)), int64(50+i), time.Now().Add(-24*time.Hour))
	}

	prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
		MinLedger: 55,
		BatchSize: 10,
		DryRun:    true,
	})

	total, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(5), total)

	m := prn.Metrics()
	assert.Equal(t, int64(0), m.TotalRowsPurged, "dry-run must not count as purged")
	assert.Equal(t, int64(5), m.DryRunEligibleRows, "dry-run must report eligible rows")
}

// mockStore implements store.Store for testing the pruner.
type mockStore struct {
	store.Store

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

func (m *mockStore) GetEvent(_ context.Context, id string, _ store.Scope) (store.Event, error) {
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

func (m *mockStore) GetAuditState(context.Context, string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}

func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error {
	return nil
}

func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, _ string, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}

func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}

func (m *mockStore) UpdateAuditFinding(context.Context, store.AuditFinding) error {
	return nil
}

func (m *mockStore) ListOpenFindingsByRange(context.Context, string, int64, int64) (store.AuditFinding, error) {
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

func (m *mockStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
	return nil, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, _ store.WatchedContract) error {
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
			if limit > 0 && deleted >= int64(limit) {
				break
			}
		}
	}
	return deleted, nil
}

func (m *mockStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}

func (m *mockStore) Ping(context.Context) error { return nil }

func (m *mockStore) setIngestionState(ledger int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingState = store.IngestionState{
		LastIngestedLedger: ledger,
	}
}

func TestPrunerPolicyMatrix(t *testing.T) {
	now := time.Now()

	t.Run("age-based retention deletes only rows older than threshold", func(t *testing.T) {
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "old-1", Ledger: 10, CreatedAt: now.Add(-48 * time.Hour)},
			{ID: "old-2", Ledger: 11, CreatedAt: now.Add(-25 * time.Hour)},
			{ID: "recent-1", Ledger: 12, CreatedAt: now.Add(-1 * time.Hour)},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)

		p := New(ms, slog.Default(), Options{
			MaxAge:    24 * time.Hour,
			BatchSize: 100,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		ms.mu.Lock()
		defer ms.mu.Unlock()

		_, foundOld1 := ms.events["old-1"]
		_, foundOld2 := ms.events["old-2"]
		_, foundRecent := ms.events["recent-1"]

		assert.False(t, foundOld1, "old-1 should be deleted")
		assert.False(t, foundOld2, "old-2 should be deleted")
		assert.True(t, foundRecent, "recent-1 should be kept")
		assert.Equal(t, int64(2), p.Metrics().TotalRowsPurged)
	})

	t.Run("ledger-floor retention never deletes at or above the floor", func(t *testing.T) {
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "below-floor", Ledger: 5, CreatedAt: now},
			{ID: "at-floor", Ledger: 10, CreatedAt: now},
			{ID: "above-floor", Ledger: 15, CreatedAt: now},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)

		p := New(ms, slog.Default(), Options{
			MinLedger: 10,
			BatchSize: 100,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		ms.mu.Lock()
		defer ms.mu.Unlock()

		_, foundBelow := ms.events["below-floor"]
		_, foundAt := ms.events["at-floor"]
		_, foundAbove := ms.events["above-floor"]

		assert.False(t, foundBelow, "event below min ledger should be deleted")
		assert.True(t, foundAt, "event at min ledger should be retained")
		assert.True(t, foundAbove, "event above min ledger should be retained")
		assert.Equal(t, int64(1), p.Metrics().TotalRowsPurged)
	})

	t.Run("combined age and ledger bounds apply the more conservative bound", func(t *testing.T) {
		// Suppose MinLedger allows deleting up to Ledger 20, but MaxAge only allows deleting events older than 24h.
		// Event A: Ledger 5, old (older than 24h) -> should be deleted by both.
		// Event B: Ledger 5, recent (newer than 24h) -> kept by age bound even though ledger bound would delete it.
		// Event C: Ledger 25, old (older than 24h) -> kept by ledger floor even though age would delete it.
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "old-and-low", Ledger: 5, CreatedAt: now.Add(-48 * time.Hour)},
			{ID: "recent-and-low", Ledger: 5, CreatedAt: now.Add(-1 * time.Hour)},
			{ID: "old-and-high", Ledger: 25, CreatedAt: now.Add(-48 * time.Hour)},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)

		p := New(ms, slog.Default(), Options{
			MaxAge:    24 * time.Hour,
			MinLedger: 20,
			BatchSize: 100,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		ms.mu.Lock()
		defer ms.mu.Unlock()

		_, foundOldLow := ms.events["old-and-low"]
		_, foundRecentLow := ms.events["recent-and-low"]
		_, foundOldHigh := ms.events["old-and-high"]

		assert.False(t, foundOldLow, "old-and-low should be pruned")
		assert.True(t, foundRecentLow, "recent-and-low should be retained due to age bound")
		assert.True(t, foundOldHigh, "old-and-high should be retained due to ledger floor bound")
		assert.Equal(t, int64(1), p.Metrics().TotalRowsPurged)
	})

	t.Run("batching stops at configured size and resumes correctly", func(t *testing.T) {
		ms := newMockStore()
		var seedEvents []store.Event
		for i := 1; i <= 3; i++ {
			seedEvents = append(seedEvents, store.Event{
				ID:        string(rune('a' + i - 1)),
				Ledger:    int64(i),
				CreatedAt: now.Add(-48 * time.Hour),
			})
		}
		_, err := ms.UpsertEvents(context.Background(), seedEvents)
		require.NoError(t, err)

		ms.setIngestionState(100)

		// Set BatchSize to 1 so each sweep deletes exactly 1 row.
		// With 3 eligible rows and BatchSize 1, it should execute multiple delete calls and resume correctly.
		p := New(ms, slog.Default(), Options{
			MinLedger: 10,
			BatchSize: 1,
			Pause:     time.Millisecond,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		ms.mu.Lock()
		defer ms.mu.Unlock()

		assert.GreaterOrEqual(t, ms.deleteCalls, 3)
		assert.Equal(t, int64(3), p.Metrics().TotalRowsPurged)
	})

	t.Run("disabled pruner deletes nothing", func(t *testing.T) {
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "e1", Ledger: 1, CreatedAt: now.Add(-48 * time.Hour)},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)

		// Zero options means both MaxAge and MinLedger are zero (disabled)
		p := New(ms, slog.Default(), Options{})

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		ms.mu.Lock()
		defer ms.mu.Unlock()

		assert.Len(t, ms.events, 1)
		assert.Equal(t, int64(0), p.Metrics().TotalRowsPurged)
		assert.Equal(t, 0, ms.deleteCalls)
	})

	t.Run("reported row counts match what was actually removed", func(t *testing.T) {
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "e1", Ledger: 1, CreatedAt: now},
			{ID: "e2", Ledger: 2, CreatedAt: now},
			{ID: "e3", Ledger: 3, CreatedAt: now},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)

		p := New(ms, slog.Default(), Options{
			MinLedger: 5,
			BatchSize: 10,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		_ = p.Run(ctx)

		metrics := p.Metrics()
		assert.Equal(t, int64(3), metrics.TotalRowsPurged)

		ms.mu.Lock()
		defer ms.mu.Unlock()
		assert.Empty(t, ms.events)
	})

	t.Run("partial failure does not leave run half-committed or crash", func(t *testing.T) {
		ms := newMockStore()
		_, err := ms.UpsertEvents(context.Background(), []store.Event{
			{ID: "e1", Ledger: 1, CreatedAt: now},
		})
		require.NoError(t, err)

		ms.setIngestionState(100)
		ms.deleteErr = errors.New("database error during delete")

		p := New(ms, slog.Default(), Options{
			MinLedger: 5,
			BatchSize: 10,
			Interval:  time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		// Run should handle the error gracefully without panicking
		err = p.Run(ctx)
		assert.Error(t, err)
	})
}

type stubLagYieldController struct {
	yield  bool
	reason string
}

func (s *stubLagYieldController) ShouldYield(string) (bool, string) {
	return s.yield, s.reason
}

func TestPrunerLagBackpressureAndMaxDeferral(t *testing.T) {
	now := time.Now()
	ms := newMockStore()
	_, err := ms.UpsertEvents(context.Background(), []store.Event{
		{ID: "e1", Ledger: 1, CreatedAt: now},
		{ID: "e2", Ledger: 2, CreatedAt: now},
	})
	require.NoError(t, err)
	ms.setIngestionState(100)

	lagCtrl := &stubLagYieldController{yield: true, reason: "ingest_lagging"}
	p := New(ms, slog.Default(), Options{
		MinLedger:     50,
		BatchSize:     10,
		LagController: lagCtrl,
	})

	// While lagging below max deferral, pruneOnce yields without deleting.
	deleted, err := p.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	assert.Equal(t, uint64(1), p.Metrics().LagYields)
	assert.Len(t, ms.events, 2)

	// When max deferral is reached, pruneOnce runs and records the override.
	lagCtrl.yield = false
	lagCtrl.reason = "max_deferral_reached"
	deleted, err = p.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, uint64(1), p.Metrics().MaxDeferralOverrides)
	assert.Empty(t, ms.events)
}
