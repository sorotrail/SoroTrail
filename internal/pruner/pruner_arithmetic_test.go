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

// mockArithmeticStore implements store.Store specifically for testing pruner deletion arithmetic,
// bound interactions, partial failures, batching stops/resumes, and exact counts.
type mockArithmeticStore struct {
	sync.Mutex
	ingestedLedger int64
	events         map[string]store.Event
	deleteErr      error
	deleteCalls    int
}

func newMockArithmeticStore() *mockArithmeticStore {
	return &mockArithmeticStore{
		events: make(map[string]store.Event),
	}
}

func (m *mockArithmeticStore) SetIngestionState(ledger int64) {
	m.Lock()
	defer m.Unlock()
	m.ingestedLedger = ledger
}

func (m *mockArithmeticStore) AddEvent(id string, ledger int64, createdAt time.Time) {
	m.Lock()
	defer m.Unlock()
	m.events[id] = store.Event{
		ID:        id,
		Ledger:    ledger,
		CreatedAt: createdAt,
	}
}

func (m *mockArithmeticStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	m.Lock()
	defer m.Unlock()
	if m.ingestedLedger == 0 && len(m.events) == 0 {
		return store.IngestionState{}, store.ErrNotFound
	}
	return store.IngestionState{
		Network:            "default",
		LastIngestedLedger: m.ingestedLedger,
	}, nil
}

func (m *mockArithmeticStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	m.Lock()
	defer m.Unlock()
	m.deleteCalls++

	if m.deleteErr != nil {
		return 0, m.deleteErr
	}

	var matched []string
	for id, ev := range m.events {
		matchLedger := ev.Ledger < maxLedger
		matchTime := true
		if !beforeTime.IsZero() {
			matchTime = ev.CreatedAt.Before(beforeTime)
		}
		if matchLedger && matchTime {
			matched = append(matched, id)
			if len(matched) >= limit {
				break
			}
		}
	}

	for _, id := range matched {
		delete(m.events, id)
	}

	return int64(len(matched)), nil
}

// Unused store interface stubs to satisfy store.Store interface
func (m *mockArithmeticStore) UpsertEvents(context.Context, []store.Event) (int64, error) {
	return 0, nil
}
func (m *mockArithmeticStore) GetEvent(context.Context, string, store.Scope) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}
func (m *mockArithmeticStore) QueryEvents(context.Context, store.EventFilter, store.Scope) ([]store.Event, error) {
	return nil, nil
}
func (m *mockArithmeticStore) AddWatchedContract(context.Context, string) error    { return nil }
func (m *mockArithmeticStore) RemoveWatchedContract(context.Context, string) error { return nil }
func (m *mockArithmeticStore) ListWatchedContracts(context.Context) ([]string, error) {
	return nil, nil
}
func (m *mockArithmeticStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}
func (m *mockArithmeticStore) SaveReplayState(context.Context, store.ReplayState) error { return nil }
func (m *mockArithmeticStore) GetReplayState(context.Context) (store.ReplayState, error) {
	return store.ReplayState{}, store.ErrNotFound
}
func (m *mockArithmeticStore) ReplaceEventsInRange(context.Context, int64, int64, []store.Event) error {
	return nil
}
func (m *mockArithmeticStore) GetRawXDRBatch(context.Context, int64, int64, int) ([]any, error) {
	return nil, nil
}
func (m *mockArithmeticStore) SaveAuditState(context.Context, store.AuditState) error { return nil }
func (m *mockArithmeticStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (m *mockArithmeticStore) Ping(context.Context) error { return nil }
func (m *mockArithmeticStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (m *mockArithmeticStore) CountEventsBefore(context.Context, int64, time.Time) (int64, error) {
	return 0, nil
}

func TestPrunerDeletionArithmetic_Cases(t *testing.T) {
	now := time.Now()

	t.Run("age_based_bound_alone", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		st.AddEvent("old", 500, now.Add(-5*time.Hour))
		st.AddEvent("recent", 600, now.Add(-30*time.Minute))

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MaxAge: 2 * time.Hour,
		})
		require.True(t, prn.Enabled())

		deleted, err := prn.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
		assert.Equal(t, 1, len(st.events))
		_, exists := st.events["recent"]
		assert.True(t, exists)
	})

	t.Run("ledger_floor_bound_alone", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		st.AddEvent("low", 100, now.Add(-10*time.Minute))
		st.AddEvent("high", 800, now.Add(-10*time.Minute))

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MinLedger: 500,
		})
		require.True(t, prn.Enabled())

		deleted, err := prn.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
		assert.Equal(t, 1, len(st.events))
		_, exists := st.events["high"]
		assert.True(t, exists)
	})

	t.Run("conservative_bound_winning_when_both_apply", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		// e1: old enough (-5h > 2h) AND ledger < minLedger (300 < 500) -> deleted
		st.AddEvent("e1", 300, now.Add(-5*time.Hour))
		// e2: old enough (-5h > 2h), but ledger 700 >= minLedger 500 -> kept because ledger bound is more conservative (higher than minLedger threshold)
		st.AddEvent("e2", 700, now.Add(-5*time.Hour))
		// e3: ledger 200 < 500, but recent (-10min > -2h) -> kept because age bound is more conservative
		st.AddEvent("e3", 200, now.Add(-10*time.Minute))

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MaxAge:    2 * time.Hour,
			MinLedger: 500,
		})
		require.True(t, prn.Enabled())

		deleted, err := prn.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
		_, e1Exists := st.events["e1"]
		_, e2Exists := st.events["e2"]
		_, e3Exists := st.events["e3"]
		assert.False(t, e1Exists)
		assert.True(t, e2Exists)
		assert.True(t, e3Exists)
	})

	t.Run("batching_stopping_and_resuming_correctly", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		for i := 0; i < 7; i++ {
			st.AddEvent(string(rune('a'+i)), int64(10+i), now.Add(-10*time.Hour))
		}

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MinLedger: 100,
			BatchSize: 3,
			Pause:     1 * time.Millisecond,
		})
		require.True(t, prn.Enabled())

		// Simulate full Run loop or pruneOnce batch drain. pruneOnce loops until batch returns < BatchSize.
		deleted, err := prn.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(7), deleted)
		assert.Equal(t, 0, len(st.events))
		assert.Equal(t, 3, st.deleteCalls, "should take 3 batches (3+3+1)")
	})

	t.Run("disabled_pruner_deleting_nothing", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		st.AddEvent("e1", 50, now.Add(-10*time.Hour))

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{})
		assert.False(t, prn.Enabled())

		// Run should return immediately without touching store
		ctx := context.Background()
		err := prn.Run(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(st.events))
	})

	t.Run("reported_counts_matching_removed", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		st.AddEvent("e1", 10, now.Add(-10*time.Hour))
		st.AddEvent("e2", 20, now.Add(-10*time.Hour))
		st.AddEvent("e3", 30, now.Add(-10*time.Hour))

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MinLedger: 50,
			BatchSize: 2,
		})

		deleted, err := prn.pruneOnce(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(3), deleted)
		assert.Equal(t, int64(3), prn.Metrics().TotalRowsPurged)
	})

	t.Run("partial_failure_not_leaving_half_committed_run", func(t *testing.T) {
		st := newMockArithmeticStore()
		st.SetIngestionState(1000)
		st.AddEvent("e1", 10, now.Add(-10*time.Hour))
		st.AddEvent("e2", 20, now.Add(-10*time.Hour))

		st.deleteErr = errors.New("database connection lost")

		prn := New(st, slog.New(slog.NewTextHandler(nopWriter{}, nil)), Options{
			MinLedger: 50,
			BatchSize: 1,
		})

		deleted, err := prn.pruneOnce(context.Background())
		assert.Error(t, err)
		assert.Equal(t, int64(0), deleted)
		assert.Equal(t, 2, len(st.events), "no rows should be permanently deleted if batch delete errors")
	})
}
