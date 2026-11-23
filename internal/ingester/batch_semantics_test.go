package ingester

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type recordingDeadLetterSink struct {
	mu      sync.Mutex
	letters []store.DeadLetterInput
	failErr error
}

func (s *recordingDeadLetterSink) DeadLetterEvent(_ context.Context, in store.DeadLetterInput) (store.DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil {
		return store.DeadLetter{}, s.failErr
	}
	s.letters = append(s.letters, in)
	return store.DeadLetter{ID: int64(len(s.letters)), EventID: in.EventID, Ledger: in.Ledger}, nil
}

type partialFailureStore struct {
	*mockStore
	poisonID     string
	transientAt  int // 1-indexed UpsertEvents call that fails transiently
	upsertCalls  int
	persistedIDs map[string]store.Event
}

func newPartialFailureStore() *partialFailureStore {
	return &partialFailureStore{
		mockStore:    newMockStore(),
		persistedIDs: make(map[string]store.Event),
	}
}

func (s *partialFailureStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	s.upsertCalls++
	if s.transientAt > 0 && s.upsertCalls == s.transientAt {
		return 0, errors.New("connection reset by peer")
	}
	for _, ev := range events {
		if s.poisonID != "" && ev.ID == s.poisonID {
			return 0, fmt.Errorf("%w: invalid json payload on %s", store.ErrPermanentEventFailure, ev.ID)
		}
	}
	var inserted int64
	for _, ev := range events {
		if _, exists := s.persistedIDs[ev.ID]; !exists {
			s.persistedIDs[ev.ID] = ev
			inserted++
		}
	}
	return inserted, nil
}

func TestBatchPartialFailureSemantics(t *testing.T) {
	t.Run("permanent store poison event is isolated to dead_letters and cursor advances", func(t *testing.T) {
		client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{
			Events: []rpc.Event{
				rpcEvent("ev-1", 100),
				rpcEvent("ev-poison", 101),
				rpcEvent("ev-3", 102),
			},
			LatestLedger: 105,
		}}}
		st := newPartialFailureStore()
		st.poisonID = "ev-poison"
		dl := &recordingDeadLetterSink{}

		ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})
		ing.SetDeadLetterSink(dl)

		beforeMetric := testutil.ToFloat64(metrics.DeadLettersTotal.WithLabelValues("store"))
		_, err := ing.runOnce(context.Background())
		require.NoError(t, err)

		// Healthy events are durably stored; the poison event is recorded in dead_letters; cursor advances.
		assert.Contains(t, st.persistedIDs, "ev-1")
		assert.Contains(t, st.persistedIDs, "ev-3")
		assert.NotContains(t, st.persistedIDs, "ev-poison")
		require.Len(t, dl.letters, 1)
		assert.Equal(t, "ev-poison", dl.letters[0].EventID)
		assert.Equal(t, int64(101), dl.letters[0].Ledger)
		assert.Equal(t, int64(102), st.state.LastIngestedLedger, "cursor advances once healthy events and dead letter are committed")
		assert.Equal(t, beforeMetric+1, testutil.ToFloat64(metrics.DeadLettersTotal.WithLabelValues("store")))
	})

	t.Run("transient mid-batch failure aborts without advancing cursor and retry is idempotent", func(t *testing.T) {
		page := rpc.GetEventsResponse{
			Events: []rpc.Event{
				rpcEvent("ev-1", 200),
				rpcEvent("ev-2", 201),
				rpcEvent("ev-3", 202),
				rpcEvent("ev-4", 203),
			},
			LatestLedger: 210,
		}
		client := &mockRPC{eventsResps: []rpc.GetEventsResponse{page, page}}
		st := newPartialFailureStore()
		// Chunk size is 2: 1st chunk [ev-1, ev-2] succeeds; 2nd chunk [ev-3, ev-4] fails transiently.
		st.transientAt = 2
		dl := &recordingDeadLetterSink{}

		ing := newTestIngester(client, st, Options{
			StartLedger: 200,
			PageLimit:   100,
			BatchSize:   2,
		})
		ing.SetDeadLetterSink(dl)

		_, err := ing.runOnce(context.Background())
		require.Error(t, err)
		assert.Nil(t, st.state, "cursor must never advance past a batch that failed transiently")
		assert.Empty(t, dl.letters, "transient errors must not be dead-lettered")

		// Retry on next cycle replays the full batch; idempotent upserts absorb [ev-1, ev-2] and commit [ev-3, ev-4].
		_, err = ing.runOnce(context.Background())
		require.NoError(t, err)
		assert.Len(t, st.persistedIDs, 4, "retry after partial commit stores all 4 events without duplicates")
		require.NotNil(t, st.state)
		assert.Equal(t, int64(203), st.state.LastIngestedLedger)
	})

	t.Run("dead-letter sink failure aborts cycle so no event is silently dropped", func(t *testing.T) {
		client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("ev-poison", 300)},
			LatestLedger: 310,
		}}}
		st := newPartialFailureStore()
		st.poisonID = "ev-poison"
		dl := &recordingDeadLetterSink{failErr: errors.New("dead_letters table unavailable")}

		ing := newTestIngester(client, st, Options{StartLedger: 300, PageLimit: 100})
		ing.SetDeadLetterSink(dl)

		_, err := ing.runOnce(context.Background())
		require.Error(t, err)
		assert.Nil(t, st.state, "cursor must not advance when dead-lettering fails")
	})
}
