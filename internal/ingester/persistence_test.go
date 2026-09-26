package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

func TestIngester_Persistence(t *testing.T) {
	t.Run("clean_page_persists_every_event", func(t *testing.T) {
		st := newMockStore()
		ing := newTestIngester(&mockRPC{}, st, Options{})
		events := []rpc.Event{rpcEvent("1", 10), rpcEvent("2", 10)}

		require.NoError(t, ing.persistEvents(context.Background(), events, 10))
		assert.Contains(t, st.events, "1")
		assert.Contains(t, st.events, "2")
	})

	t.Run("idempotent_re_persisted_page", func(t *testing.T) {
		st := newMockStore()
		ing := newTestIngester(&mockRPC{}, st, Options{})
		events := []rpc.Event{rpcEvent("1", 10)}

		require.NoError(t, ing.persistEvents(context.Background(), events, 10))
		require.NoError(t, ing.persistEvents(context.Background(), events, 10))

		// The mock records one UpsertEvents batch per persist call; both
		// passes succeeded so the event is stored once and the page was
		// re-persisted without error.
		assert.Len(t, st.upserted, 2)
		assert.Equal(t, "1", st.events["1"].ID)
	})

	// poisonEvent fails toStoreEvent via un-marshalable TopicJSON, mimicking
	// an event that cannot be decoded/persisted.
	poisonEvent := func() rpc.Event {
		return rpc.Event{
			ID:        "poison",
			Type:      "contract",
			Ledger:    10,
			TopicJSON: []json.RawMessage{json.RawMessage(`{`)},
		}
	}

	t.Run("poison_event_quarantined_with_sink", func(t *testing.T) {
		st := newMockStore()
		ing := newTestIngester(&mockRPC{}, st, Options{})
		// The mock store's DeadLetterEvent satisfies DeadLetterSink.
		ing.deadLetterStore = st

		events := []rpc.Event{rpcEvent("clean1", 10), poisonEvent(), rpcEvent("clean2", 10)}

		require.NoError(t, ing.persistEvents(context.Background(), events, 10))
		assert.Contains(t, st.events, "clean1")
		assert.Contains(t, st.events, "clean2")
		assert.NotContains(t, st.events, "poison")
		require.Len(t, st.deadLetters, 1)
		assert.Equal(t, "poison", st.deadLetters[0].EventID)
	})

	t.Run("poison_event_aborts_without_sink", func(t *testing.T) {
		st := newMockStore()
		ing := newTestIngester(&mockRPC{}, st, Options{})

		events := []rpc.Event{rpcEvent("clean", 10), poisonEvent()}

		err := ing.persistEvents(context.Background(), events, 10)
		assert.Error(t, err)
		assert.Len(t, st.events, 0)
	})
}

type recordingNotifier struct {
	mu     sync.Mutex
	calls  int
	events int
}

func (n *recordingNotifier) NotifyEvents(_ context.Context, events []store.Event) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls++
	n.events += len(events)
}

func (n *recordingNotifier) snapshot() (calls, events int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls, n.events
}

func TestIngester_DryRunDoesNotWriteOrNotify(t *testing.T) {
	st := newMockStore()
	log, buf := recordingLogger()
	notifier := &recordingNotifier{}
	ing := New(&mockRPC{}, st, passthroughDecoder{}, log, Options{DryRun: true})
	ing.SetNotifier(notifier)

	events := []rpc.Event{rpcEvent("1", 10), rpcEvent("2", 10)}
	require.NoError(t, ing.persistEvents(context.Background(), events, 10))

	assert.Empty(t, st.events)
	assert.Empty(t, st.upserted)
	assert.Equal(t, 0, st.stateSaves)
	assert.Equal(t, 0, st.addressUpserts)
	calls, notified := notifier.snapshot()
	assert.Zero(t, calls)
	assert.Zero(t, notified)

	records := logRecords(t, buf, nil)
	var found bool
	for _, record := range records {
		if record["msg"] == "dry-run: would write events" {
			found = true
			assert.Equal(t, float64(2), record["count"])
			assert.Contains(t, fmt.Sprint(record["event_ids"]), "1")
			assert.Contains(t, fmt.Sprint(record["event_ids"]), "2")
		}
	}
	assert.True(t, found, "dry run must log the candidate write")
}

func TestIngester_DryRunDoesNotDeadLetterDecodeFailures(t *testing.T) {
	st := newMockStore()
	ing := newTestIngester(&mockRPC{}, st, Options{DryRun: true})
	poison := rpc.Event{ID: "poison", Ledger: 10, TopicJSON: []json.RawMessage{json.RawMessage(`{`)}}

	require.NoError(t, ing.persistEvents(context.Background(), []rpc.Event{poison}, 10))
	assert.Empty(t, st.deadLetters)
	assert.Empty(t, st.events)
}

func TestIngester_DryRunLeavesExistingStateUntouched(t *testing.T) {
	st := newMockStore()
	initial := store.IngestionState{Network: "testnet", LastIngestedLedger: 42, LastCursor: "saved-cursor"}
	st.state = &initial
	client := &mockRPC{
		health:      rpc.Health{LatestLedger: 50},
		eventsResps: []rpc.GetEventsResponse{{LatestLedger: 50}},
	}
	ing := newTestIngester(client, st, Options{DryRun: true, PageLimit: 10})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, initial, *st.state)
	assert.Equal(t, 0, st.stateSaves)
	assert.Empty(t, st.upserted)
}

func TestIngester_DryRunUsesEphemeralCursor(t *testing.T) {
	client := &mockRPC{
		health: rpc.Health{LatestLedger: 12},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 10)}, LatestLedger: 12, Cursor: "cursor-1"},
			{Events: []rpc.Event{rpcEvent("e2", 11)}, LatestLedger: 12, Cursor: "cursor-2"},
			{LatestLedger: 12},
		},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{
		DryRun:         true,
		StartLedger:    10,
		PageLimit:      1,
		WriteBatchSize: 1,
	})

	require.NoError(t, ing.RunDryRun(context.Background()))
	require.Len(t, client.eventsRequests, 3)
	assert.Empty(t, client.eventsRequests[0].Pagination.Cursor)
	assert.Equal(t, "cursor-1", client.eventsRequests[1].Pagination.Cursor)
	assert.Equal(t, "cursor-2", client.eventsRequests[2].Pagination.Cursor)
	assert.Empty(t, st.events)
	assert.Empty(t, st.upserted)
	assert.Equal(t, 0, st.stateSaves)
}

func TestIngester_DryRunReingestDoesNotReplace(t *testing.T) {
	client := &mockRPC{
		eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("reorg-event", 20)},
			LatestLedger: 30,
		}},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{DryRun: true, PageLimit: 10})

	n, err := ing.ReingestRange(context.Background(), client, 20, 20)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 0, st.replaceCalls)
	assert.Empty(t, st.events)
}
