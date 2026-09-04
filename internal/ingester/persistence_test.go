package ingester

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
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
