package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestIngester_Persistence(t *testing.T) {
	t.Run("clean_page_persists_every_event", func(t *testing.T) {
		ms := &mockStore{}
		ing := &Ingester{store: ms}
		events := []store.Event{{ID: "1"}, {ID: "2"}}
		
		err := ing.persistEvents(context.Background(), events)
		require.NoError(t, err)
		assert.Equal(t, 2, len(ms.upsertedEvents))
		assert.Equal(t, 1, ms.indexedCalls)
	})

	t.Run("idempotent_re_persisted_page", func(t *testing.T) {
		ms := &mockStore{err: nil}
		ing := &Ingester{store: ms}
		events := []store.Event{{ID: "1"}}
		
		// First pass
		require.NoError(t, ing.persistEvents(context.Background(), events))
		// Second pass
		require.NoError(t, ing.persistEvents(context.Background(), events))
		
		assert.Equal(t, 2, ms.upsertedEventsCount)
		assert.Equal(t, 2, ms.indexedCalls)
	})

	t.Run("poison_event_quarantined_with_sink", func(t *testing.T) {
		ms := &mockStore{poison: "poison"}
		// Dead letter sink
		dls := &mockDeadLetterSink{}
		ing := &Ingester{store: ms, deadLetterSink: dls}
		events := []store.Event{{ID: "clean1"}, {ID: "poison"}, {ID: "clean2"}}
		
		err := ing.persistEvents(context.Background(), events)
		require.NoError(t, err)
		assert.Equal(t, 2, len(ms.upsertedEvents))
		assert.Equal(t, 1, len(dls.quarantined))
		assert.Equal(t, "poison", dls.quarantined[0].ID)
	})

	t.Run("poison_event_aborts_without_sink", func(t *testing.T) {
		ms := &mockStore{poison: "poison"}
		ing := &Ingester{store: ms}
		events := []store.Event{{ID: "clean"}, {ID: "poison"}}
		
		err := ing.persistEvents(context.Background(), events)
		assert.Error(t, err)
		assert.Equal(t, 0, len(ms.upsertedEvents))
	})
}

type mockDeadLetterSink struct {
	quarantined []store.Event
}

func (m *mockDeadLetterSink) DeadLetter(ctx context.Context, e store.Event, reason error) error {
	m.quarantined = append(m.quarantined, e)
	return nil
}
