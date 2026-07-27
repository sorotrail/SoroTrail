package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sorotrail/sorotrail/internal/store"
)

func mkEvent(id, contractID string, ledger int64) store.Event {
	return store.Event{
		ID:         id,
		ContractID: contractID,
		Ledger:     ledger,
		Type:       "contract",
		Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:      json.RawMessage(`"100"`),
		CreatedAt:  time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
	}
}

func TestBroadcaster_DeliversMatchingEvents(t *testing.T) {
	b := New(10)
	sub := b.Subscribe(store.EventFilter{ContractID: "CA"})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{
		mkEvent("1", "CA", 1),
		mkEvent("2", "CB", 1),
		mkEvent("3", "CA", 2),
	})

	got := <-sub.Events()
	assert.Equal(t, "1", got.ID)
	got = <-sub.Events()
	assert.Equal(t, "3", got.ID)

	// No more events.
	select {
	case <-sub.Events():
		t.Fatal("unexpected event")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestBroadcaster_FilterByType(t *testing.T) {
	b := New(10)
	sub := b.Subscribe(store.EventFilter{Types: []string{"system"}})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{
		mkEvent("1", "CA", 1),
		{ID: "2", ContractID: "CA", Ledger: 1, Type: "system", Topics: json.RawMessage(`[]`), Value: json.RawMessage(`"x"`), CreatedAt: time.Now()},
	})

	got := <-sub.Events()
	assert.Equal(t, "2", got.ID)

	select {
	case <-sub.Events():
		t.Fatal("unexpected event")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestBroadcaster_FilterByLedgerRange(t *testing.T) {
	b := New(10)
	sub := b.Subscribe(store.EventFilter{FromLedger: 5, ToLedger: 10})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{
		mkEvent("1", "CA", 1),
		mkEvent("2", "CA", 5),
		mkEvent("3", "CA", 10),
		mkEvent("4", "CA", 11),
	})

	got := <-sub.Events()
	assert.Equal(t, "2", got.ID)
	got = <-sub.Events()
	assert.Equal(t, "3", got.ID)

	select {
	case <-sub.Events():
		t.Fatal("unexpected event")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestBroadcaster_SlowConsumerEviction(t *testing.T) {
	b := New(1) // tiny buffer
	sub := b.Subscribe(store.EventFilter{})
	defer sub.Close()

	// Fill the buffer (size 1) and then send another event to trigger eviction.
	b.Publish(context.Background(), []store.Event{mkEvent("1", "CA", 1)})
	b.Publish(context.Background(), []store.Event{mkEvent("2", "CA", 1)})

	// The channel is closed after eviction, but the buffered event ("1") is
	// still readable. Read it first, then the channel should report closed.
	ev, ok := <-sub.Events()
	assert.True(t, ok, "expected buffered event to be readable")
	assert.Equal(t, "1", ev.ID)
	_, ok = <-sub.Events()
	assert.False(t, ok, "expected channel to be closed after draining buffered events")
}

func TestBroadcaster_NoMatch(t *testing.T) {
	b := New(10)
	sub := b.Subscribe(store.EventFilter{ContractID: "CB"})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{
		mkEvent("1", "CA", 1),
	})

	select {
	case <-sub.Events():
		t.Fatal("unexpected event")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestBroadcaster_CloseUnsubscribes(t *testing.T) {
	b := New(10)
	sub := b.Subscribe(store.EventFilter{})
	sub.Close()

	// Should not panic; the subscriber should be removed.
	b.Publish(context.Background(), []store.Event{mkEvent("1", "CA", 1)})
}

func TestBroadcaster_ConcurrentPublish(t *testing.T) {
	b := New(64)
	sub := b.Subscribe(store.EventFilter{})
	defer sub.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			b.Publish(context.Background(), []store.Event{mkEvent(fmt.Sprintf("e%d", n), "CA", 1)})
		}(i)
	}
	wg.Wait()

	received := 0
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				return
			}
			received++
		case <-time.After(100 * time.Millisecond):
			if received == 10 {
				return
			}
			t.Fatalf("expected 10 events, got %d", received)
		}
	}
}
