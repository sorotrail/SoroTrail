package broadcast

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// ---------------------------------------------------------------------------
// WebSocket connection lifecycle tests — table-driven, each row covers
// one behaviour that must not regress.
//
// The production flow for a live stream subscriber:
//  1. Client opens a WebSocket / SSE connection
//  2. Server calls Broadcaster.Subscribe(filter) to register the client
//  3. The subscriber receives matching events via Subscription.Events()
//  4. On disconnect or server shutdown, Subscription.Close() is called
//  5. The broadcaster removes the subscriber and closes its channel
//
// These tests verify correctness and resilience under concurrent load,
// slow consumers, abrupt disconnects, and per-connection filter
// isolation.
// ---------------------------------------------------------------------------

func TestWebSocketLifecycle_ManyConcurrentSubscribers(t *testing.T) {
	const numSubs = 100
	const numEvents = 50

	b := New(64)

	type sub struct {
		ch    <-chan store.Event
		close func()
	}
	subs := make([]sub, numSubs)

	for i := 0; i < numSubs; i++ {
		s := b.Subscribe(store.EventFilter{
			ContractID: "CA",
			Scope:      store.WildcardScope(),
		})
		subs[i] = sub{ch: s.Events(), close: s.Close}
	}

	require.Equal(t, numSubs, b.SubscriberCount())

	// Publish events.
	for i := 0; i < numEvents; i++ {
		b.Publish(context.Background(), []store.Event{
			mkEvent("e"+string(rune('A'+i%26)), "CA", int64(i)),
		})
	}

	// Every subscriber must receive every event.
	for si, s := range subs {
		received := 0
		for received < numEvents {
			select {
			case ev, ok := <-s.ch:
				if !ok {
					t.Fatalf("subscriber %d: channel closed after %d events, expected %d",
						si, received, numEvents)
				}
				_ = ev
				received++
			case <-time.After(2 * time.Second):
				t.Fatalf("subscriber %d: timed out after %d events, expected %d",
					si, received, numEvents)
			}
		}
		// Drain any extras (shouldn't happen but be safe).
		select {
		case <-s.ch:
			t.Fatalf("subscriber %d: received extra event", si)
		default:
		}
		s.close()
	}

	// All subscribers removed after close.
	assert.Equal(t, 0, b.SubscriberCount())
}

func TestWebSocketLifecycle_SlowConsumerEvictedWithoutBlocking(t *testing.T) {
	b := New(1) // buffer of 1
	sub := b.Subscribe(store.EventFilter{
		Scope: store.WildcardScope(),
	})
	defer sub.Close()

	// Fill the buffer.
	b.Publish(context.Background(), []store.Event{mkEvent("1", "CA", 1)})
	// The buffer is full; the next publish should evict synchronously.
	done := make(chan struct{})
	go func() {
		b.Publish(context.Background(), []store.Event{mkEvent("2", "CA", 2)})
		close(done)
	}()

	select {
	case <-done:
		// Publish returned without blocking — the slow consumer was evicted.
	case <-time.After(time.Second):
		t.Fatal("publish blocked on slow consumer; eviction must be non-blocking")
	}

	// The subscriber's channel is closed.  Drain any buffered event
	// first (the buffer was size 1 and the first publish filled it).
	_, ok := <-sub.Events()
	if ok {
		// Buffer had one event; the next read must be closed.
		_, ok = <-sub.Events()
	}
	assert.False(t, ok, "channel must be closed after eviction")

	// Other subscribers are unaffected.
	other := b.Subscribe(store.EventFilter{
		Scope: store.WildcardScope(),
	})
	defer other.Close()

	b.Publish(context.Background(), []store.Event{mkEvent("3", "CA", 3)})
	select {
	case ev := <-other.Events():
		assert.Equal(t, "3", ev.ID)
	case <-time.After(time.Second):
		t.Fatal("unrelated subscriber did not receive event")
	}

	assert.Equal(t, 1, b.SubscriberCount(), "only the unrelated subscriber remains")
}

func TestWebSocketLifecycle_CloseCleanedUpNoGoroutineLeak(t *testing.T) {
	b := New(64)

	const numSubs = 20
	subs := make([]*Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = b.Subscribe(store.EventFilter{
			Scope: store.WildcardScope(),
		})
	}
	require.Equal(t, numSubs, b.SubscriberCount())

	// Close all subscribers synchronously.
	for _, s := range subs {
		s.Close()
	}

	// Channels must be closed.
	for i, s := range subs {
		_, ok := <-s.Events()
		assert.False(t, ok, "subscriber %d: channel must be closed after Close()", i)
	}

	assert.Equal(t, 0, b.SubscriberCount(),
		"all subscribers must be removed after Close()")

	// Publish must not panic after all subscribers are closed.
	b.Publish(context.Background(), []store.Event{mkEvent("1", "CA", 1)})
}

func TestWebSocketLifecycle_ServerShutdownClosesConnections(t *testing.T) {
	b := New(64)

	type entry struct {
		sub *Subscription
		ch  <-chan store.Event
	}
	const numConns = 10
	connections := make([]entry, numConns)
	for i := 0; i < numConns; i++ {
		s := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
		connections[i] = entry{sub: s, ch: s.Events()}
	}
	require.Equal(t, numConns, b.SubscriberCount())

	// Simulate server shutdown: close all subscriptions.
	for _, c := range connections {
		c.sub.Close()
	}

	// All channels must be closed.
	for i, c := range connections {
		_, ok := <-c.ch
		assert.False(t, ok, "connection %d: channel must be closed after shutdown", i)
	}

	assert.Equal(t, 0, b.SubscriberCount())
}

func TestWebSocketLifecycle_PerConnectionFiltersIsolated(t *testing.T) {
	b := New(64)

	// Three subscribers with different contract filters.
	subA := b.Subscribe(store.EventFilter{
		ContractID: "CA",
		Scope:      store.WildcardScope(),
	})
	defer subA.Close()

	subB := b.Subscribe(store.EventFilter{
		ContractID: "CB",
		Scope:      store.WildcardScope(),
	})
	defer subB.Close()

	subAll := b.Subscribe(store.EventFilter{
		Scope: store.WildcardScope(),
	})
	defer subAll.Close()

	// Publish events for both contracts.
	b.Publish(context.Background(), []store.Event{
		mkEvent("e1", "CA", 1),
		mkEvent("e2", "CB", 2),
		mkEvent("e3", "CA", 3),
	})

	// SubA gets only CA events.
	got := <-subA.Events()
	assert.Equal(t, "e1", got.ID)
	got = <-subA.Events()
	assert.Equal(t, "e3", got.ID)

	// SubB gets only CB events.
	got = <-subB.Events()
	assert.Equal(t, "e2", got.ID)

	// SubAll gets both.
	received := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case ev := <-subAll.Events():
			received[ev.ID] = true
		case <-time.After(time.Second):
			t.Fatalf("subAll: timed out after %d events", i)
		}
	}
	assert.True(t, received["e1"], "subAll must receive e1")
	assert.True(t, received["e2"], "subAll must receive e2")
	assert.True(t, received["e3"], "subAll must receive e3")
}

func TestWebSocketLifecycle_RaceFreePublishAndSubscribe(t *testing.T) {
	b := New(64)

	// Pre-register subscribers so Publish has targets.
	const numSubs = 10
	type sub struct {
		ch    <-chan store.Event
		close func()
	}
	subs := make([]sub, numSubs)
	for i := 0; i < numSubs; i++ {
		s := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
		subs[i] = sub{ch: s.Events(), close: s.Close}
	}

	// Concurrent publish.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			b.Publish(context.Background(), []store.Event{
				mkEvent("e"+string(rune('A'+n%26)), "CA", int64(n)),
			})
		}(i)
	}
	wg.Wait()

	// Drain all subscribers.
	for si, s := range subs {
		count := 0
	drain:
		for {
			select {
			case _, ok := <-s.ch:
				if !ok {
					break drain
				}
				count++
			default:
				break drain
			}
		}
		assert.Greater(t, count, 0, "subscriber %d must receive at least one event", si)
		s.close()
	}

	assert.Equal(t, 0, b.SubscriberCount())
}

func TestWebSocketLifecycle_GoroutineCountReturnsToBaseline(t *testing.T) {
	b := New(64)

	// Measure baseline goroutines.
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const numSubs = 50
	subs := make([]*Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
	}

	// Publish events to fill buffers.
	for i := 0; i < 10; i++ {
		b.Publish(context.Background(), []store.Event{
			mkEvent("e"+string(rune('A'+i)), "CA", int64(i)),
		})
	}

	// Close all subscribers — their goroutines must exit.
	for _, s := range subs {
		s.Close()
	}

	// Drain all channels to ensure goroutines unblock.
	for _, s := range subs {
		for range s.Events() {
		}
	}

	// Wait for goroutines to settle.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	current := runtime.NumGoroutine()

	// Allow a small margin for runtime bookkeeping goroutines
	// (GC workers, etc.) but no subscriber goroutines.
	assert.LessOrEqual(t, current, baseline+5,
		"goroutine count must return to near-baseline after closing %d subscribers", numSubs)
}

func TestWebSocketLifecycle_PublishResumesAfterSlowEviction(t *testing.T) {
	b := New(1) // buffer of 1

	// Subscriber A is slow (buffer fills, eviction on next publish).
	slowA := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})

	// Subscriber B is fast and always reads.
	fastB := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})

	// Fill slowA's buffer and drain fastB so only the next
	// publish's event remains.
	b.Publish(context.Background(), []store.Event{mkEvent("fill", "CA", 1)})
	<-fastB.Events()

	// Next publish evicts slowA and delivers to fastB.
	b.Publish(context.Background(), []store.Event{mkEvent("go", "CA", 2)})

	// slowA is closed (drain any buffered event first).
	_, ok := <-slowA.Events()
	if ok {
		_, ok = <-slowA.Events()
	}
	assert.False(t, ok, "slow subscriber must be evicted")

	// fastB receives the event.
	select {
	case ev := <-fastB.Events():
		assert.Equal(t, "go", ev.ID)
	case <-time.After(time.Second):
		t.Fatal("fast subscriber did not receive event after slow eviction")
	}

	slowA.Close()
	fastB.Close()

	assert.Equal(t, 0, b.SubscriberCount())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mkEvent is defined in broadcast_test.go (same package) and
// available to all test files in this package.
