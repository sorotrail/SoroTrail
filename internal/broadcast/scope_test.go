package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	scopedA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	scopedB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func ev(id, contract string) store.Event {
	return store.Event{ID: id, ContractID: contract, Type: "contract"}
}

// receive drains one event, or reports that nothing arrived.
func receive(t *testing.T, sub *Subscription) (store.Event, bool) {
	t.Helper()
	select {
	case e, ok := <-sub.Events():
		return e, ok
	case <-time.After(200 * time.Millisecond):
		return store.Event{}, false
	}
}

// assertQuiet fails if anything is delivered within the window.
func assertQuiet(t *testing.T, sub *Subscription, msg string) {
	t.Helper()
	select {
	case e := <-sub.Events():
		t.Fatalf("%s: unexpectedly received %s (%s)", msg, e.ID, e.ContractID)
	case <-time.After(150 * time.Millisecond):
	}
}

// A subscriber receives only events for contracts its scope permits, even
// with a filter that would otherwise match everything.
func TestPublish_EnforcesScope(t *testing.T) {
	b := New(16)
	sub := b.Subscribe(store.EventFilter{Scope: store.NewScope([]string{scopedA})})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{
		ev("b1", scopedB),
		ev("a1", scopedA),
		ev("b2", scopedB),
	})

	got, ok := receive(t, sub)
	require.True(t, ok)
	assert.Equal(t, "a1", got.ID)
	assertQuiet(t, sub, "events outside the scope must not be delivered")
}

// The zero Scope on a hand-built filter must deliver nothing rather than
// everything — the streaming counterpart of the store's fail-closed read.
func TestPublish_UnscopedSubscriptionReceivesNothing(t *testing.T) {
	b := New(16)
	sub := b.Subscribe(store.EventFilter{})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{ev("a1", scopedA), ev("b1", scopedB)})

	assertQuiet(t, sub, "a subscription with no scope must receive nothing")
}

func TestPublish_WildcardScopeReceivesEverything(t *testing.T) {
	b := New(16)
	sub := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{ev("a1", scopedA), ev("b1", scopedB)})

	first, ok := receive(t, sub)
	require.True(t, ok)
	second, ok := receive(t, sub)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"a1", "b1"}, []string{first.ID, second.ID})
}

// The mid-stream cases the issue asks to define. A long-lived stream must
// track its tenant's grants in both directions: a snapshot taken at connect
// time would let a revoked tenant keep reading for as long as it stays
// connected, and would make a new grant invisible until it reconnected.
func TestSetScope_AppliesMidStream(t *testing.T) {
	t.Run("a grant added mid-stream starts flowing", func(t *testing.T) {
		b := New(16)
		sub := b.Subscribe(store.EventFilter{Scope: store.NewScope([]string{scopedA})})
		defer sub.Close()

		b.Publish(context.Background(), []store.Event{ev("b1", scopedB)})
		assertQuiet(t, sub, "not yet granted")

		sub.SetScope(store.NewScope([]string{scopedA, scopedB}))
		b.Publish(context.Background(), []store.Event{ev("b2", scopedB)})

		got, ok := receive(t, sub)
		require.True(t, ok)
		assert.Equal(t, "b2", got.ID)
	})

	t.Run("a revoked grant stops flowing", func(t *testing.T) {
		b := New(16)
		sub := b.Subscribe(store.EventFilter{Scope: store.NewScope([]string{scopedA, scopedB})})
		defer sub.Close()

		b.Publish(context.Background(), []store.Event{ev("b1", scopedB)})
		got, ok := receive(t, sub)
		require.True(t, ok)
		require.Equal(t, "b1", got.ID)

		sub.SetScope(store.NewScope([]string{scopedA}))
		b.Publish(context.Background(), []store.Event{ev("b2", scopedB)})

		assertQuiet(t, sub, "a revoked contract must stop being delivered")

		// The surviving grant is unaffected.
		b.Publish(context.Background(), []store.Event{ev("a1", scopedA)})
		got, ok = receive(t, sub)
		require.True(t, ok)
		assert.Equal(t, "a1", got.ID)
	})

	t.Run("clearing the scope silences the stream", func(t *testing.T) {
		b := New(16)
		sub := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
		defer sub.Close()

		sub.SetScope(store.Scope{})
		b.Publish(context.Background(), []store.Event{ev("a1", scopedA)})

		assertQuiet(t, sub, "a cleared scope must deliver nothing")
	})
}

// Two tenants streaming concurrently must not see each other's events.
func TestPublish_ConcurrentSubscribersAreIsolated(t *testing.T) {
	b := New(16)
	subA := b.Subscribe(store.EventFilter{Scope: store.NewScope([]string{scopedA})})
	defer subA.Close()
	subB := b.Subscribe(store.EventFilter{Scope: store.NewScope([]string{scopedB})})
	defer subB.Close()

	b.Publish(context.Background(), []store.Event{ev("a1", scopedA), ev("b1", scopedB)})

	gotA, ok := receive(t, subA)
	require.True(t, ok)
	assert.Equal(t, "a1", gotA.ID)
	assertQuiet(t, subA, "subscriber A must not receive B's events")

	gotB, ok := receive(t, subB)
	require.True(t, ok)
	assert.Equal(t, "b1", gotB.ID)
	assertQuiet(t, subB, "subscriber B must not receive A's events")
}

// A user filter cannot be crafted to widen the authorization: scope is
// evaluated independently of, and before, the filter.
func TestPublish_FilterCannotWidenScope(t *testing.T) {
	b := New(16)
	sub := b.Subscribe(store.EventFilter{
		ContractID: scopedB, // asking for a contract the scope excludes
		Scope:      store.NewScope([]string{scopedA}),
	})
	defer sub.Close()

	b.Publish(context.Background(), []store.Event{ev("b1", scopedB)})

	assertQuiet(t, sub, "naming a contract in the filter must not grant access to it")
}

// SetScope races against Publish in production (a sync goroutine writes
// while the broadcaster reads). Run under -race to make that meaningful.
func TestSetScope_IsRaceFree(t *testing.T) {
	b := New(64)
	sub := b.Subscribe(store.EventFilter{Scope: store.WildcardScope()})
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			if i%2 == 0 {
				sub.SetScope(store.NewScope([]string{scopedA}))
			} else {
				sub.SetScope(store.WildcardScope())
			}
		}
	}()
	for range 200 {
		b.Publish(context.Background(), []store.Event{ev("a1", scopedA)})
		// Keep the buffer from filling and evicting the subscriber.
		select {
		case <-sub.Events():
		default:
		}
	}
	<-done
}
