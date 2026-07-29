// Package broadcast provides a pub-sub mechanism for distributing ingested
// events to connected clients (SSE, WebSocket, etc.). It is the streaming
// counterpart of the store package's query interface.
package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/sorotrail/sorotrail/internal/store"
)

// DefaultBufferSize is the per-subscriber channel buffer. When a subscriber's
// channel is full the subscriber is evicted (slow-consumer policy).
const DefaultBufferSize = 64

// Broadcaster distributes events to subscribers whose filters match.
type Broadcaster struct {
	mu         sync.RWMutex
	subs       map[string]*Subscription
	bufferSize int
	nextID     atomic.Int64
}

// Subscription represents a single subscriber's connection to the event stream.
// The caller receives events on Events() and must call Close() when done.
type Subscription struct {
	id     string
	ch     chan store.Event
	filter store.EventFilter
	b      *Broadcaster
	once   sync.Once

	// scopeMu guards scope, which — unlike the rest of filter — changes
	// during the subscription's life. A long-lived stream must react to
	// grants and revocations that happen after it was opened; see SetScope.
	scopeMu sync.RWMutex
	scope   store.Scope
}

// New creates a Broadcaster. bufferSize is the per-subscriber channel
// capacity; a subscriber that falls behind gets evicted.
func New(bufferSize int) *Broadcaster {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &Broadcaster{
		subs:       make(map[string]*Subscription),
		bufferSize: bufferSize,
	}
}

// Subscribe registers a new subscriber with the given filter. The returned
// Subscription receives matching events on Events() until Close() is called.
//
// The filter's Scope is the subscriber's authorization boundary and is
// enforced on every dispatch, not merely at subscribe time. As everywhere
// else, its zero value grants nothing: a subscriber registered with a
// hand-built filter receives no events rather than all of them.
func (b *Broadcaster) Subscribe(filter store.EventFilter) *Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := fmt.Sprintf("sub-%d", b.nextID.Add(1))
	s := &Subscription{
		id:     id,
		ch:     make(chan store.Event, b.bufferSize),
		filter: filter,
		scope:  filter.Scope,
		b:      b,
	}
	b.subs[id] = s
	return s
}

// SetScope replaces the subscription's authorization boundary in place.
//
// This is how a stream handles a tenant's grants changing while it is open.
// Capturing the scope once at subscribe time would mean a revoked tenant
// keeps receiving a contract's events for as long as it holds the
// connection open — indefinitely — which makes revocation advisory rather
// than real. Re-resolving instead bounds the exposure to one sync interval,
// and does so symmetrically: a contract granted mid-stream starts flowing
// within the same interval without the client reconnecting.
func (s *Subscription) SetScope(sc store.Scope) {
	s.scopeMu.Lock()
	s.scope = sc
	s.scopeMu.Unlock()
}

// currentScope reads the live scope under the read lock.
func (s *Subscription) currentScope() store.Scope {
	s.scopeMu.RLock()
	defer s.scopeMu.RUnlock()
	return s.scope
}

func (b *Broadcaster) unsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[id]; ok {
		close(s.ch)
		delete(b.subs, id)
	}
}

// SubscriberCount returns the number of subscribers currently registered.
// Exposed primarily for tests that need to verify the subscription
// lifecycle (e.g. confirming that a handler's deferred sub.Close()
// actually ran on connection teardown), but also useful for operators
// who want to see how many live consumers the broadcaster is feeding.
func (b *Broadcaster) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Publish sends events to all subscribers whose filter matches. Slow
// consumers (full channel) are silently evicted.
func (b *Broadcaster) Publish(ctx context.Context, events []store.Event) {
	b.mu.RLock()
	subs := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.RUnlock()

	var evict []string
	for _, s := range subs {
		// Read the scope once per subscriber per publish rather than once
		// per event: it cannot change mid-batch in a way that matters, and
		// taking the lock per event would put it on the hot path.
		scope := s.currentScope()
		for _, ev := range events {
			// Authorization first, and independently of the user's filter,
			// so no filter expression can be crafted to bypass it.
			if !scope.Allows(ev.ContractID) {
				continue
			}
			if !eventMatches(ev, s.filter) {
				continue
			}
			select {
			case s.ch <- ev:
			default:
				evict = append(evict, s.id)
				goto nextSub
			}
		}
	nextSub:
	}
	if len(evict) > 0 {
		b.mu.Lock()
		for _, id := range evict {
			if s, ok := b.subs[id]; ok {
				close(s.ch)
				delete(b.subs, id)
			}
		}
		b.mu.Unlock()
	}
}

// Events returns a receive-only channel of events matching the subscriber's
// filter. The channel is closed when the subscription is terminated (either
// by the caller calling Close() or by the broadcaster evicting a slow
// consumer).
func (s *Subscription) Events() <-chan store.Event {
	return s.ch
}

// Close terminates the subscription. The subscriber will receive no more
// events.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.b.unsubscribe(s.id)
	})
}

// eventMatches reports whether an event satisfies the given filter.
// Zero-valued filter fields are treated as "no constraint".
func eventMatches(ev store.Event, f store.EventFilter) bool {
	if f.ContractID != "" && ev.ContractID != f.ContractID {
		return false
	}
	if len(f.Types) > 0 {
		ok := false
		for _, t := range f.Types {
			if ev.Type == t {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(f.Topic) > 0 {
		if !topicContains(ev.Topics, f.Topic) {
			return false
		}
	}
	if f.FromLedger > 0 && ev.Ledger < f.FromLedger {
		return false
	}
	if f.ToLedger > 0 && ev.Ledger > f.ToLedger {
		return false
	}
	if !f.FromTime.IsZero() && ev.CreatedAt.Before(f.FromTime) {
		return false
	}
	if !f.ToTime.IsZero() && ev.CreatedAt.After(f.ToTime) {
		return false
	}
	return true
}

// topicContains reports whether the topics JSON array contains the needle
// JSON value at any position (equivalent to Postgres's @> containment).
func topicContains(topics json.RawMessage, needle json.RawMessage) bool {
	var arr []json.RawMessage
	if err := json.Unmarshal(topics, &arr); err != nil {
		return false
	}
	for _, t := range arr {
		if jsonDeepEqual(t, needle) {
			return true
		}
	}
	return false
}

// jsonDeepEqual reports whether two JSON values are semantically equal.
func jsonDeepEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}
