// Package api serves stored events over HTTP, including a Server-Sent Events
// subscription endpoint that pushes newly ingested events to clients in
// real time.
package api

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/sorotrail/sorotrail/internal/store"
)

// subscriberBufferSize bounds the event channel per subscriber. When a
// subscriber's buffer is full, publishes to it are dropped (non-blocking).
const subscriberBufferSize = 256

// Broker fans out newly ingested events to subscribers with matching filters.
// It is the seam between the ingester (which calls Publish) and the SSE
// endpoint (which calls Subscribe). A nil *Broker is safe: all methods no-op.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[int64]*subscriber
	nextID      int64
}

type subscriber struct {
	id     int64
	filter store.EventFilter
	ch     chan store.Event
}

// NewBroker creates a Broker ready for use.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[int64]*subscriber),
	}
}

// Subscribe registers a filter and returns a receive-only channel of
// store.Event values plus a cancel function that removes the subscriber.
// The channel has a bounded buffer; when it overflows, the subscriber is
// dropped and the channel is closed. Callers must call cancel() to clean up.
func (b *Broker) Subscribe(filter store.EventFilter) (<-chan store.Event, func()) {
	if b == nil {
		ch := make(chan store.Event)
		close(ch)
		return ch, func() {}
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++
	sub := &subscriber{
		id:     id,
		filter: filter,
		ch:     make(chan store.Event, subscriberBufferSize),
	}
	b.subscribers[id] = sub

	cancel := func() {
		if b == nil {
			return
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if s, ok := b.subscribers[id]; ok {
			close(s.ch)
			delete(b.subscribers, id)
		}
	}
	return sub.ch, cancel
}

// Publish sends events to all subscribers whose filters match. Sends are
// non-blocking: a subscriber whose buffer is full is dropped (its channel is
// closed and it is removed). Publish runs in the ingester goroutine and must
// return quickly — it never blocks on a slow consumer.
func (b *Broker) Publish(events []store.Event) {
	if b == nil || len(events) == 0 {
		return
	}
	b.mu.RLock()
	// Snapshot subscribers under the read lock, then publish outside it so a
	// slow send doesn't stall new subscriptions.
	subs := make(map[int64]*subscriber, len(b.subscribers))
	for id, s := range b.subscribers {
		subs[id] = s
	}
	b.mu.RUnlock()

	for _, s := range subs {
		for _, e := range events {
			if !eventMatches(s.filter, e) {
				continue
			}
			select {
			case s.ch <- e:
			default:
				// Buffer full: drop this subscriber.
				b.mu.Lock()
				if existing, ok := b.subscribers[s.id]; ok && existing == s {
					close(s.ch)
					delete(b.subscribers, s.id)
				}
				b.mu.Unlock()
				goto nextSub // break out of the inner event loop
			}
		}
	nextSub:
	}
}

// Shutdown closes all subscriber channels and removes them.
func (b *Broker) Shutdown() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, s := range b.subscribers {
		close(s.ch)
		delete(b.subscribers, id)
	}
}

// eventMatches reports whether e satisfies every constraint in f. Zero-value
// fields mean "unconstrained".
// containsType reports whether want appears in types.
func containsType(types []string, want string) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

func eventMatches(f store.EventFilter, e store.Event) bool {
	if f.ContractID != "" && e.ContractID != f.ContractID {
		return false
	}
	if len(f.Types) > 0 && !containsType(f.Types, e.Type) {
		return false
	}
	if f.FromLedger > 0 && e.Ledger < f.FromLedger {
		return false
	}
	if f.ToLedger > 0 && e.Ledger > f.ToLedger {
		return false
	}
	if len(f.Topic) > 0 {
		// Topics is a JSON array; check whether f.Topic (a single JSON value)
		// appears anywhere in it via simple JSON substring containment.
		if !topicMatches(e.Topics, f.Topic) {
			return false
		}
	}
	return true
}

// topicMatches reports whether a JSON topic value is contained in a JSON
// topics array. Both sides are compacted so whitespace differences from
// user-supplied query params don't cause mismatches.
func topicMatches(topics, topic json.RawMessage) bool {
	if len(topics) == 0 || len(topic) == 0 {
		return false
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(topics, &arr); err != nil {
		return false
	}
	var buf bytes.Buffer
	_ = json.Compact(&buf, topic)
	needle := buf.String()
	for _, el := range arr {
		buf.Reset()
		_ = json.Compact(&buf, el)
		if buf.String() == needle {
			return true
		}
	}
	return false
}
