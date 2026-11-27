package broadcast

import (
	"context"
	"sync"
)

// Message represents a broadcast payload.
type Message struct {
	TenantID string
	Payload  any
}

// Subscriber represents a consumer channel with a gap indicator.
type Subscriber struct {
	Ch      chan Message
	Dropped int64
}

// Broker manages subscriptions and broadcasts with backpressure protection.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[string]map[*Subscriber]struct{}
	bufferSize  int
}

// NewBroker creates a new broker.
func NewBroker(bufferSize int) *Broker {
	return &Broker{
		subscribers: make(map[string]map[*Subscriber]struct{}),
		bufferSize:  bufferSize,
	}
}

// Close closes all subscriber channels and clears the subscriber map.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, subs := range b.subscribers {
		for sub := range subs {
			close(sub.Ch)
		}
	}
	b.subscribers = make(map[string]map[*Subscriber]struct{})
}

// Subscribe registers a subscriber for a given tenant.
func (b *Broker) Subscribe(tenantID string) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub := &Subscriber{
		Ch: make(chan Message, b.bufferSize),
	}

	if b.subscribers[tenantID] == nil {
		b.subscribers[tenantID] = make(map[*Subscriber]struct{})
	}
	b.subscribers[tenantID][sub] = struct{}{}
	return sub
}

// Unsubscribe removes a subscriber.
func (b *Broker) Unsubscribe(tenantID string, sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, ok := b.subscribers[tenantID]; ok {
		if _, exists := subs[sub]; exists {
			delete(subs, sub)
			close(sub.Ch)
		}
		if len(subs) == 0 {
			delete(b.subscribers, tenantID)
		}
	}
}

// Publish sends a message non-blockingly to all subscribers of the tenant.
func (b *Broker) Publish(ctx context.Context, msg Message) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs := b.subscribers[msg.TenantID]
	for sub := range subs {
		select {
		case sub.Ch <- msg:
		default:
			// Non-blocking drop with gap indicator tracking to prevent ingestion stall
			sub.Dropped++
		}
	}
}
