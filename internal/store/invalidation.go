package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// SpecInvalidationChannel is the Postgres LISTEN/NOTIFY channel that carries
// cross-replica cache invalidations.
//
// Converging every security- and correctness-relevant cache onto a single
// channel avoids holding multiple dedicated LISTEN connections per replica
// while guaranteeing that spec overrides, tenant suspensions/grants, API key
// revocations, and persisted GraphQL allowlist updates fan out through one
// mechanism.
//
// Payloads are either a bare contract ID (for backward compatibility with
// spec-only publishers) or a kind-prefixed string "<kind>:<target>":
//   - "spec:<contract_id>" (or bare "<contract_id>")
//   - "tenant:<tenant_id>"
//   - "apikey:<key_id>"
//   - "persisted:<operation_id_or_*>"
const SpecInvalidationChannel = "sorotrail_spec_invalidation"

// Invalidation event kinds carried over SpecInvalidationChannel.
const (
	InvalidationKindSpec      = "spec"
	InvalidationKindTenant    = "tenant"
	InvalidationKindAPIKey    = "apikey"
	InvalidationKindPersisted = "persisted"
)

// InvalidationEvent is a parsed cross-replica cache invalidation message.
type InvalidationEvent struct {
	Kind   string
	Target string
}

// FormatInvalidationPayload encodes kind and target into a wire payload
// suitable for Postgres NOTIFY (<= 8000 bytes). For InvalidationKindSpec, a
// bare contractID is emitted so legacy spec-only listeners continue to
// receive the exact contract ID they expect.
func FormatInvalidationPayload(kind, target string) string {
	switch kind {
	case "", InvalidationKindSpec:
		return target
	default:
		return kind + ":" + target
	}
}

// ParseInvalidationPayload decodes a NOTIFY payload into its kind and target.
// Payloads without a recognized "<kind>:" prefix are treated as contract spec
// invalidations so bare contract IDs remain 100% compatible.
func ParseInvalidationPayload(payload string) InvalidationEvent {
	if idx := strings.IndexByte(payload, ':'); idx > 0 {
		prefix := payload[:idx]
		rest := payload[idx+1:]
		switch prefix {
		case InvalidationKindSpec, InvalidationKindTenant, InvalidationKindAPIKey, InvalidationKindPersisted:
			return InvalidationEvent{Kind: prefix, Target: rest}
		}
	}
	return InvalidationEvent{
		Kind:   InvalidationKindSpec,
		Target: payload,
	}
}

// SpecInvalidationPublisher publishes a contract ID (or formatted invalidation
// payload) whose cached state must be evicted in every replica.
//
// The spec package defines its own equivalent interface so it does not have
// to depend on this package's concrete types.
type SpecInvalidationPublisher interface {
	PublishSpecInvalidation(ctx context.Context, contractID string) error
}

// InvalidationPublisher publishes a typed cross-replica cache invalidation.
type InvalidationPublisher interface {
	SpecInvalidationPublisher
	PublishInvalidation(ctx context.Context, kind, target string) error
}

// PublishSpecInvalidation notifies every other replica that the contract's
// cached spec (and its override) must be evicted, or delivers a typed
// invalidation payload on SpecInvalidationChannel.
//
// The payload carries only public or internal numeric identifiers (never
// secrets or key hashes), keeping it safe and well within the 8000-byte
// NOTIFY limit.
func (p *Postgres) PublishSpecInvalidation(ctx context.Context, contractID string) error {
	_, err := p.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, SpecInvalidationChannel, contractID)
	if err != nil {
		return fmt.Errorf("publishing spec invalidation for %s: %w", contractID, err)
	}
	return nil
}

// PublishInvalidation notifies every other replica of a cache invalidation
// of the given kind and target over SpecInvalidationChannel.
func (p *Postgres) PublishInvalidation(ctx context.Context, kind, target string) error {
	payload := FormatInvalidationPayload(kind, target)
	if err := p.PublishSpecInvalidation(ctx, payload); err != nil {
		return fmt.Errorf("publishing %s invalidation for %s: %w", kind, target, err)
	}
	return nil
}

// InvalidationListenerStats reports cumulative observability counters for a
// SpecInvalidationListener so missed-invalidation windows (disconnects and
// reconnect flushes) are visible to operators and tests.
type InvalidationListenerStats struct {
	NotificationsReceived uint64 `json:"notifications_received"`
	DisconnectErrors      uint64 `json:"disconnect_errors"`
	Reconnects            uint64 `json:"reconnects"`
}

// SpecInvalidationListener subscribes to the cross-replica invalidation channel.
//
// The subscription uses a dedicated connection rather than one from the
// pool: a LISTEN blocks its connection until a notification arrives, so
// borrowing a pooled connection would hold it for the life of the process.
type SpecInvalidationListener struct {
	// DatabaseURL is the connection string the listener dials.
	DatabaseURL string
	// OnInvalidation is called for every contract spec invalidation
	// received. It runs on the listener's goroutine, so it must not block
	// for long.
	OnInvalidation func(contractID string)
	// OnEvent is called for every invalidation event of any kind (spec,
	// tenant, apikey, persisted). Optional.
	OnEvent func(ev InvalidationEvent)
	// OnReconnect is called after every successful (re)subscription. A
	// reconnect may have missed notifications, so caches are cleared.
	OnReconnect func()
	// OnDisconnect is called when an active subscription breaks so
	// callers can record or alert on the missed-invalidation window.
	OnDisconnect func(err error)
	// Logger receives the listener's diagnostics. Optional.
	Logger *slog.Logger
	// Backoff is the initial delay before a reconnect attempt. It doubles
	// up to maxBackoff.
	Backoff time.Duration
	// MaxBackoff caps the reconnect delay.
	MaxBackoff time.Duration

	notificationsReceived atomic.Uint64
	disconnectErrors      atomic.Uint64
	reconnects            atomic.Uint64
}

// listenBackoff is the initial reconnect delay, and the floor the backoff
// grows from. A dropped connection usually means a database restart, so the
// listener retries quickly at first and then settles into a slower poll.
const (
	listenBackoff    = 100 * time.Millisecond
	listenMaxBackoff = 5 * time.Second
)

// Stats returns a snapshot of the listener's telemetry counters.
func (l *SpecInvalidationListener) Stats() InvalidationListenerStats {
	return InvalidationListenerStats{
		NotificationsReceived: l.notificationsReceived.Load(),
		DisconnectErrors:      l.disconnectErrors.Load(),
		Reconnects:            l.reconnects.Load(),
	}
}

// DeliverPayload parses payload and dispatches it to OnEvent and (for spec
// invalidations) OnInvalidation, incrementing the received counter.
func (l *SpecInvalidationListener) DeliverPayload(payload string) {
	l.notificationsReceived.Add(1)
	ev := ParseInvalidationPayload(payload)
	if l.OnEvent != nil {
		l.OnEvent(ev)
	}
	if ev.Kind == InvalidationKindSpec && l.OnInvalidation != nil {
		l.OnInvalidation(ev.Target)
	}
}

// RecordDisconnect logs and counts a broken invalidation subscription so a
// replica that misses invalidation messages never does so silently.
func (l *SpecInvalidationListener) RecordDisconnect(err error, backoff time.Duration) {
	count := l.disconnectErrors.Add(1)
	l.logger().Warn("spec invalidation listener disconnected; reconnecting",
		"channel", SpecInvalidationChannel,
		"error", err,
		"backoff", backoff,
		"disconnect_errors", count,
	)
	if l.OnDisconnect != nil {
		l.OnDisconnect(err)
	}
}

// RecordReconnect counts a (re)subscription and invokes OnReconnect so the
// replica flushes any state that may have gone stale while disconnected.
func (l *SpecInvalidationListener) RecordReconnect() {
	count := l.reconnects.Add(1)
	l.logger().Debug("subscribed to the spec invalidation channel",
		"channel", SpecInvalidationChannel,
		"reconnect_count", count,
	)
	if l.OnReconnect != nil {
		l.OnReconnect()
	}
}

// Run subscribes to the invalidation channel and delivers every notification
// to the listener's callbacks. It blocks until ctx is done.
//
// A dropped connection is not fatal: the listener reconnects with an
// exponential backoff and, after every successful (re)subscription, calls
// OnReconnect — because a reconnect may have missed notifications, the
// subscriber is expected to clear its whole cache at that point.
func (l *SpecInvalidationListener) Run(ctx context.Context) error {
	backoff := l.Backoff
	if backoff <= 0 {
		backoff = listenBackoff
	}
	maxBackoff := l.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = listenMaxBackoff
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := l.listenOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.RecordDisconnect(err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// listenOnce holds one subscription open until it fails or ctx is done.
func (l *SpecInvalidationListener) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, l.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting for LISTEN: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if _, err := conn.Exec(ctx, "LISTEN "+SpecInvalidationChannel); err != nil {
		return fmt.Errorf("subscribing to %s: %w", SpecInvalidationChannel, err)
	}

	// A (re)subscription may have missed notifications, so the subscriber
	// is told to clear everything it holds.
	l.RecordReconnect()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for a notification: %w", err)
		}
		l.DeliverPayload(notification.Payload)
	}
}

// logger returns the listener's logger, or the default one.
func (l *SpecInvalidationListener) logger() *slog.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return slog.Default()
}

// InMemoryInvalidationBus fans invalidation payloads out across multiple
// in-process SpecInvalidationListener instances. It implements
// InvalidationPublisher and SpecInvalidationPublisher so multi-replica tests
// and embedded deployments without a Postgres LISTEN connection use the exact
// same payload format and listener callbacks as production.
type InMemoryInvalidationBus struct {
	mu        sync.RWMutex
	nextID    int
	listeners map[int]*inMemorySubscriber
	failErr   error
}

type inMemorySubscriber struct {
	listener  *SpecInvalidationListener
	connected bool
}

// NewInMemoryInvalidationBus creates a fan-out bus for cross-replica
// invalidation testing and in-memory stores.
func NewInMemoryInvalidationBus() *InMemoryInvalidationBus {
	return &InMemoryInvalidationBus{
		listeners: make(map[int]*inMemorySubscriber),
	}
}

// Attach registers a SpecInvalidationListener with the bus, invokes its
// initial RecordReconnect (mirroring listenOnce), and returns its subscriber
// ID so tests can simulate disconnects and reconnects.
func (b *InMemoryInvalidationBus) Attach(l *SpecInvalidationListener) int {
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	b.listeners[id] = &inMemorySubscriber{
		listener:  l,
		connected: true,
	}
	b.mu.Unlock()
	l.RecordReconnect()
	return id
}

// SetConnected simulates a replica's LISTEN connection dropping
// (connected=false) or recovering (connected=true). While disconnected,
// published notifications are missed by that replica and counted via
// RecordDisconnect; on reconnect, RecordReconnect flushes its caches.
func (b *InMemoryInvalidationBus) SetConnected(id int, connected bool, disconnectErr error) {
	b.mu.Lock()
	sub, ok := b.listeners[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	wasConnected := sub.connected
	sub.connected = connected
	l := sub.listener
	b.mu.Unlock()

	if wasConnected && !connected {
		if disconnectErr == nil {
			disconnectErr = fmt.Errorf("simulated listener disconnect")
		}
		l.RecordDisconnect(disconnectErr, listenBackoff)
	} else if !wasConnected && connected {
		l.RecordReconnect()
	}
}

// SetPublishError forces PublishSpecInvalidation / PublishInvalidation to
// fail with err (or clears the failure when err is nil) so callers can test
// publish-failure logging and metrics.
func (b *InMemoryInvalidationBus) SetPublishError(err error) {
	b.mu.Lock()
	b.failErr = err
	b.mu.Unlock()
}

// PublishSpecInvalidation delivers payload to every connected subscriber.
func (b *InMemoryInvalidationBus) PublishSpecInvalidation(_ context.Context, payload string) error {
	b.mu.RLock()
	if b.failErr != nil {
		err := b.failErr
		b.mu.RUnlock()
		return err
	}
	subs := make([]*SpecInvalidationListener, 0, len(b.listeners))
	for _, s := range b.listeners {
		if s.connected && s.listener != nil {
			subs = append(subs, s.listener)
		}
	}
	b.mu.RUnlock()

	for _, l := range subs {
		l.DeliverPayload(payload)
	}
	return nil
}

// PublishInvalidation formats and publishes a typed invalidation event.
func (b *InMemoryInvalidationBus) PublishInvalidation(ctx context.Context, kind, target string) error {
	return b.PublishSpecInvalidation(ctx, FormatInvalidationPayload(kind, target))
}
