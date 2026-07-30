package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mkEvents creates n rpc.Event for the given ledger with the given contract.
func mkEvents(ledger uint32, n int, contractID string) []rpc.Event {
	events := make([]rpc.Event, n)
	for i := 0; i < n; i++ {
		events[i] = rpc.Event{
			ID:         fmt.Sprintf("%020d-%05d", ledger, i),
			Ledger:     ledger,
			ContractID: contractID,
			Type:       "contract",
			Topic:      []string{"AAAAAA=="},
			Value:      "AAAAAA==",
			TopicJSON:  []json.RawMessage{json.RawMessage(`{"symbol":"test"}`)},
			ValueJSON:  json.RawMessage(`{"u64":1}`),
		}
	}
	return events
}

type mockRPC struct {
	mu             sync.Mutex
	muAudit        sync.Mutex
	health         rpc.Health
	extraResponses func(callIdx int) (rpc.GetEventsResponse, error)
	eventsResps    []rpc.GetEventsResponse
	eventsRequests []rpc.GetEventsRequest
}

func (m *mockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventsRequests = append(m.eventsRequests, req)
	if m.extraResponses != nil {
		return m.extraResponses(len(m.eventsRequests) - 1)
	}
	return rpc.GetEventsResponse{}, nil
}

func (m *mockRPC) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *mockRPC) GetHealth(_ context.Context) (rpc.Health, error) {
	return m.health, nil
}

func (m *mockRPC) GetLedgerEntries(_ context.Context, _ rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

type mockStore struct {
	mu       sync.Mutex
	events   map[string]store.Event
	ingress  store.IngestionState
	audit    store.AuditState
	watched  []string
	findings []store.AuditFinding
}

func newMockStore() *mockStore {
	return &mockStore{events: map[string]store.Event{}}
}

// seedLedgers creates one event per ledger with the given contract ID.
func (m *mockStore) seedLedgers(ledgers []int, contractID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ledger := range ledgers {
		id := fmt.Sprintf("%020d-%05d", ledger, 0)
		m.events[id] = store.Event{
			ID:         id,
			ContractID: contractID,
			Ledger:     int64(ledger),
			Type:       "contract",
			Topics:     json.RawMessage(`[{"symbol":"test"}]`),
			Value:      json.RawMessage(`{"u64":1}`),
		}
	}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, e := range events {
		if _, ok := m.events[e.ID]; !ok {
			n++
		}
		m.events[e.ID] = e
	}
	return n, nil
}

func (m *mockStore) ReplaceEventsInRange(_ context.Context, events []store.Event, fromLedger, toLedger int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, e := range m.events {
		if e.Ledger >= fromLedger && e.Ledger <= toLedger {
			delete(m.events, id)
		}
	}
	for _, e := range events {
		m.events[e.ID] = e
	}
	return nil
}

func (m *mockStore) GetEvent(_ context.Context, id string) (store.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return store.Event{}, store.ErrNotFound
	}
	return e, nil
}

func (m *mockStore) EventExists(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.events[id]
	return ok, nil
}

func (m *mockStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]store.LedgerCensus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byLedger := map[int64][]string{}
	for _, e := range m.events {
		if e.Ledger >= fromLedger && e.Ledger <= toLedger {
			byLedger[e.Ledger] = append(byLedger[e.Ledger], e.ID)
		}
	}
	var out []store.LedgerCensus
	for l := fromLedger; l <= toLedger; l++ {
		ids := byLedger[l]
		if len(ids) == 0 {
			continue
		}
		c := store.LedgerCensus{Ledger: l, Count: len(ids)}
		if idsOnly {
			c.IDs = ids
		}
		out = append(out, c)
	}
	return out, nil
}

func (m *mockStore) GetIngestionState(_ context.Context, network string) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ingress.LastIngestedLedger <= 0 && m.ingress.LastCursor == "" {
		return store.IngestionState{}, store.ErrNotFound
	}
	return m.ingress, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingress = s
	return nil
}

func (m *mockStore) GetAuditState(_ context.Context, network string) (store.AuditState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.audit, nil
}

func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = s
	return nil
}

func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, network string, ledger int64) (store.AuditState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ledger > m.audit.VerifiedThroughLedger {
		m.audit.VerifiedThroughLedger = ledger
	}
	return m.audit, nil
}

func (m *mockStore) ListWatchedContracts(context.Context) ([]string, error) {
	return m.watched, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, id string) error {
	m.watched = append(m.watched, id)
	return nil
}

func (m *mockStore) Stats(context.Context) (store.Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s store.Stats
	s.TotalEvents = int64(len(m.events))
	return s, nil
}

func (m *mockStore) Ping(context.Context) error { return nil }

func (m *mockStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (m *mockStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }

func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f.ID = int64(len(m.findings) + 1)
	m.findings = append(m.findings, f)
	return f, nil
}
func (m *mockStore) UpdateAuditFinding(_ context.Context, f store.AuditFinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.findings {
		if existing.ID == f.ID {
			m.findings[i] = f
			return nil
		}
	}
	return nil
}
func (m *mockStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}

func (m *mockStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (m *mockStore) GetSubscription(context.Context, int64) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (m *mockStore) ListSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (m *mockStore) UpdateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	return sub, nil
}
func (m *mockStore) DeleteSubscription(context.Context, int64) error { return nil }
func (m *mockStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (m *mockStore) IncrementSubscriptionFailures(context.Context, int64, int) (int, bool, error) {
	return 0, false, nil
}
func (m *mockStore) ResetSubscriptionFailures(context.Context, int64) error { return nil }
func (m *mockStore) RecordDeliveryAttempt(_ context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	a.ID = 1
	return a, nil
}
func (m *mockStore) ListDeliveryAttempts(context.Context, int64, int) ([]store.DeliveryAttempt, error) {
	return nil, nil
}
