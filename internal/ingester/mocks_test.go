package ingester

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// mockRPC scripts getEvents responses in order and records the requests it
// received.
type mockRPC struct {
	mu             sync.Mutex
	health         rpc.Health
	healthErr      error
	eventsResps    []rpc.GetEventsResponse
	eventsErrs     []error
	eventsRequests []rpc.GetEventsRequest
}

func (m *mockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventsRequests = append(m.eventsRequests, req)
	i := len(m.eventsRequests) - 1
	var err error
	if i < len(m.eventsErrs) {
		err = m.eventsErrs[i]
	}
	var resp rpc.GetEventsResponse
	if i < len(m.eventsResps) {
		resp = m.eventsResps[i]
	}
	return resp, err
}

func (m *mockRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *mockRPC) GetHealth(context.Context) (rpc.Health, error) {
	return m.health, m.healthErr
}

func (m *mockRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

// mockStore is an in-memory Store.
type mockStore struct {
	mu       sync.Mutex
	events   map[string]store.Event
	state    *store.IngestionState
	watched  []store.WatchedContract
	upserted [][]store.Event
}

func newMockStore() *mockStore {
	return &mockStore{events: map[string]store.Event{}}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upserted = append(m.upserted, events)
	var inserted int64
	for _, e := range events {
		if _, dup := m.events[e.ID]; !dup {
			m.events[e.ID] = e
			inserted++
		}
	}
	return inserted, nil
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

// EventExists is the cheap existence probe added to the Store interface
// for the API's 304 path. Unused by ingester tests but needed to
// satisfy the interface.
func (m *mockStore) EventExists(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.events[id]
	return ok, nil
}

func (m *mockStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) CountEvents(context.Context, store.EventFilter) (int64, error) {
	return 0, nil
}

// LedgerRangeCensus is unused by ingester tests but needed to satisfy
// the expanded store.Store interface.
func (m *mockStore) LedgerRangeCensus(context.Context, int64, int64, bool) ([]store.LedgerCensus, error) {
	return nil, nil
}

// GetAuditState / SaveAuditState are unused by ingester tests.
func (m *mockStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error {
	return nil
}
func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}

// Record/Update/ListOpenFindings are unused by ingester tests.
func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}
func (m *mockStore) UpdateAuditFinding(context.Context, store.AuditFinding) error {
	return nil
}
func (m *mockStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}

func (m *mockStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return store.IngestionState{}, store.ErrNotFound
	}
	return *m.state, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = &s
	return nil
}

func (m *mockStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
	return m.watched, nil
}

func (m *mockStore) AddWatchedContract(_ context.Context, id string) error {
	m.watched = append(m.watched, store.WatchedContract{ContractID: id})
	return nil
}

func (m *mockStore) RemoveWatchedContract(_ context.Context, id string) error {
	for i, wc := range m.watched {
		if wc.ContractID == id {
			m.watched = append(m.watched[:i], m.watched[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

func (m *mockStore) Stats(context.Context) (store.Stats, error) { return store.Stats{}, nil }
func (m *mockStore) Ping(context.Context) error                 { return nil }

func (m *mockStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (m *mockStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }

// Subscription stubs for the webhook feature.
func (m *mockStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (m *mockStore) GetSubscription(_ context.Context, id int64) (store.Subscription, error) {
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

// passthroughDecoder avoids XDR fixtures in ingester tests.
type passthroughDecoder struct{}

func (passthroughDecoder) DecodeScVal(string) (json.RawMessage, error) {
	return json.RawMessage(`"decoded"`), nil
}
