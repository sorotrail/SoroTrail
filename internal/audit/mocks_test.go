package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
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

func (m *mockRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

// mockStore is an in-memory implementation of store.Store good enough
// for the auditor. It mirrors and extends the ingester test mock so we
// don't import the ingester test package.
type mockStore struct {
	// Embedded so the mock keeps satisfying store.Store as the
	// interface grows; unstubbed methods panic if a test calls them.
	store.Store

	mu sync.Mutex

	events map[string]store.Event

	ingress *store.IngestionState
	audit   *store.AuditState
	watched []store.WatchedContract

	findings []store.AuditFinding
	nextFID  int64
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

// EventExists mirrors GetEvent but stops at presence — the auditor's
// interface compliance is enough for the cache layer's needs.
func (m *mockStore) EventExists(_ context.Context, id string, _ store.Scope) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.events[id]; !ok {
		return false, nil
	}
	return true, nil
}

func (m *mockStore) GetEvent(_ context.Context, id string, _ store.Scope) (store.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return store.Event{}, store.ErrNotFound
	}
	return e, nil
}

func (m *mockStore) GetEventsByTxHash(_ context.Context, txHash, excludeID string) ([]store.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Event
	for _, e := range m.events {
		if e.TxHash == txHash && e.ID != excludeID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *mockStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) CountEvents(context.Context, store.EventFilter) (int64, error) {
	return 0, nil
}

func (m *mockStore) AggregateEvents(context.Context, store.EventFilter, string) ([]store.AggregateBucket, error) {
	return nil, nil
}

func (m *mockStore) LedgerRangeCensus(_ context.Context, from, to int64, idsOnly bool) ([]store.LedgerCensus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byLedger := map[int64][]string{}
	for _, e := range m.events {
		if e.Ledger >= from && e.Ledger <= to {
			byLedger[e.Ledger] = append(byLedger[e.Ledger], e.ID)
		}
	}
	var out []store.LedgerCensus
	for l := from; l <= to; l++ {
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

func (m *mockStore) GetIngestionState(_ context.Context) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ingress.LastIngestedLedger <= 0 && m.ingress.LastCursor == "" {
		return store.IngestionState{}, store.ErrNotFound
	}
	return *m.ingress, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingress = &s
	return nil
}

func (m *mockStore) GetAuditState(_ context.Context, _ string) (store.AuditState, error) {
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

func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, _ string, ledger int64) (store.AuditState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ledger > m.audit.VerifiedThroughLedger {
		m.audit.VerifiedThroughLedger = ledger
	}
	return m.audit, nil
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

func (m *mockStore) GetContractCursor(_ context.Context, _ string) (store.ContractCursor, error) {
	return store.ContractCursor{}, store.ErrNotFound
}

func (m *mockStore) SaveContractCursor(_ context.Context, _ store.ContractCursor) error {
	return nil
}

func (m *mockStore) DeleteContractCursor(_ context.Context, _ string) error {
	return nil
}

func (m *mockStore) ListContractCursors(context.Context) ([]store.ContractCursor, error) {
	return nil, nil
}

func (m *mockStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextFID++
	f.ID = m.nextFID
	if f.Status == "" {
		f.Status = store.FindingOpen
	}
	m.findings = append(m.findings, f)
	return f, nil
}

func (m *mockStore) UpdateAuditFinding(_ context.Context, f store.AuditFinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.findings {
		if m.findings[i].ID == f.ID {
			m.findings[i] = f
			return nil
		}
	}
	return store.ErrNotFound
}

func (m *mockStore) ListOpenFindingsByRange(_ context.Context, _ string, from, to int64) (store.AuditFinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.findings) - 1; i >= 0; i-- {
		f := m.findings[i]
		if f.Status != store.FindingOpen && f.Status != store.FindingUnrecoverable {
			continue
		}
		if f.FromLedger <= to && f.ToLedger >= from {
			return f, nil
		}
	}
	return store.AuditFinding{}, store.ErrNotFound
}

func (m *mockStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s store.Stats
	s.TotalEvents = int64(len(m.events))
	return s, nil
}

func (m *mockStore) DeleteEventsBeforeLedger(context.Context, int64) (int64, error) {
	return 0, nil
}

func (m *mockStore) MigrationVersion(context.Context) (int, bool, error) {
	return 9, false, nil
}

func (m *mockStore) Ping(context.Context) error { return nil }
func (m *mockStore) ListContractIDs(context.Context) ([]string, error) {
	return nil, nil
}
func (m *mockStore) GetContractMeta(context.Context, string) (store.ContractMeta, error) {
	return store.ContractMeta{}, store.ErrNotFound
}
func (m *mockStore) UpsertContractMeta(context.Context, store.ContractMeta) error {
	return nil
}
func (m *mockStore) CountContractEvents(context.Context, string) (int64, error) {
	return 0, nil
}
func (m *mockStore) ListContractsNeedingRefresh(context.Context, time.Time) ([]string, error) {
	return nil, nil
}

func (m *mockStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (m *mockStore) GetSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (m *mockStore) ListSubscriptions(context.Context, store.SubscriptionOwner) ([]store.Subscription, error) {
	return nil, nil
}
func (m *mockStore) UpdateSubscription(_ context.Context, sub store.Subscription, _ store.SubscriptionOwner) (store.Subscription, error) {
	return sub, nil
}
func (m *mockStore) DeleteSubscription(context.Context, int64, store.SubscriptionOwner) error {
	return nil
}
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
func (m *mockStore) ListDeliveryAttempts(context.Context, int64, int, store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	return nil, nil
}

func (m *mockStore) GetEarliestLedger(ctx context.Context, network, contractID string) (int64, error) {
	return 0, nil
}

func (m *mockStore) ListContracts(context.Context, store.ContractsFilter) ([]store.ContractSummary, string, error) {
	return nil, "", nil
}
func (m *mockStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (m *mockStore) DeadLetterEvent(context.Context, store.DeadLetterInput) (store.DeadLetter, error) {
	return store.DeadLetter{}, nil
}
func (m *mockStore) ListDeadLetters(context.Context, string, int, string) ([]store.DeadLetter, string, error) {
	return nil, "", nil
}
func (m *mockStore) GetDeadLetter(context.Context, int64) (store.DeadLetter, error) {
	return store.DeadLetter{}, store.ErrNotFound
}
func (m *mockStore) DeleteDeadLetter(context.Context, int64) error { return nil }

func (m *mockStore) UpsertAddressRefs(context.Context, []store.AddressRef) error { return nil }
func (m *mockStore) QueryAddressEvents(context.Context, string, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}
func (m *mockStore) CountAddressEvents(context.Context, string) (int64, error) { return 0, nil }
func (m *mockStore) GetAddressSummary(context.Context, string) (store.AddressSummary, error) {
	return store.AddressSummary{}, nil
}
