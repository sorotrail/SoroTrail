package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// scriptedRPC scripts getEvents responses by the cursor value the
// request carries. The key "" (empty cursor) is the first-call response —
// the one for a cold start or a warm resume with no pagination cursor yet.
// Subsequent pages are keyed by the Cursor they expect to see on the
// inbound request, mirroring how the ingester's singlePage / windowSweep
// resume pagination: the Cursor field on page N is the cursor the
// ingester sends for page N+1.
//
// Use it in tests that need to express a deterministic N-page sequence
// in a single literal, rather than threading an eventsResps slice and
// counting call indices. The map shape also documents the resume
// relationship at a glance — every page key (except the first) is a
// Cursor value some prior page returned.
//
// Calls are recorded in order; an error is returned for unknown
// cursors so a missed entry surfaces as a test failure rather than a
// silent empty page that would pass.
type scriptedRPC struct {
	mu     sync.Mutex
	health rpc.Health
	// pages is the cursor → response mapping. The "" key answers the
	// first call.
	pages map[string]rpc.GetEventsResponse
	// calls records every GetEvents request received, in order.
	calls []rpc.GetEventsRequest
}

func newScriptedRPC(pages map[string]rpc.GetEventsResponse) *scriptedRPC {
	return &scriptedRPC{pages: pages}
}

func (m *scriptedRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
	m.calls = append(m.calls, req)
	var cursor string
	if req.Pagination != nil {
		cursor = req.Pagination.Cursor
	}
	resp, ok := m.pages[cursor]
	m.mu.Unlock()
	if !ok {
		return rpc.GetEventsResponse{}, fmt.Errorf("scriptedRPC: no page scripted for cursor %q", cursor)
	}
	return resp, nil
}

func (m *scriptedRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *scriptedRPC) GetHealth(context.Context) (rpc.Health, error) {
	return m.health, nil
}

func (m *scriptedRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

// mockRPC scripts getEvents responses in order and records the requests it
// received.
type mockRPC struct {
	mu             sync.Mutex
	health         rpc.Health
	healthErr      error
	eventsResps    []rpc.GetEventsResponse
	eventsErrs     []error
	eventsRequests []rpc.GetEventsRequest
	// firstCycle, when non-nil, receives on the FIRST GetEvents call.
	// Tests that need a "first cycle completed" signal without reading
	// eventsRequests directly (which would race with the writer under
	// -race) pass a buffered channel here.
	firstCycle chan struct{}
}

func (m *mockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
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
	firstCycle := m.firstCycle
	m.mu.Unlock()
	if firstCycle != nil && i == 0 {
		select {
		case firstCycle <- struct{}{}:
		default:
		}
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
	// Embedded so the mock keeps satisfying store.Store as the
	// interface grows; unstubbed methods panic if a test calls them.
	store.Store

	mu          sync.Mutex
	events      map[string]store.Event
	state       *store.IngestionState
	watched     []store.WatchedContract
	upserted    [][]store.Event
	deadLetters []store.DeadLetterInput
	// ingestErr, when set, is returned by GetIngestionState so tests can
	// exercise the ingester's error path.
	ingestErr error
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

// EventExists is the cheap existence probe added to the Store interface
// for the API's 304 path. Unused by ingester tests but needed to
// satisfy the interface.
func (m *mockStore) EventExists(_ context.Context, id string, _ store.Scope) (bool, error) {
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
	if m.ingestErr != nil {
		return store.IngestionState{}, m.ingestErr
	}
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

func (m *mockStore) DeleteEventsBeforeLedger(context.Context, int64) (int64, error) {
	return 0, nil
}

func (m *mockStore) MigrationVersion(context.Context) (int, bool, error) {
	return 9, false, nil
}

func (m *mockStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}
func (m *mockStore) Ping(context.Context) error { return nil }

func (m *mockStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (m *mockStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }

// Subscription stubs for the webhook feature.
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

func (m *mockStore) ListContracts(context.Context, store.ContractsFilter) ([]store.ContractSummary, string, error) {
	return nil, "", nil
}
func (m *mockStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (m *mockStore) DeadLetterEvent(_ context.Context, in store.DeadLetterInput) (store.DeadLetter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadLetters = append(m.deadLetters, in)
	return store.DeadLetter{ID: int64(len(m.deadLetters)), EventID: in.EventID, ContractID: in.ContractID, Ledger: in.Ledger, Type: in.Type, TxHash: in.TxHash, TopicXDR: in.TopicXDR, ValueXDR: in.ValueXDR, Error: in.Err.Error()}, nil
}
func (m *mockStore) ListDeadLetters(context.Context, string, int, string) ([]store.DeadLetter, string, error) {
	return nil, "", nil
}
func (m *mockStore) GetDeadLetter(context.Context, int64) (store.DeadLetter, error) {
	return store.DeadLetter{}, store.ErrNotFound
}
func (m *mockStore) DeleteDeadLetter(context.Context, int64) error { return nil }

// passthroughDecoder avoids XDR fixtures in ingester tests.
type passthroughDecoder struct{}

func (passthroughDecoder) DecodeScVal(string) (json.RawMessage, error) {
	return json.RawMessage(`"decoded"`), nil
}

// DeleteEventsBefore satisfies store.Store; this mock never prunes.
func (m *mockStore) DeleteEventsBefore(context.Context, int64, time.Time, int) (int64, error) {
	return 0, nil
}

func (m *mockStore) UpsertAddressRefs(context.Context, []store.AddressRef) error { return nil }
func (m *mockStore) QueryAddressEvents(context.Context, string, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}
func (m *mockStore) CountAddressEvents(context.Context, string) (int64, error) { return 0, nil }
func (m *mockStore) GetAddressSummary(context.Context, string) (store.AddressSummary, error) {
	return store.AddressSummary{}, nil
}
