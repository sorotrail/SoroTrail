package audit

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// testLogger returns a slog.Logger that drops everything.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockRPC scripts getEvents/getHealth responses in order and records every
// request. It also exposes helper methods for tests to swap replays,
// simulate RPC self-disagreement, etc.
type mockRPC struct {
	mu sync.Mutex

	health     rpc.Health
	healthErrs []error

	// eventsResps[i] is returned to the i-th getEvents call. Cycle via
	// extraResponses when a test doesn't know up-front how many pages.
	eventsResps    []rpc.GetEventsResponse
	eventsRespsErr []error
	eventsRequests []rpc.GetEventsRequest
	extraResponses func(callIdx int) (rpc.GetEventsResponse, error)
}

func (m *mockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventsRequests = append(m.eventsRequests, req)
	idx := len(m.eventsRequests) - 1

	// Apply the request's filters to the scripted response so tests can
	// exercise filter parity the way a real RPC would: only events that
	// match at least one filter survive. Both filter.Type and event.Type
	// are treated as "match any type" when unset, mirroring production
	// behavior.
	filterEvents := func(evs []rpc.Event) []rpc.Event {
		if len(req.Filters) == 0 {
			return evs
		}
		out := evs[:0:0]
		for _, e := range evs {
			for _, f := range req.Filters {
				if f.Type != "" && e.Type != "" && f.Type != e.Type {
					continue
				}
				if len(f.ContractIDs) > 0 {
					ok := false
					for _, cid := range f.ContractIDs {
						if cid == e.ContractID {
							ok = true
							break
						}
					}
					if !ok {
						continue
					}
				}
				out = append(out, e)
				break
			}
		}
		return out
	}

	if m.extraResponses != nil {
		resp, err := m.extraResponses(idx)
		if err != nil {
			return resp, err
		}
		resp.Events = filterEvents(resp.Events)
		return resp, nil
	}
	var resp rpc.GetEventsResponse
	if idx < len(m.eventsResps) {
		resp = m.eventsResps[idx]
	}
	resp.Events = filterEvents(resp.Events)
	var err error
	if idx < len(m.eventsRespsErr) {
		err = m.eventsRespsErr[idx]
	}
	return resp, err
}

func (m *mockRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *mockRPC) GetHealth(context.Context) (rpc.Health, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.healthErrs) > 0 {
		err := m.healthErrs[0]
		m.healthErrs = m.healthErrs[1:]
		return m.health, err
	}
	return m.health, nil
}

func (m *mockRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

// mockStore is an in-memory implementation of store.Store good enough
// for the auditor. It mirrors and extends the ingester test mock so we
// don't import the ingester test package.
type mockStore struct {
	mu sync.Mutex

	events map[string]store.Event

	ingestionState *store.IngestionState
	auditState     *store.AuditState

	findings []store.AuditFinding
	nextFID  int64

	watched []store.WatchedContract

	// ledgers lets assertions check whether a finding record was written.
	ledgerCensusCalls []struct {
		From, To int64
		IDsOnly  bool
	}
	replaceCalls []struct {
		From, To int64
		Count    int
	}
}

func newMockStore() *mockStore {
	return &mockStore{events: map[string]store.Event{}}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var inserted int64
	for _, e := range events {
		if _, dup := m.events[e.ID]; !dup {
			m.events[e.ID] = e
			inserted++
		}
	}
	return inserted, nil
}

func (m *mockStore) ReplaceEventsInRange(_ context.Context, events []store.Event, from, to int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaceCalls = append(m.replaceCalls, struct {
		From, To int64
		Count    int
	}{from, to, len(events)})
	for id, e := range m.events {
		if e.Ledger >= from && e.Ledger <= to {
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
func (m *mockStore) EventExists(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.events[id]; !ok {
		return false, nil
	}
	return true, nil
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

func (m *mockStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) CountEvents(context.Context, store.EventFilter) (int64, error) {
	return 0, nil
}

func (m *mockStore) LedgerRangeCensus(_ context.Context, from, to int64, idsOnly bool) ([]store.LedgerCensus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ledgerCensusCalls = append(m.ledgerCensusCalls, struct {
		From, To int64
		IDsOnly  bool
	}{from, to, idsOnly})
	byLedger := map[int64][]string{}
	for _, e := range m.events {
		if e.Ledger < from || e.Ledger > to {
			continue
		}
		byLedger[e.Ledger] = append(byLedger[e.Ledger], e.ID)
	}
	ledgers := make([]int, 0, len(byLedger))
	for l := range byLedger {
		ledgers = append(ledgers, int(l))
	}
	sort.Ints(ledgers)
	out := make([]store.LedgerCensus, 0, len(ledgers))
	for _, l := range ledgers {
		ids := byLedger[int64(l)]
		sort.Strings(ids)
		c := store.LedgerCensus{Ledger: int64(l), Count: len(ids)}
		if idsOnly {
			c.IDs = ids
		}
		out = append(out, c)
	}
	return out, nil
}

func (m *mockStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ingestionState == nil {
		return store.IngestionState{}, store.ErrNotFound
	}
	return *m.ingestionState, nil
}

func (m *mockStore) SaveIngestionState(_ context.Context, s store.IngestionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s2 := s
	m.ingestionState = &s2
	return nil
}

func (m *mockStore) GetAuditState(context.Context) (store.AuditState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.auditState == nil {
		return store.AuditState{}, store.ErrNotFound
	}
	return *m.auditState, nil
}

func (m *mockStore) SaveAuditState(_ context.Context, s store.AuditState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s2 := s
	m.auditState = &s2
	return nil
}

func (m *mockStore) SaveAuditStateIfGreater(_ context.Context, ledger int64) (store.AuditState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.auditState == nil {
		m.auditState = &store.AuditState{}
	}
	if ledger > m.auditState.VerifiedThroughLedger {
		m.auditState.VerifiedThroughLedger = ledger
	}
	return *m.auditState, nil
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

func (m *mockStore) ListOpenFindingsByRange(_ context.Context, from, to int64) (store.AuditFinding, error) {
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

func (m *mockStore) Stats(context.Context) (store.Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s store.Stats
	for range m.events {
		s.TotalEvents++
	}
	if m.ingestionState != nil {
		s.LastIngestedLedger = m.ingestionState.LastIngestedLedger
	}
	if m.auditState != nil {
		s.VerifiedThroughLedger = m.auditState.VerifiedThroughLedger
	}
	return s, nil
}

func (m *mockStore) Ping(context.Context) error { return nil }

func (m *mockStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (m *mockStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }

// Subscription stubs for the webhook feature — unused by auditor tests.
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

// seedLedgers records pre-existing events in m.events so tests can set up
// "stored state that diverges from the RPC" without a database. IDs use
// the same %020d-%05d format as mkEvents, so seeded events and RPC
// events are comparable by id.
func (m *mockStore) seedLedgers(ledgers []int, contractID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range ledgers {
		id := fmt.Sprintf("%020d-00000", l)
		m.events[id] = store.Event{
			ID:         id,
			ContractID: contractID,
			Ledger:     int64(l),
			Type:       "contract",
		}
	}
}

// mkEvents constructs `count` rpc.Event objects with stable IDs for ledger.
// IDs use the same %020d-%05d format the auditor compares against, so seeded
// events and RPC events align.
func mkEvents(ledger uint32, count int, contractID string) []rpc.Event {
	out := make([]rpc.Event, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%020d-%05d", ledger, i)
		out[i] = rpc.Event{
			ID:         id,
			ContractID: contractID,
			TxHash:     "deadbeef",
			Type:       "contract",
			Ledger:     ledger,
		}
	}
	return out
}
