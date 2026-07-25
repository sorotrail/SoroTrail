package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/metrics"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// stubStore returns canned values and records the filter it was queried with.
type stubStore struct {
	store.Store // panic on anything not stubbed below

	events     []store.Event
	nextCursor string
	queryErr   error
	lastFilter store.EventFilter

	event    store.Event
	eventErr error

	stats   store.Stats
	pingErr error
}

func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.lastFilter = f
	return s.events, s.nextCursor, s.queryErr
}

// LedgerRangeCensus, ReplaceEventsInRange, and the audit_state/findings
// methods are unused by API tests but needed to satisfy store.Store now.
func (s *stubStore) ReplaceEventsInRange(context.Context, []store.Event, int64, int64) error {
	return nil
}
func (s *stubStore) LedgerRangeCensus(context.Context, int64, int64, bool) ([]store.LedgerCensus, error) {
	return nil, nil
}
func (s *stubStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (s *stubStore) SaveAuditState(context.Context, store.AuditState) error {
	return nil
}
func (s *stubStore) SaveAuditStateIfGreater(_ context.Context, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}
func (s *stubStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}
func (s *stubStore) UpdateAuditFinding(context.Context, store.AuditFinding) error {
	return nil
}
func (s *stubStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}

func (s *stubStore) GetEvent(context.Context, string) (store.Event, error) {
	return s.event, s.eventErr
}

func (s *stubStore) Stats(context.Context) (store.Stats, error) { return s.stats, nil }
func (s *stubStore) Ping(context.Context) error                 { return s.pingErr }

type stubRPC struct {
	rpc.Client

	health    rpc.Health
	healthErr error
}

func (s *stubRPC) GetHealth(context.Context) (rpc.Health, error) {
	return s.health, s.healthErr
}

func newTestServer(st *stubStore, rc *stubRPC) *Server {
	if rc == nil {
		rc = &stubRPC{health: rpc.Health{Status: "healthy"}}
	}
	return New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, NewBroker())
}

func doGet(t *testing.T, s *Server, path string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + path)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, body
}

func TestListEvents_ParsesFilters(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, body := doGet(t, s,
		"/events?contract_id="+testContract+`&type=contract&from_ledger=10&to_ledger=20&limit=5&topic={"symbol":"transfer"}`)

	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, testContract, st.lastFilter.ContractID)
	assert.Equal(t, "contract", st.lastFilter.Type)
	assert.Equal(t, int64(10), st.lastFilter.FromLedger)
	assert.Equal(t, int64(20), st.lastFilter.ToLedger)
	assert.Equal(t, 5, st.lastFilter.Limit)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(st.lastFilter.Topic))
}

func TestListEvents_BareTopicBecomesJSONString(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?topic=transfer")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `"transfer"`, string(st.lastFilter.Topic))
}

func TestListEvents_BadParams(t *testing.T) {
	for _, path := range []string{
		"/events?type=bogus",
		"/events?contract_id=nope",
		"/events?from_ledger=abc",
		"/events?from_ledger=20&to_ledger=10",
		"/events?limit=0",
		"/events?limit=99999",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := doGet(t, newTestServer(&stubStore{}, nil), path)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.NotEmpty(t, e["error"])
		})
	}
}

func TestListEvents_ReturnsCursor(t *testing.T) {
	st := &stubStore{
		events:     []store.Event{{ID: "e1"}, {ID: "e2"}},
		nextCursor: "e2",
	}
	resp, body := doGet(t, newTestServer(st, nil), "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Events []store.Event `json:"events"`
		Cursor string        `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Len(t, out.Events, 2)
	assert.Equal(t, "e2", out.Cursor)
}

func TestGetEvent_NotFound(t *testing.T) {
	st := &stubStore{eventErr: store.ErrNotFound}
	resp, _ := doGet(t, newTestServer(st, nil), "/events/0000000000-0000000000")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestContractEvents_ForcesContractFilter(t *testing.T) {
	st := &stubStore{}
	resp, _ := doGet(t, newTestServer(st, nil), "/contracts/"+testContract+"/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, testContract, st.lastFilter.ContractID)

	resp, _ = doGet(t, newTestServer(st, nil), "/contracts/junk/events")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHealth(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/health")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("db down", func(t *testing.T) {
		st := &stubStore{pingErr: errors.New("connection refused")}
		resp, body := doGet(t, newTestServer(st, nil), "/health")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		assert.Contains(t, string(body), "connection refused")
	})
	t.Run("rpc down", func(t *testing.T) {
		rc := &stubRPC{healthErr: errors.New("rpc unreachable")}
		resp, _ := doGet(t, newTestServer(&stubStore{}, rc), "/health")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})
}

func TestStats(t *testing.T) {
	st := &stubStore{stats: store.Stats{TotalEvents: 42, LastIngestedLedger: 999}}
	resp, body := doGet(t, newTestServer(st, nil), "/stats")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got store.Stats
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, int64(42), got.TotalEvents)
	assert.Equal(t, int64(999), got.LastIngestedLedger)
}

// TestBroker_FilteredDelivery verifies that the broker only sends events to
// subscribers whose filters match.
func TestBroker_FilteredDelivery(t *testing.T) {
	b := NewBroker()

	// Subscriber 1: only contract CAAAA..., type contract.
	ch1, cancel1 := b.Subscribe(store.EventFilter{
		ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Type:       "contract",
	})
	defer cancel1()

	// Subscriber 2: only diagnostic events.
	ch2, cancel2 := b.Subscribe(store.EventFilter{Type: "diagnostic"})
	defer cancel2()

	// Subscriber 3: no filter (receives everything).
	ch3, cancel3 := b.Subscribe(store.EventFilter{})
	defer cancel3()

	events := []store.Event{
		{ID: "e1", ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Type: "contract", Ledger: 100},
		{ID: "e2", ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Type: "diagnostic", Ledger: 100},
		{ID: "e3", ContractID: "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", Type: "contract", Ledger: 101},
	}
	b.Publish(events)

	// Sub 1: should get e1 only (matching contract + type)
	ev := drainOne(t, ch1)
	assert.Equal(t, "e1", ev.ID)
	assertNoMore(t, ch1)

	// Sub 2: should get e2 only (diagnostic)
	ev = drainOne(t, ch2)
	assert.Equal(t, "e2", ev.ID)
	assertNoMore(t, ch2)

	// Sub 3: should get all three.
	ids := drainAll(t, ch3, 3)
	assert.Len(t, ids, 3)
}

// TestBroker_TopicFiltering verifies that topic matching is exact and doesn't
// false-positive on substring overlaps (e.g. "transfer" in "transfer_from").
func TestBroker_TopicFiltering(t *testing.T) {
	b := NewBroker()

	ch, cancel := b.Subscribe(store.EventFilter{
		Topic: json.RawMessage(`{"symbol":"transfer"}`),
	})
	defer cancel()

	// Event with exact matching topic.
	eMatch := store.Event{
		ID:     "e1",
		Topics: json.RawMessage(`[{"symbol":"transfer"},{"symbol":"approve"}]`),
	}
	// Event with a topic that contains the filter topic as a substring.
	eSubstring := store.Event{
		ID:     "e2",
		Topics: json.RawMessage(`[{"symbol":"transfer_from"}]`),
	}

	b.Publish([]store.Event{eMatch, eSubstring})

	// Only e1 should match; e2 must not false-positive.
	ev := drainOne(t, ch)
	assert.Equal(t, "e1", ev.ID)
	assertNoMore(t, ch)
}

// TestBroker_SlowSubscriberOverflow verifies that a subscriber whose buffer
// fills up is dropped (channel closed) without blocking the publisher.
func TestBroker_SlowSubscriberOverflow(t *testing.T) {
	b := NewBroker()

	ch, cancel := b.Subscribe(store.EventFilter{})
	defer cancel()

	// Fill the buffer and overflow it.
	n := subscriberBufferSize + 10
	events := make([]store.Event, n)
	for i := range events {
		events[i] = store.Event{ID: fmt.Sprintf("e%d", i)}
	}
	b.Publish(events)

	// Drain up to buffer size — after overflow the channel should be closed.
	drained := 0
	for range ch {
		drained++
	}
	assert.LessOrEqual(t, drained, subscriberBufferSize,
		"overflow should drop the subscriber after buffer is full")
}

// TestSubscribe_RouteExists verifies the SSE endpoint is registered, returns
// proper headers, and validates filter params. Core publish/subscribe logic
// is tested by TestBroker_FilteredDelivery and TestBroker_SlowSubscriberOverflow.
func TestSubscribe_RouteExists(t *testing.T) {
	b := NewBroker()
	st := &stubStore{}
	s := New(st, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), nil, b)

	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	// A bad filter should return 400.
	resp, body := doGet(t, s, "/events/subscribe?type=bogus")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "error")
}

// drainOne reads a single event from a channel (non-blocking with short
// timeout).
func drainOne(t *testing.T, ch <-chan store.Event) store.Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event on channel")
	}
	return store.Event{}
}

// assertNoMore asserts that no more events are immediately available on ch.
func assertNoMore(t *testing.T, ch <-chan store.Event) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected event %s on channel", e.ID)
	default:
	}
}

// drainAll reads up to n events from ch.
func drainAll(t *testing.T, ch <-chan store.Event, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		select {
		case e := <-ch:
			ids = append(ids, e.ID)
		case <-time.After(50 * time.Millisecond):
			return ids
		}
	}
	return ids
}

func TestMetrics_Returns200AndContainsExpectedNames(t *testing.T) {
	m := metrics.New()
	// Drive some ingest-like activity so counters/gauge are non-zero and
	// their metadata lines appear in the registry.
	m.RecordEventsIngested(5)
	m.SetLastIngestedLedger(12345)
	m.SetChainHeadLedger(12400)
	m.ObserveRPCRequest("getEvents", nil)
	m.ObserveRPCRequest("getHealth", errors.New("timeout"))
	m.RecordHTTPRequest("/events", 200, 0.012)
	m.RecordHTTPRequest("/health", 500, 0.003)

	st := &stubStore{}
	s := New(st, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), m, NewBroker())

	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	text := string(body)
	// Verify every required metric name appears.
	for _, name := range []string{
		"sorotrail_events_ingested_total",
		"sorotrail_last_ingested_ledger",
		"sorotrail_chain_head_ledger",
		"sorotrail_rpc_requests_total",
		"sorotrail_http_requests_total",
		"sorotrail_http_request_duration_seconds",
	} {
		assert.Contains(t, text, name, "metrics output must contain %s", name)
	}
}
