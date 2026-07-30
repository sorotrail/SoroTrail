package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/buildinfo"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// stubStore implements store.Store for API tests.
type stubStore struct {
	mu         sync.Mutex
	events     []store.Event
	eventByID  map[string]store.Event
	lastFilter store.EventFilter
	nextCursor string

	stats      store.Stats
	ingState   store.IngestionState
	stateErr   error

	// Single event test fields
	event    store.Event
	eventErr error

	// Cache test fields
	exists       bool
	existsErr    error
	existsCalls  int // count of EventExists calls
	lastExistsID string

	ingestion    store.IngestionState
	ingestionErr error

	// Watched contract fields
	watchedList    []store.WatchedContract
	watchedListErr error
	added          []string
	removed        []string
	addErr         error
	removeErr      error

	ingestionState   *store.IngestionState
	ingestionStateEr error

	pingErr error
}

func (s *stubStore) UpsertEvents(ctx context.Context, events []store.Event) (int64, error) {
	return 0, nil
}
func (s *stubStore) ReplaceEventsInRange(ctx context.Context, events []store.Event, fromLedger, toLedger int64) error {
	return nil
}
func (s *stubStore) GetEvent(ctx context.Context, id string) (store.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventByID != nil {
		if e, ok := s.eventByID[id]; ok {
			return e, nil
		}
		return store.Event{}, store.ErrNotFound
	}
	if s.event.ID != "" {
		return s.event, nil
	}
	return store.Event{}, store.ErrNotFound
}
func (s *stubStore) EventExists(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls++
	s.lastExistsID = id
	if s.eventByID != nil {
		_, ok := s.eventByID[id]
		return ok, nil
	}
	return s.exists, s.existsErr
}

// GetIngestionState backs the list-cache frontier lookup. Tests stage
// LastIngestedLedger to drive the boundary decisions (just-below, at,
// and above the frontier).
func (s *stubStore) GetIngestionState(_ context.Context, network string) (store.IngestionState, error) {
	if s.ingestionState != nil {
		return *s.ingestionState, s.ingestionStateEr
	}
	return s.ingestion, s.ingestionErr
}
func (s *stubStore) SaveIngestionState(ctx context.Context, state store.IngestionState) error {
	return nil
}

func (s *stubStore) GetAuditState(_ context.Context, _ string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (s *stubStore) SaveAuditState(_ context.Context, _ store.AuditState) error {
	return nil
}
func (s *stubStore) SaveAuditStateIfGreater(_ context.Context, _ string, ledger int64) (store.AuditState, error) {
	return store.AuditState{VerifiedThroughLedger: ledger}, nil
}
func (s *stubStore) RecordAuditFinding(_ context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	f.ID = 1
	return f, nil
}
func (s *stubStore) UpdateAuditFinding(context.Context, store.AuditFinding) error {
	return nil
}
func (s *stubStore) ListOpenFindingsByRange(_ context.Context, _ string, _, _ int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}

func (s *stubStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (s *stubStore) SetContractSpec(context.Context, string, string, []byte) error {
	return nil
}

func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.mu.Lock()
	s.lastFilter = f
	s.mu.Unlock()
	if len(s.events) == 0 {
		return nil, "", nil
	}
	n := f.Limit
	if n <= 0 {
		n = 50
	}
	if n > len(s.events) {
		n = len(s.events)
	}
	return s.events[:n], s.nextCursor, nil
}
func (s *stubStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]store.LedgerCensus, error) {
	return nil, nil
}
func (s *stubStore) Stats(_ context.Context, _ string) (store.Stats, error) {
	return s.stats, nil
}
func (s *stubStore) Ping(ctx context.Context) error {
	return s.pingErr
}
func (s *stubStore) ListWatchedContracts(ctx context.Context) ([]store.WatchedContract, error) {
	return s.watchedList, s.watchedListErr
}
func (s *stubStore) AddWatchedContract(_ context.Context, id string) error {
	s.added = append(s.added, id)
	return s.addErr
}
func (s *stubStore) RemoveWatchedContract(_ context.Context, id string) error {
	s.removed = append(s.removed, id)
	return s.removeErr
}

func (s *stubStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (s *stubStore) GetSubscription(ctx context.Context, id int64) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (s *stubStore) ListSubscriptions(ctx context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) UpdateSubscription(ctx context.Context, sub store.Subscription) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) DeleteSubscription(ctx context.Context, id int64) error {
	return nil
}
func (s *stubStore) ListEnabledSubscriptions(ctx context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	return 0, false, nil
}
func (s *stubStore) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	return nil
}
func (s *stubStore) RecordDeliveryAttempt(ctx context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	return a, nil
}
func (s *stubStore) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]store.DeliveryAttempt, error) {
	return nil, nil
}

type stubRPC struct {
	rpc.Client

	health    rpc.Health
	healthErr error
}

func (s *stubRPC) GetHealth(context.Context) (rpc.Health, error) {
	return s.health, s.healthErr
}

func newTestServer(st *stubStore, rc *stubRPC) *Server {
	return newTestServerWithKey(st, rc, "test-key")
}

func newTestServerWithKey(st *stubStore, rc *stubRPC, apiKey string) *Server {
	if rc == nil {
		rc = &stubRPC{health: rpc.Health{Status: "healthy"}}
	}
	return New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)), apiKey)
}

// doGet performs a GET request against the test server.
func doGet(t *testing.T, s *Server, path string) (*http.Response, []byte) {
	t.Helper()
	return doGetWithHeader(t, s, path, "", "")
}

func doGetWithHeader(t *testing.T, s *Server, path, key, value string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	require.NoError(t, err)
	if key != "" {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, body
}

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

func TestListEvents_ParsesFilters(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, body := doGet(t, s,
		"/events?contract_id="+testContract+`&type=contract&from_ledger=10&to_ledger=20&limit=5&topic={"symbol":"transfer"}&topic_contains=[{"address":"G..."}]&from_time=2026-07-21T00:00:00Z&to_time=2026-07-22T00:00:00Z`)

	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, testContract, st.lastFilter.ContractID)
	assert.Equal(t, "contract", st.lastFilter.Type)
	assert.Equal(t, int64(10), st.lastFilter.FromLedger)
	assert.Equal(t, int64(20), st.lastFilter.ToLedger)
	assert.Equal(t, 5, st.lastFilter.Limit)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(st.lastFilter.Topic))
	assert.JSONEq(t, `[{"address":"G..."}]`, string(st.lastFilter.TopicContains))
	assert.Equal(t, "2026-07-21T00:00:00Z", st.lastFilter.FromTime.Format(time.RFC3339))
	assert.Equal(t, "2026-07-22T00:00:00Z", st.lastFilter.ToTime.Format(time.RFC3339))
}

func TestListEvents_BareTopicBecomesJSONString(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?topic=transfer")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `"transfer"`, string(st.lastFilter.Topic))
}

func TestListEvents_PositionalTopicFiltersParse(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s,
		"/events?topic0={\"symbol\":\"transfer\"}&topic1=GABC&topic2={\"x\":123}")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(st.lastFilter.Topic0))
	assert.JSONEq(t, `"GABC"`, string(st.lastFilter.Topic1))
	assert.JSONEq(t, `{"x":123}`, string(st.lastFilter.Topic2))
}

func TestListEvents_TopicAndPositionalFiltersConflict(t *testing.T) {
	resp, body := doGet(t, newTestServer(&stubStore{}, nil), "/events?topic=transfer&topic0=GABC")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "cannot be combined")
}

func TestListEvents_BadParams(t *testing.T) {
	for _, path := range []string{
		"/events?type=bogus",
		"/events?contract_id=nope",
		"/events?from_ledger=abc",
		"/events?from_ledger=20&to_ledger=10",
		"/events?from_time=not-a-time",
		"/events?from_time=2026-07-21T00:00:00",
		"/events?from_time=2026-07-21T00:00:00.123Z",
		"/events?from_time=2026-07-22T00:00:00Z&to_time=2026-07-21T00:00:00Z",
		"/events?limit=0",
		"/events?limit=-1",
		"/events?limit=99999",
		"/events?topic_contains=not-valid-json",
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

func TestListEvents_CursorAndLimitValidation(t *testing.T) {
	st := &stubStore{
		events: []store.Event{{ID: "e1"}},
	}
	srv := newTestServer(st, nil)

	// Valid limit and valid cursor
	resp, _ := doGet(t, srv, "/events?limit=10&cursor=0001099511627776-0000000001")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 10, st.lastFilter.Limit)
	assert.Equal(t, "0001099511627776-0000000001", st.lastFilter.Cursor)

	// Omitted limit applies default
	st.lastFilter = store.EventFilter{}
	resp, _ = doGet(t, srv, "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, store.DefaultQueryLimit, st.lastFilter.Limit)

	// Invalid limit returns 400
	for _, badLimit := range []string{"0", "-5", "201", "xyz"} {
		resp, body := doGet(t, srv, "/events?limit="+badLimit)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp map[string]string
		require.NoError(t, json.Unmarshal(body, &errResp))
		assert.Contains(t, errResp["error"], "limit must be an integer in [1,200]")
	}

	// Malformed cursor returns 400
	for _, badCursor := range []string{"has%20space", "e1%3BDROP", "cursor%27OR%271%3D%271", "%3Cscript%3E"} {
		resp, body := doGet(t, srv, "/events?cursor="+badCursor)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp map[string]string
		require.NoError(t, json.Unmarshal(body, &errResp))
		assert.Contains(t, errResp["error"], "invalid cursor")
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

func TestListEvents_IncludeXDR(t *testing.T) {
	event := store.Event{
		ID:          "e1",
		RawTopicXDR: []string{"topic-xdr"},
		RawValueXDR: "value-xdr",
	}
	st := &stubStore{events: []store.Event{event}}
	s := newTestServer(st, nil)

	resp, body := doGet(t, s, "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(body), "topics_xdr")
	assert.NotContains(t, string(body), "value_xdr")

	resp, body = doGet(t, s, "/events?include_xdr=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Events []struct {
			TopicsXDR []string `json:"topics_xdr"`
			ValueXDR  *string  `json:"value_xdr"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	require.Len(t, out.Events, 1)
	assert.Equal(t, []string{"topic-xdr"}, out.Events[0].TopicsXDR)
	require.NotNil(t, out.Events[0].ValueXDR)
	assert.Equal(t, "value-xdr", *out.Events[0].ValueXDR)
}

func TestGetEvent_NotFound(t *testing.T) {
	st := &stubStore{eventErr: store.ErrNotFound}
	resp, _ := doGet(t, newTestServer(st, nil), "/events/0000000000-0000000000")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetEvent_IncludeXDR(t *testing.T) {
	st := &stubStore{event: store.Event{
		ID:          "0000000000-0000000001",
		RawTopicXDR: []string{"topic-xdr"},
		RawValueXDR: "value-xdr",
	}}
	s := newTestServer(st, nil)

	resp, body := doGet(t, s, "/events/0000000000-0000000001")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(body), "topics_xdr")
	assert.NotContains(t, string(body), "value_xdr")

	resp, body = doGet(t, s, "/events/0000000000-0000000001?include_xdr=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		TopicsXDR []string `json:"topics_xdr"`
		ValueXDR  *string  `json:"value_xdr"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, []string{"topic-xdr"}, out.TopicsXDR)
	require.NotNil(t, out.ValueXDR)
	assert.Equal(t, "value-xdr", *out.ValueXDR)
}

func TestContractEvents_ForcesContractFilter(t *testing.T) {
	st := &stubStore{}
	resp, _ := doGet(t, newTestServer(st, nil), "/contracts/"+testContract+"/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, testContract, st.lastFilter.ContractID)

	resp, _ = doGet(t, newTestServer(st, nil), "/contracts/junk/events")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListEvents_TopicContainsValidation(t *testing.T) {
	t.Run("valid JSON array", func(t *testing.T) {
		st := &stubStore{}
		s := newTestServer(st, nil)
		resp, _ := doGet(t, s, `/events?topic_contains=[{"address":"G..."}]`)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.JSONEq(t, `[{"address":"G..."}]`, string(st.lastFilter.TopicContains))
	})

	t.Run("valid JSON object", func(t *testing.T) {
		st := &stubStore{}
		s := newTestServer(st, nil)
		resp, _ := doGet(t, s, `/events?topic_contains={"address":"G..."}`)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.JSONEq(t, `{"address":"G..."}`, string(st.lastFilter.TopicContains))
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		st := &stubStore{}
		s := newTestServer(st, nil)
		resp, body := doGet(t, s, `/events?topic_contains=not-json`)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var e map[string]string
		require.NoError(t, json.Unmarshal(body, &e))
		assert.Contains(t, e["error"], "valid JSON")
	})

	t.Run("empty topic_contains is a no-op", func(t *testing.T) {
		st := &stubStore{}
		s := newTestServer(st, nil)
		resp, _ := doGet(t, s, `/events?topic_contains=`)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, st.lastFilter.TopicContains)
	})

	t.Run("combined with contract_id and ledger", func(t *testing.T) {
		st := &stubStore{}
		s := newTestServer(st, nil)
		resp, _ := doGet(t, s, `/events?contract_id=`+testContract+`&from_ledger=100&topic_contains=[{"symbol":"transfer"}]`)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testContract, st.lastFilter.ContractID)
		assert.Equal(t, int64(100), st.lastFilter.FromLedger)
		assert.JSONEq(t, `[{"symbol":"transfer"}]`, string(st.lastFilter.TopicContains))
	})
}

func TestHealth(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	handler := s.Router()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp healthResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "ok", resp.Status)
}

func TestGetEvent(t *testing.T) {
	st := &stubStore{
		eventByID: map[string]store.Event{
			"ev-1": {ID: "ev-1", ContractID: "C1", Ledger: 100},
		},
	}
	s := newTestServer(st, nil)
	handler := s.Router()

	t.Run("found", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events/ev-1", nil)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		var ev store.Event
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&ev))
		assert.Equal(t, "ev-1", ev.ID)
	})

	t.Run("not found", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events/unknown", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestListEvents(t *testing.T) {
	st := &stubStore{
		events: []store.Event{
			{ID: "e1", ContractID: "C1"},
			{ID: "e2", ContractID: "C2"},
		},
	}
	s := newTestServer(st, nil)
	handler := s.Router()

	t.Run("requires network when multiple configured", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("accepts valid network", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events?network=testnet", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})
}

func TestStats(t *testing.T) {
	st := &stubStore{stats: store.Stats{TotalEvents: 42, LastIngestedLedger: 999}}
	s := newTestServer(st, nil)
	handler := s.Router()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var got store.Stats
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&got))
	assert.Equal(t, int64(42), got.TotalEvents)
}

func TestContractEvents(t *testing.T) {
	st := &stubStore{
		events: []store.Event{
			{ID: "e1", ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
	}
	s := newTestServer(st, nil)
	handler := s.Router()

	t.Run("valid contract", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/contracts/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("invalid contract ID", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/contracts/not-a-contract/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

func TestVersion(t *testing.T) {
	t.Run("returns default values", func(t *testing.T) {
		resp, body := doGet(t, newTestServer(&stubStore{}, nil), "/version")
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildDate string `json:"build_date"`
		}
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "unknown", got.Version)
		assert.Equal(t, "unknown", got.Commit)
		assert.Equal(t, "unknown", got.BuildDate)
	})

	t.Run("reflects injected build info", func(t *testing.T) {
		origV, origC, origD := buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate
		buildinfo.Version = "v1.2.3"
		buildinfo.Commit = "abc1234"
		buildinfo.BuildDate = "2026-07-26T00:00:00Z"
		t.Cleanup(func() {
			buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate = origV, origC, origD
		})

		resp, body := doGet(t, newTestServer(&stubStore{}, nil), "/version")
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var got struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildDate string `json:"build_date"`
		}
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "v1.2.3", got.Version)
		assert.Equal(t, "abc1234", got.Commit)
		assert.Equal(t, "2026-07-26T00:00:00Z", got.BuildDate)
	})

	t.Run("no-store cache header", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/version")
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	})

	t.Run("content type is json", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/version")
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	})
}

func TestRequestID(t *testing.T) {
	tests := []struct {
		name       string
		incomingID string
		wantEcho   bool
	}{
		{name: "generated when absent", incomingID: "", wantEcho: false},
		{name: "echoes incoming ID", incomingID: "test-request-id-123", wantEcho: true},
		{name: "echoes long ID", incomingID: strings.Repeat("a", 100), wantEcho: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			s := New(&stubStore{}, nil, log, "test-key")
			srv := httptest.NewServer(s.Router())
			defer srv.Close()

			req, err := http.NewRequest("GET", srv.URL+"/health", nil)
			require.NoError(t, err)
			if tt.incomingID != "" {
				req.Header.Set("X-Request-ID", tt.incomingID)
			}
			resp, err := http.DefaultTransport.RoundTrip(req)
			require.NoError(t, err)
			resp.Body.Close()

			got := resp.Header.Get("X-Request-ID")
			require.NotEmpty(t, got)
			if tt.wantEcho {
				assert.Equal(t, tt.incomingID, got)
			}
			assert.Contains(t, buf.String(), got)
		})
	}
}
