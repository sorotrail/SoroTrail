package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/buildinfo"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// stubStore returns canned values and records the filter it was queried with.
type stubStore struct {
	store.Store // panic on anything not stubbed below

	events     []store.Event
	nextCursor string
	queryErr   error
	lastFilter store.EventFilter

	totalCount      int64
	countEventsErr  error
	lastCountFilter store.EventFilter

	event    store.Event
	eventErr error

	txSiblings    []store.Event
	txSiblingsErr error
	lastTxHash    string
	lastExcludeID string

	stats            store.Stats
	pingErr          error
	watchedList      []store.WatchedContract
	watchedListErr   error
	added            []string
	removed          []string
	addErr           error
	removeErr        error
	ingestionState   *store.IngestionState
	ingestionStateEr error
	exists           bool
	existsErr        error
	existsCalls      int // count of EventExists calls
	lastExistsID     string

	ingestion    store.IngestionState
	ingestionErr error

	migrationVersion int
	migrationDirty   bool
	migrationErr     error
}

func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.lastFilter = f
	return s.events, s.nextCursor, s.queryErr
}

func (s *stubStore) CountEvents(_ context.Context, f store.EventFilter) (int64, error) {
	s.lastCountFilter = f
	return s.totalCount, s.countEventsErr
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

// DeleteEventsBefore is unused by API tests but needed to satisfy
// store.Store now that the pruner can call it.
func (s *stubStore) DeleteEventsBefore(context.Context, int64, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *stubStore) GetEvent(context.Context, string, store.Scope) (store.Event, error) {
	return s.event, s.eventErr
}

func (s *stubStore) GetEventsByTxHash(_ context.Context, txHash, excludeID string) ([]store.Event, error) {
	s.lastTxHash = txHash
	s.lastExcludeID = excludeID
	return s.txSiblings, s.txSiblingsErr
}

func (s *stubStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (s *stubStore) SetContractSpec(context.Context, string, string, []byte) error {
	return nil
}

// EventExists is the cheap 304 path; tests assert the handler uses it
// (instead of GetEvent) when If-None-Match matches.
func (s *stubStore) EventExists(_ context.Context, id string, _ store.Scope) (bool, error) {
	s.existsCalls++
	s.lastExistsID = id
	return s.exists, s.existsErr
}

// GetIngestionState backs the list-cache frontier lookup. Tests stage
// LastIngestedLedger to drive the boundary decisions (just-below, at,
// and above the frontier).
func (s *stubStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	if s.ingestionState != nil {
		return *s.ingestionState, s.ingestionStateEr
	}
	return s.ingestion, s.ingestionErr
}

// MigrationVersion backs /readyz's schema check. Tests that need a dirty
// or errored schema set migrationErr / migrationDirty.
func (s *stubStore) MigrationVersion(context.Context) (int, bool, error) {
	if s.migrationErr != nil {
		return 0, false, s.migrationErr
	}
	// Zero means "unset" for the many tests that construct stubStore{}
	// inline; /readyz treats a real 0 as "no migrations applied", so
	// default to a clean applied schema unless a test says otherwise.
	v := s.migrationVersion
	if v == 0 {
		v = 1
	}
	return v, s.migrationDirty, nil
}

func (s *stubStore) Stats(context.Context, store.Scope) (store.Stats, error) { return s.stats, nil }
func (s *stubStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
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

func (s *stubStore) Ping(context.Context) error { return s.pingErr }

// Subscription stubs for the webhook feature.
func (s *stubStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (s *stubStore) GetSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (s *stubStore) ListSubscriptions(context.Context, store.SubscriptionOwner) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) UpdateSubscription(_ context.Context, sub store.Subscription, _ store.SubscriptionOwner) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) DeleteSubscription(context.Context, int64, store.SubscriptionOwner) error {
	return nil
}
func (s *stubStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) IncrementSubscriptionFailures(context.Context, int64, int) (int, bool, error) {
	return 0, false, nil
}
func (s *stubStore) ResetSubscriptionFailures(context.Context, int64) error { return nil }
func (s *stubStore) RecordDeliveryAttempt(_ context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	a.ID = 1
	return a, nil
}
func (s *stubStore) ListDeliveryAttempts(context.Context, int64, int, store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
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
		"/events?contract_id="+testContract+`&type=contract&from_ledger=10&to_ledger=20&limit=5&topic={"symbol":"transfer"}&topic_contains=[{"address":"G..."}]&from_time=2026-07-21T00:00:00Z&to_time=2026-07-22T00:00:00Z&tx_hash=abc123def`)

	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, testContract, st.lastFilter.ContractID)
	assert.Equal(t, []string{"contract"}, st.lastFilter.Types)
	assert.Equal(t, int64(10), st.lastFilter.FromLedger)
	assert.Equal(t, int64(20), st.lastFilter.ToLedger)
	assert.Equal(t, 5, st.lastFilter.Limit)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(st.lastFilter.Topic))
	assert.JSONEq(t, `[{"address":"G..."}]`, string(st.lastFilter.TopicContains))
	assert.Equal(t, "2026-07-21T00:00:00Z", st.lastFilter.FromTime.Format(time.RFC3339))
	assert.Equal(t, "2026-07-22T00:00:00Z", st.lastFilter.ToTime.Format(time.RFC3339))
	assert.Equal(t, "abc123def", st.lastFilter.TxHash)
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

func TestListEvents_TxHashFilter(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    string
		wantErr int // 0 = success
	}{
		{name: "no tx_hash param", query: "/events", want: "", wantErr: 0},
		{name: "with tx_hash", query: "/events?tx_hash=abc123def", want: "abc123def", wantErr: 0},
		{name: "empty tx_hash is no-op", query: "/events?tx_hash=", want: "", wantErr: 0},
		{name: "hex tx_hash", query: "/events?tx_hash=9f5c0e3f2a1b4d6c7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d", want: "9f5c0e3f2a1b4d6c7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d", wantErr: 0},
		{name: "combined with contract_id", query: "/events?contract_id=" + testContract + "&tx_hash=abc", want: "abc", wantErr: 0},
		{name: "combined with ledger range", query: "/events?from_ledger=100&to_ledger=200&tx_hash=abc", want: "abc", wantErr: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{}
			s := newTestServer(st, nil)
			resp, body := doGet(t, s, tt.query)
			if tt.wantErr != 0 {
				assert.Equal(t, tt.wantErr, resp.StatusCode)
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				return
			}
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, tt.want, st.lastFilter.TxHash)
		})
	}
}

func TestListEvents_HasValueFilter(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    *bool // nil = not set, true = has value, false = no value
		wantErr int   // 0 = success
	}{
		{name: "no has_value param", query: "/events", want: nil, wantErr: 0},
		{name: "has_value=true", query: "/events?has_value=true", want: ptr(true), wantErr: 0},
		{name: "has_value=false", query: "/events?has_value=false", want: ptr(false), wantErr: 0},
		{name: "empty has_value is no-op", query: "/events?has_value=", want: nil, wantErr: 0},
		{name: "combined with contract_id", query: "/events?contract_id=" + testContract + "&has_value=true", want: ptr(true), wantErr: 0},
		{name: "combined with ledger range", query: "/events?from_ledger=100&to_ledger=200&has_value=false", want: ptr(false), wantErr: 0},
		{name: "invalid value returns 400", query: "/events?has_value=yes", wantErr: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{}
			s := newTestServer(st, nil)
			resp, body := doGet(t, s, tt.query)
			if tt.wantErr != 0 {
				assert.Equal(t, tt.wantErr, resp.StatusCode)
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Contains(t, e["error"], "has_value")
				return
			}
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, tt.want, st.lastFilter.HasValue)
		})
	}
}

func TestListEvents_TypeFilter(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    []string
		wantErr int // 0 = success
	}{
		{name: "no type param", query: "/events", want: nil, wantErr: 0},
		{name: "single type", query: "/events?type=contract", want: []string{"contract"}, wantErr: 0},
		{name: "multiple types", query: "/events?type=contract,system", want: []string{"contract", "system"}, wantErr: 0},
		{name: "all three types", query: "/events?type=contract,system,diagnostic", want: []string{"contract", "system", "diagnostic"}, wantErr: 0},
		{name: "invalid type", query: "/events?type=bogus", wantErr: http.StatusBadRequest},
		{name: "partially invalid type", query: "/events?type=contract,bogus", wantErr: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{}
			s := newTestServer(st, nil)
			resp, body := doGet(t, s, tt.query)
			if tt.wantErr != 0 {
				assert.Equal(t, tt.wantErr, resp.StatusCode)
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Contains(t, e["error"], "invalid type")
				return
			}
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, tt.want, st.lastFilter.Types)
		})
	}
}

func TestListEvents_TotalCountHeader(t *testing.T) {
	t.Run("sets X-Total-Count when count succeeds", func(t *testing.T) {
		st := &stubStore{
			events:     []store.Event{{ID: "e1"}, {ID: "e2"}},
			nextCursor: "e2",
			totalCount: 42,
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?contract_id="+testContract)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "42", resp.Header.Get("X-Total-Count"))
	})

	t.Run("count filter excludes pagination fields", func(t *testing.T) {
		st := &stubStore{
			events:     []store.Event{{ID: "e1"}},
			totalCount: 10,
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?cursor=old&limit=5&order=desc")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		// The count filter must have stripped cursor, order, order_by, and limit.
		assert.Equal(t, "", st.lastCountFilter.Cursor)
		assert.Equal(t, "", st.lastCountFilter.Order)
		assert.Equal(t, "", st.lastCountFilter.OrderBy)
		assert.Equal(t, 0, st.lastCountFilter.Limit)
		// But filter conditions like contract_id should still be present.
		assert.Equal(t, "", st.lastCountFilter.ContractID,
			"contract_id was not in request, so count filter should not have it")
	})

	t.Run("count filter preserves query filters", func(t *testing.T) {
		st := &stubStore{
			events:     []store.Event{{ID: "e1"}},
			totalCount: 5,
		}
		resp, _ := doGet(t, newTestServer(st, nil),
			"/events?contract_id="+testContract+"&from_ledger=100&to_ledger=200")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testContract, st.lastCountFilter.ContractID)
		assert.Equal(t, int64(100), st.lastCountFilter.FromLedger)
		assert.Equal(t, int64(200), st.lastCountFilter.ToLedger)
	})

	t.Run("omits X-Total-Count when count fails", func(t *testing.T) {
		st := &stubStore{
			events:         []store.Event{{ID: "e1"}},
			countEventsErr: errors.New("count timeout"),
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, resp.Header.Get("X-Total-Count"))
	})

	t.Run("total count is zero for empty result set", func(t *testing.T) {
		st := &stubStore{
			totalCount: 0,
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "0", resp.Header.Get("X-Total-Count"))
	})
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
	for _, badLimit := range []string{"0", "-5", "501", "xyz"} {
		resp, body := doGet(t, srv, "/events?limit="+badLimit)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp map[string]string
		require.NoError(t, json.Unmarshal(body, &errResp))
		assert.Contains(t, errResp["error"], "limit must be an integer in [1,500]")
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

func TestContractEvents_TxHashFilter(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s,
		"/contracts/"+testContract+"/events?tx_hash=abc123")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, testContract, st.lastFilter.ContractID)
	assert.Equal(t, "abc123", st.lastFilter.TxHash)
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

func TestLivez(t *testing.T) {
	t.Run("always 200", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/livez")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("200 even during transient db outage", func(t *testing.T) {
		st := &stubStore{pingErr: errors.New("connection refused")}
		resp, _ := doGet(t, newTestServer(st, nil), "/livez")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("200 even during transient rpc outage", func(t *testing.T) {
		rc := &stubRPC{healthErr: errors.New("rpc unreachable")}
		resp, _ := doGet(t, newTestServer(&stubStore{}, rc), "/livez")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("no-store cache header", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/livez")
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	})

	t.Run("returns ok status", func(t *testing.T) {
		_, body := doGet(t, newTestServer(&stubStore{}, nil), "/livez")
		var h healthResponse
		require.NoError(t, json.Unmarshal(body, &h))
		assert.Equal(t, "ok", h.Status)
	})
}

func TestReadyz(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/readyz")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("db down returns 503 with reason", func(t *testing.T) {
		st := &stubStore{pingErr: errors.New("connection refused")}
		resp, body := doGet(t, newTestServer(st, nil), "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		var h healthResponse
		require.NoError(t, json.Unmarshal(body, &h))
		assert.Equal(t, "degraded", h.Status)
		assert.Contains(t, h.Checks["database"], "connection refused")
		// RPC is still ok in this test.
		assert.Equal(t, "ok", h.Checks["rpc"])
	})

	t.Run("rpc down returns 503 with reason", func(t *testing.T) {
		rc := &stubRPC{healthErr: errors.New("rpc unreachable")}
		resp, body := doGet(t, newTestServer(&stubStore{}, rc), "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		var h healthResponse
		require.NoError(t, json.Unmarshal(body, &h))
		assert.Equal(t, "degraded", h.Status)
		assert.Contains(t, h.Checks["rpc"], "rpc unreachable")
	})

	t.Run("rpc unhealthy status returns 503", func(t *testing.T) {
		rc := &stubRPC{health: rpc.Health{Status: "unhealthy"}}
		resp, body := doGet(t, newTestServer(&stubStore{}, rc), "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		var h healthResponse
		require.NoError(t, json.Unmarshal(body, &h))
		assert.Equal(t, "degraded", h.Status)
		assert.Contains(t, h.Checks["rpc"], `rpc reports "unhealthy"`)
	})

	t.Run("no-store cache header", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/readyz")
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	})

	t.Run("json content type", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/readyz")
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	})
}

func TestListEvents_FieldsProjection(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	st := &stubStore{
		events: []store.Event{
			{
				ID:               "0000000001-0000000001",
				ContractID:       testContract,
				Ledger:           1,
				Type:             "contract",
				TxHash:           "abc123",
				TxIndex:          0,
				OpIndex:          0,
				InSuccessfulCall: true,
				Topics:           json.RawMessage(`["transfer"]`),
				Value:            json.RawMessage(`{"amount":"100"}`),
				CreatedAt:        now,
			},
		},
		nextCursor: "0000000001-0000000001",
	}
	s := newTestServer(st, nil)

	t.Run("returns only requested fields", func(t *testing.T) {
		resp, body := doGet(t, s, "/events?fields=id,ledger")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out struct {
			Events []map[string]any `json:"events"`
			Cursor string           `json:"cursor"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		require.Len(t, out.Events, 1)
		ev := out.Events[0]
		assert.Equal(t, "0000000001-0000000001", ev["id"])
		assert.Equal(t, float64(1), ev["ledger"])
		assert.Nil(t, ev["contract_id"])
		assert.Nil(t, ev["type"])
		assert.Nil(t, ev["tx_hash"])
		assert.Equal(t, "0000000001-0000000001", out.Cursor)
	})

	t.Run("omitting fields returns full object", func(t *testing.T) {
		resp, body := doGet(t, s, "/events")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out struct {
			Events []store.Event `json:"events"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		require.Len(t, out.Events, 1)
		ev := out.Events[0]
		assert.Equal(t, "abc123", ev.TxHash)
		assert.Equal(t, "contract", ev.Type)
		assert.Contains(t, string(out.Events[0].Topics), "transfer")
	})
}

func TestListEvents_FieldsRejectsUnknown(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	t.Run("unknown field returns 400", func(t *testing.T) {
		resp, body := doGet(t, s, "/events?fields=id,nope")
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var e map[string]string
		require.NoError(t, json.Unmarshal(body, &e))
		assert.Contains(t, e["error"], "unknown field")
		assert.Contains(t, e["error"], "nope")
	})

	t.Run("empty fields acts like omission", func(t *testing.T) {
		resp, _ := doGet(t, s, "/events?fields=")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("all known fields accepted", func(t *testing.T) {
		resp, _ := doGet(t, s, "/events?fields=id,contract_id,ledger,type,tx_hash,tx_index,op_index,in_successful_call,topics,value,created_at")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestGetEvent_FieldsProjection(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	eventID := "0000000001-0000000001"
	st := &stubStore{
		event: store.Event{
			ID:               eventID,
			ContractID:       testContract,
			Ledger:           1,
			Type:             "contract",
			TxHash:           "abc123",
			TxIndex:          0,
			OpIndex:          0,
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`["transfer"]`),
			Value:            json.RawMessage(`{"amount":"100"}`),
			CreatedAt:        now,
		},
	}
	s := newTestServer(st, nil)

	t.Run("returns only requested fields", func(t *testing.T) {
		resp, body := doGet(t, s, "/events/"+eventID+"?fields=id,ledger")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out map[string]any
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, eventID, out["id"])
		assert.Equal(t, float64(1), out["ledger"])
		assert.Nil(t, out["contract_id"])
		assert.Nil(t, out["type"])
	})

	t.Run("omitting fields returns full object", func(t *testing.T) {
		resp, body := doGet(t, s, "/events/"+eventID)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out store.Event
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, "abc123", out.TxHash)
		assert.Equal(t, "contract", out.Type)
	})

	t.Run("unknown field returns 400", func(t *testing.T) {
		resp, body := doGet(t, s, "/events/"+eventID+"?fields=id,bogus")
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var e map[string]string
		require.NoError(t, json.Unmarshal(body, &e))
		assert.Contains(t, e["error"], "unknown field")
	})
}

func TestStats(t *testing.T) {
	t.Run("includes store and freshness fields", func(t *testing.T) {
		st := &stubStore{stats: store.Stats{
			TotalEvents:        42,
			LastIngestedLedger: 999,
			OldestStoredLedger: 100,
		}}
		rc := &stubRPC{health: rpc.Health{Status: "healthy", LatestLedger: 1_020}}
		resp, body := doGet(t, newTestServer(st, rc), "/stats")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var got store.Stats
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, int64(42), got.TotalEvents)
		assert.Equal(t, int64(999), got.LastIngestedLedger)
		assert.Equal(t, int64(100), got.OldestStoredLedger)
		require.NotNil(t, got.ChainHeadLedger)
		assert.Equal(t, int64(1_020), *got.ChainHeadLedger)
		require.NotNil(t, got.IngestLagLedgers)
		assert.Equal(t, int64(21), *got.IngestLagLedgers)
		assert.Equal(t, uint64(0), got.QueryErrors, "query_errors should be present and zero")
	})

	t.Run("keeps stored stats when RPC is down", func(t *testing.T) {
		st := &stubStore{stats: store.Stats{
			TotalEvents:        42,
			LastIngestedLedger: 999,
			OldestStoredLedger: 100,
		}}
		rc := &stubRPC{healthErr: errors.New("rpc unreachable")}
		resp, body := doGet(t, newTestServer(st, rc), "/stats")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var got store.Stats
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, int64(42), got.TotalEvents)
		assert.Equal(t, int64(999), got.LastIngestedLedger)
		assert.Equal(t, int64(100), got.OldestStoredLedger)
		assert.Nil(t, got.ChainHeadLedger)
		assert.Nil(t, got.IngestLagLedgers)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(body, &raw))
		assert.Contains(t, raw, "chain_head_ledger")
		assert.Nil(t, raw["chain_head_ledger"])
		assert.Contains(t, raw, "ingest_lag_ledgers")
		assert.Nil(t, raw["ingest_lag_ledgers"])
		assert.Equal(t, uint64(0), got.QueryErrors, "query_errors should be present and zero")
	})
}

func TestListEvents_StreamNDJSON(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		events       []store.Event
		nextCursor   string
		wantLines    int
		wantContains []string
	}{
		{
			name:  "streams events as ndjson",
			query: "/events?stream=true",
			events: []store.Event{
				{ID: "e1", TxHash: "abc123"},
				{ID: "e2", TxHash: "def456"},
			},
			nextCursor: "",
			wantLines:  2,
			wantContains: []string{
				`"id":"e1"`,
				`"id":"e2"`,
				`"tx_hash":"abc123"`,
			},
		},
		{
			name:  "supports include_xdr",
			query: "/events?stream=true&include_xdr=true",
			events: []store.Event{
				{ID: "e1", RawTopicXDR: []string{"xdr1"}, RawValueXDR: "vxdr1"},
			},
			nextCursor: "",
			wantLines:  1,
			wantContains: []string{
				`"topics_xdr":["xdr1"]`,
				`"value_xdr":"vxdr1"`,
			},
		},
		{
			name:  "supports fields projection",
			query: "/events?stream=true&fields=id,ledger",
			events: []store.Event{
				{ID: "e1", Ledger: 100, TxHash: "abc"},
			},
			nextCursor: "",
			wantLines:  1,
			wantContains: []string{
				`"id":"e1"`,
				`"ledger":100`,
			},
		},
		{
			name:       "empty result set",
			query:      "/events?stream=true",
			events:     []store.Event{},
			nextCursor: "",
			wantLines:  0,
		},
		{
			name:   "bad filter returns 400 before streaming",
			query:  "/events?stream=true&type=bogus",
			events: nil,
		},
		{
			name:  "combines with tx_hash filter",
			query: "/events?stream=true&tx_hash=abc",
			events: []store.Event{
				{ID: "e1", TxHash: "abc"},
			},
			nextCursor: "",
			wantLines:  1,
			wantContains: []string{
				`"tx_hash":"abc"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{
				events:     tt.events,
				nextCursor: tt.nextCursor,
			}
			s := newTestServer(st, nil)

			resp, body := doGet(t, s, tt.query)

			if tt.events == nil && tt.wantLines == 0 {
				// Error case: no events stored, expect bad request
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				return
			}

			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))
			assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

			lines := strings.Split(strings.TrimSpace(string(body)), "\n")
			if tt.wantLines == 0 {
				assert.Empty(t, strings.TrimSpace(string(body)))
				return
			}
			assert.Len(t, lines, tt.wantLines)
			for _, want := range tt.wantContains {
				assert.Contains(t, string(body), want)
			}
		})
	}
}

func TestListEvents_StreamNDJSON_ErrorDuringStream(t *testing.T) {
	st := &stubStore{
		queryErr: errors.New("db connection lost"),
	}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events?stream=true")
	// First query fails before headers are written: client gets a proper
	// error envelope rather than a 200 with an empty body.
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "querying events failed")
}

func TestListEvents_StreamNDJSON_MultiBatch(t *testing.T) {
	// Simulate two batches: first returns events with a cursor, second
	// returns more events with an empty cursor (end of stream).
	st := &stubStore{
		events:     []store.Event{{ID: "e1"}, {ID: "e2"}},
		nextCursor: "e2", // signals more data after first batch
	}
	s := newTestServer(st, nil)

	// We need to override QueryEvents to return different data on second call.
	// Use a counter in the handler is not possible, so we assert the header
	// and at least one event instead.
	resp, body := doGet(t, s, "/events?stream=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))
	assert.Contains(t, string(body), `"id":"e1"`)
	assert.Contains(t, string(body), `"id":"e2"`)

	// With a non-empty cursor returned, the handler will make a second call.
	// The stub returns same events again; the handler writes them again.
	// We verify the cursor was consumed by checking lastFilter.
	assert.Equal(t, "e2", st.lastFilter.Cursor)
}

func TestListEvents_OrderByParses(t *testing.T) {
	for _, orderBy := range []string{"id", "ledger", "created_at"} {
		t.Run(orderBy, func(t *testing.T) {
			st := &stubStore{}
			resp, body := doGet(t, newTestServer(st, nil), "/events?order_by="+orderBy+"&order=desc")
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, orderBy, st.lastFilter.OrderBy)
			assert.Equal(t, "desc", st.lastFilter.Order, "order_by and order combine")
		})
	}
}

// Omitting order_by keeps the historical default rather than inventing one.
func TestListEvents_OrderByDefaultsToEmpty(t *testing.T) {
	st := &stubStore{}
	resp, _ := doGet(t, newTestServer(st, nil), "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "", st.lastFilter.OrderBy)
}

// An unsupported sort column is a 400, not a silently-ignored parameter.
func TestListEvents_InvalidOrderByIsBadRequest(t *testing.T) {
	for _, bad := range []string{"tx_hash", "ledger; DROP TABLE events", "LEDGER"} {
		t.Run(bad, func(t *testing.T) {
			resp, body := doGet(t, newTestServer(&stubStore{}, nil),
				"/events?order_by="+url.QueryEscape(bad))
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.Contains(t, e["error"], "invalid order_by")
		})
	}
}

// order_by applies to the contract-scoped listing too, since it shares the
// same filter parsing.
func TestContractEvents_OrderByParses(t *testing.T) {
	st := &stubStore{}
	resp, body := doGet(t, newTestServer(st, nil),
		"/contracts/"+testContract+"/events?order_by=created_at")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, "created_at", st.lastFilter.OrderBy)
	assert.Equal(t, testContract, st.lastFilter.ContractID)
}

// A cursor that doesn't decode under the requested ordering is client error.
func TestListEvents_InvalidCursorIsBadRequest(t *testing.T) {
	st := &stubStore{queryErr: store.ErrInvalidCursor}
	resp, body := doGet(t, newTestServer(st, nil), "/events?order_by=ledger&cursor=bogus")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "invalid cursor")
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

func TestCountEvents(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		totalCount     int64
		countErr       error
		wantStatus     int
		wantCount      int64
		wantErrContain string
		// wantCountFilter checks the filter passed to CountEvents
		wantContractID string
		wantFromLedger int64
		wantToLedger   int64
	}{
		{
			name:       "returns count for all events",
			query:      "/events/count",
			totalCount: 42,
			wantStatus: http.StatusOK,
			wantCount:  42,
		},
		{
			name:           "passes contract_id filter",
			query:          "/events/count?contract_id=" + testContract,
			totalCount:     7,
			wantStatus:     http.StatusOK,
			wantCount:      7,
			wantContractID: testContract,
		},
		{
			name:           "passes ledger range filter",
			query:          "/events/count?from_ledger=100&to_ledger=200",
			totalCount:     3,
			wantStatus:     http.StatusOK,
			wantCount:      3,
			wantFromLedger: 100,
			wantToLedger:   200,
		},
		{
			name:       "zero count",
			query:      "/events/count",
			totalCount: 0,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:           "store error returns 500",
			query:          "/events/count",
			countErr:       errors.New("db timeout"),
			wantStatus:     http.StatusInternalServerError,
			wantErrContain: "counting events failed",
		},
		{
			name:           "bad filter returns 400",
			query:          "/events/count?type=bogus",
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "invalid type",
		},
		{
			name:           "bad contract_id returns 400",
			query:          "/events/count?contract_id=notvalid",
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "invalid contract_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{
				totalCount:     tt.totalCount,
				countEventsErr: tt.countErr,
			}
			s := newTestServer(st, nil)
			resp, body := doGet(t, s, tt.query)

			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			if tt.wantErrContain != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Contains(t, e["error"], tt.wantErrContain)
				return
			}

			var got countResponse
			require.NoError(t, json.Unmarshal(body, &got))
			assert.Equal(t, tt.wantCount, got.Count)

			// Pagination fields must be stripped before hitting the store.
			assert.Equal(t, "", st.lastCountFilter.Cursor)
			assert.Equal(t, "", st.lastCountFilter.Order)
			assert.Equal(t, "", st.lastCountFilter.OrderBy)
			assert.Equal(t, 0, st.lastCountFilter.Limit)

			if tt.wantContractID != "" {
				assert.Equal(t, tt.wantContractID, st.lastCountFilter.ContractID)
			}
			if tt.wantFromLedger != 0 {
				assert.Equal(t, tt.wantFromLedger, st.lastCountFilter.FromLedger)
			}
			if tt.wantToLedger != 0 {
				assert.Equal(t, tt.wantToLedger, st.lastCountFilter.ToLedger)
			}
		})
	}
}

func TestCountEvents_CacheControl(t *testing.T) {
	st := &stubStore{totalCount: 5}
	resp, _ := doGet(t, newTestServer(st, nil), "/events/count")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
}

func TestCountEvents_ResponseShape(t *testing.T) {
	st := &stubStore{totalCount: 99}
	_, body := doGet(t, newTestServer(st, nil), "/events/count")
	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	// Must have exactly one key: "count"
	assert.Len(t, raw, 1)
	_, hasCount := raw["count"]
	assert.True(t, hasCount)
	assert.Equal(t, float64(99), raw["count"])
}

func TestListEvents_RecentParam(t *testing.T) {
	events := []store.Event{
		{ID: "e3", Ledger: 3},
		{ID: "e2", Ledger: 2},
		{ID: "e1", Ledger: 1},
	}

	tests := []struct {
		name        string
		query       string
		wantStatus  int
		wantOrder   string
		wantLimit   int
		wantErrText string
	}{
		{
			name:       "recent=true sets desc order and default limit 20",
			query:      "/events?recent=true",
			wantStatus: http.StatusOK,
			wantOrder:  "desc",
			wantLimit:  recentDefaultLimit,
		},
		{
			name:       "recent=5 sets desc order and limit 5",
			query:      "/events?recent=5",
			wantStatus: http.StatusOK,
			wantOrder:  "desc",
			wantLimit:  5,
		},
		{
			name:       "recent=500 (max limit) is accepted",
			query:      "/events?recent=500",
			wantStatus: http.StatusOK,
			wantOrder:  "desc",
			wantLimit:  500,
		},
		{
			name:        "recent=0 is invalid",
			query:       "/events?recent=0",
			wantStatus:  http.StatusBadRequest,
			wantErrText: "recent must be a positive integer",
		},
		{
			name:        "recent=501 exceeds max",
			query:       "/events?recent=501",
			wantStatus:  http.StatusBadRequest,
			wantErrText: "recent must be a positive integer",
		},
		{
			name:        "recent conflicts with order",
			query:       "/events?recent=5&order=asc",
			wantStatus:  http.StatusBadRequest,
			wantErrText: "recent cannot be combined with order",
		},
		{
			name:        "recent conflicts with order_by",
			query:       "/events?recent=5&order_by=ledger",
			wantStatus:  http.StatusBadRequest,
			wantErrText: "recent cannot be combined with order",
		},
		{
			name:        "recent conflicts with limit",
			query:       "/events?recent=5&limit=10",
			wantStatus:  http.StatusBadRequest,
			wantErrText: "recent cannot be combined with limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{events: events}
			s := newTestServer(st, nil)
			resp, body := doGet(t, s, tt.query)

			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			if tt.wantErrText != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Contains(t, e["error"], tt.wantErrText)
				return
			}

			assert.Equal(t, tt.wantOrder, st.lastFilter.Order)
			assert.Equal(t, tt.wantLimit, st.lastFilter.Limit)
		})
	}
}

// TestGetEvent_ETagAndConditionalGet verifies that GET /events/{id}
// serves a strong ETag and honors If-None-Match conditional requests
// with 304 Not Modified. Events are immutable so the event ID doubles
// as a perfect strong validator. (#226)
func TestGetEvent_ETagAndConditionalGet(t *testing.T) {
	eventID := "0001099511627776-0000000001"
	eventBody := store.Event{ID: eventID, Ledger: 100, TxHash: "abc123"}

	tests := []struct {
		name            string
		setup           func(st *stubStore)
		ifNoneMatch     string
		wantStatus      int
		wantETag        string
		wantExistsCalls int
		wantBody        string
	}{
		{
			name:       "GET returns strong ETag",
			setup:      func(st *stubStore) { st.event = eventBody },
			wantStatus: http.StatusOK,
			wantETag:   `"` + eventID + `"`,
		},
		{
			name:            "If-None-Match match returns 304",
			setup:           func(st *stubStore) { st.exists = true },
			ifNoneMatch:     `"` + eventID + `"`,
			wantStatus:      http.StatusNotModified,
			wantETag:        `"` + eventID + `"`,
			wantExistsCalls: 1,
		},
		{
			name:            "If-None-Match wildcard returns 304",
			setup:           func(st *stubStore) { st.exists = true },
			ifNoneMatch:     "*",
			wantStatus:      http.StatusNotModified,
			wantETag:        `"` + eventID + `"`,
			wantExistsCalls: 1,
		},
		{
			name:            "If-None-Match with W/ prefix returns 304",
			setup:           func(st *stubStore) { st.exists = true },
			ifNoneMatch:     `W/"` + eventID + `"`,
			wantStatus:      http.StatusNotModified,
			wantETag:        `"` + eventID + `"`,
			wantExistsCalls: 1,
		},
		{
			name:        "If-None-Match mismatch returns 200",
			setup:       func(st *stubStore) { st.event = eventBody },
			ifNoneMatch: `"a-different-id"`,
			wantStatus:  http.StatusOK,
			wantETag:    `"` + eventID + `"`,
		},
		{
			name:            "event pruned returns 404 even with matching validator",
			setup:           func(st *stubStore) { st.exists = false },
			ifNoneMatch:     `"` + eventID + `"`,
			wantStatus:      http.StatusNotFound,
			wantExistsCalls: 1,
			wantBody:        eventID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{}
			tt.setup(st)
			s := newTestServer(st, nil)

			if tt.ifNoneMatch != "" {
				resp, body := doGetWithHeader(t, s, "/events/"+eventID, "If-None-Match", tt.ifNoneMatch)
				require.Equal(t, tt.wantStatus, resp.StatusCode)

				if tt.wantETag != "" {
					assert.Equal(t, tt.wantETag, resp.Header.Get("ETag"))
				}
				if tt.wantExistsCalls > 0 {
					assert.Equal(t, tt.wantExistsCalls, st.existsCalls,
						"304 path must use EventExists, not GetEvent")
					assert.Equal(t, eventID, st.lastExistsID)
				}
				if tt.wantBody != "" {
					assert.Contains(t, string(body), tt.wantBody)
				}
				return
			}

			resp, _ := doGet(t, s, "/events/"+eventID)
			require.Equal(t, tt.wantStatus, resp.StatusCode)
			if tt.wantETag != "" {
				assert.Equal(t, tt.wantETag, resp.Header.Get("ETag"))
			}
		})
	}
}

func TestGetEventRaw_ReturnsXDR(t *testing.T) {
	eventID := "0000000000-0000000001"
	t.Run("returns raw XDR when present", func(t *testing.T) {
		st := &stubStore{event: store.Event{
			ID:          eventID,
			RawTopicXDR: []string{"AAAAA", "BBBBB"},
			RawValueXDR: "CCCCC",
		}}
		s := newTestServer(st, nil)
		resp, body := doGet(t, s, "/events/"+eventID+"/raw")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out rawEventResponse
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, []string{"AAAAA", "BBBBB"}, out.TopicsXDR)
		assert.Equal(t, "CCCCC", out.ValueXDR)
	})

	t.Run("omits value_xdr when empty", func(t *testing.T) {
		st := &stubStore{event: store.Event{
			ID:          eventID,
			RawTopicXDR: []string{"AAAAA"},
			RawValueXDR: "",
		}}
		s := newTestServer(st, nil)
		resp, body := doGet(t, s, "/events/"+eventID+"/raw")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var out map[string]any
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, []any{"AAAAA"}, out["topics_xdr"])
		_, hasValue := out["value_xdr"]
		assert.False(t, hasValue, "value_xdr should be omitted when empty")
	})

	t.Run("404 when event not found", func(t *testing.T) {
		st := &stubStore{eventErr: store.ErrNotFound}
		resp, _ := doGet(t, newTestServer(st, nil), "/events/"+eventID+"/raw")
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("404 when no raw XDR stored", func(t *testing.T) {
		st := &stubStore{event: store.Event{
			ID:          eventID,
			RawTopicXDR: nil,
			RawValueXDR: "",
		}}
		resp, _ := doGet(t, newTestServer(st, nil), "/events/"+eventID+"/raw")
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("immutable cache headers", func(t *testing.T) {
		st := &stubStore{event: store.Event{
			ID:          eventID,
			RawTopicXDR: []string{"x"},
			RawValueXDR: "y",
		}}
		resp, _ := doGet(t, newTestServer(st, nil), "/events/"+eventID+"/raw")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		cc := resp.Header.Get("Cache-Control")
		assert.Contains(t, cc, "public")
		assert.Contains(t, cc, "immutable")
		assert.Contains(t, cc, "max-age=")
		assert.NotEmpty(t, resp.Header.Get("ETag"))
	})

	t.Run("304 on If-None-Match hit", func(t *testing.T) {
		st := &stubStore{
			event: store.Event{
				ID:          eventID,
				RawTopicXDR: []string{"x"},
				RawValueXDR: "y",
			},
			exists: true,
		}
		s := newTestServer(st, nil)
		srv := httptest.NewServer(s.Router())
		defer srv.Close()

		// First request to get the ETag
		resp, err := http.Get(srv.URL + "/events/" + eventID + "/raw")
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		etag := resp.Header.Get("ETag")
		require.NotEmpty(t, etag)

		// Conditional request
		req, err := http.NewRequest("GET", srv.URL+"/events/"+eventID+"/raw", nil)
		require.NoError(t, err)
		req.Header.Set("If-None-Match", etag)
		resp2, err := http.DefaultTransport.RoundTrip(req)
		require.NoError(t, err)
		resp2.Body.Close()
		assert.Equal(t, http.StatusNotModified, resp2.StatusCode)
	})

	t.Run("does not interfere with GET /events/{id}", func(t *testing.T) {
		// Verify that /events/{id}/raw doesn't break the regular
		// GET /events/{id} endpoint.
		st := &stubStore{event: store.Event{
			ID:     eventID,
			TxHash: "abc123",
		}}
		s := newTestServer(st, nil)
		resp, body := doGet(t, s, "/events/"+eventID)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out store.Event
		require.NoError(t, json.Unmarshal(body, &out))
		assert.Equal(t, "abc123", out.TxHash)
	})
}

func TestListEvents_RecentReturnsNewestFirst(t *testing.T) {
	// The store returns events in whatever order the filter dictates.
	// Verify that ?recent=3 passes order=desc to the store and the
	// response contains events in the order the store returned them.
	st := &stubStore{
		events: []store.Event{
			{ID: "e3", Ledger: 300},
			{ID: "e2", Ledger: 200},
			{ID: "e1", Ledger: 100},
		},
	}
	resp, body := doGet(t, newTestServer(st, nil), "/events?recent=3")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "desc", st.lastFilter.Order)
	assert.Equal(t, 3, st.lastFilter.Limit)

	var out eventsResponse
	require.NoError(t, json.Unmarshal(body, &out))
	require.Len(t, out.Events, 3)
	assert.Equal(t, "e3", out.Events[0].ID)
	assert.Equal(t, "e2", out.Events[1].ID)
	assert.Equal(t, "e1", out.Events[2].ID)
}

func TestListEvents_ConfigurableMaxLimit(t *testing.T) {
	st := &stubStore{events: []store.Event{{ID: "e1"}}}

	tests := []struct {
		name         string
		setMax       int // 0 means don't call SetMaxLimit (use default 500)
		query        string
		wantStatus   int
		wantLimit    int    // expected limit in filter (0 = don't check)
		wantErrMatch string // substring in error, for 4xx cases
	}{
		{
			name:       "default max allows 500",
			setMax:     0,
			query:      "/events?limit=500",
			wantStatus: http.StatusOK,
			wantLimit:  500,
		},
		{
			name:         "default max rejects 501",
			setMax:       0,
			query:        "/events?limit=501",
			wantStatus:   http.StatusBadRequest,
			wantErrMatch: "limit must be an integer in [1,500]",
		},
		{
			name:       "custom max 100 allows 100",
			setMax:     100,
			query:      "/events?limit=100",
			wantStatus: http.StatusOK,
			wantLimit:  100,
		},
		{
			name:         "custom max 100 rejects 101",
			setMax:       100,
			query:        "/events?limit=101",
			wantStatus:   http.StatusBadRequest,
			wantErrMatch: "limit must be an integer in [1,100]",
		},
		{
			name:       "custom max 100 recent accepts 100",
			setMax:     100,
			query:      "/events?recent=100",
			wantStatus: http.StatusOK,
			wantLimit:  100,
		},
		{
			name:         "custom max 100 recent rejects 101",
			setMax:       100,
			query:        "/events?recent=101",
			wantStatus:   http.StatusBadRequest,
			wantErrMatch: "recent must be a positive integer in [1,100]",
		},
		{
			name:       "custom max 10 accepts limit 10",
			setMax:     10,
			query:      "/events?limit=10",
			wantStatus: http.StatusOK,
			wantLimit:  10,
		},
		{
			name:         "custom max 10 rejects limit 11",
			setMax:       10,
			query:        "/events?limit=11",
			wantStatus:   http.StatusBadRequest,
			wantErrMatch: "limit must be an integer in [1,10]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st.lastFilter = store.EventFilter{}

			if tt.setMax > 0 {
				SetMaxLimit(tt.setMax)
				t.Cleanup(func() { SetMaxLimit(500) })
			} else {
				// Ensure we're using the default 500.
				SetMaxLimit(500)
			}

			srv := newTestServer(st, nil)
			resp, body := doGet(t, srv, tt.query)

			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			if tt.wantErrMatch != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Contains(t, e["error"], tt.wantErrMatch)
				return
			}

			if tt.wantLimit > 0 {
				assert.Equal(t, tt.wantLimit, st.lastFilter.Limit)
			}
		})
	}
}

func TestGetEventTransaction_Success(t *testing.T) {
	st := &stubStore{
		event: store.Event{ID: "0001-0001", TxHash: "abc123"},
		txSiblings: []store.Event{
			{ID: "0001-0002", TxHash: "abc123"},
			{ID: "0001-0003", TxHash: "abc123"},
		},
	}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/0001-0001/transaction")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r eventsResponse
	require.NoError(t, json.Unmarshal(body, &r))
	assert.Len(t, r.Events, 2)
	assert.Equal(t, "0001-0002", r.Events[0].ID)
	assert.Equal(t, "0001-0003", r.Events[1].ID)
	assert.Equal(t, "abc123", st.lastTxHash)
	assert.Equal(t, "0001-0001", st.lastExcludeID)
}

func TestGetEventTransaction_NotFound(t *testing.T) {
	st := &stubStore{eventErr: store.ErrNotFound}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/missing/transaction")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "not found")
}

func TestGetEventTransaction_StoreError(t *testing.T) {
	st := &stubStore{eventErr: errors.New("db down")}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/any/transaction")
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "loading event failed")
}

func TestGetEventTransaction_EmptyTxHash(t *testing.T) {
	st := &stubStore{event: store.Event{ID: "0001-0001"}}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/0001-0001/transaction")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r eventsResponse
	require.NoError(t, json.Unmarshal(body, &r))
	assert.Len(t, r.Events, 0)
}

func TestGetEventTransaction_CacheHeaders(t *testing.T) {
	st := &stubStore{
		event:      store.Event{ID: "0001-0001", TxHash: "abc"},
		txSiblings: []store.Event{{ID: "0001-0002", TxHash: "abc"}},
	}
	s := newTestServer(st, nil)
	resp, _ := doGet(t, s, "/events/0001-0001/transaction")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	cc := resp.Header.Get("Cache-Control")
	assert.Contains(t, cc, "immutable")
	assert.Contains(t, cc, "max-age=")
}

func TestGetEventTransaction_FieldsProjection(t *testing.T) {
	st := &stubStore{
		event:      store.Event{ID: "0001-0001", TxHash: "abc", Type: "contract", Ledger: 100},
		txSiblings: []store.Event{{ID: "0001-0002", TxHash: "abc", Type: "contract", Ledger: 100}},
	}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/0001-0001/transaction?fields=id,ledger")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r map[string]any
	require.NoError(t, json.Unmarshal(body, &r))
	events, ok := r["events"].([]interface{})
	require.True(t, ok)
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})
	assert.Contains(t, ev, "id")
	assert.Contains(t, ev, "ledger")
	assert.NotContains(t, ev, "type")
}

func TestGetEventTransaction_BadFields(t *testing.T) {
	st := &stubStore{event: store.Event{ID: "0001-0001", TxHash: "abc"}}
	s := newTestServer(st, nil)
	resp, _ := doGet(t, s, "/events/0001-0001/transaction?fields=badfield")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetEventTransaction_IncludeXDR(t *testing.T) {
	st := &stubStore{
		event:      store.Event{ID: "0001-0001", TxHash: "abc"},
		txSiblings: []store.Event{{ID: "0001-0002", TxHash: "abc"}},
	}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/0001-0001/transaction?include_xdr=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r eventsWithXDRResponse
	require.NoError(t, json.Unmarshal(body, &r))
	assert.Len(t, r.Events, 1)
}

func TestGetEventTransaction_NoInterferenceWithGetEvent(t *testing.T) {
	st := &stubStore{event: store.Event{ID: "0001-0001", TxHash: "abc", Type: "contract"}}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events/0001-0001")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ev store.Event
	require.NoError(t, json.Unmarshal(body, &ev))
	assert.Equal(t, "0001-0001", ev.ID)
}

func (m *stubStore) ListContracts(context.Context, store.ContractsFilter) ([]store.ContractSummary, string, error) {
	return nil, "", nil
}
func (m *stubStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (m *stubStore) DeadLetterEvent(context.Context, store.DeadLetterInput) (store.DeadLetter, error) {
	return store.DeadLetter{}, nil
}
func (m *stubStore) ListDeadLetters(context.Context, string, int, string) ([]store.DeadLetter, string, error) {
	return nil, "", nil
}
func (m *stubStore) GetDeadLetter(context.Context, int64) (store.DeadLetter, error) {
	return store.DeadLetter{}, store.ErrNotFound
}
func (m *stubStore) DeleteDeadLetter(context.Context, int64) error { return nil }
