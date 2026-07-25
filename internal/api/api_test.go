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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/buildinfo"
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

func (s *stubStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (s *stubStore) SetContractSpec(context.Context, string, string, []byte) error {
	return nil
}

// EventExists is the cheap 304 path; tests assert the handler uses it
// (instead of GetEvent) when If-None-Match matches.
func (s *stubStore) EventExists(_ context.Context, id string) (bool, error) {
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

func (s *stubStore) Stats(context.Context) (store.Stats, error) { return s.stats, nil }
func (s *stubStore) Ping(context.Context) error                 { return s.pingErr }
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

// Subscription stubs for the webhook feature.
func (s *stubStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (s *stubStore) GetSubscription(_ context.Context, id int64) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (s *stubStore) ListSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) UpdateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) DeleteSubscription(context.Context, int64) error { return nil }
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
func (s *stubStore) ListDeliveryAttempts(context.Context, int64, int) ([]store.DeliveryAttempt, error) {
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
		"/events?limit=abc",
		"/events?cursor=bad%20cursor",
		"/events?cursor=e1%3BDROP",
		"/events?cursor=%3Cscript%3E",
		"/events?cursor=cursor%27OR%271%3D%271",
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
