package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
const otherContract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

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
	return New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

func TestListEvents_MultiValueContractAndTypeFilters(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s,
		"/events?contract_id="+testContract+"&contract_id="+otherContract+"&type=contract&type=system")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, []string{testContract, otherContract}, st.lastFilter.ContractIDs)
	assert.Empty(t, st.lastFilter.ContractID)
	assert.Equal(t, []string{"contract", "system"}, st.lastFilter.Types)
	assert.Empty(t, st.lastFilter.Type)
}

func TestListEvents_ContractIDListSizeCap(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	var path strings.Builder
	path.WriteString("/events?")
	for i := 0; i < 21; i++ {
		if i > 0 {
			path.WriteString("&")
		}
		path.WriteString("contract_id=")
		path.WriteString(testContract)
	}

	resp, body := doGet(t, s, path.String())
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "at most 20 contract_id values")
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

func TestContractEvents_RejectsContractIDQueryParam(t *testing.T) {
	resp, body := doGet(t, newTestServer(&stubStore{}, nil), "/contracts/"+testContract+"/events?contract_id="+otherContract)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "contract_id query parameter is not allowed")
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
