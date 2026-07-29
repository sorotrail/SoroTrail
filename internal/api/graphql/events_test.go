package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/store"
)

// stubStore is a minimal store.Store implementation. Methods not
// overridden will nil-panic if called — the GraphQL resolvers only
// touch the four below.
type stubStore struct {
	store.Store

	events     []store.Event
	nextCursor string
	queryErr   error
	lastFilter store.EventFilter

	totalCount      int64
	countEventsErr  error
	lastCountFilter store.EventFilter

	event    store.Event
	eventErr error

	watchedList    []store.WatchedContract
	watchedListErr error
}

func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.lastFilter = f
	return s.events, s.nextCursor, s.queryErr
}

func (s *stubStore) CountEvents(_ context.Context, f store.EventFilter) (int64, error) {
	s.lastCountFilter = f
	return s.totalCount, s.countEventsErr
}

func (s *stubStore) GetEvent(_ context.Context, id string) (store.Event, error) {
	if s.eventErr != nil {
		return store.Event{}, s.eventErr
	}
	if s.event.ID != id {
		return store.Event{}, store.ErrNotFound
	}
	return s.event, nil
}

func (s *stubStore) ListWatchedContracts(_ context.Context) ([]store.WatchedContract, error) {
	return s.watchedList, s.watchedListErr
}

// newGraphQLTestServer wires the stub store and returns a Handler.
func newGraphQLTestServer(t *testing.T, st *stubStore) *Handler {
	t.Helper()
	h, err := New(api.ServerDeps{Store: st}, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	require.NoError(t, err)
	return h
}

// postQuery POSTs a GraphQL query document to the test server and
// returns the parsed JSON envelope. Queries must be valid SDL — a
// leading `{` is required.
func postQuery(t *testing.T, h *Handler, q string) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"query": q})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	h.ServeHTTP(w, req)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

// errorsOf extracts the errors[] array safely from the response.
// Returns an empty slice (not nil) when the field is missing or
// null, so callers can assert on contents without a nil-intercept
// panic.
func errorsOf(t *testing.T, resp map[string]any) []any {
	t.Helper()
	v, ok := resp["errors"]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		t.Fatalf("expected errors to be []any, got %T", v)
	}
	return arr
}

// TestGraphQL_KnownContractID is the predicate-friendly contract
// string used across all examples. Kept as a const so a single
// change updates every reference.
const knownContractID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// TestGraphQL_EventsResolver demonstrates the events query against a
// stub store, asserts filter + pagination fields reach the wire as
// expected, and confirms the resolver sent the right shape to the
// store layer.
func TestGraphQL_EventsResolver(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	st := &stubStore{
		events: []store.Event{
			{ID: "0000000001-0000000001", ContractID: "CABC", Ledger: 1, Type: "contract", CreatedAt: now},
			{ID: "0000000001-0000000002", ContractID: "CABC", Ledger: 2, Type: "contract", CreatedAt: now},
		},
		nextCursor: "0000000001-0000000002",
		totalCount: 42,
	}
	h := newGraphQLTestServer(t, st)

	q := `query Q($id: String!) {
	  events(filter:{contractId:$id, page:{first:10}}) {
	    nodes { id contractId ledger type }
	    edges { cursor }
	    pageInfo { hasNextPage endCursor }
	    totalCount
	  }
	}`
	body, _ := json.Marshal(map[string]any{
		"query":     q,
		"variables": map[string]any{"id": knownContractID},
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	h.ServeHTTP(w, req)
	resp := map[string]any{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Empty(t, errorsOf(t, resp), "no errors expected, got %v", resp["errors"])
	data, ok := resp["data"].(map[string]any)
	require.True(t, ok)
	events := data["events"].(map[string]any)
	nodes := events["nodes"].([]any)
	require.Len(t, nodes, 2)
	assert.Equal(t, "0000000001-0000000001", nodes[0].(map[string]any)["id"])
	assert.Equal(t, "CABC", nodes[0].(map[string]any)["contractId"])

	pageInfo := events["pageInfo"].(map[string]any)
	assert.Equal(t, true, pageInfo["hasNextPage"])
	assert.NotEmpty(t, pageInfo["endCursor"])

	total := events["totalCount"]
	assert.Equal(t, float64(42), total)
	assert.Equal(t, knownContractID, st.lastFilter.ContractID,
		"store should see the requested contractId")
}

// TestGraphQL_EventByID demonstrates single-event lookup returns the
// object, or null when not in the store.
func TestGraphQL_EventByID(t *testing.T) {
	st := &stubStore{
		event: store.Event{ID: "1", ContractID: "CABC", Ledger: 1, Type: "contract"},
	}
	h := newGraphQLTestServer(t, st)

	resp := postQuery(t, h, `{ event(id:"1") { id contractId ledger } }`)
	data := resp["data"].(map[string]any)
	ev := data["event"].(map[string]any)
	require.NotNil(t, ev)
	assert.Equal(t, "1", ev["id"])

	resp2 := postQuery(t, h, `{ event(id:"missing") { id } }`)
	data2 := resp2["data"].(map[string]any)
	assert.Nil(t, data2["event"], "missing event id should serialize as null")
}

// TestGraphQL_StoreErrorSurfacesInEnvelope ensures a store-side
// error is propagated through the GraphQL errors[] envelope with the
// original message preserved on the wire.
func TestGraphQL_StoreErrorSurfacesInEnvelope(t *testing.T) {
	st := &stubStore{queryErr: errors.New("db connection lost")}
	h := newGraphQLTestServer(t, st)

	resp := postQuery(t, h, `{ events { totalCount } }`)
	errs := errorsOf(t, resp)
	require.NotEmpty(t, errs)
	assert.Contains(t, errs[0].(map[string]any)["message"], "db connection lost")
}

// TestGraphQL_UnknownFieldReturnsEnvelope confirms a syntactically
// valid but schema-unsupported root field is surfaced as an error
// rather than a 200 with empty data.
func TestGraphQL_UnknownFieldReturnsEnvelope(t *testing.T) {
	st := &stubStore{}
	h := newGraphQLTestServer(t, st)

	resp := postQuery(t, h, `{ notARealField { id } }`)
	errs := errorsOf(t, resp)
	require.NotEmpty(t, errs)
	assert.Contains(t, errs[0].(map[string]any)["message"], "notARealField")
}

// TestGraphQL_ContractsResolver demonstrates that the watched
// contracts query runs against the store and emits the Connection
// envelope (edges/nodes/pageInfo/totalCount).
func TestGraphQL_ContractsResolver(t *testing.T) {
	st := &stubStore{
		watchedList: []store.WatchedContract{
			{ContractID: "CABC", AddedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
			{ContractID: "CDEF", AddedAt: time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)},
		},
	}
	h := newGraphQLTestServer(t, st)

	resp := postQuery(t, h, `{ contracts { nodes { contractId addedAt } totalCount } }`)
	require.Empty(t, errorsOf(t, resp))
	data := resp["data"].(map[string]any)
	contracts := data["contracts"].(map[string]any)
	nodes := contracts["nodes"].([]any)
	require.Len(t, nodes, 2)
	assert.Equal(t, "CABC", nodes[0].(map[string]any)["contractId"])
	assert.Equal(t, float64(2), contracts["totalCount"])
}
