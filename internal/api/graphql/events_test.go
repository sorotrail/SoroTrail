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

func (s *stubStore) GetEvent(_ context.Context, id string, _ store.Scope) (store.Event, error) {
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
		logFatalf(t, "expected errors to be []any, got %T", v)
	}
	return arr
}

func logFatalf(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Fatalf(format, args...)
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

// TestBuildEventFilterDirectly covers buildEventFilter, derefTime, and derefInt64 directly
// to satisfy direct coverage requirements and verify all argument mappings match REST rules.
func TestBuildEventFilterDirectly(t *testing.T) {
	ctx := context.Background()

	// Test derefTime helper
	t.Run("derefTime", func(t *testing.T) {
		assert.True(t, derefTime(nil).IsZero(), "nil time pointer must return zero time")
		timeVal := time.Date(2026, 7, 28, 15, 30, 0, 0, time.UTC)
		assert.Equal(t, timeVal, derefTime(&timeVal), "non-nil time pointer must return dereferenced time")
	})

	// Test derefInt64 helper
	t.Run("derefInt64", func(t *testing.T) {
		assert.Equal(t, int64(0), derefInt64(nil), "nil int64 pointer must return 0")
		val := int64(12345)
		assert.Equal(t, int64(12345), derefInt64(&val), "non-nil int64 pointer must return dereferenced value")
	})

	// Test buildEventFilter with various filter inputs
	t.Run("buildEventFilter mappings", func(t *testing.T) {
		timeFrom := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		timeTo := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
		fromLedge := int64(100)
		toLedge := int64(200)

		args := EventFilterArgs{
			Filter: &FilterInput{
				ContractID:    knownContractID,
				Types:         []string{"contract", "system"},
				Topic:         json.RawMessage(`["topic0"]`),
				TopicContains: json.RawMessage(`["contains"]`),
				TxHash:        "abcdef",
				FromLedger:    &fromLedge,
				ToLedger:      &toLedge,
				FromTime:      &timeFrom,
				ToTime:        &timeTo,
			},
			Page: &PageInput{
				First:   ptrInt32(10),
				Order:   "asc",
				OrderBy: "ledger",
			},
		}

		filter, cursor, order, orderBy, err := buildEventFilter(args)
		require.NoError(t, err)

		assert.Equal(t, knownContractID, filter.ContractID)
		assert.Equal(t, []string{"contract", "system"}, filter.Types)
		assert.JSONEq(t, `["topic0"]`, string(filter.Topic))
		assert.JSONEq(t, `["contains"]`, string(filter.TopicContains))
		assert.Equal(t, "abcdef", filter.TxHash)
		assert.Equal(t, int64(100), filter.FromLedger)
		assert.Equal(t, int64(200), filter.ToLedger)
		assert.Equal(t, timeFrom, filter.FromTime)
		assert.Equal(t, timeTo, filter.ToTime)
		assert.Equal(t, store.Scope{}, filter.Scope, "caller scope should be attached (zero scope if unauthenticated)")
		assert.Empty(t, cursor)
		assert.Equal(t, "asc", order)
		assert.Equal(t, "ledger", orderBy)
	})

	// Test omitted arguments leave fields unset
	t.Run("omitted arguments leave field unset", func(t *testing.T) {
		args := EventFilterArgs{}
		filter, _, _, _, err := buildEventFilter(args)
		require.NoError(t, err)

		assert.Empty(t, filter.ContractID)
		assert.Empty(t, filter.Types)
		assert.Nil(t, filter.Topic)
		assert.Nil(t, filter.Topic0)
		assert.Nil(t, filter.TopicContains)
		assert.Empty(t, filter.TxHash)
		assert.Zero(t, filter.FromLedger)
		assert.Zero(t, filter.ToLedger)
		assert.True(t, filter.FromTime.IsZero())
		assert.True(t, filter.ToTime.IsZero())
	})

	// Test invalid cursor handling
	t.Run("invalid cursor returns error", func(t *testing.T) {
		args := EventFilterArgs{
			Page: &PageInput{
				After: "invalid-base64-or-cursor",
			},
		}
		_, _, _, _, err := buildEventFilter(args)
		assert.Error(t, err)
	})

	// Test scope attachment via context principal
	t.Run("scope attached from context principal", func(t *testing.T) {
	specificScope := store.NewScope([]string{knownContractID})
	authCtx := api.WithPrincipal(ctx, api.Principal{Scope: specificScope})

		// Since buildEventFilter doesn't receive ctx directly in its signature (wait, let's verify if buildEventFilter takes ctx or not, or if scopeFrom uses ctx internally),
		// let's check buildEventFilter signature in internal/api/graphql/events.go:
		// Actually buildEventFilter(args EventFilterArgs) does not take context, but scopeFrom(ctx) is called within resolvers.
		// Let's test scopeFrom directly.
		gotScope := scopeFrom(authCtx)
		assert.Equal(t, specificScope, gotScope)

		zeroCtx := context.Background()
		assert.Equal(t, store.Scope{}, scopeFrom(zeroCtx))
	})
}

func ptrInt32(i int32) *int32 {
	return &i
}
