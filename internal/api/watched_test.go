package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

const watchedTestKey = "watched-test-key"
const watchedOtherContract = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"

// watchedRoute hits the watched-contracts surface with the given HTTP method
// and path (relative to /watched-contracts), optionally adding the API key
// header and a JSON body.
func watchedRoute(t *testing.T, s *Server, method, path string, withKey bool, body any) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	var rdr *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, srv.URL+"/watched-contracts"+path, rdr)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if withKey {
		req.Header.Set("X-API-Key", watchedTestKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	require.NoError(t, err)
	return resp, buf.Bytes()
}

// All three endpoints demand the X-API-Key header. A request without it is
// rejected — including the read endpoints, since the watch list is part of
// the write surface and auth gating is symmetric.
func TestWatchedContracts_RequiresAuth(t *testing.T) {
	ts := newTestServerWithKey(&stubStore{}, nil, watchedTestKey)

	methods := []struct {
		name, method, path string
		body               any
	}{
		{"get", http.MethodGet, "", nil},
		{"post", http.MethodPost, "", map[string]string{"contract_id": "C"}},
		{"delete", http.MethodDelete, "/" + testContract, nil},
	}
	for _, tc := range methods {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := watchedRoute(t, ts, tc.method, tc.path, false, tc.body)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, string(body))
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.Contains(t, e["error"], "X-API-Key")
		})
	}
}

// No API_KEY env configured = the surface starts up rejected (503) so it
// never silently accepts an unauthenticated request even when other auth
// would be disabled elsewhere.
func TestWatchedContracts_NoAPIKeyConfigured_Returns503(t *testing.T) {
	ts := newTestServerWithKey(&stubStore{}, nil, "") // empty key

	resp, _ := watchedRoute(t, ts, http.MethodGet, "", true, nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	resp, _ = watchedRoute(t, ts, http.MethodPost, "", true,
		map[string]string{"contract_id": testContract})
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestWatchedContracts_PostValidation(t *testing.T) {
	ts := newTestServerWithKey(&stubStore{}, nil, watchedTestKey)

	t.Run("missing contract_id", func(t *testing.T) {
		resp, _ := watchedRoute(t, ts, http.MethodPost, "", true, map[string]string{})
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("invalid contract_id", func(t *testing.T) {
		resp, _ := watchedRoute(t, ts, http.MethodPost, "", true,
			map[string]string{"contract_id": "not-a-strkey"})
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	// Switching from empty list to a specific list is guarded.
	t.Run("empty to non-empty requires confirm", func(t *testing.T) {
		ts := newTestServerWithKey(&stubStore{}, nil, watchedTestKey)
		resp, body := watchedRoute(t, ts, http.MethodPost, "", true,
			map[string]string{"contract_id": testContract})
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
		var e map[string]any
		require.NoError(t, json.Unmarshal(body, &e))
		assert.Contains(t, e["error"], "?confirm=true")
	})

	t.Run("empty to non-empty with confirm=true succeeds", func(t *testing.T) {
		st := &stubStore{
			ingestionState: &store.IngestionState{LastIngestedLedger: 999},
		}
		ts := newTestServerWithKey(st, nil, watchedTestKey)

		resp, body := watchedRoute(t, ts, http.MethodPost, "?confirm=true", true,
			map[string]string{"contract_id": testContract})
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

		var got addWatchedResponse
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, testContract, got.ContractID)
		assert.Equal(t, int64(999), got.HistoryFromLedger,
			"new contract history starts at the current cursor — operators should see this")
		assert.Equal(t, "all_to_specific", got.ModeTransition)

		require.Equal(t, []string{testContract}, st.added)
	})

	t.Run("non-empty add does not require confirm", func(t *testing.T) {
		st := &stubStore{
			watchedList: []store.WatchedContract{{ContractID: watchedOtherContract}},
		}
		ts := newTestServerWithKey(st, nil, watchedTestKey)

		resp, body := watchedRoute(t, ts, http.MethodPost, "", true,
			map[string]string{"contract_id": testContract})
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

		var got addWatchedResponse
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Empty(t, got.ModeTransition,
			"non-empty -> non-empty is not an ingestion-semantic change")
		require.Equal(t, []string{testContract}, st.added)
	})
}

func TestWatchedContracts_PostIncludesHistoryFromLedger(t *testing.T) {
	st := &stubStore{
		watchedList:    []store.WatchedContract{{ContractID: watchedOtherContract}},
		ingestionState: &store.IngestionState{LastIngestedLedger: 50_000},
	}
	ts := newTestServerWithKey(st, nil, watchedTestKey)

	resp, body := watchedRoute(t, ts, http.MethodPost, "", true,
		map[string]string{"contract_id": testContract})
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var got addWatchedResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, int64(50_000), got.HistoryFromLedger,
		"the response MUST surface where historical replay starts so callers don't discover the gap later")
}

func TestWatchedContracts_PostColdStartHistoryIsZero(t *testing.T) {
	st := &stubStore{
		watchedList:      []store.WatchedContract{{ContractID: watchedOtherContract}},
		ingestionState:   nil,
		ingestionStateEr: store.ErrNotFound, // explicit: production path
	}
	ts := newTestServerWithKey(st, nil, watchedTestKey)

	resp, body := watchedRoute(t, ts, http.MethodPost, "", true,
		map[string]string{"contract_id": testContract})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got addWatchedResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, int64(0), got.HistoryFromLedger,
		"with no ingestion state yet, history_from_ledger is 0")
}

func TestWatchedContracts_DeleteValidation(t *testing.T) {
	ts := newTestServerWithKey(&stubStore{}, nil, watchedTestKey)

	t.Run("invalid contract id", func(t *testing.T) {
		resp, _ := watchedRoute(t, ts, http.MethodDelete, "/junk", true, nil)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("not in list yields 404", func(t *testing.T) {
		st := &stubStore{
			watchedList: []store.WatchedContract{{ContractID: watchedOtherContract}},
			removeErr:   store.ErrNotFound,
		}
		ts := newTestServerWithKey(st, nil, watchedTestKey)
		resp, _ := watchedRoute(t, ts, http.MethodDelete, "/"+testContract, true, nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// Removing the last contract flips ingestion back to "all contract events".
// The guard is symmetric with the empty->non-empty one: either direction
// silently changes what's stored, both deserve a confirm.
func TestWatchedContracts_DeleteLastContractRequiresConfirm(t *testing.T) {
	st := &stubStore{
		watchedList: []store.WatchedContract{{ContractID: testContract}},
	}
	ts := newTestServerWithKey(st, nil, watchedTestKey)

	// Without confirm but with a valid API key: the auth check passes and
	// the confirm guard fires.
	resp, body := watchedRoute(t, ts, http.MethodDelete, "/"+testContract, true, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
	var e map[string]any
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "?confirm=true")
	assert.Empty(t, st.removed, "no removal must have happened")

	// With confirm: succeeds and explains history stays put.
	resp, body = watchedRoute(t, ts, http.MethodDelete, "/"+testContract+"?confirm=true", true, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var got removeWatchedResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, testContract, got.ContractID)
	assert.True(t, got.HistoryPreserved, "history preservation is part of the response contract")
	assert.Equal(t, "specific_to_all", got.ModeTransition)
}

func TestWatchedContracts_DeleteNonLastDoesNotRequireConfirm(t *testing.T) {
	st := &stubStore{
		watchedList: []store.WatchedContract{
			{ContractID: testContract},
			{ContractID: watchedOtherContract},
		},
	}
	ts := newTestServerWithKey(st, nil, watchedTestKey)

	resp, body := watchedRoute(t, ts, http.MethodDelete, "/"+testContract, true, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var got removeWatchedResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Empty(t, got.ModeTransition,
		"removing one of many contracts isn't an ingestion-mode change")
	assert.True(t, got.HistoryPreserved)
}

func TestWatchedContracts_GetReturnsList(t *testing.T) {
	st := &stubStore{
		watchedList: []store.WatchedContract{
			{ContractID: testContract},
			{ContractID: watchedOtherContract},
		},
	}
	ts := newTestServerWithKey(st, nil, watchedTestKey)

	resp, body := watchedRoute(t, ts, http.MethodGet, "", true, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var got watchedListResponse
	require.NoError(t, json.Unmarshal(body, &got))
	require.Len(t, got.Contracts, 2)
	assert.Equal(t, 2, got.Count)
}
