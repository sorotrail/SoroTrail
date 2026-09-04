package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/apikey"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// authStubStore wraps stubStore with in-memory API key persistence so
// the auth middleware and key-management handlers can be tested without
// a database.
type authStubStore struct {
	*stubStore

	apiKeys map[string]store.APIKey // by prefix
	byID    map[int64]store.APIKey
	nextID  int64
}

func newAuthStubStore() *authStubStore {
	return &authStubStore{
		stubStore: &stubStore{},
		apiKeys:   map[string]store.APIKey{},
		byID:      map[int64]store.APIKey{},
	}
}

func (s *authStubStore) CreateAPIKey(_ context.Context, k store.APIKey) (store.APIKey, error) {
	s.nextID++
	k.ID = s.nextID
	k.CreatedAt = time.Now()
	s.byID[k.ID] = k
	s.apiKeys[k.Prefix] = k
	return k, nil
}

func (s *authStubStore) GetAPIKey(_ context.Context, id int64) (store.APIKey, error) {
	k, ok := s.byID[id]
	if !ok {
		return store.APIKey{}, store.ErrNotFound
	}
	return k, nil
}

func (s *authStubStore) LookupAPIKeyByPrefix(_ context.Context, prefix string) (store.APIKey, error) {
	k, ok := s.apiKeys[prefix]
	if !ok || k.Revoked() {
		return store.APIKey{}, store.ErrNotFound
	}
	return k, nil
}

func (s *authStubStore) ListAPIKeys(context.Context) ([]store.APIKey, error) {
	out := make([]store.APIKey, 0, len(s.byID))
	for _, k := range s.byID {
		out = append(out, k)
	}
	return out, nil
}

// GetSubscription returns a canned subscription so write paths that
// read-then-update (PUT /subscriptions/{id}) work in tests.
func (s *authStubStore) GetSubscription(_ context.Context, id int64) (store.Subscription, error) {
	return store.Subscription{ID: id, URL: "https://example.com/hook", Secret: "whsec_x", Enabled: true}, nil
}

func (s *authStubStore) RevokeAPIKey(_ context.Context, id int64) error {
	k, ok := s.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now()
	k.RevokedAt = &now
	s.byID[id] = k
	s.apiKeys[k.Prefix] = k
	return nil
}

// addKey mints a real key (random secret + bcrypt hash) and registers it
// in the stub store. Returns the full key for presenting on requests.
func (s *authStubStore) addKey(t *testing.T, name string) string {
	t.Helper()
	key, prefix, secret, err := apikey.Generate()
	require.NoError(t, err)
	hash, err := apikey.HashSecret(secret)
	require.NoError(t, err)
	_, err = s.CreateAPIKey(context.Background(), store.APIKey{Name: name, Prefix: prefix, KeyHash: hash})
	require.NoError(t, err)
	return key
}

func newAuthServer(st store.Store, enabled bool) *Server {
	rc := &stubRPC{health: rpc.Health{Status: "healthy"}}
	s := New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.WithAPIKeyAuth(enabled)
	return s
}

// doReq runs a request against the server and returns the response.
func doReq(t *testing.T, s *Server, method, path, body string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	rb, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, rb
}

// --- Auth disabled: existing behavior is preserved ---

func TestAuth_DisabledKeepsWritesOpen(t *testing.T) {
	st := newAuthStubStore()
	s := newAuthServer(st, false)

	// Write endpoints work without any key.
	resp, body := doReq(t, s, http.MethodPost, "/subscriptions",
		`{"url":"https://example.com/hook","secret":"whsec_x"}`, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))

	// Streaming is reachable (no broadcaster wired → 501, not 401).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/events/ws"
	_, respWS, err := websocket.Dial(ctx, wsURL, nil)
	require.Error(t, err)
	require.NotNil(t, respWS)
	assert.Equal(t, http.StatusNotImplemented, respWS.StatusCode, "streaming must not require a key when auth is off")

	// Key management is still gated — an open create endpoint would let
	// anyone mint keys.
	resp, body = doReq(t, s, http.MethodPost, "/apikeys", `{}`, nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, string(body))
}

// --- Auth enabled: writes and streaming require a valid key ---

func TestAuth_EnabledRejectsRequestsWithoutKey(t *testing.T) {
	st := newAuthStubStore()
	s := newAuthServer(st, true)

	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/subscriptions", `{"url":"https://example.com/hook","secret":"whsec_x"}`},
		{http.MethodPut, "/subscriptions/1", `{}`},
		{http.MethodDelete, "/subscriptions/1", ""},
		{http.MethodGet, "/subscriptions", ""},
		{http.MethodGet, "/subscriptions/1", ""},
		{http.MethodGet, "/subscriptions/1/deliveries", ""},
		{http.MethodPost, "/apikeys", `{}`},
		{http.MethodGet, "/apikeys", ""},
		{http.MethodDelete, "/apikeys/1", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp, body := doReq(t, s, tc.method, tc.path, tc.body, nil)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, string(body))
			assert.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.NotEmpty(t, e["error"])
		})
	}
}

func TestAuth_EnabledGatesWebSocket(t *testing.T) {
	st := newAuthStubStore()
	s := newAuthServer(st, true)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/events/ws"

	// Without a key the upgrade is rejected with 401.
	_, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// With a valid key the upgrade proceeds (no broadcaster → 501, not
	// a 401, proving the key was accepted).
	key := st.addKey(t, "streamer")
	hdr := http.Header{"Authorization": {"Bearer " + key}}
	_, resp, err = websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: hdr})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

func TestAuth_EnabledAcceptsValidKey(t *testing.T) {
	st := newAuthStubStore()
	key := st.addKey(t, "ci")
	s := newAuthServer(st, true)

	// Write endpoint accepts the key (bearer header).
	resp, body := doReq(t, s, http.MethodPost, "/subscriptions",
		`{"url":"https://example.com/hook","secret":"whsec_x"}`,
		map[string]string{"Authorization": "Bearer " + key})
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))

	// X-API-Key header is an equally valid way to present it.
	resp, body = doReq(t, s, http.MethodPut, "/subscriptions/1", `{"enabled":false}`,
		map[string]string{"X-API-Key": key})
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	// Read endpoints stay open without a key.
	resp, body = doReq(t, s, http.MethodGet, "/events", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	resp, body = doReq(t, s, http.MethodGet, "/health", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, string(body))
}

func TestAuth_RejectsBadKeys(t *testing.T) {
	st := newAuthStubStore()
	st.addKey(t, "real")
	s := newAuthServer(st, true)

	badKeys := []string{
		"",                              // nothing
		"not-a-key",                     // wrong shape
		"sorotrail_",                    // truncated
		"sorotrail_short",               // no separator
		"other_aaaaaaaaaaaaaaaa_secret", // wrong scheme
		"sorotrail_" + strings.Repeat("a", 16) + "_" + strings.Repeat("b", 64), // unknown prefix
	}
	for _, bad := range badKeys {
		hdr := map[string]string{"Authorization": "Bearer " + bad}
		resp, body := doReq(t, s, http.MethodPost, "/subscriptions",
			`{"url":"https://example.com/hook","secret":"whsec_x"}`, hdr)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
			"key %q must be rejected: %s", bad, string(body))
	}

	// A key with a real prefix but a wrong secret is rejected too.
	key, prefix, _, err := apikey.Generate()
	require.NoError(t, err)
	// Register a different secret under this prefix — simulating a key
	// whose stored hash does not match the presented secret.
	otherSecret := strings.Repeat("0", 64)
	hash, err := apikey.HashSecret(otherSecret)
	require.NoError(t, err)
	_, err = st.CreateAPIKey(context.Background(), store.APIKey{Name: "tampered", Prefix: prefix, KeyHash: hash})
	require.NoError(t, err)
	resp, body := doReq(t, s, http.MethodPost, "/subscriptions",
		`{"url":"https://example.com/hook","secret":"whsec_x"}`,
		map[string]string{"Authorization": "Bearer " + key})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, string(body))
}

// --- Key management lifecycle ---

func TestAuth_IssueListRevokeLifecycle(t *testing.T) {
	st := newAuthStubStore()
	adminKey := st.addKey(t, "admin")
	s := newAuthServer(st, true)
	auth := map[string]string{"Authorization": "Bearer " + adminKey}

	// Issue a new key via the endpoint.
	resp, body := doReq(t, s, http.MethodPost, "/apikeys", `{"name":"deploy"}`, auth)
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))
	var created struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Prefix string `json:"prefix"`
		Key    string `json:"key"`
	}
	require.NoError(t, json.Unmarshal(body, &created))
	assert.Equal(t, "deploy", created.Name)
	assert.NotEmpty(t, created.Prefix)
	assert.True(t, strings.HasPrefix(created.Key, apikey.SchemePrefix), "full key is returned once")
	assert.NotContains(t, string(body), "key_hash", "the stored hash is never serialized")

	// The new key works on a protected endpoint.
	resp, body = doReq(t, s, http.MethodPost, "/subscriptions",
		`{"url":"https://example.com/hook","secret":"whsec_x"}`,
		map[string]string{"Authorization": "Bearer " + created.Key})
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))

	// List shows both keys.
	resp, body = doReq(t, s, http.MethodGet, "/apikeys", "", auth)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	var listed []store.APIKey
	require.NoError(t, json.Unmarshal(body, &listed))
	require.Len(t, listed, 2)
	assert.NotContains(t, string(body), "key_hash")

	// Revoke the new key; it stops working immediately.
	resp, body = doReq(t, s, http.MethodDelete, fmt.Sprintf("/apikeys/%d", created.ID), "", auth)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, string(body))

	resp, body = doReq(t, s, http.MethodPost, "/subscriptions",
		`{"url":"https://example.com/hook","secret":"whsec_x"}`,
		map[string]string{"Authorization": "Bearer " + created.Key})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"revoked key must stop working immediately: %s", string(body))

	// The admin key still works.
	resp, body = doReq(t, s, http.MethodGet, "/apikeys", "", auth)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	// Revoking an unknown key 404s.
	resp, _ = doReq(t, s, http.MethodDelete, "/apikeys/999", "", auth)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Bad id shape 400s.
	resp, _ = doReq(t, s, http.MethodDelete, "/apikeys/abc", "", auth)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
