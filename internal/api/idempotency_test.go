package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// idempotencyTestStore wraps store.SQLite for IdempotencyStore persistence and
// counts underlying subscription write calls so tests can prove the handler ran
// at most once on repeat and concurrent calls.
type idempotencyTestStore struct {
	*store.SQLite
	subMu          sync.Mutex
	subs           map[int64]store.Subscription
	nextSubID      int64
	createSubCalls atomic.Int64
	deleteSubCalls atomic.Int64
	failNextCreate atomic.Bool
}

func newIdempotencyTestStore(t *testing.T) *idempotencyTestStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "idempotency.db")
	require.NoError(t, store.Migrate("sqlite://"+dbPath))
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &idempotencyTestStore{
		SQLite: store.NewSQLite(db),
		subs:   make(map[int64]store.Subscription),
	}
}

func (s *idempotencyTestStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	s.createSubCalls.Add(1)
	if s.failNextCreate.CompareAndSwap(true, false) {
		return store.Subscription{}, errors.New("simulated database outage")
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.nextSubID++
	sub.ID = s.nextSubID
	sub.CreatedAt = time.Now().UTC().Truncate(time.Second)
	s.subs[sub.ID] = sub
	return sub, nil
}

func (s *idempotencyTestStore) DeleteSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) error {
	s.deleteSubCalls.Add(1)
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if _, ok := s.subs[id]; !ok {
		return store.ErrNotFound
	}
	delete(s.subs, id)
	return nil
}

type idemTestResponse struct {
	StatusCode int
	Header     http.Header
}

func doIdemRequest(t *testing.T, srv *Server, method, path, apiKey, idemKey, body string) (idemTestResponse, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if idemKey != "" {
		req.Header.Set(IdempotencyKeyHeader, idemKey)
	}
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return idemTestResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
	}, raw
}

func TestIdempotency_RepeatReturnsStoredResponseWithoutReexecuting(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	body := `{"url":"https://example.com/hook","secret":"whsec_z8eP5qL3vR2xK9yB4w","filters":{"type":"contract"},"enabled":true}`
	resp1, raw1 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "sub-create-1", body)
	require.Equal(t, http.StatusCreated, resp1.StatusCode)
	assert.Empty(t, resp1.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(1), st.createSubCalls.Load())

	// Repeat the exact same request with the same Idempotency-Key.
	resp2, raw2 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "sub-create-1", body)
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	assert.Equal(t, "true", resp2.Header.Get(IdempotencyReplayedHeader))
	assert.JSONEq(t, string(raw1), string(raw2), "replayed response must return the exact original subscription ID and signing secret")
	assert.Equal(t, int64(1), st.createSubCalls.Load(), "handler must not execute a second time")

	// Without Idempotency-Key, a second request creates a distinct subscription.
	resp3, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "", body)
	require.Equal(t, http.StatusCreated, resp3.StatusCode)
	assert.Equal(t, int64(2), st.createSubCalls.Load())
}

func TestIdempotency_MismatchOnBodyMethodOrPathReturns409Conflict(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	body1 := `{"url":"https://example.com/hook-a","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	resp1, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "shared-idem-key", body1)
	require.Equal(t, http.StatusCreated, resp1.StatusCode)

	// 1. Different request body with the same key -> 409 Conflict.
	body2 := `{"url":"https://example.com/hook-b","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	respBodyMismatch, rawBodyMismatch := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "shared-idem-key", body2)
	require.Equal(t, http.StatusConflict, respBodyMismatch.StatusCode)
	var errEnv errorResponse
	require.NoError(t, json.Unmarshal(rawBodyMismatch, &errEnv))
	assert.Equal(t, CodeConflict, errEnv.Code)
	assert.Equal(t, IdempotencyKeyHeader, errEnv.Field)

	// 2. Different HTTP method with the same key -> 409 Conflict.
	respMethodMismatch, _ := doIdemRequest(t, srv, http.MethodDelete, "/subscriptions/1", "", "shared-idem-key", "")
	require.Equal(t, http.StatusConflict, respMethodMismatch.StatusCode)

	// 3. Different path with the same key -> 409 Conflict.
	respDel1, _ := doIdemRequest(t, srv, http.MethodDelete, "/subscriptions/1", "", "shared-del-key", "")
	require.Equal(t, http.StatusNoContent, respDel1.StatusCode)
	respPathMismatch, _ := doIdemRequest(t, srv, http.MethodDelete, "/subscriptions/2", "", "shared-del-key", "")
	require.Equal(t, http.StatusConflict, respPathMismatch.StatusCode)

	// Only the original subscription was created.
	assert.Equal(t, int64(1), st.createSubCalls.Load())
}

func TestIdempotency_ConcurrencyRaceExecutesHandlerOnce(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	const workers = 16
	body := `{"url":"https://example.com/concurrent","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)

	statuses := make([]int, workers)
	bodies := make([]string, workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodPost, "/subscriptions", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(IdempotencyKeyHeader, "race-key-1")
			rec := httptest.NewRecorder()
			srv.Router().ServeHTTP(rec, req)
			statuses[idx] = rec.Code
			bodies[idx] = rec.Body.String()
		}(i)
	}

	close(start)
	wg.Wait()

	assert.Equal(t, int64(1), st.createSubCalls.Load(), "concurrent identical requests must execute the mutation exactly once")
	for i := 0; i < workers; i++ {
		assert.Equal(t, http.StatusCreated, statuses[i])
		assert.JSONEq(t, bodies[0], bodies[i], "all concurrent callers must receive the same response body")
	}
}

func TestIdempotency_ExpiryAllowsReuseAfterRetentionWindow(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")
	srv.SetIdempotencyTTL(40 * time.Millisecond)

	body1 := `{"url":"https://example.com/ttl-1","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	resp1, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "expiring-key", body1)
	require.Equal(t, http.StatusCreated, resp1.StatusCode)
	assert.Equal(t, int64(1), st.createSubCalls.Load())

	// Wait for the TTL to expire.
	time.Sleep(70 * time.Millisecond)

	// Reusing the expired key with a different body now succeeds as a fresh request.
	body2 := `{"url":"https://example.com/ttl-2","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	resp2, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "expiring-key", body2)
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	assert.Empty(t, resp2.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(2), st.createSubCalls.Load())

	// Explicit retention sweep removes expired rows cleanly.
	time.Sleep(70 * time.Millisecond)
	deleted, err := srv.SweepExpiredIdempotencyKeys(context.Background(), 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deleted, int64(1))
}

func TestIdempotency_CrossTenantIsolation(t *testing.T) {
	st := newIdempotencyTestStore(t)
	tenants := newFakeTenants()
	keyA := tenants.addTenant(t, store.Tenant{ID: 101, Name: "tenant-a", Wildcard: true, Enabled: true})
	keyB := tenants.addTenant(t, store.Tenant{ID: 202, Name: "tenant-b", Wildcard: true, Enabled: true})

	srv := New(st, nil, slog.Default(), "mgmt-secret").WithMultiTenancy(tenants, MultiTenantOptions{})

	bodyA := `{"url":"https://tenant-a.example.com/hook","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	bodyB := `{"url":"https://tenant-b.example.com/hook","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`

	// Tenant A sends Idempotency-Key "same-key".
	respA1, rawA1 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", keyA, "same-key", bodyA)
	require.Equal(t, http.StatusCreated, respA1.StatusCode)

	// Tenant B sends the exact same Idempotency-Key "same-key" with a different body.
	// Because keys are scoped per tenant, Tenant B must NOT get 409 Conflict or Tenant A's response.
	respB1, rawB1 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", keyB, "same-key", bodyB)
	require.Equal(t, http.StatusCreated, respB1.StatusCode)
	assert.Empty(t, respB1.Header.Get(IdempotencyReplayedHeader))
	assert.NotEqual(t, string(rawA1), string(rawB1))

	// Repeating Tenant A's request replays Tenant A's response.
	respA2, rawA2 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", keyA, "same-key", bodyA)
	require.Equal(t, http.StatusCreated, respA2.StatusCode)
	assert.Equal(t, "true", respA2.Header.Get(IdempotencyReplayedHeader))
	assert.JSONEq(t, string(rawA1), string(rawA2))

	// Repeating Tenant B's request replays Tenant B's response.
	respB2, rawB2 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", keyB, "same-key", bodyB)
	require.Equal(t, http.StatusCreated, respB2.StatusCode)
	assert.Equal(t, "true", respB2.Header.Get(IdempotencyReplayedHeader))
	assert.JSONEq(t, string(rawB1), string(rawB2))
}

func TestIdempotency_ReplayedClientErrorAndUncachedServerError(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	// 1. 400 Bad Request is stored and replayed on repeat with the same key + body.
	badBody := `{"url":"https://example.com/missing-secret","enabled":true}`
	resp1, raw1 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "bad-req-key", badBody)
	require.Equal(t, http.StatusBadRequest, resp1.StatusCode)
	assert.Empty(t, resp1.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(0), st.createSubCalls.Load())

	resp2, raw2 := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "bad-req-key", badBody)
	require.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	assert.Equal(t, "true", resp2.Header.Get(IdempotencyReplayedHeader))
	assert.JSONEq(t, string(raw1), string(raw2))

	// 2. 500 Internal Server Error releases the key so a subsequent retry succeeds.
	st.failNextCreate.Store(true)
	goodBody := `{"url":"https://example.com/retry-after-500","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	respFail, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "transient-500-key", goodBody)
	require.Equal(t, http.StatusInternalServerError, respFail.StatusCode)
	assert.Equal(t, int64(1), st.createSubCalls.Load())

	respRetry, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "transient-500-key", goodBody)
	require.Equal(t, http.StatusCreated, respRetry.StatusCode)
	assert.Empty(t, respRetry.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(2), st.createSubCalls.Load())
}

func TestIdempotency_ReplayedDeleteReturnsOriginal204InsteadOf404(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	createBody := `{"url":"https://example.com/to-delete","secret":"whsec_z8eP5qL3vR2xK9yB4w","enabled":true}`
	createResp, _ := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", "", createBody)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	path := "/subscriptions/1"

	// First DELETE removes the subscription and returns 204 No Content.
	resp1, _ := doIdemRequest(t, srv, http.MethodDelete, path, "", "del-sub-1", "")
	require.Equal(t, http.StatusNoContent, resp1.StatusCode)
	assert.Empty(t, resp1.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(1), st.deleteSubCalls.Load())

	// Second DELETE with the same Idempotency-Key replays 204 No Content instead of 404 Not Found.
	resp2, _ := doIdemRequest(t, srv, http.MethodDelete, path, "", "del-sub-1", "")
	require.Equal(t, http.StatusNoContent, resp2.StatusCode)
	assert.Equal(t, "true", resp2.Header.Get(IdempotencyReplayedHeader))
	assert.Equal(t, int64(1), st.deleteSubCalls.Load())

	// Without Idempotency-Key, deleting the already-removed row returns 404 Not Found.
	resp3, _ := doIdemRequest(t, srv, http.MethodDelete, path, "", "", "")
	require.Equal(t, http.StatusNotFound, resp3.StatusCode)
}

func TestIdempotency_RejectsInvalidHeaderValue(t *testing.T) {
	st := newIdempotencyTestStore(t)
	srv := New(st, nil, slog.Default(), "mgmt-secret")

	overlong := strings.Repeat("a", MaxIdempotencyKeyLength+1)
	resp, raw := doIdemRequest(t, srv, http.MethodPost, "/subscriptions", "", overlong, `{"url":"https://example.com/hook"}`)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var errEnv errorResponse
	require.NoError(t, json.Unmarshal(raw, &errEnv))
	assert.Equal(t, CodeInvalidParameter, errEnv.Code)
	assert.Equal(t, IdempotencyKeyHeader, errEnv.Field)
}
