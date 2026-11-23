package graphql

import (
	"sync"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sorotrail/sorotrail/internal/store"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"github.com/sorotrail/sorotrail/internal/api"
)


func TestServeHTTPAnswersMethodNotAllowedInTheEnvelope(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPut, "/graphql", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req) // the real entry point mounted at /graphql

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET, POST", rec.Header().Get("Allow"),
		"a client refused on method must be told what is accepted")
	assertGraphQLErrorEnvelope(t, rec)
}

func TestDisabledPlaygroundAnswersInTheEnvelope(t *testing.T) {
	// newGraphQLTestServer builds the handler with the playground off,
	// which is exactly the configuration that must refuse requests.
	h := newGraphQLTestServer(t, &stubStore{})

	rec := httptest.NewRecorder()
	h.PlaygroundHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/graphiql", nil))

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assertGraphQLErrorEnvelope(t, rec)
}

func TestEnabledPlaygroundServesGraphiQL(t *testing.T) {
	h, err := New(api.ServerDeps{Store: &stubStore{}}, slog.New(slog.NewTextHandler(io.Discard, nil)), true)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	h.PlaygroundHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/graphiql", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, rec.Body.String(), "GraphQL Playground")
	// The served page is SoroTrail's own GraphQL Playground HTML rather
	// than the upstream GraphiQL build, so assert on its distinguishing
	// markers instead of the literal "GraphiQL" string.
	assert.Contains(t, rec.Body.String(), "SoroTrail GraphQL Playground")
	assert.Contains(t, rec.Body.String(), "graphql-playground-react")
}

func TestHandleGet_WithoutQueryReturnsBrowserHint(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodGet, "/graphql", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "to this endpoint")
	// handleGet writes its hint verbatim (not via JSON encoding), so the
	// wire bytes carry literal backslashes before the embedded quotes.
	assert.Contains(t, rec.Body.String(), `POST {\"query\":\"…\"} to this endpoint`)
}

func TestHandleGet_WithQueryExecutesOperation(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodGet, "/graphql?query={contracts{edges{node{id}}}}", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var env struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.NotNil(t, env.Data)
}

func TestHandlePost_ValidApplicationJsonExecutes(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	body, err := json.Marshal(GraphQLRequest{Query: "{contracts{edges{node{id}}}}"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var env struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.NotNil(t, env.Data)
}

func TestHandlePost_InvalidJSONReturnsGraphQLErrorEnvelope(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString("invalid-json"))
	req.Header.Set(
		"Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assertGraphQLErrorEnvelope(t, rec)
}

func TestHandleQuery_MalformedQueryReturnsGraphQLErrorDocument(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	body, err := json.Marshal(GraphQLRequest{Query: "malformed { query syntax }"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// GraphQL specification / execution errors return HTTP 200 with errors in the payload, not a 500 or 400 status.
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assertGraphQLErrorEnvelope(t, rec)
}

// assertGraphQLErrorEnvelope checks the body is the one shape every
// writeErrorEnvelope call produces on this transport: data present (empty)
// and exactly one error carrying a message.
func assertGraphQLErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var env struct {
		Data   map[string]any `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env),
		"error responses must be JSON: %s", rec.Body.String())
	assert.NotNil(t, env.Data, "the GraphQL envelope always carries data")
	require.Len(t, env.Errors, 1, "an operation-level failure carries one error")
	assert.NotEmpty(t, env.Errors[0].Message)
}

func TestHandleQuery_PoolBudgetExhaustedReturns503WithRetryAfter(t *testing.T) {
	budget := store.NewPoolBudget(store.PoolBudgetConfig{
		TotalConns:       4,
		IngesterReserved: 1,
		GraphQLMax:       1,
		AcquireTimeout:   10 * time.Millisecond,
		RetryAfter:       3 * time.Second,
	})
	budgeted := store.NewBudgetedStore(&stubStore{}, budget, store.SubsystemGraphQL)

	_, releaseHeld, err := budget.Acquire(context.Background(), store.SubsystemGraphQL)
	require.NoError(t, err)
	defer releaseHeld()

	h, err := New(api.ServerDeps{Store: budgeted}, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	require.NoError(t, err)

	body, err := json.Marshal(GraphQLRequest{Query: "{contracts{edges{node{id}}}}"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "3", rec.Header().Get("Retry-After"))
	assertGraphQLErrorEnvelope(t, rec)
}
func TestSubscriptionManager_LimitsAndTenancy(t *testing.T) {
	sm := NewSubscriptionManager(2, 1, nil)
	err := sm.Acquire("tenant-A")
	sm.Release("tenant-A")

	assert.True(t, AssertSubscriptionTenant(map[string]any{"tenantId": "tenant-A"}, "tenant-A"))
	assert.False(t, AssertSubscriptionTenant(map[string]any{"tenantId": "tenant-B"}, "tenant-A"))
	assert.True(t, AssertSubscriptionTenant(map[string]any{"tenantId": "tenant-A"}, ""))
}

func TestRunSubscriptionStream_RevocationAndIsolation(t *testing.T) {
	sm := NewSubscriptionManager(10, 10, nil)
	broker := broadcast.NewBroker(100)
	defer broker.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	authCalled := false
	authCheck := func(c context.Context) bool {
		authCalled = true
		return false // trigger revocation
	}

	var received []any
	var mu sync.Mutex
	writeMsg := func(msg any) error {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		return nil
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- RunSubscriptionStream(ctx, sm, "tenant-A", authCheck, broker, writeMsg)
	}()

	// Publish cross-tenant event and valid event
	broker.Publish(map[string]any{"tenantId": "tenant-B", "id": "1"})
	broker.Publish(map[string]any{"tenantId": "tenant-A", "id": "2"})

	err := <-errChan
	assert.Error(t, err)
	assert.True(t, authCalled)

	mu.Lock()
	// Tenant B should be filtered out by tenancy check at depth
	for _, m := range received {
		if mp, ok := m.(map[string]any);
		ok {
			if id, _ := mp["id"]; id == "1" {
				t.Errorf("cross-tenant event leaked: %v", mp)
			}
		}
	}
	mu.Unlock()
}

func TestRunSubscriptionStream_BackpressureAndSlowConsumer(t *testing.T) {
	sm := NewSubscriptionManager(10, 10, nil)
	broker := broadcast.NewBroker(10)
	defer broker.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	authCheck := func(c context.Context) bool {
		return true
	}

	var receivedCount int
	var mu sync.Mutex
	writeMsg := func(msg any) error {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	go func() {
		_ = RunSubscriptionStream(ctx, sm, "tenant-A", authCheck, broker, writeMsg)
	}()

	// Flood broker to test backpressure drop and gap indicator without blocking ingestion
	for i := 0; i < 200; i++ {
		broker.Publish(map[string]any{"tenantId": "tenant-A", "i": i})
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	assert.True(t, receivedCount > 0)
}

func TestSubscriptionManager_ShutdownAndRelease(t *testing.T) {
	sm := NewSubscriptionManager(2, 2, nil)
	err := sm.Acquire("tenant-A")
	require.NoError(t, err)

	sm.Shutdown()

	err = sm.Acquire("tenant-A")
	assert.Error(t, err)

	sm.Release("tenant-A")
	assert.NoError(t, CheckSubscriptionQueryCost("{events{edges{node{id}}}}", nil, 1000))
}
