package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/pkg/client"
)

func TestListEventsBuildsRequestAndDecodesResponse(t *testing.T) {
	var gotMethod, gotPath, gotAuthorization string
	var gotQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		gotAuthorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[{"id":"event-1","contract_id":"contract-1","ledger":42,"type":"contract","tx_hash":"abc","topics":[{"symbol":"transfer"}],"value":{"amount":7}}],"cursor":"next-page"}`))
	}))
	defer server.Close()

	apiKey := "test-api-key"
	c := client.New(server.URL+"/", client.WithAPIKey(apiKey))
	page, err := c.ListEvents(context.Background(), client.ListEventsParams{
		ContractID: "contract-1",
		FromLedger: 40,
		ToLedger:   50,
		Limit:      1,
		Order:      "desc",
	})
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Len(t, page.Events, 1)
	assert.Equal(t, "event-1", page.Events[0].ID)
	assert.Equal(t, "contract-1", page.Events[0].ContractID)
	assert.Equal(t, int64(42), page.Events[0].Ledger)
	assert.Equal(t, map[string]any{"symbol": "transfer"}, page.Events[0].Topics[0])
	assert.Equal(t, "next-page", page.Cursor)

	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/events", gotPath)
	assert.Equal(t, "Bearer"+" "+apiKey, gotAuthorization)
	assert.Equal(t, "contract-1", gotQuery.Get("contract_id"))
	assert.Equal(t, "40", gotQuery.Get("from_ledger"))
	assert.Equal(t, "50", gotQuery.Get("to_ledger"))
	assert.Equal(t, "1", gotQuery.Get("limit"))
	assert.Equal(t, "desc", gotQuery.Get("order"))
	assert.Empty(t, gotQuery.Get("cursor"))
}

func TestListEventsPaginatesWithReturnedCursor(t *testing.T) {
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursors = append(cursors, r.URL.Query().Get("cursor"))
		w.Header().Set("Content-Type", "application/json")
		if len(cursors) == 1 {
			_, _ = w.Write([]byte(`{"events":[{"id":"event-1","ledger":1}],"cursor":"next / page"}`))
			return
		}
		_, _ = w.Write([]byte(`{"events":[{"id":"event-2","ledger":2}]}`))
	}))
	defer server.Close()

	c := client.New(server.URL)
	first, err := c.ListEvents(context.Background(), client.ListEventsParams{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, "next / page", first.Cursor)
	second, err := c.ListEvents(context.Background(), client.ListEventsParams{
		Limit:  1,
		Cursor: first.Cursor,
	})
	require.NoError(t, err)
	require.Len(t, second.Events, 1)
	assert.Equal(t, "event-2", second.Events[0].ID)
	assert.Equal(t, []string{"", "next / page"}, cursors)
}

func TestClientMapsHTTPFailuresToAPIError(t *testing.T) {
	for _, tt := range []struct {
		name       string
		statusCode int
		body       string
		wantMsg    string
		wantCode   client.ErrorCode
		wantField  string
	}{
		{
			name:       "client error with code and field",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"limit must be an integer in [1,1000]","code":"LIMIT_EXCEEDED","field":"limit"}`,
			wantMsg:    "limit must be an integer in [1,1000]",
			wantCode:   client.CodeLimitExceeded,
			wantField:  "limit",
		},
		{
			name:       "not found error with code",
			statusCode: http.StatusNotFound,
			body:       `{"error":"event \"0001\" not found","code":"NOT_FOUND"}`,
			wantMsg:    `event "0001" not found`,
			wantCode:   client.CodeNotFound,
		},
		{
			name:       "server error",
			statusCode: http.StatusInternalServerError,
			body:       `{"error":"database unavailable","code":"INTERNAL_ERROR"}`,
			wantMsg:    "database unavailable",
			wantCode:   client.CodeInternalError,
		},
		{
			name:       "error without envelope",
			statusCode: http.StatusBadGateway,
			body:       "upstream unavailable",
			wantMsg:    "sorotrail: HTTP 502",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			_, err := client.New(server.URL).Health(context.Background())
			require.Error(t, err)
			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.statusCode, apiErr.StatusCode)
			assert.Equal(t, tt.wantCode, apiErr.Code)
			assert.Equal(t, tt.wantField, apiErr.Field)
			if tt.wantMsg == "sorotrail: HTTP 502" {
				assert.Equal(t, tt.wantMsg, apiErr.Error())
			} else {
				assert.Equal(t, tt.wantMsg, apiErr.Message)
				assert.Equal(t, "sorotrail: "+tt.wantMsg+" (HTTP "+strconv.Itoa(tt.statusCode)+")", apiErr.Error())
			}
		})
	}
}

func TestClientRejectsMalformedSuccessJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[`))
	}))
	defer server.Close()

	_, err := client.New(server.URL).ListEvents(context.Background(), client.ListEventsParams{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding GET /events response")
}

func TestClientCancellationAbortsInFlightRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	serverCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
		close(serverCanceled)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.New(server.URL).Health(ctx)
		result <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the test server")
	}
	cancel()

	select {
	case err := <-result:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation, got %v", err)
	case <-time.After(time.Second):
		t.Fatal("client call blocked after its context was cancelled")
	}

	select {
	case <-serverCanceled:
	case <-time.After(time.Second):
		t.Fatal("server did not observe request cancellation")
	}
}

func TestClientSendsAuthorizationHeaderOnlyWhenConfigured(t *testing.T) {
	authorization := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	_, err := client.New(server.URL).Health(context.Background())
	require.NoError(t, err)
	assert.Empty(t, authorization)

	apiKey := "client-test-key"
	_, err = client.New(server.URL, client.WithAPIKey(apiKey)).Health(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer"+" "+apiKey, authorization)
}

func TestGeneratedClientSliceRawAndNoContentMethods(t *testing.T) {
	var methods, paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/apikeys":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":7,"name":"automation","prefix":"abcd","created_at":"2026-10-20T00:00:00Z"}]`))
		case "/metrics":
			_, _ = w.Write([]byte("sorotrail_events_total 7\n"))
		case "/apikeys/7":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := client.New(server.URL)
	keys, err := c.ListAPIKeys(context.Background())
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, int64(7), keys[0].ID)
	assert.Equal(t, "automation", keys[0].Name)

	metrics, err := c.Metrics(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "sorotrail_events_total 7\n", string(metrics))

	require.NoError(t, c.RevokeAPIKey(context.Background(), 7))
	assert.Equal(t, []string{http.MethodGet, http.MethodGet, http.MethodDelete}, methods)
	assert.Equal(t, []string{"/apikeys", "/metrics", "/apikeys/7"}, paths)
}

func TestGetEventEscapesPathParameter(t *testing.T) {
	var escapedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{"id":"event/1","ledger":9}`))
	}))
	defer server.Close()

	event, err := client.New(server.URL).GetEvent(context.Background(), "event/1", client.GetEventParams{})
	require.NoError(t, err)
	assert.Equal(t, "event/1", event.ID)
	assert.Equal(t, "/events/event%2F1", escapedPath)
}

func TestClientSendsIdempotencyKeyFromContextAndClientClone(t *testing.T) {
	var gotKeys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKeys = append(gotKeys, r.Header.Get(client.IdempotencyKeyHeader))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	base := client.New(server.URL)
	ctx := client.WithIdempotencyKey(context.Background(), "ctx-key-123")
	require.Equal(t, "ctx-key-123", client.IdempotencyKeyFromContext(ctx))
	require.NoError(t, base.DeleteSubscription(ctx, 42))

	scoped := base.WithIdempotencyKey("client-key-456")
	require.NoError(t, scoped.DeleteSubscription(context.Background(), 42))
	// Context key overrides client-scoped key.
	require.NoError(t, scoped.DeleteSubscription(client.WithIdempotencyKey(context.Background(), "override-key-789"), 42))

	assert.Equal(t, []string{"ctx-key-123", "client-key-456", "override-key-789"}, gotKeys)
}

func TestClientDetectsStaleCursorErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantStale  bool
	}{
		{
			name:       "INVALID_CURSOR code",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid cursor \"v99.pg.abc.1234\"","code":"INVALID_CURSOR","field":"cursor"}`,
			wantStale:  true,
		},
		{
			name:       "legacy cursor message without code",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid cursor \"v1.sq.abc\""}`,
			wantStale:  true,
		},
		{
			name:       "non-cursor bad request",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"limit must be an integer in [1,1000]","code":"LIMIT_EXCEEDED","field":"limit"}`,
			wantStale:  false,
		},
		{
			name:       "server error",
			statusCode: http.StatusInternalServerError,
			body:       `{"error":"database unavailable","code":"INTERNAL_ERROR"}`,
			wantStale:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			_, err := client.New(server.URL).ListEvents(context.Background(), client.ListEventsParams{
				Cursor: "v99.pg.abc.1234",
			})
			require.Error(t, err)
			assert.Equal(t, tt.wantStale, client.IsStaleCursor(err))
			assert.Equal(t, tt.wantStale, errors.Is(err, client.ErrStaleCursor))

			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantStale, apiErr.IsStaleCursor())
		})
	}
}
