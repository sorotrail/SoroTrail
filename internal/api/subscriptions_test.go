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

	"github.com/sorotrail/sorotrail/internal/store"
)

// Issue #307: handler tests for 400/404 paths. The subscription
// handlers (POST/GET/PUT/DELETE /subscriptions, GET .../deliveries)
// have no success-path coverage yet, and their error branches are
// unexercised. This file provides a single table-driven test that
// enumerates each handler's 400 (bad client input) and 404 (missing
// resource) branches so a regression that drops an error path is
// caught immediately.
//
// Each row is one HTTP request: the test sets up the stub store to
// return whatever the row needs (e.g. ErrNotFound for "missing
// subscription"), issues the request, and asserts the status code
// plus the error envelope's message substring. Rows are grouped by
// endpoint via the `name` field so a failure points straight at the
// affected handler.

// subErrorStub extends stubStore with named error fields the table
// can drive per row. It embeds *stubStore so all the existing helper
// methods (QueryEvents, CountEvents, etc.) keep their default
// behavior — we only need to swap out the four subscription methods
// plus the deliveries call.
//
// Default semantics: when a row leaves an error field nil, the
// corresponding operation succeeds. Concretely:
//   - getErr == nil → GetSubscription returns a valid Subscription
//     (URL/Secret set) so handlers that call GetSubscription before
//     their main work proceed to it. Tests that need "subscription
//     missing" set getErr = store.ErrNotFound explicitly.
//   - createErr/updateErr/deleteErr/deliveriesErr == nil → the
//     operation succeeds (Create/Update return a default record;
//     Delete returns nil; ListDeliveryAttempts returns an empty slice).
type subErrorStub struct {
	*stubStore
	getErr        error
	createErr     error
	updateErr     error
	deleteErr     error
	deliveriesErr error
}

func newSubErrorStub() *subErrorStub {
	return &subErrorStub{stubStore: &stubStore{}}
}

func (s *subErrorStub) GetSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) (store.Subscription, error) {
	if s.getErr != nil {
		return store.Subscription{}, s.getErr
	}
	// Return a valid subscription so handlers that fetch before
	// performing their main work (e.g. handleUpdateSubscription
	// reads the existing row before decoding the PATCH body) reach
	// the branch under test instead of bailing at "not found".
	return store.Subscription{ID: id, URL: "https://example.com/hook", Secret: "s"}, nil
}

func (s *subErrorStub) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	if s.createErr != nil {
		return store.Subscription{}, s.createErr
	}
	return s.stubStore.CreateSubscription(context.Background(), sub)
}

func (s *subErrorStub) UpdateSubscription(_ context.Context, sub store.Subscription, owner store.SubscriptionOwner) (store.Subscription, error) {
	if s.updateErr != nil {
		return store.Subscription{}, s.updateErr
	}
	return s.stubStore.UpdateSubscription(context.Background(), sub, owner)
}

func (s *subErrorStub) DeleteSubscription(_ context.Context, id int64, owner store.SubscriptionOwner) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.stubStore.DeleteSubscription(context.Background(), id, owner)
}

func (s *subErrorStub) ListDeliveryAttempts(_ context.Context, id int64, limit int, owner store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	if s.deliveriesErr != nil {
		return nil, s.deliveriesErr
	}
	return s.stubStore.ListDeliveryAttempts(context.Background(), id, limit, owner)
}

// doAPIRequest issues one HTTP request against a freshly-spun-up
// test server and returns the response and body. Unlike doGet, this
// helper accepts a method and body so the table can drive POST/PUT/
// DELETE in addition to GET.
func doAPIRequest(t *testing.T, s *Server, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, b
}

// errorEnvelope decodes a JSON body of the form {"error": "..."} so a
// row can assert on the message substring without re-parsing JSON.
func errorEnvelope(t *testing.T, body []byte) string {
	t.Helper()
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	return e["error"]
}

// newServerFromStub wires a Server around the given store with a
// discarded logger and the apiKey used elsewhere in the test suite.
// Centralizing here means the table rows don't have to repeat the
// slog construction on every line.
func newServerFromStub(st store.Store) *Server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(st, nil, log, "test-key")
}

// TestSubscriptions_ErrorPaths covers every 400/404 branch of the
// subscription handlers in one table. The table is grouped by
// endpoint (POST/GET/PUT/DELETE /subscriptions, GET .../deliveries).
func TestSubscriptions_ErrorPaths(t *testing.T) {
	dbErr := errors.New("db timeout")

	tests := []struct {
		name          string
		method        string
		path          string
		body          string
		getErr        error
		createErr     error
		updateErr     error
		deleteErr     error
		deliveriesErr error
		wantStatus    int
		wantErrSub    string
	}{
		// --- POST /subscriptions ---
		{
			name:       "POST: malformed JSON returns 400",
			method:     "POST",
			path:       "/subscriptions",
			body:       `{not valid json`,
			wantStatus: http.StatusBadRequest,
			wantErrSub: "invalid JSON",
		},
		{
			name:       "POST: missing url returns 400",
			method:     "POST",
			path:       "/subscriptions",
			body:       `{"secret":"s"}`,
			wantStatus: http.StatusBadRequest,
			wantErrSub: "url is required",
		},
		{
			name:       "POST: missing secret returns 400",
			method:     "POST",
			path:       "/subscriptions",
			body:       `{"url":"https://example.com/hook"}`,
			wantStatus: http.StatusBadRequest,
			wantErrSub: "secret is required",
		},
		{
			name:       "POST: store error returns 500",
			method:     "POST",
			path:       "/subscriptions",
			body:       `{"url":"https://example.com/hook","secret":"s"}`,
			createErr:  dbErr,
			wantStatus: http.StatusInternalServerError,
			wantErrSub: "creating subscription failed",
		},

		// --- GET /subscriptions/{id} ---
		{
			name:       "GET: non-numeric id returns 400",
			method:     "GET",
			path:       "/subscriptions/not-a-number",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "GET: zero id returns 400",
			method:     "GET",
			path:       "/subscriptions/0",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "GET: negative id returns 400",
			method:     "GET",
			path:       "/subscriptions/-1",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "GET: missing subscription returns 404",
			method:     "GET",
			path:       "/subscriptions/42",
			getErr:     store.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantErrSub: "subscription 42 not found",
		},
		{
			name:       "GET: store error returns 500",
			method:     "GET",
			path:       "/subscriptions/42",
			getErr:     dbErr,
			wantStatus: http.StatusInternalServerError,
			wantErrSub: "getting subscription failed",
		},

		// --- PUT /subscriptions/{id} ---
		{
			name:       "PUT: non-numeric id returns 400",
			method:     "PUT",
			path:       "/subscriptions/abc",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "PUT: missing subscription returns 404",
			method:     "PUT",
			path:       "/subscriptions/99",
			body:       `{}`,
			getErr:     store.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantErrSub: "subscription 99 not found",
		},
		{
			name:       "PUT: empty url after partial update returns 400",
			method:     "PUT",
			path:       "/subscriptions/1",
			body:       `{"url":""}`,
			wantStatus: http.StatusBadRequest,
			wantErrSub: "url must not be empty",
		},
		{
			name:       "PUT: store error during update returns 500",
			method:     "PUT",
			path:       "/subscriptions/1",
			body:       `{"enabled":false}`,
			updateErr:  dbErr,
			wantStatus: http.StatusInternalServerError,
			wantErrSub: "updating subscription failed",
		},

		// --- DELETE /subscriptions/{id} ---
		{
			name:       "DELETE: non-numeric id returns 400",
			method:     "DELETE",
			path:       "/subscriptions/foo",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "DELETE: missing subscription returns 404",
			method:     "DELETE",
			path:       "/subscriptions/77",
			deleteErr:  store.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantErrSub: "subscription 77 not found",
		},
		{
			name:       "DELETE: store error returns 500",
			method:     "DELETE",
			path:       "/subscriptions/77",
			deleteErr:  dbErr,
			wantStatus: http.StatusInternalServerError,
			wantErrSub: "deleting subscription failed",
		},

		// --- GET /subscriptions/{id}/deliveries ---
		{
			name:       "DELIVERIES: non-numeric id returns 400",
			method:     "GET",
			path:       "/subscriptions/abc/deliveries",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "subscription id must be a positive integer",
		},
		{
			name:       "DELIVERIES: missing subscription returns 404",
			method:     "GET",
			path:       "/subscriptions/55/deliveries",
			getErr:     store.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantErrSub: "subscription 55 not found",
		},
		{
			name:       "DELIVERIES: invalid limit returns 400",
			method:     "GET",
			path:       "/subscriptions/1/deliveries?limit=foo",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "limit must be an integer in",
		},
		{
			name:       "DELIVERIES: limit out of range returns 400",
			method:     "GET",
			path:       "/subscriptions/1/deliveries?limit=0",
			wantStatus: http.StatusBadRequest,
			wantErrSub: "limit must be an integer in",
		},
		{
			name:          "DELIVERIES: store error returns 500",
			method:        "GET",
			path:          "/subscriptions/1/deliveries",
			deliveriesErr: dbErr,
			wantStatus:    http.StatusInternalServerError,
			wantErrSub:    "listing delivery attempts failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newSubErrorStub()
			st.getErr = tt.getErr
			st.createErr = tt.createErr
			st.updateErr = tt.updateErr
			st.deleteErr = tt.deleteErr
			st.deliveriesErr = tt.deliveriesErr
			s := newServerFromStub(st)
			resp, body := doAPIRequest(t, s, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, resp.StatusCode,
				"body: %s", string(body))
			if tt.wantErrSub != "" {
				assert.Contains(t, errorEnvelope(t, body), tt.wantErrSub,
					"body: %s", string(body))
			}
		})
	}
}

// TestGetEvent_FieldsUnknownReturns400 covers the ?fields= 400 branch
// of GET /events/{id}. The list endpoint exercises the same parser
// and has its own coverage; pinning the single-event branch here so
// a regression that drops parseFields from the handler is caught
// next to the other 400-path tests.
func TestGetEvent_FieldsUnknownReturns400(t *testing.T) {
	st := &stubStore{event: store.Event{ID: "0000000001-0000000001"}}
	s := newServerFromStub(st)
	resp, body := doAPIRequest(t, s, "GET", "/events/0000000001-0000000001?fields=id,nope", "")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
	assert.Contains(t, errorEnvelope(t, body), "unknown field")
}

// TestCountEvents_BadFilterPaths pins the count endpoint's 400
// branches against the broader rejection set. The handler shares
// filterFromQuery with /events so the matrix mirrors the existing
// TestListEvents_BadParams — adding it here keeps issue #307's
// 400/404 catalog self-contained.
func TestCountEvents_BadFilterPaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{"bad type", "/events/count?type=bogus", "invalid type"},
		{"bad contract_id", "/events/count?contract_id=nope", "invalid contract_id"},
		{"bad limit", "/events/count?limit=99999", "limit must be an integer in"},
		{"inverted ledger range", "/events/count?from_ledger=20&to_ledger=10", "after to_ledger"},
		{"bad from_time", "/events/count?from_time=not-a-time", "RFC 3339"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &stubStore{}
			s := newServerFromStub(st)
			resp, body := doAPIRequest(t, s, "GET", tc.path, "")
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
			assert.Contains(t, errorEnvelope(t, body), tc.want)
		})
	}
}
