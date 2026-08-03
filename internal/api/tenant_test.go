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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Cross-tenant isolation tests.
//
// The leak matrix below is the deliverable for #48: every endpoint that can
// return event data, exercised as a tenant that is not entitled to it. The
// assertions are deliberately two-sided. Checking only "B's data is absent"
// would pass trivially against a handler that returns nothing at all — which
// is exactly what a scope-forgetting handler does under our fail-closed
// design — so every case also asserts that A still sees its own data. A
// change that breaks propagation therefore fails whichever way it breaks.

const (
	contractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	contractC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
)

// scopedStore is an in-memory store that applies store.Scope the same way
// the Postgres implementation does. It exists so the API-level tests can
// assert on response bodies end to end; that Postgres itself honors a scope
// is proven separately, against a real database, in
// internal/store/tenant_postgres_test.go.
type scopedStore struct {
	store.Store // panics on anything not implemented here

	events []store.Event

	// seenScopes records the scope handed to each read method, so a test can
	// assert the boundary was propagated and not merely that the answer
	// happened to look right.
	seenScopes []store.Scope
}

func (s *scopedStore) record(sc store.Scope) { s.seenScopes = append(s.seenScopes, sc) }

func (s *scopedStore) visible(sc store.Scope) []store.Event {
	if sc.DeniesAll() {
		return nil
	}
	out := []store.Event{}
	for _, e := range s.events {
		if sc.Allows(e.ContractID) {
			out = append(out, e)
		}
	}
	return out
}

func (s *scopedStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.record(f.Scope)
	out := []store.Event{}
	for _, e := range s.visible(f.Scope) {
		if f.ContractID != "" && e.ContractID != f.ContractID {
			continue
		}
		out = append(out, e)
	}
	return out, "", nil
}

// CountEvents applies the same boundary as QueryEvents. A count is an
// aggregate over rows the caller may not read, so counting the whole table
// here would leak through X-Total-Count and /events/count even while the
// event bodies themselves stayed correctly filtered.
func (s *scopedStore) CountEvents(_ context.Context, f store.EventFilter) (int64, error) {
	s.record(f.Scope)
	var n int64
	for _, e := range s.visible(f.Scope) {
		if f.ContractID != "" && e.ContractID != f.ContractID {
			continue
		}
		n++
	}
	return n, nil
}

func (s *scopedStore) GetEvent(_ context.Context, id string, sc store.Scope) (store.Event, error) {
	s.record(sc)
	for _, e := range s.visible(sc) {
		if e.ID == id {
			return e, nil
		}
	}
	return store.Event{}, store.ErrNotFound
}

func (s *scopedStore) EventExists(_ context.Context, id string, sc store.Scope) (bool, error) {
	s.record(sc)
	for _, e := range s.visible(sc) {
		if e.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (s *scopedStore) Stats(_ context.Context, sc store.Scope) (store.Stats, error) {
	s.record(sc)
	visible := s.visible(sc)
	contracts := map[string]struct{}{}
	for _, e := range visible {
		contracts[e.ContractID] = struct{}{}
	}
	return store.Stats{
		TotalEvents:   int64(len(visible)),
		ContractCount: int64(len(contracts)),
	}, nil
}

func (s *scopedStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	return store.IngestionState{LastIngestedLedger: 1_000_000}, nil
}
func (s *scopedStore) Ping(context.Context) error { return nil }

// fakeTenants is an in-memory TenantStore covering the parts the API needs.
type fakeTenants struct {
	store.TenantStore

	tenants map[int64]store.Tenant
	grants  map[int64][]string
	keys    map[string]keyRecord

	usage map[int64][]store.TenantUsage
}

type keyRecord struct {
	id       int64
	tenantID int64
	digest   []byte
}

func newFakeTenants() *fakeTenants {
	return &fakeTenants{
		tenants: map[int64]store.Tenant{},
		grants:  map[int64][]string{},
		keys:    map[string]keyRecord{},
		usage:   map[int64][]store.TenantUsage{},
	}
}

// addTenant registers a tenant with the given grants and returns a usable
// plaintext API key for it.
func (f *fakeTenants) addTenant(t *testing.T, tenant store.Tenant, grants ...string) string {
	t.Helper()
	f.tenants[tenant.ID] = tenant
	f.grants[tenant.ID] = grants

	plaintext, prefix, digest, err := GenerateAPIKey()
	require.NoError(t, err)
	f.keys[prefix] = keyRecord{id: tenant.ID * 100, tenantID: tenant.ID, digest: digest}
	return plaintext
}

func (f *fakeTenants) LookupAPIKey(_ context.Context, prefix string) (store.APIKey, []byte, store.Tenant, error) {
	rec, ok := f.keys[prefix]
	if !ok {
		return store.APIKey{}, nil, store.Tenant{}, store.ErrNotFound
	}
	return store.APIKey{ID: rec.id, TenantID: rec.tenantID}, rec.digest, f.tenants[rec.tenantID], nil
}

func (f *fakeTenants) ScopeForTenant(_ context.Context, t store.Tenant) (store.Scope, error) {
	if t.Wildcard {
		return store.WildcardScope(), nil
	}
	return store.NewScope(f.grants[t.ID]), nil
}

func (f *fakeTenants) GetTenant(_ context.Context, id int64) (store.Tenant, error) {
	t, ok := f.tenants[id]
	if !ok {
		return store.Tenant{}, store.ErrNotFound
	}
	return t, nil
}

func (f *fakeTenants) ListGrants(_ context.Context, id int64) ([]string, error) {
	return f.grants[id], nil
}
func (f *fakeTenants) TouchAPIKey(context.Context, int64) error { return nil }
func (f *fakeTenants) AddUsage(_ context.Context, _ time.Time, deltas map[int64]store.UsageDelta) error {
	for id, d := range deltas {
		f.usage[id] = append(f.usage[id], store.TenantUsage{
			TenantID: id, Requests: d.Requests,
			EventsServed: d.EventsServed, StreamSeconds: d.StreamSeconds,
		})
	}
	return nil
}
func (f *fakeTenants) ListUsage(_ context.Context, id int64, _ int) ([]store.TenantUsage, error) {
	return f.usage[id], nil
}

// tenantFixture is a two-tenant deployment: A holds contractA, B holds
// contractB, and contractC is ingested but granted to nobody.
type tenantFixture struct {
	srv     http.Handler
	st      *scopedStore
	tenants *fakeTenants
	keyA    string
	keyB    string
	keyNone string // authenticated, granted nothing
	keyAdmin,
	keyWildcard string
}

func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()
	// Package-level caching flags are process-wide; reset so tests do not
	// leak state into one another.
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	st := &scopedStore{events: []store.Event{
		{ID: "ev-a1", ContractID: contractA, Ledger: 100, Type: "contract"},
		{ID: "ev-a2", ContractID: contractA, Ledger: 101, Type: "contract"},
		{ID: "ev-b1", ContractID: contractB, Ledger: 100, Type: "contract"},
		{ID: "ev-c1", ContractID: contractC, Ledger: 100, Type: "contract"},
	}}
	tenants := newFakeTenants()

	f := &tenantFixture{st: st, tenants: tenants}
	f.keyA = tenants.addTenant(t, store.Tenant{ID: 1, Name: "a", Enabled: true}, contractA)
	f.keyB = tenants.addTenant(t, store.Tenant{ID: 2, Name: "b", Enabled: true}, contractB)
	f.keyNone = tenants.addTenant(t, store.Tenant{ID: 3, Name: "none", Enabled: true})
	f.keyAdmin = tenants.addTenant(t, store.Tenant{ID: 4, Name: "admin", Enabled: true, Admin: true})
	f.keyWildcard = tenants.addTenant(t, store.Tenant{ID: 5, Name: "legacy", Enabled: true, Wildcard: true})

	srv := New(st, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").
		WithMultiTenancy(tenants, MultiTenantOptions{MaxWatchedContracts: 10})
	f.srv = srv.Router()
	return f
}

// get issues an authenticated GET.
func (f *tenantFixture) get(t *testing.T, key, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func bodyString(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	return rec.Body.String()
}

// TestCrossTenantLeakMatrix walks every read endpoint as tenant A and
// asserts that no response mentions tenant B's or the ungranted contract's
// data, while A's own data still comes back.
func TestCrossTenantLeakMatrix(t *testing.T) {
	// Each case is a request A is entitled to make. `wantVisible` must
	// appear in the body; `wantHidden` must not appear anywhere in it.
	cases := []struct {
		name        string
		path        string
		wantStatus  int
		wantVisible []string
		wantHidden  []string
	}{
		{
			name:        "list events is implicitly filtered",
			path:        "/events",
			wantStatus:  http.StatusOK,
			wantVisible: []string{"ev-a1", "ev-a2"},
			wantHidden:  []string{"ev-b1", "ev-c1", contractB, contractC},
		},
		{
			name:        "list events filtered to own contract",
			path:        "/events?contract_id=" + contractA,
			wantStatus:  http.StatusOK,
			wantVisible: []string{"ev-a1"},
			wantHidden:  []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "list events naming another tenant's contract is refused",
			path:       "/events?contract_id=" + contractB,
			wantStatus: http.StatusForbidden,
			wantHidden: []string{"ev-b1"},
		},
		{
			name:       "list events naming an ungranted contract is refused",
			path:       "/events?contract_id=" + contractC,
			wantStatus: http.StatusForbidden,
			wantHidden: []string{"ev-c1"},
		},
		{
			name:        "contract events for own contract",
			path:        "/contracts/" + contractA + "/events",
			wantStatus:  http.StatusOK,
			wantVisible: []string{"ev-a1", "ev-a2"},
			wantHidden:  []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "contract events for another tenant's contract",
			path:       "/contracts/" + contractB + "/events",
			wantStatus: http.StatusForbidden,
			wantHidden: []string{"ev-b1"},
		},
		{
			name:       "contract events for an ungranted contract",
			path:       "/contracts/" + contractC + "/events",
			wantStatus: http.StatusForbidden,
			wantHidden: []string{"ev-c1"},
		},
		{
			name:        "own event by ID",
			path:        "/events/ev-a1",
			wantStatus:  http.StatusOK,
			wantVisible: []string{"ev-a1"},
		},
		{
			// 404 and not 403: event IDs are guessable, so distinguishing
			// "exists but forbidden" would be an enumeration oracle.
			name:       "another tenant's event by ID is indistinguishable from absent",
			path:       "/events/ev-b1",
			wantStatus: http.StatusNotFound,
			wantHidden: []string{contractB},
		},
		{
			name:       "ungranted contract's event by ID",
			path:       "/events/ev-c1",
			wantStatus: http.StatusNotFound,
			wantHidden: []string{contractC},
		},
		{
			// The count is A's two events, not the fixture's four. Asserting
			// on the exact body catches the case the substring checks below
			// cannot: a count has no IDs in it, so an unscoped total would
			// slip through wantHidden untouched.
			name:        "count is scoped to what the tenant can read",
			path:        "/events/count",
			wantStatus:  http.StatusOK,
			wantVisible: []string{`"count":2`},
		},
		{
			name:       "count naming another tenant's contract is refused",
			path:       "/events/count?contract_id=" + contractB,
			wantStatus: http.StatusForbidden,
		},
		{
			// A bulk export is the highest-volume read there is, so it is
			// the worst place for the scope to go missing.
			name:        "export of own contract",
			path:        "/contracts/" + contractA + "/export?from_ledger=1&to_ledger=1000",
			wantStatus:  http.StatusOK,
			wantVisible: []string{"ev-a1", "ev-a2"},
			wantHidden:  []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "export of another tenant's contract is refused",
			path:       "/contracts/" + contractB + "/export?from_ledger=1&to_ledger=1000",
			wantStatus: http.StatusForbidden,
			wantHidden: []string{"ev-b1"},
		},
		{
			// The raw view is a second projection of the same row, so it
			// needs the same 404 — otherwise it reads bodies /events/{id}
			// refuses.
			name:       "another tenant's event raw XDR is indistinguishable from absent",
			path:       "/events/ev-b1/raw",
			wantStatus: http.StatusNotFound,
			wantHidden: []string{contractB},
		},
		{
			name:       "decoded view is filtered too",
			path:       "/events?decoded=true",
			wantStatus: http.StatusOK,
			wantHidden: []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "topic filters cannot widen the scope",
			path:       "/events?topic=" + `{"symbol":"transfer"}`,
			wantStatus: http.StatusOK,
			wantHidden: []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "positional topic filters cannot widen the scope",
			path:       "/events?topic0=" + `{"symbol":"transfer"}`,
			wantStatus: http.StatusOK,
			wantHidden: []string{"ev-b1", "ev-c1"},
		},
		{
			name:       "a wide ledger range cannot widen the scope",
			path:       "/events?from_ledger=1&to_ledger=999999&limit=200",
			wantStatus: http.StatusOK,
			wantHidden: []string{"ev-b1", "ev-c1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newTenantFixture(t)
			rec := f.get(t, f.keyA, tc.path)
			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", bodyString(t, rec))

			body := bodyString(t, rec)
			for _, want := range tc.wantVisible {
				assert.Contains(t, body, want,
					"tenant A must still see its own data (a handler that denies everything is also broken)")
			}
			for _, hidden := range tc.wantHidden {
				assert.NotContains(t, body, hidden,
					"cross-tenant data must not appear in the response")
			}
		})
	}
}

// Counts are data too: an unscoped aggregate tells a tenant how much exists
// outside its grants and lets it watch other tenants' ingestion.
func TestStatsIsScoped(t *testing.T) {
	f := newTenantFixture(t)

	var statsA store.Stats
	rec := f.get(t, f.keyA, "/stats")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statsA))

	assert.Equal(t, int64(2), statsA.TotalEvents, "only tenant A's two events are counted")
	assert.Equal(t, int64(1), statsA.ContractCount)

	var statsWild store.Stats
	rec = f.get(t, f.keyWildcard, "/stats")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statsWild))
	assert.Equal(t, int64(4), statsWild.TotalEvents, "a wildcard tenant still sees the whole store")
}

// A tenant with no grants is authenticated but entitled to nothing.
func TestTenantWithNoGrantsSeesNothing(t *testing.T) {
	f := newTenantFixture(t)

	rec := f.get(t, f.keyNone, "/events")
	require.Equal(t, http.StatusOK, rec.Code)
	var resp eventsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Events)

	rec = f.get(t, f.keyNone, "/events/ev-a1")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Orchestrator probes carry no credential. If enabling MULTI_TENANT gated
// them, every pod would fail readiness on rollout and the deployment would
// stall — a failure that looks like an unhealthy process rather than a
// misconfigured one. They expose no tenant data, so they stay open.
func TestProbesStayPublicInMultiTenantMode(t *testing.T) {
	f := newTenantFixture(t)
	for _, path := range []string{"/health", "/livez", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			rec := f.get(t, "", path) // no API key at all
			assert.NotEqual(t, http.StatusUnauthorized, rec.Code,
				"%s must not require a credential", path)
		})
	}
}

// The scope must actually reach the store. Asserting on bodies alone would
// not catch a handler that queries unscoped and filters afterwards — which
// works until someone adds pagination.
func TestScopeReachesTheStore(t *testing.T) {
	f := newTenantFixture(t)
	for _, path := range []string{
		"/events",
		"/events/count",
		"/contracts/" + contractA + "/events",
		"/contracts/" + contractA + "/export?from_ledger=1&to_ledger=1000",
		"/events/ev-a1",
		"/stats",
	} {
		t.Run(path, func(t *testing.T) {
			f.st.seenScopes = nil
			f.get(t, f.keyA, path)

			require.NotEmpty(t, f.st.seenScopes, "the store was never given a scope")
			for _, sc := range f.st.seenScopes {
				assert.False(t, sc.IsWildcard(), "a granted tenant must not reach the store as wildcard")
				assert.Equal(t, []string{contractA}, sc.Contracts())
			}
		})
	}
}

func TestAuthentication(t *testing.T) {
	f := newTenantFixture(t)

	t.Run("no credential is rejected", func(t *testing.T) {
		rec := f.get(t, "", "/events")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("WWW-Authenticate"))
	})

	t.Run("malformed credential is rejected", func(t *testing.T) {
		rec := f.get(t, "not-a-key", "/events")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("unknown key is rejected", func(t *testing.T) {
		plaintext, _, _, err := GenerateAPIKey()
		require.NoError(t, err)
		rec := f.get(t, plaintext, "/events")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("a valid prefix with the wrong secret is rejected", func(t *testing.T) {
		// The prefix is the indexed lookup handle and is not itself a
		// secret; possessing it must not be enough.
		parts := strings.Split(f.keyA, "_")
		require.Len(t, parts, 3)
		forged := parts[0] + "_" + parts[1] + "_" + "wrongsecretwrongsecretwrongsecret"

		rec := f.get(t, forged, "/events")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.NotContains(t, bodyString(t, rec), "ev-a1")
	})

	t.Run("X-API-Key is accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		req.Header.Set("X-API-Key", f.keyA)
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("health stays public", func(t *testing.T) {
		// Orchestrator probes hold no credential, and /health reports only
		// liveness — no tenant data.
		rec := f.get(t, "", "/health")
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("a disabled tenant is refused", func(t *testing.T) {
		disabled := f.tenants.addTenant(t,
			store.Tenant{ID: 9, Name: "suspended", Enabled: false}, contractA)
		rec := f.get(t, disabled, "/events")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.NotContains(t, bodyString(t, rec), "ev-a1")
	})
}

func TestAdminRoutesRequireAdmin(t *testing.T) {
	f := newTenantFixture(t)

	rec := f.get(t, f.keyA, "/admin/tenants")
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"a non-admin tenant must not enumerate tenants")

	rec = f.get(t, f.keyAdmin, "/admin/tenants/1/grants")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// A tenant's self-service view is derived from its own principal, so there
// is no path parameter to tamper with.
func TestSelfServiceUsesOwnIdentity(t *testing.T) {
	f := newTenantFixture(t)

	rec := f.get(t, f.keyA, "/tenant")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp whoAmIResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, int64(1), resp.Tenant.ID)
	assert.Equal(t, []string{contractA}, resp.Grants)
	assert.NotContains(t, bodyString(t, rec), contractB)
}

// Conditional requests are the subtlest leak in the set: the 304 is produced
// by this server, so no CDN has to misbehave for a tenant to be handed a
// page it was never entitled to.
func TestConditionalRequestsCannotCrossTenants(t *testing.T) {
	f := newTenantFixture(t)

	// A page entirely behind the ingest frontier is cacheable and carries a
	// strong validator.
	path := "/events?to_ledger=500"
	recA := f.get(t, f.keyA, path)
	require.Equal(t, http.StatusOK, recA.Code)
	etagA := recA.Header().Get("ETag")
	require.NotEmpty(t, etagA, "the page must be cacheable for this test to mean anything")

	// B replays A's validator against the identical URL.
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+f.keyB)
	req.Header.Set("If-None-Match", etagA)
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusNotModified, rec.Code,
		"a validator minted for another tenant must not produce a 304")
	assert.NotContains(t, bodyString(t, rec), "ev-a1")

	// And the two tenants' validators for the same URL must differ.
	recB := f.get(t, f.keyB, path)
	assert.NotEqual(t, etagA, recB.Header().Get("ETag"),
		"two tenants must not share a cache validator for the same URL")
}

func TestTenantScopedResponsesAreNotShareable(t *testing.T) {
	f := newTenantFixture(t)

	rec := f.get(t, f.keyA, "/events?to_ledger=500")
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Contains(t, rec.Header().Get("Cache-Control"), "private",
		"tenant-scoped bodies must never be marked publicly cacheable")
	vary := rec.Header().Get("Vary")
	assert.Contains(t, vary, "Authorization")
	assert.Contains(t, vary, "X-API-Key")
}

// Usage accounting must attribute consumption to the right tenant.
func TestUsageAccounting(t *testing.T) {
	f := newTenantFixture(t)

	f.get(t, f.keyA, "/events")
	f.get(t, f.keyA, "/events")
	f.get(t, f.keyB, "/events")

	rec := f.get(t, f.keyA, "/tenant/usage") // flushes before reading
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Usage []store.TenantUsage `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Usage)

	var requests, events int64
	for _, u := range resp.Usage {
		requests += u.Requests
		events += u.EventsServed
	}
	assert.GreaterOrEqual(t, requests, int64(3), "A's own requests are counted")
	assert.Equal(t, int64(4), events, "two pages of two events each")

	// B's consumption is recorded against B, not folded into A.
	assert.Len(t, f.tenants.usage[2], 1)
	assert.Equal(t, int64(1), f.tenants.usage[2][0].EventsServed)
}

// Single-tenant deployments must be untouched: no key required, no tenant
// endpoints, no caching changes.
func TestSingleTenantModeIsUnchanged(t *testing.T) {
	SetTenantScopedCaching(false)
	st := &scopedStore{events: []store.Event{
		{ID: "ev-a1", ContractID: contractA},
		{ID: "ev-b1", ContractID: contractB},
	}}
	srv := New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").Router()

	t.Run("no credential is needed and everything is visible", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "ev-a1")
		assert.Contains(t, body, "ev-b1", "single-tenant mode applies no boundary")
	})

	t.Run("the store is reached with a wildcard scope", func(t *testing.T) {
		st.seenScopes = nil
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		srv.ServeHTTP(httptest.NewRecorder(), req)

		require.NotEmpty(t, st.seenScopes)
		assert.True(t, st.seenScopes[0].IsWildcard())
	})

	t.Run("responses stay publicly cacheable", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/events?to_ledger=500", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		assert.Contains(t, rec.Header().Get("Cache-Control"), "public")
		assert.NotContains(t, rec.Header().Get("Vary"), "Authorization")
	})

	t.Run("tenant and admin endpoints are absent", func(t *testing.T) {
		for _, path := range []string{"/tenant", "/admin/tenants"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code, path)
		}
	})
}

func TestTenantLimitResolver(t *testing.T) {
	rps, burst := 7.0, 3
	tenant := store.Tenant{ID: 42, RateLimitRPS: &rps, RateLimitBurst: &burst}

	newReq := func(p Principal) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		return req.WithContext(WithPrincipal(req.Context(), p))
	}

	t.Run("tenant overrides win", func(t *testing.T) {
		key, gotRPS, gotBurst, ok := TenantLimitResolver(1, 1)(newReq(Principal{Tenant: tenant}))
		require.True(t, ok)
		assert.Equal(t, "tenant:42", key)
		assert.Equal(t, 7.0, gotRPS)
		assert.Equal(t, 3, gotBurst)
	})

	t.Run("instance defaults apply without an override", func(t *testing.T) {
		key, gotRPS, gotBurst, ok := TenantLimitResolver(5, 2)(
			newReq(Principal{Tenant: store.Tenant{ID: 9}}))
		require.True(t, ok)
		assert.Equal(t, "tenant:9", key)
		assert.Equal(t, 5.0, gotRPS)
		assert.Equal(t, 2, gotBurst)
	})

	t.Run("two tenants get separate buckets", func(t *testing.T) {
		k1, _, _, _ := TenantLimitResolver(5, 2)(newReq(Principal{Tenant: store.Tenant{ID: 1}}))
		k2, _, _, _ := TenantLimitResolver(5, 2)(newReq(Principal{Tenant: store.Tenant{ID: 2}}))
		assert.NotEqual(t, k1, k2)
	})

	t.Run("untenanted requests fall back to IP keying", func(t *testing.T) {
		_, _, _, ok := TenantLimitResolver(5, 2)(newReq(Principal{Untenanted: true}))
		assert.False(t, ok)
	})

	t.Run("no override and no default falls back", func(t *testing.T) {
		_, _, _, ok := TenantLimitResolver(0, 0)(newReq(Principal{Tenant: store.Tenant{ID: 9}}))
		assert.False(t, ok, "a deployment with no limits configured must not start denying")
	})
}

// A tenant configured with a zero quota is shut off, not silently granted
// the instance default.
func TestZeroQuotaDenies(t *testing.T) {
	zero := 0.0
	zeroBurst := 0
	tenant := store.Tenant{ID: 3, Enabled: true, RateLimitRPS: &zero, RateLimitBurst: &zeroBurst}

	limiter := NewRateLimiter(0, 0, false,
		WithLimitResolver(TenantLimitResolver(100, 100)))
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req = req.WithContext(WithPrincipal(req.Context(), Principal{Tenant: tenant}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

func TestAPIKeyGeneration(t *testing.T) {
	plaintext, prefix, digest, err := GenerateAPIKey()
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(plaintext, "st_"))
	assert.Len(t, prefix, keyPrefixLen)
	assert.Len(t, digest, 32)

	gotPrefix, gotDigest, ok := parseAPIKey(plaintext)
	require.True(t, ok)
	assert.Equal(t, prefix, gotPrefix)
	assert.Equal(t, digest, gotDigest)

	t.Run("keys are unique", func(t *testing.T) {
		seen := map[string]bool{}
		for range 100 {
			p, _, _, err := GenerateAPIKey()
			require.NoError(t, err)
			require.False(t, seen[p], "generated a duplicate key")
			seen[p] = true
		}
	})

	t.Run("malformed keys are rejected", func(t *testing.T) {
		for _, bad := range []string{
			"", "st_", "st_short_secret", "xx_" + prefix + "_secret",
			"st_" + prefix, "st_" + prefix + "_", strings.ReplaceAll(plaintext, "_", "-"),
		} {
			_, _, ok := parseAPIKey(bad)
			assert.False(t, ok, "should reject %q", bad)
		}
	})
}

func TestParseAPIKeyForBootstrapMatchesGeneration(t *testing.T) {
	plaintext, prefix, digest, err := GenerateAPIKey()
	require.NoError(t, err)

	gotPrefix, gotDigest, ok := ParseAPIKeyForBootstrap(plaintext)
	require.True(t, ok)
	assert.Equal(t, prefix, gotPrefix)
	assert.Equal(t, digest, gotDigest,
		"the bootstrap path must derive the same digest the API does, or the key would not authenticate")
}

// A handler that builds its own filter, bypassing filterFromQuery, must
// return nothing rather than everything. This is the property the whole
// fail-closed design exists to guarantee.
func TestForgottenScopeDeniesRatherThanLeaks(t *testing.T) {
	st := &scopedStore{events: []store.Event{
		{ID: "ev-a1", ContractID: contractA},
		{ID: "ev-b1", ContractID: contractB},
	}}

	// Exactly what a contributor adding an endpoint might write.
	forgetful := store.EventFilter{Limit: 100}
	got, _, err := st.QueryEvents(context.Background(), forgetful)

	require.NoError(t, err)
	assert.Empty(t, got, "a filter with no scope must match nothing")
}

func TestForbiddenContractResponseShape(t *testing.T) {
	f := newTenantFixture(t)
	rec := f.get(t, f.keyA, "/contracts/"+contractB+"/events")

	require.Equal(t, http.StatusForbidden, rec.Code)
	var resp errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp.Error, "not granted",
		"the error envelope should say why, so a missing grant is not debugged as missing data")
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"),
		fmt.Sprintf("errors must not be cached: %s", rec.Header().Get("Cache-Control")))
}

// --- Webhook subscriptions ---
//
// A subscription is a read path whose sink is a URL the subscriber picks, so
// an unowned one is not merely a leak but an exfiltration primitive:
// subscribe to a contract you cannot read and have its events delivered to
// your own server. These tests cover both halves — who may see a
// subscription, and what a subscription may be made to match.

// subStore records subscriptions and applies the owner filter the way
// Postgres does.
type subStore struct {
	scopedStore
	subs      map[int64]store.Subscription
	nextID    int64
	lastOwner store.SubscriptionOwner
}

func newSubStore() *subStore {
	return &subStore{subs: map[int64]store.Subscription{}}
}

func (s *subStore) owned(sub store.Subscription, owner store.SubscriptionOwner) bool {
	if owner.IsAll() {
		return true
	}
	return sub.TenantID != nil && *sub.TenantID == owner.TenantID()
}

func (s *subStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	s.nextID++
	sub.ID = s.nextID
	s.subs[sub.ID] = sub
	return sub, nil
}

func (s *subStore) GetSubscription(_ context.Context, id int64, owner store.SubscriptionOwner) (store.Subscription, error) {
	s.lastOwner = owner
	sub, ok := s.subs[id]
	if !ok || !s.owned(sub, owner) {
		return store.Subscription{}, store.ErrNotFound
	}
	return sub, nil
}

func (s *subStore) ListSubscriptions(_ context.Context, owner store.SubscriptionOwner) ([]store.Subscription, error) {
	s.lastOwner = owner
	out := []store.Subscription{}
	for _, sub := range s.subs {
		if s.owned(sub, owner) {
			out = append(out, sub)
		}
	}
	return out, nil
}

func (s *subStore) UpdateSubscription(_ context.Context, sub store.Subscription, owner store.SubscriptionOwner) (store.Subscription, error) {
	existing, ok := s.subs[sub.ID]
	if !ok || !s.owned(existing, owner) {
		return store.Subscription{}, store.ErrNotFound
	}
	s.subs[sub.ID] = sub
	return sub, nil
}

func (s *subStore) DeleteSubscription(_ context.Context, id int64, owner store.SubscriptionOwner) error {
	sub, ok := s.subs[id]
	if !ok || !s.owned(sub, owner) {
		return store.ErrNotFound
	}
	delete(s.subs, id)
	return nil
}

func (s *subStore) ListDeliveryAttempts(ctx context.Context, id int64, _ int, owner store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	if _, err := s.GetSubscription(ctx, id, owner); err != nil {
		return nil, err
	}
	return []store.DeliveryAttempt{{SubscriptionID: id}}, nil
}

// newSubFixture is newTenantFixture with a subscription-aware store.
func newSubFixture(t *testing.T) (*tenantFixture, *subStore) {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	st := newSubStore()
	tenants := newFakeTenants()
	f := &tenantFixture{st: &st.scopedStore, tenants: tenants}
	f.keyA = tenants.addTenant(t, store.Tenant{ID: 1, Name: "a", Enabled: true}, contractA)
	f.keyB = tenants.addTenant(t, store.Tenant{ID: 2, Name: "b", Enabled: true}, contractB)
	f.keyAdmin = tenants.addTenant(t, store.Tenant{ID: 4, Name: "admin", Enabled: true, Admin: true})
	f.keyWildcard = tenants.addTenant(t,
		store.Tenant{ID: 5, Name: "legacy", Enabled: true, Wildcard: true})

	f.srv = New(st, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").
		WithMultiTenancy(tenants, MultiTenantOptions{}).Router()
	return f, st
}

func (f *tenantFixture) post(t *testing.T, key, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func (f *tenantFixture) do(t *testing.T, method, key, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func subBody(contractID string) string {
	filters := ""
	if contractID != "" {
		filters = fmt.Sprintf(`,"filters":{"contract_id":%q}`, contractID)
	}
	return fmt.Sprintf(`{"url":"https://example.test/hook","secret":"s3cret"%s}`, filters)
}

// A tenant may only subscribe to what it may read; otherwise a webhook is a
// way to have another tenant's events delivered to a server you control.
func TestSubscriptionCreationIsScoped(t *testing.T) {
	f, _ := newSubFixture(t)

	t.Run("own contract is allowed", func(t *testing.T) {
		rec := f.post(t, f.keyA, "/subscriptions", subBody(contractA))
		assert.Equal(t, http.StatusCreated, rec.Code, bodyString(t, rec))
	})

	t.Run("another tenant's contract is refused", func(t *testing.T) {
		rec := f.post(t, f.keyA, "/subscriptions", subBody(contractB))
		assert.Equal(t, http.StatusForbidden, rec.Code,
			"subscribing to an ungranted contract would exfiltrate it")
	})

	t.Run("an unfiltered subscription is refused", func(t *testing.T) {
		// Without a contract_id the subscription would match every ingested
		// event, which is the same leak with extra steps.
		rec := f.post(t, f.keyA, "/subscriptions", subBody(""))
		assert.Equal(t, http.StatusBadRequest, rec.Code, bodyString(t, rec))
		assert.Contains(t, bodyString(t, rec), "contract_id")
	})

	t.Run("a wildcard tenant may subscribe unfiltered", func(t *testing.T) {
		rec := f.post(t, f.keyWildcard, "/subscriptions", subBody(""))
		assert.Equal(t, http.StatusCreated, rec.Code, bodyString(t, rec))
	})

	t.Run("an admin without wildcard is still constrained", func(t *testing.T) {
		// Admin is management authority, not read breadth. An admin that
		// holds no grants can read nothing, so it may not subscribe to
		// everything either — it has to grant itself first, which is an
		// auditable act.
		rec := f.post(t, f.keyAdmin, "/subscriptions", subBody(""))
		assert.Equal(t, http.StatusBadRequest, rec.Code, bodyString(t, rec))
	})
}

// Ownership: one tenant's callbacks are invisible and immutable to another.
func TestSubscriptionOwnershipIsEnforced(t *testing.T) {
	f, st := newSubFixture(t)

	rec := f.post(t, f.keyA, "/subscriptions", subBody(contractA))
	require.Equal(t, http.StatusCreated, rec.Code)
	var created store.Subscription
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.NotNil(t, created.TenantID)
	assert.Equal(t, int64(1), *created.TenantID, "ownership comes from the credential")

	t.Run("another tenant cannot read it", func(t *testing.T) {
		rec := f.get(t, f.keyB, fmt.Sprintf("/subscriptions/%d", created.ID))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.NotContains(t, bodyString(t, rec), "example.test")
	})

	t.Run("another tenant cannot list it", func(t *testing.T) {
		rec := f.get(t, f.keyB, "/subscriptions")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, bodyString(t, rec), "example.test",
			"a tenant must not enumerate another's callbacks or their secrets")
	})

	t.Run("another tenant cannot delete it", func(t *testing.T) {
		rec := f.do(t, http.MethodDelete, f.keyB, fmt.Sprintf("/subscriptions/%d", created.ID), "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, st.subs, created.ID, "the subscription must survive")
	})

	t.Run("another tenant cannot update it", func(t *testing.T) {
		rec := f.do(t, http.MethodPut, f.keyB,
			fmt.Sprintf("/subscriptions/%d", created.ID),
			`{"url":"https://attacker.test/hook"}`)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "https://example.test/hook", st.subs[created.ID].URL)
	})

	t.Run("another tenant cannot read its delivery history", func(t *testing.T) {
		rec := f.get(t, f.keyB, fmt.Sprintf("/subscriptions/%d/deliveries", created.ID))
		assert.Equal(t, http.StatusNotFound, rec.Code,
			"delivery history names the events that matched")
	})

	t.Run("the owner can still do all of it", func(t *testing.T) {
		rec := f.get(t, f.keyA, fmt.Sprintf("/subscriptions/%d", created.ID))
		assert.Equal(t, http.StatusOK, rec.Code)
		rec = f.get(t, f.keyA, "/subscriptions")
		assert.Contains(t, bodyString(t, rec), "example.test")
		rec = f.get(t, f.keyA, fmt.Sprintf("/subscriptions/%d/deliveries", created.ID))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("an admin sees every tenant's", func(t *testing.T) {
		rec := f.get(t, f.keyAdmin, "/subscriptions")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, bodyString(t, rec), "example.test")
	})
}

// An update must not be usable to widen a subscription past what its owner
// could have created in the first place.
func TestSubscriptionUpdateCannotWidenScope(t *testing.T) {
	f, st := newSubFixture(t)

	rec := f.post(t, f.keyA, "/subscriptions", subBody(contractA))
	require.Equal(t, http.StatusCreated, rec.Code)
	var created store.Subscription
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec = f.do(t, http.MethodPut, f.keyA,
		fmt.Sprintf("/subscriptions/%d", created.ID),
		fmt.Sprintf(`{"filters":{"contract_id":%q}}`, contractB))
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, contractA, st.subs[created.ID].Filters.ContractID,
		"the stored filter must not have moved")

	rec = f.do(t, http.MethodPut, f.keyA,
		fmt.Sprintf("/subscriptions/%d", created.ID),
		`{"filters":{}}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"clearing the contract filter would match every ingested event")
}

// Single-tenant deployments keep the pre-#48 subscription behavior: no
// ownership, no required filter.
func TestSubscriptionsUnchangedInSingleTenantMode(t *testing.T) {
	SetTenantScopedCaching(false)
	st := newSubStore()
	srv := New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").Router()

	req := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(subBody("")))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var created store.Subscription
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Nil(t, created.TenantID, "single-tenant subscriptions stay operator-owned")

	req = httptest.NewRequest(http.MethodGet, "/subscriptions", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	assert.Contains(t, rec.Body.String(), "example.test")
}

// The owner filter's zero value must deny, matching store.Scope.
func TestSubscriptionOwnerZeroValueDenies(t *testing.T) {
	var zero store.SubscriptionOwner
	assert.False(t, zero.IsAll())
	assert.Zero(t, zero.TenantID(),
		"the zero owner restricts to tenant 0, which no bigserial row can be")
	assert.True(t, store.AllSubscriptions().IsAll())
	assert.Equal(t, int64(7), store.OwnedBy(7).TenantID())
}
