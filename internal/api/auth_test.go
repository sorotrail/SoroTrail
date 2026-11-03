package api

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// The authentication helpers are the API's authorization boundary: they
// decide who may reach a gated endpoint, and a regression here is silent
// until someone reads data they were never granted. These tests exercise
// each helper directly rather than only through the router, so a failure
// names the helper that broke instead of a status code somewhere.
//
// The fail-closed direction is asserted at least as carefully as success:
// an absent key, an unknown key, a disabled tenant, and an unconfigured
// API_KEY must all deny, and must deny without leaking why.

func TestCredentialFromRequest(t *testing.T) {
	cases := []struct {
		name          string
		authorization string
		apiKey        string
		want          string
	}{
		{name: "missing headers"},
		{name: "Bearer token", authorization: "Bearer tenant-key", want: "tenant-key"},
		{name: "Bearer credential whitespace", authorization: "Bearer \t tenant-key \t", want: "tenant-key"},
		{name: "API key", apiKey: "header-key", want: "header-key"},
		{name: "API key whitespace", apiKey: " \t header-key \t", want: "header-key"},
		{name: "whitespace-only API key", apiKey: " \t "},
		{name: "Bearer takes precedence", authorization: "Bearer tenant-key", apiKey: "header-key", want: "tenant-key"},
		{name: "empty Bearer does not fall back", authorization: "Bearer ", apiKey: "header-key"},
		{name: "whitespace-only Bearer does not fall back", authorization: "Bearer \t ", apiKey: "header-key"},
		{name: "bare token is rejected", authorization: "tenant-key"},
		{name: "bare token falls back", authorization: "tenant-key", apiKey: "header-key", want: "header-key"},
		{name: "lowercase scheme is rejected", authorization: "bearer tenant-key"},
		{name: "mixed-case scheme is rejected", authorization: "bEaReR tenant-key"},
		{name: "lowercase scheme falls back", authorization: "bearer tenant-key", apiKey: "header-key", want: "header-key"},
		{name: "other scheme is rejected", authorization: "Basic tenant-key"},
		{name: "other scheme falls back", authorization: "Basic tenant-key", apiKey: "header-key", want: "header-key"},
		{name: "missing separator", authorization: "Bearertenant-key"},
		{name: "missing credential", authorization: "Bearer"},
		{name: "tab separator is rejected", authorization: "Bearer\ttenant-key"},
		{name: "leading scheme whitespace is rejected", authorization: " Bearer tenant-key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", tc.authorization)
			req.Header.Set("X-API-Key", tc.apiKey)
			assert.Equal(t, tc.want, credentialFromRequest(req))
		})
	}
	t.Run("unintended credential sources are ignored", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?api_key=query-key", nil)
		req.Header.Set("Proxy-Authorization", "Bearer proxy-key")
		req.Header.Set("X-Auth-Token", "header-key")
		req.AddCookie(&http.Cookie{Name: "api_key", Value: "cookie-key"})
		assert.Empty(t, credentialFromRequest(req))
	})
}

func TestScopeFrom(t *testing.T) {
	contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	scope := store.NewScope([]string{contractID})

	cases := []struct {
		name        string
		ctx         context.Context
		want        store.Scope
		wantDenyAll bool
	}{
		{
			name: "context principal scope",
			ctx:  WithPrincipal(context.Background(), Principal{Scope: scope}),
			want: scope,
		},
		{
			name:        "missing principal denies every contract",
			ctx:         context.Background(),
			want:        store.Scope{},
			wantDenyAll: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scopeFrom(tc.ctx)
			assert.Equal(t, tc.want, got)

			if tc.wantDenyAll {
				assert.True(t, got.DeniesAll(), "the fallback scope must grant no contracts")
				assert.False(t, got.IsWildcard(), "the fallback must never become a wildcard")
				assert.False(t, got.Allows(contractID), "the fallback must not allow any contract")
			}
		})
	}
}

// authRecorder is a terminal handler that records whether it ran and the
// request it was handed, so a test can assert both "the gate allowed the
// call through" and "the principal reached the handler".
type authRecorder struct {
	called bool
	req    *http.Request
}

func (h *authRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.called = true
	h.req = r
	w.WriteHeader(http.StatusOK)
}

// runAuthMiddleware drives one middleware chain against req and reports the
// response alongside the terminal handler.
func runAuthMiddleware(mw func(http.Handler) http.Handler, req *http.Request) (*httptest.ResponseRecorder, *authRecorder) {
	h := &authRecorder{}
	rec := httptest.NewRecorder()
	mw(h).ServeHTTP(rec, req)
	return rec, h
}

// authTenants builds the three-tenant deployment the authentication tests
// share: an ordinary tenant, an admin tenant, and a suspended tenant. It
// reuses the package's fakeTenants so the tests stay on the same fakes as
// the rest of the suite.
func authTenants(t *testing.T) (ts *fakeTenants, keyA, keyAdmin, keyDisabled string) {
	t.Helper()
	ts = newFakeTenants()
	keyA = ts.addTenant(t, store.Tenant{ID: 1, Name: "a", Enabled: true}, contractA)
	keyAdmin = ts.addTenant(t, store.Tenant{ID: 2, Name: "admin", Enabled: true, Admin: true}, contractA)
	keyDisabled = ts.addTenant(t, store.Tenant{ID: 3, Name: "suspended", Enabled: false}, contractA)
	return ts, keyA, keyAdmin, keyDisabled
}

// newMultiTenantAuthServer builds a Server in multi-tenant mode, which is
// the only mode in which authenticate consults credentials at all. The
// package-level caching flag is process-wide, so it is reset on cleanup.
func newMultiTenantAuthServer(t *testing.T, ts store.TenantStore) *Server {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })
	return New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "").
		WithMultiTenancy(ts, MultiTenantOptions{MaxWatchedContracts: 10})
}

// decodeEnvelope asserts the body is the shared {"error": ...} envelope and
// returns it, so every rejection is checked against one shape.
func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) errorResponse {
	t.Helper()
	var env errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env),
		"body must be the shared error envelope, got %q", rec.Body.String())
	return env
}

// errTenants lets a test force a persistence failure at a chosen point in
// authenticate, so the 500 branches are covered rather than assumed. It
// embeds the real fake so only the injected method changes.
type errTenants struct {
	*fakeTenants
	lookupErr error
	scopeErr  error
}

func (e *errTenants) LookupTenantAPIKey(ctx context.Context, prefix string) (store.TenantAPIKey, []byte, store.Tenant, error) {
	if e.lookupErr != nil {
		return store.TenantAPIKey{}, nil, store.Tenant{}, e.lookupErr
	}
	return e.fakeTenants.LookupTenantAPIKey(ctx, prefix)
}

func (e *errTenants) ScopeForTenant(ctx context.Context, t store.Tenant) (store.Scope, error) {
	if e.scopeErr != nil {
		return store.Scope{}, e.scopeErr
	}
	return e.fakeTenants.ScopeForTenant(ctx, t)
}

// TestAPIKeyAuthHelper covers the operator API_KEY gate on the writes.
// Its defining property is fail-closed: no configured key means no writes.
func TestAPIKeyAuthHelper(t *testing.T) {
	t.Run("a matching header is accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/events", nil)
		req.Header.Set("X-API-Key", "operator-secret")
		rec, h := runAuthMiddleware(apiKeyAuth("operator-secret"), req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.True(t, h.called, "a correct key must reach the handler")
	})

	t.Run("a missing or wrong header is rejected", func(t *testing.T) {
		cases := []struct {
			name   string
			header string
		}{
			{"absent header", ""},
			{"empty header", " "},
			{"wrong key", "not-the-key"},
			{"a prefix of the key", "operator-secre"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodDelete, "/events", nil)
				if tc.header != "" {
					req.Header.Set("X-API-Key", tc.header)
				}
				rec, h := runAuthMiddleware(apiKeyAuth("operator-secret"), req)

				require.Equal(t, http.StatusUnauthorized, rec.Code)
				assert.False(t, h.called, "a rejected key must never reach the handler")
				assert.Equal(t, errAuthFailed.Error(), decodeEnvelope(t, rec).Error)
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			})
		}
	})

	t.Run("an empty configured API_KEY fails closed for every caller", func(t *testing.T) {
		// The dangerous regression is treating "" as "no auth required":
		// a request carrying any (or no) header would then reach the write
		// handler. Every caller must instead get 503 naming the env var.
		cases := []struct {
			name   string
			header string
		}{
			{"no header", ""},
			{"an arbitrary header", "anything"},
			{"an empty-string header", " "},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodDelete, "/events", nil)
				if tc.header != "" {
					req.Header.Set("X-API-Key", tc.header)
				}
				rec, h := runAuthMiddleware(apiKeyAuth(""), req)

				require.Equal(t, http.StatusServiceUnavailable, rec.Code)
				assert.False(t, h.called, "writes must be closed when no key is configured")
				assert.Equal(t, errAuthNotConfigured.Error(), decodeEnvelope(t, rec).Error)
			})
		}
	})
}

// TestAuthenticateHelper covers the credential-presentation path: which
// requests authenticate, which are rejected, and what principal a successful
// request carries. The principal's scope is the value every downstream read
// depends on, so it is asserted directly.
func TestAuthenticateHelper(t *testing.T) {
	tenants, keyA, _, keyDisabled := authTenants(t)

	unknown, _, _, err := GenerateAPIKey()
	require.NoError(t, err)
	parts := strings.Split(keyA, "_")
	require.Len(t, parts, 3)
	// A well-formed key that reuses a real, indexed prefix but a secret
	// nobody minted. Knowing the prefix must not be enough.
	forged := parts[0] + "_" + parts[1] + "_" + strings.Repeat("A", keySecretLen)

	build := func(bearer, xkey string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if xkey != "" {
			req.Header.Set("X-API-Key", xkey)
		}
		return req
	}

	cases := []struct {
		name         string
		bearer, xkey string
		wantStatus   int
		wantCalled   bool
		wantError    string
		wantWWWAuth  bool
		wantTenantID int64
		wantKeyID    int64
		wantScope    []string
	}{
		{
			name: "a valid bearer key authenticates", bearer: keyA,
			wantStatus: http.StatusOK, wantCalled: true,
			wantTenantID: 1, wantKeyID: 100, wantScope: []string{contractA},
		},
		{
			name: "a valid X-API-Key authenticates", xkey: keyA,
			wantStatus: http.StatusOK, wantCalled: true,
			wantTenantID: 1, wantKeyID: 100, wantScope: []string{contractA},
		},
		{
			name: "bearer wins when both credentials are present", bearer: keyA, xkey: "some-other-key",
			wantStatus: http.StatusOK, wantCalled: true,
			wantTenantID: 1, wantKeyID: 100, wantScope: []string{contractA},
		},
		{
			// The precedence runs the other way too: a bad Bearer must not
			// be rescued by a good X-API-Key.
			name: "a wrong bearer is not rescued by a valid X-API-Key", bearer: forged, xkey: keyA,
			wantStatus: http.StatusUnauthorized, wantError: "unknown or revoked API key", wantWWWAuth: true,
		},
		{
			name:       "an absent credential is rejected",
			wantStatus: http.StatusUnauthorized, wantError: "missing API key", wantWWWAuth: true,
		},
		{
			name: "a malformed credential is rejected", bearer: "not-a-key",
			wantStatus: http.StatusUnauthorized, wantError: "malformed API key", wantWWWAuth: true,
		},
		{
			name: "an unknown but well-formed key is rejected", bearer: unknown,
			wantStatus: http.StatusUnauthorized, wantError: "unknown or revoked API key", wantWWWAuth: true,
		},
		{
			name: "the right prefix with the wrong secret is rejected", bearer: forged,
			wantStatus: http.StatusUnauthorized, wantError: "unknown or revoked API key", wantWWWAuth: true,
		},
		{
			name: "a disabled tenant is refused", bearer: keyDisabled,
			wantStatus: http.StatusForbidden, wantError: "tenant is disabled",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newMultiTenantAuthServer(t, tenants)
			rec, h := runAuthMiddleware(srv.authenticate, build(tc.bearer, tc.xkey))

			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, tc.wantCalled, h.called)

			if !tc.wantCalled {
				assert.Equal(t, tc.wantError, decodeEnvelope(t, rec).Error)
				if tc.wantWWWAuth {
					assert.Equal(t, `Bearer realm="sorotrail"`, rec.Header().Get("WWW-Authenticate"),
						"RFC 7235 §4.1 requires a challenge on 401")
				} else {
					assert.Empty(t, rec.Header().Get("WWW-Authenticate"),
						"403 is not an authentication challenge")
				}
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"),
					"a rejection body must not be cached")
				return
			}

			p, ok := PrincipalFrom(h.req.Context())
			require.True(t, ok, "an authenticated request must carry a principal")
			assert.False(t, p.Untenanted)
			assert.Equal(t, tc.wantTenantID, p.Tenant.ID)
			assert.Equal(t, tc.wantKeyID, p.KeyID, "the key that authenticated must be recorded")
			assert.False(t, p.Scope.IsWildcard())
			assert.Equal(t, tc.wantScope, p.Scope.Contracts(),
				"the caller's scope must be resolved at authentication time")
		})
	}
}

// TestAuthenticatePublicPathsBypassCredentials pins the one deliberate
// exception: orchestrator probes carry no credential but expose no tenant
// data, so gating them would leave a multi-tenant pod failing readiness.
func TestAuthenticatePublicPathsBypassCredentials(t *testing.T) {
	tenants, _, _, _ := authTenants(t)
	srv := newMultiTenantAuthServer(t, tenants)

	for _, path := range []string{"/health", "/livez", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil) // no credential
			rec, h := runAuthMiddleware(srv.authenticate, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.True(t, h.called, "%s must stay reachable without a key", path)
			_, ok := PrincipalFrom(h.req.Context())
			assert.False(t, ok, "a public path must not be given a principal it did not earn")
		})
	}

	t.Run("a non-public path still requires a credential", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec, h := runAuthMiddleware(srv.authenticate, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.False(t, h.called)
	})
}

// TestAuthenticateSingleTenantMode covers the default deployment: no
// credential is required, and every request runs as the implicit wildcard
// principal with Untenanted set, so handlers can tell the two modes apart.
func TestAuthenticateSingleTenantMode(t *testing.T) {
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })
	srv := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	cases := []struct {
		name   string
		header func(*http.Request)
	}{
		{"no credential", func(*http.Request) {}},
		{"a bogus credential is ignored, not rejected", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer garbage")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/events", nil)
			tc.header(req)
			rec, h := runAuthMiddleware(srv.authenticate, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.True(t, h.called)

			p, ok := PrincipalFrom(h.req.Context())
			require.True(t, ok, "single-tenant mode must always install a principal")
			assert.True(t, p.Untenanted)
			assert.True(t, p.Scope.IsWildcard())
			assert.Zero(t, p.Tenant.ID)
		})
	}
}

// TestAuthenticateFailsClosedOnStoreErrors covers the two persistence
// failures on the authentication path. Both must deny with 500 rather than
// let a request through with a missing or partial principal.
func TestAuthenticateFailsClosedOnStoreErrors(t *testing.T) {
	cases := []struct {
		name     string
		inject   func(*errTenants)
		wantCode int
	}{
		{
			name:     "a key-lookup failure denies",
			inject:   func(e *errTenants) { e.lookupErr = errors.New("database down") },
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "a scope-resolution failure denies",
			inject:   func(e *errTenants) { e.scopeErr = errors.New("database down") },
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, keyA, _, _ := authTenants(t)
			ts := &errTenants{fakeTenants: base}
			tc.inject(ts)

			srv := newMultiTenantAuthServer(t, ts)
			req := httptest.NewRequest(http.MethodGet, "/events", nil)
			req.Header.Set("Authorization", "Bearer "+keyA)

			rec, h := runAuthMiddleware(srv.authenticate, req)
			assert.Equal(t, tc.wantCode, rec.Code, "body: %s", rec.Body.String())
			assert.False(t, h.called)
			assert.NotContains(t, rec.Body.String(), "database down",
				"internal errors must not be echoed to the caller")
		})
	}
}

// TestRequireAdminHelper covers the /admin/* gate independently of the
// router, including the single-tenant answer (404, not 403: there are no
// tenants to administer, so the route reads as absent).
func TestRequireAdminHelper(t *testing.T) {
	admin := Principal{Tenant: store.Tenant{ID: 2, Admin: true}, Scope: store.WildcardScope()}
	nonAdmin := Principal{Tenant: store.Tenant{ID: 1}, Scope: store.NewScope([]string{contractA})}
	untenanted := Principal{Untenanted: true, Scope: store.WildcardScope()}

	cases := []struct {
		name       string
		principal  *Principal
		wantStatus int
		wantCalled bool
		wantError  string
	}{
		{
			name: "no principal is treated as not-multi-tenant", wantStatus: http.StatusNotFound,
			wantError: "admin API is available only when MULTI_TENANT=true",
		},
		{
			name: "the single-tenant principal is refused", principal: &untenanted,
			wantStatus: http.StatusNotFound,
			wantError:  "admin API is available only when MULTI_TENANT=true",
		},
		{
			name: "a non-admin tenant is forbidden", principal: &nonAdmin,
			wantStatus: http.StatusForbidden, wantError: "admin privileges required",
		},
		{
			name: "an admin tenant is allowed", principal: &admin,
			wantStatus: http.StatusOK, wantCalled: true,
		},
	}

	srv := newMultiTenantAuthServer(t, newFakeTenants())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
			if tc.principal != nil {
				req = req.WithContext(WithPrincipal(req.Context(), *tc.principal))
			}
			rec, h := runAuthMiddleware(srv.requireAdmin, req)

			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			assert.Equal(t, tc.wantCalled, h.called)
			if !tc.wantCalled {
				assert.Equal(t, tc.wantError, decodeEnvelope(t, rec).Error)
			}
		})
	}

	t.Run("an allowed request keeps its principal", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
		req = req.WithContext(WithPrincipal(req.Context(), admin))
		_, h := runAuthMiddleware(srv.requireAdmin, req)

		p, ok := PrincipalFrom(h.req.Context())
		require.True(t, ok)
		assert.Equal(t, int64(2), p.Tenant.ID)
		assert.True(t, p.Tenant.Admin)
	})
}

// TestRequireTenantHelper covers the /tenant/* gate. Unlike requireAdmin it
// has no admin check: any real tenant may see its own identity and usage.
func TestRequireTenantHelper(t *testing.T) {
	tenant := Principal{Tenant: store.Tenant{ID: 7, Name: "seven"}, Scope: store.NewScope([]string{contractA})}
	untenanted := Principal{Untenanted: true, Scope: store.WildcardScope()}

	cases := []struct {
		name       string
		principal  *Principal
		wantStatus int
		wantCalled bool
		wantError  string
	}{
		{
			name: "no principal is treated as not-multi-tenant", wantStatus: http.StatusNotFound,
			wantError: "tenant API is available only when MULTI_TENANT=true",
		},
		{
			name: "the single-tenant principal is refused", principal: &untenanted,
			wantStatus: http.StatusNotFound, wantError: "tenant API is available only when MULTI_TENANT=true",
		},
		{
			name: "a real tenant is allowed", principal: &tenant,
			wantStatus: http.StatusOK, wantCalled: true,
		},
	}

	srv := newMultiTenantAuthServer(t, newFakeTenants())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tenant", nil)
			if tc.principal != nil {
				req = req.WithContext(WithPrincipal(req.Context(), *tc.principal))
			}
			rec, h := runAuthMiddleware(srv.requireTenant, req)

			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			assert.Equal(t, tc.wantCalled, h.called)
			if !tc.wantCalled {
				assert.Equal(t, tc.wantError, decodeEnvelope(t, rec).Error)
			}
		})
	}
}

// TestTenantRouteCarriesAuthenticatedScope chains the real middleware the
// router installs (authenticate → requireTenant) and asserts the handler
// sees the caller's resolved scope. The scope is what every tenant read
// depends on; a gate that admits the request but drops the scope would be
// broken in the quietest possible way.
func TestTenantRouteCarriesAuthenticatedScope(t *testing.T) {
	tenants, keyA, _, _ := authTenants(t)
	srv := newMultiTenantAuthServer(t, tenants)

	h := &authRecorder{}
	chained := srv.authenticate(srv.requireTenant(h))

	req := httptest.NewRequest(http.MethodGet, "/tenant", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.True(t, h.called)

	p, ok := PrincipalFrom(h.req.Context())
	require.True(t, ok)
	assert.Equal(t, int64(1), p.Tenant.ID)
	assert.Equal(t, []string{contractA}, scopeFrom(h.req.Context()).Contracts())
	assert.False(t, scopeFrom(h.req.Context()).IsWildcard())
}

// TestWriteUnauthorizedHelper pins the shape of every 401 the API emits:
// the shared error envelope, a challenge header, no caching, and no detail
// beyond the message it was given.
func TestWriteUnauthorizedHelper(t *testing.T) {
	rec := httptest.NewRecorder()
	writeUnauthorized(rec, "missing API key")

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, `Bearer realm="sorotrail"`, rec.Header().Get("WWW-Authenticate"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	// Exactly two keys ("error" and "code"): an extra field would be a
	// place for an internal reason to leak; a missing one would break
	// clients that decode the shared envelope.
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body, 2, "the envelope must carry only the error and code fields")
	assert.Equal(t, "missing API key", body["error"])
	assert.Equal(t, string(CodeUnauthorized), body["code"])

	var env errorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, "missing API key", env.Error)
	assert.Equal(t, CodeUnauthorized, env.Code)
}

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
func (s *authStubStore) GetSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) (store.Subscription, error) {
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
	s := New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	s.WithAPIKeyAuth(enabled)
	return s
}

// doReq runs a request against the server and returns the drained
// testResponse rather than a *http.Response with a dead body; the helper
// consumes and closes the body itself so bodyclose can see the response
// is fully handled.
func doReq(t *testing.T, s *Server, method, path, body string, headers map[string]string) (testResponse, []byte) {
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
	return testResponse{StatusCode: resp.StatusCode, Header: resp.Header}, rb
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
	drainWSResp(respWS)
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

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/events/ws"

	// Each dial gets its own deadline instead of sharing one context
	// with the setup: bcrypt hashing in addKey takes over a second
	// under -race, and a shared 2s budget made the second dial fail
	// with "context deadline exceeded" instead of exercising the gate.
	dial := func(opts *websocket.DialOptions) (*http.Response, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, resp, err := websocket.Dial(ctx, wsURL, opts)
		return resp, err
	}

	// Without a key the upgrade is rejected with 401.
	resp, err := dial(nil)
	drainWSResp(resp)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// With a valid key the upgrade proceeds (no broadcaster → 501, not
	// a 401, proving the key was accepted).
	key := st.addKey(t, "streamer")
	hdr := http.Header{"Authorization": {"Bearer " + key}}
	resp, err = dial(&websocket.DialOptions{HTTPHeader: hdr})
	drainWSResp(resp)
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
