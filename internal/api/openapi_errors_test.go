package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// The spec used to document a 200 per path and nothing else, so a generated
// client treated every failure as an unexpected status. The fix is only
// worth anything if the documented failures are the ones the handlers
// actually produce, which is what this file checks: each case drives a real
// request through the real router, observes the status the middleware and
// handlers really return, and then asserts the shipped spec documents that
// status for that route.
//
// The direction matters. Asserting "everything documented is reachable"
// would push toward documenting less; asserting "everything reachable is
// documented" is what a generated client needs.

// specOperation is one method on one path in the embedded spec.
type specOperation struct {
	Security  []map[string][]string   `json:"security"`
	Responses map[string]specResponse `json:"responses"`
}

type specResponse struct {
	Ref         string         `json:"$ref"`
	Description string         `json:"description"`
	Headers     map[string]any `json:"headers"`
	Content     map[string]any `json:"content"`
}

type specDocument struct {
	Security   []map[string][]string               `json:"security"`
	Paths      map[string]map[string]specOperation `json:"paths"`
	Components struct {
		Responses       map[string]specResponse `json:"responses"`
		SecuritySchemes map[string]struct {
			Type        string `json:"type"`
			Scheme      string `json:"scheme"`
			In          string `json:"in"`
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"securitySchemes"`
	} `json:"components"`
}

func loadSpec(t *testing.T) specDocument {
	t.Helper()
	var doc specDocument
	require.NoError(t, json.Unmarshal(openapiSpec, &doc), "openapi.json must parse")
	return doc
}

// resolve follows a response's $ref into components/responses, so a caller
// can inspect the concrete response regardless of how the path spelled it.
func (d specDocument) resolve(t *testing.T, r specResponse) specResponse {
	t.Helper()
	if r.Ref == "" {
		return r
	}
	const prefix = "#/components/responses/"
	require.True(t, len(r.Ref) > len(prefix) && r.Ref[:len(prefix)] == prefix,
		"unexpected $ref target %q", r.Ref)
	name := r.Ref[len(prefix):]
	target, ok := d.Components.Responses[name]
	require.True(t, ok, "spec references components/responses/%s, which does not exist", name)
	return target
}

// documented returns the response the spec declares for one status on one
// operation, failing the test when the operation says nothing about it.
func (d specDocument) documented(t *testing.T, method, route string, status int) specResponse {
	t.Helper()
	pathItem, ok := d.Paths[route]
	require.True(t, ok, "spec documents no path %q", route)
	op, ok := pathItem[method]
	require.True(t, ok, "spec documents no %s operation on %q", method, route)
	resp, ok := op.Responses[strconv.Itoa(status)]
	require.Truef(t, ok, "%s %s can return %d but the spec documents only %v",
		method, route, status, statusKeys(op.Responses))
	return d.resolve(t, resp)
}

func statusKeys(responses map[string]specResponse) []string {
	out := make([]string, 0, len(responses))
	for k := range responses {
		out = append(out, k)
	}
	return out
}

// errorCase is one reachable failure: how to build a server that produces
// it, the request that triggers it, and the chi route pattern the spec keys
// that request under.
type errorCase struct {
	name string
	// route is the chi pattern ("/events/{id}"), which is how both the
	// router and the spec name the operation; target is the concrete URL.
	route  string
	target string
	method string
	status int
	// envelope is false for the handful of responses written by http.Error
	// rather than writeError, which are plain text by design.
	envelope bool
	build    func(t *testing.T) (http.Handler, *http.Request)
}

func errorCases() []errorCase {
	const badContract = "CNOTACONTRACT"

	// Single-tenant server with a management key configured — the default
	// deployment shape, and the one most of these statuses come from.
	single := func(st *stubStore) func(*testing.T) http.Handler {
		return func(*testing.T) http.Handler { return newTestServer(st, nil).Router() }
	}
	plain := func(st *stubStore, method, target string) func(*testing.T) (http.Handler, *http.Request) {
		return func(t *testing.T) (http.Handler, *http.Request) {
			return single(st)(t), httptest.NewRequest(method, target, nil)
		}
	}

	return []errorCase{
		{
			name: "400 on an out-of-range limit", route: "/events", target: "/events?limit=99999",
			method: http.MethodGet, status: http.StatusBadRequest, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/events?limit=99999"),
		},
		{
			name: "400 when aggregate is given no bucket", route: "/events/aggregate",
			target: "/events/aggregate", method: http.MethodGet,
			status: http.StatusBadRequest, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/events/aggregate"),
		},
		{
			name: "400 on a malformed contract ID", route: "/contracts/{id}",
			target: "/contracts/" + badContract, method: http.MethodGet,
			status: http.StatusBadRequest, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/contracts/"+badContract),
		},
		{
			name: "400 when export omits its ledger bounds", route: "/contracts/{id}/export",
			target: "/contracts/" + contractA + "/export", method: http.MethodGet,
			status: http.StatusBadRequest, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/contracts/"+contractA+"/export"),
		},
		{
			name: "400 on a subscription ID that is not a number", route: "/subscriptions/{id}",
			target: "/subscriptions/not-a-number", method: http.MethodGet,
			status: http.StatusBadRequest, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/subscriptions/not-a-number"),
		},
		{
			name: "404 on an event that is not stored", route: "/events/{id}",
			target: "/events/0001099511627776-0000000001", method: http.MethodGet,
			status: http.StatusNotFound, envelope: true,
			build: plain(&stubStore{eventErr: store.ErrNotFound},
				http.MethodGet, "/events/0001099511627776-0000000001"),
		},
		{
			name: "404 on a contract with no indexed events", route: "/contracts/{id}",
			target: "/contracts/" + contractA, method: http.MethodGet,
			status: http.StatusNotFound, envelope: true,
			build: plain(&stubStore{contractSummaryErr: store.ErrNotFound},
				http.MethodGet, "/contracts/"+contractA),
		},
		{
			name:  "404 because the tenant API is inert on a single-tenant instance",
			route: "/tenant", target: "/tenant", method: http.MethodGet,
			status: http.StatusNotFound, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/tenant"),
		},
		{
			name:  "404 because the admin API is inert on a single-tenant instance",
			route: "/admin/tenants", target: "/admin/tenants", method: http.MethodGet,
			status: http.StatusNotFound, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/admin/tenants"),
		},
		{
			name: "500 when the store fails a list query", route: "/events", target: "/events",
			method: http.MethodGet, status: http.StatusInternalServerError, envelope: true,
			build: plain(&stubStore{queryErr: errStoreDown}, http.MethodGet, "/events"),
		},
		{
			name: "500 when the store fails a single-event read", route: "/events/{id}",
			target: "/events/0001099511627776-0000000001", method: http.MethodGet,
			status: http.StatusInternalServerError, envelope: true,
			build: plain(&stubStore{eventErr: errStoreDown},
				http.MethodGet, "/events/0001099511627776-0000000001"),
		},
		{
			name: "304 when a conditional event GET still matches", route: "/events/{id}",
			target: "/events/0001099511627776-0000000001", method: http.MethodGet,
			status: http.StatusNotModified,
			build: func(t *testing.T) (http.Handler, *http.Request) {
				h := newTestServer(&stubStore{exists: true}, nil).Router()
				req := httptest.NewRequest(http.MethodGet, "/events/0001099511627776-0000000001", nil)
				req.Header.Set("If-None-Match", `"0001099511627776-0000000001"`)
				return h, req
			},
		},
		{
			name: "401 when the management key is missing", route: "/events",
			target: "/events?before_ledger=5", method: http.MethodDelete,
			status: http.StatusUnauthorized, envelope: true,
			build: plain(&stubStore{}, http.MethodDelete, "/events?before_ledger=5"),
		},
		{
			name: "401 when the management key is wrong", route: "/watched-contracts",
			target: "/watched-contracts", method: http.MethodGet,
			status: http.StatusUnauthorized, envelope: true,
			build: func(t *testing.T) (http.Handler, *http.Request) {
				h := newTestServer(&stubStore{}, nil).Router()
				req := httptest.NewRequest(http.MethodGet, "/watched-contracts", nil)
				req.Header.Set("X-API-Key", "not-the-key")
				return h, req
			},
		},
		{
			name:  "503 when no management key is configured at all",
			route: "/dead-letters", target: "/dead-letters", method: http.MethodGet,
			status: http.StatusServiceUnavailable, envelope: true,
			build: func(t *testing.T) (http.Handler, *http.Request) {
				h := newTestServerWithKey(&stubStore{}, nil, "").Router()
				return h, httptest.NewRequest(http.MethodGet, "/dead-letters", nil)
			},
		},
		{
			name:  "503 when the metrics collector is not wired in",
			route: "/metrics", target: "/metrics", method: http.MethodGet,
			status: http.StatusServiceUnavailable, envelope: true,
			build: func(t *testing.T) (http.Handler, *http.Request) {
				s := newTestServer(&stubStore{}, nil)
				s.metrics = nil
				return s.Router(), httptest.NewRequest(http.MethodGet, "/metrics", nil)
			},
		},
		{
			name: "501 when the live stream has no broadcaster", route: "/events/ws",
			target: "/events/ws", method: http.MethodGet,
			status: http.StatusNotImplemented, envelope: true,
			build: plain(&stubStore{}, http.MethodGet, "/events/ws"),
		},
		{
			name:  "503 when the readiness probe's database is unreachable",
			route: "/readyz", target: "/readyz", method: http.MethodGet,
			status: http.StatusServiceUnavailable,
			build:  plain(&stubStore{pingErr: errStoreDown}, http.MethodGet, "/readyz"),
		},
		{
			name:  "503 when the health check's database is unreachable",
			route: "/health", target: "/health", method: http.MethodGet,
			status: http.StatusServiceUnavailable,
			build:  plain(&stubStore{pingErr: errStoreDown}, http.MethodGet, "/health"),
		},
	}
}

// errStoreDown stands in for any store failure the caller cannot act on.
var errStoreDown = errStore("store unavailable")

type errStore string

func (e errStore) Error() string { return string(e) }

// TestDocumentedErrorsMatchTheHandlers is the core check: drive the real
// router until it produces each failure, then require the spec to document
// exactly that status on exactly that route.
func TestDocumentedErrorsMatchTheHandlers(t *testing.T) {
	doc := loadSpec(t)

	for _, tc := range errorCases() {
		t.Run(tc.name, func(t *testing.T) {
			handler, req := tc.build(t)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			require.Equal(t, tc.status, rec.Code,
				"the case no longer produces the status it describes; body: %s", rec.Body.String())

			resp := doc.documented(t, methodKey(tc.method), tc.route, tc.status)
			assert.NotEmpty(t, resp.Description,
				"%s %s %d is documented but says nothing about what it means",
				tc.method, tc.route, tc.status)

			if tc.envelope {
				assertErrorEnvelope(t, rec)
				assertDocumentsErrorEnvelope(t, resp)
			}
		})
	}
}

// TestRateLimitedResponseMatchesItsDocumentation covers 429 separately
// because producing one needs a limiter wired in, and because the spec
// promises a Retry-After header that a generated client will want typed.
func TestRateLimitedResponseMatchesItsDocumentation(t *testing.T) {
	doc := loadSpec(t)

	// burst=1 at 0.5 rps: the second request inside ~2s is refused.
	lim := NewRateLimiter(0.5, 1, false)
	t.Cleanup(startLimiter(t, lim))

	s := newTestServer(&stubStore{}, nil)
	s.SetRateLimiter(lim)
	handler := s.Router()

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, mkReq(http.MethodGet, "/events"))
	require.Equal(t, http.StatusOK, first.Code)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, mkReq(http.MethodGet, "/events"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	retryAfter := rec.Header().Get("Retry-After")
	require.NotEmpty(t, retryAfter, "the limiter must send Retry-After")
	secs, err := strconv.Atoi(retryAfter)
	require.NoError(t, err, "Retry-After must be integer delta-seconds")
	assert.GreaterOrEqual(t, secs, 1)

	resp := doc.documented(t, "get", "/events", http.StatusTooManyRequests)
	assertErrorEnvelope(t, rec)
	assertDocumentsErrorEnvelope(t, resp)
	require.Contains(t, resp.Headers, "Retry-After",
		"the 429 response sends Retry-After but the spec does not document it")

	// The probes stay reachable under the same limiter, which is why they
	// carry no 429 in the spec.
	for _, exempt := range []string{"/health", "/livez", "/readyz", "/metrics"} {
		probe := httptest.NewRecorder()
		handler.ServeHTTP(probe, mkReq(http.MethodGet, exempt))
		assert.NotEqual(t, http.StatusTooManyRequests, probe.Code,
			"%s is exempt from rate limiting", exempt)

		op := doc.Paths[exempt]["get"]
		assert.NotContains(t, op.Responses, "429",
			"%s cannot be rate limited, so the spec must not claim it can", exempt)
	}
}

// TestTenantAuthFailuresMatchTheirDocumentation exercises the statuses that
// only exist once MULTI_TENANT is on, and checks the spec describes the
// scheme a caller has to satisfy rather than just naming the status.
func TestTenantAuthFailuresMatchTheirDocumentation(t *testing.T) {
	doc := loadSpec(t)
	f := newTenantFixture(t)

	t.Run("401 without a credential", func(t *testing.T) {
		rec := f.get(t, "", "/events")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, `Bearer realm="sorotrail"`, rec.Header().Get("WWW-Authenticate"),
			"RFC 7235 §4.1 requires a challenge on 401")

		resp := doc.documented(t, "get", "/events", http.StatusUnauthorized)
		require.Contains(t, resp.Headers, "WWW-Authenticate",
			"the 401 carries a challenge header the spec does not document")
		assertErrorEnvelope(t, rec)
	})

	t.Run("401 with an unknown key", func(t *testing.T) {
		rec := f.get(t, "st_AAAAAAAAAAAAAAAA_BBBBBBBB", "/events")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		doc.documented(t, "get", "/events", http.StatusUnauthorized)
	})

	t.Run("403 on a contract outside the tenant's grants", func(t *testing.T) {
		rec := f.get(t, f.keyA, "/events?contract_id="+contractB)
		require.Equal(t, http.StatusForbidden, rec.Code)
		doc.documented(t, "get", "/events", http.StatusForbidden)
		assertErrorEnvelope(t, rec)
	})

	t.Run("403 when a non-admin calls the admin API", func(t *testing.T) {
		rec := f.get(t, f.keyA, "/admin/tenants")
		require.Equal(t, http.StatusForbidden, rec.Code)
		doc.documented(t, "get", "/admin/tenants", http.StatusForbidden)
	})

	t.Run("409 when a tenant deletes itself", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/admin/tenants/4", nil)
		req.Header.Set("Authorization", "Bearer "+f.keyAdmin)
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		require.Equal(t, http.StatusConflict, rec.Code)
		doc.documented(t, "delete", "/admin/tenants/{id}", http.StatusConflict)
		assertErrorEnvelope(t, rec)
	})

	t.Run("403 for a tenant whose account is disabled", func(t *testing.T) {
		tenants := newFakeTenants()
		key := tenants.addTenant(t, store.Tenant{ID: 9, Name: "suspended"}, contractA)
		srv := New(&scopedStore{}, &stubRPC{health: rpc.Health{Status: "healthy"}},
			slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").
			WithMultiTenancy(tenants, MultiTenantOptions{}).
			Router()
		t.Cleanup(func() { SetTenantScopedCaching(false) })

		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code)
		doc.documented(t, "get", "/events", http.StatusForbidden)
	})

	// A caller who reads the spec has to be able to work out which
	// credential the 401 is asking for.
	require.Contains(t, doc.Components.SecuritySchemes, "TenantAuth")
	require.Contains(t, doc.Components.SecuritySchemes, "ManagementKey")
	assert.Equal(t, "bearer", doc.Components.SecuritySchemes["TenantAuth"].Scheme)
	assert.Equal(t, "X-API-Key", doc.Components.SecuritySchemes["ManagementKey"].Name)
	assert.NotEmpty(t, doc.Security, "the default security requirement must be declared")
}

// TestManagementEndpointsDeclareTheirScheme checks the endpoints that are
// gated no matter how the instance is configured say so in the spec, since
// that is the difference between "401 only under MULTI_TENANT" and "401
// always" — and the spec is where an operator has to learn it.
func TestManagementEndpointsDeclareTheirScheme(t *testing.T) {
	doc := loadSpec(t)

	gated := []struct{ method, route string }{
		{"delete", "/events"},
		{"get", "/watched-contracts"},
		{"post", "/watched-contracts"},
		{"delete", "/watched-contracts/{id}"},
		{"get", "/dead-letters"},
		{"delete", "/dead-letters/{id}"},
	}
	for _, g := range gated {
		op := doc.Paths[g.route][g.method]
		require.Lenf(t, op.Security, 1,
			"%s %s is API_KEY-gated and must declare exactly one security requirement",
			g.method, g.route)
		require.Contains(t, op.Security[0], "ManagementKey")
		require.Contains(t, op.Responses, "401",
			"%s %s rejects a missing key with 401", g.method, g.route)
		require.Contains(t, op.Responses, "503",
			"%s %s fails closed with 503 when API_KEY is unset", g.method, g.route)
	}
}

// TestEveryReusableResponseIsUsed keeps components/responses honest in the
// other direction: a reusable response nothing references is documentation
// that has outlived its endpoint.
func TestEveryReusableResponseIsUsed(t *testing.T) {
	doc := loadSpec(t)
	used := make(map[string]bool, len(doc.Components.Responses))
	for _, pathItem := range doc.Paths {
		for _, op := range pathItem {
			for _, resp := range op.Responses {
				const prefix = "#/components/responses/"
				if len(resp.Ref) > len(prefix) && resp.Ref[:len(prefix)] == prefix {
					used[resp.Ref[len(prefix):]] = true
				}
			}
		}
	}
	for name := range doc.Components.Responses {
		assert.Truef(t, used[name], "components/responses/%s is referenced by no path", name)
	}
}

// TestEveryErrorStatusIsAReusableResponse stops the shared envelope from
// being re-described inline. Everything the handlers write through
// writeError has the same shape, so every 4xx/5xx that carries it must
// reach that description through a $ref rather than restating it.
func TestEveryErrorStatusIsAReusableResponse(t *testing.T) {
	doc := loadSpec(t)

	// The exceptions are the responses that genuinely are not the error
	// envelope: the probes report health with a status/checks body rather
	// than an error message.
	inline := map[string]bool{
		"get /health 503": true,
		"get /readyz 503": true,
	}

	for route, pathItem := range doc.Paths {
		for method, op := range pathItem {
			for status, resp := range op.Responses {
				code, err := strconv.Atoi(status)
				require.NoError(t, err)
				if code < 400 {
					continue
				}
				if inline[method+" "+route+" "+status] {
					continue
				}
				assert.NotEmptyf(t, resp.Ref,
					"%s %s %s describes an error inline; reference components/responses instead",
					method, route, status)
			}
		}
	}
}

func methodKey(method string) string {
	switch method {
	case http.MethodGet:
		return "get"
	case http.MethodPost:
		return "post"
	case http.MethodPut:
		return "put"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	default:
		return method
	}
}

// assertErrorEnvelope checks the response body is the one shape every
// writeError produces, so a client can decode failures with a single type.
func assertErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var env struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env),
		"error responses must be JSON: %s", rec.Body.String())
	assert.NotEmpty(t, env.Error, "the error envelope must carry a message")
}

// assertDocumentsErrorEnvelope checks the spec points the same body at the
// one shared schema rather than redescribing it per path.
func assertDocumentsErrorEnvelope(t *testing.T, resp specResponse) {
	t.Helper()
	body, ok := resp.Content["application/json"].(map[string]any)
	require.True(t, ok, "the response must document an application/json body")
	schema, ok := body["schema"].(map[string]any)
	require.True(t, ok, "the response body must document a schema")
	assert.Equal(t, "#/components/schemas/ErrorResponse", schema["$ref"],
		"error bodies must reference the shared envelope")
}
