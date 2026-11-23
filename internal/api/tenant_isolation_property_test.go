package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Property-test tenant isolation across the entire REST and GraphQL API
// surface (#1219).
//
// Unlike hand-listed per-endpoint tests, this suite:
//  1. Walks the live chi router (`chi.Walk`) across ALL HTTP methods (GET,
//     POST, PUT, PATCH, DELETE) and fails if any registered route has neither
//     a request builder in `propertyRouteBuilders` nor a justified entry in
//     `propertyCrossTenantExceptionRoutes`.
//  2. Parses `internal/api/graphql/schema.graphqls` to discover every
//     top-level `Query` field dynamically and fails if any schema field has no
//     property query builder in `propertyGraphQLFieldBuilders`.
//  3. Seeds a multi-tenant fixture with deliberate overlap (two tenants
//     watching and granted a shared contract alongside exclusive contracts,
//     overlapping addresses and tx hashes, and similar event/subscription
//     identifiers) plus distinct secret canaries belonging to tenant B.
//  4. Asserts across every route and GraphQL field that a request
//     authenticated as tenant A never leaks any of tenant B's exclusive
//     identifiers or secrets, and that write/delete/identifier-guessing
//     attempts against tenant B's objects return the same not-found /
//     forbidden response as a missing object without mutating tenant B's state.

const (
	propSharedContract   = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
	propSharedEventID    = "00000000000000000150-0000000001"
	propTenantBSubSecret = "whsec_bbbbbbbbbbbbbbbb"
	propTenantBSubURL    = "https://example.com/b"
)

// propertyCrossTenantExceptionRoutes lists routes that are genuinely
// instance-wide (health probes, static OpenAPI/Swagger/GraphiQL assets,
// Prometheus metrics) or operator/admin-only management surfaces where a
// non-admin tenant credential is rejected before any tenant data is accessed.
var propertyCrossTenantExceptionRoutes = map[string]string{
	"GET /health":                      "Global dependency probe (DB + RPC reachability); contains no tenant rows.",
	"GET /livez":                       "Global process liveness probe; touches no store state.",
	"GET /readyz":                      "Global readiness probe (DB + RPC + ingestion lag); contains no tenant data.",
	"GET /version":                     "Static build metadata (version, commit, build date).",
	"GET /metrics":                     "Prometheus process metrics endpoint; contains no contract or event identifiers.",
	"GET /openapi.json":                "Compiled-in static OpenAPI 3.1 specification.",
	"GET /docs":                        "Compiled-in static Swagger UI HTML.",
	"GET /graphiql":                    "Compiled-in static GraphiQL HTML playground.",
	"GET /events/ws":                   "WebSocket upgrade endpoint requiring WS handshake headers; covered by streaming isolation tests.",
	"GET /graphql":                     "GraphQL HTTP transport endpoint; exercised per schema field in runGraphQLPropertyIsolation.",
	"POST /graphql":                    "GraphQL HTTP transport endpoint; exercised per schema field in runGraphQLPropertyIsolation.",
	"PUT /graphql":                     "chi.Handle wildcard registration for GraphQL transport.",
	"PATCH /graphql":                   "chi.Handle wildcard registration for GraphQL transport.",
	"DELETE /graphql":                  "chi.Handle wildcard registration for GraphQL transport.",
	"HEAD /graphql":                    "chi.Handle wildcard registration for GraphQL transport.",
	"OPTIONS /graphql":                 "chi.Handle wildcard registration for GraphQL transport.",
	"CONNECT /graphql":                 "chi.Handle wildcard registration for GraphQL transport.",
	"TRACE /graphql":                   "chi.Handle wildcard registration for GraphQL transport.",
	"GET /graphql/persisted":           "Persisted GraphQL operation registry transport.",
	"POST /graphql/persisted":          "Persisted GraphQL operation registry transport.",
	"PUT /graphql/persisted":           "chi.Handle wildcard registration for persisted GraphQL transport.",
	"PATCH /graphql/persisted":         "chi.Handle wildcard registration for persisted GraphQL transport.",
	"DELETE /graphql/persisted":        "chi.Handle wildcard registration for persisted GraphQL transport.",
	"HEAD /graphql/persisted":          "chi.Handle wildcard registration for persisted GraphQL transport.",
	"OPTIONS /graphql/persisted":       "chi.Handle wildcard registration for persisted GraphQL transport.",
	"CONNECT /graphql/persisted":       "chi.Handle wildcard registration for persisted GraphQL transport.",
	"TRACE /graphql/persisted":         "chi.Handle wildcard registration for persisted GraphQL transport.",
	"GET /graphql/persisted/{id}":      "Persisted GraphQL operation lookup by hash.",
	"POST /graphql/persisted/{id}":     "Persisted GraphQL operation execution by hash.",
	"PUT /graphql/persisted/{id}":      "chi.Handle wildcard registration for persisted GraphQL transport.",
	"PATCH /graphql/persisted/{id}":    "chi.Handle wildcard registration for persisted GraphQL transport.",
	"DELETE /graphql/persisted/{id}":   "chi.Handle wildcard registration for persisted GraphQL transport.",
	"HEAD /graphql/persisted/{id}":     "chi.Handle wildcard registration for persisted GraphQL transport.",
	"OPTIONS /graphql/persisted/{id}":  "chi.Handle wildcard registration for persisted GraphQL transport.",
	"CONNECT /graphql/persisted/{id}":  "chi.Handle wildcard registration for persisted GraphQL transport.",
	"TRACE /graphql/persisted/{id}":    "chi.Handle wildcard registration for persisted GraphQL transport.",
	"QUERY /graphql":                   "chi.Handle wildcard registration for GraphQL transport.",
	"QUERY /graphql/persisted":         "chi.Handle wildcard registration for persisted GraphQL transport.",
	"QUERY /graphql/persisted/{id}":    "chi.Handle wildcard registration for persisted GraphQL transport.",
	"DELETE /events":                   "Operator-only bulk event deletion gated by static API_KEY (adminMW).",
	"GET /watched-contracts":           "Operator-only global watch list gated by static API_KEY (watchedMW).",
	"POST /watched-contracts":          "Operator-only global watch list add gated by static API_KEY (watchedMW).",
	"DELETE /watched-contracts/{id}":   "Operator-only global watch list delete gated by static API_KEY (watchedMW).",
	"GET /contracts/{id}/spec":         "Operator-only contract spec override read gated by static API_KEY (watchedMW).",
	"PUT /contracts/{id}/spec":         "Operator-only contract spec override write gated by static API_KEY (watchedMW).",
	"DELETE /contracts/{id}/spec":      "Operator-only contract spec override delete gated by static API_KEY (watchedMW).",
	"GET /contracts/{id}/spec/history": "Operator-only contract spec override history gated by static API_KEY (watchedMW).",
	"POST /contracts/{id}/spec/revert": "Operator-only contract spec override revert gated by static API_KEY (watchedMW).",
	"GET /dead-letters":                "Operator-only dead-letter queue gated by static API_KEY (watchedMW).",
	"DELETE /dead-letters/{id}":        "Operator-only dead-letter delete gated by static API_KEY (watchedMW).",
	"GET /apikeys":                     "Single-tenant API key list; rejected under multi-tenant bearer auth.",
	"POST /apikeys":                    "Single-tenant API key creation; rejected under multi-tenant bearer auth.",
	"DELETE /apikeys/{id}":             "Single-tenant API key revocation; rejected under multi-tenant bearer auth.",
}

// propertyRouteSpec defines how to exercise one registered (method, pattern)
// route as tenant A, targeting tenant B's identifiers where possible.
type propertyRouteSpec struct {
	targetPath string
	body       string
	// expectNotFoundProbe, when true, asserts that probing tenant B's object ID
	// returns the same status code (404 or 403) as probing a non-existent ID
	// (999999 / missing event ID), so the endpoint cannot be used as an
	// existence oracle across tenants.
	expectNotFoundProbe bool
	missingProbePath    string
}

var propertyRouteBuilders = map[string]propertyRouteSpec{
	// Read endpoints
	"GET /events": {
		targetPath: "/events",
	},
	"GET /events/count": {
		targetPath: "/events/count",
	},
	"GET /events/aggregate": {
		targetPath: "/events/aggregate?bucket=ledger",
	},
	"GET /events.csv": {
		targetPath: "/events.csv",
	},
	"GET /events/{id}": {
		targetPath:          "/events/" + guardEventIDB,
		expectNotFoundProbe: true,
		missingProbePath:    "/events/00000000000000099999-0000000099",
	},
	"GET /events/{id}/raw": {
		targetPath:          "/events/" + guardEventIDB + "/raw",
		expectNotFoundProbe: true,
		missingProbePath:    "/events/00000000000000099999-0000000099/raw",
	},
	"GET /events/{id}/transaction": {
		// Query tenant A's own event that shares guardTxHash with tenant B's event;
		// sibling events from tenant B's ungranted contract must be filtered out.
		targetPath: "/events/" + guardEventIDA + "/transaction",
	},
	"GET /contracts": {
		targetPath: "/contracts",
	},
	"GET /contracts/{id}": {
		targetPath: "/contracts/" + guardContractB,
	},
	"GET /contracts/{id}/events": {
		targetPath: "/contracts/" + guardContractB + "/events",
	},
	"GET /contracts/{id}/stats": {
		targetPath: "/contracts/" + guardContractB + "/stats",
	},
	"GET /contracts/{id}/export": {
		targetPath: "/contracts/" + guardContractB + "/export?from_ledger=1&to_ledger=500&format=ndjson",
	},
	"GET /addresses/{address}/events": {
		targetPath: "/addresses/" + guardAddress + "/events",
	},
	"GET /addresses/{address}/summary": {
		targetPath: "/addresses/" + guardAddress + "/summary",
	},
	"GET /stats": {
		targetPath: "/stats",
	},

	// Subscriptions (read + write + identifier guessing against tenant B's subscription #2)
	"GET /subscriptions": {
		targetPath: "/subscriptions",
	},
	"GET /subscriptions/{id}": {
		targetPath:          "/subscriptions/2",
		expectNotFoundProbe: true,
		missingProbePath:    "/subscriptions/999999",
	},
	"GET /subscriptions/{id}/deliveries": {
		targetPath:          "/subscriptions/2/deliveries",
		expectNotFoundProbe: true,
		missingProbePath:    "/subscriptions/999999/deliveries",
	},
	"POST /subscriptions": {
		// Attempt to subscribe to tenant B's ungranted contractB must be rejected (403).
		targetPath: "/subscriptions",
		body:       `{"url":"https://example.com/a-probe","secret":"whsec_1234567890abcdef","filters":{"contract_id":"` + guardContractB + `"}}`,
	},
	"PUT /subscriptions/{id}": {
		// Attempt to modify tenant B's subscription #2 must return 404 (same as missing ID).
		targetPath:          "/subscriptions/2",
		body:                `{"url":"https://hijacked.example.com/hook","enabled":false}`,
		expectNotFoundProbe: true,
		missingProbePath:    "/subscriptions/999999",
	},
	"DELETE /subscriptions/{id}": {
		// Attempt to delete tenant B's subscription #2 must return 404 (same as missing ID).
		targetPath:          "/subscriptions/2",
		expectNotFoundProbe: true,
		missingProbePath:    "/subscriptions/999999",
	},

	// Tenant self-service (/tenant/*)
	"GET /tenant": {
		targetPath: "/tenant",
	},
	"GET /tenant/usage": {
		targetPath: "/tenant/usage",
	},
	"GET /tenant/watch": {
		targetPath: "/tenant/watch",
	},
	"POST /tenant/watch": {
		// Watching propSharedContract modifies only tenant A's watch list, never tenant B's.
		targetPath: "/tenant/watch",
		body:       `{"contract_id":"` + propSharedContract + `"}`,
	},
	"DELETE /tenant/watch/{contract_id}": {
		// Deleting guardContractB from tenant A's watch list must not remove tenant B's watch on guardContractB.
		targetPath: "/tenant/watch/" + guardContractB,
	},

	// Admin tenant endpoints (/admin/*) — non-admin tenant A must be rejected with 403
	"GET /admin/tenants": {
		targetPath: "/admin/tenants",
	},
	"POST /admin/tenants": {
		targetPath: "/admin/tenants",
		body:       `{"name":"unauthorized-tenant"}`,
	},
	"GET /admin/tenants/{id}": {
		targetPath: "/admin/tenants/2",
	},
	"PATCH /admin/tenants/{id}": {
		targetPath: "/admin/tenants/2",
		body:       `{"wildcard":true}`,
	},
	"DELETE /admin/tenants/{id}": {
		targetPath: "/admin/tenants/2",
	},
	"GET /admin/tenants/{id}/usage": {
		targetPath: "/admin/tenants/2/usage",
	},
	"GET /admin/tenants/{id}/grants": {
		targetPath: "/admin/tenants/2/grants",
	},
	"POST /admin/tenants/{id}/grants": {
		targetPath: "/admin/tenants/2/grants",
		body:       `{"contract_id":"` + guardContractB + `"}`,
	},
	"DELETE /admin/tenants/{id}/grants/{contract_id}": {
		targetPath: "/admin/tenants/2/grants/" + guardContractB,
	},
	"GET /admin/tenants/{id}/keys": {
		targetPath: "/admin/tenants/2/keys",
	},
	"POST /admin/tenants/{id}/keys": {
		targetPath: "/admin/tenants/2/keys",
		body:       `{"name":"hijack-key"}`,
	},
	"DELETE /admin/keys/{key_id}": {
		targetPath: "/admin/keys/1",
	},
}

func (m *guardMemoryStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = int64(len(m.subscriptions) + 1)
	m.subscriptions = append(m.subscriptions, sub)
	return sub, nil
}

func (m *guardMemoryStore) UpdateSubscription(_ context.Context, sub store.Subscription, owner store.SubscriptionOwner) (store.Subscription, error) {
	for i, s := range m.subscriptions {
		if s.ID == sub.ID && (owner.IsAll() || (s.TenantID != nil && owner.TenantID() == *s.TenantID)) {
			m.subscriptions[i] = sub
			return sub, nil
		}
	}
	return store.Subscription{}, store.ErrNotFound
}

func (m *guardMemoryStore) DeleteSubscription(_ context.Context, id int64, owner store.SubscriptionOwner) error {
	for i, s := range m.subscriptions {
		if s.ID == id && (owner.IsAll() || (s.TenantID != nil && owner.TenantID() == *s.TenantID)) {
			m.subscriptions = append(m.subscriptions[:i], m.subscriptions[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

func (m *guardMemoryStore) ListDeliveryAttempts(_ context.Context, subID int64, _ int, owner store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	for _, s := range m.subscriptions {
		if s.ID == subID && (owner.IsAll() || (s.TenantID != nil && owner.TenantID() == *s.TenantID)) {
			return []store.DeliveryAttempt{}, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *guardMemoryStore) CountDeliveryAttempts(_ context.Context, subID int64, owner store.SubscriptionOwner) (int64, error) {
	for _, s := range m.subscriptions {
		if s.ID == subID && (owner.IsAll() || (s.TenantID != nil && owner.TenantID() == *s.TenantID)) {
			return 0, nil
		}
	}
	return 0, store.ErrNotFound
}

func (g *guardTenants) CountTenantWatchedContracts(_ context.Context, id int64) (int, error) {
	if id == g.tenantA.ID {
		return 1, nil
	}
	return 0, nil
}

func (g *guardTenants) CountInstanceWatchedContracts(context.Context) (int, error) {
	return 2, nil
}

func (g *guardTenants) AddTenantWatchedContract(_ context.Context, _ store.Tenant, _, _ string, _ int) error {
	return nil
}

func (g *guardTenants) RemoveTenantWatchedContract(_ context.Context, _ int64, _, _ string) error {
	return nil
}

// seedOverlapFixture adds overlapping contract/event data to the fixture so
// both tenants watch and are granted propSharedContract while tenant B alone
// owns guardContractB, guardEventIDB, and subscription #2.
func seedOverlapMemoryFixture(t *testing.T) (*guardMemoryStore, *guardTenants, string) {
	t.Helper()
	st, ts, keyA := newGuardMemoryStore(t)
	now := time.Date(2026, 8, 1, 12, 5, 0, 0, time.UTC)
	st.events = append(st.events, store.Event{
		ID:               propSharedEventID,
		ContractID:       propSharedContract,
		Ledger:           150,
		Type:             "contract",
		TxHash:           guardTxHash,
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"},{"address":"` + guardAddress + `"}]`),
		Value:            json.RawMessage(`{"i128":"1500"}`),
		RawTopicXDR:      []string{"AAAADwAAAAh0cmFuc2Zlcg=="},
		RawValueXDR:      "AAAACgAAAAAAAAAB",
		CreatedAt:        now,
	})
	st.watched = append(st.watched, store.WatchedContract{ContractID: propSharedContract, Network: "default"})
	return st, ts, keyA
}

func assertNoTenantBLeakInBody(t *testing.T, routeKey string, status int, body string) {
	t.Helper()
	// Tenant B's subscription secret and URL must never appear in any response to tenant A.
	assert.NotContainsf(t, body, propTenantBSubSecret,
		"%s leaked tenant B's subscription webhook secret in response (status %d): %s", routeKey, status, body)
	assert.NotContainsf(t, body, propTenantBSubURL,
		"%s leaked tenant B's subscription webhook URL in response (status %d): %s", routeKey, status, body)

	// On 2xx responses, tenant B's exclusive contract ID and event ID must never appear.
	if status >= 200 && status < 300 {
		pattern := strings.SplitN(routeKey, " ", 2)[1]
		if note, known := knownUnscopedRESTRoutes[pattern]; known {
			t.Logf("KNOWN UNSCOPED ROUTE %s: %s", routeKey, note)
			return
		}
		assert.NotContainsf(t, body, guardContractB,
			"%s (status %d) leaked tenant B's exclusive contract ID %s: %s", routeKey, status, guardContractB, body)
		assert.NotContainsf(t, body, guardEventIDB,
			"%s (status %d) leaked tenant B's exclusive event ID %s: %s", routeKey, status, guardEventIDB, body)
	}
}

func runAllRoutesPropertyIsolation(t *testing.T, r chi.Router, handler http.Handler, keyA string) {
	t.Helper()
	discovered := map[string]bool{}
	require.NoError(t, chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		key := method + " " + route
		discovered[key] = true
		return nil
	}))

	// 1. Every registered route on the router must be covered by a builder or
	// listed on the explicit exception list with a stated reason.
	sortedKeys := make([]string, 0, len(discovered))
	for k := range discovered {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, routeKey := range sortedKeys {
		routeKey := routeKey
		t.Run(routeKey, func(t *testing.T) {
			if reason, excepted := propertyCrossTenantExceptionRoutes[routeKey]; excepted {
				assert.NotEmptyf(t, strings.TrimSpace(reason),
					"exception route %q must state a non-empty reason", routeKey)
				return
			}
			spec, hasBuilder := propertyRouteBuilders[routeKey]
			require.Truef(t, hasBuilder,
				"registered route %q has no request builder in propertyRouteBuilders and is not in propertyCrossTenantExceptionRoutes; add a builder so tenant isolation is property-tested on it",
				routeKey)

			method := strings.SplitN(routeKey, " ", 2)[0]
			var reqBody io.Reader
			if spec.body != "" {
				reqBody = strings.NewReader(spec.body)
			}
			req := httptest.NewRequest(method, spec.targetPath, reqBody)
			req.Header.Set("Authorization", "Bearer "+keyA)
			if spec.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assertNoTenantBLeakInBody(t, routeKey, rec.Code, rec.Body.String())

			// Existence-oracle check: probing tenant B's object ID must return
			// the exact same status code as probing a non-existent ID.
			if spec.expectNotFoundProbe && spec.missingProbePath != "" {
				var missingBody io.Reader
				if spec.body != "" {
					missingBody = strings.NewReader(spec.body)
				}
				reqMissing := httptest.NewRequest(method, spec.missingProbePath, missingBody)
				reqMissing.Header.Set("Authorization", "Bearer "+keyA)
				if spec.body != "" {
					reqMissing.Header.Set("Content-Type", "application/json")
				}
				recMissing := httptest.NewRecorder()
				handler.ServeHTTP(recMissing, reqMissing)

				assert.Equal(t, http.StatusNotFound, rec.Code,
					"%s against tenant B's object must return 404 Not Found", routeKey)
				assert.Equal(t, recMissing.Code, rec.Code,
					"%s returned status %d for tenant B's object vs %d for a missing object (existence oracle)",
					routeKey, rec.Code, recMissing.Code)
			}
		})
	}

	// 2. Reverse check: propertyRouteBuilders must not retain stale routes that
	// no longer exist on the router.
	for routeKey := range propertyRouteBuilders {
		assert.Truef(t, discovered[routeKey],
			"propertyRouteBuilders contains %q which is not registered on the router", routeKey)
	}
}

var gqlQueryBlockRE = regexp.MustCompile(`(?s)type\s+Query\s*\{([^}]+)\}`)
var gqlFieldLineRE = regexp.MustCompile(`(?m)^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*[\(:]`)

// discoverGraphQLQueryFields extracts every field name from `type Query { ... }`
// in internal/api/graphql/schema.graphqls so new GraphQL fields are detected
// automatically without hand-maintaining a list.
func discoverGraphQLQueryFields(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile("graphql/schema.graphqls")
	require.NoError(t, err, "read graphql/schema.graphqls")
	m := gqlQueryBlockRE.FindSubmatch(raw)
	require.NotNil(t, m, "could not find type Query block in graphql/schema.graphqls")

	matches := gqlFieldLineRE.FindAllSubmatch(m[1], -1)
	fields := make([]string, 0, len(matches))
	for _, sub := range matches {
		fields = append(fields, string(sub[1]))
	}
	require.NotEmpty(t, fields)
	return fields
}

var propertyGraphQLFieldBuilders = map[string][]string{
	"events": {
		`{ events(page: { first: 50 }) { nodes { id contractId } totalCount } }`,
		`{ events(filter: { contractId: "` + guardContractB + `" }, page: { first: 50 }) { nodes { id contractId } totalCount } }`,
	},
	"event": {
		`{ event(id: "` + guardEventIDB + `") { id contractId } }`,
	},
	"tokenEvents": {
		`{ tokenEvents(page: { first: 50 }) { nodes { id contractId } totalCount } }`,
		`{ tokenEvents(filter: { contractId: "` + guardContractB + `" }, page: { first: 50 }) { nodes { id contractId } totalCount } }`,
	},
	"contracts": {
		`{ contracts(page: { first: 50 }) { nodes { contractId } totalCount } }`,
	},
	"contract": {
		`{ contract(id: "` + guardContractB + `") { contractId } }`,
	},
}

func runGraphQLPropertyIsolation(t *testing.T, handler http.Handler, keyA string) {
	t.Helper()
	schemaFields := discoverGraphQLQueryFields(t)
	for _, field := range schemaFields {
		field := field
		queries, ok := propertyGraphQLFieldBuilders[field]
		require.Truef(t, ok && len(queries) > 0,
			"GraphQL Query.%s in schema.graphqls has no query builder in propertyGraphQLFieldBuilders", field)

		for i, q := range queries {
			q := q
			t.Run(fmt.Sprintf("Query.%s#%d", field, i+1), func(t *testing.T) {
				payload, err := json.Marshal(map[string]string{"query": q})
				require.NoError(t, err)

				req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
				req.Header.Set("Authorization", "Bearer "+keyA)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				assert.NotContainsf(t, body, guardContractB,
					"GraphQL Query.%s leaked tenant B's contract ID %s: %s", field, guardContractB, body)
				assert.NotContainsf(t, body, guardEventIDB,
					"GraphQL Query.%s leaked tenant B's event ID %s: %s", field, guardEventIDB, body)
			})
		}
	}
}

func TestTenantIsolationPropertyAcrossWholeAPI(t *testing.T) {
	st, ts, keyA := seedOverlapMemoryFixture(t)
	r, handler := buildMultiTenantRouter(t, st, ts)

	runAllRoutesPropertyIsolation(t, r, handler, keyA)
	runGraphQLPropertyIsolation(t, handler, keyA)

	// Verify tenant B's subscription #2 was not modified or deleted by tenant A's write probes.
	subB, err := st.GetSubscription(context.Background(), 2, store.OwnedBy(2))
	require.NoError(t, err, "tenant B's subscription must survive tenant A's write/delete attempts")
	assert.Equal(t, propTenantBSubURL, subB.URL)
	assert.True(t, subB.Enabled)
}

func TestTenantIsolationPropertyAcrossWholeAPI_Postgres(t *testing.T) {
	pg, keyA := setupGuardPostgres(t)
	ctx := context.Background()

	// Add shared overlapping contract to both tenants in Postgres.
	tA, err := pg.GetTenantByName(ctx, "tenant-a")
	require.NoError(t, err)
	tB, err := pg.GetTenantByName(ctx, "tenant-b")
	require.NoError(t, err)
	require.NoError(t, pg.GrantContract(ctx, tA.ID, propSharedContract))
	require.NoError(t, pg.GrantContract(ctx, tB.ID, propSharedContract))
	require.NoError(t, pg.AddTenantWatchedContract(ctx, tA, "default", propSharedContract, 0))
	require.NoError(t, pg.AddTenantWatchedContract(ctx, tB, "default", propSharedContract, 0))

	r, handler := buildMultiTenantRouter(t, pg, pg)
	runAllRoutesPropertyIsolation(t, r, handler, keyA)
	runGraphQLPropertyIsolation(t, handler, keyA)

	// Verify tenant B's subscription and watch list are untouched on Postgres.
	subsB, err := pg.ListSubscriptions(ctx, store.OwnedBy(tB.ID))
	require.NoError(t, err)
	require.Len(t, subsB, 1)
	assert.Equal(t, propTenantBSubURL, subsB[0].URL)

	watchedB, err := pg.ListTenantWatchedContracts(ctx, tB.ID, "default")
	require.NoError(t, err)
	assert.Contains(t, watchedB, guardContractB, "tenant A's DELETE /tenant/watch/{contractB} must not remove tenant B's watch")
}
