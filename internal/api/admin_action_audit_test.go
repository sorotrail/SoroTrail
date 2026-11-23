package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/store"
)

// TestMutatingRoutes_EmitAdminActionAuditRecord mechanically walks the router
// via chi.Walk and fails if:
//  1. Any mutating route (POST, PUT, PATCH, DELETE) registered on s.router()
//     is missing from the administrative action inventory, OR
//  2. Any mutating route in the inventory is no longer registered on s.router(), OR
//  3. Executing a request against any mutating route fails to write a durable
//     store.ActionAuditRecord.
func TestMutatingRoutes_EmitAdminActionAuditRecord(t *testing.T) {
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	st := newAuthStubStore()
	tenants := newFakeTenants()
	adminKey := tenants.addTenant(t, store.Tenant{ID: 1, Name: "admin-tenant", Admin: true, Enabled: true, Wildcard: true})

	srv := New(st, nil, testLogger(), "operator-static-secret").
		WithMultiTenancy(tenants, MultiTenantOptions{})
	router := srv.router()

	var discovered []routeEntry
	err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if isMutatingHTTPMethod(method) {
			discovered = append(discovered, routeEntry{
				Method: strings.ToUpper(method),
				Path:   strings.TrimRight(route, "/"),
			})
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, discovered, "expected mutating routes on s.router()")

	registeredSet := map[string]bool{}
	for _, rt := range discovered {
		key := rt.Method + " " + rt.Path
		registeredSet[key] = true

		spec, ok := audit.ClassifyHTTPMutation(rt.Method, rt.Path)
		require.True(t, ok, "mutating route %s %s is registered on s.router() but missing from audit.ClassifyHTTPMutation inventory (#1277)", rt.Method, rt.Path)
		assert.NotEmpty(t, spec.Action)
		assert.NotEmpty(t, spec.ResourceType)
	}

	for _, inv := range audit.HTTPMutationActions() {
		key := inv.Method + " " + inv.Route
		assert.True(t, registeredSet[key], "inventory entry %s (%s) has no matching route on s.router()", key, inv.Action)
	}

	// Drive every single mutating route through the router and assert that each
	// call produces a durable ActionAuditRecord with the expected Action and Route.
	for _, rt := range discovered {
		t.Run(rt.Method+"_"+rt.Path, func(t *testing.T) {
			before, err := srv.ActionAuditStore().QueryActionAuditLogs(context.Background(), store.ActionAuditFilter{
				Scope: store.OperatorActionAuditScope(),
				Limit: 1000,
			})
			require.NoError(t, err)

			concretePath := materializeAuditTestPath(rt.Path)
			req := httptest.NewRequest(rt.Method, concretePath, bytes.NewBufferString(`{}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+adminKey)
			req.Header.Set("X-Request-ID", "walk-"+rt.Method+"-"+concretePath)
			req.RemoteAddr = "192.0.2.55:12345"

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			after, err := srv.ActionAuditStore().QueryActionAuditLogs(context.Background(), store.ActionAuditFilter{
				Scope: store.OperatorActionAuditScope(),
				Limit: 1000,
			})
			require.NoError(t, err)
			require.Equal(t, len(before)+1, len(after), "mutating route %s %s must write exactly 1 administrative action audit record", rt.Method, rt.Path)

			latest := after[0]
			expectedSpec, _ := audit.ClassifyHTTPMutation(rt.Method, rt.Path)
			assert.Equal(t, expectedSpec.Action, latest.Action)
			assert.Equal(t, expectedSpec.ResourceType, latest.ResourceType)
			assert.Equal(t, rt.Method, latest.Method)
			assert.Equal(t, rt.Path, latest.Route)
			assert.Equal(t, "192.0.2.55:12345", latest.RemoteAddr)
			assert.Equal(t, "walk-"+rt.Method+"-"+concretePath, latest.RequestID)
			assert.Equal(t, rec.Code, latest.HTTPStatus)
		})
	}
}

func materializeAuditTestPath(pattern string) string {
	replacer := strings.NewReplacer(
		"{id}", "1",
		"{contract_id}", contractA,
		"{key_id}", "100",
	)
	return replacer.Replace(pattern)
}

func TestAdminActionAudit_NeverStoresSecretMaterial(t *testing.T) {
	st := newAuthStubStore()
	// Pre-seed a bootstrap DB API key so POST /apikeys can authenticate with it.
	bootKey, bootPrefix, bootSecret, err := apikey.Generate()
	require.NoError(t, err)
	bootHash, err := apikey.HashSecret(bootSecret)
	require.NoError(t, err)
	_, err = st.CreateAPIKey(context.Background(), store.APIKey{
		Name:    "bootstrap-operator",
		Prefix:  bootPrefix,
		KeyHash: bootHash,
	})
	require.NoError(t, err)

	srv := New(st, nil, testLogger(), "static-operator-secret-do-not-store").
		WithAPIKeyAuth(true)
	router := srv.router()

	// 1. Create a new API key via POST /apikeys (returns plaintext "key" in response).
	reqKey := httptest.NewRequest(http.MethodPost, "/apikeys", strings.NewReader(`{"name":"deploy-bot"}`))
	reqKey.Header.Set("Content-Type", "application/json")
	reqKey.Header.Set("Authorization", "Bearer "+bootKey)
	reqKey.Header.Set("User-Agent", "GitHub-Actions/2.0")
	recKey := httptest.NewRecorder()
	router.ServeHTTP(recKey, reqKey)
	require.Equal(t, http.StatusCreated, recKey.Code, "body: %s", recKey.Body.String())

	var createdKeyResp struct {
		ID     int64  `json:"id"`
		Prefix string `json:"prefix"`
		Key    string `json:"key"`
	}
	require.NoError(t, json.Unmarshal(recKey.Body.Bytes(), &createdKeyResp))
	require.NotEmpty(t, createdKeyResp.Key)
	_, createdSecret, ok := apikey.Parse(createdKeyResp.Key)
	require.True(t, ok)

	// 2. Create a webhook subscription via POST /subscriptions with a sensitive "secret".
	webhookSecret := "whsec_top_secret_signing_material_999"
	subBody := `{"url":"https://example.com/webhook","secret":"` + webhookSecret + `"}`
	reqSub := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(subBody))
	reqSub.Header.Set("Content-Type", "application/json")
	reqSub.Header.Set("Authorization", "Bearer "+bootKey)
	recSub := httptest.NewRecorder()
	router.ServeHTTP(recSub, reqSub)
	require.Equal(t, http.StatusCreated, recSub.Code, "body: %s", recSub.Body.String())

	// 3. Bulk delete events using the static operator secret via X-API-Key.
	reqDel := httptest.NewRequest(http.MethodDelete, "/events?before_ledger=500", nil)
	reqDel.Header.Set("X-API-Key", "static-operator-secret-do-not-store")
	recDel := httptest.NewRecorder()
	router.ServeHTTP(recDel, reqDel)
	require.Equal(t, http.StatusOK, recDel.Code, "body: %s", recDel.Body.String())

	// Query all recorded audit logs and verify that NO secret material appears anywhere.
	logs, err := srv.ActionAuditStore().QueryActionAuditLogs(context.Background(), store.ActionAuditFilter{
		Scope: store.OperatorActionAuditScope(),
	})
	require.NoError(t, err)
	require.Len(t, logs, 3)
	require.NoError(t, store.VerifyActionAuditChain(logs))

	rawJSON, err := json.Marshal(logs)
	require.NoError(t, err)
	serialized := string(rawJSON)

	assert.NotContains(t, serialized, bootSecret, "caller's API key secret must never be stored")
	assert.NotContains(t, serialized, createdSecret, "newly created API key secret must never be stored")
	assert.NotContains(t, serialized, createdKeyResp.Key, "full newly created API key must never be stored")
	assert.NotContains(t, serialized, webhookSecret, "webhook signing secret must never be stored")
	assert.NotContains(t, serialized, "static-operator-secret-do-not-store", "static operator bearer secret must never be stored")

	// Verify the public prefixes and IDs ARE recorded.
	assert.Contains(t, serialized, "sorotrail_"+bootPrefix, "caller's key prefix must be recorded")
	assert.Contains(t, serialized, "sorotrail_"+createdKeyResp.Prefix, "created key prefix must be recorded")

	// Check the POST /apikeys audit record specifically.
	apiKeyAudit := logs[2]
	assert.Equal(t, audit.ActionCreateAPIKey, apiKeyAudit.Action)
	assert.Equal(t, store.ActorTypeAutomation, apiKeyAudit.ActorType, "GitHub-Actions User-Agent must classify as automation")
	assert.Equal(t, "sorotrail_"+bootPrefix, apiKeyAudit.ActorID)
	assert.Equal(t, "sorotrail_"+createdKeyResp.Prefix, apiKeyAudit.Metadata["key_prefix"])
	assert.Equal(t, "deploy-bot", apiKeyAudit.Metadata["name"])
}

func TestAdminActionAudit_FailedAttemptsAndTenantScoping(t *testing.T) {
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	st := newAuthStubStore()
	tenants := newFakeTenants()

	maxWatch := 10
	keyAdmin := tenants.addTenant(t, store.Tenant{ID: 1, Name: "ops-admin", Admin: true, Enabled: true, Wildcard: true})
	keyTenantA := tenants.addTenant(t, store.Tenant{ID: 10, Name: "tenant-a", Enabled: true, MaxWatchedContracts: &maxWatch}, contractA)
	keyTenantB := tenants.addTenant(t, store.Tenant{ID: 20, Name: "tenant-b", Enabled: true, MaxWatchedContracts: &maxWatch}, contractB)

	srv := New(st, nil, testLogger(), "operator-secret").
		WithMultiTenancy(tenants, MultiTenantOptions{})
	router := srv.router()

	// 1. Tenant A succeeds in adding a watched contract to its own watch list.
	reqA := httptest.NewRequest(http.MethodPost, "/tenant/watch", strings.NewReader(`{"contract_id":"`+contractA+`"}`))
	reqA.Header.Set("Content-Type", "application/json")
	reqA.Header.Set("Authorization", "Bearer "+keyTenantA)
	recA := httptest.NewRecorder()
	router.ServeHTTP(recA, reqA)
	require.Equal(t, http.StatusOK, recA.Code, "body: %s", recA.Body.String())

	// 2. Tenant B attempts to revoke an admin tenant key (403 Forbidden) -> failed attempt MUST be recorded!
	reqB := httptest.NewRequest(http.MethodDelete, "/admin/keys/1000", nil)
	reqB.Header.Set("Authorization", "Bearer "+keyTenantB)
	recB := httptest.NewRecorder()
	router.ServeHTTP(recB, reqB)
	require.Equal(t, http.StatusForbidden, recB.Code)

	// 3. Unauthenticated caller attempts to delete a watched contract (401 Unauthorized) -> failed attempt MUST be recorded!
	reqUnauth := httptest.NewRequest(http.MethodDelete, "/watched-contracts/"+contractA, nil)
	recUnauth := httptest.NewRecorder()
	router.ServeHTTP(recUnauth, reqUnauth)
	require.Equal(t, http.StatusUnauthorized, recUnauth.Code)

	// 4. Operator records a programmatic backfill run.
	_, err := srv.RecordBackfillAction(context.Background(), audit.JobRunReport{
		ActorID:    "sorotrail_0123456789abcdef",
		FromLedger: 5000,
		ToLedger:   6000,
		ContractID: contractA,
	})
	require.NoError(t, err)

	// Verify Tenant A sees ONLY Tenant A's record (even if passing OperatorActionAuditScope).
	ctxTenantA := WithPrincipal(context.Background(), Principal{
		Tenant: store.Tenant{ID: 10, Name: "tenant-a", Enabled: true},
	})
	logsA, err := srv.QueryActionAuditLogs(ctxTenantA, store.ActionAuditFilter{
		Scope: store.OperatorActionAuditScope(), // must be overridden by Tenant A's principal
	})
	require.NoError(t, err)
	require.Len(t, logsA, 1)
	assert.Equal(t, audit.ActionAddTenantWatch, logsA[0].Action)
	assert.Equal(t, store.ActionAuditStatusSucceeded, logsA[0].Status)
	require.NotNil(t, logsA[0].TenantID)
	assert.Equal(t, int64(10), *logsA[0].TenantID)

	// Verify Tenant B sees ONLY Tenant B's failed attempt record.
	ctxTenantB := WithPrincipal(context.Background(), Principal{
		Tenant: store.Tenant{ID: 20, Name: "tenant-b", Enabled: true},
	})
	logsB, err := srv.QueryActionAuditLogs(ctxTenantB, store.ActionAuditFilter{})
	require.NoError(t, err)
	require.Len(t, logsB, 1)
	assert.Equal(t, audit.ActionRevokeTenantKey, logsB[0].Action)
	assert.Equal(t, store.ActionAuditStatusFailed, logsB[0].Status)
	assert.Equal(t, http.StatusForbidden, logsB[0].HTTPStatus)
	assert.Equal(t, string(CodeForbidden), logsB[0].ErrorCode)
	require.NotNil(t, logsB[0].TenantID)
	assert.Equal(t, int64(20), *logsB[0].TenantID)

	// Verify Admin / Operator sees ALL 4 records (Tenant A, Tenant B failed attempt, unauthenticated failed attempt, and backfill run).
	ctxAdmin := WithPrincipal(context.Background(), Principal{
		Tenant: store.Tenant{ID: 1, Name: "ops-admin", Admin: true, Enabled: true},
	})
	logsAdmin, err := srv.QueryActionAuditLogs(ctxAdmin, store.ActionAuditFilter{})
	require.NoError(t, err)
	require.Len(t, logsAdmin, 4)
	require.NoError(t, store.VerifyActionAuditChain(logsAdmin))

	// Check the unauthenticated failed attempt record.
	unauthRec := logsAdmin[1]
	assert.Equal(t, audit.ActionRemoveWatchedContract, unauthRec.Action)
	assert.Equal(t, store.ActionAuditStatusFailed, unauthRec.Status)
	assert.Equal(t, http.StatusUnauthorized, unauthRec.HTTPStatus)
	assert.Equal(t, string(CodeUnauthorized), unauthRec.ErrorCode)
	assert.Equal(t, contractA, unauthRec.ResourceID)

	// Verify neither keyTenantA nor keyTenantB nor keyAdmin secrets were stored.
	rawAdminJSON, err := json.Marshal(logsAdmin)
	require.NoError(t, err)
	assert.NotContains(t, string(rawAdminJSON), keyTenantA)
	assert.NotContains(t, string(rawAdminJSON), keyTenantB)
	assert.NotContains(t, string(rawAdminJSON), keyAdmin)
}
