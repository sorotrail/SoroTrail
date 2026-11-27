package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Per-tenant retention (#1249) at the API: setting a policy, the
// confirmation a shortening needs, the audit trail it leaves, and the
// read-time horizon a tenant's scope carries.

const retentionTestDefault = 30 * 24 * time.Hour

type retentionFixture struct {
	*crudpFixture
	server *Server
}

func newRetentionFixture(t *testing.T) *retentionFixture {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	base := newFakeTenants()
	crud := newCRUDPTenants(base)
	base.tenants[4] = store.Tenant{ID: 4, Name: "admin", Enabled: true, Admin: true}
	base.grants[4] = nil
	plaintext, prefix, digest, err := GenerateAPIKey()
	require.NoError(t, err)
	base.keys[prefix] = keyRecord{id: 400, tenantID: 4, digest: digest}

	server := New(&scopedStore{}, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").
		WithMultiTenancy(crud, MultiTenantOptions{DefaultRetention: retentionTestDefault})
	server.SetHTTPRequestBodyLimit(1 << 20)
	return &retentionFixture{
		crudpFixture: &crudpFixture{srv: server.Router(), tenants: crud, adminPlaintext: plaintext},
		server:       server,
	}
}

func (f *retentionFixture) patch(t *testing.T, id int64, body string) (int, store.Tenant, string) {
	t.Helper()
	rec := f.do(t, http.MethodPatch, fmt.Sprintf("/admin/tenants/%d", id), body)
	var out store.Tenant
	if rec.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}
	return rec.Code, out, rec.Body.String()
}

func TestTenantRetention_SetLengthenAndClear(t *testing.T) {
	f := newRetentionFixture(t)
	created := f.makeTenant(t, "treasury")
	assert.Nil(t, created.RetentionMaxAgeSeconds, "a new tenant inherits the instance default")

	// 365 days is longer than the 30-day default: no confirmation needed.
	code, got, body := f.patch(t, created.ID, `{"retention_max_age_seconds":31536000}`)
	require.Equal(t, http.StatusOK, code, body)
	require.NotNil(t, got.RetentionMaxAgeSeconds)
	assert.Equal(t, int64(31536000), *got.RetentionMaxAgeSeconds)

	// Clearing back to the 30-day default shortens the history.
	code, _, body = f.patch(t, created.ID, `{"retention_max_age_seconds":0}`)
	assert.Equal(t, http.StatusConflict, code)
	assert.Contains(t, body, "confirm_retention_shortening")

	code, got, body = f.patch(t, created.ID, `{"retention_max_age_seconds":0,"confirm_retention_shortening":true}`)
	require.Equal(t, http.StatusOK, code, body)
	assert.Nil(t, got.RetentionMaxAgeSeconds, "0 means inherit the instance default")
}

func TestTenantRetention_ShorteningNeedsConfirmationAndIsAudited(t *testing.T) {
	f := newRetentionFixture(t)
	created := f.makeTenant(t, "testnet-watcher")

	code, _, body := f.patch(t, created.ID, `{"retention_max_age_seconds":604800}`)
	require.Equal(t, http.StatusConflict, code, "7d is shorter than the 30d default")
	assert.Contains(t, body, "permanently delete")
	_, stored := f.tenants.updatedTenants[created.ID]
	assert.False(t, stored, "a refused shortening must not reach the store")

	code, got, body := f.patch(t, created.ID, `{"retention_max_age_seconds":604800,"confirm_retention_shortening":true}`)
	require.Equal(t, http.StatusOK, code, body)
	require.NotNil(t, got.RetentionMaxAgeSeconds)
	assert.Equal(t, int64(604800), *got.RetentionMaxAgeSeconds)

	recs, err := f.server.ActionAuditStore().QueryActionAuditLogs(context.Background(),
		store.ActionAuditFilter{Scope: store.OperatorActionAuditScope(), Limit: 100})
	require.NoError(t, err)
	var audited *store.ActionAuditRecord
	for i := range recs {
		if recs[i].Status == store.ActionAuditStatusSucceeded && recs[i].Metadata["retention_shortened"] == true {
			audited = &recs[i]
		}
	}
	require.NotNil(t, audited, "the confirmed shortening must leave an audit entry saying so")
	assert.NotEmpty(t, audited.ActorID, "the entry names who shortened it")
	assert.EqualValues(t, 2592000, audited.Metadata["retention_max_age_seconds_before"])
	assert.EqualValues(t, 604800, audited.Metadata["retention_max_age_seconds_after"])
}

func TestTenantRetention_RejectsOutOfRangeValues(t *testing.T) {
	f := newRetentionFixture(t)
	created := f.makeTenant(t, "acme")

	code, _, body := f.patch(t, created.ID, `{"retention_max_age_seconds":-5}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "retention_max_age_seconds")

	rec := f.do(t, http.MethodPost, "/admin/tenants", `{"name":"forever","retention_max_age_seconds":99999999999}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTenantRetention_ScopeCarriesTheTenantHorizon(t *testing.T) {
	f := newRetentionFixture(t)
	ctx := context.Background()

	inherit := store.Tenant{ID: 7, Enabled: true}
	sc, err := f.server.tenantScope(ctx, inherit)
	require.NoError(t, err)
	assert.Equal(t, retentionTestDefault, sc.Retention(),
		"a tenant without a policy reads only the default horizon, even if rows are kept longer for others")

	week := int64(7 * 24 * 60 * 60)
	short := store.Tenant{ID: 8, Enabled: true, RetentionMaxAgeSeconds: &week}
	sc, err = f.server.tenantScope(ctx, short)
	require.NoError(t, err)
	assert.Equal(t, 7*24*time.Hour, sc.Retention())

	wild := store.Tenant{ID: 9, Enabled: true, Wildcard: true, RetentionMaxAgeSeconds: &week}
	sc, err = f.server.tenantScope(ctx, wild)
	require.NoError(t, err)
	assert.True(t, sc.IsWildcard())
	assert.Equal(t, 7*24*time.Hour, sc.Retention(), "wildcard reads are bounded by the horizon too")
}
