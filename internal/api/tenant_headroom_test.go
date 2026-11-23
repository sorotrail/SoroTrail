package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Tests for the watch-list headroom GET /tenant/usage and the admin view of a
// tenant's usage report (#1157). A tenant should see how close it is to its
// own cap before POST /tenant/watch starts returning 429.

type headroomFixture struct {
	tenants *fakeTenants
	srv     http.Handler
	keys    map[string]string
}

// newHeadroomFixture serves the given tenants with an instance-wide cap of
// instanceCap (0 disables it), plus an admin tenant under the name "admin".
func newHeadroomFixture(t *testing.T, instanceCap int, tenants ...store.Tenant) *headroomFixture {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	ft := newFakeTenants()
	f := &headroomFixture{tenants: ft, keys: map[string]string{}}
	for _, tenant := range tenants {
		f.keys[tenant.Name] = ft.addTenant(t, tenant)
	}
	f.keys["admin"] = ft.addTenant(t, store.Tenant{ID: 99, Name: "admin", Enabled: true, Admin: true})

	srv := New(&scopedStore{}, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key").
		WithMultiTenancy(ft, MultiTenantOptions{MaxWatchedContracts: instanceCap})
	srv.SetHTTPRequestBodyLimit(1 << 20)
	f.srv = srv.Router()
	return f
}

func (f *headroomFixture) get(t *testing.T, key, path string) map[string]json.RawMessage {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

func intPtr(n int) *int { return &n }

// watch records n watched contracts for a tenant on the default network.
func (f *headroomFixture) watch(tenantID int64, contracts ...string) {
	if f.tenants.watched[tenantID] == nil {
		f.tenants.watched[tenantID] = map[string][]string{}
	}
	f.tenants.watched[tenantID]["default"] = append(f.tenants.watched[tenantID]["default"], contracts...)
}

func decodeHeadroom(t *testing.T, raw json.RawMessage) (held int, limit *int) {
	t.Helper()
	var h struct {
		Held  int  `json:"held"`
		Limit *int `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(raw, &h))
	return h.Held, h.Limit
}

func TestUsageReportsOwnWatchListHeadroom(t *testing.T) {
	capped := store.Tenant{ID: 1, Name: "capped", Enabled: true, MaxWatchedContracts: intPtr(3)}
	atCap := store.Tenant{ID: 2, Name: "at-cap", Enabled: true, MaxWatchedContracts: intPtr(2)}
	uncapped := store.Tenant{ID: 3, Name: "uncapped", Enabled: true}
	empty := store.Tenant{ID: 4, Name: "empty", Enabled: true, MaxWatchedContracts: intPtr(5)}

	f := newHeadroomFixture(t, 0, capped, atCap, uncapped, empty)
	f.watch(1, contractA)
	f.watch(2, contractA, contractB)
	f.watch(3, contractA, contractB, contractC)

	tests := []struct {
		name      string
		tenant    string
		wantHeld  int
		wantLimit *int
	}{
		{"below its cap", "capped", 1, intPtr(3)},
		{"exactly at its cap", "at-cap", 2, intPtr(2)},
		{"no cap of its own reports a null limit", "uncapped", 3, nil},
		{"an empty list reports zero held", "empty", 0, intPtr(5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := f.get(t, f.keys[tt.tenant], "/tenant/usage")
			held, limit := decodeHeadroom(t, body["watched_contracts"])
			assert.Equal(t, tt.wantHeld, held)
			assert.Equal(t, tt.wantLimit, limit)
		})
	}
}

func TestUsageNullLimitIsAnExplicitNullNotAMissingField(t *testing.T) {
	f := newHeadroomFixture(t, 0, store.Tenant{ID: 1, Name: "uncapped", Enabled: true})

	body := f.get(t, f.keys["uncapped"], "/tenant/usage")
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body["watched_contracts"], &raw))
	limit, present := raw["limit"]
	require.True(t, present, "limit must be present so a client can tell \"no cap\" from \"field not supported\"")
	assert.JSONEq(t, "null", string(limit))
}

func TestUsageCountsOnlyTheCallersWatchList(t *testing.T) {
	a := store.Tenant{ID: 1, Name: "a", Enabled: true, MaxWatchedContracts: intPtr(10)}
	b := store.Tenant{ID: 2, Name: "b", Enabled: true, MaxWatchedContracts: intPtr(10)}
	f := newHeadroomFixture(t, 0, a, b)
	f.watch(1, contractA)
	f.watch(2, contractA, contractB, contractC)

	held, _ := decodeHeadroom(t, f.get(t, f.keys["a"], "/tenant/usage")["watched_contracts"])
	assert.Equal(t, 1, held, "another tenant's claims on the same contracts are not counted")
}

func TestUsageDoesNotRevealInstanceWideNumbersToATenant(t *testing.T) {
	a := store.Tenant{ID: 1, Name: "a", Enabled: true}
	b := store.Tenant{ID: 2, Name: "b", Enabled: true}

	t.Run("instance cap not reached", func(t *testing.T) {
		f := newHeadroomFixture(t, 4, a, b)
		f.watch(1, contractA)
		f.watch(2, contractB)

		body := f.get(t, f.keys["a"], "/tenant/usage")
		assert.JSONEq(t, "false", string(body["instance_watch_list_full"]))
		_, leaked := body["instance_watched_contracts"]
		assert.False(t, leaked, "aggregate usage across tenants is for the operator, not a tenant")
	})

	t.Run("instance cap reached", func(t *testing.T) {
		f := newHeadroomFixture(t, 2, a, b)
		f.watch(1, contractA)
		f.watch(2, contractB)

		body := f.get(t, f.keys["a"], "/tenant/usage")
		assert.JSONEq(t, "true", string(body["instance_watch_list_full"]))
		_, leaked := body["instance_watched_contracts"]
		assert.False(t, leaked)
	})

	t.Run("a contract another tenant already watches does not fill the instance", func(t *testing.T) {
		f := newHeadroomFixture(t, 2, a, b)
		f.watch(1, contractA)
		f.watch(2, contractA, contractB)

		// Two distinct contracts exist across both lists, which is the cap.
		body := f.get(t, f.keys["a"], "/tenant/usage")
		assert.JSONEq(t, "true", string(body["instance_watch_list_full"]))
	})

	t.Run("no instance cap", func(t *testing.T) {
		f := newHeadroomFixture(t, 0, a)
		f.watch(1, contractA)

		body := f.get(t, f.keys["a"], "/tenant/usage")
		_, present := body["instance_watch_list_full"]
		assert.False(t, present, "with no instance cap there is nothing to report")
	})
}

func TestAdminUsageShowsTheSameNumbersAndTheInstanceTotals(t *testing.T) {
	a := store.Tenant{ID: 1, Name: "a", Enabled: true, MaxWatchedContracts: intPtr(3)}
	b := store.Tenant{ID: 2, Name: "b", Enabled: true}
	f := newHeadroomFixture(t, 5, a, b)
	f.watch(1, contractA, contractB)
	f.watch(2, contractB, contractC)

	own := f.get(t, f.keys["a"], "/tenant/usage")
	admin := f.get(t, f.keys["admin"], "/admin/tenants/1/usage")

	ownHeld, ownLimit := decodeHeadroom(t, own["watched_contracts"])
	adminHeld, adminLimit := decodeHeadroom(t, admin["watched_contracts"])
	assert.Equal(t, ownHeld, adminHeld, "the operator sees the tenant's own figures")
	assert.Equal(t, ownLimit, adminLimit)
	assert.Equal(t, 2, adminHeld)
	assert.Equal(t, intPtr(3), adminLimit)

	instHeld, instLimit := decodeHeadroom(t, admin["instance_watched_contracts"])
	assert.Equal(t, 3, instHeld, "three distinct contracts across both lists")
	assert.Equal(t, intPtr(5), instLimit)
	_, boolPresent := admin["instance_watch_list_full"]
	assert.False(t, boolPresent, "the operator gets numbers, not the tenant's one-bit summary")
}

func TestAdminUsageWithNoInstanceCapReportsANullLimit(t *testing.T) {
	f := newHeadroomFixture(t, 0, store.Tenant{ID: 1, Name: "a", Enabled: true})
	f.watch(1, contractA)

	admin := f.get(t, f.keys["admin"], "/admin/tenants/1/usage")
	held, limit := decodeHeadroom(t, admin["instance_watched_contracts"])
	assert.Equal(t, 1, held)
	assert.Nil(t, limit)
}
