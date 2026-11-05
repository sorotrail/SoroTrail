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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Tests for the tenant self-service watch endpoints (POST /tenant/watch and
// DELETE /tenant/watch/{contract_id}, #1150).

type watchCall struct {
	op          string // "add" | "remove" | "list"
	tenantID    int64
	network     string
	contractID  string
	instanceCap int
}

type recordingWatchTenants struct {
	*fakeTenants
	calls     []watchCall
	addErr    error
	removeErr error
	listErr   error
}

func newRecordingWatchTenants() *recordingWatchTenants {
	return &recordingWatchTenants{fakeTenants: newFakeTenants()}
}

func (r *recordingWatchTenants) AddTenantWatchedContract(ctx context.Context, t store.Tenant, network, contractID string, instanceCap int) error {
	network = defaultNetworkLabel(network)
	r.calls = append(r.calls, watchCall{
		op:          "add",
		tenantID:    t.ID,
		network:     network,
		contractID:  contractID,
		instanceCap: instanceCap,
	})
	if r.addErr != nil {
		return r.addErr
	}
	if r.watched[t.ID] != nil {
		for _, c := range r.watched[t.ID][network] {
			if c == contractID {
				return nil
			}
		}
	}
	if t.MaxWatchedContracts != nil {
		held, _ := r.CountTenantWatchedContracts(ctx, t.ID)
		if held >= *t.MaxWatchedContracts {
			return fmt.Errorf("tenant watch limit (%d) reached: %w", *t.MaxWatchedContracts, store.ErrQuotaExceeded)
		}
	}
	if instanceCap > 0 {
		alreadyWatchedOnInstance := false
		for _, byNet := range r.watched {
			for _, c := range byNet[network] {
				if c == contractID {
					alreadyWatchedOnInstance = true
					break
				}
			}
		}
		if !alreadyWatchedOnInstance {
			instHeld, _ := r.CountInstanceWatchedContracts(ctx)
			if instHeld >= instanceCap {
				return fmt.Errorf("instance watch limit (%d) reached: %w", instanceCap, store.ErrQuotaExceeded)
			}
		}
	}
	return r.fakeTenants.AddTenantWatchedContract(ctx, t, network, contractID, instanceCap)
}

func (r *recordingWatchTenants) RemoveTenantWatchedContract(ctx context.Context, tenantID int64, network, contractID string) error {
	network = defaultNetworkLabel(network)
	r.calls = append(r.calls, watchCall{
		op:         "remove",
		tenantID:   tenantID,
		network:    network,
		contractID: contractID,
	})
	if r.removeErr != nil {
		return r.removeErr
	}
	return r.fakeTenants.RemoveTenantWatchedContract(ctx, tenantID, network, contractID)
}

func (r *recordingWatchTenants) ListTenantWatchedContracts(ctx context.Context, tenantID int64, network string) ([]string, error) {
	network = defaultNetworkLabel(network)
	r.calls = append(r.calls, watchCall{
		op:       "list",
		tenantID: tenantID,
		network:  network,
	})
	if r.listErr != nil {
		return nil, r.listErr
	}
	out, err := r.fakeTenants.ListTenantWatchedContracts(ctx, tenantID, network)
	if out == nil {
		return []string{}, err
	}
	return out, err
}

type watchEndpointFixture struct {
	tenants *recordingWatchTenants
	srv     http.Handler
	keyA    string
	keyB    string
}

func newWatchEndpointFixture(t *testing.T, tenantCap *int, instanceCap int) *watchEndpointFixture {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	rt := newRecordingWatchTenants()
	tenantA := store.Tenant{ID: 1, Name: "tenant-a", Enabled: true, MaxWatchedContracts: tenantCap}
	tenantB := store.Tenant{ID: 2, Name: "tenant-b", Enabled: true, MaxWatchedContracts: tenantCap}
	keyA := rt.addTenant(t, tenantA, contractA)
	keyB := rt.addTenant(t, tenantB, contractB)

	srv := New(&scopedStore{}, &stubRPC{health: rpc.Health{Status: "healthy"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "operator-secret").
		WithMultiTenancy(rt, MultiTenantOptions{MaxWatchedContracts: instanceCap})
	srv.SetHTTPRequestBodyLimit(1 << 20)

	return &watchEndpointFixture{
		tenants: rt,
		srv:     srv.Router(),
		keyA:    keyA,
		keyB:    keyB,
	}
}

func (f *watchEndpointFixture) do(t *testing.T, method, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, path, reader)
	require.NoError(t, err)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func decodeWatchContractIDs(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	var payload struct {
		ContractIDs []string `json:"contract_ids"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload), "body=%s", rec.Body.String())
	return payload.ContractIDs
}

func TestHandleAddOwnWatched(t *testing.T) {
	t.Run("adds valid contract and returns updated list with X-Total-Count", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		rec := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "1", rec.Header().Get("X-Total-Count"))
		assert.Equal(t, []string{contractA}, decodeWatchContractIDs(t, rec))

		rec = f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractB))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "2", rec.Header().Get("X-Total-Count"))
		assert.Equal(t, []string{contractA, contractB}, decodeWatchContractIDs(t, rec))
	})

	t.Run("rejects invalid contract_id with 400 Bad Request", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		rec := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, `{"contract_id":"not-a-valid-strkey"}`)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid contract_id")
		assert.Empty(t, f.tenants.calls, "store must not be called when contract_id fails validation")
	})

	t.Run("rejects unknown fields in JSON body with 400 Bad Request", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		rec := f.do(t, http.MethodPost, "/tenant/watch", f.keyA,
			fmt.Sprintf(`{"contract_id":%q,"tenant_id":2}`, contractA))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Empty(t, f.tenants.calls, "store must not be called when body contains unknown fields")
	})

	t.Run("returns 429 Too Many Requests when tenant watch cap is reached", func(t *testing.T) {
		f := newWatchEndpointFixture(t, intPtr(1), 0)

		rec1 := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		require.Equal(t, http.StatusOK, rec1.Code)

		rec2 := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractB))
		require.Equal(t, http.StatusTooManyRequests, rec2.Code)
		assert.Contains(t, rec2.Body.String(), "watch limit")
	})

	t.Run("returns 429 Too Many Requests when instance watch cap is reached", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 1)

		rec1 := f.do(t, http.MethodPost, "/tenant/watch", f.keyB, fmt.Sprintf(`{"contract_id":%q}`, contractB))
		require.Equal(t, http.StatusOK, rec1.Code)

		rec2 := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		require.Equal(t, http.StatusTooManyRequests, rec2.Code)
		assert.Contains(t, rec2.Body.String(), "instance watch limit")
	})

	t.Run("returns 500 Internal Server Error on store failure", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)
		f.tenants.addErr = errors.New("db connection reset")

		rec := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "adding watched contract failed")
	})
}

func TestHandleRemoveOwnWatched(t *testing.T) {
	t.Run("removes held contract and returns updated list with X-Total-Count", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)
		f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractB))

		rec := f.do(t, http.MethodDelete, "/tenant/watch/"+contractA, f.keyA, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "1", rec.Header().Get("X-Total-Count"))
		assert.Equal(t, []string{contractB}, decodeWatchContractIDs(t, rec))
	})

	t.Run("removing a contract not held is idempotent and returns 200 with X-Total-Count", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)
		f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))

		rec := f.do(t, http.MethodDelete, "/tenant/watch/"+contractC, f.keyA, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "1", rec.Header().Get("X-Total-Count"))
		assert.Equal(t, []string{contractA}, decodeWatchContractIDs(t, rec))
	})

	t.Run("rejects invalid contract_id in path with 400 Bad Request", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		rec := f.do(t, http.MethodDelete, "/tenant/watch/bad-contract-id", f.keyA, "")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid contract_id")
		assert.Empty(t, f.tenants.calls)
	})

	t.Run("returns 500 Internal Server Error on store failure", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)
		f.tenants.removeErr = errors.New("disk I/O error")

		rec := f.do(t, http.MethodDelete, "/tenant/watch/"+contractA, f.keyA, "")
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "removing watched contract failed")
	})
}

func TestTenantWatchRequiresTenantPrincipalAndIsolatesTenants(t *testing.T) {
	t.Run("refuses requests without a tenant principal via requireTenant", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		// Unauthenticated request on multi-tenant router -> 401
		recUnauth := f.do(t, http.MethodPost, "/tenant/watch", "", fmt.Sprintf(`{"contract_id":%q}`, contractA))
		assert.Equal(t, http.StatusUnauthorized, recUnauth.Code)

		// Single-tenant server (Untenanted principal) -> refused by requireTenant with 404
		singleSrv := New(&scopedStore{}, &stubRPC{health: rpc.Health{Status: "healthy"}},
			slog.New(slog.NewTextHandler(io.Discard, nil)), "operator-secret")
		singleSrv.SetHTTPRequestBodyLimit(1 << 20)
		singleRouter := singleSrv.Router()

		reqAdd := httptest.NewRequest(http.MethodPost, "/tenant/watch", strings.NewReader(fmt.Sprintf(`{"contract_id":%q}`, contractA)))
		reqAdd.Header.Set("Authorization", "Bearer operator-secret")
		reqAdd.Header.Set("Content-Type", "application/json")
		recAdd := httptest.NewRecorder()
		singleRouter.ServeHTTP(recAdd, reqAdd)
		assert.Equal(t, http.StatusNotFound, recAdd.Code)
		assert.Contains(t, recAdd.Body.String(), "tenant API is available only when MULTI_TENANT=true")

		reqDel := httptest.NewRequest(http.MethodDelete, "/tenant/watch/"+contractA, nil)
		reqDel.Header.Set("Authorization", "Bearer operator-secret")
		recDel := httptest.NewRecorder()
		singleRouter.ServeHTTP(recDel, reqDel)
		assert.Equal(t, http.StatusNotFound, recDel.Code)
		assert.Contains(t, recDel.Body.String(), "tenant API is available only when MULTI_TENANT=true")
	})

	t.Run("tenant A cannot modify or delete tenant B watch list", func(t *testing.T) {
		f := newWatchEndpointFixture(t, nil, 0)

		// Tenant B watches contractB and contractC.
		require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/tenant/watch", f.keyB, fmt.Sprintf(`{"contract_id":%q}`, contractB)).Code)
		require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/tenant/watch", f.keyB, fmt.Sprintf(`{"contract_id":%q}`, contractC)).Code)

		// Tenant A adds contractA and tries to remove contractB.
		recAddA := f.do(t, http.MethodPost, "/tenant/watch", f.keyA, fmt.Sprintf(`{"contract_id":%q}`, contractA))
		require.Equal(t, http.StatusOK, recAddA.Code)
		assert.Equal(t, []string{contractA}, decodeWatchContractIDs(t, recAddA))

		recDelA := f.do(t, http.MethodDelete, "/tenant/watch/"+contractB, f.keyA, "")
		require.Equal(t, http.StatusOK, recDelA.Code)
		assert.Equal(t, []string{contractA}, decodeWatchContractIDs(t, recDelA))

		// Verify every store call authenticated as A used tenant ID 1, and B's list is untouched.
		recListB := f.do(t, http.MethodGet, "/tenant/watch", f.keyB, "")
		require.Equal(t, http.StatusOK, recListB.Code)
		assert.Equal(t, "2", recListB.Header().Get("X-Total-Count"))
		assert.Equal(t, []string{contractB, contractC}, decodeWatchContractIDs(t, recListB))
	})
}
