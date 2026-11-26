package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestSetActionAuditStore_happyPath(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	require.NotNil(t, s)

	as := store.NewMemoryActionAuditStore()
	s.SetActionAuditStore(as)

	// After setting, ActionAuditStore() should return the store we just set
	got := s.ActionAuditStore()
	require.NotNil(t, got)
	assert.Equal(t, as, got)
}

func TestSetActionAuditStore_nilAccepted(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	require.NotNil(t, s)

	// Setting nil should not panic; ActionAuditStore() falls through to
	// the store or its fallback.
	s.SetActionAuditStore(nil)

	got := s.ActionAuditStore()
	// Should not panic; may return the default fallback store.
	assert.NotNil(t, got, "ActionAuditStore() must not panic and must return a store")
}

func TestSetActionAuditStore_verifiedStore(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	require.NotNil(t, s)

	// Set a store that implements ActionAuditStore via the embedded store.
	// The stubStore does not implement it, so ActionAuditStore() will use
	// the fallback, but the set value should still be remembered.
	as := store.NewMemoryActionAuditStore()
	s.SetActionAuditStore(as)

	got := s.ActionAuditStore()
	assert.Equal(t, as, got, "SetActionAuditStore should store and return the given store")
}

func TestSetMetricsEnabled_DisablesMetricsEndpoint(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	s.SetMetricsEnabled(false)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestSetMetricsEnabled_EnabledByDefault(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)

	require.NotEqual(t, http.StatusServiceUnavailable, rec.Code)
}

func TestUsage_ReturnsRecorderWhenMultiTenant(t *testing.T) {
	ts := newFakeTenants()
	s := newMultiTenantAuthServer(t, ts)
	require.NotNil(t, s.Usage())
}

func TestEffectiveRequestBodyLimit(t *testing.T) {
	tests := []struct {
		name           string
		defaultLimit   int64
		overrides      map[string]int64
		method         string
		path           string
		wantLimit      int64
		nilRequestWant int64
	}{
		{
			name:           "falls back to default limit when no override matches",
			defaultLimit:   1024,
			overrides:      map[string]int64{"POST /watched-contracts": 4096},
			method:         http.MethodPost,
			path:           "/subscriptions",
			wantLimit:      1024,
			nilRequestWant: 1024,
		},
		{
			name:         "uses per-endpoint override for matching method and path",
			defaultLimit: 1024,
			overrides:    map[string]int64{"POST /watched-contracts": 4096},
			method:       http.MethodPost,
			path:         "/watched-contracts",
			wantLimit:    4096,
		},
		{
			name:         "does not apply override when method differs",
			defaultLimit: 1024,
			overrides:    map[string]int64{"POST /watched-contracts": 4096},
			method:       http.MethodGet,
			path:         "/watched-contracts",
			wantLimit:    1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{httpRequestBodyLimit: tt.defaultLimit}
			s.SetEndpointRequestBodyLimits(tt.overrides)

			if tt.nilRequestWant != 0 || tt.name == "falls back to default limit when no override matches" {
				require.Equal(t, tt.defaultLimit, s.EffectiveRequestBodyLimit(nil))
			}

			req := httptest.NewRequest(tt.method, tt.path, nil)
			require.Equal(t, tt.wantLimit, s.EffectiveRequestBodyLimit(req))
		})
	}
}
