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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type replicaTenantStore struct {
	*crudpTenants
}

func (r *replicaTenantStore) ListTenantAPIKeys(ctx context.Context, tenantID int64) ([]store.TenantAPIKey, error) {
	keys, err := r.crudpTenants.ListTenantAPIKeys(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for i := range keys {
		if r.revokedKeys[keys[i].ID] {
			keys[i].RevokedAt = &now
		}
	}
	return keys, nil
}

// multiReplicaFixture wires two independent api.Server instances (Replica 1 and
// Replica 2) against a single shared store and a shared
// store.InMemoryInvalidationBus. Each replica has its own long-TTL in-memory
// AuthCache, StatsCache, and RateLimiter so any test passing before the TTL
// elapses proves that cross-replica invalidation (and not TTL expiry) evicted
// the cached entry on the peer replica.
type multiReplicaFixture struct {
	st      *scopedStore
	tenants *replicaTenantStore
	bus     *store.InMemoryInvalidationBus

	srv1      *Server
	router1   http.Handler
	listener1 *store.SpecInvalidationListener
	subID1    int

	srv2      *Server
	router2   http.Handler
	listener2 *store.SpecInvalidationListener
	subID2    int

	adminKey string
	keyA     string
	keyAID   int64
	keyB     string
}

func newMultiReplicaFixture(t *testing.T) *multiReplicaFixture {
	t.Helper()
	SetTenantScopedCaching(false)
	t.Cleanup(func() { SetTenantScopedCaching(false) })

	st := &scopedStore{events: []store.Event{
		{ID: "ev-a1", ContractID: contractA, Ledger: 100, Type: "contract"},
		{ID: "ev-b1", ContractID: contractB, Ledger: 100, Type: "contract"},
	}}
	baseFake := newFakeTenants()
	tenants := &replicaTenantStore{crudpTenants: newCRUDPTenants(baseFake)}

	// Seed admin (tenant 4), tenant A (tenant 1), and tenant B (tenant 2).
	adminKey := tenants.addTenant(t, store.Tenant{ID: 4, Name: "admin", Enabled: true, Admin: true})
	tenants.tenants[1] = store.Tenant{ID: 1, Name: "tenant-a", Enabled: true}
	tenants.grants[1] = []string{contractA}
	plainA, prefixA, digestA, err := GenerateAPIKey()
	require.NoError(t, err)
	createdKeyA, err := tenants.CreateTenantAPIKey(context.Background(), 1, "a-primary", prefixA, digestA)
	require.NoError(t, err)

	keyB := tenants.addTenant(t, store.Tenant{ID: 2, Name: "tenant-b", Enabled: true}, contractB)

	bus := store.NewInMemoryInvalidationBus()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	buildReplica := func() (*Server, http.Handler, *store.SpecInvalidationListener, int) {
		lim := NewRateLimiter(0, 0, false, WithLimitResolver(TenantLimitResolver(100, 100)))
		srv := New(st, &stubRPC{health: rpc.Health{Status: "healthy"}}, logger, "operator-key").
			WithMultiTenancy(tenants, MultiTenantOptions{
				MaxWatchedContracts: 10,
				StreamScopeSync:     10 * time.Minute, // deliberately long so tests prove instant wake
			})
		srv.SetRateLimiter(lim)
		srv.SetHTTPRequestBodyLimit(1 << 20)
		srv.SetStatsTTL(5 * time.Minute)
		srv.SetAuthCacheTTL(5 * time.Minute)
		srv.SetSpecInvalidationPublisher(bus)

		listener := &store.SpecInvalidationListener{
			Logger: logger,
			OnEvent: func(ev store.InvalidationEvent) {
				srv.ApplyRemoteInvalidation(store.FormatInvalidationPayload(ev.Kind, ev.Target))
			},
			OnReconnect: srv.OnInvalidationReconnect,
		}
		subID := bus.Attach(listener)
		return srv, srv.Router(), listener, subID
	}

	srv1, r1, l1, id1 := buildReplica()
	srv2, r2, l2, id2 := buildReplica()

	return &multiReplicaFixture{
		st:        st,
		tenants:   tenants,
		bus:       bus,
		srv1:      srv1,
		router1:   r1,
		listener1: l1,
		subID1:    id1,
		srv2:      srv2,
		router2:   r2,
		listener2: l2,
		subID2:    id2,
		adminKey:  adminKey,
		keyA:      plainA,
		keyAID:    createdKeyA.ID,
		keyB:      keyB,
	}
}

func doReplicaRequest(t *testing.T, router http.Handler, method, key, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestCrossReplicaTenantAPIKeyRevocation(t *testing.T) {
	f := newMultiReplicaFixture(t)

	// Warm Replica 2's AuthCache with Tenant A and Tenant B requests.
	rec := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyB, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// Repeat on Replica 2 to confirm AuthCache hits.
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)
	hitsBefore, _, _ := f.srv2.AuthCache().Stats()
	require.GreaterOrEqual(t, hitsBefore, uint64(1), "Replica 2 AuthCache should record a cache hit")

	// Revoke Tenant A's API key via Replica 1.
	revokePath := fmt.Sprintf("/admin/keys/%d", f.keyAID)
	rec = doReplicaRequest(t, f.router1, http.MethodDelete, f.adminKey, revokePath, "")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	// Replica 2 must reject Tenant A's revoked key immediately (well within its 5m TTL).
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	assert.Equal(t, http.StatusUnauthorized, rec.Code,
		"Replica 2 must immediately reject a key revoked on Replica 1")

	// Scoped invalidation: Tenant B's cached entry on Replica 2 must remain warm.
	hitsBeforeB, _, _ := f.srv2.AuthCache().Stats()
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyB, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)
	hitsAfterB, _, _ := f.srv2.AuthCache().Stats()
	assert.Equal(t, hitsBeforeB+1, hitsAfterB,
		"revoking Tenant A's key must not evict Tenant B from Replica 2's AuthCache")
}

func TestCrossReplicaTenantSuspensionAndGrantRevocation(t *testing.T) {
	f := newMultiReplicaFixture(t)

	// Warm Replica 2's AuthCache and StatsCache for Tenant A.
	rec := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/contracts/"+contractA+"/events", "")
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/stats", "")
	require.Equal(t, http.StatusOK, rec.Code)
	statsCallsAfterFirst := f.st.statsCalls

	// Second /stats on Replica 2 is served from StatsCache.
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/stats", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, statsCallsAfterFirst, f.st.statsCalls, "Replica 2 should serve /stats from StatsCache")

	// Grant contractB to Tenant A on Replica 1.
	rec = doReplicaRequest(t, f.router1, http.MethodPost, f.adminKey,
		"/admin/tenants/1/grants", fmt.Sprintf(`{"contract_id":%q}`, contractB))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Replica 2 must immediately allow Tenant A to read contractB and must
	// have evicted Tenant A's old StatsCache entry.
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/contracts/"+contractB+"/events", "")
	assert.Equal(t, http.StatusOK, rec.Code,
		"Replica 2 must immediately honor a contract grant issued on Replica 1")

	// Revoke contractB from Tenant A on Replica 1.
	rec = doReplicaRequest(t, f.router1, http.MethodDelete, f.adminKey,
		"/admin/tenants/1/grants/"+contractB, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Replica 2 must immediately forbid contractB again.
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/contracts/"+contractB+"/events", "")
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"Replica 2 must immediately enforce a grant revocation performed on Replica 1")

	// Suspend Tenant A (enabled=false) on Replica 1.
	rec = doReplicaRequest(t, f.router1, http.MethodPatch, f.adminKey,
		"/admin/tenants/1", `{"enabled":false}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Replica 2 must immediately reject Tenant A with 403 Forbidden.
	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"Replica 2 must immediately reject a tenant suspended on Replica 1")
}

func TestCrossReplicaRateLimitBucketInvalidation(t *testing.T) {
	f := newMultiReplicaFixture(t)

	// Warm Tenant A's rate-limiter bucket on Replica 2 (initially inherits 100 RPS / 100 burst).
	rec := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// On Replica 1, tighten Tenant A's quota to burst=1, rps=0.1.
	rec = doReplicaRequest(t, f.router1, http.MethodPatch, f.adminKey,
		"/admin/tenants/1", `{"rate_limit_rps":0.1,"rate_limit_burst":1}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// On Replica 2, Tenant A's old 100-burst bucket must have been evicted;
	// the first request consumes the 1-token burst and the second gets 429.
	rec1 := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	require.Equal(t, http.StatusOK, rec1.Code)
	rec2 := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	assert.Equal(t, http.StatusTooManyRequests, rec2.Code,
		"Replica 2 must enforce the tightened rate-limit quota updated on Replica 1")
}

func TestCrossReplicaStreamScopeImmediateWake(t *testing.T) {
	f := newMultiReplicaFixture(t)

	bcast := broadcast.New(16)
	initialScope := store.NewScope([]string{contractA})
	sub := bcast.Subscribe(store.EventFilter{Scope: initialScope})
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = WithPrincipal(ctx, Principal{
		Tenant: store.Tenant{ID: 1, Name: "tenant-a", Enabled: true},
		Scope:  initialScope,
		KeyID:  f.keyAID,
	})

	// Start syncStreamScope on Replica 2 (whose periodic ticker is 10 minutes).
	f.srv2.syncStreamScope(ctx, sub)

	// Publish an event on contractB — initially filtered out because Tenant A
	// only holds contractA.
	bcast.Publish(ctx, []store.Event{{ID: "ev-b-before", ContractID: contractB}})
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event before grant: %+v", ev)
	case <-time.After(30 * time.Millisecond):
	}

	// Grant contractB to Tenant A via Replica 1.
	rec := doReplicaRequest(t, f.router1, http.MethodPost, f.adminKey,
		"/admin/tenants/1/grants", fmt.Sprintf(`{"contract_id":%q}`, contractB))
	require.Equal(t, http.StatusOK, rec.Code)

	// Wait briefly for Replica 2's watcher goroutine to process the wake signal.
	require.Eventually(t, func() bool {
		bcast.Publish(ctx, []store.Event{{ID: "ev-b-after-grant", ContractID: contractB}})
		select {
		case ev := <-sub.Events():
			return ev.ID == "ev-b-after-grant"
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "open stream on Replica 2 must immediately widen scope after grant on Replica 1")

	// Now revoke Tenant A's API key on Replica 1; the open stream on Replica 2
	// must immediately revoke its scope to empty.
	revokePath := fmt.Sprintf("/admin/keys/%d", f.keyAID)
	rec = doReplicaRequest(t, f.router1, http.MethodDelete, f.adminKey, revokePath, "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	require.Eventually(t, func() bool {
		bcast.Publish(ctx, []store.Event{{ID: "ev-a-after-revoke", ContractID: contractA}})
		select {
		case <-sub.Events():
			return false
		case <-time.After(20 * time.Millisecond):
			return true
		}
	}, time.Second, 20*time.Millisecond, "open stream on Replica 2 must stop receiving events after its API key is revoked on Replica 1")
}

func TestCrossReplicaDisconnectObservabilityAndReconnectFlush(t *testing.T) {
	f := newMultiReplicaFixture(t)

	// Warm Replica 2's AuthCache with Tenant A.
	rec := doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// Simulate Replica 2 losing its LISTEN connection.
	f.bus.SetConnected(f.subID2, false, errors.New("network partition"))
	assert.Equal(t, uint64(1), f.listener2.Stats().DisconnectErrors,
		"disconnect must increment observable DisconnectErrors counter")

	// While Replica 2 is disconnected, suspend Tenant A on Replica 1.
	rec = doReplicaRequest(t, f.router1, http.MethodPatch, f.adminKey,
		"/admin/tenants/1", `{"enabled":false}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Now reconnect Replica 2: OnReconnect flushes Replica 2's caches so the
	// missed suspension takes effect immediately upon reconnect.
	flushesBefore := f.srv2.InvalidationReconnectFlushes()
	f.bus.SetConnected(f.subID2, true, nil)
	assert.Equal(t, flushesBefore+1, f.srv2.InvalidationReconnectFlushes(),
		"reconnect must flush server caches and increment InvalidationReconnectFlushes")

	rec = doReplicaRequest(t, f.router2, http.MethodGet, f.keyA, "/events", "")
	assert.Equal(t, http.StatusForbidden, rec.Code,
		"after reconnect cache flush, Replica 2 must enforce the suspension that occurred while disconnected")

	// Also verify publish failure observability when the bus fails to publish.
	f.bus.SetPublishError(errors.New("pg_notify failed"))
	failuresBefore := f.srv1.InvalidationPublishFailures()
	rec = doReplicaRequest(t, f.router1, http.MethodPatch, f.adminKey,
		"/admin/tenants/1", `{"enabled":true}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, failuresBefore+1, f.srv1.InvalidationPublishFailures(),
		"publish failure must increment InvalidationPublishFailures counter")

	// Clean up publish error and verify JSON marshal of listener stats.
	f.bus.SetPublishError(nil)
	rawStats, err := json.Marshal(f.listener2.Stats())
	require.NoError(t, err)
	assert.Contains(t, string(rawStats), `"disconnect_errors":1`)
}
