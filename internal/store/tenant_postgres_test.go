//go:build integration

package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for the multi-tenancy persistence layer (#48). Like the
// rest of this package's Postgres tests they are skipped unless
// TEST_DATABASE_URL is set.
//
// The API-level isolation tests in internal/api prove that a scope reaches
// the store on every endpoint; these prove the store actually honors one.

// contractC is a third contract for union/quota cases; contractA and
// contractB come from postgres_test.go.
const contractC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"

// testTenantStore truncates the tenancy tables on top of what testStore
// already clears, leaving the seeded "default" tenant in place because the
// upgrade path depends on it existing.
func testTenantStore(t *testing.T) *Postgres {
	t.Helper()
	p := testStore(t)
	_, err := p.pool.Exec(context.Background(),
		`TRUNCATE tenant_usage, tenant_api_keys, tenant_watched_contracts,
		          tenant_contract_grants, tenants RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	// Re-seed what migration 0016 inserts, since the truncate above removed it.
	_, err = p.pool.Exec(context.Background(),
		`INSERT INTO tenants (name, wildcard, is_admin, enabled)
		 VALUES ('default', true, true, true)`)
	require.NoError(t, err)
	return p
}

func mustTenant(t *testing.T, p *Postgres, name string, grants ...string) Tenant {
	t.Helper()
	ctx := context.Background()
	tenant, err := p.CreateTenant(ctx, Tenant{Name: name, Enabled: true})
	require.NoError(t, err)
	for _, g := range grants {
		require.NoError(t, p.GrantContract(ctx, tenant.ID, g))
	}
	return tenant
}

// The upgrade path: a database migrated from a pre-#48 schema must come out
// with a wildcard admin tenant, or an operator enabling multi-tenancy has no
// way into their own instance.
func TestMigration_SeedsDefaultWildcardAdminTenant(t *testing.T) {
	p := testStore(t)

	def, err := p.GetTenantByName(context.Background(), "default")
	require.NoError(t, err)

	assert.True(t, def.Wildcard, "the default tenant must read everything, as pre-#48 callers did")
	assert.True(t, def.Admin, "the default tenant must be able to administer the instance")
	assert.True(t, def.Enabled)
}

// QueryEvents must AND the scope into the statement, whatever else the
// caller asked for.
func TestQueryEvents_EnforcesScope(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	_, err := p.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
		testEvent(eventID(3), 102, contractB),
	})
	require.NoError(t, err)

	t.Run("granted contracts only", func(t *testing.T) {
		got, _, err := p.QueryEvents(ctx, EventFilter{Scope: NewScope([]string{contractA})})
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, e := range got {
			assert.Equal(t, contractA, e.ContractID)
		}
	})

	t.Run("an empty scope returns nothing", func(t *testing.T) {
		got, next, err := p.QueryEvents(ctx, EventFilter{})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("wildcard sees everything", func(t *testing.T) {
		got, _, err := p.QueryEvents(ctx, EventFilter{Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("naming an ungranted contract yields nothing", func(t *testing.T) {
		// Defense in depth: the API refuses this with 403 before it reaches
		// here, but the store must not serve it even if that check is
		// removed.
		got, _, err := p.QueryEvents(ctx, EventFilter{
			ContractID: contractB,
			Scope:      NewScope([]string{contractA}),
		})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("pagination cannot walk out of the scope", func(t *testing.T) {
		var all []Event
		cursor := ""
		for range 10 {
			page, next, err := p.QueryEvents(ctx, EventFilter{
				Scope:  NewScope([]string{contractA}),
				Limit:  1,
				Cursor: cursor,
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		require.Len(t, all, 2)
		for _, e := range all {
			assert.Equal(t, contractA, e.ContractID)
		}
	})
}

func TestGetEventAndExists_EnforceScope(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	_, err := p.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	})
	require.NoError(t, err)

	scopeA := NewScope([]string{contractA})

	got, err := p.GetEvent(ctx, eventID(1), scopeA)
	require.NoError(t, err)
	assert.Equal(t, contractA, got.ContractID)

	// Another tenant's event is reported as absent, not forbidden: event IDs
	// are guessable, so a distinguishable error would enumerate them.
	_, err = p.GetEvent(ctx, eventID(2), scopeA)
	assert.ErrorIs(t, err, ErrNotFound)

	_, err = p.GetEvent(ctx, eventID(1), Scope{})
	assert.ErrorIs(t, err, ErrNotFound, "an empty scope must find nothing")

	exists, err := p.EventExists(ctx, eventID(1), scopeA)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = p.EventExists(ctx, eventID(2), scopeA)
	require.NoError(t, err)
	assert.False(t, exists, "existence must not be probeable across the boundary")

	exists, err = p.EventExists(ctx, eventID(1), Scope{})
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestStats_EnforcesScope(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	_, err := p.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
		testEvent(eventID(3), 102, contractB),
	})
	require.NoError(t, err)

	scoped, err := p.Stats(ctx, NewScope([]string{contractA}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), scoped.TotalEvents)
	assert.Equal(t, int64(1), scoped.ContractCount)

	all, err := p.Stats(ctx, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, int64(3), all.TotalEvents)
	assert.Equal(t, int64(2), all.ContractCount)

	// A caller with no grants learns nothing about volume, but still sees
	// the instance's own progress.
	none, err := p.Stats(ctx, Scope{})
	require.NoError(t, err)
	assert.Zero(t, none.TotalEvents)
	assert.Zero(t, none.ContractCount)
}

func TestScopeForTenant(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	t.Run("grants become the scope", func(t *testing.T) {
		tenant := mustTenant(t, p, "granted", contractA, contractB)
		sc, err := p.ScopeForTenant(ctx, tenant)
		require.NoError(t, err)
		assert.Equal(t, []string{contractA, contractB}, sc.Contracts())
		assert.False(t, sc.IsWildcard())
	})

	t.Run("a tenant with no grants is denied everything", func(t *testing.T) {
		tenant := mustTenant(t, p, "ungranted")
		sc, err := p.ScopeForTenant(ctx, tenant)
		require.NoError(t, err)
		assert.True(t, sc.DeniesAll())
	})

	t.Run("a wildcard tenant ignores its grants", func(t *testing.T) {
		tenant, err := p.CreateTenant(ctx, Tenant{Name: "wild", Enabled: true, Wildcard: true})
		require.NoError(t, err)
		sc, err := p.ScopeForTenant(ctx, tenant)
		require.NoError(t, err)
		assert.True(t, sc.IsWildcard())
	})

	t.Run("revoking narrows the scope", func(t *testing.T) {
		tenant := mustTenant(t, p, "revoked", contractA, contractB)
		require.NoError(t, p.RevokeContract(ctx, tenant.ID, contractB))

		sc, err := p.ScopeForTenant(ctx, tenant)
		require.NoError(t, err)
		assert.Equal(t, []string{contractA}, sc.Contracts())
		assert.False(t, sc.Allows(contractB))
	})

	t.Run("revoking a grant never held is not an error", func(t *testing.T) {
		tenant := mustTenant(t, p, "norevoke")
		assert.NoError(t, p.RevokeContract(ctx, tenant.ID, contractA))
	})
}

// Ingestion must follow the union of every tenant's watch list plus the
// operator's own, and one tenant dropping a contract must not stop
// ingestion for another that still wants it.
func TestWatchedContracts_UnionAndRefcounting(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	one := mustTenant(t, p, "one")
	two := mustTenant(t, p, "two")

	// The operator's global list still counts.
	require.NoError(t, p.AddWatchedContract(ctx, WatchedContract{ContractID: contractC}))

	require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractA, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractA, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractB, 0))

	assert.Equal(t, []string{contractA, contractB, contractC}, watchedContractIDs(t, p),
		"a contract two tenants both want appears once")

	// One tenant drops the shared contract; the other still needs it.
	require.NoError(t, p.RemoveTenantWatchedContract(ctx, one.ID, "", contractA))
	assert.Contains(t, watchedContractIDs(t, p), contractA,
		"a contract another tenant still watches must keep being ingested")

	// The last tenant drops it: now it leaves the union.
	require.NoError(t, p.RemoveTenantWatchedContract(ctx, two.ID, "", contractA))
	watched := watchedContractIDs(t, p)
	assert.NotContains(t, watched, contractA)
	assert.Contains(t, watched, contractB)
	assert.Contains(t, watched, contractC, "the operator's own list is unaffected")
}

// Deleting a tenant releases its watch-list claims but leaves contracts
// other tenants still want.
func TestDeleteTenant_ReleasesWatchClaims(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	one := mustTenant(t, p, "one")
	two := mustTenant(t, p, "two")
	require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractA, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractB, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractB, 0))

	require.NoError(t, p.DeleteTenant(ctx, one.ID))

	watched := watchedContractIDs(t, p)
	assert.NotContains(t, watched, contractA, "nobody wants A any more")
	assert.Contains(t, watched, contractB, "B is still wanted by the surviving tenant")

	_, err := p.GetTenant(ctx, one.ID)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestWatchedContracts_Quotas(t *testing.T) {
	ctx := context.Background()

	t.Run("the tenant's own cap is enforced", func(t *testing.T) {
		p := testTenantStore(t)
		cap1 := 1
		tenant, err := p.CreateTenant(ctx, Tenant{
			Name: "capped", Enabled: true, MaxWatchedContracts: &cap1,
		})
		require.NoError(t, err)

		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))
		err = p.AddTenantWatchedContract(ctx, tenant, "", contractB, 0)
		assert.ErrorIs(t, err, ErrQuotaExceeded)

		// Re-adding something already held stays idempotent at the cap.
		assert.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))
	})

	t.Run("the instance cap bounds the union", func(t *testing.T) {
		p := testTenantStore(t)
		one := mustTenant(t, p, "one")
		two := mustTenant(t, p, "two")

		require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractA, 2))
		require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractB, 2))

		err := p.AddTenantWatchedContract(ctx, two, "", contractC, 2)
		assert.ErrorIs(t, err, ErrQuotaExceeded, "a third distinct contract exceeds the cap")

		// A contract already in the union costs nothing to join, because
		// the cap bounds ingestion cost and that contract is already polled.
		assert.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractA, 2))
	})

	t.Run("no cap means unlimited", func(t *testing.T) {
		p := testTenantStore(t)
		tenant := mustTenant(t, p, "uncapped")
		for _, c := range []string{contractA, contractB, contractC} {
			require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", c, 0))
		}
		watched, err := p.ListTenantWatchedContracts(ctx, tenant.ID, "")
		require.NoError(t, err)
		assert.Len(t, watched, 3)
	})
}

// Watching is a resource request; reading is an authorization decision.
// Adding a contract to the watch list must not grant read access to it, or
// any tenant could grant itself anything on the network.
func TestWatchingDoesNotGrantAccess(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	tenant := mustTenant(t, p, "watcher")
	require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))

	sc, err := p.ScopeForTenant(ctx, tenant)
	require.NoError(t, err)
	assert.True(t, sc.DeniesAll(), "watching a contract must not grant reading it")

	_, err = p.UpsertEvents(ctx, []Event{testEvent(eventID(1), 100, contractA)})
	require.NoError(t, err)

	got, _, err := p.QueryEvents(ctx, EventFilter{Scope: sc})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestWatchedContracts_MultiNetwork(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	tenant := mustTenant(t, p, "multi-net")
	require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "mainnet", contractA, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "testnet", contractA, 0))
	require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "testnet", contractB, 0))

	mainnetWatched, err := p.ListTenantWatchedContracts(ctx, tenant.ID, "mainnet")
	require.NoError(t, err)
	assert.Equal(t, []string{contractA}, mainnetWatched)

	testnetWatched, err := p.ListTenantWatchedContracts(ctx, tenant.ID, "testnet")
	require.NoError(t, err)
	assert.Equal(t, []string{contractA, contractB}, testnetWatched)

	require.NoError(t, p.RemoveTenantWatchedContract(ctx, tenant.ID, "testnet", contractA))
	mainnetWatched, err = p.ListTenantWatchedContracts(ctx, tenant.ID, "mainnet")
	require.NoError(t, err)
	assert.Equal(t, []string{contractA}, mainnetWatched, "mainnet watch is preserved when testnet watch is removed")
}

func TestTenantAPIKeys(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()
	tenant := mustTenant(t, p, "keyed")

	digest := []byte("0123456789abcdef0123456789abcdef")
	key, err := p.CreateTenantAPIKey(ctx, tenant.ID, "primary", "PREFIX0000000001", digest)
	require.NoError(t, err)
	assert.Equal(t, tenant.ID, key.TenantID)
	assert.Nil(t, key.RevokedAt)

	t.Run("lookup returns the digest and tenant", func(t *testing.T) {
		got, gotDigest, gotTenant, err := p.LookupTenantAPIKey(ctx, "PREFIX0000000001")
		require.NoError(t, err)
		assert.Equal(t, key.ID, got.ID)
		assert.Equal(t, digest, gotDigest)
		assert.Equal(t, tenant.ID, gotTenant.ID)
		assert.Equal(t, tenant.Name, gotTenant.Name)
	})

	t.Run("an unknown prefix is not found", func(t *testing.T) {
		_, _, _, err := p.LookupTenantAPIKey(ctx, "NOSUCHPREFIX0000")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("a revoked key stops resolving", func(t *testing.T) {
		require.NoError(t, p.RevokeTenantAPIKey(ctx, key.ID))
		_, _, _, err := p.LookupTenantAPIKey(ctx, "PREFIX0000000001")
		assert.ErrorIs(t, err, ErrNotFound,
			"revocation must be enforced by the lookup, not by the caller remembering to check")

		// The row survives for audit.
		keys, err := p.ListTenantAPIKeys(ctx, tenant.ID)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.NotNil(t, keys[0].RevokedAt)
	})

	t.Run("revoking twice reports not found", func(t *testing.T) {
		assert.ErrorIs(t, p.RevokeTenantAPIKey(ctx, key.ID), ErrNotFound)
	})

	t.Run("deleting a tenant cascades its keys", func(t *testing.T) {
		victim := mustTenant(t, p, "doomed")
		_, err := p.CreateTenantAPIKey(ctx, victim.ID, "k", "PREFIX0000000002", digest)
		require.NoError(t, err)

		require.NoError(t, p.DeleteTenant(ctx, victim.ID))
		_, _, _, err = p.LookupTenantAPIKey(ctx, "PREFIX0000000002")
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

// The bootstrap path runs on every startup, so it must be idempotent.
func TestCreateTenantAPIKeyIfAbsent_IsIdempotent(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()
	tenant := mustTenant(t, p, "bootstrapped")

	first := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, p.CreateTenantAPIKeyIfAbsent(ctx, tenant.ID, "bootstrap", "BOOTPREFIX000001", first))
	require.NoError(t, p.CreateTenantAPIKeyIfAbsent(ctx, tenant.ID, "bootstrap", "BOOTPREFIX000001", first))

	keys, err := p.ListTenantAPIKeys(ctx, tenant.ID)
	require.NoError(t, err)
	assert.Len(t, keys, 1, "a restart must not accumulate duplicate bootstrap keys")

	t.Run("rotating the configured value takes effect", func(t *testing.T) {
		second := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		require.NoError(t, p.CreateTenantAPIKeyIfAbsent(ctx, tenant.ID, "bootstrap", "BOOTPREFIX000001", second))

		_, digest, _, err := p.LookupTenantAPIKey(ctx, "BOOTPREFIX000001")
		require.NoError(t, err)
		assert.Equal(t, second, digest)
	})
}

func TestTenantCRUD(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	t.Run("duplicate names are rejected", func(t *testing.T) {
		_, err := p.CreateTenant(ctx, Tenant{Name: "dup", Enabled: true})
		require.NoError(t, err)
		_, err = p.CreateTenant(ctx, Tenant{Name: "dup", Enabled: true})
		assert.ErrorIs(t, err, ErrDuplicate)
	})

	t.Run("quota overrides round-trip", func(t *testing.T) {
		rps, burst, watched := 12.5, 30, 7
		created, err := p.CreateTenant(ctx, Tenant{
			Name: "quotas", Enabled: true,
			RateLimitRPS: &rps, RateLimitBurst: &burst, MaxWatchedContracts: &watched,
		})
		require.NoError(t, err)

		got, err := p.GetTenant(ctx, created.ID)
		require.NoError(t, err)
		require.NotNil(t, got.RateLimitRPS)
		assert.InDelta(t, 12.5, *got.RateLimitRPS, 0.001)
		require.NotNil(t, got.RateLimitBurst)
		assert.Equal(t, 30, *got.RateLimitBurst)
		require.NotNil(t, got.MaxWatchedContracts)
		assert.Equal(t, 7, *got.MaxWatchedContracts)
	})

	t.Run("nil overrides stay nil rather than becoming zero", func(t *testing.T) {
		// The distinction matters: nil inherits the instance default, 0 is
		// an explicit deny.
		created, err := p.CreateTenant(ctx, Tenant{Name: "inherits", Enabled: true})
		require.NoError(t, err)

		got, err := p.GetTenant(ctx, created.ID)
		require.NoError(t, err)
		assert.Nil(t, got.RateLimitRPS)
		assert.Nil(t, got.RateLimitBurst)
		assert.Nil(t, got.MaxWatchedContracts)
	})

	t.Run("updates persist", func(t *testing.T) {
		created, err := p.CreateTenant(ctx, Tenant{Name: "mutable", Enabled: true})
		require.NoError(t, err)

		created.Enabled = false
		created.Name = "renamed"
		updated, err := p.UpdateTenant(ctx, created)
		require.NoError(t, err)
		assert.False(t, updated.Enabled)
		assert.Equal(t, "renamed", updated.Name)
	})

	t.Run("updating a missing tenant reports not found", func(t *testing.T) {
		_, err := p.UpdateTenant(ctx, Tenant{ID: 999999, Name: "ghost"})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("deleting a missing tenant reports not found", func(t *testing.T) {
		assert.ErrorIs(t, p.DeleteTenant(ctx, 999999), ErrNotFound)
	})
}

func TestUsageAccounting(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()
	one := mustTenant(t, p, "one")
	two := mustTenant(t, p, "two")
	day := time.Now().UTC()

	require.NoError(t, p.AddUsage(ctx, day, map[int64]UsageDelta{
		one.ID: {Requests: 3, EventsServed: 30, StreamSeconds: 5},
		two.ID: {Requests: 1, EventsServed: 10},
	}))

	// Counters accumulate rather than overwrite, so several API servers can
	// flush independently.
	require.NoError(t, p.AddUsage(ctx, day, map[int64]UsageDelta{
		one.ID: {Requests: 2, EventsServed: 20, StreamSeconds: 7},
	}))

	usage, err := p.ListUsage(ctx, one.ID, 30)
	require.NoError(t, err)
	require.Len(t, usage, 1)
	assert.Equal(t, int64(5), usage[0].Requests)
	assert.Equal(t, int64(50), usage[0].EventsServed)
	assert.Equal(t, int64(12), usage[0].StreamSeconds)

	t.Run("usage is per tenant", func(t *testing.T) {
		other, err := p.ListUsage(ctx, two.ID, 30)
		require.NoError(t, err)
		require.Len(t, other, 1)
		assert.Equal(t, int64(1), other[0].Requests)
		assert.Equal(t, int64(10), other[0].EventsServed)
	})

	t.Run("an empty batch is a no-op", func(t *testing.T) {
		assert.NoError(t, p.AddUsage(ctx, day, nil))
		assert.NoError(t, p.AddUsage(ctx, day, map[int64]UsageDelta{one.ID: {}}))
	})

	t.Run("usage for a tenant deleted mid-flush is dropped, not fatal", func(t *testing.T) {
		// The API server accumulates in memory; a tenant can be deleted
		// between accounting and the flush that writes it.
		assert.NoError(t, p.AddUsage(ctx, day, map[int64]UsageDelta{
			999999: {Requests: 1},
			one.ID: {Requests: 1},
		}))
	})

	t.Run("deleting a tenant removes its usage history", func(t *testing.T) {
		require.NoError(t, p.DeleteTenant(ctx, two.ID))
		usage, err := p.ListUsage(ctx, two.ID, 30)
		require.NoError(t, err)
		assert.Empty(t, usage)
	})
}

// watchedContractIDs flattens ListWatchedContracts to just the contract IDs.
//
// The union assertions are about which contracts ingestion follows, not when
// each was claimed — and added_at is aggregated across the operator's list
// and every tenant's, so it is not a stable thing for a test to pin.
func watchedContractIDs(t *testing.T, p *Postgres) []string {
	t.Helper()
	watched, err := p.ListWatchedContracts(context.Background())
	require.NoError(t, err)
	ids := make([]string, 0, len(watched))
	for _, w := range watched {
		ids = append(ids, w.ContractID)
	}
	return ids
}

// The headroom the API reports (#1157) is counted with the same queries
// AddTenantWatchedContract enforces its caps with, so what a tenant is told it
// holds is exactly what the limit compares against.
func TestWatchedContracts_CountsAgreeWithTheEnforcedCaps(t *testing.T) {
	ctx := context.Background()

	t.Run("a tenant's count tracks adds and stops at its cap", func(t *testing.T) {
		p := testTenantStore(t)
		limit := 2
		tenant, err := p.CreateTenant(ctx, Tenant{Name: "capped", Enabled: true, MaxWatchedContracts: &limit})
		require.NoError(t, err)

		n, err := p.CountTenantWatchedContracts(ctx, tenant.ID)
		require.NoError(t, err)
		assert.Equal(t, 0, n, "an empty watch list")

		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))
		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractB, 0))
		n, err = p.CountTenantWatchedContracts(ctx, tenant.ID)
		require.NoError(t, err)
		assert.Equal(t, limit, n, "exactly at the cap")

		// The add that the cap refuses leaves the count where it was.
		require.ErrorIs(t, p.AddTenantWatchedContract(ctx, tenant, "", contractC, 0), ErrQuotaExceeded)
		n, err = p.CountTenantWatchedContracts(ctx, tenant.ID)
		require.NoError(t, err)
		assert.Equal(t, limit, n)

		// An idempotent re-add does not inflate it.
		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))
		n, err = p.CountTenantWatchedContracts(ctx, tenant.ID)
		require.NoError(t, err)
		assert.Equal(t, limit, n)
	})

	t.Run("a tenant's count excludes other tenants' claims", func(t *testing.T) {
		p := testTenantStore(t)
		one := mustTenant(t, p, "one")
		two := mustTenant(t, p, "two")
		require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractA, 0))
		require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractA, 0))
		require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractB, 0))

		n, err := p.CountTenantWatchedContracts(ctx, one.ID)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("the instance count is the union the instance cap bounds", func(t *testing.T) {
		p := testTenantStore(t)
		one := mustTenant(t, p, "one")
		two := mustTenant(t, p, "two")

		require.NoError(t, p.AddTenantWatchedContract(ctx, one, "", contractA, 2))
		require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractA, 2))
		n, err := p.CountInstanceWatchedContracts(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n, "two tenants watching one contract is one entry in the union")

		require.NoError(t, p.AddTenantWatchedContract(ctx, two, "", contractB, 2))
		n, err = p.CountInstanceWatchedContracts(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "at the cap of 2")

		// The count says full, and the enforcement agrees.
		assert.ErrorIs(t, p.AddTenantWatchedContract(ctx, one, "", contractC, 2), ErrQuotaExceeded)
	})
}

func syntheticContractID(i int) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	b := make([]byte, 56)
	b[0] = 'C'
	n := i + 1
	for pos := 55; pos >= 1; pos-- {
		b[pos] = alphabet[n%32]
		n /= 32
	}
	return string(b)
}

// TestAddTenantWatchedContract_ConcurrentCapEnforcement fires parallel adds at
// a tenant one below its cap and at two tenants competing for the final
// instance-wide slot (#1140).
func TestAddTenantWatchedContract_ConcurrentCapEnforcement(t *testing.T) {
	ctx := context.Background()

	t.Run("parallel adds for one tenant one below its cap admit exactly one", func(t *testing.T) {
		p := testTenantStore(t)
		capLimit := 2
		tenant, err := p.CreateTenant(ctx, Tenant{
			Name:                "race-tenant",
			Enabled:             true,
			MaxWatchedContracts: &capLimit,
		})
		require.NoError(t, err)

		// Pre-fill to cap - 1.
		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))

		const workers = 16
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := range workers {
			cid := syntheticContractID(i)
			wg.Add(1)
			go func(contract string) {
				defer wg.Done()
				<-start
				errs <- p.AddTenantWatchedContract(ctx, tenant, "", contract, 0)
			}(cid)
		}
		close(start)
		wg.Wait()
		close(errs)

		var successes, quotaExceededs int
		for err := range errs {
			switch {
			case err == nil:
				successes++
			case assert.ErrorIs(t, err, ErrQuotaExceeded, "unexpected error: %v", err):
				quotaExceededs++
			}
		}
		assert.Equal(t, 1, successes, "exactly one concurrent add may take the last tenant slot")
		assert.Equal(t, workers-1, quotaExceededs)

		held, err := p.CountTenantWatchedContracts(ctx, tenant.ID)
		require.NoError(t, err)
		assert.Equal(t, capLimit, held)

		// Idempotent re-add of an already held contract still succeeds at cap.
		require.NoError(t, p.AddTenantWatchedContract(ctx, tenant, "", contractA, 0))
	})

	t.Run("parallel adds across two tenants one below instance cap admit", func(t *testing.T) {
		p := testTenantStore(t)
		t1 := mustTenant(t, p, "race-inst-1")
		t2 := mustTenant(t, p, "race-inst-2")

		const instanceCap = 2
		// Pre-fill 1 contract in the union (instanceCap - 1).
		require.NoError(t, p.AddTenantWatchedContract(ctx, t1, "", contractA, instanceCap))

		const perTenantWorkers = 8
		totalWorkers := perTenantWorkers * 2
		start := make(chan struct{})
		errs := make(chan error, totalWorkers)
		var wg sync.WaitGroup

		for i := range perTenantWorkers {
			c1 := syntheticContractID(100 + i)
			c2 := syntheticContractID(200 + i)
			wg.Add(2)
			go func(contract string) {
				defer wg.Done()
				<-start
				errs <- p.AddTenantWatchedContract(ctx, t1, "", contract, instanceCap)
			}(c1)
			go func(contract string) {
				defer wg.Done()
				<-start
				errs <- p.AddTenantWatchedContract(ctx, t2, "", contract, instanceCap)
			}(c2)
		}
		close(start)
		wg.Wait()
		close(errs)

		var successes, quotaExceededs int
		for err := range errs {
			switch {
			case err == nil:
				successes++
			case assert.ErrorIs(t, err, ErrQuotaExceeded, fmt.Sprintf("unexpected error: %v", err)):
				quotaExceededs++
			}
		}
		assert.Equal(t, 1, successes, "exactly one concurrent add across tenants may take the last instance slot")
		assert.Equal(t, totalWorkers-1, quotaExceededs)

		unionHeld, err := p.CountInstanceWatchedContracts(ctx)
		require.NoError(t, err)
		assert.Equal(t, instanceCap, unionHeld)
	})
}
