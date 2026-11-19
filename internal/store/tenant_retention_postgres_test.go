//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Per-tenant retention (#1249) against a real Postgres: the policy the
// pruner reads, the contract-scoped delete it issues, and the read-time
// horizon a tenant's scope applies.

func agedEvent(id string, ledger int64, contractID string, age time.Duration) Event {
	e := testEvent(id, ledger, contractID)
	e.CreatedAt = time.Now().Add(-age).UTC()
	return e
}

func TestTenantRetention_PostgresSharedContractAndReadVisibility(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()
	day := 24 * time.Hour

	week := int64(7 * 24 * 60 * 60)
	year := int64(365 * 24 * 60 * 60)
	short := mustTenant(t, p, "short", contractA, contractB)
	short.RetentionMaxAgeSeconds = &week
	short, err := p.UpdateTenant(ctx, short)
	require.NoError(t, err)
	long := mustTenant(t, p, "long", contractA)
	long.RetentionMaxAgeSeconds = &year
	_, err = p.UpdateTenant(ctx, long)
	require.NoError(t, err)

	_, err = p.UpsertEvents(ctx, []Event{
		agedEvent(eventID(1), 10, contractA, 60*day), // shared, inside the 365d horizon
		agedEvent(eventID(2), 11, contractB, 60*day), // the 7d tenant's alone
		agedEvent(eventID(3), 12, contractA, 1*day),
	})
	require.NoError(t, err)

	policies, err := p.ListTenantRetention(ctx)
	require.NoError(t, err)
	require.Len(t, policies, 3, "the seeded default tenant plus the two created here")

	// The seeded "default" tenant is a wildcard on the instance default, so
	// model an instance whose default is 7d: the wildcard floor is 7d.
	plan := PlanRetention(7*day, policies)
	for _, g := range plan.Groups {
		if g.MaxAge <= 0 {
			continue
		}
		_, err := p.DeleteEventsBeforeIn(ctx, 100, time.Now().Add(-g.MaxAge), 1000, g.Contracts)
		require.NoError(t, err)
	}

	wild := WildcardScope()
	_, err = p.GetEvent(ctx, eventID(1), wild)
	assert.NoError(t, err, "the 365d tenant is entitled to the shared row, so it survives")
	_, err = p.GetEvent(ctx, eventID(2), wild)
	assert.ErrorIs(t, err, ErrNotFound, "only the 7d tenant reads contract B")

	// Read time: the 7d tenant must not see the 60-day-old shared row
	// that is being kept for the 365d tenant.
	shortScope, err := p.ScopeForTenant(ctx, short)
	require.NoError(t, err)
	shortScope = shortScope.WithRetention(7 * day)

	_, err = p.GetEvent(ctx, eventID(1), shortScope)
	assert.ErrorIs(t, err, ErrNotFound)
	exists, err := p.EventExists(ctx, eventID(1), shortScope)
	require.NoError(t, err)
	assert.False(t, exists)
	events, _, err := p.QueryEvents(ctx, EventFilter{Scope: shortScope, Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, eventID(3), events[0].ID)
	n, err := p.CountEvents(ctx, EventFilter{Scope: shortScope})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	longScope, err := p.ScopeForTenant(ctx, long)
	require.NoError(t, err)
	events, _, err = p.QueryEvents(ctx, EventFilter{Scope: longScope.WithRetention(365 * day), Limit: 10})
	require.NoError(t, err)
	assert.Len(t, events, 2, "the 365d tenant still reads its whole history")
}

func TestTenantRetention_PostgresRoundTripsAndClears(t *testing.T) {
	p := testTenantStore(t)
	ctx := context.Background()

	tn := mustTenant(t, p, "acme")
	assert.Nil(t, tn.RetentionMaxAgeSeconds, "NULL inherits the instance default")

	secs := int64(86400)
	tn.RetentionMaxAgeSeconds = &secs
	got, err := p.UpdateTenant(ctx, tn)
	require.NoError(t, err)
	require.NotNil(t, got.RetentionMaxAgeSeconds)
	assert.Equal(t, secs, *got.RetentionMaxAgeSeconds)

	got.RetentionMaxAgeSeconds = nil
	got, err = p.UpdateTenant(ctx, got)
	require.NoError(t, err)
	assert.Nil(t, got.RetentionMaxAgeSeconds)

	bad := int64(0)
	got.RetentionMaxAgeSeconds = &bad
	_, err = p.UpdateTenant(ctx, got)
	assert.Error(t, err, "the CHECK constraint rejects a non-positive horizon")
}
