package pruner

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Per-tenant retention (#1249) through the batched pruner.

const (
	day      = 24 * time.Hour
	ctrShare = "CSHARESHARESHARESHARESHARESHARESHARESHARESHARESHARESHAR"
	ctrShort = "CSHORTSHORTSHORTSHORTSHORTSHORTSHORTSHORTSHORTSHORTSHOR"
	ctrOther = "COTHEROTHEROTHEROTHEROTHEROTHEROTHEROTHEROTHEROTHEROTHE"
)

// policyStore adds the RetentionPolicyStore surface to mockStore, applying
// the contract set the pruner passes exactly as the Postgres predicate does.
type policyStore struct {
	*mockStore
	tenants []store.TenantRetention
	scoped  int
}

func (p *policyStore) ListTenantRetention(context.Context) ([]store.TenantRetention, error) {
	return p.tenants, nil
}

func (p *policyStore) DeleteEventsBeforeIn(_ context.Context, maxLedger int64, beforeTime time.Time, limit int, set store.ContractSet) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scoped++
	var deleted int64
	for id, e := range p.events {
		if e.Ledger >= maxLedger || !set.Contains(e.ContractID) {
			continue
		}
		if !beforeTime.IsZero() && !e.CreatedAt.Before(beforeTime) {
			continue
		}
		delete(p.events, id)
		deleted++
		if deleted >= int64(limit) {
			break
		}
	}
	return deleted, nil
}

func (p *policyStore) CountEventsBeforeIn(_ context.Context, maxLedger int64, beforeTime time.Time, limit int, set store.ContractSet) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var n int64
	for _, e := range p.events {
		if e.Ledger < maxLedger && set.Contains(e.ContractID) &&
			(beforeTime.IsZero() || e.CreatedAt.Before(beforeTime)) {
			n++
		}
	}
	return min(n, int64(limit)), nil
}

func (p *policyStore) PruneEventsBeforeIn(context.Context, time.Time, store.ContractSet) (int64, error) {
	return 0, nil
}

func (p *policyStore) add(id, contract string, age time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events[id] = store.Event{ID: id, ContractID: contract, Ledger: 10, CreatedAt: time.Now().Add(-age)}
}

func (p *policyStore) has(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.events[id]
	return ok
}

func newPolicyStore(tenants ...store.TenantRetention) *policyStore {
	ps := &policyStore{mockStore: newMockStore(), tenants: tenants}
	ps.setIngestionState(100)
	return ps
}

func ageP(d time.Duration) *time.Duration { return &d }

func newPolicyPruner(ps *policyStore, opts Options) *Pruner {
	opts.Policies = ps
	return New(ps, slog.New(slog.NewTextHandler(nopWriter{}, nil)), opts)
}

func TestPrunerTenantRetention_SharedContractKeepsTheLongestPolicy(t *testing.T) {
	ps := newPolicyStore(
		store.TenantRetention{TenantID: 2, MaxAge: ageP(7 * day), Contracts: []string{ctrShare, ctrShort}},
		store.TenantRetention{TenantID: 3, MaxAge: ageP(365 * day), Contracts: []string{ctrShare}},
	)
	ps.add("shared-60d", ctrShare, 60*day)   // inside the 365d tenant's horizon
	ps.add("shared-400d", ctrShare, 400*day) // past every horizon
	ps.add("short-10d", ctrShort, 10*day)    // past the 7d tenant's horizon
	ps.add("short-3d", ctrShort, 3*day)
	ps.add("other-40d", ctrOther, 40*day) // ungranted: 30d default

	prn := newPolicyPruner(ps, Options{MaxAge: 30 * day})
	_, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)

	assert.True(t, ps.has("shared-60d"), "the 365d tenant is still entitled to it")
	assert.False(t, ps.has("shared-400d"))
	assert.False(t, ps.has("short-10d"), "only the 7d tenant reads this contract")
	assert.True(t, ps.has("short-3d"))
	assert.False(t, ps.has("other-40d"), "the instance default still applies to everything else")

	m := prn.Metrics()
	assert.Equal(t, int64(3), m.RetentionPolicyGroups)
	assert.Equal(t, int64(2), m.TenantPolicyRowsPurged)
	assert.Equal(t, int64(3), m.TotalRowsPurged)
}

func TestPrunerTenantRetention_ShortenedPolicyTakesEffectNextSweep(t *testing.T) {
	ps := newPolicyStore(store.TenantRetention{TenantID: 2, MaxAge: ageP(90 * day), Contracts: []string{ctrShort}})
	ps.add("e-20d", ctrShort, 20*day)
	prn := newPolicyPruner(ps, Options{MaxAge: 30 * day})

	_, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	require.True(t, ps.has("e-20d"), "inside the 90d policy")

	ps.tenants[0].MaxAge = ageP(7 * day) // the operator shortens the policy
	_, err = prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, ps.has("e-20d"), "the plan is reloaded every sweep")
}

func TestPrunerTenantRetention_UnsetPolicyFallsBackToTheDefault(t *testing.T) {
	ps := newPolicyStore(store.TenantRetention{TenantID: 2, Contracts: []string{ctrShort}})
	ps.add("e-40d", ctrShort, 40*day)
	ps.add("e-20d", ctrShort, 20*day)
	prn := newPolicyPruner(ps, Options{MaxAge: 30 * day})

	_, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, ps.has("e-40d"))
	assert.True(t, ps.has("e-20d"))
	assert.Zero(t, ps.scoped, "no overrides: the unscoped pre-#1249 delete runs")
	assert.Equal(t, int64(1), prn.Metrics().RetentionPolicyGroups)
}

func TestPrunerTenantRetention_NoBoundNeverDeletes(t *testing.T) {
	// Instance keeps forever and the inheriting tenant shares ctrShare, so
	// that contract has no bound at all and must survive; ctrShort is the
	// 7d tenant's alone.
	ps := newPolicyStore(
		store.TenantRetention{TenantID: 2, MaxAge: ageP(7 * day), Contracts: []string{ctrShare, ctrShort}},
		store.TenantRetention{TenantID: 3, Contracts: []string{ctrShare}},
	)
	ps.add("shared-old", ctrShare, 1000*day)
	ps.add("other-old", ctrOther, 1000*day)
	ps.add("short-old", ctrShort, 10*day)
	prn := newPolicyPruner(ps, Options{})
	require.True(t, prn.Enabled(), "tenant policies enable the pruner on a keep-forever instance")

	_, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, ps.has("shared-old"))
	assert.True(t, ps.has("other-old"), "an unbounded group is skipped, never deleted wholesale")
	assert.False(t, ps.has("short-old"))
}

func TestPrunerTenantRetention_DryRunCountsPerGroup(t *testing.T) {
	ps := newPolicyStore(store.TenantRetention{TenantID: 2, MaxAge: ageP(365 * day), Contracts: []string{ctrShare}})
	ps.add("shared-60d", ctrShare, 60*day)
	ps.add("other-60d", ctrOther, 60*day)
	prn := newPolicyPruner(ps, Options{MaxAge: 30 * day, DryRun: true})

	n, err := prn.pruneOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "only the default-horizon row is eligible")
	assert.True(t, ps.has("shared-60d"))
	assert.True(t, ps.has("other-60d"), "dry run deletes nothing")
}
