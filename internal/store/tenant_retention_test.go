package store

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	retDay  = 24 * time.Hour
	retCtrA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	retCtrB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	retCtrC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
)

func durPtr(d time.Duration) *time.Duration { return &d }

// groupFor returns the horizon the plan applies to contractID, and fails
// the test unless exactly one group selects it.
func groupFor(t *testing.T, plan RetentionPlan, contractID string) RetentionGroup {
	t.Helper()
	var found []RetentionGroup
	for _, g := range plan.Groups {
		if g.Contracts.Contains(contractID) {
			found = append(found, g)
		}
	}
	require.Len(t, found, 1, "contract %s must fall in exactly one group", contractID)
	return found[0]
}

func TestPlanRetention_NoTenantPoliciesIsTheInstanceDefault(t *testing.T) {
	plan := PlanRetention(30*retDay, []TenantRetention{
		{TenantID: 1, Wildcard: true},
		{TenantID: 2, Contracts: []string{retCtrA}},
	})
	assert.Equal(t, DefaultRetentionPlan(30*retDay), plan,
		"with no tenant overrides the plan must be the pre-#1249 single sweep")
	assert.True(t, plan.Groups[0].Contracts.All())
}

func TestPlanRetention_SharedContractKeepsTheLongestPolicy(t *testing.T) {
	plan := PlanRetention(30*retDay, []TenantRetention{
		{TenantID: 2, MaxAge: durPtr(7 * retDay), Contracts: []string{retCtrA, retCtrB}},
		{TenantID: 3, MaxAge: durPtr(365 * retDay), Contracts: []string{retCtrA}},
	})
	assert.Equal(t, 365*retDay, groupFor(t, plan, retCtrA).MaxAge,
		"a contract shared by a 7d and a 365d tenant is kept for 365d")
	assert.Equal(t, 7*retDay, groupFor(t, plan, retCtrB).MaxAge,
		"a contract only the 7d tenant reads is kept for 7d")
	def := groupFor(t, plan, retCtrC)
	assert.Equal(t, 30*retDay, def.MaxAge, "an ungranted contract keeps the instance default")
	assert.Equal(t, RetentionPolicyDefault, def.Policy)
	assert.Equal(t, 365*retDay, plan.LongestMaxAge())
}

func TestPlanRetention_UnsetPolicyFallsBackToTheDefault(t *testing.T) {
	plan := PlanRetention(30*retDay, []TenantRetention{
		{TenantID: 2, MaxAge: durPtr(7 * retDay), Contracts: []string{retCtrA}},
		{TenantID: 3, Contracts: []string{retCtrA}}, // inherits 30d
	})
	assert.Equal(t, 30*retDay, groupFor(t, plan, retCtrA).MaxAge,
		"the inheriting tenant is entitled to the 30d default, so the shared contract keeps it")
}

func TestPlanRetention_WildcardTenantIsAFloorForEveryContract(t *testing.T) {
	plan := PlanRetention(30*retDay, []TenantRetention{
		{TenantID: 1, Wildcard: true, MaxAge: durPtr(90 * retDay)},
		{TenantID: 2, MaxAge: durPtr(7 * retDay), Contracts: []string{retCtrA}},
	})
	g := groupFor(t, plan, retCtrA)
	assert.Equal(t, 90*retDay, g.MaxAge, "a wildcard tenant reads every contract")
	def := groupFor(t, plan, retCtrC)
	assert.Equal(t, 90*retDay, def.MaxAge)
	assert.Equal(t, RetentionPolicyWildcard, def.Policy)
}

func TestPlanRetention_UnlimitedDefaultIsNeverShortened(t *testing.T) {
	// Instance keeps forever; an inheriting tenant shares contract A with a
	// 7d tenant, so A must stay unlimited. B is the 7d tenant's alone.
	plan := PlanRetention(0, []TenantRetention{
		{TenantID: 2, MaxAge: durPtr(7 * retDay), Contracts: []string{retCtrA, retCtrB}},
		{TenantID: 3, Contracts: []string{retCtrA}},
	})
	assert.Equal(t, time.Duration(0), groupFor(t, plan, retCtrA).MaxAge)
	assert.Equal(t, 7*retDay, groupFor(t, plan, retCtrB).MaxAge)
	assert.Equal(t, time.Duration(0), plan.LongestMaxAge(),
		"partitions can never be dropped while any contract is unlimited")
}

func TestPlanRetention_IsDeterministic(t *testing.T) {
	tenants := []TenantRetention{
		{TenantID: 2, MaxAge: durPtr(7 * retDay), Contracts: []string{retCtrB, retCtrA}},
		{TenantID: 3, MaxAge: durPtr(365 * retDay), Contracts: []string{retCtrC}},
	}
	first := PlanRetention(30*retDay, tenants)
	for range 20 {
		assert.Equal(t, first, PlanRetention(30*retDay, tenants))
	}
	require.Len(t, first.Groups, 3)
	assert.Equal(t, 7*retDay, first.Groups[0].MaxAge, "finite horizons ascend")
	assert.Equal(t, []string{retCtrA, retCtrB}, first.Groups[0].Contracts.IDs)
	assert.Equal(t, []string{retCtrA, retCtrB, retCtrC}, first.Groups[2].Contracts.IDs,
		"the default group excludes every listed contract")
}

func TestRetentionShortens(t *testing.T) {
	assert.True(t, RetentionShortens(30*retDay, 7*retDay))
	assert.True(t, RetentionShortens(0, 7*retDay), "unlimited to finite shortens")
	assert.False(t, RetentionShortens(7*retDay, 30*retDay))
	assert.False(t, RetentionShortens(7*retDay, 0), "finite to unlimited lengthens")
	assert.False(t, RetentionShortens(7*retDay, 7*retDay))
}

func TestTenantRetentionOverride(t *testing.T) {
	assert.Nil(t, Tenant{}.RetentionOverride())
	secs := int64(3600)
	got := Tenant{RetentionMaxAgeSeconds: &secs}.RetentionOverride()
	require.NotNil(t, got)
	assert.Equal(t, time.Hour, *got)
	assert.Equal(t, time.Hour, EffectiveRetention(got, retDay))
	assert.Equal(t, retDay, EffectiveRetention(nil, retDay))
}

func TestContractSetPredicate(t *testing.T) {
	var args []any
	arg := func(v any) string { args = append(args, v); return "$1" }

	pred, ok := contractSetPredicate(ContractSet{Exclude: true}, arg)
	assert.True(t, ok)
	assert.Empty(t, pred, "the all-contracts set adds no predicate, keeping the legacy SQL")

	pred, ok = contractSetPredicate(ContractSet{IDs: []string{retCtrA}, Exclude: true}, arg)
	assert.True(t, ok)
	assert.Equal(t, "NOT (contract_id = ANY($1))", pred)

	pred, ok = contractSetPredicate(ContractSet{IDs: []string{retCtrA}}, arg)
	assert.True(t, ok)
	assert.Equal(t, "contract_id = ANY($1)", pred)

	_, ok = contractSetPredicate(ContractSet{}, arg)
	assert.False(t, ok, "an empty include set matches nothing and must not issue SQL")
}

func TestScopeWithRetention(t *testing.T) {
	sc := NewScope([]string{retCtrA})
	_, ok := sc.RetentionCutoff(time.Now())
	assert.False(t, ok)

	bounded := sc.WithRetention(7 * retDay)
	now := time.Now()
	cutoff, ok := bounded.RetentionCutoff(now)
	require.True(t, ok)
	assert.Equal(t, now.Add(-7*retDay), cutoff)
	assert.True(t, bounded.Allows(retCtrA), "the horizon narrows by age, not by contract")
	assert.NotEqual(t, sc.Fingerprint(), bounded.Fingerprint(),
		"same grants, different horizons: different cache representations")
	assert.Equal(t, bounded.Fingerprint(), sc.WithRetention(7*retDay).Fingerprint())

	assert.True(t, Scope{}.WithRetention(retDay).DeniesAll(), "a denying scope stays denying")
	assert.Equal(t, "none", Scope{}.WithRetention(retDay).Fingerprint())
	assert.Equal(t, sc, sc.WithRetention(0))
}

func TestBuildEventWhereClause_AppliesTheScopeRetention(t *testing.T) {
	f := EventFilter{Scope: NewScope([]string{retCtrA}).WithRetention(7 * retDay)}
	where, args := buildEventWhereClause(f)
	joined := strings.Join(where, " AND ")
	assert.Contains(t, joined, "created_at >= $")
	var sawCutoff bool
	for _, a := range args {
		if ts, ok := a.(time.Time); ok {
			sawCutoff = true
			assert.WithinDuration(t, time.Now().Add(-7*retDay), ts, time.Minute)
		}
	}
	assert.True(t, sawCutoff)

	where, _ = buildEventWhereClause(EventFilter{Scope: NewScope([]string{retCtrA})})
	assert.NotContains(t, strings.Join(where, " AND "), "created_at >=",
		"no horizon, no predicate: untouched SQL for every other caller")

	q, qargs := appendRetentionCutoff(`SELECT 1 FROM events WHERE id = $1`, []any{"x"},
		WildcardScope().WithRetention(retDay))
	assert.Equal(t, `SELECT 1 FROM events WHERE id = $1 AND created_at >= $2`, q)
	assert.Len(t, qargs, 2)
}

// retentionPolicyFake is a RetentionPolicyStore for the legacy age job.
type retentionPolicyFake struct {
	tenants []TenantRetention
	pruned  []ContractSet
	cutoffs []time.Time
}

func (f *retentionPolicyFake) ListTenantRetention(context.Context) ([]TenantRetention, error) {
	return f.tenants, nil
}
func (f *retentionPolicyFake) DeleteEventsBeforeIn(context.Context, int64, time.Time, int, ContractSet) (int64, error) {
	return 0, nil
}
func (f *retentionPolicyFake) CountEventsBeforeIn(context.Context, int64, time.Time, int, ContractSet) (int64, error) {
	return 0, nil
}
func (f *retentionPolicyFake) PruneEventsBeforeIn(_ context.Context, cutoff time.Time, set ContractSet) (int64, error) {
	f.pruned = append(f.pruned, set)
	f.cutoffs = append(f.cutoffs, cutoff)
	return 1, nil
}

func TestRetentionPruner_AppliesTenantPolicies(t *testing.T) {
	legacy := &retentionStoreFake{}
	policies := &retentionPolicyFake{tenants: []TenantRetention{
		{TenantID: 2, MaxAge: durPtr(365 * retDay), Contracts: []string{retCtrA}},
	}}
	p := NewRetentionPruner(legacy, slog.New(slog.NewTextHandler(io.Discard, nil)), RetentionOptions{
		Age:      30 * retDay,
		Policies: policies,
	})
	p.prune(context.Background())

	assert.Empty(t, legacy.cutoffs, "with a tenant policy in force the unscoped delete must not run")
	require.Len(t, policies.pruned, 2)
	assert.Equal(t, ContractSet{IDs: []string{retCtrA}}, policies.pruned[0])
	assert.WithinDuration(t, time.Now().Add(-365*retDay), policies.cutoffs[0], time.Minute)
	assert.Equal(t, ContractSet{IDs: []string{retCtrA}, Exclude: true}, policies.pruned[1],
		"the default 30d sweep excludes the contract the 365d tenant is entitled to")
	assert.WithinDuration(t, time.Now().Add(-30*retDay), policies.cutoffs[1], time.Minute)
}

func TestRetentionPruner_WithoutTenantPoliciesIsUnchanged(t *testing.T) {
	legacy := &retentionStoreFake{}
	policies := &retentionPolicyFake{tenants: []TenantRetention{{TenantID: 1, Wildcard: true}}}
	p := NewRetentionPruner(legacy, slog.New(slog.NewTextHandler(io.Discard, nil)), RetentionOptions{
		Age:      30 * retDay,
		Policies: policies,
	})
	p.prune(context.Background())
	require.Len(t, legacy.cutoffs, 1, "no overrides: the original single statement runs")
	assert.Empty(t, policies.pruned)
}
