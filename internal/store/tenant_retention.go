package store

import (
	"context"
	"sort"
	"time"
)

// Per-tenant retention (#1249).
//
// Retention used to be one instance-wide horizon. With tenants it becomes a
// property of each tenant, but the rows it governs are not: an event belongs
// to a contract, and one contract can be granted to several tenants with
// different policies. Deleting a shared row for the tenant with the shortest
// horizon would delete it from every other tenant too.
//
// The rule is therefore defined per contract, never per row:
//
//	A contract's events are kept for the LONGEST horizon among the tenants
//	entitled to read it.
//
// A tenant is entitled to a contract when it holds a grant for it, and a
// wildcard tenant is entitled to every contract. A contract nobody is
// entitled to keeps the instance default. The pruner deletes only past that
// combined horizon, so no tenant ever loses rows it is still entitled to;
// a tenant with a shorter policy is held to it at read time instead (see
// Scope.WithRetention).
//
// Planning reduces the tenants table to a handful of (horizon, contract
// set) groups once per sweep, and each group is one batched DELETE with a
// contract_id = ANY(...) predicate that the existing contract index serves.
// The cost scales with the number of distinct horizons, not with rows.

// Policy labels reported in the pruner's logs, so an operator reading a
// sweep can tell which rule set the horizon it applied.
const (
	// RetentionPolicyDefault: no tenant policy reaches these contracts; the
	// instance-wide horizon applies.
	RetentionPolicyDefault = "instance_default"
	// RetentionPolicyWildcard: the horizon of a wildcard tenant, which is
	// entitled to every contract and so sets the floor for all of them.
	RetentionPolicyWildcard = "wildcard_tenant"
	// RetentionPolicyTenant: the longest horizon of the tenants granted the
	// group's contracts.
	RetentionPolicyTenant = "tenant"
)

// TenantRetention is one tenant's input to retention planning.
type TenantRetention struct {
	TenantID int64
	Wildcard bool
	// MaxAge is the tenant's own horizon. Nil inherits the instance default.
	MaxAge *time.Duration
	// Contracts are the tenant's grants. Ignored for a wildcard tenant,
	// which is entitled to every contract.
	Contracts []string
}

// EffectiveRetention returns the horizon a tenant is entitled to: its own
// override when set, otherwise the instance default. Zero means no age
// limit — events are kept until something other than age removes them.
func EffectiveRetention(override *time.Duration, instanceDefault time.Duration) time.Duration {
	if override != nil && *override > 0 {
		return *override
	}
	return instanceDefault
}

// longerRetention returns whichever horizon keeps data longer. Zero means
// unlimited, so it wins over any finite horizon.
func longerRetention(a, b time.Duration) time.Duration {
	if a == 0 || b == 0 {
		return 0
	}
	return max(a, b)
}

// RetentionShortens reports whether moving from horizon before to after
// makes events eligible for deletion that were not before — the change that
// cannot be undone once the pruner has run. Zero is unlimited.
func RetentionShortens(before, after time.Duration) bool {
	return after > 0 && (before == 0 || after < before)
}

// ContractSet selects the contracts a policy-scoped delete applies to.
// With Exclude false it matches exactly IDs (an empty list matches
// nothing); with Exclude true it matches every contract not in IDs (an
// empty list matches everything).
type ContractSet struct {
	IDs     []string
	Exclude bool
}

// All reports whether the set matches every contract, in which case a
// delete needs no contract predicate at all.
func (c ContractSet) All() bool { return c.Exclude && len(c.IDs) == 0 }

// Contains reports whether contractID is selected by the set.
func (c ContractSet) Contains(contractID string) bool {
	for _, id := range c.IDs {
		if id == contractID {
			return !c.Exclude
		}
	}
	return c.Exclude
}

// RetentionGroup is a set of contracts that share one effective horizon.
type RetentionGroup struct {
	// Policy names the rule that set MaxAge (one of the RetentionPolicy*
	// labels).
	Policy string
	// MaxAge is the group's horizon. Zero means no age limit.
	MaxAge time.Duration
	// Contracts selects the group's events. The default group selects
	// every contract no other group lists.
	Contracts ContractSet
}

// RetentionPlan is the per-contract retention policy reduced to groups.
// Every contract falls in exactly one group: the tenant groups list their
// contracts explicitly and the last group is the default, which excludes
// all of them.
type RetentionPlan struct {
	Groups []RetentionGroup
}

// DefaultRetentionPlan is the plan without tenant policies: one group, the
// instance default, over every contract. It is exactly the pre-#1249
// behaviour.
func DefaultRetentionPlan(instanceDefault time.Duration) RetentionPlan {
	return RetentionPlan{Groups: []RetentionGroup{{
		Policy:    RetentionPolicyDefault,
		MaxAge:    instanceDefault,
		Contracts: ContractSet{Exclude: true},
	}}}
}

// LongestMaxAge returns the longest horizon any group keeps, zero when some
// group is unlimited. It is the only horizon at which dropping a whole
// ledger partition is safe: a partition holds every contract's events for
// its ledger range, so it can go only once every row in it is past the
// longest horizon of any contract.
func (p RetentionPlan) LongestMaxAge() time.Duration {
	var longest time.Duration
	for i, g := range p.Groups {
		if i == 0 {
			longest = g.MaxAge
			continue
		}
		longest = longerRetention(longest, g.MaxAge)
	}
	return longest
}

// PlanRetention computes the retention plan for the given tenants under
// instanceDefault. The rule is documented at the top of this file: a
// contract keeps the longest horizon of the tenants entitled to it.
func PlanRetention(instanceDefault time.Duration, tenants []TenantRetention) RetentionPlan {
	// A wildcard tenant is entitled to every contract, so its horizon is a
	// floor under all of them, including contracts granted to nobody.
	base, basePolicy := instanceDefault, RetentionPolicyDefault
	hasWildcard := false
	for _, t := range tenants {
		if !t.Wildcard {
			continue
		}
		eff := EffectiveRetention(t.MaxAge, instanceDefault)
		if !hasWildcard {
			base, hasWildcard = eff, true
		} else {
			base = longerRetention(base, eff)
		}
	}
	if base != instanceDefault {
		basePolicy = RetentionPolicyWildcard
	}

	horizons := map[string]time.Duration{}
	for _, t := range tenants {
		if t.Wildcard {
			continue
		}
		eff := EffectiveRetention(t.MaxAge, instanceDefault)
		for _, id := range t.Contracts {
			cur, seen := horizons[id]
			switch {
			case seen:
				cur = longerRetention(cur, eff)
			case hasWildcard:
				cur = longerRetention(base, eff)
			default:
				cur = eff
			}
			horizons[id] = cur
		}
	}

	byAge := map[time.Duration][]string{}
	var listed []string
	for id, h := range horizons {
		if h == base {
			continue // the default group already keeps it this long
		}
		byAge[h] = append(byAge[h], id)
		listed = append(listed, id)
	}

	ages := make([]time.Duration, 0, len(byAge))
	for h := range byAge {
		ages = append(ages, h)
	}
	// Finite horizons in ascending order, unlimited last, so the plan (and
	// the logs that print it) are deterministic.
	sort.Slice(ages, func(i, j int) bool {
		if ages[i] == 0 || ages[j] == 0 {
			return ages[j] == 0 && ages[i] != 0
		}
		return ages[i] < ages[j]
	})

	plan := RetentionPlan{Groups: make([]RetentionGroup, 0, len(ages)+1)}
	for _, h := range ages {
		ids := byAge[h]
		sort.Strings(ids)
		plan.Groups = append(plan.Groups, RetentionGroup{
			Policy:    RetentionPolicyTenant,
			MaxAge:    h,
			Contracts: ContractSet{IDs: ids},
		})
	}
	sort.Strings(listed)
	plan.Groups = append(plan.Groups, RetentionGroup{
		Policy:    basePolicy,
		MaxAge:    base,
		Contracts: ContractSet{IDs: listed, Exclude: true},
	})
	return plan
}

// RetentionPolicyStore is the persistence surface the pruners need to apply
// per-tenant retention. It is separate from Store because only a backend
// with tenant storage can implement it; the pruners fall back to the
// instance-wide sweep when they are not given one.
type RetentionPolicyStore interface {
	// ListTenantRetention returns every tenant's horizon and grants in one
	// round trip, for PlanRetention.
	ListTenantRetention(ctx context.Context) ([]TenantRetention, error)
	// DeleteEventsBeforeIn is DeleteEventsBefore restricted to the events
	// of the contracts in set.
	DeleteEventsBeforeIn(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int, set ContractSet) (int64, error)
	// CountEventsBeforeIn is CountEventsBefore restricted the same way.
	CountEventsBeforeIn(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int, set ContractSet) (int64, error)
	// PruneEventsBeforeIn is PruneEventsBefore restricted the same way.
	PruneEventsBeforeIn(ctx context.Context, cutoff time.Time, set ContractSet) (int64, error)
}

// LoadRetentionPlan reads the tenants' policies from ps and plans them under
// instanceDefault. A nil ps yields the default plan.
func LoadRetentionPlan(ctx context.Context, ps RetentionPolicyStore, instanceDefault time.Duration) (RetentionPlan, error) {
	if ps == nil {
		return DefaultRetentionPlan(instanceDefault), nil
	}
	tenants, err := ps.ListTenantRetention(ctx)
	if err != nil {
		return RetentionPlan{}, err
	}
	return PlanRetention(instanceDefault, tenants), nil
}
