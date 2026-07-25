package store

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// Scope is the authorization boundary for every read of event data. It
// answers exactly one question — "which contracts may this caller see" —
// and it is threaded into the store's queries rather than checked in
// handlers, so a read path physically cannot return rows the caller is not
// entitled to.
//
// # Why the fields are unexported
//
// The obvious shape for this is a plain `AllowedContracts []string` on
// EventFilter. That fails open: the zero value is an empty slice, an empty
// slice reads naturally as "no constraint", and a handler that forgets to
// populate it leaks every tenant's data to whoever asked. The failure is
// silent and looks like a working endpoint.
//
// So Scope inverts it. The zero value grants nothing (DeniesAll reports
// true), and because the fields are unexported no code outside this package
// can mint a permissive Scope by struct literal — it must call
// WildcardScope or SystemScope, both of which are greppable. A read path
// that forgets its Scope returns an empty page in every deployment, which
// is loud, immediate, and caught by the first test that looks at it. The
// safe direction to fail is the direction that returns nothing.
//
// Scope is immutable once constructed and safe for concurrent use.
type Scope struct {
	// wildcard short-circuits every check. Set only by WildcardScope and
	// SystemScope.
	wildcard bool
	// contracts is sorted and deduplicated so Fingerprint is stable across
	// two Scopes granting the same set in different orders.
	contracts []string
}

// WildcardScope returns a Scope that can read every contract in the store,
// including contracts granted to nobody. It is the scope of an untenanted
// (MULTI_TENANT=false) deployment and of tenants flagged wildcard.
//
// Reviewers: every call site of this function and of SystemScope is a place
// where the tenant boundary is deliberately not applied. That is the
// complete audit surface — there is no other way to obtain a permissive
// Scope.
func WildcardScope() Scope { return Scope{wildcard: true} }

// SystemScope is the scope of the process's own machinery — the ingester,
// the auditor, the webhook notifier, the replay tool. None of these act on
// behalf of a caller, so none of them are subject to a tenant boundary.
//
// It is a distinct constructor from WildcardScope despite being identical,
// because the two mean different things at a call site: WildcardScope is an
// authorization decision about a request, SystemScope is the absence of a
// request. Keeping them separate keeps a grep for one from drowning in the
// other.
func SystemScope() Scope { return Scope{wildcard: true} }

// NewScope returns a Scope granting exactly the given contract IDs. An empty
// or nil slice yields a Scope that denies everything, which is the correct
// reading of "this tenant has been granted no contracts".
func NewScope(contractIDs []string) Scope {
	if len(contractIDs) == 0 {
		return Scope{}
	}
	seen := make(map[string]struct{}, len(contractIDs))
	out := make([]string, 0, len(contractIDs))
	for _, id := range contractIDs {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return Scope{}
	}
	sort.Strings(out)
	return Scope{contracts: out}
}

// IsWildcard reports whether the scope is unrestricted.
func (s Scope) IsWildcard() bool { return s.wildcard }

// DeniesAll reports whether the scope permits nothing at all. Read paths
// check this first and return an empty result without touching the
// database — there is no query that could produce an authorized row.
func (s Scope) DeniesAll() bool { return !s.wildcard && len(s.contracts) == 0 }

// Allows reports whether a single contract ID is readable under this scope.
// Used by the streaming dispatch path, which filters event-by-event in
// memory rather than through SQL.
func (s Scope) Allows(contractID string) bool {
	if s.wildcard {
		return true
	}
	// Linear scan beats a map here: grants are small (tens of contracts,
	// capped by max_watched_contracts) and this runs per event on the
	// broadcast hot path, where avoiding the map's pointer chasing and
	// allocation matters more than the asymptotics.
	for _, id := range s.contracts {
		if id == contractID {
			return true
		}
	}
	return false
}

// Contracts returns the granted contract IDs, sorted. The result is a copy;
// callers cannot widen a Scope by writing through it.
func (s Scope) Contracts() []string {
	if len(s.contracts) == 0 {
		return nil
	}
	out := make([]string, len(s.contracts))
	copy(out, s.contracts)
	return out
}

// Fingerprint is a short stable digest of what this scope permits, for use
// as a cache-key component.
//
// This exists because of a specific cross-tenant leak: response caching
// keys list pages by their filter, and two tenants issuing byte-identical
// requests would otherwise collide on one cache entry and be served each
// other's rows by any shared cache — or by a conditional request that
// matched the other tenant's ETag. Mixing the scope into the validator
// makes those two requests distinct representations, which is what they
// actually are.
func (s Scope) Fingerprint() string {
	if s.wildcard {
		return "wildcard"
	}
	if len(s.contracts) == 0 {
		return "none"
	}
	// The separator cannot appear in a contract strkey (base32, uppercase),
	// so distinct grant sets cannot collide by concatenation.
	sum := sha256.Sum256([]byte(strings.Join(s.contracts, "\n")))
	return hex.EncodeToString(sum[:8])
}
