package store

import (
	"context"
	"errors"
	"time"
)

// ErrDuplicate is returned when a create would violate a uniqueness
// constraint (a tenant name, an API key prefix). Callers map it to 409.
var ErrDuplicate = errors.New("already exists")

// ErrQuotaExceeded is returned when an operation would push a tenant, or
// the instance, past a configured cap. Callers map it to 429.
var ErrQuotaExceeded = errors.New("quota exceeded")

// Tenant is one consumer of a shared SoroTrail deployment.
type Tenant struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	// Wildcard tenants read every contract. Reserved for the legacy
	// "default" tenant and for operator-run tooling.
	Wildcard bool `json:"wildcard"`
	// Admin tenants may call /admin/*. Deliberately independent of
	// Wildcard: read breadth and management rights are different powers.
	Admin bool `json:"admin"`
	// Enabled=false rejects the tenant's keys at authentication without
	// destroying them, so suspension is reversible.
	Enabled bool `json:"enabled"`
	// RateLimitRPS/Burst override the instance-wide limiter for this
	// tenant. Nil inherits. Zero is meaningful (deny), which is why these
	// are pointers rather than zero-valued scalars.
	RateLimitRPS   *float64 `json:"rate_limit_rps,omitempty"`
	RateLimitBurst *int     `json:"rate_limit_burst,omitempty"`
	// MaxWatchedContracts caps how many contracts this tenant may add to
	// the ingestion watch list. Nil means unlimited, still subject to the
	// instance-wide cap.
	MaxWatchedContracts *int      `json:"max_watched_contracts,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
}

// APIKey is a credential belonging to a tenant. The secret itself is never
// stored or returned after creation — only its SHA-256 digest lives in the
// database, and Secret is populated exactly once, by CreateAPIKey, for the
// response that hands it to the operator.
type APIKey struct {
	ID         int64      `json:"id"`
	TenantID   int64      `json:"tenant_id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	// Secret is the plaintext key, set only in the create response.
	Secret string `json:"secret,omitempty"`
}

// TenantUsage is one tenant's accumulated consumption for a UTC day.
type TenantUsage struct {
	TenantID      int64     `json:"tenant_id"`
	Day           time.Time `json:"day"`
	Requests      int64     `json:"requests"`
	EventsServed  int64     `json:"events_served"`
	StreamSeconds int64     `json:"stream_seconds"`
}

// UsageDelta is an increment to a tenant's usage counters. The API server
// accumulates these in memory and flushes them in batches.
type UsageDelta struct {
	Requests      int64
	EventsServed  int64
	StreamSeconds int64
}

// Empty reports whether the delta would change nothing, so a flush can skip
// writing it.
func (d UsageDelta) Empty() bool {
	return d.Requests == 0 && d.EventsServed == 0 && d.StreamSeconds == 0
}

// TenantStore is the multi-tenancy persistence boundary. It is deliberately
// separate from Store: a backend that only ever serves a single tenant has
// no reason to implement any of it, and keeping it out of Store means the
// ingester and auditor — which have no tenant concept — do not carry the
// dependency.
type TenantStore interface {
	CreateTenant(ctx context.Context, t Tenant) (Tenant, error)
	GetTenant(ctx context.Context, id int64) (Tenant, error)
	GetTenantByName(ctx context.Context, name string) (Tenant, error)
	ListTenants(ctx context.Context) ([]Tenant, error)
	UpdateTenant(ctx context.Context, t Tenant) (Tenant, error)
	DeleteTenant(ctx context.Context, id int64) error

	// GrantContract makes contractID readable by the tenant. Idempotent.
	GrantContract(ctx context.Context, tenantID int64, contractID string) error
	// RevokeContract removes the grant. Revoking a contract the tenant does
	// not hold is not an error — the postcondition ("tenant cannot read it")
	// holds either way.
	RevokeContract(ctx context.Context, tenantID int64, contractID string) error
	// ListGrants returns the contract IDs the tenant may read, sorted.
	ListGrants(ctx context.Context, tenantID int64) ([]string, error)

	// ScopeForTenant resolves a tenant's read scope in one round trip.
	// Wildcard tenants short-circuit without reading the grants table.
	ScopeForTenant(ctx context.Context, t Tenant) (Scope, error)

	// AddTenantWatchedContract adds to the tenant's watch list, which feeds
	// the ingestion union. instanceCap bounds the size of the union across
	// all tenants; 0 means no instance cap. Returns ErrQuotaExceeded when
	// either the tenant's own cap or the instance cap would be exceeded.
	AddTenantWatchedContract(ctx context.Context, t Tenant, contractID string, instanceCap int) error
	// RemoveTenantWatchedContract drops one tenant's claim on a contract.
	// The contract remains ingested while any other tenant, or the global
	// watched_contracts list, still names it.
	RemoveTenantWatchedContract(ctx context.Context, tenantID int64, contractID string) error
	// ListTenantWatchedContracts returns just this tenant's claims.
	ListTenantWatchedContracts(ctx context.Context, tenantID int64) ([]string, error)

	// CreateAPIKey stores the digest of secret and returns the key record.
	CreateAPIKey(ctx context.Context, tenantID int64, name, prefix string, digest []byte) (APIKey, error)
	// CreateAPIKeyIfAbsent is CreateAPIKey made idempotent on the prefix,
	// for the startup bootstrap path: an operator-supplied key must survive
	// process restarts without erroring on the second boot.
	CreateAPIKeyIfAbsent(ctx context.Context, tenantID int64, name, prefix string, digest []byte) error
	// LookupAPIKey returns the key with the given prefix along with its
	// tenant, or ErrNotFound. Revoked keys are not returned. The caller is
	// responsible for comparing the secret half against Digest in constant
	// time — this method deliberately does not take the secret, so the
	// comparison cannot accidentally become a SQL equality test (which
	// would be both timing-leaky and index-searchable).
	LookupAPIKey(ctx context.Context, prefix string) (key APIKey, digest []byte, t Tenant, err error)
	// TouchAPIKey records that a key was used. Best-effort and advisory.
	TouchAPIKey(ctx context.Context, id int64) error
	ListAPIKeys(ctx context.Context, tenantID int64) ([]APIKey, error)
	RevokeAPIKey(ctx context.Context, id int64) error

	// AddUsage applies a batch of per-tenant increments in one statement.
	AddUsage(ctx context.Context, day time.Time, deltas map[int64]UsageDelta) error
	// ListUsage returns a tenant's daily usage rows, most recent first.
	ListUsage(ctx context.Context, tenantID int64, days int) ([]TenantUsage, error)
}
