package store

// CacheInventoryEntry documents one in-process cache in SoroTrail: what it
// stores, how long entries live, what invalidates it, whether cross-replica
// invalidation is wired via the shared PostgreSQL LISTEN/NOTIFY channel
// (sorotrail_spec_invalidation), and the maximum revocation/staleness latency
// an operator should expect across replicas.
//
// Adding any new cache type or cache-bearing container in internal/ without
// registering an entry in AuthoritativeCacheInventory fails
// TestCacheInventoryCoversAllRepoCaches.
type CacheInventoryEntry struct {
	ID                     string
	Package                string
	Symbol                 string
	File                   string
	Holds                  string
	LifetimeTTL            string
	InvalidationTrigger    string
	CrossReplica           bool
	CrossReplicaKind       string
	MaxRevocationLatency   string
	SecurityClassification string
}

// AuthoritativeCacheInventory is the single source of truth for every in-memory
// cache across the SoroTrail repository.
var AuthoritativeCacheInventory = []CacheInventoryEntry{
	{
		ID:                     "spec.Cache",
		Package:                "internal/spec",
		Symbol:                 "Cache",
		File:                   "internal/spec/cache.go",
		Holds:                  "Parsed contract specs (by spec hash) and contractID -> specHash mappings",
		LifetimeTTL:            "Spec TTL (default 1h) and contract-hash mapping TTL (default 5m)",
		InvalidationTrigger:    "Spec override upsert/delete, contract WASM upgrade detection, or explicit Invalidate(contractID)",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindSpec,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY); fallback 5m hashTTL if listener disconnected",
		SecurityClassification: "integrity",
	},
	{
		ID:                     "api.AuthCache",
		Package:                "internal/api",
		Symbol:                 "AuthCache",
		File:                   "internal/api/auth_cache.go",
		Holds:                  "Resolved tenant API key digests, Tenant records (Enabled, Admin, Wildcard, RateLimit*), and resolved contract Scope",
		LifetimeTTL:            "Bounded short TTL (DefaultAuthCacheTTL = 5s)",
		InvalidationTrigger:    "Tenant update/suspension/deletion, contract grant/revocation, or API key revocation",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindTenant,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY for tenant:<id> and apikey:<id>); hard upper bound 5s TTL",
		SecurityClassification: "security-critical",
	},
	{
		ID:                     "api.StatsCache",
		Package:                "internal/api",
		Symbol:                 "StatsCache",
		File:                   "internal/api/stats_cache.go",
		Holds:                  "Assembled /stats responses keyed by tenant Scope.Fingerprint() and indexed by tenant ID",
		LifetimeTTL:            "STATS_CACHE_TTL (default 5s)",
		InvalidationTrigger:    "TTL expiry or tenant update/delete/contract grant/revocation",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindTenant,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY for tenant:<id>); hard upper bound 5s TTL",
		SecurityClassification: "tenant-isolation",
	},
	{
		ID:                     "api.RateLimiter",
		Package:                "internal/api",
		Symbol:                 "RateLimiter",
		File:                   "internal/api/ratelimit.go",
		Holds:                  "Per-client IP and per-tenant (tenant:<id>) token-bucket rate limiters",
		LifetimeTTL:            "10m idle eviction (evictAfter = 10m, sweep every 5m)",
		InvalidationTrigger:    "Idle eviction or tenant quota update/deletion",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindTenant,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY for tenant:<id>); re-seeded from updated Tenant quota on next request",
		SecurityClassification: "security-critical",
	},
	{
		ID:                     "api.syncStreamScope",
		Package:                "internal/api",
		Symbol:                 "syncStreamScope",
		File:                   "internal/api/handlers.go",
		Holds:                  "Open SSE/WebSocket broadcast.Subscription tenant Scope snapshot and active API key liveness",
		LifetimeTTL:            "Refreshed every StreamScopeSync (default 30s) and woken immediately on invalidation",
		InvalidationTrigger:    "Cross-replica tenant:<id> or apikey:<id> invalidation signal, or periodic 30s refresh ticker",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindTenant,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY wake channel); fallback 30s StreamScopeSync",
		SecurityClassification: "security-critical",
	},
	{
		ID:                     "graphql.PersistedRegistry",
		Package:                "internal/api/graphql",
		Symbol:                 "PersistedRegistry",
		File:                   "internal/api/graphql/persisted.go",
		Holds:                  "Registered persisted GraphQL operations (SHA-256 hash -> validated query document)",
		LifetimeTTL:            "Process lifetime or manifest reload",
		InvalidationTrigger:    "Admin PUT/POST/DELETE on /graphql/persisted or cross-replica persisted invalidation",
		CrossReplica:           true,
		CrossReplicaKind:       InvalidationKindPersisted,
		MaxRevocationLatency:   "Immediate (<100ms via LISTEN/NOTIFY for persisted:<hash|*> when backed by shared manifest)",
		SecurityClassification: "security-critical",
	},
	{
		ID:                     "api.LagTracker",
		Package:                "internal/api",
		Symbol:                 "LagTracker",
		File:                   "internal/api/lag.go",
		Holds:                  "Latest Stellar RPC chain head ledger number (cachedLedger) for /stats and /metrics lag calculation",
		LifetimeTTL:            "Bounded short TTL (default 5s)",
		InvalidationTrigger:    "5s TTL expiry on next LatestLedger(ctx) call",
		CrossReplica:           false,
		CrossReplicaKind:       "",
		MaxRevocationLatency:   "5s TTL (read-only external RPC chain head telemetry)",
		SecurityClassification: "non-security-telemetry",
	},
	{
		ID:                     "decode.CachingDecoder",
		Package:                "internal/decode",
		Symbol:                 "CachingDecoder",
		File:                   "internal/decode/cache.go",
		Holds:                  "Bounded LRU (4096 entries) memoizing pure base64 ScVal XDR -> decoded JSON output",
		LifetimeTTL:            "Process lifetime with 4096-entry LRU eviction",
		InvalidationTrigger:    "LRU capacity eviction (pure deterministic XDR decoding)",
		CrossReplica:           false,
		CrossReplicaKind:       "",
		MaxRevocationLatency:   "0ms (pure deterministic function of immutable XDR input)",
		SecurityClassification: "pure-memoization",
	},
	{
		ID:                     "meta.workerSingleFlightMap",
		Package:                "internal/meta",
		Symbol:                 "workerSingleFlightMap",
		File:                   "internal/meta/worker.go",
		Holds:                  "Process-local singleflight deduplication map (contractID -> *singleFlightItem) for concurrent metadata fetches",
		LifetimeTTL:            "Process lifetime (singleflight coordination)",
		InvalidationTrigger:    "None (coordinates concurrent RPC metadata lookups into MetadataStore)",
		CrossReplica:           false,
		CrossReplicaKind:       "",
		MaxRevocationLatency:   "N/A (non-security contract metadata singleflight helper)",
		SecurityClassification: "non-security-metadata",
	},
}
