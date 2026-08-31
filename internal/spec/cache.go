package spec

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khaylebfortune/sorotrail/internal/store"
)

const (
	// DefaultSpecTTL is how long a cached spec stays fresh in memory.
	// Specs are immutable per wasm hash, so the TTL only bounds how long
	// a stale *mapping* (see DefaultHashTTL) can keep an unused spec alive.
	DefaultSpecTTL = 10 * time.Minute
	// DefaultHashTTL is how long the cache trusts its contractID →
	// wasm-hash mapping before re-resolving it. A contract upgrade that
	// changes the wasm hash is detected within at most one hash TTL.
	DefaultHashTTL = time.Minute
)

// WasmHashResolver resolves a contract ID to its current wasm hash.
// *Fetcher implements it; the cache uses it to detect contract upgrades
// (a changed hash invalidates the previously cached spec).
type WasmHashResolver interface {
	FetchWasmHash(ctx context.Context, contractID string) (string, error)
}

// Store is the subset of store.Store the cache needs: persisting and loading
// parsed specs. This narrow interface lets tests substitute a mock without
// importing the full store package.
type Store interface {
	// GetContractSpec returns the JSON-serialized spec for a wasm_hash, or
	// an error (which callers should treat as "not cached").
	GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error)
	// SetContractSpec persists a JSON-serialized spec keyed by wasm_hash.
	SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error
}

// entry is one in-memory cache slot: the spec plus the time it was cached,
// used for TTL expiry.
type entry struct {
	spec     *ContractSpec
	cachedAt time.Time
}

// hashEntry is the cached contractID → wasm-hash mapping plus the time it
// was last verified against the resolver.
type hashEntry struct {
	hash    string
	checked time.Time
}

// Cache caches parsed ContractSpec values, keyed by Wasm hash.
// It uses an in-memory map as the primary (fast) cache and a
// pluggable Store as the secondary (durable) cache.
//
// Invalidation model:
//   - Entries expire after the spec TTL.
//   - The contractID → wasm-hash mapping is re-resolved after the hash
//     TTL; when the resolved hash differs from the cached one (contract
//     upgrade), the old spec is invalidated immediately and the next
//     lookup fetches the new spec.
//   - Every lookup is counted: Hits are served from cache, Misses
//     required a fetch, and Fetches/Expiries/Invalidations explain why.
type Cache struct {
	dbStore  Store
	resolver WasmHashResolver
	ttl      time.Duration
	hashTTL  time.Duration
	now      func() time.Time

	mu     sync.Mutex
	mem    map[string]*entry    // wasm_hash → spec entry
	hashes map[string]hashEntry // contract_id → wasm hash mapping

	hits          atomic.Uint64
	misses        atomic.Uint64
	fetches       atomic.Uint64
	expiries      atomic.Uint64
	invalidations atomic.Uint64
}

// Option configures the cache.
type Option func(*Cache)

// WithTTL sets the in-memory spec TTL. Zero or negative disables expiry.
func WithTTL(ttl time.Duration) Option {
	return func(c *Cache) { c.ttl = ttl }
}

// WithHashTTL sets how long the contractID → wasm-hash mapping is trusted
// before re-resolution. Zero or negative disables re-resolution.
func WithHashTTL(ttl time.Duration) Option {
	return func(c *Cache) { c.hashTTL = ttl }
}

// WithWasmHashResolver wires the resolver used to detect wasm-hash changes.
// Without one, lookups by contract ID fall back to a reverse scan of the
// in-memory cache and upgrades are only picked up at spec-TTL expiry.
func WithWasmHashResolver(r WasmHashResolver) Option {
	return func(c *Cache) { c.resolver = r }
}

// WithClock overrides the time source (tests).
func WithClock(now func() time.Time) Option {
	return func(c *Cache) { c.now = now }
}

// NewCache creates a spec cache. Pass nil for dbStore to use only
// in-memory caching. Defaults: DefaultSpecTTL, DefaultHashTTL, no resolver.
func NewCache(dbStore Store, opts ...Option) *Cache {
	c := &Cache{
		dbStore: dbStore,
		ttl:     DefaultSpecTTL,
		hashTTL: DefaultHashTTL,
		now:     time.Now,
		mem:     make(map[string]*entry),
		hashes:  make(map[string]hashEntry),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get returns a cached, non-expired spec for the given Wasm hash, or nil.
// Counts a hit or a miss; an expired entry is evicted and counted as both
// an expiry and a miss.
func (c *Cache) Get(wasmHash string) *ContractSpec {
	return c.get(wasmHash)
}

// get is the TTL-aware, metrics-counting lookup by wasm hash.
func (c *Cache) get(wasmHash string) *ContractSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.mem[wasmHash]
	if !ok {
		c.misses.Add(1)
		return nil
	}
	if c.ttl > 0 && c.now().Sub(e.cachedAt) >= c.ttl {
		delete(c.mem, wasmHash)
		c.expiries.Add(1)
		c.misses.Add(1)
		return nil
	}
	c.hits.Add(1)
	return e.spec
}

// GetForContract returns the cached spec for a contract, verifying that
// the contract's wasm hash has not changed. When a resolver is wired and
// the hash mapping is older than the hash TTL, the current hash is
// re-resolved; a changed hash invalidates the previously cached spec
// immediately (contract upgrade), so the next fetch picks up the new spec.
//
// A failed re-resolution is stale-tolerant: the last known mapping is
// used rather than flushing the cache on a transient RPC error.
func (c *Cache) GetForContract(ctx context.Context, contractID string) *ContractSpec {
	if c.resolver == nil {
		if s := c.GetByContractID(contractID); s != nil {
			return s
		}
		c.misses.Add(1)
		return nil
	}

	c.mu.Lock()
	he, known := c.hashes[contractID]
	fresh := known && (c.hashTTL <= 0 || c.now().Sub(he.checked) < c.hashTTL)
	c.mu.Unlock()

	if !fresh {
		current, err := c.resolver.FetchWasmHash(ctx, contractID)
		if err != nil {
			// Stale-tolerant: prefer the last known mapping.
			if known {
				return c.get(he.hash)
			}
			c.misses.Add(1)
			return nil
		}
		c.mu.Lock()
		if known && he.hash != current {
			// Contract upgrade: the wasm hash changed, so the old
			// spec can no longer describe this contract's events.
			delete(c.mem, he.hash)
			c.invalidations.Add(1)
		}
		c.hashes[contractID] = hashEntry{hash: current, checked: c.now()}
		c.mu.Unlock()
		he = hashEntry{hash: current}
	}

	return c.get(he.hash)
}

// GetByContractID returns a cached (non-expired) spec whose ContractID
// matches, or nil. Specs are keyed by wasm hash, so callers holding only
// a contract ID need this reverse lookup. Expired entries are skipped and
// evicted.
func (c *Cache) GetByContractID(contractID string) *ContractSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for hash, e := range c.mem {
		if e.spec.ContractID != contractID {
			continue
		}
		if c.ttl > 0 && now.Sub(e.cachedAt) >= c.ttl {
			delete(c.mem, hash)
			c.expiries.Add(1)
			continue
		}
		return e.spec
	}
	return nil
}

// Set stores a spec in both memory and (if configured) database.
// Counts a fetch: Set is only called after a spec was (re)fetched.
func (c *Cache) Set(ctx context.Context, spec *ContractSpec) error {
	c.mu.Lock()
	c.mem[spec.WasmHash] = &entry{spec: spec, cachedAt: c.now()}
	if spec.ContractID != "" {
		c.hashes[spec.ContractID] = hashEntry{hash: spec.WasmHash, checked: c.now()}
	}
	c.mu.Unlock()
	c.fetches.Add(1)

	if c.dbStore != nil {
		specJSON, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("marshaling spec for db cache: %w", err)
		}
		if err := c.dbStore.SetContractSpec(ctx, spec.WasmHash, spec.ContractID, specJSON); err != nil {
			return fmt.Errorf("persisting spec to db: %w", err)
		}
	}
	return nil
}

// Invalidate evicts one spec by wasm hash (e.g. after detecting that a
// contract upgraded away from it).
func (c *Cache) Invalidate(wasmHash string) {
	c.mu.Lock()
	delete(c.mem, wasmHash)
	c.mu.Unlock()
	c.invalidations.Add(1)
}

// InvalidateContract evicts the spec cached for a contract and its
// wasm-hash mapping, forcing a full re-resolve on the next lookup.
func (c *Cache) InvalidateContract(contractID string) {
	c.mu.Lock()
	he, ok := c.hashes[contractID]
	if ok {
		delete(c.mem, he.hash)
		delete(c.hashes, contractID)
	}
	c.mu.Unlock()
	if ok {
		c.invalidations.Add(1)
	}
}

// LoadFromDB hydrates the in-memory cache from the database for the given
// wasm hash. Returns nil when the spec is not in the database.
func (c *Cache) LoadFromDB(ctx context.Context, wasmHash string) (*ContractSpec, error) {
	if c.dbStore == nil {
		return nil, nil
	}
	data, err := c.dbStore.GetContractSpec(ctx, wasmHash)
	if err != nil {
		// Per the Store contract, any error means "not cached".
		return nil, nil
	}
	if data == nil {
		return nil, nil
	}
	var spec ContractSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("unmarshaling cached spec: %w", err)
	}
	c.mu.Lock()
	c.mem[spec.WasmHash] = &entry{spec: &spec, cachedAt: c.now()}
	c.mu.Unlock()
	return &spec, nil
}

// CacheStats holds cache metrics.
type CacheStats struct {
	CachedSpecs int `json:"cached_specs"`
	// Hits are lookups served from cache; Misses required a fetch.
	Hits   uint64 `json:"hits"`
	Misses uint64 `json:"misses"`
	// Fetches counts specs stored after (re)fetching.
	Fetches uint64 `json:"fetches"`
	// Expiries counts entries evicted by TTL.
	Expiries uint64 `json:"expiries"`
	// Invalidations counts entries evicted because the contract's wasm
	// hash changed (or explicit Invalidate calls).
	Invalidations uint64 `json:"invalidations"`
}

// Stats returns cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	cached := len(c.mem)
	c.mu.Unlock()
	return CacheStats{
		CachedSpecs:   cached,
		Hits:          c.hits.Load(),
		Misses:        c.misses.Load(),
		Fetches:       c.fetches.Load(),
		Expiries:      c.expiries.Load(),
		Invalidations: c.invalidations.Load(),
	}
}

// SpecCacheStats returns the JSON-friendly metrics view the API surfaces
// via GET /stats (same pattern as audit.Metrics → store.AuditStats).
func (c *Cache) SpecCacheStats() store.SpecCacheStats {
	s := c.Stats()
	return store.SpecCacheStats{
		CachedSpecs:   s.CachedSpecs,
		Hits:          s.Hits,
		Misses:        s.Misses,
		Fetches:       s.Fetches,
		Expiries:      s.Expiries,
		Invalidations: s.Invalidations,
	}
}
