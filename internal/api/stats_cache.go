package api

import (
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// StatsCache caches the assembled /stats result keyed by tenant scope so a
// busy endpoint does not re-run the expensive store aggregation on every
// request.
//
// The store.Stats aggregate (CountEvents/CountContracts over the events
// table, plus on-disk table sizes) is the genuinely costly part served by a
// single /stats call; the in-memory counters the handler overlays (auditor,
// pruner, recoverer, RPC errors) are cheap reads. Caching the finished
// struct keeps the DB from being hit on every call while the whole response
// stays consistent within the refresh interval.
//
// Entries are keyed by scope.Fingerprint so two tenants' scoped /stats
// responses can never collide and serve each other's numbers (the same
// isolation the HTTP cache headers enforce for list pages). In addition,
// entries may be associated with a tenant ID so cross-replica invalidation
// events can evict a single tenant's cached stats without flushing other
// tenants' entries.
type StatsCache struct {
	mu sync.Mutex
	// ttl is the freshness window. Zero means don't cache.
	ttl time.Duration
	// keys preserves insertion order so the oldest entry can be evicted
	// when the cache grows past maxEntries. Scoped (tenant) keys come
	// first and evict before the shared wildcard entry, which is the most
	// commonly hit.
	scopedKeys []string
	entries    map[string]statsCacheEntry
	// byTenant maps tenantID -> set of scope fingerprints owned by that
	// tenant so InvalidateTenant evicts only that tenant's entries.
	byTenant map[int64]map[string]struct{}
}

// maxStatsCacheEntries bounds how many tenant-scoped snapshots are held.
// Tenants are few (a hand-configurable union bounded by
// max_watched_contracts); even a modest cap far exceeds realistic concurrency
// while keeping the cache a constant-size structure.
const maxStatsCacheEntries = 64

type statsCacheEntry struct {
	stats    store.Stats
	cachedAt time.Time
}

func newStatsCache(ttl time.Duration) *StatsCache {
	return &StatsCache{
		ttl:      ttl,
		entries:  make(map[string]statsCacheEntry),
		byTenant: make(map[int64]map[string]struct{}),
	}
}

// Get returns the cached stats for the given scope fingerprint when a fresh
// entry exists. ok is false when there is nothing cached or it has expired.
func (c *StatsCache) Get(fingerprint string, now time.Time) (store.Stats, bool) {
	if c == nil || c.ttl <= 0 {
		return store.Stats{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[fingerprint]
	if !ok || now.Sub(e.cachedAt) >= c.ttl {
		return store.Stats{}, false
	}
	return e.stats, true
}

// Put stores a freshly assembled stats result for the scope fingerprint.
func (c *StatsCache) Put(fingerprint string, stats store.Stats, now time.Time) {
	c.PutForTenant(0, fingerprint, stats, now)
}

// PutForTenant stores a freshly assembled stats result and indexes it under
// tenantID (when tenantID > 0) for tenant-scoped cross-replica invalidation.
func (c *StatsCache) PutForTenant(tenantID int64, fingerprint string, stats store.Stats, now time.Time) {
	if c == nil || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[fingerprint]; !exists {
		c.scopedKeys = append(c.scopedKeys, fingerprint)
		if len(c.scopedKeys) > maxStatsCacheEntries {
			evict := c.scopedKeys[0]
			c.scopedKeys = c.scopedKeys[1:]
			delete(c.entries, evict)
		}
	}
	c.entries[fingerprint] = statsCacheEntry{stats: stats, cachedAt: now}
	if tenantID > 0 {
		if c.byTenant == nil {
			c.byTenant = make(map[int64]map[string]struct{})
		}
		fset, ok := c.byTenant[tenantID]
		if !ok {
			fset = make(map[string]struct{})
			c.byTenant[tenantID] = fset
		}
		fset[fingerprint] = struct{}{}
	}
}

// InvalidateTenant evicts cached stats snapshots belonging to tenantID without
// flushing other tenants' entries.
func (c *StatsCache) InvalidateTenant(tenantID int64) {
	if c == nil || tenantID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fset, ok := c.byTenant[tenantID]
	if !ok {
		return
	}
	for fp := range fset {
		delete(c.entries, fp)
	}
	delete(c.byTenant, tenantID)
}

// InvalidateScope evicts a single scope fingerprint from the cache.
func (c *StatsCache) InvalidateScope(fingerprint string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, fingerprint)
}

// Clear empties all cached stats snapshots (called on invalidation listener
// reconnect).
func (c *StatsCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scopedKeys = nil
	c.entries = make(map[string]statsCacheEntry)
	c.byTenant = make(map[int64]map[string]struct{})
}
