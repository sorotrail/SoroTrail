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
// isolation the HTTP cache headers enforce for list pages).
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
		ttl:     ttl,
		entries: make(map[string]statsCacheEntry),
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
}
