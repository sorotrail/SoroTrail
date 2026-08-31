package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// TestStatsCache unit-tests the per-scope TTL cache in isolation: expiry
// semantics and scope fingerprint isolation. HTTP-level behaviors (that a
// live server stops calling the store within the TTL) are covered in
// api_test.go and tenant_test.go.
func TestStatsCache(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	c := newStatsCache(10 * time.Second)

	// Nothing cached yet.
	_, ok := c.Get("wildcard", now)
	assert.False(t, ok, "cold cache must miss")

	// Cache a value.
	c.Put("wildcard", store.Stats{TotalEvents: 42}, now)

	// Within TTL -> hit.
	got, ok := c.Get("wildcard", now.Add(5*time.Second))
	require.True(t, ok, "fresh entry must hit")
	assert.Equal(t, int64(42), got.TotalEvents)

	// A different scope fingerprint never collides with an existing entry.
	assert.NotEqual(t, "wildcard", store.NewScope([]string{"CAAAA"}).Fingerprint())
	c.Put(store.NewScope([]string{"CAAAA"}).Fingerprint(), store.Stats{TotalEvents: 7}, now.Add(2*time.Second))
	got, ok = c.Get(store.NewScope([]string{"CAAAA"}).Fingerprint(), now.Add(3*time.Second))
	require.True(t, ok, "scoped entry must be retrievable")
	assert.Equal(t, int64(7), got.TotalEvents)
	// The wildcard entry is untouched by the scoped one.
	got, ok = c.Get("wildcard", now.Add(3*time.Second))
	require.True(t, ok)
	assert.Equal(t, int64(42), got.TotalEvents, "scoped write must not clobber wildcard entry")

	// Just past TTL -> miss (>= ttl).
	_, ok = c.Get("wildcard", now.Add(10*time.Second))
	assert.False(t, ok, "expired entry must miss")

	// A refreshed entry serves again.
	c.Put("wildcard", store.Stats{TotalEvents: 99}, now.Add(12*time.Second))
	got, ok = c.Get("wildcard", now.Add(15*time.Second))
	require.True(t, ok)
	assert.Equal(t, int64(99), got.TotalEvents, "a refreshed value supersedes the expired one")
}

// TestStatsCache_Disabled verifies a zero TTL disables caching entirely.
func TestStatsCache_Disabled(t *testing.T) {
	c := newStatsCache(0)
	c.Put("wildcard", store.Stats{TotalEvents: 42}, time.Now())
	_, ok := c.Get("wildcard", time.Now())
	assert.False(t, ok, "a zero TTL must never serve from cache")
}

// TestStatsCache_Eviction bounds the cache size so a flood of tenant scopes
// cannot grow the map unboundedly. Each scope grants a single distinct
// contract ID, so every fingerprint is unique.
func TestStatsCache_Eviction(t *testing.T) {
	c := newStatsCache(time.Hour)
	// Enough distinct scopes to overflow the cap comfortably.
	for i := 0; i < maxStatsCacheEntries+50; i++ {
		id := testContract[:len(testContract)-1] + string(rune('A'+i%26))
		fp := store.NewScope([]string{id}).Fingerprint()
		c.Put(fp, store.Stats{TotalEvents: int64(i)}, time.Now())
	}
	assert.LessOrEqual(t, len(c.entries), maxStatsCacheEntries,
		"cache must never hold more than maxStatsCacheEntries entries")
	// Every entry currently held must still be retrievable (no dangling key
	// left behind by eviction).
	for fp := range c.entries {
		_, ok := c.Get(fp, time.Now())
		assert.True(t, ok, "held entries must still be retrievable")
	}
}
