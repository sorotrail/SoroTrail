package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestAuthCacheGetTenantAuth(t *testing.T) {
	now := time.Now()
	key := store.TenantAPIKey{ID: 7, TenantID: 42, Prefix: "pfx1", Name: "deploy"}
	tenant := store.Tenant{ID: 42, Name: "acme", Enabled: true}
	scope := store.NewScope([]string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"})
	digest := []byte("digest-bytes")

	t.Run("happy path returns cached auth", func(t *testing.T) {
		c := NewAuthCache(time.Minute)
		c.PutTenantAuth(key, digest, tenant, scope, now)

		gotKey, gotDigest, gotTenant, gotScope, ok := c.GetTenantAuth("pfx1", now.Add(10*time.Second))
		require.True(t, ok)
		assert.Equal(t, key, gotKey)
		assert.Equal(t, digest, gotDigest)
		assert.Equal(t, tenant, gotTenant)
		assert.Equal(t, scope, gotScope)

		hits, misses, _ := c.Stats()
		assert.Equal(t, uint64(1), hits)
		assert.Equal(t, uint64(0), misses)
	})

	t.Run("error path returns miss", func(t *testing.T) {
		tests := []struct {
			name   string
			prefix string
			at     time.Time
			setup  func(*AuthCache)
		}{
			{
				name:   "unknown prefix",
				prefix: "missing",
				at:     now,
				setup:  func(*AuthCache) {},
			},
			{
				name:   "expired entry",
				prefix: "pfx1",
				at:     now.Add(2 * time.Minute),
				setup: func(c *AuthCache) {
					c.PutTenantAuth(key, digest, tenant, scope, now)
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c := NewAuthCache(time.Minute)
				tt.setup(c)

				_, _, _, _, ok := c.GetTenantAuth(tt.prefix, tt.at)
				assert.False(t, ok)

				hits, misses, _ := c.Stats()
				assert.Equal(t, uint64(0), hits)
				assert.Equal(t, uint64(1), misses)
			})
		}
	})
}
