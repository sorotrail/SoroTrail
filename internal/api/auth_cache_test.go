package api

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestApplyInvalidation(t *testing.T) {
	now := time.Now()
	key := store.TenantAPIKey{ID: 7, TenantID: 42, Prefix: "pfx1", Name: "deploy"}
	tenant := store.Tenant{ID: 42, Name: "acme", Enabled: true}
	scope := store.NewScope([]string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"})

	newServer := func(t *testing.T) *Server {
		t.Helper()
		srv := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
		srv.SetAuthCacheTTL(time.Minute)
		srv.AuthCache().PutTenantAuth(key, []byte("digest-bytes"), tenant, scope, now)
		return srv
	}

	t.Run("happy path evicts the tenant's cached auth", func(t *testing.T) {
		srv := newServer(t)
		_, _, _, _, ok := srv.AuthCache().GetTenantAuth("pfx1", now)
		require.True(t, ok, "the entry is cached before the invalidation")

		srv.ApplyInvalidation(store.InvalidationEvent{Kind: store.InvalidationKindTenant, Target: "42"})

		_, _, _, _, ok = srv.AuthCache().GetTenantAuth("pfx1", now)
		require.False(t, ok, "ApplyInvalidation must evict the tenant's cached credentials")
		_, _, invalidations := srv.AuthCache().Stats()
		assert.Equal(t, uint64(1), invalidations)
	})

	t.Run("unparsable or out-of-range targets change nothing", func(t *testing.T) {
		tests := []struct {
			name   string
			kind   string
			target string
		}{
			{name: "tenant target is not a number", kind: store.InvalidationKindTenant, target: "not-a-tenant"},
			{name: "tenant target is empty", kind: store.InvalidationKindTenant, target: ""},
			{name: "tenant target is not positive", kind: store.InvalidationKindTenant, target: "0"},
			{name: "apikey target is not a number", kind: store.InvalidationKindAPIKey, target: "abc"},
			{name: "apikey target is negative", kind: store.InvalidationKindAPIKey, target: "-3"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				srv := newServer(t)

				srv.ApplyInvalidation(store.InvalidationEvent{Kind: tt.kind, Target: tt.target})

				_, _, _, _, ok := srv.AuthCache().GetTenantAuth("pfx1", now)
				assert.True(t, ok, "a bad target must not evict cached auth")
				_, _, invalidations := srv.AuthCache().Stats()
				assert.Zero(t, invalidations, "a bad target must not count as an invalidation")
			})
		}
	})
}

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
