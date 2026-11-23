package api

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// DefaultAuthCacheTTL is the bounded lifetime for cached API keys, tenant
// state, and contract scopes when in-memory auth caching is enabled. Active
// cross-replica invalidation via store.SpecInvalidationChannel evicts modified
// or revoked entries immediately; the short TTL bounds worst-case staleness if
// a notification is delayed.
const DefaultAuthCacheTTL = 5 * time.Second

// AuthCache is a short-lived, tenant- and key-indexed in-memory cache of
// resolved API credentials, tenant status (Enabled/Admin/Wildcard/quotas), and
// contract scopes.
//
// Invalidation is scoped by tenant ID and by API key ID so a write affecting
// one tenant or revoking one key evicts only the affected entries without
// flushing unrelated tenants' cached state.
type AuthCache struct {
	mu  sync.Mutex
	ttl time.Duration

	tenantByPrefix      map[string]cachedTenantAuth
	prefixesByTenant    map[int64]map[string]struct{}
	prefixByTenantKeyID map[int64]string

	singleByPrefix      map[string]cachedSingleAPIKey
	prefixBySingleKeyID map[int64]string

	hits          atomic.Uint64
	misses        atomic.Uint64
	invalidations atomic.Uint64
}

type cachedTenantAuth struct {
	key      store.TenantAPIKey
	digest   []byte
	tenant   store.Tenant
	scope    store.Scope
	cachedAt time.Time
}

type cachedSingleAPIKey struct {
	key      store.APIKey
	cachedAt time.Time
}

// NewAuthCache constructs an AuthCache with the given TTL. A non-positive TTL
// disables caching so all lookups pass straight through to the store.
func NewAuthCache(ttl time.Duration) *AuthCache {
	return &AuthCache{
		ttl:                 ttl,
		tenantByPrefix:      make(map[string]cachedTenantAuth),
		prefixesByTenant:    make(map[int64]map[string]struct{}),
		prefixByTenantKeyID: make(map[int64]string),
		singleByPrefix:      make(map[string]cachedSingleAPIKey),
		prefixBySingleKeyID: make(map[int64]string),
	}
}

// SetTTL updates the cache's freshness window and clears existing entries when
// caching is disabled.
func (c *AuthCache) SetTTL(ttl time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ttl = ttl
	if ttl <= 0 {
		c.resetLocked()
	}
	c.mu.Unlock()
}

// GetTenantAuth returns the cached tenant API key, secret digest, tenant
// record, and resolved contract scope for prefix when a fresh entry exists.
func (c *AuthCache) GetTenantAuth(prefix string, now time.Time) (store.TenantAPIKey, []byte, store.Tenant, store.Scope, bool) {
	if c == nil {
		return store.TenantAPIKey{}, nil, store.Tenant{}, store.Scope{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl <= 0 {
		return store.TenantAPIKey{}, nil, store.Tenant{}, store.Scope{}, false
	}
	entry, ok := c.tenantByPrefix[prefix]
	if !ok {
		c.misses.Add(1)
		return store.TenantAPIKey{}, nil, store.Tenant{}, store.Scope{}, false
	}
	if now.Sub(entry.cachedAt) >= c.ttl {
		c.deleteTenantPrefixLocked(prefix, entry.tenant.ID, entry.key.ID)
		c.misses.Add(1)
		return store.TenantAPIKey{}, nil, store.Tenant{}, store.Scope{}, false
	}
	c.hits.Add(1)
	return entry.key, entry.digest, entry.tenant, entry.scope, true
}

// PutTenantAuth stores a resolved tenant credential, tenant state, and scope
// indexed by prefix, tenant ID, and key ID.
func (c *AuthCache) PutTenantAuth(key store.TenantAPIKey, digest []byte, tenant store.Tenant, scope store.Scope, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl <= 0 {
		return
	}
	digestCopy := append([]byte(nil), digest...)
	c.tenantByPrefix[key.Prefix] = cachedTenantAuth{
		key:      key,
		digest:   digestCopy,
		tenant:   tenant,
		scope:    scope,
		cachedAt: now,
	}
	pset, ok := c.prefixesByTenant[tenant.ID]
	if !ok {
		pset = make(map[string]struct{})
		c.prefixesByTenant[tenant.ID] = pset
	}
	pset[key.Prefix] = struct{}{}
	c.prefixByTenantKeyID[key.ID] = key.Prefix
}

// GetAPIKey returns a cached single-tenant API key for prefix when fresh.
func (c *AuthCache) GetAPIKey(prefix string, now time.Time) (store.APIKey, bool) {
	if c == nil {
		return store.APIKey{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl <= 0 {
		return store.APIKey{}, false
	}
	entry, ok := c.singleByPrefix[prefix]
	if !ok {
		c.misses.Add(1)
		return store.APIKey{}, false
	}
	if now.Sub(entry.cachedAt) >= c.ttl {
		delete(c.singleByPrefix, prefix)
		delete(c.prefixBySingleKeyID, entry.key.ID)
		c.misses.Add(1)
		return store.APIKey{}, false
	}
	c.hits.Add(1)
	return entry.key, true
}

// PutAPIKey caches a single-tenant API key indexed by prefix and key ID.
func (c *AuthCache) PutAPIKey(key store.APIKey, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl <= 0 {
		return
	}
	c.singleByPrefix[key.Prefix] = cachedSingleAPIKey{
		key:      key,
		cachedAt: now,
	}
	c.prefixBySingleKeyID[key.ID] = key.Prefix
}

// InvalidateTenant evicts every cached credential, tenant record, and contract
// scope belonging to tenantID without disturbing any other tenant's entries.
func (c *AuthCache) InvalidateTenant(tenantID int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pset, ok := c.prefixesByTenant[tenantID]
	if !ok {
		return
	}
	for prefix := range pset {
		if entry, exists := c.tenantByPrefix[prefix]; exists {
			delete(c.prefixByTenantKeyID, entry.key.ID)
			delete(c.tenantByPrefix, prefix)
			c.invalidations.Add(1)
		}
	}
	delete(c.prefixesByTenant, tenantID)
}

// InvalidateAPIKey evicts a single API key (tenant-scoped or single-tenant) by
// its database ID.
func (c *AuthCache) InvalidateAPIKey(keyID int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prefix, ok := c.prefixByTenantKeyID[keyID]; ok {
		if entry, exists := c.tenantByPrefix[prefix]; exists {
			c.deleteTenantPrefixLocked(prefix, entry.tenant.ID, keyID)
			c.invalidations.Add(1)
		} else {
			delete(c.prefixByTenantKeyID, keyID)
		}
	}
	if prefix, ok := c.prefixBySingleKeyID[keyID]; ok {
		delete(c.singleByPrefix, prefix)
		delete(c.prefixBySingleKeyID, keyID)
		c.invalidations.Add(1)
	}
}

// Clear empties the entire AuthCache (used after an invalidation listener
// reconnects and may have missed notifications).
func (c *AuthCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	evicted := uint64(len(c.tenantByPrefix) + len(c.singleByPrefix))
	c.resetLocked()
	if evicted > 0 {
		c.invalidations.Add(evicted)
	}
}

func (c *AuthCache) deleteTenantPrefixLocked(prefix string, tenantID, keyID int64) {
	delete(c.tenantByPrefix, prefix)
	delete(c.prefixByTenantKeyID, keyID)
	if pset, ok := c.prefixesByTenant[tenantID]; ok {
		delete(pset, prefix)
		if len(pset) == 0 {
			delete(c.prefixesByTenant, tenantID)
		}
	}
}

func (c *AuthCache) resetLocked() {
	c.tenantByPrefix = make(map[string]cachedTenantAuth)
	c.prefixesByTenant = make(map[int64]map[string]struct{})
	c.prefixByTenantKeyID = make(map[int64]string)
	c.singleByPrefix = make(map[string]cachedSingleAPIKey)
	c.prefixBySingleKeyID = make(map[int64]string)
}

// Stats returns cumulative hit, miss, and invalidation counters for the cache.
func (c *AuthCache) Stats() (hits, misses, invalidations uint64) {
	if c == nil {
		return 0, 0, 0
	}
	return c.hits.Load(), c.misses.Load(), c.invalidations.Load()
}

// serverInvalidationState holds per-Server cross-replica cache coordination
// state (AuthCache, live stream scope watchers, and telemetry counters).
type serverInvalidationState struct {
	authCache *AuthCache

	watchersMu sync.Mutex
	nextWatch  int64
	watchers   map[int64]*streamScopeWatcher

	publishFailures       atomic.Uint64
	invalidationsReceived atomic.Uint64
	reconnectFlushes      atomic.Uint64
}

type streamScopeWatcher struct {
	tenantID int64
	keyID    int64
	ch       chan struct{}
}

var (
	serverInvStateMu sync.Mutex
	serverInvStates  = make(map[*Server]*serverInvalidationState)
)

func (s *Server) invState() *serverInvalidationState {
	serverInvStateMu.Lock()
	defer serverInvStateMu.Unlock()
	st, ok := serverInvStates[s]
	if !ok {
		st = &serverInvalidationState{
			authCache: NewAuthCache(0),
			watchers:  make(map[int64]*streamScopeWatcher),
		}
		serverInvStates[s] = st
	}
	return st
}

// SetAuthCacheTTL configures the in-memory AuthCache TTL for this Server.
// Pass 0 to disable in-memory credential/scope caching.
func (s *Server) SetAuthCacheTTL(ttl time.Duration) {
	s.invState().authCache.SetTTL(ttl)
}

// AuthCache returns the Server's AuthCache instance.
func (s *Server) AuthCache() *AuthCache {
	return s.invState().authCache
}

// InvalidationPublishFailures returns the number of cross-replica invalidation
// publish calls that failed on this Server.
func (s *Server) InvalidationPublishFailures() uint64 {
	return s.invState().publishFailures.Load()
}

// InvalidationsReceived returns the number of remote invalidation messages
// applied by this Server.
func (s *Server) InvalidationsReceived() uint64 {
	return s.invState().invalidationsReceived.Load()
}

// InvalidationReconnectFlushes returns how many times this Server flushed its
// caches after a listener reconnect.
func (s *Server) InvalidationReconnectFlushes() uint64 {
	return s.invState().reconnectFlushes.Load()
}

func (s *Server) registerStreamWatcher(tenantID, keyID int64) (<-chan struct{}, func()) {
	st := s.invState()
	ch := make(chan struct{}, 1)
	st.watchersMu.Lock()
	st.nextWatch++
	id := st.nextWatch
	st.watchers[id] = &streamScopeWatcher{
		tenantID: tenantID,
		keyID:    keyID,
		ch:       ch,
	}
	st.watchersMu.Unlock()

	cleanup := func() {
		st.watchersMu.Lock()
		delete(st.watchers, id)
		st.watchersMu.Unlock()
	}
	return ch, cleanup
}

func (s *Server) wakeStreamWatchers(tenantID, keyID int64) {
	st := s.invState()
	st.watchersMu.Lock()
	defer st.watchersMu.Unlock()
	for _, w := range st.watchers {
		matchAll := tenantID == 0 && keyID == 0
		matchTenant := tenantID > 0 && w.tenantID == tenantID
		matchKey := keyID > 0 && w.keyID == keyID
		if matchAll || matchTenant || matchKey {
			select {
			case w.ch <- struct{}{}:
			default:
			}
		}
	}
}

// ApplyInvalidation evicts local cached state matching ev across AuthCache,
// StatsCache, RateLimiter, spec cache, persisted GraphQL operations, and open
// SSE/WebSocket streams.
func (s *Server) ApplyInvalidation(ev store.InvalidationEvent) {
	switch ev.Kind {
	case store.InvalidationKindSpec:
		if ev.Target != "" {
			s.invalidateSpecContract(ev.Target)
		}
	case store.InvalidationKindTenant:
		tenantID, err := strconv.ParseInt(ev.Target, 10, 64)
		if err != nil || tenantID <= 0 {
			return
		}
		s.invState().authCache.InvalidateTenant(tenantID)
		if s.statsCache != nil {
			s.statsCache.InvalidateTenant(tenantID)
		}
		if s.limiter != nil {
			s.limiter.InvalidateTenant(tenantID)
		}
		s.wakeStreamWatchers(tenantID, 0)
	case store.InvalidationKindAPIKey:
		keyID, err := strconv.ParseInt(ev.Target, 10, 64)
		if err != nil || keyID <= 0 {
			return
		}
		s.invState().authCache.InvalidateAPIKey(keyID)
		s.wakeStreamWatchers(0, keyID)
	case store.InvalidationKindPersisted:
		if s.graphqlHandler != nil {
			type persistedReloader interface {
				ReloadPersistedManifest() error
			}
			if rel, ok := s.graphqlHandler.(persistedReloader); ok {
				if err := rel.ReloadPersistedManifest(); err != nil && s.log != nil {
					s.log.Warn("reloading persisted graphql manifest on invalidation failed", "error", err)
				}
			}
		}
	}
}

// ApplyRemoteInvalidation parses a raw payload from store.SpecInvalidationChannel
// and applies it to this Server, incrementing the received counter.
func (s *Server) ApplyRemoteInvalidation(payload string) {
	s.invState().invalidationsReceived.Add(1)
	s.ApplyInvalidation(store.ParseInvalidationPayload(payload))
}

// OnInvalidationReconnect flushes all security-relevant caches on this Server
// after the cross-replica invalidation listener reconnects and may have missed
// notifications while disconnected.
func (s *Server) OnInvalidationReconnect() {
	st := s.invState()
	st.reconnectFlushes.Add(1)
	st.authCache.Clear()
	if s.statsCache != nil {
		s.statsCache.Clear()
	}
	if s.limiter != nil {
		s.limiter.ClearBuckets()
	}
	if sc := getSpecCache(); sc != nil {
		if clearer, ok := sc.(interface{ Clear() }); ok {
			clearer.Clear()
		}
	}
	s.wakeStreamWatchers(0, 0)
	if s.log != nil {
		s.log.Info("cleared server caches after reconnecting to the invalidation channel")
	}
}

func (s *Server) publishInvalidation(ctx context.Context, kind, target string) {
	if s.specInvalidationPublisher == nil {
		return
	}
	payload := store.FormatInvalidationPayload(kind, target)
	if err := s.specInvalidationPublisher.PublishSpecInvalidation(ctx, payload); err != nil {
		count := s.invState().publishFailures.Add(1)
		if s.log != nil {
			s.log.Warn("publishing cross-replica invalidation failed",
				"kind", kind,
				"target", target,
				"error", err,
				"publish_failures", count,
			)
		}
	}
}

func (s *Server) notifyTenantInvalidation(ctx context.Context, tenantID int64) {
	target := strconv.FormatInt(tenantID, 10)
	s.ApplyInvalidation(store.InvalidationEvent{
		Kind:   store.InvalidationKindTenant,
		Target: target,
	})
	s.publishInvalidation(ctx, store.InvalidationKindTenant, target)
}

func (s *Server) notifyAPIKeyInvalidation(ctx context.Context, keyID int64) {
	target := strconv.FormatInt(keyID, 10)
	s.ApplyInvalidation(store.InvalidationEvent{
		Kind:   store.InvalidationKindAPIKey,
		Target: target,
	})
	s.publishInvalidation(ctx, store.InvalidationKindAPIKey, target)
}
