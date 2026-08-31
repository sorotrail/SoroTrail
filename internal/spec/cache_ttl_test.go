package spec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a controllable time source for TTL tests.
type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }
func (f *fakeClock) Advance(d time.Duration) {
	f.t = f.t.Add(d)
}

// stubResolver is a controllable WasmHashResolver.
type stubResolver struct {
	hashes map[string]string
	err    error
	calls  int
}

func (s *stubResolver) FetchWasmHash(_ context.Context, contractID string) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	if h, ok := s.hashes[contractID]; ok {
		return h, nil
	}
	return "", errors.New("unknown contract")
}

// countFetcher counts FetchSpec calls to prove caching avoids refetches.
type countFetcher struct {
	Fetcher
	calls int
}

func newTestSpec(wasmHash, contractID string) *ContractSpec {
	return &ContractSpec{
		WasmHash:   wasmHash,
		ContractID: contractID,
		Events:     []EventSpec{{Name: "transfer"}},
	}
}

// stubSpecStore is an in-memory spec.Store for the DB-backed cache tests.
type stubSpecStore struct {
	specs map[string][]byte
}

func (s *stubSpecStore) GetContractSpec(_ context.Context, wasmHash string) ([]byte, error) {
	b, ok := s.specs[wasmHash]
	if !ok {
		return nil, errors.New("not cached")
	}
	return b, nil
}

func (s *stubSpecStore) SetContractSpec(_ context.Context, wasmHash, _ string, specJSON []byte) error {
	s.specs[wasmHash] = specJSON
	return nil
}

func TestCache_TTLExpiry(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	cache := NewCache(nil, WithClock(clock.Now))

	spec := newTestSpec("hash-a", "CABC")
	require.NoError(t, cache.Set(context.Background(), spec))

	// Fresh: hit.
	assert.NotNil(t, cache.Get("hash-a"))

	// Advance past the TTL: expired, evicted, returns nil.
	clock.Advance(DefaultSpecTTL + time.Second)
	assert.Nil(t, cache.Get("hash-a"))
	assert.Equal(t, uint64(1), cache.Stats().Expiries)
}

func TestCache_DisabledTTL(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	cache := NewCache(nil, WithClock(clock.Now), WithTTL(0))

	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-a", "CABC")))
	clock.Advance(24 * time.Hour)
	assert.NotNil(t, cache.Get("hash-a"), "TTL disabled: entry must survive")
}

func TestCache_WasmHashChangeInvalidates(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	resolver := &stubResolver{hashes: map[string]string{"CABC": "hash-old"}}
	cache := NewCache(nil,
		WithClock(clock.Now),
		WithWasmHashResolver(resolver),
		WithHashTTL(time.Minute),
	)

	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-old", "CABC")))

	// Fresh mapping: served from cache.
	got := cache.GetForContract(context.Background(), "CABC")
	require.NotNil(t, got)
	assert.Equal(t, "hash-old", got.WasmHash)

	// Contract upgrade: the wasm hash changes on-chain.
	resolver.hashes["CABC"] = "hash-new"
	clock.Advance(DefaultHashTTL + time.Second)

	// Old spec must be gone; the cache has no spec for the new hash.
	got = cache.GetForContract(context.Background(), "CABC")
	assert.Nil(t, got, "upgraded contract must not be served the old spec")
	st := cache.Stats()
	assert.Equal(t, uint64(1), st.Invalidations, "hash change must count one invalidation")
	assert.Equal(t, 0, st.CachedSpecs, "old spec must be evicted")

	// After the new spec is fetched and stored, the lookup serves it.
	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-new", "CABC")))
	got = cache.GetForContract(context.Background(), "CABC")
	require.NotNil(t, got)
	assert.Equal(t, "hash-new", got.WasmHash)
}

func TestCache_ResolverErrorIsStaleTolerant(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	resolver := &stubResolver{hashes: map[string]string{"CABC": "hash-old"}}
	cache := NewCache(nil,
		WithClock(clock.Now),
		WithWasmHashResolver(resolver),
		WithHashTTL(time.Minute),
	)

	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-old", "CABC")))
	clock.Advance(DefaultHashTTL + time.Second)

	// Resolver fails transiently: keep serving the last known spec.
	resolver.err = errors.New("rpc down")
	got := cache.GetForContract(context.Background(), "CABC")
	require.NotNil(t, got, "transient resolver failure must not flush the cache")
	assert.Equal(t, "hash-old", got.WasmHash)
	assert.Equal(t, uint64(0), cache.Stats().Invalidations)
}

func TestCache_HitMissMetrics(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	cache := NewCache(nil, WithClock(clock.Now), WithTTL(0))

	// Miss on absent key.
	assert.Nil(t, cache.Get("nope"))
	st := cache.Stats()
	assert.Equal(t, uint64(0), st.Hits)
	assert.Equal(t, uint64(1), st.Misses)

	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-a", "CABC")))

	// Hit after Set.
	assert.NotNil(t, cache.Get("hash-a"))
	assert.NotNil(t, cache.Get("hash-a"))
	st = cache.Stats()
	assert.Equal(t, uint64(2), st.Hits)
	assert.Equal(t, uint64(1), st.Misses)
	assert.Equal(t, uint64(1), st.Fetches)
	assert.Equal(t, 1, st.CachedSpecs)
}

func TestCache_Invalidate(t *testing.T) {
	cache := NewCache(nil, WithTTL(0))
	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-a", "CABC")))

	cache.Invalidate("hash-a")
	assert.Nil(t, cache.Get("hash-a"))
	st := cache.Stats()
	assert.Equal(t, uint64(1), st.Invalidations)
	assert.Equal(t, 0, st.CachedSpecs)
}

func TestCache_InvalidateContract(t *testing.T) {
	cache := NewCache(nil, WithTTL(0))
	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-a", "CABC")))

	cache.InvalidateContract("CABC")
	assert.Nil(t, cache.Get("hash-a"))
	assert.Nil(t, cache.GetByContractID("CABC"))
	assert.Equal(t, uint64(1), cache.Stats().Invalidations)
}

func TestCache_GetByContractIDSkipsExpired(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	cache := NewCache(nil, WithClock(clock.Now))

	require.NoError(t, cache.Set(context.Background(), newTestSpec("hash-a", "CABC")))
	clock.Advance(DefaultSpecTTL + time.Second)

	assert.Nil(t, cache.GetByContractID("CABC"), "expired entry must not be served")
	assert.Equal(t, uint64(1), cache.Stats().Expiries)
}

func TestCache_LoadFromDBResetsTTL(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	db := &stubSpecStore{specs: map[string][]byte{}}
	cache := NewCache(db, WithClock(clock.Now))

	orig := newTestSpec("dbhash", "CDLZ")
	data, err := json.Marshal(orig)
	require.NoError(t, err)
	require.NoError(t, db.SetContractSpec(context.Background(), "dbhash", "CDLZ", data))

	clock.Advance(time.Hour)
	spec, err := cache.LoadFromDB(context.Background(), "dbhash")
	require.NoError(t, err)
	require.NotNil(t, spec)

	// Just hydrated: must be a hit, not immediately expired.
	assert.NotNil(t, cache.Get("dbhash"))
}
