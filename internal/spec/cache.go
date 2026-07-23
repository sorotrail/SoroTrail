package spec

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

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

// Cache caches parsed ContractSpec values, keyed by Wasm hash.
// It uses an in-memory sync.Map as the primary (fast) cache and a
// pluggable Store as the secondary (durable) cache.
type Cache struct {
	dbStore Store
	mu      sync.RWMutex
	mem     map[string]*ContractSpec // wasm_hash → spec
}

// NewCache creates a spec cache. Pass nil for dbStore to use only
// in-memory caching.
func NewCache(dbStore Store) *Cache {
	return &Cache{
		dbStore: dbStore,
		mem:     make(map[string]*ContractSpec),
	}
}

// Get returns a cached spec for the given Wasm hash, or nil if not cached.
func (c *Cache) Get(wasmHash string) *ContractSpec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mem[wasmHash]
}

// Set stores a spec in both memory and (if configured) database.
func (c *Cache) Set(ctx context.Context, spec *ContractSpec) error {
	c.mu.Lock()
	c.mem[spec.WasmHash] = spec
	c.mu.Unlock()

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

// LoadFromDB hydrates the in-memory cache from the database for the given
// wasm hash. Returns nil when the spec is not in the database.
func (c *Cache) LoadFromDB(ctx context.Context, wasmHash string) (*ContractSpec, error) {
	if c.dbStore == nil {
		return nil, nil
	}
	data, err := c.dbStore.GetContractSpec(ctx, wasmHash)
	if err != nil {
		return nil, err // let the caller decide how to handle (e.g. not found)
	}
	if data == nil {
		return nil, nil
	}
	var spec ContractSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("unmarshaling cached spec: %w", err)
	}
	c.mu.Lock()
	c.mem[spec.WasmHash] = &spec
	c.mu.Unlock()
	return &spec, nil
}

// Stats returns cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		CachedSpecs: len(c.mem),
	}
}

// CacheStats holds cache metrics.
type CacheStats struct {
	CachedSpecs int `json:"cached_specs"`
}
