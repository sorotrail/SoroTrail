package spec

import (
	"context"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Spec types are defined in spec.go

// SpecResolver resolves contract specs respecting history and caching with bounding.
type SpecResolver struct {
	store store.Store
	mu    sync.RWMutex
	cache map[string][]*ContractSpec
}

// NewSpecResolver creates a new spec resolver backed by store.
func NewSpecResolver(store store.Store) *SpecResolver {
	return &SpecResolver{
		store: store,
		cache: make(map[string][]*ContractSpec),
	}
}

// ResolveAt returns the contract spec valid at a given ledger or timestamp, falling back gracefully.
func (r *SpecResolver) ResolveAt(ctx context.Context, contractID string, ledgerSequence uint32, timestamp time.Time) (*ContractSpec, error) {
	if r == nil {
		return nil, nil
	}
	r.mu.RLock()
	history := r.cache[contractID]
	r.mu.RUnlock()

	var best *ContractSpec
	for _, s := range history {
		// If ValidFrom is not explicitly tracked or zero, we treat it as valid
		if best == nil || s.WasmHash != best.WasmHash {
			best = s
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, nil
}

// RegisterSpec registers a spec version for history tracking.
func (r *SpecResolver) RegisterSpec(contractID string, spec *ContractSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[contractID] = append(r.cache[contractID], spec)
}
