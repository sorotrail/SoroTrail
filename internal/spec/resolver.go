package spec

import (
	"context"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// ContractSpec definitions are in spec.go

// ContractSpec represents a parsed contract spec with events.
type ContractSpec struct {
	ContractID string      `json:"contract_id"`
	WasmHash   string      `json:"wasm_hash"`
	Events     []EventSpec `json:"events"`
	ValidFrom  uint32      `json:"valid_from"`
}

// EventSpec describes an individual event in a contract spec.
type EventSpec struct {
	Name       string      `json:"name"`
	TopicSpecs []FieldSpec `json:"topic_specs"`
	ValueSpec  *FieldSpec  `json:"value_spec,omitempty"`
}

// FieldSpec describes a single field name and type.
type FieldSpec struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

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
		if s.ValidFrom <= ledgerSequence {
			if best == nil || s.ValidFrom > best.ValidFrom {
				best = s
			}
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
