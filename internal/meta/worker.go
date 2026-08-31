package meta

import (
	"context"
	"sync"
	"time"
)

// Metadata represents resolved contract token information or negative cache marker.
type Metadata struct {
	Name     string
	Symbol   string
	Decimals uint32
	IsToken  bool
}

type RPCClient interface {
	GetMetadata(ctx context.Context, contractID string) (Metadata, error)
}

type MetadataStore interface {
	Get(contractID string) (Metadata, bool)
	Put(contractID string, m Metadata, t time.Time)
}

// Worker orchestrates contract metadata background refresh.
type Worker struct {
	singleFlight sync.Map
}

func NewWorker() *Worker {
	return &Worker{}
}

// ResolveMetadata resolves contract metadata with caching and interval checks.
func ResolveMetadata(ctx context.Context, rpc RPCClient, store MetadataStore, contractID string) (Metadata, error) {
	if m, ok := store.Get(contractID); ok {
		return m, nil
	}
	meta, err := rpc.GetMetadata(ctx, contractID)
	if err != nil {
		return Metadata{}, err
	}
	store.Put(contractID, meta, time.Now())
	return meta, nil
}

// ResolveMetadataWithFallbacks resolves metadata and falls back to existing store state on RPC failure.
func ResolveMetadataWithFallbacks(ctx context.Context, rpc RPCClient, store MetadataStore, contractID string) (Metadata, error) {
	meta, err := rpc.GetMetadata(ctx, contractID)
	if err != nil {
		if existing, ok := store.Get(contractID); ok {
			return existing, nil
		}
		return Metadata{}, err
	}
	store.Put(contractID, meta, time.Now())
	return meta, nil
}

// ResolveMetadataCachedSingleFlight ensures concurrent fetches for the same contract execute once.
func ResolveMetadataCachedSingleFlight(ctx context.Context, rpc RPCClient, store MetadataStore, contractID string) (Metadata, error) {
	if m, ok := store.Get(contractID); ok {
		return m, nil
	}
	
	val, _ := workerSingleFlightMap.LoadOrStore(contractID, &singleFlightItem{
		once: &sync.Once{},
	})
	item := val.(*singleFlightItem)
	
	var meta Metadata
	var err error
	item.once.Do(func() {
		meta, err = rpc.GetMetadata(ctx, contractID)
		if err == nil {
			store.Put(contractID, meta, time.Now())
		}
		item.meta = meta
		item.err = err
	})

	return item.meta, item.err
}

type singleFlightItem struct {
	once *sync.Once
	meta Metadata
	err  error
}

var workerSingleFlightMap sync.Map
