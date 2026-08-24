package api

import (
	"context"
	"sync"
	"time"
)

// LatestLedgerProvider returns the chain head ledger from the RPC endpoint.
// It is satisfied by the internal/rpc client and kept as an interface here so
// the lag computation can be unit-tested without a live Stellar network.
type LatestLedgerProvider interface {
	GetLatestLedger(ctx context.Context) (uint32, error)
}

// LagTracker computes ingestion lag = latest RPC ledger - last ingested ledger.
// The latest-ledger lookup is cached for ttl so a busy /stats or /metrics
// endpoint does not hammer the RPC provider on every request.
type LagTracker struct {
	provider LatestLedgerProvider
	ttl      time.Duration

	mu           sync.Mutex
	cachedLedger uint32
	cachedAt     time.Time
}

// NewLagTracker builds a LagTracker. A non-positive ttl defaults to 5s.
func NewLagTracker(provider LatestLedgerProvider, ttl time.Duration) *LagTracker {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return &LagTracker{provider: provider, ttl: ttl}
}

// LatestLedger returns the chain head, serving from cache within the ttl.
func (t *LagTracker) LatestLedger(ctx context.Context) (uint32, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cachedLedger > 0 && time.Since(t.cachedAt) < t.ttl {
		return t.cachedLedger, nil
	}

	head, err := t.provider.GetLatestLedger(ctx)
	if err != nil {
		return 0, err
	}
	t.cachedLedger = head
	t.cachedAt = time.Now()
	return head, nil
}

// Lag reports how many ledgers the ingester is behind the chain head.
// A result of 0 means the ingester is caught up (head <= lastIngested).
func (t *LagTracker) Lag(ctx context.Context, lastIngested uint32) (uint32, error) {
	head, err := t.LatestLedger(ctx)
	if err != nil {
		return 0, err
	}
	if head <= lastIngested {
		return 0, nil
	}
	return head - lastIngested, nil
}
