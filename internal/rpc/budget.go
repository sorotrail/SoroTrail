package rpc

import (
	"context"
	"fmt"
	"math"

	"golang.org/x/time/rate"
)

// Budget splits a shared request rate into two pools, an "ingest" pool and
// an "audit" pool, using one token-bucket per pool. The sum of the two
// steady-state rates is exactly maxRPS, so callers can express a strict
// share (e.g. the audit pool gets exactly 10% of the budget).
//
// A nil *Budget is a no-op Waiter so callers can pass nil in tests.
type Budget struct {
	ingest *rate.Limiter
	audit  *rate.Limiter
}

// NewBudget returns a budget of maxRPS requests per second total, of which
// auditShare ∈ [0,1] is allocated to the audit pool and the remainder to
// the ingest pool. Bursts of roughly one second of capacity are allowed
// (a token-bucket gets a burst equal to its rate, in tokens per second).
func NewBudget(maxRPS float64, auditShare float64) (*Budget, error) {
	if maxRPS <= 0 {
		return nil, fmt.Errorf("maxRPS must be positive, got %f", maxRPS)
	}
	if math.IsNaN(auditShare) || auditShare < 0 || auditShare > 1 {
		return nil, fmt.Errorf("auditShare must be in [0,1], got %f", auditShare)
	}
	ingestRPS := maxRPS * (1 - auditShare)
	auditRPS := maxRPS * auditShare
	return &Budget{
		ingest: rate.NewLimiter(rate.Limit(ingestRPS), burstFor(ingestRPS)),
		audit:  rate.NewLimiter(rate.Limit(auditRPS), burstFor(auditRPS)),
	}, nil
}

func burstFor(rps float64) int {
	// Minimum burst of 1 token so the limiter isn't held at zero; round
	// up to at least one token per second of sustained rate.
	n := int(math.Ceil(rps))
	if n < 1 {
		return 1
	}
	return n
}

// WaitIngest blocks until one token is available from the ingest pool or
// ctx is done.
func (b *Budget) WaitIngest(ctx context.Context) error {
	if b == nil || b.ingest == nil {
		return nil
	}
	return b.ingest.Wait(ctx)
}

// WaitAudit blocks until one token is available from the audit pool or
// ctx is done.
func (b *Budget) WaitAudit(ctx context.Context) error {
	if b == nil || b.audit == nil {
		return nil
	}
	return b.audit.Wait(ctx)
}
