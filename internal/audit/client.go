package audit

import (
	"context"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// Client is the audit-side view of the RPC. It is the same interface as
// rpc.Client (so the auditor can swap in a fake in tests) but every
// outbound call gates through rpc.Budget.WaitAudit so audit traffic
// receives at most its configured share of the total request budget.
type Client interface {
	GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error)
	GetLatestLedger(ctx context.Context) (rpc.LatestLedger, error)
	GetHealth(ctx context.Context) (rpc.Health, error)
	GetLedgerEntries(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error)
}

// budgetedClient wraps an inner rpc.Client, accounting every call against
// Budget.WaitAudit before dispatching.
type budgetedClient struct {
	inner  rpc.Client
	budget *rpc.Budget
}

// NewBudgetedClient returns an audit-scoped Client that shares the same
// underlying connection as inner but reserves tokens from the audit pool
// of b on every call. A nil b is permitted (the audit becomes un-paced;
// useful only in tests).
func NewBudgetedClient(inner rpc.Client, b *rpc.Budget) Client {
	return &budgetedClient{inner: inner, budget: b}
}

func (c *budgetedClient) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	if err := c.budget.WaitAudit(ctx); err != nil {
		return rpc.GetEventsResponse{}, err
	}
	return c.inner.GetEvents(ctx, req)
}

func (c *budgetedClient) GetLatestLedger(ctx context.Context) (rpc.LatestLedger, error) {
	if err := c.budget.WaitAudit(ctx); err != nil {
		return rpc.LatestLedger{}, err
	}
	return c.inner.GetLatestLedger(ctx)
}

func (c *budgetedClient) GetHealth(ctx context.Context) (rpc.Health, error) {
	if err := c.budget.WaitAudit(ctx); err != nil {
		return rpc.Health{}, err
	}
	return c.inner.GetHealth(ctx)
}

func (c *budgetedClient) GetLedgerEntries(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	if err := c.budget.WaitAudit(ctx); err != nil {
		return rpc.GetLedgerEntriesResponse{}, err
	}
	return c.inner.GetLedgerEntries(ctx, req)
}

// Compile-time check that we satisfy the audit Client interface.
var _ Client = (*budgetedClient)(nil)
