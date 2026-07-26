package rpc

import (
	"context"
	"sync/atomic"
)

// ErrorCounts holds per-method RPC error totals. Fields are updated
// atomically so the struct may be read from any goroutine while the
// CountingClient is in use.
type ErrorCounts struct {
	GetEvents        atomic.Uint64
	GetLatestLedger  atomic.Uint64
	GetHealth        atomic.Uint64
	GetLedgerEntries atomic.Uint64
}

// Snapshot returns a plain (non-atomic) copy of the current error counts,
// safe to marshal or compare in tests.
func (e *ErrorCounts) Snapshot() ErrorCountSnapshot {
	return ErrorCountSnapshot{
		GetEvents:        e.GetEvents.Load(),
		GetLatestLedger:  e.GetLatestLedger.Load(),
		GetHealth:        e.GetHealth.Load(),
		GetLedgerEntries: e.GetLedgerEntries.Load(),
	}
}

// ErrorCountSnapshot is a plain value copy of ErrorCounts, suitable for
// JSON marshaling and test assertions.
type ErrorCountSnapshot struct {
	GetEvents        uint64 `json:"getEvents"`
	GetLatestLedger  uint64 `json:"getLatestLedger"`
	GetHealth        uint64 `json:"getHealth"`
	GetLedgerEntries uint64 `json:"getLedgerEntries"`
}

// CountingClient wraps any Client and increments a per-method error counter
// whenever the underlying call returns a non-nil error.
type CountingClient struct {
	inner  Client
	errors ErrorCounts
}

var _ Client = (*CountingClient)(nil)

// NewCountingClient wraps inner and starts all error counters at zero.
func NewCountingClient(inner Client) *CountingClient {
	return &CountingClient{inner: inner}
}

// Errors returns a pointer to the live error counters. The caller may read
// individual fields with Load() or call Snapshot() for a value copy.
func (c *CountingClient) Errors() *ErrorCounts { return &c.errors }

func (c *CountingClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	resp, err := c.inner.GetEvents(ctx, req)
	if err != nil {
		c.errors.GetEvents.Add(1)
	}
	return resp, err
}

func (c *CountingClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	resp, err := c.inner.GetLatestLedger(ctx)
	if err != nil {
		c.errors.GetLatestLedger.Add(1)
	}
	return resp, err
}

func (c *CountingClient) GetHealth(ctx context.Context) (Health, error) {
	resp, err := c.inner.GetHealth(ctx)
	if err != nil {
		c.errors.GetHealth.Add(1)
	}
	return resp, err
}

func (c *CountingClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	resp, err := c.inner.GetLedgerEntries(ctx, req)
	if err != nil {
		c.errors.GetLedgerEntries.Add(1)
	}
	return resp, err
}
