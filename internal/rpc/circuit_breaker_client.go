package rpc

import (
	"context"
	"errors"
)

// CircuitBreakerClient wraps an rpc.Client with a circuit breaker that
// stops calling the RPC after too many consecutive failures. When the
// breaker is open all requests are rejected immediately without hitting
// the network. After ProbeTimeout the breaker transitions to half-open
// and allows a single probe request to test recovery.
//
// This prevents tight retry loops during sustained RPC outages while
// still recovering automatically once the RPC is healthy again.
type CircuitBreakerClient struct {
	inner   Client
	breaker *CircuitBreaker
}

// NewCircuitBreakerClient wraps inner with the given circuit breaker.
// If breaker is nil, requests pass through unmodified.
func NewCircuitBreakerClient(inner Client, breaker *CircuitBreaker) *CircuitBreakerClient {
	return &CircuitBreakerClient{inner: inner, breaker: breaker}
}

// Breaker returns the underlying circuit breaker for inspection.
func (c *CircuitBreakerClient) Breaker() *CircuitBreaker {
	return c.breaker
}

func (c *CircuitBreakerClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	if c.breaker != nil && !c.breaker.Allow() {
		return GetEventsResponse{}, ErrCircuitOpen
	}
	resp, err := c.inner.GetEvents(ctx, req)
	if c.breaker != nil {
		if err != nil {
			c.breaker.RecordFailure()
		} else {
			c.breaker.RecordSuccess()
		}
	}
	return resp, err
}

func (c *CircuitBreakerClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	if c.breaker != nil && !c.breaker.Allow() {
		return LatestLedger{}, ErrCircuitOpen
	}
	resp, err := c.inner.GetLatestLedger(ctx)
	if c.breaker != nil {
		if err != nil {
			c.breaker.RecordFailure()
		} else {
			c.breaker.RecordSuccess()
		}
	}
	return resp, err
}

func (c *CircuitBreakerClient) GetHealth(ctx context.Context) (Health, error) {
	if c.breaker != nil && !c.breaker.Allow() {
		return Health{}, ErrCircuitOpen
	}
	resp, err := c.inner.GetHealth(ctx)
	if c.breaker != nil {
		if err != nil {
			c.breaker.RecordFailure()
		} else {
			c.breaker.RecordSuccess()
		}
	}
	return resp, err
}

func (c *CircuitBreakerClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	if c.breaker != nil && !c.breaker.Allow() {
		return GetLedgerEntriesResponse{}, ErrCircuitOpen
	}
	resp, err := c.inner.GetLedgerEntries(ctx, req)
	if c.breaker != nil {
		if err != nil {
			c.breaker.RecordFailure()
		} else {
			c.breaker.RecordSuccess()
		}
	}
	return resp, err
}

// ErrCircuitOpen is returned when the circuit breaker is open and
// rejecting requests. It is a plain sentinel error (not a JSON-RPC
// *Error) so callers can use errors.Is to detect it.
var ErrCircuitOpen = errors.New("circuit breaker open: RPC endpoint unhealthy, retry after probe timeout")
