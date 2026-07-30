package rpc

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient is a minimal rpc.Client for testing the breaker wrapper.
type mockClient struct {
	getEventsFn    func(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error)
	getHealthFn    func(ctx context.Context) (Health, error)
	getLedgerFn    func(ctx context.Context) (LatestLedger, error)
	getEntriesFn   func(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error)
	callCount      atomic.Int64
}

func (m *mockClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	m.callCount.Add(1)
	if m.getEventsFn != nil {
		return m.getEventsFn(ctx, req)
	}
	return GetEventsResponse{}, nil
}
func (m *mockClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	m.callCount.Add(1)
	if m.getLedgerFn != nil {
		return m.getLedgerFn(ctx)
	}
	return LatestLedger{}, nil
}
func (m *mockClient) GetHealth(ctx context.Context) (Health, error) {
	m.callCount.Add(1)
	if m.getHealthFn != nil {
		return m.getHealthFn(ctx)
	}
	return Health{}, nil
}
func (m *mockClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	m.callCount.Add(1)
	if m.getEntriesFn != nil {
		return m.getEntriesFn(ctx, req)
	}
	return GetLedgerEntriesResponse{}, nil
}

var _ Client = (*mockClient)(nil)

// silentLogger returns a logger that discards output during tests.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func TestCircuitBreaker_ClosedPassesThrough(t *testing.T) {
	inner := &mockClient{}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ProbeTimeout:     time.Hour,
	}, silentLogger())

	c := NewCircuitBreakerClient(inner, cb)

	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), resp.LatestLedger)
	assert.Equal(t, int64(1), inner.callCount.Load())
	assert.Equal(t, BreakerClosed, cb.State())
}

func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	inner := &mockClient{
		getEventsFn: func(_ context.Context, _ GetEventsRequest) (GetEventsResponse, error) {
			return GetEventsResponse{}, errors.New("connection refused")
		},
	}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ProbeTimeout:     time.Hour,
	}, silentLogger())

	c := NewCircuitBreakerClient(inner, cb)
	ctx := context.Background()

	// First 3 calls fail and increment the counter.
	for i := 0; i < 3; i++ {
		_, err := c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, BreakerOpen, cb.State())
	assert.Equal(t, 3, cb.ConsecutiveFailures())

	// 4th call is rejected without hitting the network.
	_, err := c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCircuitOpen)
	assert.Equal(t, int64(3), inner.callCount.Load(), "no extra RPC call after breaker opened")
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	var fails atomic.Int32
	inner := &mockClient{
		getEventsFn: func(_ context.Context, _ GetEventsRequest) (GetEventsResponse, error) {
			if fails.Load() > 0 {
				return GetEventsResponse{}, errors.New("still down")
			}
			return GetEventsResponse{LatestLedger: 42}, nil
		},
	}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		ProbeTimeout:     50 * time.Millisecond, // short for test
	}, silentLogger())

	c := NewCircuitBreakerClient(inner, cb)
	ctx := context.Background()

	// Trip the breaker.
	fails.Store(1)
	for i := 0; i < 2; i++ {
		_, _ = c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	}
	assert.Equal(t, BreakerOpen, cb.State())

	// Reject while open (probe timeout not yet elapsed).
	_, err := c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	assert.ErrorIs(t, err, ErrCircuitOpen)

	// Wait for probe timeout.
	time.Sleep(60 * time.Millisecond)

	// Probe succeeds -> breaker closes.
	fails.Store(0)
	resp, err := c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(42), resp.LatestLedger)
	assert.Equal(t, BreakerClosed, cb.State())
	assert.Equal(t, 0, cb.ConsecutiveFailures())
}

func TestCircuitBreaker_HalfOpenProbeFailure(t *testing.T) {
	inner := &mockClient{
		getEventsFn: func(_ context.Context, _ GetEventsRequest) (GetEventsResponse, error) {
			return GetEventsResponse{}, errors.New("still broken")
		},
	}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		ProbeTimeout:     50 * time.Millisecond,
	}, silentLogger())

	c := NewCircuitBreakerClient(inner, cb)
	ctx := context.Background()

	// Trip the breaker.
	for i := 0; i < 2; i++ {
		_, _ = c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	}
	assert.Equal(t, BreakerOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	// Probe fails -> reopens.
	_, _ = c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	assert.Equal(t, BreakerOpen, cb.State())
	assert.Equal(t, 3, cb.ConsecutiveFailures(), "failure count should increment on probe failure")
}

func TestCircuitBreaker_NilIsPassthrough(t *testing.T) {
	inner := &mockClient{}
	c := NewCircuitBreakerClient(inner, nil)

	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), resp.LatestLedger)
	assert.Equal(t, int64(1), inner.callCount.Load())

	h, err := c.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), inner.callCount.Load())
	_ = h
}

func TestCircuitBreaker_AllMethodsWrapped(t *testing.T) {
	inner := &mockClient{}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 100, // won't trip
		ProbeTimeout:     time.Hour,
	}, silentLogger())
	c := NewCircuitBreakerClient(inner, cb)
	ctx := context.Background()

	_, _ = c.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	_, _ = c.GetLatestLedger(ctx)
	_, _ = c.GetHealth(ctx)
	_, _ = c.GetLedgerEntries(ctx, GetLedgerEntriesRequest{Keys: []string{"abc"}})

	assert.Equal(t, int64(4), inner.callCount.Load())
}

func TestCircuitBreaker_GetHealthRecordsOutcome(t *testing.T) {
	callCount := 0
	inner := &mockClient{
		getHealthFn: func(_ context.Context) (Health, error) {
			callCount++
			if callCount <= 2 {
				return Health{}, errors.New("timeout")
			}
			return Health{Status: "ok"}, nil
		},
	}
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		ProbeTimeout:     50 * time.Millisecond,
	}, silentLogger())
	c := NewCircuitBreakerClient(inner, cb)
	ctx := context.Background()

	// Trip on GetHealth.
	for i := 0; i < 2; i++ {
		_, _ = c.GetHealth(ctx)
	}
	assert.Equal(t, BreakerOpen, cb.State())

	// Probe via GetHealth.
	time.Sleep(60 * time.Millisecond)
	_, err := c.GetHealth(ctx)
	require.NoError(t, err)
	assert.Equal(t, BreakerClosed, cb.State())
}

func TestCircuitBreaker_Stats(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		ProbeTimeout:     30 * time.Second,
	}, silentLogger())

	stats := cb.Stats()
	assert.Equal(t, BreakerClosed, stats.State)
	assert.Equal(t, 0, stats.ConsecutiveFailures)
	assert.Equal(t, 5, stats.FailureThreshold)
	assert.Equal(t, 30*time.Second, stats.ProbeTimeout)
}

func TestCircuitBreaker_DefaultsApplied(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{}, silentLogger())
	stats := cb.Stats()
	assert.Equal(t, 5, stats.FailureThreshold, "default failure threshold")
	assert.Equal(t, 30*time.Second, stats.ProbeTimeout, "default probe timeout")
}

func TestBreakerState_String(t *testing.T) {
	assert.Equal(t, "closed", BreakerClosed.String())
	assert.Equal(t, "open", BreakerOpen.String())
	assert.Equal(t, "half-open", BreakerHalfOpen.String())
	assert.Equal(t, "unknown", BreakerState(99).String())
}
