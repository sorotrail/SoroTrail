package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCircuitBreakerClient_BreakerAccessor pins the inspection hook operators
// use to read breaker state. It must hand back the same instance that is
// recording outcomes — a copy or a lazily created breaker would report a state
// no request ever affected — and a client built without one must stay
// passthrough rather than conjuring a default.
func TestCircuitBreakerClient_BreakerAccessor(t *testing.T) {
	t.Run("returns the wired breaker", func(t *testing.T) {
		breaker := NewCircuitBreaker(CircuitBreakerConfig{
			FailureThreshold: 1,
			ProbeTimeout:     time.Second,
		}, silentLogger())
		mock := &routingMock{url: "https://a.example", failErr: errors.New("calling getHealth: EOF")}
		c := NewCircuitBreakerClient(mock, breaker)

		require.Same(t, breaker, c.Breaker())

		// The accessor reaches the live object: driving a failure through the
		// client must be visible on the returned breaker.
		_, err := c.GetHealth(context.Background())
		require.Error(t, err)
		assert.False(t, c.Breaker().Allow(), "one recorded failure must open the breaker at threshold 1")
	})

	t.Run("nil breaker stays nil", func(t *testing.T) {
		c := NewCircuitBreakerClient(&routingMock{url: "https://a.example"}, nil)
		assert.Nil(t, c.Breaker(), "a passthrough client must not report a breaker that does not exist")

		mock := &routingMock{url: "https://a.example", failErr: errors.New("boom")}
		c = NewCircuitBreakerClient(mock, nil)
		_, err := c.GetHealth(context.Background())
		require.Error(t, err, "the inner error surfaces unchanged with no breaker")
		assert.Equal(t, []string{"getHealth"}, mock.methodCalls())
	})
}

// TestCountingClient_SimulateTransactionIsNotCounted pins the documented
// exception in the error counters: simulation is a spec/metadata lookup path,
// so its failures must not be read as ingestion health. A counter that did
// include it would make a contract with a trapping method look like an RPC
// outage on the stats endpoint.
func TestCountingClient_SimulateTransactionIsNotCounted(t *testing.T) {
	mock := &routingMock{url: "https://a.example", failErr: errors.New("simulateTransaction: contract truncated")}
	c := NewCountingClient(mock)

	_, err := c.SimulateTransaction(context.Background(), SimulateTransactionRequest{Transaction: "AAAA"})
	require.Error(t, err, "the error must still reach the caller")

	assert.Equal(t, []string{"simulateTransaction"}, mock.methodCalls(), "it must pass through to the inner client")
	snap := c.Errors().Snapshot()
	assert.Zero(t, snap.GetEvents)
	assert.Zero(t, snap.GetLatestLedger)
	assert.Zero(t, snap.GetHealth)
	assert.Zero(t, snap.GetLedgerEntries, "no counter may move for a simulation failure")

	// The same client does count the ingestion-path methods, which is what
	// makes the simulation exception meaningful rather than an accident.
	_, err = c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.Error(t, err)
	assert.Equal(t, uint64(1), c.Errors().Snapshot().GetEvents)
}

// TestCountingClient_SnapshotIsIndependentOfLiveCounters checks that Snapshot
// hands back plain values: a caller that keeps the snapshot (for a stats
// response, say) must not see it mutate as more failures arrive.
func TestCountingClient_SnapshotIsIndependentOfLiveCounters(t *testing.T) {
	mock := &routingMock{url: "https://a.example", failErr: errors.New("boom")}
	c := NewCountingClient(mock)

	_, _ = c.GetHealth(context.Background())
	snap := c.Errors().Snapshot()
	assert.Equal(t, uint64(1), snap.GetHealth)

	_, _ = c.GetHealth(context.Background())
	assert.Equal(t, uint64(1), snap.GetHealth, "the earlier snapshot must be a copy, not a view")
	assert.Equal(t, uint64(2), c.Errors().Snapshot().GetHealth)

	// Errors() returns the live struct, so it does track subsequent calls.
	live := c.Errors()
	_, _ = c.GetLatestLedger(context.Background())
	assert.Equal(t, uint64(1), live.GetLatestLedger.Load())
}
