package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// countingMockClient satisfies rpc.Client for testing CountingClient.
type countingMockClient struct {
	errorToReturn error
}

func (m *countingMockClient) GetEvents(context.Context, GetEventsRequest) (GetEventsResponse, error) {
	return GetEventsResponse{}, m.errorToReturn
}
func (m *countingMockClient) GetLatestLedger(context.Context) (LatestLedger, error) {
	return LatestLedger{}, m.errorToReturn
}
func (m *countingMockClient) GetHealth(context.Context) (Health, error) {
	return Health{}, m.errorToReturn
}
func (m *countingMockClient) GetLedgerEntries(context.Context, GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	return GetLedgerEntriesResponse{}, m.errorToReturn
}
func (m *countingMockClient) SimulateTransaction(context.Context, SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	return SimulateTransactionResponse{}, m.errorToReturn
}

func TestCountingClient_Accounting(t *testing.T) {
	m := &countingMockClient{}
	c := NewCountingClient(m)
	ctx := context.Background()

	// Test error increment for each method
	m.errorToReturn = errors.New("failed")
	_, _ = c.GetEvents(ctx, GetEventsRequest{})
	_, _ = c.GetLatestLedger(ctx)
	_, _ = c.GetHealth(ctx)
	_, _ = c.GetLedgerEntries(ctx, GetLedgerEntriesRequest{})

	snap := c.Errors().Snapshot()
	assert.Equal(t, uint64(1), snap.GetEvents)
	assert.Equal(t, uint64(1), snap.GetLatestLedger)
	assert.Equal(t, uint64(1), snap.GetHealth)
	assert.Equal(t, uint64(1), snap.GetLedgerEntries)

	// Test success does not increment
	m.errorToReturn = nil
	_, _ = c.GetEvents(ctx, GetEventsRequest{})
	snap = c.Errors().Snapshot()
	assert.Equal(t, uint64(1), snap.GetEvents, "success should not increment counter")
}

func TestCountingClient_ConcurrentRace(t *testing.T) {
	m := &countingMockClient{errorToReturn: errors.New("fail")}
	c := NewCountingClient(m)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = c.GetEvents(ctx, GetEventsRequest{})
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(n), c.Errors().GetEvents.Load())
}
