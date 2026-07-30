package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClient is a minimal Client implementation for unit tests.
// Each method returns the configured error (or nil) and zero-value responses.
type stubClient struct {
	errGetEvents        error
	errGetLatestLedger  error
	errGetHealth        error
	errGetLedgerEntries error
}

func (s *stubClient) GetEvents(_ context.Context, _ GetEventsRequest) (GetEventsResponse, error) {
	return GetEventsResponse{}, s.errGetEvents
}
func (s *stubClient) GetLatestLedger(_ context.Context) (LatestLedger, error) {
	return LatestLedger{}, s.errGetLatestLedger
}
func (s *stubClient) GetHealth(_ context.Context) (Health, error) {
	return Health{}, s.errGetHealth
}
func (s *stubClient) GetLedgerEntries(_ context.Context, _ GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	return GetLedgerEntriesResponse{}, s.errGetLedgerEntries
}

func TestCountingClient_CountsErrorsByMethod(t *testing.T) {
	sentinel := errors.New("rpc failure")

	tests := []struct {
		name string
		stub *stubClient
		call func(c *CountingClient) error
		want ErrorCountSnapshot
	}{
		{
			name: "GetEvents error increments only GetEvents",
			stub: &stubClient{errGetEvents: sentinel},
			call: func(c *CountingClient) error {
				_, err := c.GetEvents(context.Background(), GetEventsRequest{})
				return err
			},
			want: ErrorCountSnapshot{GetEvents: 1},
		},
		{
			name: "GetLatestLedger error increments only GetLatestLedger",
			stub: &stubClient{errGetLatestLedger: sentinel},
			call: func(c *CountingClient) error {
				_, err := c.GetLatestLedger(context.Background())
				return err
			},
			want: ErrorCountSnapshot{GetLatestLedger: 1},
		},
		{
			name: "GetHealth error increments only GetHealth",
			stub: &stubClient{errGetHealth: sentinel},
			call: func(c *CountingClient) error {
				_, err := c.GetHealth(context.Background())
				return err
			},
			want: ErrorCountSnapshot{GetHealth: 1},
		},
		{
			name: "GetLedgerEntries error increments only GetLedgerEntries",
			stub: &stubClient{errGetLedgerEntries: sentinel},
			call: func(c *CountingClient) error {
				_, err := c.GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{})
				return err
			},
			want: ErrorCountSnapshot{GetLedgerEntries: 1},
		},
		{
			name: "success does not increment any counter",
			stub: &stubClient{},
			call: func(c *CountingClient) error {
				_, err := c.GetEvents(context.Background(), GetEventsRequest{})
				return err
			},
			want: ErrorCountSnapshot{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCountingClient(tc.stub)
			_ = tc.call(c)
			assert.Equal(t, tc.want, c.Errors().Snapshot())
		})
	}
}

func TestCountingClient_AccumulatesMultipleErrors(t *testing.T) {
	sentinel := errors.New("rpc failure")
	stub := &stubClient{errGetEvents: sentinel}
	c := NewCountingClient(stub)

	for i := 0; i < 5; i++ {
		_, err := c.GetEvents(context.Background(), GetEventsRequest{})
		require.Error(t, err)
	}

	snap := c.Errors().Snapshot()
	assert.Equal(t, uint64(5), snap.GetEvents)
	assert.Zero(t, snap.GetLatestLedger)
	assert.Zero(t, snap.GetHealth)
	assert.Zero(t, snap.GetLedgerEntries)
}

func TestCountingClient_ErrorNotSwallowed(t *testing.T) {
	sentinel := errors.New("rpc failure")
	stub := &stubClient{
		errGetEvents:        sentinel,
		errGetLatestLedger:  sentinel,
		errGetHealth:        sentinel,
		errGetLedgerEntries: sentinel,
	}
	c := NewCountingClient(stub)

	_, err := c.GetEvents(context.Background(), GetEventsRequest{})
	require.ErrorIs(t, err, sentinel)

	_, err = c.GetLatestLedger(context.Background())
	require.ErrorIs(t, err, sentinel)

	_, err = c.GetHealth(context.Background())
	require.ErrorIs(t, err, sentinel)

	_, err = c.GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{})
	require.ErrorIs(t, err, sentinel)
}

func TestCountingClient_ImplementsClientInterface(t *testing.T) {
	// Compile-time assertion that CountingClient satisfies the Client interface.
	var _ Client = (*CountingClient)(nil)
}

func TestErrorCountSnapshot(t *testing.T) {
	c := NewCountingClient(&stubClient{errGetEvents: errors.New("fail")})

	_, _ = c.GetEvents(context.Background(), GetEventsRequest{})
	_, _ = c.GetEvents(context.Background(), GetEventsRequest{})

	snap := c.Errors().Snapshot()
	assert.Equal(t, uint64(2), snap.GetEvents)
	assert.Zero(t, snap.GetLatestLedger)
}
