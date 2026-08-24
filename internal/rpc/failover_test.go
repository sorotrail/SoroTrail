package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger returns a logger that discards output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// failoverMockClient is a controllable rpc.Client for failover tests.
type failoverMockClient struct {
	mu            sync.Mutex
	url           string
	getEventsResp []GetEventsResponse
	getEventsErr  []error
	getHealthResp []Health
	getHealthErr  []error
	callCount     atomic.Int32
}

func (m *failoverMockClient) GetEvents(_ context.Context, _ GetEventsRequest) (GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := int(m.callCount.Add(1) - 1)
	if idx < len(m.getEventsErr) && m.getEventsErr[idx] != nil {
		return GetEventsResponse{}, m.getEventsErr[idx]
	}
	if idx < len(m.getEventsResp) {
		return m.getEventsResp[idx], nil
	}
	return GetEventsResponse{LatestLedger: 100}, nil
}

func (m *failoverMockClient) GetLatestLedger(_ context.Context) (LatestLedger, error) {
	return LatestLedger{Sequence: 100}, nil
}

func (m *failoverMockClient) GetHealth(_ context.Context) (Health, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.getHealthErr) > 0 {
		err := m.getHealthErr[0]
		m.getHealthErr = m.getHealthErr[1:]
		return Health{}, err
	}
	if len(m.getHealthResp) > 0 {
		resp := m.getHealthResp[0]
		m.getHealthResp = m.getHealthResp[1:]
		return resp, nil
	}
	return Health{Status: "healthy", LatestLedger: 100, OldestLedger: 10}, nil
}

func (m *failoverMockClient) GetLedgerEntries(_ context.Context, _ GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	return GetLedgerEntriesResponse{}, nil
}

func (m *failoverMockClient) SimulateTransaction(_ context.Context, _ SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	return SimulateTransactionResponse{}, nil
}

// resetCallCount resets the call counter. Must only be called between test
// phases (not concurrently with GetEvents/GetHealth).
func (m *failoverMockClient) resetCallCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount.Store(0)
}

// newFailoverTestClient creates a FailoverClient backed by mockClients.
// Returns the failover client and the individual mocks for scripting.
func newFailoverTestClient(urls []string, opts ...FailoverOption) (*FailoverClient, []*failoverMockClient) {
	mocks := make([]*failoverMockClient, len(urls))
	newClient := func(url string, _ float64) Client {
		for i, u := range urls {
			if u == url {
				return mocks[i]
			}
		}
		panic("unexpected url: " + url)
	}
	for i, u := range urls {
		mocks[i] = &failoverMockClient{url: u}
	}
	fc := NewFailoverClient(urls, 10.0, newClient, opts...)
	return fc, mocks
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestFailover_PriorityOrder(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
	)

	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), resp.LatestLedger)
	assert.Equal(t, int32(1), mocks[0].callCount.Load(), "call went to provider 0")
	assert.Equal(t, int32(0), mocks[1].callCount.Load(), "provider 1 not called")
}

func TestFailover_DemotionAfterConsecutiveErrors(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// Provider 0 fails 3 times → demoted to degraded.
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}

	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// 4th call: provider 0 is degraded, provider 1 is active.
	// getActive() prefers active → call goes to provider 1.
	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), resp.LatestLedger)
	assert.Greater(t, mocks[1].callCount.Load(), int32(0), "failover to provider 1")
}

func TestFailover_SemanticErrorsDoNotDemote(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// IsLedgerOutOfRange errors must NOT count toward demotion.
	outOfRange := &Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}
	mocks[0].getEventsErr = []error{
		fmt.Errorf("getEvents: %w", outOfRange),
		fmt.Errorf("getEvents: %w", outOfRange),
		fmt.Errorf("getEvents: %w", outOfRange),
		fmt.Errorf("getEvents: %w", outOfRange),
		fmt.Errorf("getEvents: %w", outOfRange),
	}

	for i := 0; i < 5; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
		assert.True(t, IsLedgerOutOfRange(err), "call %d: expected IsLedgerOutOfRange", i)
	}
	// Provider should still be active — semantic errors don't demote.
	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()))
	assert.Equal(t, int32(0), fc.providers[0].errCount.Load())
}

func TestFailover_HTTP5xxDemotes(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	mocks[0].getEventsErr = []error{
		fmt.Errorf("getEvents returned HTTP 502: server error"),
		fmt.Errorf("getEvents returned HTTP 503: unavailable"),
		fmt.Errorf("getEvents returned HTTP 502: server error"),
	}

	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))
}

func TestFailover_PromotionAfterProbation(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
		WithFailoverProbationSuccesses(2),
	)

	// Script 3 network errors → demotion.
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}

	// Fail 3 times → demoted.
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// Reset mock to return successes for probation.
	mocks[0].getEventsErr = nil
	mocks[0].resetCallCount()

	// 2 successful calls → promoted back to active.
	for i := 0; i < 2; i++ {
		resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.NoError(t, err)
		assert.Equal(t, uint32(100), resp.LatestLedger)
	}
	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()),
		"provider should be promoted back to active after probation")
}

func TestFailover_CursorReanchorOnProviderSwitch(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// First call with cursor on provider 0 succeeds.
	mocks[0].getEventsResp = []GetEventsResponse{{LatestLedger: 100}}
	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{
		Pagination: &Pagination{Cursor: "cursor-from-p0"},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), resp.LatestLedger)
	assert.Equal(t, int32(0), fc.lastActive.Load())

	// Now script 3 network errors on provider 0. Reset call count so the
	// error slice starts from index 0.
	mocks[0].getEventsResp = nil
	mocks[0].resetCallCount()
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}

	// 3 ledger-based (no cursor) calls fail → demote provider 0.
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// Next call WITH cursor: provider switch detected (lastActive=0, new=1).
	// Should return ErrFailoverReanchor.
	_, err = fc.GetEvents(context.Background(), GetEventsRequest{
		Pagination: &Pagination{Cursor: "cursor-from-p0"},
	})
	require.Error(t, err)
	assert.True(t, IsFailoverReanchor(err), "expected ErrFailoverReanchor, got: %v", err)
}

func TestFailover_AllDownBackoff(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
	)

	// Manually set all providers to StateDown.
	for _, p := range fc.providers {
		p.state.Store(int32(StateDown))
	}

	_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAllProvidersDown), "expected ErrAllProvidersDown, got: %v", err)
}

func TestFailover_NoCursorNoReanchor(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// First call to provider 0 succeeds.
	mocks[0].getEventsResp = []GetEventsResponse{{LatestLedger: 100}}
	_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, int32(0), fc.lastActive.Load())

	// Provider 0 fails and gets demoted.
	mocks[0].getEventsResp = nil
	mocks[0].resetCallCount()
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// Next call WITHOUT cursor: goes to provider 1 (active).
	// Should NOT return ErrFailoverReanchor.
	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), resp.LatestLedger)
	assert.Equal(t, int32(1), fc.lastActive.Load())
}

func TestFailover_ErrorClassDiscrimination(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		demotable bool
	}{
		{"connection refused", fmt.Errorf("dial tcp: connection refused"), true},
		{"deadline exceeded", fmt.Errorf("context deadline exceeded"), true},
		{"HTTP 502", fmt.Errorf("getEvents returned HTTP 502: Bad Gateway"), true},
		{"HTTP 503", fmt.Errorf("getHealth returned HTTP 503: Service Unavailable"), true},
		{"rpc error -32000", &Error{Code: -32000, Message: "internal error"}, true},
		{"rpc error -32603", &Error{Code: -32603, Message: "internal error"}, true},
		{"ledger out of range -32600", &Error{Code: -32600, Message: "startLedger must be within the ledger range: 90 - 200"}, false},
		{"HTTP 400", fmt.Errorf("getEvents returned HTTP 400: bad request"), false},
		{"HTTP 404", fmt.Errorf("getEvents returned HTTP 404: not found"), false},
		{"rpc error -32602", &Error{Code: -32602, Message: "invalid params"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.demotable, isDemotableError(tt.err),
				"isDemotableError(%v) = %v, want %v", tt.err, isDemotableError(tt.err), tt.demotable)
		})
	}
}

func TestFailover_ProviderStates(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"rpc0", "rpc1"},
		WithFailoverLogger(testLogger()),
	)

	// Set provider 1 to down.
	fc.providers[1].state.Store(int32(StateDown))

	states := fc.ProviderStates()
	require.Len(t, states, 2)
	assert.Equal(t, "rpc0", states[0].URL)
	assert.Equal(t, StateActive, states[0].State)
	assert.Equal(t, "rpc1", states[1].URL)
	assert.Equal(t, StateDown, states[1].State)
}

func TestFailover_DegradedToDownTransition(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// Script 10 errors (network errors) so we can push through degraded → down.
	mocks[0].getEventsErr = make([]error, 10)
	for i := range mocks[0].getEventsErr {
		mocks[0].getEventsErr[i] = fmt.Errorf("connection refused")
	}

	// 3 errors → degraded.
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// 3 more errors → down (threshold is 2x maxConsecutiveErrors = 6 total).
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDown, ProviderState(fc.providers[0].state.Load()))
}

func TestFailover_FirstCallNoReanchor(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
	)

	// Very first call with cursor: lastActive is -1, so no re-anchor.
	_, err := fc.GetEvents(context.Background(), GetEventsRequest{
		Pagination: &Pagination{Cursor: "some-cursor"},
	})
	require.NoError(t, err)
}

func TestFailover_ProbesPromoteDownProvider(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
		WithFailoverProbationSuccesses(2),
	)

	// Set provider to down.
	fc.providers[0].state.Store(int32(StateDown))

	// First probe — still down (need 2 for promotion).
	fc.probeDownProviders(context.Background())
	assert.Equal(t, StateDown, ProviderState(fc.providers[0].state.Load()),
		"still down after 1 probe")

	// Second probe — promoted.
	fc.probeDownProviders(context.Background())
	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()),
		"promoted after 2 successful probes")
}

func TestFailover_NewFailoverClient(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"https://rpc1.example.com", "https://rpc2.example.com"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(5),
		WithFailoverProbationSuccesses(3),
		WithFailoverProbeInterval(15*time.Second),
		WithFailoverHeadSkew(5),
	)

	require.Len(t, fc.providers, 2)
	assert.Equal(t, 5, fc.maxConsecutiveErrors)
	assert.Equal(t, 3, fc.probationSuccesses)
	assert.Equal(t, 15*time.Second, fc.probeInterval)
	assert.Equal(t, uint32(5), fc.headSkewTolerance)

	states := fc.ProviderStates()
	assert.Equal(t, "https://rpc1.example.com", states[0].URL)
	assert.Equal(t, StateActive, states[0].State)
	assert.Equal(t, "https://rpc2.example.com", states[1].URL)
	assert.Equal(t, StateActive, states[1].State)

	// Verify the client works.
	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), resp.LatestLedger)
}

func TestFailover_ContextCancellation(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"rpc0"},
		WithFailoverLogger(testLogger()),
	)

	// All providers down → backoff, but context is already cancelled.
	for _, p := range fc.providers {
		p.state.Store(int32(StateDown))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrAllProvidersDown),
		"expected context.Canceled or ErrAllProvidersDown, got: %v", err)
}

func TestProviderState_String(t *testing.T) {
	assert.Equal(t, "active", StateActive.String())
	assert.Equal(t, "degraded", StateDegraded.String())
	assert.Equal(t, "down", StateDown.String())
	assert.Equal(t, "unknown", ProviderState(99).String())
}

func TestIsDemotableError_NonError(t *testing.T) {
	assert.False(t, isDemotableError(nil))
}

func TestIsDemotableError_EOF(t *testing.T) {
	assert.True(t, isDemotableError(fmt.Errorf("EOF")))
}

func TestIsDemotableError_HTTP4xxNotDemotable(t *testing.T) {
	assert.False(t, isDemotableError(fmt.Errorf("getEvents returned HTTP 400: bad request")))
	assert.False(t, isDemotableError(fmt.Errorf("getEvents returned HTTP 403: forbidden")))
}

func TestIsFailoverReanchor(t *testing.T) {
	assert.True(t, IsFailoverReanchor(ErrFailoverReanchor))
	assert.True(t, IsFailoverReanchor(fmt.Errorf("wrapped: %w", ErrFailoverReanchor)))
	assert.False(t, IsFailoverReanchor(errors.New("some other error")))
}

func TestErrAllProvidersDown(t *testing.T) {
	assert.True(t, errors.Is(ErrAllProvidersDown, ErrAllProvidersDown))
	assert.True(t, errors.Is(fmt.Errorf("wrapped: %w", ErrAllProvidersDown), ErrAllProvidersDown))
}
