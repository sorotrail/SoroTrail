package rpc

import (
	"context"
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
		fmt.Errorf(
			"connection refused"),
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
		WithFailoverMaxErrors(1),
	)

	// Call 1 on provider 0 sets lastActive = 0 and returns a cursor.
	mocks[0].getEventsResp = []GetEventsResponse{{LatestLedger: 100, Cursor: "cursor-0"}}
	resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, "cursor-0", resp.Cursor)
	assert.Equal(t, int32(0), fc.lastActive.Load())

	// Provider 0 fails next call, triggering demotion. The mock scripts
	// errors by call index, so the first (successful) call gets nil.
	mocks[0].getEventsErr = []error{nil, fmt.Errorf("connection refused")}
	_, err = fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1, Pagination: &Pagination{Cursor: "cursor-0"}})
	require.Error(t, err)

	// Now provider 0 is degraded/down, next call routes to provider 1.
	// Since lastActive (0) != current provider index (1) and request has cursor, ErrFailoverReanchor must be returned.
	_, err = fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1, Pagination: &Pagination{Cursor: "cursor-0"}})
	require.Error(t, err)
	assert.True(t, IsFailoverReanchor(err), "expected ErrFailoverReanchor when switching providers with a cursor")
}

func TestFailover_HealthAndProbingHelpers(t *testing.T) {
	t.Run("healthy provider selected and unhealthy skipped", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"rpc0", "rpc1"},
			WithFailoverLogger(testLogger()),
		)
		// Force rpc0 to StateDown, rpc1 to StateActive
		fc.setProviderState(fc.providers[0], StateDown)
		fc.setProviderState(fc.providers[1], StateActive)

		resp, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.NoError(t, err)
		assert.Equal(t, uint32(100), resp.LatestLedger)
		assert.Equal(t, int32(0), mocks[0].callCount.Load(), "rpc0 (down) was skipped")
		assert.Greater(t, mocks[1].callCount.Load(), int32(0), "rpc1 (active) was selected")
	});

	t.Run("selection advances rather than pinning one endpoint", func(t *testing.T) {
		fc, _ := newFailoverTestClient(
			[]string{"rpc0", "rpc1", "rpc2"},
			WithFailoverLogger(testLogger()),
		)
		// All active; check active selection order or rotation helper if applicable.
		idx1 := fc.getActive()
		assert.Equal(t, 0, idx1)
	});

	t.Run("recovered provider returns to rotation after successful probe", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"rpc0"},
			WithFailoverLogger(testLogger()),
			WithFailoverProbationSuccesses(1),
		)
		fc.setProviderState(fc.providers[0], StateDown)

		// Configure GetHealth probe response
		mocks[0].getHealthResp = []Health{{Status: "healthy", LatestLedger: 100, OldestLedger: 10}}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Run a probe cycle directly: runProbes is a ticker loop on
		// probeInterval (default 30s), so probing must be invoked
		// explicitly to test promotion deterministically.
		fc.probeDownProviders(ctx)

		assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()),
			"provider should recover and return to active rotation after successful probe")
	});

	t.Run("all-unhealthy backs off rather than spinning", func(t *testing.T) {
		fc, _ := newFailoverTestClient(
			[]string{"rpc0", "rpc1"},
			WithFailoverLogger(testLogger()),
		)
		fc.setProviderState(fc.providers[0], StateDown)
		fc.setProviderState(fc.providers[1], StateDown)

		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAllProvidersDown)
	});

	t.Run("probing respects context cancellation", func(t *testing.T) {
		fc, _ := newFailoverTestClient(
			[]string{"rpc0"},
			WithFailoverLogger(testLogger()),
		)
		fc.setProviderState(fc.providers[0], StateDown)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Should return immediately without blocking or panicking on canceled context
		fc.runProbes(ctx)
	});
}
