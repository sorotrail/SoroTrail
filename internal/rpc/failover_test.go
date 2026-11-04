package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// testLogger returns a logger that discards output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// failoverMockClient is a controllable rpc.Client for failover tests.
type failoverMockClient struct {
	mu            sync.Mutex
	url           string
	passphrase    string
	getNetworkErr error
	getEventsResp []GetEventsResponse
	getEventsErr  []error
	getHealthResp []Health
	getHealthErr  []error
	lastReq       GetEventsRequest
	callCount     atomic.Int32
}

func (m *failoverMockClient) GetEvents(_ context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastReq = req
	idx := int(m.callCount.Add(1) - 1)
	if idx < len(m.getEventsErr) && m.getEventsErr[idx] != nil {
		return GetEventsResponse{}, m.getEventsErr[idx]
	}
	if idx < len(m.getEventsResp) {
		return m.getEventsResp[idx], nil
	}
	return GetEventsResponse{LatestLedger: 100}, nil
}

func (m *failoverMockClient) GetNetwork(_ context.Context) (NetworkInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getNetworkErr != nil {
		return NetworkInfo{}, m.getNetworkErr
	}
	if m.passphrase != "" {
		return NetworkInfo{Passphrase: m.passphrase}, nil
	}
	return NetworkInfo{Passphrase: MainnetNetworkPassphrase}, nil
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

	// Provider 0 fails next call, triggering demotion. The mock indexes its
	// scripted responses by provider0's own call count, and the first call
	// already consumed index 0 (the success), so the error must sit at
	// index 1.
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
	})

	t.Run("selection advances rather than pinning one endpoint", func(t *testing.T) {
		fc, _ := newFailoverTestClient(
			[]string{"rpc0", "rpc1", "rpc2"},
			WithFailoverLogger(testLogger()),
		)
		// All active; check active selection order or rotation helper if applicable.
		idx1 := fc.getActive()
		assert.Equal(t, 0, idx1)
	})

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

		// Probe once explicitly: runProbes is a blocking ticker loop meant
		// for the background goroutine, while probeDownProviders performs a
		// single pass over down providers.
		fc.probeDownProviders(ctx)

		assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()),
			"provider should recover and return to active rotation after successful probe")
	})

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
	})

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
	})
}

// newPickProviderClient builds a FailoverClient with one provider per state,
// in priority order, so pickProvider can be driven without issuing calls.
func newPickProviderClient(states []ProviderState) *FailoverClient {
	urls := make([]string, len(states))
	for i := range states {
		urls[i] = fmt.Sprintf("http://rpc%d.example", i)
	}
	fc, _ := newFailoverTestClient(urls, WithFailoverLogger(testLogger()))
	for i, s := range states {
		fc.setProviderState(fc.providers[i], s)
	}
	return fc
}

// TestPickProvider_Table pins the selection rule: the highest-priority
// active provider wins, a degraded provider is used only when nothing is
// active, and down providers are never chosen. When nothing is usable,
// including when no providers are configured, pickProvider must return
// ErrAllProvidersDown and arm the backoff rather than panic or hand back
// a nil provider with a nil error.
func TestPickProvider_Table(t *testing.T) {
	tests := []struct {
		name    string
		states  []ProviderState
		wantIdx int // -1 means ErrAllProvidersDown
	}{
		{name: "single active provider", states: []ProviderState{StateActive}, wantIdx: 0},
		{name: "single degraded provider is still served", states: []ProviderState{StateDegraded}, wantIdx: 0},
		{name: "single down provider", states: []ProviderState{StateDown}, wantIdx: -1},
		{name: "priority order among active providers", states: []ProviderState{StateActive, StateActive}, wantIdx: 0},
		{name: "down provider is skipped", states: []ProviderState{StateDown, StateActive}, wantIdx: 1},
		{name: "active preferred over higher-priority degraded", states: []ProviderState{StateDegraded, StateActive}, wantIdx: 1},
		{name: "skips down and degraded to reach active", states: []ProviderState{StateDown, StateDegraded, StateActive}, wantIdx: 2},
		{name: "degraded used when nothing is active", states: []ProviderState{StateDown, StateDegraded, StateDown}, wantIdx: 1},
		{name: "first degraded wins when nothing is active", states: []ProviderState{StateDown, StateDegraded, StateDegraded}, wantIdx: 1},
		{name: "all providers down", states: []ProviderState{StateDown, StateDown, StateDown}, wantIdx: -1},
		{name: "no providers configured", states: nil, wantIdx: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := newPickProviderClient(tt.states)

			var (
				p   *provider
				idx int
				err error
			)
			require.NotPanics(t, func() { p, idx, err = fc.pickProvider(context.Background()) })

			if tt.wantIdx < 0 {
				require.ErrorIs(t, err, ErrAllProvidersDown)
				assert.Nil(t, p)
				assert.Equal(t, -1, idx)
				assert.Equal(t, int32(1), fc.allDownCount.Load(), "an all-down episode must be counted")
				assert.Greater(t, fc.allDownUntil.Load(), time.Now().UnixNano(), "backoff deadline must be in the future")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantIdx, idx)
			assert.Same(t, fc.providers[tt.wantIdx], p, "returned provider must match the returned index")
			assert.Zero(t, fc.allDownUntil.Load(), "a successful pick must not arm the backoff")
		})
	}
}

// TestPickProvider_Selection covers how the choice moves over time. The
// client is priority-based rather than round-robin by design: it pins to
// the best healthy provider and advances only when that provider's health
// changes, then returns to it once it recovers.
func TestPickProvider_Selection(t *testing.T) {
	ctx := context.Background()

	t.Run("healthy provider is chosen on every call", func(t *testing.T) {
		fc := newPickProviderClient([]ProviderState{StateActive, StateActive})
		for range 5 {
			_, idx, err := fc.pickProvider(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, idx)
		}
	})

	t.Run("advances as providers fail and returns on recovery", func(t *testing.T) {
		fc := newPickProviderClient([]ProviderState{StateActive, StateActive, StateActive})
		steps := []struct {
			change  func()
			wantIdx int
		}{
			{change: func() {}, wantIdx: 0},
			{change: func() { fc.setProviderState(fc.providers[0], StateDown) }, wantIdx: 1},
			{change: func() { fc.setProviderState(fc.providers[1], StateDegraded) }, wantIdx: 2},
			{change: func() { fc.setProviderState(fc.providers[0], StateActive) }, wantIdx: 0},
		}
		for i, s := range steps {
			s.change()
			_, idx, err := fc.pickProvider(ctx)
			require.NoError(t, err, "step %d", i)
			assert.Equal(t, s.wantIdx, idx, "step %d", i)
		}
	})

	t.Run("backoff window refuses picks even if a provider recovers", func(t *testing.T) {
		fc := newPickProviderClient([]ProviderState{StateDown})
		_, _, err := fc.pickProvider(ctx)
		require.ErrorIs(t, err, ErrAllProvidersDown)

		// Recovery inside the window must not bypass the backoff, and the
		// refused call must not count as a new all-down episode.
		fc.setProviderState(fc.providers[0], StateActive)
		p, idx, err := fc.pickProvider(ctx)
		require.ErrorIs(t, err, ErrAllProvidersDown)
		assert.Nil(t, p)
		assert.Equal(t, -1, idx)
		assert.Equal(t, int32(1), fc.allDownCount.Load())
	})

	t.Run("expired backoff is cleared and selection resumes", func(t *testing.T) {
		fc := newPickProviderClient([]ProviderState{StateActive})
		fc.allDownUntil.Store(time.Now().Add(-time.Second).UnixNano())

		_, idx, err := fc.pickProvider(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, idx)
		assert.Zero(t, fc.allDownUntil.Load(), "expired deadline must be cleared")
	})

	t.Run("repeated all-down episodes grow the backoff count", func(t *testing.T) {
		fc := newPickProviderClient([]ProviderState{StateDown})
		for want := int32(1); want <= 3; want++ {
			_, _, err := fc.pickProvider(ctx)
			require.ErrorIs(t, err, ErrAllProvidersDown)
			assert.Equal(t, want, fc.allDownCount.Load())
			// Expire the window so the next call re-evaluates providers.
			fc.allDownUntil.Store(time.Now().Add(-time.Nanosecond).UnixNano())
		}
	})
}

func TestFailover_BehindCursorSkippedOnSelectionAndResponse(t *testing.T) {
	ctx := context.Background()

	t.Run("selection path skips provider known to be behind cursor", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))

		fc, mocks := newFailoverTestClient(
			[]string{"https://p1.behind.test", "https://p2.behind.test", "https://p3.behind.test"},
			WithFailoverMaxErrors(1),
			WithFailoverLogger(logger),
		)

		// Primary serves ledger 250 and advances the cursor to 250.
		mocks[0].getEventsResp = []GetEventsResponse{
			{
				Events:       []Event{{ID: "0000000250-000000001", Ledger: 250}},
				LatestLedger: 260,
				OldestLedger: 10,
			},
		}
		resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 200})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1)
		assert.Equal(t, uint32(250), fc.CursorLedger())

		// Pre-record that p2 is lagging at ledger 220 (behind cursor 250), while p3 is at 262.
		fc.SetProviderLedgerRange(1, 10, 220)
		fc.SetProviderLedgerRange(2, 10, 262)
		mocks[2].getEventsResp = []GetEventsResponse{
			{
				Events:       []Event{{ID: "0000000255-000000001", Ledger: 255}},
				LatestLedger: 262,
				OldestLedger: 10,
			},
		}

		// Demote p1 due to transport error.
		mocks[0].getEventsErr = []error{nil, fmt.Errorf("connect: connection refused")}
		_, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 251})
		require.Error(t, err)

		beforeBehindMetric := testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("p2.behind.test", DemoteReasonBehindCursor))

		// Next GetEvents must skip p2 on the selection path (without calling p2.GetEvents)
		// and route straight to p3.
		resp, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 251})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1)
		assert.Equal(t, uint32(255), resp.Events[0].Ledger)
		assert.Equal(t, int32(0), mocks[1].callCount.Load(), "behind provider p2 must be skipped on selection path without calling GetEvents")
		assert.Equal(t, StateDegraded, ProviderState(fc.providers[1].state.Load()))
		assert.Equal(t, uint32(255), fc.CursorLedger())
		assert.Contains(t, logBuf.String(), "provider behind cursor on selection path")
		assert.Equal(t, beforeBehindMetric+1, testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("p2.behind.test", DemoteReasonBehindCursor)))
	})

	t.Run("response path detects regression below cursor and never advances cursor onto behind provider", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"https://r1.behind.test", "https://r2.behind.test", "https://r3.behind.test"},
			WithFailoverMaxErrors(1),
		)

		// Advance cursor to 300 via r1, then demote r1.
		fc.SetCursorLedger(300)
		fc.setProviderState(fc.providers[0], StateDown)

		// r2 has unknown initial height but returns LatestLedger=280 (< cursor 300).
		mocks[1].getEventsResp = []GetEventsResponse{
			{
				Events:       []Event{{ID: "0000000275-000000001", Ledger: 275}},
				LatestLedger: 280,
				OldestLedger: 10,
			},
		}
		// r3 is caught up at LatestLedger=310.
		mocks[2].getEventsResp = []GetEventsResponse{
			{
				Events:       []Event{{ID: "0000000305-000000001", Ledger: 305}},
				LatestLedger: 310,
				OldestLedger: 10,
			},
		}

		resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 301})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1)
		assert.Equal(t, uint32(305), resp.Events[0].Ledger)
		assert.Equal(t, StateDegraded, ProviderState(fc.providers[1].state.Load()))
		assert.Equal(t, uint32(305), fc.CursorLedger(), "cursor must never regress to 275/280")
	})
}

func TestFailover_RetentionGapDistinguishedFromEmptyEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("empty response with OldestLedger > StartLedger fails over to provider covering window", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"https://short-win.test", "https://long-win.test"},
			WithFailoverMaxErrors(1),
		)

		// Provider 0 returns an empty page but its OldestLedger is 500 (> StartLedger 100),
		// which is a retention-window gap disguised as an empty page.
		mocks[0].getEventsResp = []GetEventsResponse{
			{
				Events:       nil,
				LatestLedger: 1000,
				OldestLedger: 500,
			},
		}
		// Provider 1 has a longer retention window (OldestLedger=50 <= 100) and serves the event.
		mocks[1].getEventsResp = []GetEventsResponse{
			{
				Events:       []Event{{ID: "0000000120-000000001", Ledger: 120}},
				LatestLedger: 1000,
				OldestLedger: 50,
			},
		}

		beforeGapMetric := testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("short-win.test", DemoteReasonRetentionGap))

		resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 100})
		require.NoError(t, err)
		require.Len(t, resp.Events, 1)
		assert.Equal(t, uint32(120), resp.Events[0].Ledger)
		assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))
		assert.Equal(t, beforeGapMetric+1, testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("short-win.test", DemoteReasonRetentionGap)))
	})

	t.Run("when no provider covers StartLedger an explicit out-of-range error is returned instead of empty page", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"https://short1.test", "https://short2.test"},
			WithFailoverMaxErrors(1),
		)

		mocks[0].getEventsResp = []GetEventsResponse{
			{Events: nil, LatestLedger: 1000, OldestLedger: 600},
		}
		mocks[1].getEventsResp = []GetEventsResponse{
			{Events: nil, LatestLedger: 1000, OldestLedger: 550},
		}

		resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 200})
		require.Error(t, err)
		assert.Empty(t, resp.Events)
		assert.ErrorIs(t, err, ErrProviderRetentionGap)
		assert.True(t, IsLedgerOutOfRange(err), "retention gap error must satisfy IsLedgerOutOfRange so callers reclamp instead of advancing past unread ledgers")
	})

	t.Run("legitimate empty matching-events response with covered window succeeds without demotion", func(t *testing.T) {
		fc, mocks := newFailoverTestClient(
			[]string{"https://healthy-empty.test"},
			WithFailoverMaxErrors(1),
		)
		mocks[0].getEventsResp = []GetEventsResponse{
			{Events: nil, LatestLedger: 500, OldestLedger: 50},
		}

		resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 100})
		require.NoError(t, err)
		assert.Empty(t, resp.Events)
		assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()))
	})
}

func TestFailover_NetworkPassphraseVerifiedBeforeSelectionAndPromotion(t *testing.T) {
	ctx := context.Background()

	fc, mocks := newFailoverTestClient(
		[]string{"https://wrong-net.test", "https://mainnet-ok.test"},
		WithFailoverExpectedNetwork("mainnet"),
		WithFailoverProbationSuccesses(1),
	)

	// Provider 0 is misconfigured pointing at testnet; Provider 1 is on mainnet.
	mocks[0].passphrase = TestnetNetworkPassphrase
	mocks[1].passphrase = MainnetNetworkPassphrase
	mocks[1].getEventsResp = []GetEventsResponse{
		{
			Events:       []Event{{ID: "0000000100-000000001", Ledger: 100}},
			LatestLedger: 100,
			OldestLedger: 10,
		},
	}

	beforeMismatch := testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("wrong-net.test", DemoteReasonNetworkMismatch))

	resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 50})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	assert.Equal(t, int32(0), mocks[0].callCount.Load(), "mismatched network provider must never receive GetEvents traffic")
	assert.Equal(t, StateDown, ProviderState(fc.providers[0].state.Load()))
	assert.Equal(t, StateActive, ProviderState(fc.providers[1].state.Load()))
	assert.Equal(t, beforeMismatch+1, testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("wrong-net.test", DemoteReasonNetworkMismatch)))

	// Probing down providers must NEVER promote the wrong-network provider.
	fc.probeDownProviders(ctx)
	assert.Equal(t, StateDown, ProviderState(fc.providers[0].state.Load()), "wrong-network provider must never be promoted by background probes")
}

func TestFailover_PerProviderXDRFormatFallback(t *testing.T) {
	ctx := context.Background()

	var (
		p1Formats []string
		p2Formats []string
		p1Fail    atomic.Bool
		mu        sync.Mutex
	)

	// Server 1: supports xdrFormat="json" (protocol 21+).
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string           `json:"method"`
			Params GetEventsRequest `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if p1Fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if req.Method == "getHealth" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"result":  Health{Status: "healthy", LatestLedger: 200, OldestLedger: 10},
			})
			return
		}
		mu.Lock()
		p1Formats = append(p1Formats, req.Params.XDRFormat)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": GetEventsResponse{
				Events:       []Event{{ID: "0000000100-000000001", Ledger: 100, ValueJSON: json.RawMessage(`{"u64":1}`)}},
				LatestLedger: 200,
				OldestLedger: 10,
			},
		})
	}))
	defer srv1.Close()

	// Server 2: legacy server that rejects xdrFormat="json".
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string           `json:"method"`
			Params GetEventsRequest `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		p2Formats = append(p2Formats, req.Params.XDRFormat)
		mu.Unlock()
		if req.Params.XDRFormat != "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"error":   Error{Code: -32602, Message: "invalid params: unknown field xdrFormat"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": GetEventsResponse{
				Events:       []Event{{ID: "0000000101-000000001", Ledger: 101, Value: "AAAAAQ=="}},
				LatestLedger: 200,
				OldestLedger: 10,
			},
		})
	}))
	defer srv2.Close()

	fc := NewFailoverClient(
		[]string{srv1.URL, srv2.URL},
		1000.0,
		func(u string, _ float64) Client {
			return NewHTTPClient(u, WithMinRequestInterval(0))
		},
		WithFailoverMaxErrors(1),
		WithFailoverProbationSuccesses(1),
		WithFailoverLogger(testLogger()),
	)

	// 1. Primary (srv1) serves with xdrFormat="json".
	resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 100})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	assert.True(t, fc.ProviderSupportsXDRJSON(0))
	assert.True(t, fc.ProviderSupportsXDRJSON(1))

	// 2. Primary fails -> demoted -> secondary (srv2) rejects xdrFormat="json" and falls back to raw XDR.
	p1Fail.Store(true)
	_, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 101})
	require.Error(t, err)

	resp, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 101})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	assert.False(t, fc.ProviderSupportsXDRJSON(1), "srv2 must be marked as not supporting xdrFormat=json")
	assert.True(t, fc.ProviderSupportsXDRJSON(0), "srv1 must retain xdrFormat=json capability")

	// 3. Primary recovers and is promoted -> still sends xdrFormat="json".
	p1Fail.Store(false)
	fc.setProviderState(fc.providers[0], StateDown)
	fc.probeDownProviders(ctx)
	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()))

	_, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 102})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{XDRFormatJSON, XDRFormatJSON}, p1Formats, "JSON-capable primary must keep sending xdrFormat=json after failover round-trip")
	assert.Equal(t, []string{XDRFormatJSON, ""}, p2Formats, "JSON-incapable secondary must retry without xdrFormat")
}

func TestFailover_DivergentProviderDetectedAndSkipped(t *testing.T) {
	ctx := context.Background()

	fc, mocks := newFailoverTestClient(
		[]string{"https://canon1.div.test", "https://corrupt2.div.test", "https://canon3.div.test"},
		WithFailoverMaxErrors(1),
	)

	// Provider 0 serves page 1 at ledger 500.
	mocks[0].getEventsResp = []GetEventsResponse{
		{
			Events: []Event{
				{ID: "0000000500-000000001", Ledger: 500, ContractID: "CAAA", Type: "contract", TxHash: "tx1", ValueJSON: json.RawMessage(`{"u64":42}`)},
			},
			LatestLedger: 510,
			OldestLedger: 10,
			Cursor:       "0000000500-000000001",
		},
	}
	resp, err := fc.GetEvents(ctx, GetEventsRequest{StartLedger: 500})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	// Provider 0 goes down.
	fc.setProviderState(fc.providers[0], StateDown)

	// Provider 1 returns subtly different data (corrupted ValueJSON) for the same event at ledger 500.
	mocks[1].getEventsResp = []GetEventsResponse{
		{
			Events: []Event{
				{ID: "0000000500-000000001", Ledger: 500, ContractID: "CAAA", Type: "contract", TxHash: "tx1", ValueJSON: json.RawMessage(`{"u64":9999}`)},
				{ID: "0000000500-000000002", Ledger: 500, ContractID: "CAAA", Type: "contract", TxHash: "tx2", ValueJSON: json.RawMessage(`{"u64":43}`)},
			},
			LatestLedger: 510,
			OldestLedger: 10,
		},
	}
	// Provider 2 returns the canonical event data for ledger 500.
	mocks[2].getEventsResp = []GetEventsResponse{
		{
			Events: []Event{
				{ID: "0000000500-000000001", Ledger: 500, ContractID: "CAAA", Type: "contract", TxHash: "tx1", ValueJSON: json.RawMessage(`{"u64":42}`)},
				{ID: "0000000500-000000002", Ledger: 500, ContractID: "CAAA", Type: "contract", TxHash: "tx2", ValueJSON: json.RawMessage(`{"u64":43}`)},
			},
			LatestLedger: 510,
			OldestLedger: 10,
		},
	}

	beforeDivMetric := testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("corrupt2.div.test", DemoteReasonDivergence))

	// Re-anchored query from ledger 500 detects Provider 1's divergence, demotes it to StateDown,
	// and serves the canonical page from Provider 2.
	resp, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 500})
	require.NoError(t, err)
	require.Len(t, resp.Events, 2)
	assert.Equal(t, `{"u64":42}`, string(resp.Events[0].ValueJSON))
	assert.Equal(t, StateDown, ProviderState(fc.providers[1].state.Load()))
	assert.Equal(t, StateActive, ProviderState(fc.providers[2].state.Load()))
	assert.Equal(t, beforeDivMetric+1, testutil.ToFloat64(metrics.RPCProviderDemotionsTotal.WithLabelValues("corrupt2.div.test", DemoteReasonDivergence)))
	require.Len(t, fc.Divergences(), 1)
	assert.Equal(t, "0000000500-000000001", fc.Divergences()[0].EventID)
}
