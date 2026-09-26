package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// routingMock records the method names it receives so a test can assert which
// provider handled a call, not merely that some provider answered. It is
// separate from failoverMockClient because that mock only counts GetEvents
// and GetHealth calls.
type routingMock struct {
	url string

	mu      sync.Mutex
	calls   []string
	failErr error
}

func newRoutingMock(url string) *routingMock { return &routingMock{url: url} }

func (m *routingMock) record(method string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, method)
	return m.failErr
}

func (m *routingMock) methodCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

func (m *routingMock) GetEvents(context.Context, GetEventsRequest) (GetEventsResponse, error) {
	if err := m.record("getEvents"); err != nil {
		return GetEventsResponse{}, err
	}
	return GetEventsResponse{LatestLedger: 11}, nil
}

func (m *routingMock) GetLatestLedger(context.Context) (LatestLedger, error) {
	if err := m.record("getLatestLedger"); err != nil {
		return LatestLedger{}, err
	}
	return LatestLedger{Sequence: 12}, nil
}

func (m *routingMock) GetHealth(context.Context) (Health, error) {
	if err := m.record("getHealth"); err != nil {
		return Health{}, err
	}
	return Health{Status: "healthy"}, nil
}

func (m *routingMock) GetLedgerEntries(context.Context, GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	if err := m.record("getLedgerEntries"); err != nil {
		return GetLedgerEntriesResponse{}, err
	}
	return GetLedgerEntriesResponse{Entries: []LedgerEntryResult{{Key: "K"}}}, nil
}

func (m *routingMock) SimulateTransaction(context.Context, SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	if err := m.record("simulateTransaction"); err != nil {
		return SimulateTransactionResponse{}, err
	}
	return SimulateTransactionResponse{TransactionData: "TD"}, nil
}

// allMethods enumerates the Client surface so a table can drive every one of
// them without repeating the wiring five times.
var allMethods = []struct {
	name   string
	method string
	invoke func(Client) error
}{
	{
		name:   "GetEvents",
		method: "getEvents",
		invoke: func(c Client) error {
			_, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
			return err
		},
	},
	{
		name:   "GetLatestLedger",
		method: "getLatestLedger",
		invoke: func(c Client) error {
			_, err := c.GetLatestLedger(context.Background())
			return err
		},
	},
	{
		name:   "GetHealth",
		method: "getHealth",
		invoke: func(c Client) error {
			_, err := c.GetHealth(context.Background())
			return err
		},
	},
	{
		name:   "GetLedgerEntries",
		method: "getLedgerEntries",
		invoke: func(c Client) error {
			_, err := c.GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{Keys: []string{"K"}})
			return err
		},
	},
	{
		name:   "SimulateTransaction",
		method: "simulateTransaction",
		invoke: func(c Client) error {
			_, err := c.SimulateTransaction(context.Background(), SimulateTransactionRequest{Transaction: "T"})
			return err
		},
	},
}

func newRoutingFailover(urls []string, opts ...FailoverOption) (*FailoverClient, []*routingMock) {
	mocks := make([]*routingMock, len(urls))
	for i, u := range urls {
		mocks[i] = newRoutingMock(u)
	}
	byURL := make(map[string]*routingMock, len(urls))
	for i, u := range urls {
		byURL[u] = mocks[i]
	}
	fc := NewFailoverClient(urls, 1000.0, func(url string, _ float64) Client { return byURL[url] }, opts...)
	return fc, mocks
}

// TestFailoverClient_RoutesEveryMethodThroughOneProvider pins the decorator
// invariant that matters most for correctness: routing decisions are made in
// one place, so every Client method gets the same priority treatment and the
// same health bookkeeping. A method that bypassed pickProvider would keep
// hammering a provider the rest of the client had already demoted.
func TestFailoverClient_RoutesEveryMethodThroughOneProvider(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example", "https://b.example"},
		WithFailoverLogger(testLogger()))

	for _, m := range allMethods {
		t.Run(m.name, func(t *testing.T) {
			require.NoError(t, m.invoke(fc))
			assert.Equal(t, []string{m.method}, mocks[0].methodCalls(),
				"the highest-priority active provider must serve %s", m.name)
			assert.Empty(t, mocks[1].methodCalls(), "%s must not reach a provider already served", m.name)
			mocks[0].mu.Lock()
			mocks[0].calls = nil
			mocks[0].mu.Unlock()
		})
	}

	// Every successful call records the same provider as the active one, so
	// a mid-pagination cursor stays valid across methods.
	assert.Equal(t, int32(0), fc.lastActive.Load())
}

// TestFailoverClient_FailuresDemoteTheProviderForEveryMethod checks that
// health is a property of the client, not of one call: a 5xx seen through any
// single method must degrade the provider so every other method sees the same
// verdict.
func TestFailoverClient_FailuresDemoteTheProviderForEveryMethod(t *testing.T) {
	fiveOhThree := errors.New("getEvents returned HTTP 503: upstream unavailable")

	for _, m := range allMethods {
		t.Run(m.name, func(t *testing.T) {
			fc, mocks := newRoutingFailover([]string{"https://a.example"},
				WithFailoverLogger(testLogger()), WithFailoverMaxErrors(1))
			mocks[0].failErr = fiveOhThree

			err := m.invoke(fc)
			require.Error(t, err, "the failure must still reach the caller")
			assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()),
				"a 5xx through %s must degrade the provider for every method", m.name)

			// With the only provider degraded, later calls must still be
			// served by it rather than refused — degraded is a demotion, not
			// an ejection.
			mocks[0].failErr = nil
			for _, other := range allMethods {
				require.NoError(t, other.invoke(fc), "%s must keep serving while degraded", other.name)
			}
		})
	}
}

// TestFailoverClient_AllDownBackoffCoversEveryMethod proves the backoff is a
// property of the client, not of one method: during an all-down episode all
// five methods must refuse work with ErrAllProvidersDown without dialling.
func TestFailoverClient_AllDownBackoffCoversEveryMethod(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example", "https://b.example"},
		WithFailoverLogger(testLogger()))
	for _, p := range fc.providers {
		fc.setProviderState(p, StateDown)
	}

	for _, m := range allMethods {
		t.Run(m.name, func(t *testing.T) {
			err := m.invoke(fc)
			require.ErrorIs(t, err, ErrAllProvidersDown)
			assert.Empty(t, mocks[0].methodCalls(), "%s must not dial a down provider", m.name)
			assert.Empty(t, mocks[1].methodCalls())
		})
	}
	assert.Equal(t, int32(1), fc.allDownCount.Load(),
		"the five methods are one episode, so the backoff counter must not advance per method")
}

// TestNewFailoverClient_Options pins the constructor defaults and each option,
// plus the derived per-provider state every later decision depends on.
func TestNewFailoverClient_Options(t *testing.T) {
	t.Run("documented defaults", func(t *testing.T) {
		fc, _ := newRoutingFailover([]string{"https://a.example"}, WithFailoverLogger(testLogger()))
		assert.Equal(t, 3, fc.maxConsecutiveErrors)
		assert.Equal(t, 2, fc.probationSuccesses)
		assert.Equal(t, 30*time.Second, fc.probeInterval)
		assert.Equal(t, uint32(3), fc.headSkewTolerance)
		assert.Equal(t, float64(1000.0), fc.rateLimitRPS)
		assert.Equal(t, int32(-1), fc.lastActive.Load(), "no provider has served anything yet")

		require.Len(t, fc.providers, 1)
		assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()))
		assert.Equal(t, "a.example", fc.providers[0].label)
	})

	t.Run("every option is applied", func(t *testing.T) {
		log := testLogger()
		fc, _ := newRoutingFailover([]string{"https://a.example", "https://b.example"},
			WithFailoverMaxErrors(7),
			WithFailoverProbationSuccesses(4),
			WithFailoverProbeInterval(90*time.Second),
			WithFailoverHeadSkew(11),
			WithFailoverLogger(log),
		)
		assert.Equal(t, 7, fc.maxConsecutiveErrors)
		assert.Equal(t, 4, fc.probationSuccesses)
		assert.Equal(t, 90*time.Second, fc.probeInterval)
		assert.Equal(t, uint32(11), fc.headSkewTolerance)
		assert.Same(t, log, fc.log)
	})

	t.Run("priority order is the argument order", func(t *testing.T) {
		urls := []string{"https://third.example", "https://first.example", "https://second.example"}
		fc, mocks := newRoutingFailover(urls)
		require.Len(t, fc.providers, 3)
		for i, u := range urls {
			assert.Equal(t, u, fc.providers[i].url, "provider %d must keep its declared priority", i)
			assert.Same(t, mocks[i], fc.providers[i].client)
		}
	})

	t.Run("each provider gets its own burst-sized bucket", func(t *testing.T) {
		// A fractional RPS still yields a whole-token burst, and a very low
		// RPS is floored at one token so a provider is never unreachable.
		fc, _ := newRoutingFailover([]string{"https://a.example", "https://b.example"}, WithFailoverLogger(testLogger()))
		require.Len(t, fc.providers, 2)
		for _, p := range fc.providers {
			require.NotNil(t, p.limiter)
			assert.Equal(t, rate.Limit(1000.0), p.limiter.Limit())
			assert.Equal(t, 1000, p.limiter.Burst(), "burst is one second of capacity, rounded up")
		}

		slow := NewFailoverClient([]string{"https://c.example"}, 0.4,
			func(url string, _ float64) Client { return newRoutingMock(url) },
			WithFailoverLogger(testLogger()))
		assert.Equal(t, 1, slow.providers[0].limiter.Burst(),
			"a sub-token rate must still allow one in-flight request")
	})

	t.Run("an empty provider list is usable, not a panic", func(t *testing.T) {
		fc, _ := newRoutingFailover(nil, WithFailoverLogger(testLogger()))
		assert.Empty(t, fc.providers)
		assert.Empty(t, fc.ProviderStates())
		require.ErrorIs(t, allMethods[0].invoke(fc), ErrAllProvidersDown)
	})
}

// TestProviderWaitLimiter covers the per-provider gate in front of every
// delegated call: a provider without a limiter must not block, and a drained
// one must unblock on context cancellation rather than spin.
func TestProviderWaitLimiter(t *testing.T) {
	t.Run("nil limiter never blocks", func(t *testing.T) {
		p := &provider{url: "https://a.example"}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.NoError(t, p.waitLimiter(ctx), "a provider with no bucket is unlimited by contract")
	})

	t.Run("canceled context unblocks a drained bucket", func(t *testing.T) {
		limiter := rate.NewLimiter(rate.Limit(0.001), 1)
		p := &provider{url: "https://a.example", limiter: limiter}
		ctx := context.Background()

		require.NoError(t, p.waitLimiter(ctx), "the burst token is available")

		dead, cancel := context.WithCancel(context.Background())
		cancel()
		assert.Error(t, p.waitLimiter(dead), "the second token is a year away")
	})
}

// TestFailoverClient_RunProbesPromotesADownProvider exercises the background
// recovery path end to end: only StateDown providers are probed, and
// probationSuccesses successful probes are needed before one returns to
// rotation.
func TestFailoverClient_RunProbesPromotesADownProvider(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example", "https://b.example"},
		WithFailoverLogger(testLogger()),
		WithFailoverProbeInterval(10*time.Millisecond),
		WithFailoverProbationSuccesses(2),
	)
	fc.setProviderState(fc.providers[0], StateDown)
	fc.setProviderState(fc.providers[1], StateDown)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc.RunProbes(ctx)

	require.Eventually(t, func() bool {
		return ProviderState(fc.providers[0].state.Load()) == StateActive &&
			ProviderState(fc.providers[1].state.Load()) == StateActive
	}, 2*time.Second, 5*time.Millisecond, "probes must promote recovered providers back into rotation")

	// Recovery traffic must be health checks only: a probe that used a real
	// method would spend the rate budget the demotion was meant to protect.
	calls := append(mocks[0].methodCalls(), mocks[1].methodCalls()...)
	require.NotEmpty(t, calls)
	for _, c := range calls {
		assert.Equal(t, "getHealth", c, "probing must use getHealth only")
	}
}

// TestFailoverClient_RunProbesStopsWithContext proves the goroutine RunProbes
// starts is really tied to its context: a provider that never recovers keeps
// being probed while the context lives and is left alone once it ends.
func TestFailoverClient_RunProbesStopsWithContext(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example"},
		WithFailoverLogger(testLogger()),
		WithFailoverProbeInterval(10*time.Millisecond),
		WithFailoverProbationSuccesses(1000), // never promotes: it stays down
	)
	fc.setProviderState(fc.providers[0], StateDown)
	mocks[0].failErr = errors.New("getHealth returned HTTP 500: still down")

	ctx, cancel := context.WithCancel(context.Background())
	fc.RunProbes(ctx)

	require.Eventually(t, func() bool {
		return len(mocks[0].methodCalls()) >= 3
	}, 2*time.Second, 5*time.Millisecond, "a down provider must be probed repeatedly")

	cancel()
	time.Sleep(50 * time.Millisecond) // let any in-flight tick finish
	before := len(mocks[0].methodCalls())
	time.Sleep(120 * time.Millisecond)
	assert.Equal(t, before, len(mocks[0].methodCalls()),
		"the probe goroutine must exit with its context instead of probing forever")
}

// TestFailoverClient_ProbingIgnoresHealthyProviders keeps the passive-first
// promise: an active provider must never receive a getHealth probe, so the
// rate budget stays available for real work.
func TestFailoverClient_ProbingIgnoresHealthyProviders(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example"},
		WithFailoverLogger(testLogger()),
		WithFailoverProbeInterval(5*time.Millisecond),
	)

	fc.probeDownProviders(context.Background())
	assert.Empty(t, mocks[0].methodCalls(), "an active provider must not be probed")

	// A failed probe must not promote and must not deepen the state either:
	// a down provider stays down until it answers getHealth.
	fc.setProviderState(fc.providers[0], StateDown)
	mocks[0].failErr = errors.New("getHealth returned HTTP 503: still down")
	fc.probeDownProviders(context.Background())

	assert.Equal(t, StateDown, ProviderState(fc.providers[0].state.Load()))
	assert.Zero(t, fc.providers[0].okCount.Load(), "a failed probe must reset probation progress")
}

// TestNewHTTPClientForFailover checks the constructor the deployment wires up
// for each provider URL: rate conversion to a minimum interval, with a floor
// so a high RPS cannot produce a zero or negative spacing.
func TestNewHTTPClientForFailover(t *testing.T) {
	t.Run("interval derives from rps", func(t *testing.T) {
		c, ok := NewHTTPClientForFailover("https://rpc.example/srpc", 10).(*HTTPClient)
		require.True(t, ok, "the failover constructor must return the concrete client type")
		assert.Equal(t, "https://rpc.example/srpc", c.url)
		require.NotNil(t, c.limiter)
		assert.Equal(t, 100*time.Millisecond, c.limiter.interval, "10 req/s is 100ms of spacing")

		fast, ok := NewHTTPClientForFailover("https://rpc.example", 50).(*HTTPClient)
		require.True(t, ok)
		assert.Equal(t, 20*time.Millisecond, fast.limiter.interval)
	})

	t.Run("spacing is floored at one millisecond", func(t *testing.T) {
		c, ok := NewHTTPClientForFailover("https://rpc.example", 5_000_000).(*HTTPClient)
		require.True(t, ok)
		assert.Equal(t, time.Millisecond, c.limiter.interval,
			"a sub-millisecond interval would round to zero and disable pacing entirely")
	})

	t.Run("the client can talk to its url", func(t *testing.T) {
		rs := resultServer(t, `{"status":"healthy","latestLedger":1}`)
		c, ok := NewHTTPClientForFailover(rs.srv.URL, 100).(*HTTPClient)
		require.True(t, ok)
		_, err := c.GetHealth(context.Background())
		require.NoError(t, err)
	})
}

// TestEvent_CursorValue pins the resume-cursor rule. The two fields are the
// same TOID-derived string on protocol 22+ servers, but older servers only
// fill pagingToken while newer ones also expose a top-level cursor, so a
// caller that reads the wrong one resumes at the wrong position and either
// replays or skips events.
func TestEvent_CursorValue(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want string
	}{
		{
			name: "the legacy paging token wins",
			ev:   Event{ID: "0000000007-0000000001", PagingToken: "PT-7-1"},
			want: "PT-7-1",
		},
		{
			name: "the event id is the fallback",
			ev:   Event{ID: "0000000007-0000000001"},
			want: "0000000007-0000000001",
		},
		{
			name: "an event with neither yields no cursor",
			ev:   Event{Ledger: 7},
			want: "",
		},
		{
			// A server that sends both fields must not change the value used
			// mid-pagination; this is the same shape as the first row but
			// with an id that differs, which is what a bug would surface.
			name: "token preferred even when the ids differ",
			ev:   Event{ID: "0000000008-0000000000", PagingToken: "0000000007-0000000099"},
			want: "0000000007-0000000099",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.ev.CursorValue())
		})
	}
}

// TestIsDemotableError_Classification is the mirror of the retry table: which
// failures are allowed to move traffic away from a provider. A misclassification
// here is a production incident in either direction — 4xx responses would
// thrash a healthy provider, while an unclassified 5xx would keep sending
// traffic to a broken one.
func TestIsDemotableError_Classification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "http 500", err: errors.New("getEvents returned HTTP 500: boom"), want: true},
		{name: "http 502 from a proxy", err: errors.New("getHealth returned HTTP 502: bad gateway"), want: true},
		{name: "http 503 with an empty body", err: errors.New("getLatestLedger returned HTTP 503: "), want: true},
		{name: "connection refused", err: errors.New("calling getEvents: dial tcp: connect: connection refused"), want: true},
		{name: "timeout", err: errors.New("calling getEvents: context deadline exceeded"), want: true},
		{name: "eof", err: errors.New("reading getEvents response: EOF"), want: true},
		{name: "tls handshake timeout", err: errors.New("TLS handshake timeout"), want: true},

		// JSON-RPC server-side codes: -32000..-32099 plus -32603.
		{name: "json-rpc server error range", err: errors.New("rpc error -32001: internal"), want: true},
		{name: "json-rpc internal error", err: errors.New("rpc error -32603: internal error"), want: true},

		// Client-side codes must never demote: the provider is healthy, the
		// request is wrong.
		{name: "json-rpc invalid request", err: errors.New("rpc error -32600: Invalid Request"), want: false},
		{name: "json-rpc method not found", err: errors.New("rpc error -32601: Method not found"), want: false},
		{name: "json-rpc invalid params", err: errors.New("rpc error -32602: Invalid params"), want: false},
		{name: "json-rpc parse error", err: errors.New("rpc error -32700: Parse error"), want: false},

		// HTTP 4xx must not demote either.
		{name: "http 400", err: errors.New("getEvents returned HTTP 400: bad request"), want: false},
		{name: "http 404", err: errors.New("getEvents returned HTTP 404: not found"), want: false},
		{name: "http 429", err: &RateLimitedError{StatusCode: 429}, want: false},

		// A ledger-range refusal is rendered with a -320xx code, so the
		// predicate does classify it as demotable. Protection comes from
		// recordError, which short-circuits on IsLedgerOutOfRange before
		// ever reaching this check — asserted in
		// TestFailoverClient_SemanticErrorsDoNotCount. Pinned as true here
		// because a change to either half of that pairing has to break one
		// of the two tests.
		{
			name: "ledger out of range is demotable by the predicate alone",
			err:  &Error{Code: -32004, Message: "startLedger must be within the ledger range"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDemotableError(tt.err))
		})
	}
}

// TestFailoverClient_SemanticErrorsDoNotCount verifies the other half of that
// classification at the bookkeeping level: an out-of-range error must leave
// the consecutive-error counter untouched, so a re-clamp does not degrade a
// provider that answered perfectly.
func TestFailoverClient_SemanticErrorsDoNotCount(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example"},
		WithFailoverLogger(testLogger()), WithFailoverMaxErrors(1))
	outOfRange := &Error{Code: -32004, Message: "requested ledger is outside of retention window"}
	mocks[0].failErr = outOfRange

	for range 5 {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
		assert.True(t, IsLedgerOutOfRange(err))
	}

	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()))
	assert.Zero(t, fc.providers[0].errCount.Load(), "semantic errors must not accumulate")
}

// TestFailoverClient_RateLimitedIsNotDemotable pins that a 429 is treated as
// flow control rather than an outage. Demoting on 429 would move all traffic
// to the secondary provider, which then throttles too.
func TestFailoverClient_RateLimitedIsNotDemotable(t *testing.T) {
	fc, mocks := newRoutingFailover([]string{"https://a.example", "https://b.example"},
		WithFailoverLogger(testLogger()), WithFailoverMaxErrors(1))
	mocks[0].failErr = &RateLimitedError{StatusCode: 429, RetryAfter: time.Second}

	for range 3 {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}

	assert.Equal(t, StateActive, ProviderState(fc.providers[0].state.Load()),
		"throttling is not an outage; the provider keeps its traffic")
	assert.Empty(t, mocks[1].methodCalls(), "no failover may be triggered by a 429")
}

// TestFailoverClient_MidPaginationReanchor covers the cursor rule across the
// two call shapes that can carry a cursor: only GetEvents has one, so only it
// may refuse a provider switch.
func TestFailoverClient_MidPaginationReanchor(t *testing.T) {
	ctx := context.Background()
	fc, _ := newRoutingFailover([]string{"https://a.example", "https://b.example"},
		WithFailoverLogger(testLogger()))

	// Provider 0 sets the cursor.
	_, err := fc.GetEvents(ctx, GetEventsRequest{Pagination: &Pagination{Cursor: ""}})
	require.NoError(t, err)
	require.Equal(t, int32(0), fc.lastActive.Load())

	// A cursor-bearing call while provider 0 is down must refuse rather than
	// replay provider 0's cursor against provider 1.
	fc.setProviderState(fc.providers[0], StateDown)
	_, err = fc.GetEvents(ctx, GetEventsRequest{Pagination: &Pagination{Cursor: "0000000007-0000000001"}})
	assert.True(t, IsFailoverReanchor(err), "got %v", err)

	// The same switch without a cursor is harmless: the range is re-declared
	// by the caller, so provider 1 answers normally.
	fc.setProviderState(fc.providers[0], StateActive)
	fc.lastActive.Store(1)
	_, err = fc.GetEvents(ctx, GetEventsRequest{StartLedger: 10})
	require.NoError(t, err, "a cursor-less range query must not be refused")
}

// TestFailoverClient_RecordSuccessResetsBackoff pins that one good call ends
// the all-down episode cleanly: the counter and the deadline both clear, so
// the next outage starts backoff from one second instead of the previous cap.
func TestFailoverClient_RecordSuccessResetsBackoff(t *testing.T) {
	fc, _ := newRoutingFailover([]string{"https://a.example"}, WithFailoverLogger(testLogger()))
	fc.allDownCount.Store(9)
	fc.allDownUntil.Store(time.Now().Add(time.Hour).UnixNano())

	fc.recordSuccess(0)
	assert.Zero(t, fc.allDownCount.Load())
	assert.Zero(t, fc.allDownUntil.Load())
	assert.Equal(t, int32(0), fc.lastActive.Load())

	// The reset must be reflected in the next pick: an armed deadline that
	// was cleared lets selection resume immediately.
	_, idx, err := fc.pickProvider(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, idx)
}

// TestFailoverClient_BackoffGrowsWithEpisodes complements the static schedule
// table in all_down_backoff_test.go by asserting the ordering property the
// operator depends on: a longer outage must not be answered with the same
// short wait as a fresh one, and both bounds of the jitter window stay
// ordered, so no draw can make episode N+1 shorter than episode N.
func TestFailoverClient_BackoffGrowsWithEpisodes(t *testing.T) {
	// allDownBackoff draws jitter in [0, base/4) and returns
	// base-base/4+jitter, i.e. [0.75×base, base). The comment in
	// failover.go calls this "±25%", but the window is one-sided: it can
	// only ever come out shorter than the base, never longer.
	bounds := func(count int32) time.Duration {
		base := time.Second
		for range count {
			base *= 2
			if base > 30*time.Second {
				base = 30 * time.Second
				break
			}
		}
		return base
	}

	fc, _ := newRoutingFailover([]string{"https://a.example"}, WithFailoverLogger(testLogger()))

	sample := func(count int32) (min, max time.Duration) {
		fc.allDownCount.Store(count)
		for range 25 {
			got := fc.allDownBackoff()
			if min == 0 || got < min {
				min = got
			}
			if got > max {
				max = got
			}
		}
		return min, max
	}

	// Uncapped episodes: the windows must be ordered and must not overlap,
	// so no jitter draw can make a longer outage wait less than a shorter
	// one already waited.
	var prevHi time.Duration
	for count := int32(0); count <= 4; count++ {
		base := bounds(count)
		lo, hi := base*3/4, base
		require.Less(t, lo, hi, "count %d must have a non-empty window", count)
		if count > 0 {
			assert.GreaterOrEqual(t, lo, prevHi, "episode %d must never be scheduled sooner than episode %d-1", count, count-1)
		}
		prevHi = hi

		min, max := sample(count)
		assert.GreaterOrEqual(t, min, lo, "count %d sampled below its window", count)
		assert.LessOrEqual(t, max, hi, "count %d sampled above its window", count)
		assert.Greater(t, max, min, "count %d never varied: jitter is not applied", count)
	}

	// Capped episodes must stop growing at 30s however long the outage
	// runs: ten episodes would be 1024s without the cap.
	for _, count := range []int32{5, 6, 10, 40} {
		assert.Equal(t, 30*time.Second, bounds(count), "count %d base", count)
		min, max := sample(count)
		assert.GreaterOrEqual(t, min, 22500*time.Millisecond, "count %d", count)
		assert.LessOrEqual(t, max, 30*time.Second, "count %d", count)
	}
}

// TestFailoverClient_AllDownDeadlineIsOneStep guards the backoff arming in
// pickProvider: the stored deadline must be exactly one computed backoff step,
// and a refusal inside the window must not re-arm it. It also pins the
// off-by-one that makes the *first* episode land on the two-second step
// instead of the one-second base — pickProvider increments allDownCount
// before it computes the duration, so an outage that has never been seen
// before is already treated as a repeat.
func TestFailoverClient_AllDownDeadlineIsOneStep(t *testing.T) {
	fc, _ := newRoutingFailover([]string{"https://a.example"}, WithFailoverLogger(testLogger()))
	fc.setProviderState(fc.providers[0], StateDown)

	_, _, err := fc.pickProvider(context.Background())
	require.ErrorIs(t, err, ErrAllProvidersDown)

	wait := time.Until(time.Unix(0, fc.allDownUntil.Load()))
	assert.GreaterOrEqual(t, wait, 1400*time.Millisecond, "the stored deadline must be a real wait")
	assert.LessOrEqual(t, wait, 2*time.Second,
		"the enforced window must be one backoff step, not a recomputed larger one")
	assert.Equal(t, int32(1), fc.allDownCount.Load(), "a first episode counts as one")
	assert.GreaterOrEqual(t, wait, time.Second,
		"known quirk: the first episode is scheduled at the doubled step, not the 1s base")

	// While the window is open, a second refusal must not extend it.
	before := fc.allDownUntil.Load()
	_, _, err = fc.pickProvider(context.Background())
	require.ErrorIs(t, err, ErrAllProvidersDown)
	assert.Equal(t, before, fc.allDownUntil.Load(), "a refused call must not re-arm the deadline")
	assert.Equal(t, int32(1), fc.allDownCount.Load(), "nor count a second episode")
}
