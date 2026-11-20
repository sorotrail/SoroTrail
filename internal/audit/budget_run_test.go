package audit

// Tests for the budgeted RPC client (client.go) and the Auditor.Run loop
// (audit.go). The budget is what keeps the gap auditor from starving
// ingestion, so these tests check that no method can spend more than the audit
// pool allows, and that the loop neither outlives its context nor dies on an
// audit error.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// spyRPC counts the calls it receives per method, remembers their arguments
// and returns canned values, so a test can tell whether the budgeted wrapper
// reached it and whether it passed everything through unchanged.
type spyRPC struct {
	events, latest, health, entries, simulate atomic.Int64

	mu         sync.Mutex
	eventsReq  rpc.GetEventsRequest
	entriesReq rpc.GetLedgerEntriesRequest
	simReq     rpc.SimulateTransactionRequest

	// healthErr, when it returns non-nil for call n (1-based), makes GetHealth
	// fail instead of returning healthy.
	healthErr func(n int64) error
}

var errInner = errors.New("inner rpc failure")

func (s *spyRPC) total() int64 {
	return s.events.Load() + s.latest.Load() + s.health.Load() + s.entries.Load() + s.simulate.Load()
}

func (s *spyRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	s.events.Add(1)
	s.mu.Lock()
	s.eventsReq = req
	s.mu.Unlock()
	return rpc.GetEventsResponse{LatestLedger: 11}, nil
}

func (s *spyRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	s.latest.Add(1)
	return rpc.LatestLedger{Sequence: 22}, nil
}

func (s *spyRPC) GetHealth(context.Context) (rpc.Health, error) {
	n := s.health.Add(1)
	if s.healthErr != nil {
		if err := s.healthErr(n); err != nil {
			return rpc.Health{}, err
		}
	}
	return rpc.Health{Status: "healthy", LatestLedger: 33}, nil
}

func (s *spyRPC) GetLedgerEntries(_ context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	s.entries.Add(1)
	s.mu.Lock()
	s.entriesReq = req
	s.mu.Unlock()
	return rpc.GetLedgerEntriesResponse{Entries: []rpc.LedgerEntryResult{{Key: "key", XDR: "entry"}}}, nil
}

func (s *spyRPC) SimulateTransaction(_ context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	s.simulate.Add(1)
	s.mu.Lock()
	s.simReq = req
	s.mu.Unlock()
	return rpc.SimulateTransactionResponse{TransactionData: "simulated"}, nil
}

// rpcCalls lists every rpc.Client method as a uniform closure so the budget
// tests can run the same assertions against all five.
func rpcCalls(c rpc.Client) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"GetEvents":       func(ctx context.Context) error { _, err := c.GetEvents(ctx, rpc.GetEventsRequest{}); return err },
		"GetLatestLedger": func(ctx context.Context) error { _, err := c.GetLatestLedger(ctx); return err },
		"GetHealth":       func(ctx context.Context) error { _, err := c.GetHealth(ctx); return err },
		"GetLedgerEntries": func(ctx context.Context) error {
			_, err := c.GetLedgerEntries(ctx, rpc.GetLedgerEntriesRequest{})
			return err
		},
		"SimulateTransaction": func(ctx context.Context) error {
			_, err := c.SimulateTransaction(ctx, rpc.SimulateTransactionRequest{})
			return err
		},
	}
}

// onePerSecondBudget returns a budget whose audit pool holds a single token
// and refills at one per second: after one call the pool is empty for the
// rest of the test.
func onePerSecondBudget(t *testing.T) *rpc.Budget {
	t.Helper()
	b, err := rpc.NewBudget(1, 1)
	require.NoError(t, err)
	return b
}

// shortCtx returns a context whose deadline is far shorter than the time an
// empty one-per-second pool needs to refill, so a call that would have to wait
// is rejected at once instead of sleeping.
func shortCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func TestBudgetedClientRefusesOnceTheBudgetIsSpent(t *testing.T) {
	t.Parallel()

	for name := range rpcCalls(nil) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inner := &spyRPC{}
			call := rpcCalls(NewBudgetedClient(inner, onePerSecondBudget(t)))[name]

			require.NoError(t, call(context.Background()), "the first call spends the only token")
			require.Equal(t, int64(1), inner.total())

			err := call(shortCtx(t))
			require.Error(t, err, "a spent budget must not let the call through")
			assert.Equal(t, int64(1), inner.total(), "a refused call never reaches the inner client")
		})
	}
}

func TestBudgetedClientSharesOnePoolAcrossMethods(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{}
	calls := rpcCalls(NewBudgetedClient(inner, onePerSecondBudget(t)))

	require.NoError(t, calls["GetEvents"](context.Background()))
	for name, call := range calls {
		if name == "GetEvents" {
			continue
		}
		assert.Error(t, call(shortCtx(t)), "%s draws from the same pool GetEvents emptied", name)
	}
	assert.Equal(t, int64(1), inner.total(), "five methods, one budget: only one call got through")
	assert.Equal(t, int64(1), inner.events.Load())
}

func TestBudgetedClientHonoursCancelledContext(t *testing.T) {
	t.Parallel()

	for name := range rpcCalls(nil) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inner := &spyRPC{}
			// A generous budget: only the cancelled context can stop the call.
			b, err := rpc.NewBudget(1000, 1)
			require.NoError(t, err)
			call := rpcCalls(NewBudgetedClient(inner, b))[name]

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			assert.ErrorIs(t, call(ctx), context.Canceled)
			assert.Zero(t, inner.total())
		})
	}
}

func TestBudgetedClientRefillsAtTheConfiguredRate(t *testing.T) {
	t.Parallel()

	// 20 requests a second, all of it for the audit pool: a burst of 20, then
	// one more token every 50ms.
	b, err := rpc.NewBudget(20, 1)
	require.NoError(t, err)
	inner := &spyRPC{}
	c := NewBudgetedClient(inner, b)

	for i := 0; i < 20; i++ {
		_, err := c.GetHealth(context.Background())
		require.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err = c.GetHealth(ctx)
	require.NoError(t, err, "the pool refills, so the call is delayed rather than refused")
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "the 21st call has to wait for a token")
	assert.Equal(t, int64(21), inner.health.Load())
}

func TestBudgetedClientDrawsOnlyFromTheAuditPool(t *testing.T) {
	t.Parallel()

	b, err := rpc.NewBudget(2, 0.5) // one token each for ingest and audit
	require.NoError(t, err)
	c := NewBudgetedClient(&spyRPC{}, b)

	_, err = c.GetHealth(context.Background())
	require.NoError(t, err)
	_, err = c.GetHealth(shortCtx(t))
	require.Error(t, err, "the audit pool is empty")

	assert.NoError(t, b.WaitIngest(shortCtx(t)),
		"auditing, however busy, leaves the ingest pool untouched")
}

func TestBudgetedClientWithNilBudgetIsUnpaced(t *testing.T) {
	t.Parallel()

	for name := range rpcCalls(nil) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inner := &spyRPC{}
			call := rpcCalls(NewBudgetedClient(inner, nil))[name]
			for i := 0; i < 100; i++ {
				require.NoError(t, call(shortCtx(t)))
			}
			assert.Equal(t, int64(100), inner.total())
		})
	}
}

// TestBudgetedClientAuditPoolBoundaries checks the burst sizes NewBudgetedClient
// inherits at the edges of the audit share: a pool with no refill and a
// fractional rate both still allow exactly one call before making the next one
// wait.
func TestBudgetedClientAuditPoolBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRPS     float64
		auditShare float64
	}{
		{name: "no audit share never refills", maxRPS: 1, auditShare: 0},
		{name: "audit share of one takes the whole rate", maxRPS: 1, auditShare: 1},
		{name: "fractional rate still bursts to one token", maxRPS: 0.5, auditShare: 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := rpc.NewBudget(tt.maxRPS, tt.auditShare)
			require.NoError(t, err)
			inner := &spyRPC{}
			c := NewBudgetedClient(inner, b)

			_, err = c.GetHealth(context.Background())
			require.NoError(t, err, "the minimum burst is always one call")
			_, err = c.GetHealth(shortCtx(t))
			assert.Error(t, err, "the pool is empty, so the next call waits past its deadline")
			assert.Equal(t, int64(1), inner.health.Load(), "a waiting call never reaches the inner client")
		})
	}
}

// TestNewBudgetedClientRejectsInvalidBudgets checks the invalid inputs that must
// be refused before a client can be wrapped around them: a non-positive rate
// and an audit share outside [0,1].
func TestNewBudgetedClientRejectsInvalidBudgets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRPS     float64
		auditShare float64
	}{
		{name: "zero max rate", maxRPS: 0, auditShare: 0.5},
		{name: "negative max rate", maxRPS: -1, auditShare: 0.5},
		{name: "audit share above one", maxRPS: 10, auditShare: 1.5},
		{name: "negative audit share", maxRPS: 10, auditShare: -0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := rpc.NewBudget(tt.maxRPS, tt.auditShare)
			assert.Error(t, err, "an invalid budget is rejected instead of wrapping a client")
			assert.Nil(t, b)
		})
	}
}

func TestBudgetedClientPassesRequestsAndResponsesThrough(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{}
	b, err := rpc.NewBudget(1000, 1)
	require.NoError(t, err)
	c := NewBudgetedClient(inner, b)
	ctx := context.Background()

	evReq := rpc.GetEventsRequest{StartLedger: 7, Pagination: &rpc.Pagination{Limit: 3}}
	evResp, err := c.GetEvents(ctx, evReq)
	require.NoError(t, err)
	assert.Equal(t, uint32(11), evResp.LatestLedger)
	assert.Equal(t, evReq, inner.eventsReq, "the request reaches the inner client unchanged")

	latest, err := c.GetLatestLedger(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(22), latest.Sequence)

	health, err := c.GetHealth(ctx)
	require.NoError(t, err)
	assert.Equal(t, rpc.Health{Status: "healthy", LatestLedger: 33}, health)

	entriesReq := rpc.GetLedgerEntriesRequest{Keys: []string{"AAAA", "BBBB"}}
	entries, err := c.GetLedgerEntries(ctx, entriesReq)
	require.NoError(t, err)
	assert.Equal(t, []rpc.LedgerEntryResult{{Key: "key", XDR: "entry"}}, entries.Entries)
	assert.Equal(t, entriesReq, inner.entriesReq)

	simReq := rpc.SimulateTransactionRequest{Transaction: "AAAAAgAA"}
	sim, err := c.SimulateTransaction(ctx, simReq)
	require.NoError(t, err)
	assert.Equal(t, "simulated", sim.TransactionData)
	assert.Equal(t, simReq, inner.simReq)

	assert.Equal(t, int64(5), inner.total(), "each method made exactly one inner call")
}

// failingRPC returns errInner from every method, so the wrapper's error path
// can be checked for all five.
type failingRPC struct{}

func (failingRPC) GetEvents(context.Context, rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, errInner
}
func (failingRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{}, errInner
}
func (failingRPC) GetHealth(context.Context) (rpc.Health, error) { return rpc.Health{}, errInner }
func (failingRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, errInner
}
func (failingRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, errInner
}

func TestBudgetedClientReturnsInnerErrorsUnchanged(t *testing.T) {
	t.Parallel()

	b, err := rpc.NewBudget(1000, 1)
	require.NoError(t, err)
	for name, call := range rpcCalls(NewBudgetedClient(failingRPC{}, b)) {
		assert.Equal(t, errInner, call(context.Background()), "%s must not wrap or replace the inner error", name)
	}
}

func TestAuditorNetworkAndInitialMetrics(t *testing.T) {
	t.Parallel()

	a := New(&spyRPC{}, newMockStore(), &stubReingest{}, testLogger(), Options{Network: "testnet"})
	assert.Equal(t, "testnet", a.Network())
	assert.Equal(t, Metrics{}, a.Metrics(), "a new auditor has counted nothing")
}

// runAuditor starts a.Run and returns a channel that delivers its result.
func runAuditor(ctx context.Context, a *Auditor) <-chan error {
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return done
}

// waitFor polls cond until it holds or the deadline passes. The deadline is
// generous because it only matters when the code under test is broken.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, time.Millisecond, what)
}

func awaitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
		return nil
	}
}

func newRunAuditor(client rpc.Client, log *slog.Logger, opts Options) *Auditor {
	if opts.Network == "" {
		opts.Network = "default"
	}
	st := newMockStore()
	// Nothing ingested yet: every pass asks the RPC for health, then idles.
	primeIngest(st, 0)
	return New(client, st, &stubReingest{}, log, opts)
}

func TestRunAuditsOnEveryIntervalUntilCancelled(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{}
	a := newRunAuditor(inner, testLogger(), Options{PollInterval: 2 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runAuditor(ctx, a)
	waitFor(t, "several audit passes", func() bool { return inner.health.Load() >= 4 })
	cancel()

	assert.ErrorIs(t, awaitRun(t, done), context.Canceled)
	// Metrics are read after Run returned, so there is no race with the loop.
	assert.GreaterOrEqual(t, a.Metrics().PassesRun, uint64(3), "each completed pass is counted")
	assert.GreaterOrEqual(t, a.Metrics().RPCRequests, uint64(3), "each pass spends an RPC request")
}

func TestRunStopsPromptlyWhileWaitingForTheNextInterval(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{}
	// An hour between passes: Run is asleep in the interval when we cancel.
	a := newRunAuditor(inner, testLogger(), Options{PollInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runAuditor(ctx, a)
	waitFor(t, "the first pass", func() bool { return inner.health.Load() == 1 })
	cancel()

	assert.ErrorIs(t, awaitRun(t, done), context.Canceled, "cancellation interrupts the sleep")
	assert.Equal(t, int64(1), inner.health.Load(), "no second pass was started")
}

func TestRunWithAlreadyCancelledContextReturnsAtOnce(t *testing.T) {
	t.Parallel()

	a := newRunAuditor(&spyRPC{}, testLogger(), Options{PollInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, awaitRun(t, runAuditor(ctx, a)), context.Canceled)
}

// syncBuffer is a bytes.Buffer safe to write from the Run goroutine while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunSurvivesAnAuditErrorAndKeepsGoing(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{healthErr: func(n int64) error {
		if n == 1 {
			return errInner
		}
		return nil
	}}
	logs := &syncBuffer{}
	a := newRunAuditor(inner, slog.New(slog.NewTextHandler(logs, nil)), Options{PollInterval: 2 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runAuditor(ctx, a)
	// The first pass fails; Run backs off (under a second) and retries rather
	// than returning the error.
	waitFor(t, "a pass after the failed one", func() bool { return inner.health.Load() >= 3 })
	select {
	case err := <-done:
		t.Fatalf("Run exited after an audit error: %v", err)
	default:
	}
	cancel()

	assert.ErrorIs(t, awaitRun(t, done), context.Canceled, "only the context ends the loop, never an audit error")
	assert.Contains(t, logs.String(), "audit pass failed")
	assert.Contains(t, logs.String(), errInner.Error())
	assert.Contains(t, logs.String(), "network=default")
	assert.GreaterOrEqual(t, a.Metrics().PassesRun, uint64(1), "passes after the failure are counted")
}

func TestRunCancelledDuringBackoffReturnsTheContextError(t *testing.T) {
	t.Parallel()

	inner := &spyRPC{healthErr: func(int64) error { return errInner }}
	a := newRunAuditor(inner, testLogger(), Options{PollInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := runAuditor(ctx, a)
	waitFor(t, "the failing pass", func() bool { return inner.health.Load() == 1 })
	cancel()

	err := awaitRun(t, done)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, errInner, "the audit error is logged and retried, not returned")
	assert.Zero(t, a.Metrics().PassesRun, "a failed pass is not counted")
}

func TestRunNeverSpendsMoreThanItsBudget(t *testing.T) {
	t.Parallel()

	// An audit pool of exactly one request that never refills (auditShare 0).
	b, err := rpc.NewBudget(1, 0)
	require.NoError(t, err)
	inner := &spyRPC{}
	logs := &syncBuffer{}
	a := newRunAuditor(NewBudgetedClient(inner, b), slog.New(slog.NewTextHandler(logs, nil)),
		Options{PollInterval: time.Millisecond})
	// The context needs a deadline: a limiter that can never refill only
	// reports that it cannot serve a call when it knows the caller gives up.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	done := runAuditor(ctx, a)
	// The first pass spends the token; the next one is refused by the budget,
	// which the loop logs and backs off from.
	waitFor(t, "the budget to refuse a pass", func() bool {
		return strings.Contains(logs.String(), "audit pass failed")
	})
	cancel()
	require.ErrorIs(t, awaitRun(t, done), context.Canceled)

	assert.Equal(t, int64(1), inner.total(), "the auditor stopped at its budget instead of calling the RPC again")
	assert.Contains(t, logs.String(), "getHealth")
}
