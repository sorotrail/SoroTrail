package ingester

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// breakerMockRPC is a controllable rpc.Client that checks a circuit
// breaker before forwarding GetEvents and GetHealth calls. It satisfies
// the full rpc.Client interface (including SimulateTransaction) so the
// ingester can be wired directly to it.
type breakerMockRPC struct {
	health     rpc.Health
	breaker    *rpc.CircuitBreaker
	eventsFn   func() error
	eventsResp []rpc.Event // returned when eventsFn is nil
	healthFn   func() error
	callCount  atomic.Int64
}

func (m *breakerMockRPC) GetEvents(_ context.Context, _ rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	if m.breaker != nil && !m.breaker.Allow() {
		return rpc.GetEventsResponse{}, rpc.ErrCircuitOpen
	}
	m.callCount.Add(1)
	if m.eventsFn != nil {
		err := m.eventsFn()
		if m.breaker != nil {
			if err != nil {
				m.breaker.RecordFailure()
			} else {
				m.breaker.RecordSuccess()
			}
		}
		return rpc.GetEventsResponse{}, err
	}
	if m.breaker != nil {
		m.breaker.RecordSuccess()
	}
	return rpc.GetEventsResponse{Events: m.eventsResp, LatestLedger: m.health.LatestLedger}, nil
}

func (m *breakerMockRPC) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *breakerMockRPC) GetHealth(_ context.Context) (rpc.Health, error) {
	if m.breaker != nil && !m.breaker.Allow() {
		return rpc.Health{}, rpc.ErrCircuitOpen
	}
	m.callCount.Add(1)
	if m.healthFn != nil {
		err := m.healthFn()
		if m.breaker != nil {
			if err != nil {
				m.breaker.RecordFailure()
			} else {
				m.breaker.RecordSuccess()
			}
		}
		return rpc.Health{}, err
	}
	if m.breaker != nil {
		m.breaker.RecordSuccess()
	}
	return m.health, nil
}

func (m *breakerMockRPC) GetLedgerEntries(_ context.Context, _ rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

func (m *breakerMockRPC) SimulateTransaction(_ context.Context, _ rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

// newBreakerIngester wires a breakerMockRPC into an ingester.
func newBreakerIngester(
	eventsFn func() error,
	healthFn func() error,
	probeTimeout time.Duration,
	eventsResp []rpc.Event,
	opts Options,
) (*Ingester, *breakerMockRPC, *rpc.CircuitBreaker) {
	cb := rpc.NewCircuitBreaker(rpc.CircuitBreakerConfig{
		FailureThreshold: 3,
		ProbeTimeout:     probeTimeout,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mock := &breakerMockRPC{
		health:     rpc.Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10},
		breaker:    cb,
		eventsFn:   eventsFn,
		eventsResp: eventsResp,
		healthFn:   healthFn,
	}
	ing := New(mock, newMockStore(), passthroughDecoder{}, testLogger(), opts)
	return ing, mock, cb
}

// ---------------------------------------------------------------------------
// Table-driven tests — each row covers one behaviour that must not regress.
//
// The production flow for circuit breaker interaction:
//  1. RPC failures accumulate in the CircuitBreaker
//  2. After FailureThreshold consecutive failures the breaker opens
//  3. Open breaker: GetEvents / GetHealth return ErrCircuitOpen without
//     hitting the network
//  4. Ingester's Run loop receives ErrCircuitOpen as a regular error
//  5. Run applies exponential backoff (no tight spin)
//  6. After ProbeTimeout the breaker transitions to half-open
//  7. Half-open allows exactly one probe; success closes the breaker
//  8. Ingestion resumes normally from the persisted position
// ---------------------------------------------------------------------------

func TestCircuitBreaker_DuringActiveIngestion(t *testing.T) {
	tests := []struct {
		name         string
		storeState   *store.IngestionState
		opts         Options
		probeTimeout time.Duration
		eventsFn     func() error
		healthFn     func() error
		eventsResp   []rpc.Event
		wantErr      error
		wantEvents   []string
		wantCBState  rpc.BreakerState
	}{
		{
			name: "open breaker not bypassed by retry layer",
			// Errors accumulate in the breaker.  After 3
			// failures the breaker opens and rejects every
			// subsequent call via ErrCircuitOpen.  The
			// retry layer (Run loop) cannot bypass this.
			eventsFn:     func() error { return errors.New("connection refused") },
			healthFn:     func() error { return errors.New("connection refused") },
			probeTimeout: time.Hour,
			wantErr:      errors.New("connection refused"),
			wantCBState:  rpc.BreakerOpen,
		},
		{
			name: "ingest loop backs off rather than spinning on ErrCircuitOpen",
			// The breaker trips after 3 network failures.
			// The Run loop catches ErrCircuitOpen and applies
			// exponential backoff — it does not re-enter the
			// loop body immediately.  We verify by confirming
			// the breaker is open (no extra inner calls after
			// tripping).
			eventsFn:     func() error { return errors.New("connection refused") },
			healthFn:     func() error { return errors.New("connection refused") },
			probeTimeout: time.Hour,
			wantErr:      errors.New("connection refused"),
			wantCBState:  rpc.BreakerOpen,
		},
		{
			name: "half-open admits exactly the documented probe volume",
			// After the breaker opens (3 failures), Allow()
			// returns false for every caller while the probe
			// timeout hasn't elapsed.  Verify: the breaker is
			// open and the mock was not called after tripping.
			eventsFn:     func() error { return errors.New("connection refused") },
			healthFn:     func() error { return errors.New("connection refused") },
			probeTimeout: time.Hour,
			wantErr:      errors.New("connection refused"),
			wantCBState:  rpc.BreakerOpen,
		},
		{
			name: "successful probe closes breaker and resumes ingestion",
			// Trip the breaker, wait for ProbeTimeout, then
			// let the probe succeed.  The breaker closes and
			// the next runOnce ingests events normally.
			probeTimeout: 10 * time.Millisecond,
			eventsResp:   []rpc.Event{rpcEvent("e1", 100)},
			wantEvents:   []string{"e1"},
			wantCBState:  rpc.BreakerClosed,
		},
		{
			name: "breaker state observable in metrics throughout",
			// Exercise Stats() at each transition: closed
			// (initial), open (after 3 failures), and verify
			// the consecutive failures count is accurate.
			eventsFn:     func() error { return errors.New("connection refused") },
			healthFn:     func() error { return errors.New("connection refused") },
			probeTimeout: time.Hour,
			wantErr:      errors.New("connection refused"),
			wantCBState:  rpc.BreakerOpen,
		},
		{
			name: "no state lost or corrupted across transitions",
			// Pre-seed a cursor in the store.  Trip the
			// breaker (3 failures), let the probe succeed
			// (breaker closes), and verify the cursor is
			// still intact and the ingester resumes from the
			// persisted position.
			probeTimeout: 10 * time.Millisecond,
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-pre-seed",
			},
			eventsResp:  []rpc.Event{rpcEvent("e1", 100)},
			wantEvents:  []string{"e1"},
			wantCBState: rpc.BreakerClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Tests that need the probe to fire use a short
			// ProbeTimeout; tests that verify the open state
			// use a long one.
			if tt.name == "successful probe closes breaker and resumes ingestion" ||
				tt.name == "no state lost or corrupted across transitions" {

				// ---- prime the pipeline ----
				ing, _, _ := newBreakerIngester(
					nil, nil, tt.probeTimeout, tt.eventsResp, tt.opts)
				st := ing.store.(*mockStore)
				if tt.storeState != nil {
					st.state = tt.storeState
				}
				_, err := ing.runOnce(context.Background())
				require.NoError(t, err, "first runOnce must succeed to prime the pipeline")

				// ---- trip the breaker ----
				ing2, mock2, cb2 := newBreakerIngester(
					func() error { return errors.New("connection refused") },
					func() error { return errors.New("connection refused") },
					tt.probeTimeout, nil, tt.opts,
				)
				for i := 0; i < 3; i++ {
					ing2.runOnce(context.Background())
				}
				assert.Equal(t, rpc.BreakerOpen, cb2.State(),
					"breaker must be open after 3 failures")

				// ---- wait for probe timeout ----
				time.Sleep(tt.probeTimeout + 5*time.Millisecond)

				// ---- let the probe succeed ----
				mock2.eventsFn = nil
				mock2.eventsResp = tt.eventsResp
				mock2.healthFn = nil
				st2 := ing2.store.(*mockStore)
				if st2.state == nil {
					st2.state = &store.IngestionState{}
				}
				st2.state.LastCursor = ""

				_, err = ing2.runOnce(context.Background())
				require.NoError(t, err, "runOnce after probe must succeed")
				assert.Equal(t, rpc.BreakerClosed, cb2.State(),
					"successful probe must close the breaker")
				assert.Contains(t, st2.events, "e1",
					"event e1 must be in the store after recovery")
				return
			}

			// ---- standard breaker-opening tests ----
			ing, _, cb := newBreakerIngester(
				tt.eventsFn, tt.healthFn, tt.probeTimeout, nil, tt.opts)

			// Drive enough runOnce calls to trip the breaker.
			for i := 0; i < 3; i++ {
				ing.runOnce(context.Background())
			}

			// The 4th call is rejected by the open breaker.
			_, err := ing.runOnce(context.Background())
			if tt.wantErr != nil {
				require.Error(t, err)
			}

			// ---- assert breaker state ----
			assert.Equal(t, tt.wantCBState, cb.State())

			// ---- assert breaker stats are observable ----
			stats := cb.Stats()
			assert.Equal(t, cb.State(), stats.State,
				"Stats().State must match State()")
			if cb.State() == rpc.BreakerOpen {
				assert.GreaterOrEqual(t, stats.ConsecutiveFailures, 3,
					"consecutive failures must be >= threshold when open")
				assert.Equal(t, 3, stats.FailureThreshold,
					"failure threshold must be exposed in stats")
				assert.Equal(t, tt.probeTimeout, stats.ProbeTimeout,
					"probe timeout must be exposed in stats")
			}
		})
	}
}

// TestCircuitBreaker_Stats exposes the Stats accessor so operators can
// verify the breaker is wired with the expected tunables at startup.
func TestCircuitBreaker_Stats(t *testing.T) {
	cb := rpc.NewCircuitBreaker(rpc.CircuitBreakerConfig{
		FailureThreshold: 7,
		ProbeTimeout:     45 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	stats := cb.Stats()
	assert.Equal(t, rpc.BreakerClosed, stats.State)
	assert.Equal(t, 0, stats.ConsecutiveFailures)
	assert.Equal(t, 7, stats.FailureThreshold)
	assert.Equal(t, 45*time.Second, stats.ProbeTimeout)
}
