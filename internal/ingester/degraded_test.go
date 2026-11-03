package ingester

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type degradedIngestStore struct {
	store.Store
	mu          sync.Mutex
	degraded    atomic.Bool
	writeErr    error
	state       store.IngestionState
	savedEvents []store.Event
	saveCalls   int
}

func (s *degradedIngestStore) Degraded() bool {
	return s.degraded.Load()
}

func (s *degradedIngestStore) Ping(_ context.Context) error {
	if s.degraded.Load() {
		return store.ErrUnavailable
	}
	return nil
}

func (s *degradedIngestStore) ListWatchedContracts(_ context.Context) ([]store.WatchedContract, error) {
	if s.degraded.Load() {
		return nil, store.ErrUnavailable
	}
	return nil, nil
}

func (s *degradedIngestStore) ActiveWatchedContractIDs(_ context.Context, _ string) ([]string, error) {
	if s.degraded.Load() {
		return nil, store.ErrUnavailable
	}
	return nil, nil
}

func (s *degradedIngestStore) GetIngestionState(_ context.Context) (store.IngestionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded.Load() {
		return store.IngestionState{}, store.ErrUnavailable
	}
	if s.state.LastIngestedLedger == 0 && s.state.LastCursor == "" {
		return store.IngestionState{}, store.ErrNotFound
	}
	return s.state, nil
}

func (s *degradedIngestStore) GetIngestionStateForNetwork(ctx context.Context, _ string) (store.IngestionState, error) {
	return s.GetIngestionState(ctx)
}

func (s *degradedIngestStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded.Load() {
		return 0, store.ErrUnavailable
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	s.savedEvents = append(s.savedEvents, events...)
	return int64(len(events)), nil
}

func (s *degradedIngestStore) UpsertAddressRefs(_ context.Context, _ []store.AddressRef) error {
	return nil
}

func (s *degradedIngestStore) SaveIngestionState(_ context.Context, state store.IngestionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded.Load() {
		return store.ErrUnavailable
	}
	if s.writeErr != nil {
		return s.writeErr
	}
	s.state = state
	s.saveCalls++
	return nil
}

type degradedMockRPC struct {
	rpc.Client
	getEventsCalls atomic.Int64
	events         []rpc.Event
	latestLedger   uint32
}

func (m *degradedMockRPC) GetHealth(_ context.Context) (rpc.Health, error) {
	return rpc.Health{
		Status:       "healthy",
		OldestLedger: 100,
		LatestLedger: m.latestLedger,
	}, nil
}

func (m *degradedMockRPC) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.latestLedger}, nil
}

func (m *degradedMockRPC) GetEvents(_ context.Context, _ rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.getEventsCalls.Add(1)
	return rpc.GetEventsResponse{
		Events:       m.events,
		LatestLedger: m.latestLedger,
	}, nil
}

type degradedTestClock struct {
	mu     sync.Mutex
	sleeps []time.Duration
	onStep func(step int, d time.Duration) bool
}

func (c *degradedTestClock) Now() time.Time {
	return time.Now()
}

func (c *degradedTestClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	step := len(c.sleeps)
	fn := c.onStep
	c.mu.Unlock()
	if fn != nil {
		return fn(step, d)
	}
	return ctx.Err() == nil
}

func TestIngester_DegradedStoreDoesNotAdvanceCursorAndRecovers(t *testing.T) {
	st := &degradedIngestStore{
		state: store.IngestionState{
			Network:            "default",
			LastIngestedLedger: 500,
			LastCursor:         "cursor-500",
		},
	}
	st.degraded.Store(true)

	rpcMock := &degradedMockRPC{
		latestLedger: 510,
		events: []rpc.Event{
			{
				ID:                       "0000505-1",
				Type:                     "contract",
				Ledger:                   505,
				LedgerClosedAt:           "2026-11-03T12:00:00Z",
				ContractID:               "CAAAA",
				PagingToken:              "cursor-505",
				InSuccessfulContractCall: true,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := &degradedTestClock{}
	clock.onStep = func(step int, _ time.Duration) bool {
		switch step {
		case 1, 2:
			// Store remains degraded through the first two backoff sleeps.
			return true
		case 3:
			// Store recovers before the fourth pass.
			st.degraded.Store(false)
			return true
		default:
			// Cancel after the first caught-up idle sleep following recovery.
			cancel()
			return false
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ing := New(rpcMock, st, passthroughDecoder{}, logger, Options{
		PollInterval: 5 * time.Second,
		MinBackoff:   1 * time.Second,
		MaxBackoff:   16 * time.Second,
		Clock:        clock,
		Jitter:       func(max time.Duration) time.Duration { return max / 2 },
	})

	err := ing.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// While degraded (passes 1..3), no RPC page fetch or cursor write occurred,
	// and backoff grew exponentially across the three degraded retries.
	clock.mu.Lock()
	sleeps := append([]time.Duration(nil), clock.sleeps...)
	clock.mu.Unlock()
	require.GreaterOrEqual(t, len(sleeps), 4)
	assert.Less(t, sleeps[0], sleeps[1], "backoff must increase across consecutive degraded cycles")
	assert.Less(t, sleeps[1], sleeps[2], "backoff must continue increasing until recovery")

	// Once recovered on pass 4, the ingester resumed from cursor-500 and
	// advanced cleanly to ledger 505 / cursor-505.
	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Equal(t, int64(1), rpcMock.getEventsCalls.Load(), "RPC should only be queried once the store is available")
	assert.Equal(t, 1, st.saveCalls)
	assert.Len(t, st.savedEvents, 1)
	assert.Equal(t, int64(505), st.state.LastIngestedLedger)
	assert.Equal(t, "cursor-505", st.state.LastCursor)
}

func TestIngester_WriteFailureMidCyclePreservesCommittedCursor(t *testing.T) {
	st := &degradedIngestStore{
		state: store.IngestionState{
			Network:            "default",
			LastIngestedLedger: 700,
			LastCursor:         "cursor-700",
		},
		writeErr: errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"),
	}

	rpcMock := &degradedMockRPC{
		latestLedger: 720,
		events: []rpc.Event{
			{
				ID:                       "0000710-1",
				Type:                     "contract",
				Ledger:                   710,
				LedgerClosedAt:           "2026-11-03T12:00:00Z",
				ContractID:               "CBBBB",
				PagingToken:              "cursor-710",
				InSuccessfulContractCall: true,
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ing := New(rpcMock, st, passthroughDecoder{}, logger, Options{})

	_, err := ing.runOnce(context.Background())
	require.Error(t, err)

	st.mu.Lock()
	assert.Equal(t, int64(700), st.state.LastIngestedLedger, "cursor must not advance when event persistence fails")
	assert.Equal(t, "cursor-700", st.state.LastCursor)
	assert.Zero(t, st.saveCalls)
	st.writeErr = nil
	st.mu.Unlock()

	// Next cycle after database recovery persists the batch and advances the cursor.
	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, caughtUp)

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Equal(t, int64(710), st.state.LastIngestedLedger)
	assert.Equal(t, "cursor-710", st.state.LastCursor)
	assert.Equal(t, 1, st.saveCalls)
}
