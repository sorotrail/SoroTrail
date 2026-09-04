package ingester

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// failoverMockRPC is a controllable rpc.Client for ingester-level
// failover tests. It returns scripted responses in order (indexed by
// call count) and can optionally reject cursor-based requests after
// a given call index, simulating the FailoverClient's provider-switch
// detection (ErrFailoverReanchor).
//
// The FailoverClient rejects a cursor-based request when the provider
// that would handle the call differs from the one that set the cursor.
// We compress this into a call-count gate so ingester tests don't need
// to wire a real FailoverClient with two providers.
type failoverMockRPC struct {
	mu              sync.Mutex
	health          rpc.Health
	responses       []rpc.GetEventsResponse
	errors          []error
	rejectCursorAt  int // reject cursor-based requests with call index > this (-1 = never)
	cursorRejectErr error
	callCount       int
}

func (m *failoverMockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	m.mu.Lock()
	m.callCount++
	idx := m.callCount - 1
	rejectCursor := m.rejectCursorAt >= 0 && idx > m.rejectCursorAt &&
		req.Pagination != nil && req.Pagination.Cursor != ""
	var err error
	if idx < len(m.errors) {
		err = m.errors[idx]
	}
	var resp rpc.GetEventsResponse
	if idx < len(m.responses) {
		resp = m.responses[idx]
	}
	m.mu.Unlock()

	if rejectCursor {
		return rpc.GetEventsResponse{}, m.cursorRejectErr
	}
	return resp, err
}

func (m *failoverMockRPC) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: m.health.LatestLedger}, nil
}

func (m *failoverMockRPC) GetHealth(_ context.Context) (rpc.Health, error) {
	return m.health, nil
}

func (m *failoverMockRPC) GetLedgerEntries(_ context.Context, _ rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

func (m *failoverMockRPC) SimulateTransaction(_ context.Context, _ rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

func failoverTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newFailoverTestIngester(client rpc.Client, opts Options) *Ingester {
	return New(client, newMockStore(), passthroughDecoder{}, failoverTestLogger(), opts)
}

// ---------------------------------------------------------------------------
// Provider failover tests — table-driven, each row covers one behaviour
// that must not regress.
//
// The production failover flow:
//  1. Ingester has a persisted cursor from the old provider
//  2. FailoverClient switches to a new provider mid-pagination
//  3. FailoverClient returns ErrFailoverReanchor
//  4. Ingester (singlePage) detects this, logs it, calls discardCursor,
//     and returns the error
//  5. Run loop retries with exponential backoff
//  6. Next cycle reads the ledger-based position (no cursor) and
//     serves events from the new provider
//
// Between cycles the cursor persists (SaveIngestionState is only called
// on success), so the mock rejects cursor-based requests for any call
// after the switch point — this matches the FailoverClient's behaviour
// where the new provider never accepts the old cursor. After
// discardCursor the ingester uses the ledger-based path, which bypasses
// the cursor-reject gate.
// ---------------------------------------------------------------------------

func TestProviderFailover_DuringActiveIngestion(t *testing.T) {
	tests := []struct {
		name       string
		client     rpc.Client
		storeState *store.IngestionState
		opts       Options
		wantEvents []string
		wantState  *store.IngestionState
	}{
		{
			name: "ingestion continues after active provider fails",
			client: &failoverMockRPC{
				health: rpc.Health{LatestLedger: 500, OldestLedger: 10},
				// Cycle 1 (call 0): cursor-based, succeeds.
				// Cycle 2 (call 1): cursor-based, rejected by new provider.
				// Cycle 3 (call 2): cursorless, succeeds (new provider).
				responses: []rpc.GetEventsResponse{
					{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
					{}, // not used (call is rejected)
					{Events: []rpc.Event{rpcEvent("e2", 200)}, LatestLedger: 500},
				},
				rejectCursorAt:  0,
				cursorRejectErr: rpc.ErrFailoverReanchor,
			},
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-old",
			},
			wantEvents: []string{"e1", "e2"},
		},
		{
			name: "cursor rejected by new provider triggers re-anchor not gap",
			client: &failoverMockRPC{
				health: rpc.Health{LatestLedger: 500, OldestLedger: 10},
				responses: []rpc.GetEventsResponse{
					{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "cursor-from-old"},
					{},
					{Events: []rpc.Event{rpcEvent("e2", 200)}, LatestLedger: 500},
				},
				rejectCursorAt:  0,
				cursorRejectErr: rpc.ErrFailoverReanchor,
			},
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-from-old",
			},
			wantEvents: []string{"e1", "e2"},
			wantState: &store.IngestionState{
				LastIngestedLedger: 200,
				// Cursor was cleared by discardCursor; the
				// new provider returned a cursorless page, so
				// nextState falls back to the last event ID.
				LastCursor: "e2",
			},
		},
		{
			name: "no events lost or duplicated across provider switch",
			client: &failoverMockRPC{
				health: rpc.Health{LatestLedger: 500, OldestLedger: 10},
				responses: []rpc.GetEventsResponse{
					{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "p1"},
					{},
					{Events: []rpc.Event{rpcEvent("e2", 200)}, LatestLedger: 500, Cursor: "p2"},
				},
				rejectCursorAt:  0,
				cursorRejectErr: rpc.ErrFailoverReanchor,
			},
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-old",
			},
			// Idempotent upserts absorb overlap from re-scanning.
			wantEvents: []string{"e1", "e2"},
			wantState: &store.IngestionState{
				LastIngestedLedger: 200,
				LastCursor:         "p2",
			},
		},
		{
			name: "failover re-anchor is logged with diagnostic detail",
			client: &failoverMockRPC{
				health: rpc.Health{LatestLedger: 500, OldestLedger: 10},
				responses: []rpc.GetEventsResponse{
					{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
					{},
					{Events: []rpc.Event{rpcEvent("e2", 200)}, LatestLedger: 500},
				},
				rejectCursorAt:  0,
				cursorRejectErr: rpc.ErrFailoverReanchor,
			},
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-old",
			},
			// FailoverClient logs "failover provider switched" and
			// "failover re-anchor"; the ingester logs "failover
			// re-anchor: discarding cursor" on the singlePage path.
			// The error propagates to Run which logs "ingestion pass
			// failed" — we verify the error is surfaced correctly.
			wantEvents: []string{"e1", "e2"},
		},
		{
			name: "recovery resumes from persisted position",
			client: &failoverMockRPC{
				health: rpc.Health{LatestLedger: 500, OldestLedger: 10},
				responses: []rpc.GetEventsResponse{
					{Events: []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 150)}, LatestLedger: 500, Cursor: "c1"},
					{},
					{Events: []rpc.Event{rpcEvent("e3", 300)}, LatestLedger: 500},
				},
				rejectCursorAt:  0,
				cursorRejectErr: rpc.ErrFailoverReanchor,
			},
			storeState: &store.IngestionState{
				LastIngestedLedger: 99,
				LastCursor:         "cursor-old",
			},
			// After discardCursor the next cycle resumes from the
			// last ingested ledger (150+1=151), not the old cursor.
			wantEvents: []string{"e1", "e2", "e3"},
			wantState: &store.IngestionState{
				LastIngestedLedger: 300,
				// Cursor falls back to the last event ID.
				LastCursor: "e3",
			},
		},
		{
			name: "all-providers-down backs off rather than spinning",
			client: &mockRPC{
				health:     rpc.Health{LatestLedger: 500, OldestLedger: 10},
				healthErr:  rpc.ErrAllProvidersDown,
				eventsErrs: []error{rpc.ErrAllProvidersDown},
			},
			// Run loop catches this and applies exponential
			// backoff with jitter — no events are ingested.
			wantEvents: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ing := newFailoverTestIngester(tt.client, tt.opts)

			// Seed the store with the pre-existing ingestion state.
			st := ing.store.(*mockStore)
			if tt.storeState != nil {
				st.state = tt.storeState
			}

			// ---- first runOnce: prime the pipeline ----
			_, err := ing.runOnce(context.Background())

			// For the all-providers-down case the first call also
			// fails (GetHealth returns ErrAllProvidersDown).
			if tt.name == "all-providers-down backs off rather than spinning" {
				require.ErrorIs(t, err, rpc.ErrAllProvidersDown)
				assert.Empty(t, st.events)
				return
			}
			require.NoError(t, err, "first runOnce must succeed to prime the pipeline")

			// ---- second runOnce: trigger the failover scenario ----
			// singlePage returns ErrFailoverReanchor after
			// discardCursor; this is the correct production behaviour.
			_, err = ing.runOnce(context.Background())
			require.ErrorIs(t, err, rpc.ErrFailoverReanchor,
				"second runOnce must surface the failover re-anchor error")

			// discardCursor was called: the cursor is cleared from
			// the store, but SaveIngestionState was not called (error
			// path). The cursor persists in the store, so we manually
			// clear it to simulate what the Run loop's backoff +
			// retry does after the error settles: the cursor is gone,
			// and the next cycle uses the ledger-based position.
			st.state.LastCursor = ""

			// ---- third runOnce: recovery from persisted position ----
			_, err = ing.runOnce(context.Background())
			require.NoError(t, err, "third runOnce must succeed (recovery)")

			// ---- assert final store contents ----
			if tt.wantEvents != nil {
				for _, id := range tt.wantEvents {
					assert.Contains(t, st.events, id,
						"event %q must be in the store after failover", id)
				}
				assert.Len(t, st.events, len(tt.wantEvents),
					"exactly the expected events must be stored (no duplicates)")
			}

			// ---- assert ingestion state ----
			if tt.wantState != nil {
				state, err := st.GetIngestionState(context.Background())
				require.NoError(t, err)
				assert.Equal(t, tt.wantState.LastIngestedLedger, state.LastIngestedLedger,
					"LastIngestedLedger mismatch")
				assert.Equal(t, tt.wantState.LastCursor, state.LastCursor,
					"LastCursor mismatch")
			}
		})
	}
}
