package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestIngester(client rpc.Client, st store.Store, opts Options) *Ingester {
	return New(client, st, passthroughDecoder{}, testLogger(), opts)
}

func rpcEvent(id string, ledger uint32) rpc.Event {
	return rpc.Event{
		ID:         id,
		Type:       "contract",
		Ledger:     ledger,
		ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		TxHash:     "abc123",
		ValueJSON:  json.RawMessage(`{"u64":1}`),
		TopicJSON:  []json.RawMessage{json.RawMessage(`{"symbol":"transfer"}`)},
	}
}

func TestColdStart_UsesRetentionWindow(t *testing.T) {
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 100_000, OldestLedger: 50},
		eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("e1", 90_000)},
			LatestLedger: 100_000,
		}},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{RetentionLedgers: 17_280, PageLimit: 100})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, caughtUp, "short page means caught up")

	require.Len(t, client.eventsRequests, 1)
	assert.Equal(t, uint32(100_000-17_280), client.eventsRequests[0].StartLedger)
	assert.Contains(t, st.events, "e1")
}

func TestColdStart_ClampsToOldestRetained(t *testing.T) {
	client := &mockRPC{
		health:      rpc.Health{Status: "healthy", LatestLedger: 10_000, OldestLedger: 9_000},
		eventsResps: []rpc.GetEventsResponse{{LatestLedger: 10_000}},
	}
	ing := newTestIngester(client, newMockStore(), Options{RetentionLedgers: 17_280})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(9_000), client.eventsRequests[0].StartLedger,
		"latest - retention is below the RPC's oldest ledger, so clamp up")
}

func TestColdStart_ExplicitStartLedgerOverrides(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 10_000}}}
	ing := newTestIngester(client, newMockStore(), Options{StartLedger: 1_234})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(1_234), client.eventsRequests[0].StartLedger)
}

func TestWarmStart_ResumesAfterLastIngestedLedger(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 1_000}}}
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 500}))
	ing := newTestIngester(client, st, Options{})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(501), client.eventsRequests[0].StartLedger)
}

func TestWarmStart_ResumesFromCursor(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 1_000}}}
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 500, LastCursor: "cursor-42"}))
	ing := newTestIngester(client, st, Options{})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	req := client.eventsRequests[0]
	require.NotNil(t, req.Pagination)
	assert.Equal(t, "cursor-42", req.Pagination.Cursor)
	assert.Zero(t, req.StartLedger, "cursor and startLedger are mutually exclusive")
}

func TestPagination_FullPageKeepsCursorAndContinues(t *testing.T) {
	// Page 1 is full (2 events, limit 2) with a top-level cursor; page 2 is
	// short → caught up.
	client := &mockRPC{
		eventsResps: []rpc.GetEventsResponse{
			{
				Events:       []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 101)},
				LatestLedger: 500,
				Cursor:       "cursor-e2",
			},
			{
				Events:       []rpc.Event{rpcEvent("e3", 102)},
				LatestLedger: 500,
				Cursor:       "cursor-e3",
			},
		},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 2})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp)
	state, err := st.GetIngestionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cursor-e2", state.LastCursor)
	assert.Equal(t, int64(101), state.LastIngestedLedger)

	caughtUp, err = ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, caughtUp)
	assert.Equal(t, "cursor-e2", client.eventsRequests[1].Pagination.Cursor)
	assert.Len(t, st.events, 3)

	state, err = st.GetIngestionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cursor-e3", state.LastCursor, "caught-up resume prefers the cursor")
}

func TestPagination_LegacyPagingTokenFallback(t *testing.T) {
	// Old servers return no top-level cursor; the per-event pagingToken of
	// the last event is used instead.
	client := &mockRPC{
		eventsResps: []rpc.GetEventsResponse{{
			Events: []rpc.Event{
				{ID: "e1", Ledger: 100, PagingToken: "pt-1"},
				{ID: "e2", Ledger: 100, PagingToken: "pt-2"},
			},
			LatestLedger: 500,
		}},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 2})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp)
	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, "pt-2", state.LastCursor)
}

func TestIdempotentReIngest(t *testing.T) {
	resp := rpc.GetEventsResponse{
		Events:       []rpc.Event{rpcEvent("e1", 100)},
		LatestLedger: 500,
	}
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{resp, resp}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	// Reset position so the same page is fetched again.
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 99}))
	_, err = ing.runOnce(context.Background())
	require.NoError(t, err)

	assert.Len(t, st.events, 1, "re-ingesting the same events must not duplicate")
}

// Raw XDR must be retained at ingest time, otherwise `sorotrail replay` has
// nothing to re-decode. Events the RPC delivered as JSON have no XDR to keep,
// and replay skips them.
func TestPersistEvents_RetainsRawXDR(t *testing.T) {
	fromXDR := rpc.Event{
		ID:         "e1",
		Type:       "contract",
		Ledger:     100,
		ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Topic:      []string{"topic-xdr"},
		Value:      "value-xdr",
	}
	fromJSON := rpcEvent("e2", 100) // TopicJSON/ValueJSON, no XDR

	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{
		Events:       []rpc.Event{fromXDR, fromJSON},
		LatestLedger: 500,
	}}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{"topic-xdr"}, st.events["e1"].RawTopicXDR)
	assert.Equal(t, "value-xdr", st.events["e1"].RawValueXDR)
	assert.Empty(t, st.events["e2"].RawTopicXDR, "JSON-delivered events have no XDR to retain")
	assert.Empty(t, st.events["e2"].RawValueXDR)
}

func TestFilterBatching(t *testing.T) {
	watch := func(n int) []store.WatchedContract {
		out := make([]store.WatchedContract, n)
		for i := range out {
			out[i] = store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)}
		}
		return out
	}

	t.Run("no watched contracts means one match-all filter", func(t *testing.T) {
		ing := newTestIngester(&mockRPC{}, newMockStore(), Options{})
		batches, err := ing.buildFilterBatches(context.Background())
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0], 1)
		assert.Empty(t, batches[0][0].ContractIDs)
		assert.Equal(t, "contract", batches[0][0].Type)
	})

	t.Run("12 contracts fit one request as 3 filters", func(t *testing.T) {
		st := newMockStore()
		st.watched = watch(12)
		ing := newTestIngester(&mockRPC{}, st, Options{})
		batches, err := ing.buildFilterBatches(context.Background())
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0], 3)
		assert.Len(t, batches[0][0].ContractIDs, 5)
		assert.Len(t, batches[0][2].ContractIDs, 2)
	})

	t.Run("27 contracts spill into a second batch", func(t *testing.T) {
		st := newMockStore()
		st.watched = watch(27)
		ing := newTestIngester(&mockRPC{}, st, Options{})
		batches, err := ing.buildFilterBatches(context.Background())
		require.NoError(t, err)
		require.Len(t, batches, 2)
		assert.Len(t, batches[0], 5, "first batch maxes out at 5 filters")
		require.Len(t, batches[1], 1)
		assert.Len(t, batches[1][0].ContractIDs, 2)
	})
}

func TestWindowSweep_MultiBatch(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 27; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 5_000},
			{Events: []rpc.Event{rpcEvent("e2", 180)}, LatestLedger: 5_000},
		},
	}
	ing := newTestIngester(client, st, Options{StartLedger: 100, SweepWindow: 1_000, PageLimit: 100})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp, "window ends before the chain head")

	require.Len(t, client.eventsRequests, 2, "one request chain per filter batch")
	for _, req := range client.eventsRequests {
		assert.Equal(t, uint32(100), req.StartLedger)
		assert.Equal(t, uint32(1_100), req.EndLedger, "endLedger is exclusive: window [100,1099]")
	}
	assert.Len(t, st.events, 2)

	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(1_099), state.LastIngestedLedger)
	assert.Empty(t, state.LastCursor)
}

func TestReclamp_WhenResumePointAgedOut(t *testing.T) {
	client := &mockRPC{
		health:     rpc.Health{Status: "healthy", LatestLedger: 50_000, OldestLedger: 40_000},
		eventsErrs: []error{fmt.Errorf("getEvents: %w", &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 40000 - 50000"})},
	}
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))
	ing := newTestIngester(client, st, Options{})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err, "aged-out resume point is handled, not fatal")

	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(39_999), state.LastIngestedLedger,
		"next pass resumes from the oldest retained ledger")
	assert.Empty(t, state.LastCursor)
}

func TestRunOnce_PropagatesRPCErrors(t *testing.T) {
	client := &mockRPC{eventsErrs: []error{fmt.Errorf("boom")}}
	ing := newTestIngester(client, newMockStore(), Options{StartLedger: 100})

	_, err := ing.runOnce(context.Background())
	assert.ErrorContains(t, err, "boom")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 100}}}
	ing := newTestIngester(client, newMockStore(), Options{StartLedger: 50})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ing.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// The runtime API mutates the watched_contracts table mid-run. The
// ingester's next runOnce must re-read the watch list and issue
// getEvents with the new filter contractIds — no restart required.
func TestIngester_PicksUpRuntimeWatchListChange(t *testing.T) {
	contractB := "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	contractC := "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"

	// Two scripted responses: first when the watch list contains only A,
	// then when the runtime POST adds B between passes.
	client := &mockRPC{
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 100},
			{Events: []rpc.Event{rpcEvent("e2", 101)}, LatestLedger: 101},
		},
	}
	st := newMockStore()
	require.NoError(t, st.AddWatchedContract(context.Background(),
		"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	// Pass 1: list has only A.
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, client.eventsRequests, 1)
	req1 := client.eventsRequests[0]
	require.Len(t, req1.Filters, 1)
	require.Len(t, req1.Filters[0].ContractIDs, 1)
	assert.Equal(t,
		"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		req1.Filters[0].ContractIDs[0],
		"first pass issues a filter for A only")

	// Operator POSTs B and C; ingester does not know about this directly
	// — it just re-reads the list on the next pass.
	require.NoError(t, st.AddWatchedContract(context.Background(), contractB))
	require.NoError(t, st.AddWatchedContract(context.Background(), contractC))
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))

	// Pass 2: list has A, B, C. Without a restart, runOnce should pick
	// up the new IDs and issue a single batch carrying all three.
	_, err = ing.runOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, client.eventsRequests, 2,
		"the second pass must issue a fresh getEvents — the previous response was already consumed")
	req2 := client.eventsRequests[1]
	require.Len(t, req2.Filters, 1, "three contracts still fit one filter (cap is 5)")
	assert.ElementsMatch(t,
		[]string{
			"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			contractB, contractC,
		},
		req2.Filters[0].ContractIDs,
		"second pass picks up the newly-added contracts WITHOUT a restart")
}

// Symmetric coverage in the other direction: removing the only watched
// contract changes the filter shape from \"specific\" to \"all contracts\".
func TestIngester_PicksUpRuntimeWatchListRemoval(t *testing.T) {
	contractA := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	client := &mockRPC{
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 100},
			{Events: []rpc.Event{rpcEvent("e2", 101)}, LatestLedger: 101},
		},
	}
	st := newMockStore()
	require.NoError(t, st.AddWatchedContract(context.Background(), contractA))
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, st.watched, 1)

	// Operator DELETE's A; list transitions to empty.
	require.NoError(t, st.RemoveWatchedContract(context.Background(), contractA))
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))

	_, err = ing.runOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, client.eventsRequests, 2)
	req2 := client.eventsRequests[1]
	require.Len(t, req2.Filters, 1)
	assert.Empty(t, req2.Filters[0].ContractIDs,
		"empty watch list means ingest-all: contractIds must be empty")
	assert.Equal(t, "contract", req2.Filters[0].Type)
}
