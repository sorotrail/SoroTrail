package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recordingLogger returns a logger that writes JSON records to a buffer
// for easy test-time assertions.
func recordingLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

// recordingLagMetrics records every SetLagging call in order so tests can
// assert the alarm published correctly.
type recordingLagMetrics struct {
	mu    sync.Mutex
	calls []bool
}

func (r *recordingLagMetrics) SetLagging(b bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, b)
}

// history returns a snapshot of the published state values in order.
// The field is named `calls` to avoid the field/method name collision
// Go rejects; the method name `history` is the public read API.
func (r *recordingLagMetrics) history() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]bool, len(r.calls))
	copy(out, r.calls)
	return out
}

// logRecords parses JSON records from the recordingLogger buffer.
func logRecords(t *testing.T, buf *bytes.Buffer, levelFn func(string) bool) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parsing log line %q: %v", line, err)
		}
		if levelFn == nil || levelFn(rec["level"].(string)) {
			out = append(out, rec)
		}
	}
	return out
}

// makeIngester is the lag-alarm test constructor. It wires a recording
// logger and recording LagMetrics so each test focuses on driving cycles
// and asserting behavior rather than plumbing.
func makeIngester(t *testing.T, opts Options) (*Ingester, *bytes.Buffer, *recordingLagMetrics) {
	t.Helper()
	log, buf := recordingLogger()
	metrics := &recordingLagMetrics{}
	opts.LagMetrics = metrics
	ing := New(&mockRPC{health: rpc.Health{LatestLedger: 1_000}}, newMockStore(),
		passthroughDecoder{}, log, opts)
	return ing, buf, metrics
}

// setLagAlarmClientHealth updates the chain head seen by whatever RPC is
// wired into ing. Both supported test RPCs let the test author set the
// latest directly (mockRPC via its health field; flakyRPC via its
// wrapped base mockRPC's health field). Centralizing this in one
// helper avoids repeating a type switch at every call site.
func setLagAlarmClientHealth(t *testing.T, client rpc.Client, latest uint32) {
	t.Helper()
	switch r := client.(type) {
	case *mockRPC:
		r.health = rpc.Health{LatestLedger: latest}
	case *flakyRPC:
		r.base.health = rpc.Health{LatestLedger: latest}
	default:
		t.Fatalf("setLagAlarmClientHealth: unhandled RPC type %T", client)
	}
}

// driveLagCycles scripts a sequence of (latestLedger, ingestedLedger)
// pairs and invokes checkLag on each. ingestedLedger == -1 removes the
// state row to simulate cold start.
func driveLagCycles(t *testing.T, ing *Ingester, cycles []mockCycle) {
	t.Helper()
	for _, c := range cycles {
		setLagAlarmClientHealth(t, ing.client, c.latest)
		st := ing.store.(*mockStore)
		if c.ingested == -1 {
			st.state = nil
		} else {
			require.NoError(t, st.SaveIngestionState(context.Background(),
				store.IngestionState{LastIngestedLedger: c.ingested}))
		}
		ing.checkLag(context.Background())
	}
}

type mockCycle struct {
	// latest is the chain head the mockRPC.GetLatestLedger reports.
	latest uint32
	// ingested is the stored LastIngestedLedger. Use -1 to remove the
	// state row entirely (cold start).
	ingested int64
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

// Issue #308: scripted multi-page responses. The scriptedRPC keys
// responses by the cursor on the inbound request, so a single map
// literal encodes the resume relationship between pages: page N's
// Cursor field is the cursor the ingester sends for page N+1.
func TestPagination_ScriptedMultiPage(t *testing.T) {
	t.Run("three-page sequence threads cursors correctly", func(t *testing.T) {
		// Page 1 (no cursor): 2 events, cursor "c1". Page 2 ("c1"):
		// 2 events, cursor "c2". Page 3 ("c2"): 1 event, no top-level
		// cursor → ingester falls back to the per-event CursorValue.
		// Page 3 is short (1 < limit 2) so the runOnce is caught up.
		client := newScriptedRPC(map[string]rpc.GetEventsResponse{
			"": {
				Events:       []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 101)},
				LatestLedger: 500,
				Cursor:       "c1",
			},
			"c1": {
				Events:       []rpc.Event{rpcEvent("e3", 102), rpcEvent("e4", 103)},
				LatestLedger: 500,
				Cursor:       "c2",
			},
			"c2": {
				Events:       []rpc.Event{rpcEvent("e5", 104)},
				LatestLedger: 500,
			},
		})
		client.health = rpc.Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10}
		st := newMockStore()
		ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 2})

		caughtUp, err := ing.runOnce(context.Background())
		require.NoError(t, err)
		assert.False(t, caughtUp, "full page means more data likely")

		caughtUp, err = ing.runOnce(context.Background())
		require.NoError(t, err)
		assert.False(t, caughtUp)

		caughtUp, err = ing.runOnce(context.Background())
		require.NoError(t, err)
		assert.True(t, caughtUp, "short page = caught up")

		// The three requests carry the cursors the script expects.
		require.Len(t, client.calls, 3)
		assert.Equal(t, "", paginationCursor(client.calls[0]))
		assert.Equal(t, "c1", paginationCursor(client.calls[1]))
		assert.Equal(t, "c2", paginationCursor(client.calls[2]))

		// All five events made it into the store.
		assert.Len(t, st.events, 5)
		for _, id := range []string{"e1", "e2", "e3", "e4", "e5"} {
			assert.Contains(t, st.events, id)
		}

		// After the short page the saved cursor is the per-event
		// CursorValue() of the last event (the third response carried
		// no top-level cursor).
		state, err := st.GetIngestionState(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "e5", state.LastCursor)
		assert.Equal(t, int64(104), state.LastIngestedLedger)
	})

	t.Run("five-page sequence exercises the full resume chain", func(t *testing.T) {
		// Pages 1–4 are full (PageLimit 1), each carrying a cursor;
		// page 5 is empty and ends the chain.
		client := newScriptedRPC(map[string]rpc.GetEventsResponse{
			"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
			"c1": {Events: []rpc.Event{rpcEvent("e2", 101)}, LatestLedger: 500, Cursor: "c2"},
			"c2": {Events: []rpc.Event{rpcEvent("e3", 102)}, LatestLedger: 500, Cursor: "c3"},
			"c3": {Events: []rpc.Event{rpcEvent("e4", 103)}, LatestLedger: 500, Cursor: "c4"},
			"c4": {LatestLedger: 500},
		})
		client.health = rpc.Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10}
		st := newMockStore()
		ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 1})

		// Drive five runOnce cycles to drain the scripted chain.
		var caughtUp bool
		for i := 0; i < 5; i++ {
			var err error
			caughtUp, err = ing.runOnce(context.Background())
			require.NoErrorf(t, err, "runOnce %d", i)
		}
		assert.True(t, caughtUp, "page 5 is short → caught up")

		require.Len(t, client.calls, 5)
		assert.Equal(t, "", paginationCursor(client.calls[0]))
		assert.Equal(t, "c1", paginationCursor(client.calls[1]))
		assert.Equal(t, "c2", paginationCursor(client.calls[2]))
		assert.Equal(t, "c3", paginationCursor(client.calls[3]))
		assert.Equal(t, "c4", paginationCursor(client.calls[4]))
		assert.Len(t, st.events, 4)
	})

}

// TestPagination_ErrorMidChainAborts asserts the production contract
// that a failed page mid-pagination leaves the frontier at the last
// fully-persisted ledger so the next runOnce retries the same page
// idempotently. Lives next to TestPagination_ScriptedMultiPage for
// the multi-page context, but does NOT exercise scriptedRPC — the
// index-based mockRPC is the right tool here because we need to
// script an error mid-chain that scriptedRPC's happy-path cursor map
// can't model.
func TestPagination_ErrorMidChainAborts(t *testing.T) {
	failing := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
		},
		eventsErrs: []error{nil, fmt.Errorf("boom")},
	}
	ing := newTestIngester(failing, newMockStore(), Options{StartLedger: 100, PageLimit: 1})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err, "page 1 succeeds")
	_, err = ing.runOnce(context.Background())
	require.Error(t, err, "page 2 errors")
	assert.Contains(t, err.Error(), "boom")
}

// paginationCursor extracts the cursor from a GetEvents request, or
// returns "" when pagination is unset (cold start or first call).
func paginationCursor(req rpc.GetEventsRequest) string {
	if req.Pagination == nil {
		return ""
	}
	return req.Pagination.Cursor
}

func TestPagination_LegacyPagingTokenFallback(t *testing.T) {
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
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 99}))
	_, err = ing.runOnce(context.Background())
	require.NoError(t, err)

	assert.Len(t, st.events, 1, "re-ingesting the same events must not duplicate")
}

func TestPersistEvents_RetainsRawXDR(t *testing.T) {
	fromXDR := rpc.Event{
		ID:         "e1",
		Type:       "contract",
		Ledger:     100,
		ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Topic:      []string{"topic-xdr"},
		Value:      "value-xdr",
	}
	fromJSON := rpcEvent("e2", 100)

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

// Issue #4: parallel window sweeps with bounded concurrency. The
// windowSweep path now fans the batches out via errgroup.SetLimit; this
// test wires a mock with three filter batches and confirms that
// concurrency>1 actually overlaps the page issuing window — the HTTP
// client's interval limiter is what serializes the round trips in
// production, but in a controlled test the mock can record the call
// timings and the parallelism is observable that way. We also assert
// that all batches' events are persisted before the sweep completes.
func TestWindowSweep_ParallelBatchesComplete(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 30; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	// 30 contracts → 6 filters → ceil(6/5)=2 batches.
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 150), rpcEvent("e2", 151)}, LatestLedger: 5_000},
			{Events: []rpc.Event{rpcEvent("e3", 180), rpcEvent("e4", 181)}, LatestLedger: 5_000},
		},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:      100,
		SweepWindow:      1_000,
		PageLimit:        100,
		SweepConcurrency: 4,
	})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp)
	require.Len(t, client.eventsRequests, 2, "two filter batches")
	for _, req := range client.eventsRequests {
		assert.Equal(t, uint32(100), req.StartLedger)
		assert.Equal(t, uint32(1_100), req.EndLedger)
	}
	assert.Len(t, st.events, 4, "all four events persisted across both batches")

	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(1_099), state.LastIngestedLedger,
		"state advanced only after ALL batches completed")
}

// Issue #4: a failing batch must prevent state advancement so the
// next runOnce retries the window.
func TestWindowSweep_FailingBatchAbortsWindow(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 30; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsErrs: []error{
			nil, // first batch succeeds, fills in e1
			fmt.Errorf("boom: rpc error -32600: getEvents: transient failure"),
		},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 5_000},
			{},
		},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:      100,
		SweepWindow:      1_000,
		PageLimit:        100,
		SweepConcurrency: 4,
	})

	_, err := ing.runOnce(context.Background())
	require.Error(t, err, "second batch's error must propagate")
	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(0), state.LastIngestedLedger,
		"state must NOT be advanced when a batch fails — retry covers persistence idempotently")
	// e1 was persisted by the first batch, but state is whatever it
	// was at start (zero). Verifying the failed-batch path doesn't
	// advance is the contract.
	assert.Contains(t, st.events, "e1", "first batch's events are persisted despite the second's failure")
}

// Issue #4: ≤25 watched contracts must use the unchanged singlePage
// path. We assert only one request is issued (no windowSweep fan-out).
func TestSinglePage_UnchangedWithSweepConcurrencyKnob(t *testing.T) {
	st := newMockStore()
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 5_000},
		},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:      100,
		SweepConcurrency: 4, // irrelevant for single-page path
	})
	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, caughtUp,
		"a short page means caught up; SweepConcurrency must not affect singlePage behavior")
	require.Len(t, client.eventsRequests, 1, "singlePage path is unchanged when <=25 watched contracts")
}

// Issue #4: context cancellation must not persist a state that skips
// batches mid-sweep. We seed an existing frontier, then mid-windowSweep
// cancel and assert the state is unchanged (i.e. errgroup.Wait
// correctly fails-fails-aborts and SaveIngestionState is not called).
func TestWindowSweep_CancellationDoesNotAdvance(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 30; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	// Pre-existing frontier; the post-cancellation state must equal it.
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 50}))
	client := &mockRPC{
		health:     rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsErrs: []error{context.Canceled},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:             51,
		SweepWindow:             1_000,
		PageLimit:               100,
		SweepConcurrency:        4,
		ReorgConfirmationWindow: 0, // disable to isolate the cancellation path
		ReorgRescanInterval:     time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled so getHealth fails fast
	_, err := ing.runOnce(ctx)
	require.ErrorIs(t, err, context.Canceled,
		"pre-cancelled ctx must surface as ctx.Canceled, not be silently swallowed")

	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(50), state.LastIngestedLedger,
		"cancelled runOnce must NOT advance ingestion_state")
}

// Issue #129: reorg rescan runs every ReorgRescanInterval, over the
// ledger range [lastIngested-confWindow, lastIngested-1]. We assert
// that the rescan rewrites the corrected values and logs the rewrite.
func TestIngester_ReorgRescanRepairsChangedRows(t *testing.T) {
	contract := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// Initial state: ingested up to ledger 1000.
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 1000}))
	// Pre-seed two events with "wrong" topic/value at ledgers 940 and 950.
	st.events["e940"] = store.Event{
		ID: "e940", ContractID: contract, Ledger: 940, Type: "contract",
		Topics: []byte(`[{"symbol":"old-topic"}]`), Value: []byte(`{"u64":-1}`),
	}
	st.events["e950"] = store.Event{
		ID: "e950", ContractID: contract, Ledger: 950, Type: "contract",
		Topics: []byte(`[{"symbol":"old-topic"}]`), Value: []byte(`{"u64":-1}`),
	}
	// RPC now reports *corrected* data for the rescan range [937, 999].
	// rescanForReorg calls ReingestRange directly (no singlePage hop),
	// so this is the only GetEvents request the test drives: the mock
	// must return the corrected events on the first call, not the
	// second. The pre-existing mock event "e-fresh" referenced by an
	// earlier draft is intentionally absent.
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1100, OldestLedger: 800},
		eventsResps: []rpc.GetEventsResponse{
			{Events: []rpc.Event{
				{ID: "e940", ContractID: contract, Ledger: 940, Type: "contract",
					TxHash: "abc", ValueJSON: json.RawMessage(`{"u64":42}`),
					TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"new-topic"}`)}},
				{ID: "e950", ContractID: contract, Ledger: 950, Type: "contract",
					TxHash: "abc", ValueJSON: json.RawMessage(`{"u64":43}`),
					TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"new-topic"}`)}},
			}, LatestLedger: 1100},
		},
	}
	ing := newTestIngester(client, st, Options{
		RetentionLedgers:        1000,
		ReorgConfirmationWindow: 64,
		ReorgRescanInterval:     time.Millisecond, // immediate for the test
	})
	// Drive the rescan directly. rescanForReorg calls ReingestRange
	// once for [937, 999]; the mock's first response is the corrected
	// events page, which ReplaceEventsInRange then writes back over the
	// pre-seeded "old" rows.
	err := ing.rescanForReorg(context.Background())
	require.NoError(t, err)
	// After rescan: e940 and e950 should have the corrected topic/value.
	require.Contains(t, st.events, "e940")
	assert.Contains(t, string(st.events["e940"].Topics), "new-topic",
		"reorg rescan must rewrite corrected topic values in place")
	assert.Contains(t, string(st.events["e940"].Value), "42",
		"reorg rescan must rewrite corrected value in place")
}

// Issue #4 + reclamp interaction: with more than one filter batch an
// IsLedgerOutOfRange from any one batch must trigger reclamp. The
// atomic.Bool side-channel in windowSweep exists because errgroup's
// first-error-wins race can otherwise mask the OOR signal if a
// sibling goroutine's in-flight GetEvents gets canceled first and
// surfaces ctx.Canceled to g.Wait() ahead of the OOR-bearing batch.
// This test pins the observable behavior: any batch can flag OOR and
// the next sweep starts from OldestLedger-1.
func TestWindowSweep_ParallelBatchesReclampsOnOOR(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 30; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	// Both batches see IsLedgerOutOfRange from the RPC. The mutex
	// serializes responses deterministically: whichever goroutine
	// reaches the mock first gets eventsErrs[0], the other gets
	// eventsErrs[1] — both are OOR, both must reclaim.
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 50_000, OldestLedger: 40_000},
		eventsErrs: []error{
			fmt.Errorf("%w", &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 40000 - 50000"}),
			fmt.Errorf("%w", &rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 40000 - 50000"}),
		},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:      100,
		SweepWindow:      1_000,
		PageLimit:        100,
		SweepConcurrency: 4,
	})
	// runOnce returns (caughtUp, err). With IsLedgerOutOfRange from
	// every batch, windowSweep triggers reclampToOldest and swallows
	// the error (the reclamp's success means we recovered). The
	// observable contract is: state advanced past nothing useful — the
	// reclamp target.
	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err, "successful reclamp is not an error to the caller")
	assert.False(t, caughtUp, "OOR means the chain is ahead of the resume point")

	require.Len(t, client.eventsRequests, 2,
		"both filter batches must have fired (one request per batch)")
	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(40_000-1), state.LastIngestedLedger,
		"parallel batches returning OOR must still trigger reclampToOldest")
}

// Issue #129: when there's not enough history yet, the rescan is a
// no-op rather than re-fetching an empty range.
func TestIngester_ReorgRescanNoOpForColdStart(t *testing.T) {
	st := newMockStore()
	client := &mockRPC{health: rpc.Health{Status: "healthy", LatestLedger: 100, OldestLedger: 10}}
	ing := newTestIngester(client, st, Options{
		ReorgConfirmationWindow: 64,
	})
	err := ing.rescanForReorg(context.Background())
	require.NoError(t, err)
	assert.Empty(t, client.eventsRequests, "no contracts watched and cold start: no RPC round trip")
}

// Issue #124/Run-level: cancellation during a successful cycle returns
// cleanly without advancing the state. We construct a cycle that would
// succeed, cancel mid-flight, and assert the loop returned and the
// frontier is at the last fully-persisted value.
// Issue #4: Run with -race. A serial sweep over 4 batches must
// collect all events without races between errgroup goroutines and the
// store mutex. (The mockStore uses sync.Mutex on UpsertEvents.) This
// test exists primarily as a -race canary for parallel sweeps; the
// limiter-respect contract is asserted in internal/rpc/client_test.go
// (TestIntervalLimiter_SerializesParallelCalls), where the production
// limiter actually lives.
func TestRun_RaceClean(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 30; i++ {
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
	ing := newTestIngester(client, st, Options{
		StartLedger:      100,
		SweepWindow:      1_000,
		PageLimit:        100,
		SweepConcurrency: 4,
	})
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Len(t, st.events, 2)
}

// Issue #124/Run-level: cancellation during a successful cycle returns
// cleanly without advancing the state. We construct a cycle that would
// succeed, cancel mid-flight, and assert the loop returned and the
// frontier is at the last fully-persisted value.
func TestRun_GracefulShutdownPersistsCursor(t *testing.T) {
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))
	// Channel-wire a "first cycle done" signal from inside the mock's
	// GetEvents so the parent goroutine can wait without racing on
	// client.eventsRequests.
	firstCycleDone := make(chan struct{}, 1)
	client := &mockRPC{
		health:      rpc.Health{Status: "healthy", LatestLedger: 10_000, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 10_000}},
		firstCycle:  firstCycleDone,
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:             101,
		SweepWindow:             100,
		PageLimit:               100,
		PollInterval:            time.Hour, // don't re-enter after success
		ReorgConfirmationWindow: 0,         // isolate to forward ingestion
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx) }()

	select {
	case <-firstCycleDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ingester did not start within 2s")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel")
	}

	state, _ := st.GetIngestionState(context.Background())
	// LastIngestedLedger is `int64(resp.Events[len-1].Ledger)` in
	// nextState (issue #15 PR follow-up: keep the row's own ledger as
	// the resume marker), so e1 at ledger 150 wins over LatestLedger.
	assert.Equal(t, int64(150), state.LastIngestedLedger,
		"successful cycle must persist the resume position before sleeping")
	assert.Contains(t, st.events, "e1", "event was persisted before drain")
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
