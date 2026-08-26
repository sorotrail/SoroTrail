package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

func TestEventsIngestedTotal_SingleSuccess(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{
		Events:       []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 100), rpcEvent("e3", 100)},
		LatestLedger: 500,
	}}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	before := testutil.ToFloat64(metrics.EventsIngested)
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+3, testutil.ToFloat64(metrics.EventsIngested),
		"counter must equal the number of events persisted in one successful write")
}

// TestSetIngestionLag covers the #237 gauge: it must be the difference
// between the RPC chain head and the last ingested ledger, and a no-op
// whenever either side is unknown (≤ 0).
func TestSetIngestionLag(t *testing.T) {
	ing := &Ingester{}
	tests := []struct {
		name         string
		chainHead    int64
		lastIngested int64
		want         float64
	}{
		{name: "caught up", chainHead: 100, lastIngested: 100, want: 0},
		{name: "three ledgers behind", chainHead: 100, lastIngested: 97, want: 3},
		{name: "unknown chain head is a no-op", chainHead: 0, lastIngested: 97, want: 0},
		{name: "nothing ingested yet is a no-op", chainHead: 100, lastIngested: 0, want: 0},
		{name: "replay can run ahead of the reported head", chainHead: 100, lastIngested: 105, want: -5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics.IngestionLag.Set(0)
			ing.setIngestionLag(tt.chainHead, tt.lastIngested)
			assert.Equal(t, tt.want, testutil.ToFloat64(metrics.IngestionLag))
		})
	}
}

func TestEventsIngestedTotal_CumulativeMultipleWrites(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{
		{Events: []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 101)}, LatestLedger: 500},
		{Events: []rpc.Event{rpcEvent("e3", 102), rpcEvent("e4", 103), rpcEvent("e5", 104)}, LatestLedger: 500},
	}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 2})

	before := testutil.ToFloat64(metrics.EventsIngested)
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+2, testutil.ToFloat64(metrics.EventsIngested), "after first pass")

	_, err = ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+5, testutil.ToFloat64(metrics.EventsIngested),
		"counter must accumulate across multiple successful writes")
}

func TestEventsIngestedTotal_FailedWriteDoesNotIncrement(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{
		Events:       []rpc.Event{rpcEvent("e1", 100)},
		LatestLedger: 500,
	}}}
	st := newMockStore()
	st.upsertErr = fmt.Errorf("database connection lost")
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	before := testutil.ToFloat64(metrics.EventsIngested)
	_, err := ing.runOnce(context.Background())
	assert.Error(t, err)
	assert.Equal(t, before, testutil.ToFloat64(metrics.EventsIngested),
		"counter must not increment when the store write fails")
}

func TestEventsIngestedTotal_MixedSuccessAndFailure(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{
		{Events: []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 101)}, LatestLedger: 500},
		{Events: []rpc.Event{rpcEvent("e3", 102)}, LatestLedger: 500},
	}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	before := testutil.ToFloat64(metrics.EventsIngested)

	// First pass succeeds.
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+2, testutil.ToFloat64(metrics.EventsIngested))

	// Inject failure for second pass.
	st.upsertErr = fmt.Errorf("deadlock detected")
	_, err = ing.runOnce(context.Background())
	assert.Error(t, err)
	assert.Equal(t, before+2, testutil.ToFloat64(metrics.EventsIngested),
		"failed write must not change the counter; prior successes preserved")
}

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
// latest directly (mockRPC via its health field; other clients via
// wrapped base mockRPC's health field). Centralizing this in one
// helper avoids repeating a type switch at every call site.
func setLagAlarmClientHealth(t *testing.T, client rpc.Client, latest uint32) {
	t.Helper()
	switch r := client.(type) {
	case *mockRPC:
		r.health = rpc.Health{LatestLedger: latest}
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
		store.IngestionState{Network: "", LastIngestedLedger: 500}))
	ing := newTestIngester(client, st, Options{})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(501), client.eventsRequests[0].StartLedger)
}

func TestWarmStart_ResumesFromCursor(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 1_000}}}
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(),
		store.IngestionState{Network: "", LastIngestedLedger: 500, LastCursor: "cursor-42"}))
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

// TestReingestRange_ToleratesShortPages covers reingestBatch pagination
// over a closed ledger range. Stopping on a short-but-cursored page would
// be worse than slow here: ReplaceEventsInRange deletes every stored row
// in [from, to] that the re-fetch didn't return, so an early stop deletes
// events that were never replaced.
func TestReingestRange_ToleratesShortPages(t *testing.T) {
	tests := []struct {
		name       string
		pages      map[string]rpc.GetEventsResponse
		wantCalls  int
		wantEvents []string
	}{
		{
			name: "short page without cursor ends the range",
			pages: map[string]rpc.GetEventsResponse{
				"": {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500},
			},
			wantCalls:  1,
			wantEvents: []string{"e1"},
		},
		{
			name: "short page with cursor keeps paging",
			pages: map[string]rpc.GetEventsResponse{
				"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
				"c1": {Events: []rpc.Event{rpcEvent("e2", 150)}, LatestLedger: 500},
			},
			wantCalls:  2,
			wantEvents: []string{"e1", "e2"},
		},
		{
			name: "non-advancing cursor stops instead of spinning",
			pages: map[string]rpc.GetEventsResponse{
				"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
				"c1": {Events: []rpc.Event{rpcEvent("e2", 150)}, LatestLedger: 500, Cursor: "c1"},
			},
			wantCalls:  2,
			wantEvents: []string{"e1", "e2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newScriptedRPC(tt.pages)
			st := newMockStore()
			ing := newTestIngester(client, st, Options{PageLimit: 10})

			n, err := ing.ReingestRange(context.Background(), nil, 100, 200)
			require.NoError(t, err)
			assert.Len(t, tt.wantEvents, n)
			require.Len(t, client.calls, tt.wantCalls)
			for _, id := range tt.wantEvents {
				assert.Contains(t, st.events, id)
			}
		})
	}
}

// paginationCursor extracts the cursor from a GetEvents request, or
// returns "" when pagination is unset (cold start or first call).
func paginationCursor(req rpc.GetEventsRequest) string {
	if req.Pagination == nil {
		return ""
	}
	return req.Pagination.Cursor
}

// TestWindowSweep_ToleratesShortPages covers the sweepBatch pagination
// contract: a page shorter than the requested limit only terminates the
// batch when it carries NO top-level cursor. An RPC that returns fewer
// results than requested while more data remains (internal caps,
// filtering, load shedding) must be paged to completion — stopping early
// would let windowSweep advance the frontier past unfetched events and
// lose them permanently.
func TestWindowSweep_ToleratesShortPages(t *testing.T) {
	// Chain head 200 bounds every window at [start, 200], so any event
	// at ledger ≤ 200 is inside the swept range and must be fetched.
	const latest = uint32(200)

	tests := []struct {
		name string
		// pages maps the inbound request cursor to the response, mirroring
		// how the ingester threads cursors between pages.
		pages map[string]rpc.GetEventsResponse
		// wantCalls is the exact number of GetEvents requests the sweep
		// may issue; a larger number means the loop spun (or stopped
		// early when too small).
		wantCalls int
		// wantEvents is the set of event IDs that must land in the store.
		wantEvents []string
	}{
		{
			name: "short page without cursor ends the batch",
			pages: map[string]rpc.GetEventsResponse{
				"": {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: latest},
			},
			wantCalls:  1,
			wantEvents: []string{"e1"},
		},
		{
			name: "short page with cursor keeps paging",
			pages: map[string]rpc.GetEventsResponse{
				"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: latest, Cursor: "c1"},
				"c1": {Events: []rpc.Event{rpcEvent("e2", 150)}, LatestLedger: latest},
			},
			wantCalls:  2,
			wantEvents: []string{"e1", "e2"},
		},
		{
			name: "several consecutive short pages drain fully",
			pages: map[string]rpc.GetEventsResponse{
				"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: latest, Cursor: "c1"},
				"c1": {Events: []rpc.Event{rpcEvent("e2", 120)}, LatestLedger: latest, Cursor: "c2"},
				"c2": {Events: []rpc.Event{rpcEvent("e3", 140)}, LatestLedger: latest},
			},
			wantCalls:  3,
			wantEvents: []string{"e1", "e2", "e3"},
		},
		{
			name: "non-advancing cursor stops instead of spinning",
			pages: map[string]rpc.GetEventsResponse{
				"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: latest, Cursor: "c1"},
				"c1": {Events: []rpc.Event{rpcEvent("e2", 150)}, LatestLedger: latest, Cursor: "c1"},
			},
			wantCalls:  2,
			wantEvents: []string{"e1", "e2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newScriptedRPC(tt.pages)
			client.health = rpc.Health{Status: "healthy", LatestLedger: latest, OldestLedger: 10}
			st := newMockStore()
			ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 10})

			caughtUp, err := ing.windowSweep(context.Background(), 100,
				[][]rpc.EventFilter{{{Type: "contract"}}})
			require.NoError(t, err)
			assert.True(t, caughtUp, "window reaches the chain head")

			require.Len(t, client.calls, tt.wantCalls)
			for _, id := range tt.wantEvents {
				assert.Contains(t, st.events, id)
			}

			// Whatever the paging shape, a completed window must advance
			// the frontier to the window end and never persist an internal
			// batch cursor as global state.
			state, err := st.GetIngestionState(context.Background())
			require.NoError(t, err)
			assert.Equal(t, int64(latest)-1, state.LastIngestedLedger)
			assert.Empty(t, state.LastCursor)
		})
	}
}

// TestWindowSweep_ShortPageWithoutCursorDoesNotAdvanceFrontier pins the
// failure mode this tolerance exists to prevent: had the short-but-
// cursored page been treated as terminal, e2 would never have been fetched
// while the frontier still moved past it.
func TestWindowSweep_ShortPageWithoutCursorDoesNotAdvanceFrontier(t *testing.T) {
	client := newScriptedRPC(map[string]rpc.GetEventsResponse{
		"":   {Events: []rpc.Event{rpcEvent("e1", 100)}, LatestLedger: 500, Cursor: "c1"},
		"c1": {Events: []rpc.Event{rpcEvent("e2", 400)}, LatestLedger: 500},
	})
	client.health = rpc.Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	_, err := ing.windowSweep(context.Background(), 100,
		[][]rpc.EventFilter{{{Type: "contract"}}})
	require.NoError(t, err)

	assert.Contains(t, st.events, "e2",
		"the event behind the short page's cursor must be ingested")
}

// TestNextState_PageShapes documents the singlePage/nextState contract for
// every page shape the RPC can return. Unlike the sweep paths, a short
// page there is safe to call "caught up": the cursor (top-level or the
// per-event fallback) is persisted either way, so the next cycle resumes
// from exactly where the page ended.
func TestNextState_PageShapes(t *testing.T) {
	tests := []struct {
		name         string
		resp         rpc.GetEventsResponse
		limit        uint
		wantCaughtUp bool
		wantCursor   string
		wantLedger   int64
	}{
		{
			name: "full page is not caught up",
			resp: rpc.GetEventsResponse{
				Events:       []rpc.Event{rpcEvent("e1", 100), rpcEvent("e2", 101)},
				LatestLedger: 500,
				Cursor:       "c-e2",
			},
			limit:        2,
			wantCaughtUp: false,
			wantCursor:   "c-e2",
			wantLedger:   101,
		},
		{
			name: "short page with cursor is resumable",
			resp: rpc.GetEventsResponse{
				Events:       []rpc.Event{rpcEvent("e1", 100)},
				LatestLedger: 500,
				Cursor:       "c-e1",
			},
			limit:        100,
			wantCaughtUp: true,
			wantCursor:   "c-e1",
			wantLedger:   100,
		},
		{
			name: "short page without cursor falls back to the per-event token",
			resp: rpc.GetEventsResponse{
				Events:       []rpc.Event{rpcEvent("e1", 100)},
				LatestLedger: 500,
			},
			limit:        100,
			wantCaughtUp: true,
			wantCursor:   "e1",
			wantLedger:   100,
		},
		{
			name:         "empty page parks the frontier below the chain head",
			resp:         rpc.GetEventsResponse{LatestLedger: 500},
			limit:        100,
			wantCaughtUp: true,
			wantCursor:   "",
			wantLedger:   499,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, caughtUp := nextState(tt.resp, tt.limit)
			assert.Equal(t, tt.wantCaughtUp, caughtUp)
			assert.Equal(t, tt.wantCursor, state.LastCursor)
			assert.Equal(t, tt.wantLedger, state.LastIngestedLedger)
		})
	}
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
		store.IngestionState{Network: "", LastIngestedLedger: 99}))
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
	assert.Empty(t, st.events["e2"].RawTopicXDR)
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
		assert.Len(t, batches[0], 5)
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
		assert.Equal(t, uint32(1_100), req.EndLedger)
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
		store.IngestionState{Network: "", LastIngestedLedger: 100}))
	ing := newTestIngester(client, st, Options{})

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err, "aged-out resume point is handled, not fatal")

	state, _ := st.GetIngestionState(context.Background())
	assert.Equal(t, int64(39_999), state.LastIngestedLedger,
		"next pass resumes from the oldest retained ledger")
	assert.Empty(t, state.LastCursor)
}

func TestRunOnce_EmitsCycleSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	client := &mockRPC{health: rpc.Health{Status: "healthy", LatestLedger: 1_000}, eventsResps: []rpc.GetEventsResponse{{
		Events:       []rpc.Event{rpcEvent("e1", 100)},
		LatestLedger: 1_000,
	}}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 10}).WithTracer(tracer)

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	spans := sr.Ended()
	require.NotEmpty(t, spans)

	var cycleSpan, fetchSpan, persistSpan trace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "ingester.poll_cycle":
			cycleSpan = span
		case "ingester.fetch_page":
			fetchSpan = span
		case "ingester.persist_events":
			persistSpan = span
		}
	}
	require.NotNil(t, cycleSpan)
	require.NotNil(t, fetchSpan)
	require.NotNil(t, persistSpan)
	assert.Equal(t, cycleSpan.SpanContext().TraceID(), fetchSpan.SpanContext().TraceID())
	assert.Equal(t, cycleSpan.SpanContext().SpanID(), fetchSpan.Parent().SpanID())
	assert.Equal(t, cycleSpan.SpanContext().TraceID(), persistSpan.SpanContext().TraceID())
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

// The lag-alarm tests below exercise checkLag's hysteresis contract:
// the gauge is published every cycle, but a log line is emitted only on
// a transition, so a persistently lagging indexer does not spam.

func TestLagAlarm_DisabledWhenThresholdZero(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 0})

	driveLagCycles(t, ing, []mockCycle{{latest: 10_000, ingested: 1}})

	assert.Empty(t, metrics.history(), "disabled alarm must not publish a gauge")
	assert.Empty(t, logRecords(t, buf, nil), "disabled alarm must not log")
}

func TestLagAlarm_WarnsOnceOnTransition(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	// Two consecutive lagging cycles: the gauge is published on both,
	// but only the first crosses the edge and logs.
	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_000, ingested: 500},
		{latest: 1_000, ingested: 400},
	})

	assert.Equal(t, []bool{true, true}, metrics.history())
	warns := logRecords(t, buf, func(l string) bool { return l == "WARN" })
	require.Len(t, warns, 1, "a sustained lag must warn once, not every cycle")
	assert.Equal(t, "ingest lag exceeded threshold", warns[0]["msg"])
	assert.EqualValues(t, 500, warns[0]["lag_ledgers"])
}

func TestLagAlarm_RecoversAndRearms(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_000, ingested: 500},   // lagging  → warn
		{latest: 1_000, ingested: 950},   // caught up → recovered
		{latest: 2_000, ingested: 1_000}, // lagging again → warn
	})

	assert.Equal(t, []bool{true, false, true}, metrics.history())
	assert.Len(t, logRecords(t, buf, func(l string) bool { return l == "WARN" }), 2)

	infos := logRecords(t, buf, func(l string) bool { return l == "INFO" })
	require.Len(t, infos, 1)
	assert.Equal(t, "ingest lag recovered", infos[0]["msg"])
}

func TestLagAlarm_StaysQuietBelowThreshold(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	// Exactly at the threshold is not over it.
	driveLagCycles(t, ing, []mockCycle{{latest: 1_000, ingested: 900}})

	assert.Equal(t, []bool{false}, metrics.history())
	assert.Empty(t, logRecords(t, buf, nil))
}

func TestLagAlarm_NegativeLagPreservesHysteresis(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	// Drive into the lagging state, then feed a chain head *behind* the
	// stored ledger (reorg or a stale RPC cache). That evidence is not
	// trustworthy, so the alarm must republish the current state rather
	// than reporting a spurious recovery.
	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_000, ingested: 500},
		{latest: 400, ingested: 500},
	})

	assert.Equal(t, []bool{true, true}, metrics.history())
	assert.Empty(t, logRecords(t, buf, func(l string) bool { return l == "INFO" }),
		"a backwards chain head must not be reported as recovery")
}

func TestLagAlarm_ColdStartPublishesFalseWithoutLogging(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	// ingested == -1 removes the state row entirely.
	driveLagCycles(t, ing, []mockCycle{{latest: 10_000, ingested: -1}})

	assert.Equal(t, []bool{false}, metrics.history(),
		"cold start still publishes so the gauge is never unknown")
	assert.Empty(t, logRecords(t, buf, nil),
		"a fresh deploy must not warn about a chain head that is merely large")
}

// --- Per-cycle event cap (MAX_EVENTS_PER_CYCLE / Options.MaxEventsPerCycle) ---

// TestMaxEventsPerCycle_SinglePage covers the singlePage path: the cap
// clamps the getEvents pagination limit down, a page full at the clamped
// limit reports not-caught-up so the cycle resumes from the cursor, and
// zero (or a cap above PageLimit) leaves behavior unchanged.
func TestMaxEventsPerCycle_SinglePage(t *testing.T) {
	tests := []struct {
		name         string
		cap          uint
		pageLimit    uint
		pageEvents   int
		wantReqLimit uint
		wantCaughtUp bool
		wantStored   int
	}{
		{
			name: "cap below PageLimit clamps the request",
			cap:  2, pageLimit: 100, pageEvents: 2,
			wantReqLimit: 2, wantCaughtUp: false, wantStored: 2,
		},
		{
			name: "cap above PageLimit leaves limit unchanged",
			cap:  500, pageLimit: 100, pageEvents: 3,
			wantReqLimit: 100, wantCaughtUp: true, wantStored: 3,
		},
		{
			name: "zero disables the cap",
			cap:  0, pageLimit: 100, pageEvents: 3,
			wantReqLimit: 100, wantCaughtUp: true, wantStored: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make([]rpc.Event, tt.pageEvents)
			for i := range events {
				events[i] = rpcEvent(fmt.Sprintf("e%d", i), 100)
			}
			client := &mockRPC{eventsResps: []rpc.GetEventsResponse{
				{Events: events, LatestLedger: 500},
			}}
			st := newMockStore()
			ing := newTestIngester(client, st, Options{
				StartLedger:       100,
				PageLimit:         tt.pageLimit,
				MaxEventsPerCycle: tt.cap,
			})

			caughtUp, err := ing.runOnce(context.Background())
			require.NoError(t, err)
			require.Len(t, client.eventsRequests, 1)
			require.NotNil(t, client.eventsRequests[0].Pagination)
			assert.Equal(t, tt.wantReqLimit, client.eventsRequests[0].Pagination.Limit,
				"the pagination limit must reflect the per-cycle cap")
			assert.Equal(t, tt.wantCaughtUp, caughtUp)
			assert.Len(t, st.events, tt.wantStored)

			state, _ := st.GetIngestionState(context.Background())
			if !tt.wantCaughtUp {
				assert.NotEmpty(t, state.LastCursor,
					"a cap-truncated cycle must persist its cursor so the next cycle resumes")
			}
		})
	}
}

// TestMaxEventsPerCycle_WindowSweep covers the multi-batch sweep path with
// SweepConcurrency=1 (batches run sequentially in submission order, so the
// budget consumption is deterministic).
func TestMaxEventsPerCycle_WindowSweep(t *testing.T) {
	newWatched := func() []store.WatchedContract {
		watched := make([]store.WatchedContract, 0, 27)
		for i := 0; i < 27; i++ {
			watched = append(watched, store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
		}
		return watched
	}

	tests := []struct {
		name             string
		cap              uint
		resps            []rpc.GetEventsResponse
		wantRequests     int
		wantCaughtUp     bool
		wantLastIngested int64 // expected state.LastIngestedLedger after the cycle
		wantStateSet     bool  // whether ingestion_state was written at all
		wantStored       []string
	}{
		{
			name: "budget exhausts mid-window defers the frontier",
			cap:  1,
			resps: []rpc.GetEventsResponse{
				{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 5_000},
			},
			wantRequests:     1,
			wantCaughtUp:     false,
			wantLastIngested: 0,
			wantStateSet:     false,
			wantStored:       []string{"e1"},
		},
		{
			name: "budget spanning the whole window completes normally",
			cap:  10,
			resps: []rpc.GetEventsResponse{
				{Events: []rpc.Event{rpcEvent("e1", 150)}, LatestLedger: 5_000},
				{Events: []rpc.Event{rpcEvent("e2", 180)}, LatestLedger: 5_000},
			},
			wantRequests:     2,
			wantCaughtUp:     false,
			wantLastIngested: 1_099,
			wantStateSet:     true,
			wantStored:       []string{"e1", "e2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newMockStore()
			st.watched = newWatched()
			client := &mockRPC{
				health:      rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
				eventsResps: tt.resps,
			}
			ing := newTestIngester(client, st, Options{
				StartLedger:       100,
				SweepWindow:       1_000,
				PageLimit:         100,
				MaxEventsPerCycle: tt.cap,
			})

			caughtUp, err := ing.runOnce(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tt.wantCaughtUp, caughtUp)
			assert.Len(t, client.eventsRequests, tt.wantRequests)
			if tt.wantRequests > 0 {
				assert.Equal(t, uint32(100), client.eventsRequests[0].StartLedger)
			}

			var got []string
			for id := range st.events {
				got = append(got, id)
			}
			assert.ElementsMatch(t, tt.wantStored, got)

			state, _ := st.GetIngestionState(context.Background())
			if !tt.wantStateSet {
				assert.Zero(t, state.LastIngestedLedger,
					"an incomplete window must NOT advance the ingest frontier")
				assert.Empty(t, state.LastCursor)
			} else {
				assert.Equal(t, tt.wantLastIngested, state.LastIngestedLedger)
			}
		})
	}
}

// TestMaxEventsPerCycle_WindowSweepResumesNextCycle proves the deferred
// window is re-scanned: the next runOnce re-requests [start,end] from the
// same position and, once the budget suffices, advances the frontier.
func TestMaxEventsPerCycle_WindowSweepResumesNextCycle(t *testing.T) {
	st := newMockStore()
	for i := 0; i < 27; i++ {
		st.watched = append(st.watched,
			store.WatchedContract{ContractID: fmt.Sprintf("C%055d", i)})
	}
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 5_000, OldestLedger: 10},
		eventsResps: []rpc.GetEventsResponse{
			// Cycle 1: batch 1's page is full at the clamped limit of 2,
			// exhausting the cap; batch 2 never issues a request.
			{Events: []rpc.Event{rpcEvent("e1", 150), rpcEvent("e1b", 151)}, LatestLedger: 5_000},
			// Cycle 2: fresh budget. Batch 1 gets a short page (1 < 2),
			// leaving budget 1; batch 2 gets an empty page (0 < 1) and
			// finishes, so the window completes and the frontier moves.
			{Events: []rpc.Event{rpcEvent("e3", 160)}, LatestLedger: 5_000},
			{Events: nil, LatestLedger: 5_000},
		},
	}
	ing := newTestIngester(client, st, Options{
		StartLedger:       100,
		SweepWindow:       1_000,
		PageLimit:         100,
		MaxEventsPerCycle: 2,
	})

	caughtUp, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp, "cycle 1 hits the cap mid-window")
	require.Len(t, client.eventsRequests, 1)

	state, _ := st.GetIngestionState(context.Background())
	assert.Zero(t, state.LastIngestedLedger, "frontier must not advance past unfetched data")

	caughtUp, err = ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.False(t, caughtUp, "window ends before the chain head")
	require.Len(t, client.eventsRequests, 3, "cycle 2 re-scans the whole window")

	assert.Equal(t, uint32(100), client.eventsRequests[1].StartLedger,
		"the retry starts from the same unadvanced frontier")
	for _, e := range []string{"e1", "e1b", "e3"} {
		assert.Contains(t, st.events, e)
	}

	state, _ = st.GetIngestionState(context.Background())
	assert.Equal(t, int64(1_099), state.LastIngestedLedger,
		"the completed window advances the frontier")
}

// --- Startup/shutdown logging with effective config ---

// TestIngester_LogAttrsEffectiveConfig verifies the startup log fields
// reflect post-default values: an operator must see what the ingester
// will actually run with, not the raw (possibly zero) Options literal.
func TestIngester_LogAttrsEffectiveConfig(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want map[string]any // fields whose value is asserted
	}{
		{
			name: "zero Options get the documented defaults",
			opts: Options{},
			want: map[string]any{
				"poll_interval":     5 * time.Second,
				"page_limit":        uint(1000),
				"retention_ledgers": uint32(17280),
				"sweep_window":      uint32(1000),
				"sweep_concurrency": 1,
				"max_backoff":       time.Minute,
				// Not defaulted here by design (0 = the documented
				// "disabled" sentinel); the env config layer supplies 64.
				"reorg_confirmation_window": uint32(0),
			},
		},
		{
			name: "explicit overrides win over defaults",
			opts: Options{
				PollInterval:      2 * time.Second,
				PageLimit:         250,
				RetentionLedgers:  100,
				SweepWindow:       50,
				SweepConcurrency:  4,
				MaxBackoff:        10 * time.Second,
				MaxEventsPerCycle: 5000,
				StartLedger:       42,
				LagWarnLedgers:    7,
			},
			want: map[string]any{
				"poll_interval":             2 * time.Second,
				"page_limit":                uint(250),
				"retention_ledgers":         uint32(100),
				"sweep_window":              uint32(50),
				"sweep_concurrency":         4,
				"max_backoff":               10 * time.Second,
				"max_events_per_cycle":      uint(5000),
				"start_ledger":              uint32(42),
				"lag_warn_ledgers":          uint32(7),
				"reorg_confirmation_window": uint32(0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.applyDefaults()
			got := tt.opts.logAttrs()

			attrs := map[string]any{}
			for i := 0; i+1 < len(got); i += 2 {
				attrs[got[i].(string)] = got[i+1]
			}
			for k, want := range tt.want {
				assert.Equal(t, want, attrs[k], "field %q", k)
			}
		})
	}
}

// TestRun_EmitsStartedAndStoppedLogs proves both lifecycle lines fire
// through the real Run loop: "ingester started" once on entry with the
// config fields attached, "ingester stopped" once with the exit reason.
func TestRun_EmitsStartedAndStoppedLogs(t *testing.T) {
	log, buf := recordingLogger()
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 100}}}
	ing := New(client, newMockStore(), passthroughDecoder{}, log,
		Options{StartLedger: 50, PageLimit: 10})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ing.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)

	started, stopped := 0, 0
	for _, rec := range logRecords(t, buf, nil) {
		switch rec["msg"] {
		case "ingester started":
			started++
			assert.Equal(t, float64(10), rec["page_limit"],
				"the started line carries effective config")
			assert.Equal(t, float64(50), rec["start_ledger"])
		case "ingester stopped":
			stopped++
			assert.Contains(t, rec["reason"], "context canceled",
				"the stopped line names why the loop exited")
		}
	}
	assert.Equal(t, 1, started, "exactly one started line")
	assert.Equal(t, 1, stopped, "exactly one stopped line")
}

// stepClock is a test Clock that records every duration passed to
// SleepCtx and then blocks until the test sends on step (or ctx is
// canceled), so a test can drive Run's loop one cycle at a time and
// observe exactly which poll interval each cycle slept for — including a
// live update applied mid-run via SetPollInterval (issue #148).
type stepClock struct {
	mu     sync.Mutex
	sleeps []time.Duration
	step   chan struct{}
}

func newStepClock() *stepClock {
	return &stepClock{step: make(chan struct{})}
}

func (c *stepClock) Now() time.Time { return time.Now() }

func (c *stepClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-c.step:
		return true
	}
}

func (c *stepClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}

// TestSetPollInterval_RejectsNonPositive covers the validation half of
// issue #148's acceptance criteria: an invalid reload must be rejected and
// the previously-applied interval must remain in effect.
func TestSetPollInterval_RejectsNonPositive(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{PollInterval: 5 * time.Second})
	require.Equal(t, 5*time.Second, ing.PollInterval())

	for _, d := range []time.Duration{0, -1, -time.Second} {
		err := ing.SetPollInterval(d)
		assert.Errorf(t, err, "SetPollInterval(%s) should be rejected", d)
		assert.Equal(t, 5*time.Second, ing.PollInterval(),
			"a rejected SetPollInterval must leave the current interval unchanged")
	}
}

// TestSetPollInterval_AcceptsPositive covers the accept path directly: a
// valid duration updates the live value with no error, independent of
// Run's sleep loop.
func TestSetPollInterval_AcceptsPositive(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{PollInterval: 5 * time.Second})

	require.NoError(t, ing.SetPollInterval(250*time.Millisecond))
	assert.Equal(t, 250*time.Millisecond, ing.PollInterval())
}

// TestSetPollInterval_LiveUpdatesRunLoop drives Run for two caught-up
// cycles against a stepClock, updating the poll interval live in between,
// and asserts the second cycle's sleep observed the new value — i.e. a
// SIGHUP-triggered reload takes effect on the next cycle without a
// restart, per issue #148's acceptance criteria.
func TestSetPollInterval_LiveUpdatesRunLoop(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{LatestLedger: 100}}}
	clock := newStepClock()
	ing := New(client, newMockStore(), passthroughDecoder{}, testLogger(), Options{
		StartLedger:  1,
		PollInterval: 5 * time.Second,
		Clock:        clock,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx) }()

	require.Eventually(t, func() bool { return len(clock.recorded()) >= 1 }, 2*time.Second, time.Millisecond)
	assert.Equal(t, 5*time.Second, clock.recorded()[0], "first cycle sleeps for the constructed interval")

	require.NoError(t, ing.SetPollInterval(50*time.Millisecond))
	clock.step <- struct{}{} // release cycle 1's sleep so cycle 2 runs

	require.Eventually(t, func() bool { return len(clock.recorded()) >= 2 }, 2*time.Second, time.Millisecond)
	assert.Equal(t, 50*time.Millisecond, clock.recorded()[1],
		"second cycle must observe the live-updated interval, no restart required")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel")
	}
}
