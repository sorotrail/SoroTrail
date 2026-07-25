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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
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
	watch := func(n int) []string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("C%055d", i)
		}
		return ids
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
		st.watched = append(st.watched, fmt.Sprintf("C%055d", i))
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

// -- Lag alarm tests --
//
// The lag alarm is a hysteresis-driven WARN-on-crossing / INFO-on-recovery
// signal that fires once per state change. Each test scripts a sequence
// of (latest_ledger, ingested_ledger) pairs and asserts on log counts,
// final hysteresis state, and (where it matters) the last gauge reading.
// A future "publish only on transitions" optimization should still pass.

func TestLagAlarm_DisabledIsSilent(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 0})

	driveLagCycles(t, ing, []mockCycle{{latest: 1_000, ingested: 100}})

	assert.Empty(t, strings.TrimSpace(buf.String()), "no log lines expected when alarm disabled")
	assert.Empty(t, metrics.history(), "no metric updates expected when alarm disabled")
	assert.False(t, ing.lagging, "internal state stays at false")
}

// Note: the documented default of 100 is enforced by internal/config
// (LAG_WARN_LEDGERS env, default 100). Ingester.applyDefaults intentionally
// does NOT auto-substitute 100 for zero because zero is also the
// documented "disabled" sentinel (TestLagAlarm_DisabledIsSilent above
// pins that contract). Inlining a default here would force operators to
// set LagWarnLedgers to a non-zero value to disable, which contradicts
// the README config table.

func TestLagAlarm_StayHealthy_NoLogs(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})

	driveLagCycles(t, ing, []mockCycle{
		{latest: 105, ingested: 105},
		{latest: 106, ingested: 106},
		{latest: 107, ingested: 107},
		{latest: 108, ingested: 108},
		{latest: 109, ingested: 109},
	})

	assert.Empty(t, strings.TrimSpace(buf.String()), "healthy cycles stay silent")
	assert.False(t, ing.lagging)
}

func TestLagAlarm_CrossingFiresOnceAndStaysLaggingNoSpam(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})
	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))

	driveLagCycles(t, ing, []mockCycle{
		{latest: 150, ingested: 100}, // lag 50, healthy (no log)
		{latest: 210, ingested: 100}, // lag 110, CROSS → WARN
		{latest: 220, ingested: 100}, // still lagging (no log)
		{latest: 250, ingested: 100}, // still lagging (no log)
		{latest: 300, ingested: 100}, // still lagging (no log)
	})

	warns := logRecords(t, buf, func(l string) bool { return l == "WARN" })
	require.Len(t, warns, 1, "exactly one warn on the crossing, despite 4 lagging cycles")
	assert.Equal(t, "ingest lag exceeded threshold", warns[0]["msg"])
	assert.Equal(t, float64(210), warns[0]["latest_ledger"], "snapshot of the cycle that crossed")
	assert.Equal(t, float64(100), warns[0]["last_ingested_ledger"])
	assert.Equal(t, float64(110), warns[0]["lag_ledgers"], "210 - 100 = 110")
	assert.Equal(t, float64(100), warns[0]["threshold_ledgers"])
	assert.True(t, ing.lagging, "stays true across the still-lagging tail")
}

func TestLagAlarm_RecoveryFiresOnceAndStaysQuiet(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})
	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))

	driveLagCycles(t, ing, []mockCycle{
		{latest: 500, ingested: 100},  // lag 400 → WARN
		{latest: 110, ingested: 100},  // lag 10, recovered → INFO
		{latest: 112, ingested: 112},  // catch-up → stays healthy
		{latest: 115, ingested: 115},
	})

	levels := map[string]int{}
	for _, r := range logRecords(t, buf, nil) {
		levels[r["level"].(string)]++
	}
	assert.Equal(t, 1, levels["WARN"], "exactly one crossing-warn")
	assert.Equal(t, 1, levels["INFO"], "exactly one recovery-info, even after 3 healthy cycles")

	var infoRec map[string]any
	for _, r := range logRecords(t, buf, func(l string) bool { return l == "INFO" }) {
		infoRec = r
	}
	require.NotNil(t, infoRec)
	assert.Equal(t, "ingest lag recovered", infoRec["msg"])
	assert.Equal(t, float64(110), infoRec["latest_ledger"])
	assert.Equal(t, float64(10), infoRec["lag_ledgers"])
	assert.Equal(t, float64(100), infoRec["threshold_ledgers"])
	assert.False(t, ing.lagging)
}

func TestLagAlarm_RoundTripFullCrossingSequence(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})
	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 1_000}))

	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_010, ingested: 1_000},
		{latest: 1_080, ingested: 1_000},
		{latest: 1_110, ingested: 1_000}, // cross (lag 110) → WARN
		{latest: 1_200, ingested: 1_000},
		{latest: 1_300, ingested: 1_000},
		{latest: 1_080, ingested: 1_080}, // recovered (lag 0) → INFO
		{latest: 1_100, ingested: 1_095},
		{latest: 1_120, ingested: 1_115},
	})

	warns := logRecords(t, buf, func(l string) bool { return l == "WARN" })
	infos := logRecords(t, buf, func(l string) bool { return l == "INFO" })
	assert.Len(t, warns, 1, "only one crossing despite 8 cycles")
	assert.Len(t, infos, 1, "only one recovery despite 8 cycles")
	assert.False(t, ing.lagging)
}

func TestLagAlarm_ReCrossingFiresAnotherWarn(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})
	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 1_000}))

	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_500, ingested: 1_000}, // cross 1 → WARN
		{latest: 1_050, ingested: 1_050}, // recovered → INFO
		{latest: 1_500, ingested: 1_050}, // cross 2 → WARN again
		{latest: 1_600, ingested: 1_050}, // still lagging → no log
	})

	warns := logRecords(t, buf, func(l string) bool { return l == "WARN" })
	infos := logRecords(t, buf, func(l string) bool { return l == "INFO" })
	assert.Len(t, warns, 2, "two distinct crossings → two WARNs")
	assert.Len(t, infos, 1, "one recovery in between")
	assert.True(t, ing.lagging)
}

func TestLagAlarm_ColdStart_NoFireAndSettlesGauge(t *testing.T) {
	ing, buf, metrics := makeIngester(t, Options{LagWarnLedgers: 100})

	driveLagCycles(t, ing, []mockCycle{{latest: 5_000, ingested: -1}})

	assert.Empty(t, strings.TrimSpace(buf.String()), "no log when no ingestion state exists")
	gauge := metrics.history()
	require.NotEmpty(t, gauge, "SetLagging should run on ErrNotFound to prime the gauge")
	assert.False(t, gauge[len(gauge)-1], "last gauge reading on ErrNotFound is false")
	assert.False(t, ing.lagging)
}

// seedLaggingForTest drives the alarm through a real WARN cycle so tests
// can verify the error paths preserve hysteresis without poking the
// unexported field directly. Two side effects are managed here so
// tests stay focused on post-seed behavior:
//
//  1. The seeded WARN/INFO lines are routed to a discard logger so the
//     test's own recording buf only sees events that happen AFTER the
//     seed returns. Callers can still count "WARN count == 1 if the test
//     triggered one extra crossing".
//  2. If the wired client is a *flakyRPC with failLatest=true (used by
//     error-path tests), the seed temporarily toggles it false so the
//     WARN actually fires; the original flag is restored on return.
//     Keeps testLaggingForTest unaware of which RPC variant the test
//     wired in.
func seedLaggingForTest(t *testing.T, ing *Ingester) {
	t.Helper()

	origLog := ing.log
	ing.log = testLogger()
	defer func() { ing.log = origLog }()

	if f, ok := ing.client.(*flakyRPC); ok {
		origFail := f.failLatest
		f.failLatest = false
		defer func() { f.failLatest = origFail }()
	}

	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 100}))
	driveLagCycles(t, ing, []mockCycle{
		{latest: 600, ingested: 100}, // lag 500 → triggers WARN
	})
	require.True(t, ing.lagging, "seed helper should leave hysteresis true")
}

func TestLagAlarm_GetLatestLedgerError_KeepsPriorState(t *testing.T) {
	log, buf := recordingLogger()
	metrics := &recordingLagMetrics{}
	st := newMockStore()
	flaky := &flakyRPC{base: &mockRPC{health: rpc.Health{LatestLedger: 1_000}}, failLatest: true}
	ing := newTestIngester(flaky, st, Options{LagWarnLedgers: 100})
	ing.log = log
	ing.opts.LagMetrics = metrics
	seedLaggingForTest(t, ing)
	startHistoryLen := len(metrics.history())

	flaky.failLatest = true
	ing.checkLag(context.Background())

	assert.Empty(t, strings.TrimSpace(buf.String()),
		"no log lines when getLatestLedger errors (no spurious recovery)")
	assert.True(t, ing.lagging, "prior hysteresis state preserved on transient RPC failure")
	assert.Equal(t, startHistoryLen, len(metrics.history()),
		"no new SetLagging call when evidence is missing")
}

func TestLagAlarm_GetIngestionStateError_KeepsPriorState(t *testing.T) {
	log, buf := recordingLogger()
	metrics := &recordingLagMetrics{}
	st := newMockStore()
	ing := newTestIngester(&mockRPC{health: rpc.Health{LatestLedger: 1_000}}, st,
		Options{LagWarnLedgers: 100})
	ing.log = log
	ing.opts.LagMetrics = metrics
	seedLaggingForTest(t, ing)
	startHistoryLen := len(metrics.history())

	setMockStoreIngestErr(t, st, errors.New("database on fire"))
	ing.checkLag(context.Background())

	assert.Empty(t, strings.TrimSpace(buf.String()),
		"no log when GetIngestionState errors (no spurious recovery)")
	assert.True(t, ing.lagging, "prior hysteresis state preserved on transient store error")
	assert.Equal(t, startHistoryLen, len(metrics.history()),
		"no new SetLagging call when evidence is missing")
}

// setMockStoreIngestErr writes ingestErr under mockStore.mu so the
// in-package test helper matches the rest of mockStore's lock-taking
// reads.
func setMockStoreIngestErr(t *testing.T, st *mockStore, err error) {
	t.Helper()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.ingestErr = err
}

func TestLagAlarm_NegativeLagFromReorgIsClamped(t *testing.T) {
	ing, buf, _ := makeIngester(t, Options{LagWarnLedgers: 100})
	require.NoError(t, ing.store.SaveIngestionState(context.Background(),
		store.IngestionState{LastIngestedLedger: 1_000}))

	driveLagCycles(t, ing, []mockCycle{
		{latest: 1_500, ingested: 1_000}, // lag 500 → WARN
		{latest: 900, ingested: 1_000},  // "lag" -100 (reorg)
		{latest: 950, ingested: 1_000},  // "lag" -50
	})

	warns := logRecords(t, buf, func(l string) bool { return l == "WARN" })
	infos := logRecords(t, buf, func(l string) bool { return l == "INFO" })
	assert.Len(t, warns, 1, "the original crossing still fires")
	assert.Len(t, infos, 0, "negative lag must not emit a spurious recovery")
	assert.True(t, ing.lagging, "hysteresis preserved across ambiguous cycles")
}

// flakyRPC wraps mockRPC and fails GetLatestLedger on demand so the
// alarm's error path can be tested without affecting other RPC methods.
type flakyRPC struct {
	base       *mockRPC
	failLatest bool
}

func (f *flakyRPC) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return f.base.GetEvents(ctx, req)
}

func (f *flakyRPC) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	if f.failLatest {
		return rpc.LatestLedger{}, fmt.Errorf("flaky: simulated RPC outage")
	}
	return rpc.LatestLedger{Sequence: f.base.health.LatestLedger}, nil
}

func (f *flakyRPC) GetHealth(ctx context.Context) (rpc.Health, error) {
	return f.base.GetHealth(ctx)
}
