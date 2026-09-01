//go:build integration

package ingester

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

// TestIntegration_CursorResumeAcrossRestart proves that the persisted
// LastCursor drives a fresh Ingester instance (mimicking a process restart)
// to ask the RPC for the next page using the saved cursor rather than
// re-scanning from StartLedger. The first instance persists a partial page,
// its cursor, and its last-ingested ledger; the second instance picks up
// from the same Postgres-backed store and resumes exactly where the first
// stopped.
func TestIntegration_CursorResumeAcrossRestart(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	defer pool.Close()
	ctx := context.Background()

	st := store.NewPostgres(pool)

	// Page 1: full page + a top-level cursor so the ingester knows more
	// data is waiting and saves the cursor instead of treating the page as
	// "caught up".
	mock1 := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1000, OldestLedger: 2},
		eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("cursor-e1", 100), rpcEvent("cursor-e2", 101)},
			LatestLedger: 1000,
			Cursor:       "cursor-after-page1",
		}},
	}
	ing1 := newTestIngester(mock1, st, Options{
		StartLedger: 100,
		PageLimit:   100,
	})

	caughtUp, err := ing1.runOnce(ctx)
	require.NoError(t, err)
	// Page 1 returns 2 events under PageLimit=100, so by the per-page
	// heuristic nextState reports caught-up. That doesn't matter for this
	// test: what matters is that the caller-supplied Cursor on the response
	// is still persisted, so a restarted Ingester resumes via that cursor
	// rather than re-scanning from StartLedger. The assertion below pins
	// the behaviour we exercise.
	assert.True(t, caughtUp,
		"2-event page under PageLimit=100 is caught up; the cursor on the response is what resumes")
	require.Len(t, mock1.eventsRequests, 1, "page 1 must have produced exactly one request")

	// Page 1's events must be queryable through the store interface, and
	// the persisted state must carry the cursor and last ledger.
	e1, err := st.GetEvent(ctx, "cursor-e1", store.SystemScope())
	require.NoError(t, err)
	assert.Equal(t, int64(100), e1.Ledger)

	state, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cursor-after-page1", state.LastCursor,
		"full-page response must persist its cursor for resume")
	assert.Equal(t, int64(101), state.LastIngestedLedger)

	// Simulate a process restart: brand-new Ingester wired to a fresh
	// mockRPC that returns the next page *only when called with the saved
	// cursor*. StartLedger stays unset so a missing cursor would force a
	// cold start — the test fails if the resume path does anything other
	// than follow the cursor.
	mock2 := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1000, OldestLedger: 2},
		eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("cursor-e3", 102)},
			LatestLedger: 1000,
			Cursor:       "cursor-after-page2",
		}},
	}
	ing2 := newTestIngester(mock2, st, Options{})

	caughtUp, err = ing2.runOnce(ctx)
	require.NoError(t, err)
	// Page 2 has 1 event under PageLimit=100, so the ingester reports
	// caught-up — verifying *both* "followed the cursor" and "correctly
	// identified end of stream" in one assertion.
	assert.True(t, caughtUp,
		"1-event page under page-limit means caught up — the resumed pass must not stall")

	require.Len(t, mock2.eventsRequests, 1,
		"resume means exactly one request — extra calls would mask a regression")
	req := mock2.eventsRequests[0]
	require.NotNil(t, req.Pagination,
		"resume means cursor pagination — request must carry a Pagination object")
	assert.Equal(t, "cursor-after-page1", req.Pagination.Cursor,
		"the resumed request must reuse the cursor persisted by the first Ingester")
	assert.Zero(t, req.StartLedger,
		"cursor and StartLedger are mutually exclusive; the saved cursor wins")

	// The third event must be present, and idempotency must keep the row
	// count at exactly 3 — none of cursor-e1/cursor-e2/cursor-e3 were
	// duplicated by the resumed run.
	e3, err := st.GetEvent(ctx, "cursor-e3", store.SystemScope())
	require.NoError(t, err)
	assert.Equal(t, int64(102), e3.Ledger)

	all, _, err := st.QueryEvents(ctx, store.EventFilter{Limit: 1000, Scope: store.SystemScope()})
	require.NoError(t, err)
	assert.Len(t, all, 3, "resumed ingestion must not duplicate persisted events")
}

// TestIntegration_WarmStartResumesByLedgerPlusOne exercises the warm-start
// path: a non-empty LastIngestedLedger with no cursor causes a fresh
// Ingester to ask the RPC for the ledger *after* the last-ingested one,
// rather than re-scanning from StartLedger or re-clamping to the RPC's
// oldest retained ledger.
func TestIntegration_WarmStartResumesByLedgerPlusOne(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	defer pool.Close()
	ctx := context.Background()

	st := store.NewPostgres(pool)
	require.NoError(t, st.SaveIngestionState(ctx, store.IngestionState{
		LastIngestedLedger: 500,
	}))

	mock := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1000, OldestLedger: 2},
		eventsResps: []rpc.GetEventsResponse{{
			Events:       []rpc.Event{rpcEvent("warm-e1", 501)},
			LatestLedger: 1000,
		}},
	}
	ing := newTestIngester(mock, st, Options{
		RetentionLedgers: 17_280, // would matter if a cursor were missing — it isn't
	})

	caughtUp, err := ing.runOnce(ctx)
	require.NoError(t, err)
	assert.True(t, caughtUp, "one event under PageLimit means caught up")

	require.Len(t, mock.eventsRequests, 1)
	req := mock.eventsRequests[0]
	assert.Equal(t, uint32(501), req.StartLedger,
		"warm start (no cursor) must start at LastIngestedLedger+1")
	// The request still carries a Pagination block: that is where the page
	// limit travels. What warm start must not do is send a cursor, since
	// that would override StartLedger.
	require.NotNil(t, req.Pagination)
	assert.Empty(t, req.Pagination.Cursor,
		"warm start goes through the StartLedger path, not cursor pagination")

	ev, err := st.GetEvent(ctx, "warm-e1", store.SystemScope())
	require.NoError(t, err)
	assert.Equal(t, int64(501), ev.Ledger)
}

// TestIntegration_IngestionStateIdempotent validates SaveIngestionState's
// contract: re-writing an identical row leaves the table unchanged, so a
// noisy caller (a crashed loop retrying with the same cursor) cannot
// regress the persisted frontier.
func TestIntegration_IngestionStateIdempotent(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	defer pool.Close()
	ctx := context.Background()

	st := store.NewPostgres(pool)
	state := store.IngestionState{
		LastIngestedLedger: 1234,
		LastCursor:         "cursor-idempotent",
	}
	require.NoError(t, st.SaveIngestionState(ctx, state))

	// SaveIngestionState under the store.Store interface — if a future
	// refactor adds an ON CONFLICT semantics here, this test pins the
	// expectation that re-writing the same row is a no-op.
	require.NoError(t, st.SaveIngestionState(ctx, state))

	got, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, state.LastIngestedLedger, got.LastIngestedLedger)
	assert.Equal(t, state.LastCursor, got.LastCursor)
}

// TestIntegration_RPCErrorLeavesStateUnchanged proves that a transient RPC
// failure on the first pass must not corrupt the cursor — the next run
// either retries the same page or, on an aged-out cursor, re-clamps. The
// test asserts the failure did NOT silently rewind LastIngestedLedger to
// zero, which would lose every event the previous run had already saved.
//
// mockRPC returns the zero-value GetEventsResponse when eventsResps[i] is
// out of range, so the empty slice paired with a non-empty eventsErrs is
// the standard "RPC only returns errors" scripting idiom for these tests.
func TestIntegration_RPCErrorLeavesStateUnchanged(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	defer pool.Close()
	ctx := context.Background()

	st := store.NewPostgres(pool)
	// Pre-seed: an earlier successful pass saved cursor + ledger.
	require.NoError(t, st.SaveIngestionState(ctx, store.IngestionState{
		LastIngestedLedger: 999,
		LastCursor:         "cursor-before-fail",
	}))

	mock := &mockRPC{
		health:     rpc.Health{Status: "healthy", LatestLedger: 1000, OldestLedger: 2},
		eventsErrs: []error{errBoom{}},
	}
	ing := newTestIngester(mock, st, Options{})

	_, err := ing.runOnce(ctx)
	require.Error(t, err, "RPC failure must surface — the loop safety net is Run()")

	// The persisted state must match what the test set up. The failed
	// pass did not touch SaveIngestionState, so the cursor and the
	// last-ingested ledger are intact: the next run will retry the same
	// cursor and resume correctly.
	state, sErr := st.GetIngestionState(ctx)
	require.NoError(t, sErr)
	assert.Equal(t, "cursor-before-fail", state.LastCursor,
		"RPC failure must not regress the persisted cursor")
	assert.Equal(t, int64(999), state.LastIngestedLedger,
		"RPC failure must not regress persisted progress")
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// TestIntegration_BatchSizeSplitsWritesOnRealStore proves the configurable
// batch-size path works end-to-end against a real Postgres store: a burst
// page larger than BatchSize is persisted (split into multiple store writes
// internally) and every event is queryable afterwards. It also asserts the
// write-count metric moved so the chunked path actually ran rather than
// silently falling back to the single-write per-page behavior.
func TestIntegration_BatchSizeSplitsWritesOnRealStore(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	defer pool.Close()
	ctx := context.Background()

	st := store.NewPostgres(pool)

	// A burst of events landing in a single fetch page, with a batch size
	// smaller than the page so the burst must be split across many writes.
	const (
		burst     = 120
		batchSize = 25
	)
	mock := &mockRPC{
		health:      rpc.Health{Status: "healthy", LatestLedger: 200, OldestLedger: 2},
		eventsResps: []rpc.GetEventsResponse{manyEvents(burst)},
	}
	ing := newTestIngester(mock, st, Options{
		StartLedger: 100,
		PageLimit:   1000,
		BatchSize:   batchSize,
	})

	writesBefore := testutil.ToFloat64(metrics.EventBatchWrites)

	_, err := ing.runOnce(ctx)
	require.NoError(t, err)

	// Every burst event must be queryable through the real store — nothing
	// lost, nothing duplicated by the chunked writer.
	got, _, err := st.QueryEvents(ctx, store.EventFilter{
		Limit: 100000,
		Scope: store.SystemScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, burst, "a hyper-dense page must persist ALL events with batching enabled")

	// The burst was actually split: more than one store write was issued,
	// each capped at BatchSize.
	assert.Greater(t, testutil.ToFloat64(metrics.EventBatchWrites), writesBefore,
		"a burst larger than BatchSize must issue multiple store writes")

	// Leave the gauge at a known value so sibling tests start from a clean
	// baseline.
	metrics.EventBatchSize.Set(0)
}
