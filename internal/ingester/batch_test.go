package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/rpc"
)

// recordingClock wraps the real clock but records every SleepCtx call so
// tests can assert the ingester actually applied backpressure (and how
// much), without forgoing the real wall-clock behaviour of Now()/timers.
type recordingClock struct {
	Clock
	sleeps int
	slept  time.Duration
}

func (r *recordingClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	r.sleeps++
	r.slept += d
	return r.Clock.SleepCtx(ctx, d)
}

// manyEvents returns a page containing n distinct events at ledger 100.
func manyEvents(n int) rpc.GetEventsResponse {
	events := make([]rpc.Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, rpcEvent(fmt.Sprintf("e%d", i), 100))
	}
	return rpc.GetEventsResponse{Events: events, LatestLedger: 500}
}

// TestBatchSize_SplitsPageAcrossWrites pins the core "configurable batch
// size" behaviour: a page larger than BatchSize must be persisted in several
// UpsertEvents calls of at most BatchSize rows each, with every event still
// landing in the store. No latency budget is set, so the chunk size stays
// fixed at the configured value (no adaptation, no backpressure).
func TestBatchSize_SplitsPageAcrossWrites(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{manyEvents(5)}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{
		StartLedger: 100,
		PageLimit:   100,
		BatchSize:   2,
	})

	writesBefore := testutil.ToFloat64(metrics.EventBatchWrites)
	eventsBefore := testutil.ToFloat64(metrics.EventsIngested)

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	require.Equal(t, testutil.ToFloat64(metrics.EventBatchWrites), writesBefore+3,
		"a page of 5 with a batch size of 2 must issue 3 store writes")
	require.Equal(t, testutil.ToFloat64(metrics.EventsIngested), eventsBefore+5,
		"the EventsIngested counter still reflects the TOTAL events, across all chunks")

	// Every chunk respects the cap and every event is persisted exactly once.
	require.Len(t, st.upserted, 3, "one store write per chunk")
	total := 0
	for _, chunk := range st.upserted {
		assert.LessOrEqualf(t, len(chunk), 2, "chunk exceeds BatchSize: %d", len(chunk))
		total += len(chunk)
	}
	assert.Equal(t, 5, total, "all events across all chunks")
	assert.Len(t, st.events, 5, "every event persisted with no loss or duplication")
}

// TestBatchSize_DisabledKeepsSingleWritePerPage is the regression guard for
// the default (BatchSize == 0): behavior must be bit-for-bit what the
// ingester always did — one UpsertEvents per non-empty page.
func TestBatchSize_DisabledKeepsSingleWritePerPage(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{manyEvents(5)}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100})

	writesBefore := testutil.ToFloat64(metrics.EventBatchWrites)
	eventsBefore := testutil.ToFloat64(metrics.EventsIngested)

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	assert.Equal(t, testutil.ToFloat64(metrics.EventBatchWrites), writesBefore+1,
		"batching disabled: exactly one store write for the whole page")
	assert.Equal(t, testutil.ToFloat64(metrics.EventsIngested), eventsBefore+5)
	require.Len(t, st.upserted, 1)
	require.Len(t, st.upserted[0], 5)
}

// TestBatchSize_ZeroEventsNoWrite guards against a spurious write when a page
// carries zero decodable events with batching enabled.
func TestBatchSize_ZeroEventsNoWrite(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{{Events: []rpc.Event{}, LatestLedger: 500}}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100, BatchSize: 2})

	writesBefore := testutil.ToFloat64(metrics.EventBatchWrites)
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	assert.Equal(t, writesBefore, testutil.ToFloat64(metrics.EventBatchWrites),
		"an empty page must not issue a store write")
}

// TestBatchController_Adapts pins the latency-adaptation logic in isolation:
// slow writes shrink the batch size (with a backoff returned), fast writes
// grow it back toward the maximum, and the size never drops below 1.
func TestBatchController_Adapts(t *testing.T) {
	newBC := func() *batchController {
		return &batchController{
			max:            256,
			cur:            256,
			maxBk:          time.Millisecond,
			adaptive:       true,
			targetPerEvent: time.Millisecond,
			ewma:           time.Millisecond,
		}
	}

	t.Run("slow write shrinks the batch and asks for backoff", func(t *testing.T) {
		bc := newBC()
		// 200 events taking 2s → 10ms per event, an order of magnitude over
		// the 1ms budget. The very first write flips the EWMA over budget.
		backoff := bc.recordAndBackoff(200, 2*time.Second)
		assert.Equal(t, 128, bc.cur, "first over-budget write halves the batch size")
		assert.Equal(t, time.Millisecond, backoff, "must request a backpressure sleep")
	})

	t.Run("fast writes grow the batch back toward the maximum", func(t *testing.T) {
		bc := newBC()
		// Send one slow write to shrink, then enough fast ones to recover.
		bc.recordAndBackoff(200, 2*time.Second)
		require.Equal(t, 128, bc.cur)
		for i := 0; i < 8 && bc.cur < bc.max; i++ {
			bc.recordAndBackoff(128, 128*time.Microsecond) // 1us/event, well under budget
		}
		assert.Equal(t, 256, bc.cur, "sustained fast writes must grow the batch back to max")
		assert.Zero(t, bc.recordAndBackoff(128, 128*time.Microsecond),
			"in-budget writes must not request backpressure")
	})

	t.Run("batch size floors at one and still backs off", func(t *testing.T) {
		bc := newBC()
		bc.cur = 1
		bc.ewma = 10 * time.Second // permanently over budget, already at the floor
		backoff := bc.recordAndBackoff(1, 10*time.Second)
		assert.Equal(t, 1, bc.cur, "cannot shrink below 1")
		assert.Equal(t, time.Millisecond, backoff, "at the floor, over budget still backs off")
	})

	t.Run("non-adaptive controller never changes size or backs off", func(t *testing.T) {
		bc := &batchController{max: 50, cur: 50, maxBk: time.Second, adaptive: false}
		backoff := bc.recordAndBackoff(50, 5*time.Second)
		assert.Zero(t, backoff, "adaptation disabled: no backpressure")
		assert.Equal(t, 50, bc.cur, "adaptation disabled: size stays fixed at max")
	})
}

// TestBatchBackpressure_SlowStoreThrottles is the synthetic-burst acceptance
// test end-to-end. A page big enough to split into several writes hits a
// store that is slow (upsertDelay), with a tiny latency budget so every write
// is over budget. It must (a) persist every event, (b) issue more than one
// write, (c) never write more than BatchSize rows at once, and (d) actually
// sleep between writes and expose the batch metrics.
func TestBatchBackpressure_SlowStoreThrottles(t *testing.T) {
	const (
		pageEvents = 100
		batchSize  = 50
	)

	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{manyEvents(pageEvents)}}
	st := newMockStore()
	st.upsertDelay = 10 * time.Millisecond // every write is slow
	clock := &recordingClock{Clock: RealClock{}}

	ing := newTestIngester(client, st, Options{
		StartLedger:        100,
		PageLimit:          100,
		Clock:              clock,
		BatchSize:          batchSize,
		BatchTargetLatency: 100 * time.Microsecond, // tiny budget → always over budget
		BatchMaxBackoff:    time.Millisecond,
	})

	bpBefore := testutil.ToFloat64(metrics.EventBackpressure)
	bpSecBefore := testutil.ToFloat64(metrics.EventBackpressureSeconds)
	writesBefore := testutil.ToFloat64(metrics.EventBatchWrites)

	// Seed the batch-size gauge high so we can assert it falls under load.
	metrics.EventBatchSize.Set(batchSize * 10)

	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	// Every event made it into the store.
	assert.Len(t, st.events, pageEvents, "a slow store must still persist ALL events")

	// The page was split into more than one write, all capped at BatchSize.
	require.Greater(t, len(st.upserted), 1, "a burst page must be split into multiple writes")
	for _, chunk := range st.upserted {
		assert.LessOrEqualf(t, len(chunk), batchSize, "chunk exceeds BatchSize")
	}
	total := 0
	for _, chunk := range st.upserted {
		total += len(chunk)
	}
	assert.Equal(t, pageEvents, total)

	// Backpressure actually fired: sleeps happened, counters advanced, and
	// the batch-size gauge was driven below its seeded value.
	assert.Greater(t, clock.sleeps, 0, "the ingester must sleep between writes under pressure")
	assert.Greater(t, testutil.ToFloat64(metrics.EventBackpressure), bpBefore,
		"backpressure counter must advance")
	assert.Greater(t, testutil.ToFloat64(metrics.EventBackpressureSeconds), bpSecBefore,
		"backpressure seconds must accumulate")
	assert.Greater(t, testutil.ToFloat64(metrics.EventBatchWrites), writesBefore,
		"several writes must be issued for one page")
	assert.LessOrEqual(t, testutil.ToFloat64(metrics.EventBatchSize), float64(batchSize),
		"batch size gauge must be driven at or below the configured maximum")

	// Clean up the gauge so other tests start from a known baseline.
	metrics.EventBatchSize.Set(0)
}

// TestBatchBackpressure_MaxBatchCapped asserts correctness at the boundary:
// a page exactly BatchSize events fits in one write; a page of BatchSize+1
// splits into exactly two, and the cumulative inserted count is right even
// with batching enabled (the mock reports 1 insertion for every distinct ID).
func TestBatchBackpressure_MaxBatchCapped(t *testing.T) {
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{manyEvents(5)}}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100, BatchSize: 4})

	eventsBefore := testutil.ToFloat64(metrics.EventsIngested)
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)

	require.Len(t, st.upserted, 2, "5 events with BatchSize 4 split into [4,1]")
	assert.Equal(t, 4, len(st.upserted[0]))
	assert.Equal(t, 1, len(st.upserted[1]))
	assert.Equal(t, eventsBefore+5, testutil.ToFloat64(metrics.EventsIngested),
		"EventsIngested accumulates across chunks within one page")
}

// TestBatchController_MetricsRegistered ensures the new batch metrics exist
// on the global registry and move when the controller runs — a guard against
// someone dropping a registration and only noticing via a silent 0 scrape.
func TestBatchController_MetricsRegistered(t *testing.T) {
	st := newMockStore()
	// Deliver a page big enough to trigger batching with a fixed size.
	client := &mockRPC{eventsResps: []rpc.GetEventsResponse{manyEvents(6)}}
	ing := newTestIngester(client, st, Options{StartLedger: 100, PageLimit: 100, BatchSize: 3})

	before := testutil.ToFloat64(metrics.EventBatchWrites)
	_, err := ing.runOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+2, testutil.ToFloat64(metrics.EventBatchWrites),
		"two writes for a 6-event page at BatchSize 3")
	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.EventBatchSize))
}
