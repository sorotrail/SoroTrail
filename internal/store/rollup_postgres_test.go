package store

// Integration tests for rollup correctness. They need a real database and
// are skipped unless TEST_DATABASE_URL is set.
//
// These tests verify that:
//   1. Incremental rollup counts match a from-scratch GROUP BY.
//   2. Duplicate event re-upserts do not double-count buckets.
//   3. Rebuild produces identical results to incremental rollups.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRollupStore(t *testing.T) *Postgres {
	t.Helper()
	st := testStore(t)
	// Truncate rollup tables alongside the standard set.
	_, err := st.pool.Exec(context.Background(),
		`TRUNCATE rollup_events, rollup_token_volume`)
	require.NoError(t, err)
	return st
}

// TestRollup_IncrementalCountsMatchGroupBy verifies that after arbitrary
// upsert sequences (including duplicates), the rollup_events counts match
// a from-scratch GROUP BY over the events table.
func TestRollup_IncrementalCountsMatchGroupBy(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	// Insert events across multiple ledgers, contracts, and types.
	ev1 := testEvent(eventID(1), 100, contractA) // type=contract
	ev2 := testEvent(eventID(2), 100, contractA)
	ev3 := testEvent(eventID(3), 100, contractB)
	ev4 := testEvent(eventID(4), 200, contractA)
	ev5 := testEvent(eventID(5), 200, contractB)
	ev5.Type = "diagnostic" // different type
	ev6 := testEvent(eventID(6), 300, contractA)

	events := []Event{ev1, ev2, ev3, ev4, ev5, ev6}
	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(6), inserted)

	// Query rollups and verify against GROUP BY from events table.
	buckets, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	assert.NotEmpty(t, buckets, "rollups should be populated")

	// Verify rollup counts match a direct GROUP BY on events.
	for _, b := range buckets {
		var expected int64
		row := st.pool.QueryRow(ctx, `
			SELECT count(*) FROM events WHERE contract_id = $1 AND type = $2`,
			b.ContractID, b.Type)
		require.NoError(t, row.Scan(&expected))
		assert.Equal(t, expected, b.Count,
			"bucket %v contract=%s type=%s: rollup count %d != events count %d",
			b.BucketStart, b.ContractID, b.Type, b.Count, expected)
	}
}

// TestRollup_DuplicatesDoNotDoubleCount verifies that re-upserting the
// same events does not increment rollup counts.
func TestRollup_DuplicatesDoNotDoubleCount(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	events := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 100, contractA),
	}
	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	// Snapshot rollup counts after first insert.
	buckets1, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)

	// Re-upsert identical events — must not change rollup counts.
	inserted2, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted2, "duplicate upsert should report zero new rows")

	buckets2, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	assert.Equal(t, buckets1, buckets2, "duplicate upsert must not change rollup counts")
}

// TestRollup_MixedBatchNewAndDuplicate verifies that a batch containing
// both new and duplicate events only contributes deltas from the new ones.
func TestRollup_MixedBatchNewAndDuplicate(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	// First batch: 2 events.
	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 100, contractA),
	})
	require.NoError(t, err)

	// Second batch: 1 duplicate + 1 new.
	inserted, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA), // duplicate
		testEvent(eventID(3), 100, contractA), // new
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted, "only one new event in the batch")

	var total int64
	row := st.pool.QueryRow(ctx,
		`SELECT count FROM rollup_events WHERE contract_id = $1`, contractA)
	require.NoError(t, row.Scan(&total))
	assert.Equal(t, int64(3), total, "rollup count should be 3, not 4")
}

// TestRollup_RebuildMatchesIncremental verifies that a full rollup rebuild
// produces the same results as incremental rollup accumulation.
func TestRollup_RebuildMatchesIncremental(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	// Insert events incrementally (which populates rollups as a side effect).
	var all []Event
	for i := 1; i <= 20; i++ {
		contract := contractA
		if i%3 == 0 {
			contract = contractB
		}
		e := testEvent(eventID(i), int64(100+i%5), contract)
		if i == 7 {
			e.Type = "diagnostic"
		}
		all = append(all, e)
	}
	_, err := st.UpsertEvents(ctx, all)
	require.NoError(t, err)

	// Snapshot incremental results.
	incrBuckets, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	require.NotEmpty(t, incrBuckets)

	// Rebuild for the same ledger range.
	sum, err := st.RebuildRollups(ctx, 100, 104)
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	assert.Equal(t, int64(20), sum.EventsScanned)

	// After rebuild, results must match incremental.
	rebuildBuckets, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	assert.Equal(t, incrBuckets, rebuildBuckets,
		"rebuild must produce identical results to incremental rollups")
}

// TestRollup_EmptyUpsertNoop verifies that upserting zero events is safe.
func TestRollup_EmptyUpsertNoop(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	inserted, err := st.UpsertEvents(ctx, nil)
	require.NoError(t, err)
	assert.Zero(t, inserted)

	buckets, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

// TestRollup_DayBucketAggregation verifies that day bucket aggregation
// correctly sums hourly buckets.
func TestRollup_DayBucketAggregation(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	// Insert events across two consecutive hours (via different ledgers).
	// Since bucketHour derives from ledger, use ledgers that map to
	// different hours.
	e1 := testEvent(eventID(1), 53_000_000, contractA) // ~hour 0
	e2 := testEvent(eventID(2), 53_000_720, contractA) // ~hour 1 (720 ledgers ≈ 1 hour)
	_, err := st.UpsertEvents(ctx, []Event{e1, e2})
	require.NoError(t, err)

	// Hourly should return 2 buckets.
	hourly, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	assert.Len(t, hourly, 2, "events in different hours should produce separate buckets")

	// Daily should aggregate to 1 bucket.
	daily, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "day"})
	require.NoError(t, err)
	assert.Len(t, daily, 1, "events in the same day should aggregate to one bucket")
	assert.Equal(t, int64(2), daily[0].Count)
}

// TestRollup_TimeRangeFilter verifies from/to filtering on analytics queries.
func TestRollup_TimeRangeFilter(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	e1 := testEvent(eventID(1), 53_000_000, contractA)
	e2 := testEvent(eventID(2), 53_000_720, contractA) // 1 hour later
	_, err := st.UpsertEvents(ctx, []Event{e1, e2})
	require.NoError(t, err)

	hourly, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	require.Len(t, hourly, 2)

	// Filter to only the first bucket's time range.
	from := hourly[0].BucketStart
	to := hourly[0].BucketStart.Add(time.Hour)

	filtered, err := st.QueryAnalyticsEvents(ctx, AnalyticsFilter{
		Bucket: "hour", From: from, To: to,
	})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, from, filtered[0].BucketStart)
}

// TestRollup_TokenVolumeDetectsTransfers verifies that transfer-shaped
// events populate rollup_token_volume correctly.
func TestRollup_TokenVolumeDetectsTransfers(t *testing.T) {
	st := testRollupStore(t)
	ctx := context.Background()

	transfer := testEvent(eventID(1), 53_000_000, contractA)
	transfer.Topics = json.RawMessage(`[{"symbol":"transfer"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},{"address":"GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}]`)
	transfer.Value = json.RawMessage(`{"i128":"5000000000"}`)

	nonTransfer := testEvent(eventID(2), 53_000_000, contractA)
	nonTransfer.Topics = json.RawMessage(`[{"symbol":"mint"},{"u64":42}]`)
	nonTransfer.Value = json.RawMessage(`{"u64":42}`)

	_, err := st.UpsertEvents(ctx, []Event{transfer, nonTransfer})
	require.NoError(t, err)

	vol, err := st.QueryAnalyticsTokenVolume(ctx, AnalyticsFilter{Bucket: "hour"})
	require.NoError(t, err)
	require.Len(t, vol, 1)
	assert.Equal(t, "5000000000", vol[0].Volume)
	assert.Equal(t, int64(2), vol[0].UniqueAddressCount)

	// Non-transfer event should not contribute to volume.
	assert.Equal(t, contractA, vol[0].ContractID)
}
