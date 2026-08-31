//go:build integration

package store

// Coverage for backfill running alongside live ingestion (issue #810).
//
// Backfill and the live ingester write to the same tables at the same
// time — the events table by design, and their own state rows in
// parallel. These tests pin the invariants that make the overlap safe:
// idempotent upserts under concurrency, one row per event across
// overlapping ranges, the live frontier never moved by backfill, the two
// state rows staying independent, and partition creation not deadlocking
// when both writers race a fresh ledger range. The whole file also runs
// under -race in CI, which is what "race-free" means for the suite.

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backfillEventID mimics the id backfill synthesizes for a Horizon tx:
// {tx_hash}-{ledger:020d}-{op_index:05d}-{event_index:05d} (see
// internal/horizon.formatEventID). Distinct from the live TOID shape
// used by eventID, which is what makes the two writers' id spaces
// overlap only where the same emission is concerned.
func backfillEventID(txHash string, ledger int64, event int) string {
	return fmt.Sprintf("%s-%020d-%05d-%05d", txHash, ledger, 0, event)
}

// backfillEvents builds a batch of backfill-shaped events covering the
// inclusive ledger range [from, to], one event per ledger.
func backfillEvents(txHash string, from, to int64) []Event {
	out := make([]Event, 0, to-from+1)
	for l := from; l <= to; l++ {
		e := testEvent(backfillEventID(txHash, l, 0), l, contractA)
		out = append(out, e)
	}
	return out
}

// TestBackfillLive_ConcurrentWritersStayIdempotent drives four writers —
// two emitting live TOID-shaped ids, two emitting backfill hash-shaped
// ids — at the same time over the same ledger range, every one of them
// re-sending the full shared id set twice (page overlap is normal on
// both sides: resume re-fetches, cursor-free sweeps re-scan). The
// ON CONFLICT DO NOTHING paths therefore genuinely interleave on the
// same keys.
//
// All writers process their batches in ascending id order. That is not a
// cop-out: it mirrors production (both sides page in ledger order, which
// is id order) and it is what makes the concurrency safe — PostgreSQL's
// unique-index conflict machinery cannot deadlock when every transaction
// acquires keys in the same order, so the test pins idempotency, not a
// lock-cycling artifact. Afterwards each event id must exist exactly
// once: concurrent duplicate writes must not grow the table any more
// than sequential ones do.
func TestBackfillLive_ConcurrentWritersStayIdempotent(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	// 10 live events + 10 backfill events over ledgers 100..109 — one
	// fresh partition under span 10, so partition creation races too.
	live := make([]Event, 0, 10)
	backfill := make([]Event, 0, 10)
	for i := 0; i < 10; i++ {
		live = append(live, testEvent(eventID(100+i), int64(100+i), contractA))
		backfill = append(backfill, backfillEvents("beef", int64(100+i), int64(100+i))[0])
	}
	all := append(append([]Event(nil), live...), backfill...)
	// Ascending by id, the order both writers naturally produce. The id
	// space is a mix of "0..." TOIDs and hex hash prefixes, so sorting
	// the wire strings matches no single writer's raw output exactly —
	// it is the shared ascending order every writer commits to.
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Every writer sends the same ascending list twice; batch
			// boundaries vary per writer so the interleaving still races.
			dup := make([]Event, 0, len(all)*2)
			dup = append(dup, all...)
			dup = append(dup, all...)
			step := 4 + w
			for start := 0; start < len(dup); start += step {
				end := min(start+step, len(dup))
				if _, err := st.UpsertEvents(ctx, dup[start:end]); err != nil {
					errCh <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent upsert failed: %v", err)
	}

	total, err := st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(len(all)), total,
		"overlapping concurrent writes must yield exactly one row per event")

	for _, e := range all {
		got, err := st.GetEvent(ctx, e.ID, SystemScope())
		require.NoError(t, err, "event %s must exist after concurrent writes", e.ID)
		assert.Equal(t, e.Ledger, got.Ledger)
	}
}

// TestBackfillLive_OverlappingRangesOneRowPerEvent covers the resume
// overlap that is inherent to backfill: progress is persisted as a
// ledger, not a cursor, so a resume re-fetches from the start and the
// first page of the second run overlaps the last pages of the first.
// The same hash-shaped ids must not duplicate; likewise the live side
// re-scanning a window it already ingested.
func TestBackfillLive_OverlappingRangesOneRowPerEvent(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	// Backfill run 1 covers ledgers 100..103; run 2 (a resume after a
	// partial run) re-fetches from 100 and completes through 106. The
	// overlap [100, 103] is written twice with identical ids.
	run1 := backfillEvents("beef", 100, 103)
	run2 := backfillEvents("beef", 100, 106)
	inserted, err := st.UpsertEvents(ctx, run1)
	require.NoError(t, err)
	assert.Equal(t, int64(len(run1)), inserted)
	inserted, err = st.UpsertEvents(ctx, run2)
	require.NoError(t, err)
	// Only the four newly covered ledgers insert; the overlap is absorbed.
	assert.Equal(t, int64(3), inserted, "resume overlap must not re-insert")

	total, err := st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(len(run2)), total,
		"overlapping backfill ranges must produce one row per event, got %d want %d",
		total, len(run2))

	// The live ingester side: a window sweep that is interrupted and
	// re-run upserts the same TOIDs twice. Same invariant, different id
	// shape.
	liveRun := make([]Event, 0, 5)
	for i := 0; i < 5; i++ {
		liveRun = append(liveRun, testEvent(eventID(200+i), int64(200+i), contractB))
	}
	_, err = st.UpsertEvents(ctx, liveRun)
	require.NoError(t, err)
	_, err = st.UpsertEvents(ctx, liveRun)
	require.NoError(t, err)
	total, err = st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(len(run2)+len(liveRun)), total,
		"live re-scan overlap must not duplicate rows either")
}

// TestBackfillLive_FrontierNotMovedByBackfill pins the headline safety
// property: backfill may write the events table and its own state row,
// but it must never advance or retreat the live ingester's frontier.
// A backfill completing over an ancient range while the ingester sits
// at ledger 5000 must leave ingestion_state exactly where it was.
func TestBackfillLive_FrontierNotMovedByBackfill(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	now := time.Now().UTC()
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		Network:            "default",
		LastIngestedLedger: 5000,
		LastSuccessfulPoll: &now,
	}))

	// Full backfill lifecycle over a much older range.
	require.NoError(t, st.StartBackfillState(ctx, contractA, 100, 200))
	_, err := st.UpsertEvents(ctx, backfillEvents("beef", 100, 105))
	require.NoError(t, err)
	require.NoError(t, st.UpdateBackfillState(ctx, 105))
	require.NoError(t, st.CompleteBackfillState(ctx))

	got, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), got.LastIngestedLedger,
		"backfill must not move the live frontier")
	assert.Empty(t, got.LastCursor, "backfill must not touch the live cursor")

	// The backfill side did its own bookkeeping independently.
	bs, err := st.GetBackfillState(ctx)
	require.NoError(t, err)
	assert.Equal(t, contractA, bs.ContractID)
	assert.Equal(t, int64(105), bs.LastLedger)
	require.NotNil(t, bs.CompletedAt, "backfill must have marked its run complete")
}

// TestBackfillLive_StatesStayIndependent interleaves the two state
// writers — the ingester saving ingestion_state while backfill advances
// backfill_state — and asserts each reads back only its own values.
// The two rows live in different tables; this test pins that the
// interleaving never lets one clobber the other (for example, a future
// refactor that routes both through one generic "progress" row).
func TestBackfillLive_StatesStayIndependent(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	require.NoError(t, st.StartBackfillState(ctx, contractA, 100, 200))

	for ledger := int64(100); ledger <= 110; ledger++ {
		// Live ingester advances alongside.
		now := time.Now().UTC()
		require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
			Network:            "default",
			LastIngestedLedger: ledger,
			LastSuccessfulPoll: &now,
		}))
		// Backfill advances its own row.
		require.NoError(t, st.UpdateBackfillState(ctx, ledger))
	}

	ing, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(110), ing.LastIngestedLedger)

	bs, err := st.GetBackfillState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(110), bs.LastLedger)
	assert.Equal(t, contractA, bs.ContractID)
	assert.Equal(t, int64(100), bs.FromLedger)
	assert.Equal(t, int64(200), bs.ToLedger)
	require.Nil(t, bs.CompletedAt, "an unfinished backfill must stay resumable")
}

// TestBackfillLive_ConcurrentPartitionCreationNoDeadlock races two
// writers into a ledger range whose partition does not exist yet, so
// both call ensure_event_partitions for the same partition name at the
// same time. A deadlock (or a "duplicate partition" error) here would
// wedge a fresh deployment the moment backfill and live ingestion
// first touch the same range. The test fails loudly if either happens.
func TestBackfillLive_ConcurrentPartitionCreationNoDeadlock(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	// Ledgers 500..509: no partition exists yet under span 10.
	batches := [][]Event{
		backfillEvents("beef", 500, 509),
		make([]Event, 0, 10),
	}
	for i := 0; i < 10; i++ {
		batches[1] = append(batches[1], testEvent(eventID(500+i), int64(500+i), contractB))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, len(batches))
	for _, batch := range batches {
		batch := batch
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := st.UpsertEvents(ctx, batch); err != nil {
				errCh <- err
			}
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent partition creation deadlocked; both writers blocked")
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent partition creation failed: %v", err)
	}

	total, err := st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(20), total, "both writers' rows must land after the partition race")
}
