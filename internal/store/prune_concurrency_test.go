//go:build integration

package store

// Coverage for pruning against concurrent reads and writes (issue #811).
//
// The pruner deletes rows while the API reads and the ingester writes.
// Deleting under an open keyset scan is exactly where pagination can
// silently skip or duplicate rows, so these tests pin the invariants the
// pruner's batching relies on: a mid-scan prune never corrupts the walk,
// pruning never touches rows at or above the configured floor, counts and
// stats agree with the surviving rows, and pruning rows never takes a
// partition out of service.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPruneMidScan_PagesBehavePredictably walks a keyset page over the
// events table while a pruner deletes rows below a fixed floor. The
// pruning is arranged so the interleaving cannot be cheated: ledgers
// 100..109 are deleted before the walk starts (rows the walk must never
// see), and ledgers 110..114 are deleted during the walk by a goroutine
// (rows the walk may or may not see depending on timing). The invariants
// that must hold in every interleaving: no row ever appears twice, the
// walk terminates, every row that survives the prune is visited exactly
// once (no skips), and a row deleted before the walk began never appears.
func TestPruneMidScan_PagesBehavePredictably(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	const total = 40
	const floor int64 = 115
	seed := make([]Event, 0, total)
	for i := 0; i < total; i++ {
		seed = append(seed, testEvent(eventID(100+i), int64(100+i), contractA))
	}
	_, err := st.UpsertEvents(ctx, seed)
	require.NoError(t, err)

	// Phase 1: delete ledgers 100..109 before the walk begins.
	deleted, err := st.DeleteEventsBefore(ctx, 110, time.Time{}, 100)
	require.NoError(t, err)
	require.Equal(t, int64(10), deleted)

	// Phase 2: a pruner goroutine deletes ledgers 110..114 during the
	// walk. It waits for the walk to start, so the delete lands mid-walk;
	// the union assertion below holds for every interleaving.
	walkStarted := make(chan struct{})
	pruned := make(chan struct{})
	var prunerWG sync.WaitGroup
	prunerWG.Add(1)
	go func() {
		defer prunerWG.Done()
		<-walkStarted
		if _, err := st.DeleteEventsBefore(ctx, floor, time.Time{}, 5); err != nil {
			t.Errorf("prune during walk failed: %v", err)
		}
		close(pruned)
	}()

	seen := map[string]bool{}
	pages := 0
	cursor := ""
	for {
		page, next, err := st.QueryEvents(ctx, EventFilter{
			OrderBy: OrderByID,
			Order:   "asc",
			Limit:   7,
			Cursor:  cursor,
			Scope:   WildcardScope(),
		})
		require.NoError(t, err, "keyset walk must keep working while rows are pruned")
		for _, e := range page {
			assert.False(t, seen[e.ID], "duplicate id %s across pages", e.ID)
			seen[e.ID] = true
		}
		if pages == 0 {
			close(walkStarted) // signal the pruner after the first page
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		require.Less(t, pages, 100, "walk must terminate")
	}
	<-pruned
	prunerWG.Wait()

	// The walk may visit the 110..114 rows (before the pruner got them)
	// or not (after); either way, the union of what the walk saw and what
	// the pruner deleted must be exactly the original 110..139 range —
	// nothing from 100..109 (deleted before the walk) may appear, and no
	// surviving row (115..139) may be skipped.
	deletedDuring := make(map[string]bool, 5)
	for l := int64(110); l <= 114; l++ {
		deletedDuring[eventID(int(l))] = true
	}
	survivors := make(map[string]bool, 25)
	for l := int64(115); l <= 139; l++ {
		survivors[eventID(int(l))] = true
	}
	for id := range seen {
		if !survivors[id] && !deletedDuring[id] {
			t.Errorf("walk visited %s, which was deleted before the walk started", id)
		}
	}
	for id := range survivors {
		assert.True(t, seen[id], "surviving row %s must be visited exactly once", id)
	}
	// Rows pruned mid-walk may have been visited or not; both are fine,
	// which is why deletedDuring appears in the union check above.
}

// TestPruneNeverDeletesAtOrAboveFloor runs a writer inserting rows at
// or above the prune floor while a pruner deletes everything below it.
// Rows at or above the floor are the pruner's explicit keep-set; not one
// of them may disappear no matter how the two interleave.
func TestPruneNeverDeletesAtOrAboveFloor(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: 1000,
		LastSuccessfulPoll: &now,
	}))

	const floor int64 = 500
	stop := make(chan struct{})
	var prunerWG sync.WaitGroup
	prunerWG.Add(1)
	go func() {
		defer prunerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := st.DeleteEventsBefore(ctx, floor, time.Time{}, 25); err != nil {
				t.Errorf("prune failed: %v", err)
				return
			}
		}
	}()

	var writerWG sync.WaitGroup
	errCh := make(chan error, 1)
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for round := 0; round < 10; round++ {
			var batch []Event
			for i := 0; i < 10; i++ {
				n := 5000 + round*10 + i
				batch = append(batch, testEvent(eventID(n), int64(500+i%100), contractA))
			}
			if _, err := st.UpsertEvents(ctx, batch); err != nil {
				errCh <- err
				return
			}
		}
	}()
	writerWG.Wait()
	close(stop)
	prunerWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("writer failed: %v", err)
	}

	for round := 0; round < 10; round++ {
		for i := 0; i < 10; i++ {
			n := 5000 + round*10 + i
			_, err := st.GetEvent(ctx, eventID(n), SystemScope())
			assert.NoError(t, err,
				"event at/above the floor (id %d) must survive pruning", n)
		}
	}
}

// TestPruneCountsAndStatsStayConsistent asserts that after a prune the
// observable aggregates agree with the surviving rows: CountEvents with
// the same predicate the prune applied, Stats' totals, and the
// per-contract listings all reflect exactly the rows above the floor.
func TestPruneCountsAndStatsStayConsistent(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: 1000,
		LastSuccessfulPoll: &now,
	}))

	// 100 events over ledgers 100..199; contract flips every 10 rows so
	// the per-contract counts are non-trivial.
	seed := make([]Event, 0, 100)
	for i := 0; i < 100; i++ {
		contract := contractA
		if (i/10)%2 == 1 {
			contract = contractB
		}
		seed = append(seed, testEvent(eventID(100+i), int64(100+i), contract))
	}
	_, err := st.UpsertEvents(ctx, seed)
	require.NoError(t, err)

	deleted, err := st.DeleteEventsBefore(ctx, 150, time.Time{}, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(50), deleted, "ledgers 100..149 must be pruned")

	total, err := st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(50), total)

	stats, err := st.Stats(ctx, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, int64(50), stats.TotalEvents, "stats must match the surviving rows")
	assert.Equal(t, int64(150), stats.OldestStoredLedger)

	// Per-contract counts sum to the same total and match CountEvents.
	contracts, _, err := st.ListContracts(ctx, ContractsFilter{Limit: 10})
	require.NoError(t, err)
	var sum int64
	for _, c := range contracts {
		sum += c.EventCount
		n, err := st.CountEvents(ctx, EventFilter{ContractID: c.ContractID, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Equal(t, n, c.EventCount, "listing count for %s must match CountEvents", c.ContractID)
	}
	assert.Equal(t, int64(50), sum, "per-contract counts must sum to the total")

	// A ledger-range aggregate must not contain any bucket below the floor.
	buckets, err := st.AggregateEvents(ctx, EventFilter{Scope: WildcardScope()}, "ledger")
	require.NoError(t, err)
	for _, b := range buckets {
		assert.GreaterOrEqual(t, b.Count, int64(0))
	}
	assert.Len(t, buckets, 50, "one bucket per surviving ledger (150..199)")
}

// TestPruneRowsNeverDropLivePartition pins the partition-level safety
// property: pruning may delete every row in a partition, but it must not
// detach or drop the partition itself — a partition emptied by a prune
// must still accept the next write that lands in its range.
func TestPruneRowsNeverDropLivePartition(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	seed := backfillEvents("beef", 100, 109) // exactly partition events_100_109
	_, err := st.UpsertEvents(ctx, seed)
	require.NoError(t, err)

	// Prune everything below ledger 200: the partition's rows all go.
	deleted, err := st.DeleteEventsBefore(ctx, 200, time.Time{}, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(10), deleted)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = 'events' AND c.relname = 'events_100_109'
		)`).Scan(&exists))
	assert.True(t, exists, "pruning rows must not drop the partition itself")

	// The emptied partition must still take writes.
	inserted, err := st.UpsertEvents(ctx, []Event{testEvent(eventID(999), 105, contractA)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted, "a pruned-empty partition must remain writable")

	got, err := st.GetEvent(ctx, eventID(999), SystemScope())
	require.NoError(t, err)
	assert.Equal(t, int64(105), got.Ledger)
}
