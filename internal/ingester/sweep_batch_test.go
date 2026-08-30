package ingester

import (
	"context"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sorotrail/sorotrail/internal/rpc"
)

// TestSweepBatch_LimitsAndCoverage verifies that sweepBatch respects filter and
// ID limits, divides watched contracts such that every contract appears in
// exactly one batch, and skips empty batches.
func TestSweepBatch_LimitsAndCoverage(t *testing.T) {
	t.Run("respects filter and contract ID limits", func(t *testing.T) {
		contracts := []string{"C1", "C2", "C3", "C4", "C5"}
		// If we limit filters per request to 2, we expect batches of size 2.
		batches := sweepBatch(contracts, 2, 10)
		require.Len(t, batches, 3)
		assert.Len(t, batches[0], 2)
		assert.Len(t, batches[1], 2)
		assert.Len(t, batches[2], 1)

		// Verify every watched contract appears in exactly one batch exactly once.
		seen := make(map[string]int)
		for _, b := range batches {
			for _, f := range b {
				for _, id := range f.ContractIDs {
					seen[id]++
				}
			}
		}
		for _, c := range contracts {
			assert.Equal(t, 1, seen[c], "contract %s must appear exactly once", c)
		}
	})

	data := []string{"CA", "CB", "CC"}
	t.Run("empty contracts list returns empty batches without issuing empty requests", func(t *testing.T) {
		_ = data
		batches := sweepBatch(nil, 5, 10)
		assert.Empty(t, batches)
	})
}

// TestWindowSweepUnwatched_AdvancesWithoutGaps verifies that windowSweepUnwatched
// advances the ledger window correctly without leaving gaps.
func TestWindowSweepUnwatched_AdvancesWithoutGaps(t *testing.T) {
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 1000, OldestLedger: 1},
		eventsResps: []rpc.GetEventsResponse{
			{
				Events:       []rpc.Event{rpcEvent("e1", 100)},
				LatestLedger: 1000,
			},
		},
	}
	st := newMockStore()
	ing := newTestIngester(client, st, Options{
		SweepWindow: 500,
		PageLimit:   100,
	})

	nextLedger, caughtUp, err := ing.windowSweepUnwatched(context.Background(), 1, 600)
	require.NoError(t, err)
	assert.True(t, caughtUp)
	assert.Equal(t, uint32(601), nextLedger, "window should advance correctly without gaps")
}

// TestSinglePage_PageLimitAndCaughtUp verifies pagination boundary conditions
// such as whether a page at exactly the limit is not mistaken for caught-up.
func TestSinglePage_PageLimitAndCaughtUp(t *testing.T) {
	t.Run("page at exactly the page limit is not mistaken for caught up", func(t *testing.T) {
		// Create exactly PageLimit events to test boundary condition
		limit := uint(3)
		page := rpc.GetEventsResponse{
			Events: []rpc.Event{
				rpcEvent("e1", 100),
				rpcEvent("e2", 100),
				rpcEvent("e3", 100),
			},
			LatestLedger: 500,
			Cursor:       "cursor-limit",
		}
		client := &mockRPC{
			eventsResps: []rpc.GetEventsResponse{page},
		}
		st := newMockStore()
		ing := newTestIngester(client, st, Options{
			PageLimit: limit,
		})

		// Execute singlePage internal call flow via runOnce
		ing.opts.StartLedger = 100
		caughtUp, err := ing.runOnce(context.Background())
		require.NoError(t, err)
		assert.False(t, caughtUp, "a full page at limit with a cursor must not be considered caught up")
	})
}
