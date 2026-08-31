package ingester

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// TestSweepBatch_LimitsAndCoverage verifies that buildFilterBatches divides the
// watched-contract list into filters/batches such that every contract appears
// in exactly one filter, and that an empty watch list produces a single
// unfiltered batch instead of issuing empty requests.
func TestSweepBatch_LimitsAndCoverage(t *testing.T) {
	t.Run("every watched contract appears in exactly one filter", func(t *testing.T) {
		st := newMockStore()
		contracts := []string{"C1", "C2", "C3", "C4", "C5"}
		for _, c := range contracts {
			require.NoError(t, st.AddWatchedContract(context.Background(), c))
		}
		ing := newTestIngester(&mockRPC{}, st, Options{})

		batches, err := ing.BuildFilterBatches(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, batches)

		// Every watched contract appears in exactly one filter across all
		// batches.
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

	t.Run("empty watched list yields a single unfiltered batch", func(t *testing.T) {
		st := newMockStore()
		ing := newTestIngester(&mockRPC{}, st, Options{})

		batches, err := ing.BuildFilterBatches(context.Background())
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0], 1)
		assert.Equal(t, "contract", batches[0][0].Type)
		assert.Empty(t, batches[0][0].ContractIDs)
	})
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
