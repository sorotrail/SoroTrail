package ingester

import (
	"context"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sorotrail/sorotrail/internal/rpc"
)

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
