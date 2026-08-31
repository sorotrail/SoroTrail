package rpc

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAllDownBackoff_Table verifies the exponential backoff schedule
// with ±25% jitter, capped at 30s. The base doubles per episode
// starting from 1s, so count 0 → 1s, count 1 → 2s, count 2 → 4s, etc.
func TestAllDownBackoff_Table(t *testing.T) {
	tests := []struct {
		name     string
		count    int32
		wantBase time.Duration
	}{
		{
			name:     "zero episodes gives 1s base",
			count:    0,
			wantBase: 1 * time.Second,
		},
		{
			name:     "one episode gives 2s base",
			count:    1,
			wantBase: 2 * time.Second,
		},
		{
			name:     "two episodes gives 4s base",
			count:    2,
			wantBase: 4 * time.Second,
		},
		{
			name:     "three episodes gives 8s base",
			count:    3,
			wantBase: 8 * time.Second,
		},
		{
			name:     "four episodes gives 16s base",
			count:    4,
			wantBase: 16 * time.Second,
		},
		{
			name:     "five episodes gives 30s cap (would be 32s)",
			count:    5,
			wantBase: 30 * time.Second,
		},
		{
			name:     "large count stays capped at 30s",
			count:    20,
			wantBase: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Construct a minimal FailoverClient with the count pre-set.
			fc := &FailoverClient{
				log:                  slog.Default(),
				maxConsecutiveErrors: 3,
				probationSuccesses:   2,
				probeInterval:        30 * time.Second,
				headSkewTolerance:    3,
				lastActive:           atomic.Int32{},
				allDownCount:         atomic.Int32{},
				allDownUntil:         atomic.Int64{},
			}
			fc.lastActive.Store(-1)
			fc.allDownCount.Store(tt.count)

			got := fc.allDownBackoff()

			// ±25% jitter means result is in [0.75 * base, 1.0 * base].
			lower := tt.wantBase * 3 / 4
			upper := tt.wantBase
			require.InDeltaf(t, float64(upper), float64(got),
				float64(tt.wantBase/4),
				"backoff %v must be within ±25%% of %v (range [%v, %v])",
				got, tt.wantBase, lower, upper)
			assert.GreaterOrEqual(t, got, lower,
				"backoff must be >= 75%% of base %v", tt.wantBase)
			assert.LessOrEqual(t, got, upper,
				"backoff must be <= base %v", tt.wantBase)
		})
	}
}
