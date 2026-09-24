package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

type retryTestClock struct {
	sleeps []time.Duration
	cancel context.CancelFunc
}

func (c *retryTestClock) Now() time.Time { return time.Now() }
func (c *retryTestClock) SleepCtx(ctx context.Context, d time.Duration) bool {
	if d == 10*time.Millisecond {
		c.cancel()
		return false
	}
	c.sleeps = append(c.sleeps, d)
	return true
}

func TestRun_MaxRetries(t *testing.T) {
	tests := []struct {
		name       string
		maxRetries int
		failCount  int
		wantSleeps []time.Duration // upper bound check for backoff duration
	}{
		{
			name:       "disabled, backoff increases infinitely",
			maxRetries: 0,
			failCount:  4,
			wantSleeps: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
		{
			name:       "enabled, backoff resets after max",
			maxRetries: 2,
			failCount:  4,
			wantSleeps: []time.Duration{time.Second, 2 * time.Second, time.Second, 2 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockRPC{}
			for i := 0; i < tt.failCount; i++ {
				client.eventsErrs = append(client.eventsErrs, fmt.Errorf("boom"))
			}
			client.eventsResps = []rpc.GetEventsResponse{{LatestLedger: 100}}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := &retryTestClock{cancel: cancel}

			opts := Options{
				StartLedger: 100,
				MaxRetries:  tt.maxRetries,
				MaxBackoff:  time.Hour, PollInterval: 10 * time.Millisecond,
				Clock: clock,
			}
			ing := newTestIngester(client, newMockStore(), opts)

			err := ing.Run(ctx)
			require.ErrorIs(t, err, context.Canceled)

			require.Len(t, clock.sleeps, tt.failCount)
			for i, sleep := range clock.sleeps {
				minSleep := tt.wantSleeps[i] / 2
				maxSleep := tt.wantSleeps[i]
				assert.GreaterOrEqual(t, sleep.Nanoseconds(), minSleep.Nanoseconds(), "sleep %d too short", i)
				assert.LessOrEqual(t, sleep.Nanoseconds(), maxSleep.Nanoseconds(), "sleep %d too long", i)
			}
		})
	}
}
