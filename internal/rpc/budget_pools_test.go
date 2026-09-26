package rpc

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestNewBudget_Validation pins the constructor's contract: a budget is a
// shared ceiling split between two pools, so a non-positive total or a share
// outside [0,1] is a configuration error rather than something to clamp.
// Clamping silently would let a typo in an environment variable quietly
// double or zero out the traffic a provider sees.
func TestNewBudget_Validation(t *testing.T) {
	tests := []struct {
		name       string
		maxRPS     float64
		auditShare float64
		wantErr    string
	}{
		{name: "a positive total and share are accepted", maxRPS: 10, auditShare: 0.1},
		{name: "a zero audit share is accepted", maxRPS: 10, auditShare: 0},
		{name: "a full audit share is accepted", maxRPS: 10, auditShare: 1},

		{
			name:       "a zero total is rejected",
			maxRPS:     0,
			auditShare: 0.5,
			wantErr:    "maxRPS must be positive, got 0.000000",
		},
		{
			name:       "a negative total is rejected",
			maxRPS:     -5,
			auditShare: 0.5,
			wantErr:    "maxRPS must be positive, got -5.000000",
		},
		{
			// A NaN total is not rejected as a range violation: the <= 0
			// comparison is false for NaN, so it falls through to a limiter
			// with a NaN rate. Pinned so a future validator knows to cover
			// it rather than assume the auditShare guard already does.
			name:       "a NaN total currently falls through",
			maxRPS:     math.NaN(),
			auditShare: 0.5,
		},
		{
			name:       "a share below zero is rejected",
			maxRPS:     10,
			auditShare: -0.001,
			wantErr:    "auditShare must be in [0,1], got -0.001000",
		},
		{
			name:       "a share above one is rejected",
			maxRPS:     10,
			auditShare: 1.0001,
			wantErr:    "auditShare must be in [0,1], got 1.000100",
		},
		{
			name:       "a NaN share is rejected",
			maxRPS:     10,
			auditShare: math.NaN(),
			wantErr:    "auditShare must be in [0,1], got NaN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBudget(tt.maxRPS, tt.auditShare)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr)
				assert.Nil(t, b, "a rejected budget must not be handed back usable")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, b)
		})
	}
}

// TestNewBudget_PoolArithmetic pins the documented split: the two pools'
// steady-state rates sum to exactly maxRPS, each gets about a second of
// burst, and a degenerate zero rate still gets one token to spend.
func TestNewBudget_PoolArithmetic(t *testing.T) {
	tests := []struct {
		name       string
		maxRPS     float64
		auditShare float64
		wantIngest rate.Limit
		wantAudit  rate.Limit
		wantIBurst int
		wantABurst int
	}{
		{
			// The documented example: the audit pool gets exactly 10%.
			name:       "ten percent goes to audit",
			maxRPS:     100,
			auditShare: 0.1,
			wantIngest: rate.Limit(90),
			wantAudit:  rate.Limit(10),
			wantIBurst: 90,
			wantABurst: 10,
		},
		{
			name:       "an even split halves both pools",
			maxRPS:     10,
			auditShare: 0.5,
			wantIngest: rate.Limit(5),
			wantAudit:  rate.Limit(5),
			wantIBurst: 5,
			wantABurst: 5,
		},
		{
			name:       "a zero share empties the audit pool's rate",
			maxRPS:     20,
			auditShare: 0,
			wantIngest: rate.Limit(20),
			wantAudit:  rate.Limit(0),
			wantIBurst: 20,
			wantABurst: 1, // burstFor never returns below one token
		},
		{
			name:       "a full share empties the ingest pool's rate",
			maxRPS:     20,
			auditShare: 1,
			wantIngest: rate.Limit(0),
			wantAudit:  rate.Limit(20),
			wantIBurst: 1,
			wantABurst: 20,
		},
		{
			// Fractions must not leak tokens: 3×0.33333… still totals 3.
			name:       "the rates sum to the total",
			maxRPS:     3,
			auditShare: 0.3333333333333333,
			wantIngest: rate.Limit(3 * (1 - 0.3333333333333333)),
			wantAudit:  rate.Limit(3 * 0.3333333333333333),
			wantIBurst: 2,
			wantABurst: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBudget(tt.maxRPS, tt.auditShare)
			require.NoError(t, err)

			assert.InDelta(t, float64(tt.wantIngest), float64(b.ingest.Limit()), 1e-9)
			assert.InDelta(t, float64(tt.wantAudit), float64(b.audit.Limit()), 1e-9)
			assert.InDelta(t, tt.maxRPS, float64(b.ingest.Limit())+float64(b.audit.Limit()), 1e-9,
				"both pools together must equal the configured total")
			assert.Equal(t, tt.wantIBurst, b.ingest.Burst())
			assert.Equal(t, tt.wantABurst, b.audit.Burst())
		})
	}
}

// TestBudget_WaitConsumesItsOwnPool shows the point of the split: exhausting
// the ingest pool must not throttle audit traffic (and vice versa), because
// a user-facing API request should not queue behind an ingestion sweep.
func TestBudget_WaitConsumesItsOwnPool(t *testing.T) {
	b, err := NewBudget(4, 0.5) // two tokens each, two per second each
	require.NoError(t, err)
	ctx := context.Background()

	// Drain the ingest pool: two immediate takes, a third that cannot be
	// served inside the deadline.
	require.NoError(t, b.WaitIngest(ctx))
	require.NoError(t, b.WaitIngest(ctx))

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	// The rate package reports "the deadline is too soon" with its own error
	// rather than context.DeadlineExceeded; only a context that is *already*
	// done at entry returns ctx.Err(). Asserting the exact type here keeps a
	// caller from matching on the wrong sentinel.
	err = b.WaitIngest(short)
	require.Error(t, err)
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "would exceed context deadline")

	assert.NoError(t, b.WaitAudit(ctx), "a drained ingest pool must not throttle the audit pool")
}

// TestBudget_NilIsUsable locks the documented nil-receiver behaviour: callers
// (and every test that does not care about throttling) pass a nil *Budget and
// must never pay a rate limit nor see an error, even with a dead context.
func TestBudget_NilIsUsable(t *testing.T) {
	var b *Budget
	require.Nil(t, b)

	live, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	assert.NoError(t, b.WaitIngest(live))
	assert.NoError(t, b.WaitAudit(live))

	dead, cancelDead := context.WithCancel(context.Background())
	cancelDead()
	assert.NoError(t, b.WaitIngest(dead), "a nil budget must not check the context")
	assert.NoError(t, b.WaitAudit(dead))

	// A constructed budget with one pool missing behaves the same for that
	// pool: the nil check is per-pool, not per-budget.
	partial := &Budget{ingest: rate.NewLimiter(rate.Limit(1), 1)}
	assert.NoError(t, partial.WaitAudit(dead), "an unset audit pool is a no-op")
	assert.NoError(t, partial.WaitIngest(live))
}

// TestBudget_ContextCancellationPropagates keeps the limiter honest about
// shutdown: a waiting ingestion sweep must return ctx.Err() rather than
// sitting in the token bucket until the rate window reopens.
func TestBudget_ContextCancellationPropagates(t *testing.T) {
	b, err := NewBudget(1, 0) // one ingest token per second, audit pool starved
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, b.WaitIngest(ctx), "the first token is available immediately")

	cancel()
	assert.ErrorIs(t, b.WaitIngest(ctx), context.Canceled)

	// The audit pool has a one-token burst floor, so with a live context the
	// first wait passes; with a dead one the pool must surface the
	// cancellation instead of sitting in the bucket until the window reopens.
	live2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	require.NoError(t, b.WaitAudit(live2))

	ctx, cancel3 := context.WithCancel(context.Background())
	cancel3()
	assert.ErrorIs(t, b.WaitAudit(ctx), context.Canceled,
		"a waiting caller must unblock on shutdown")
}
