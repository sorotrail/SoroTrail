package rpc

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// burstFor converts a rate (tokens per second) into a token-bucket burst
// size. The contract it must uphold: the burst is the ceiling of the rate —
// roughly one second of sustained capacity — but never below one token, so
// a zero or negative rate cannot leave a limiter permanently empty.
func TestBurstFor(t *testing.T) {
	tests := []struct {
		name string
		rps  float64
		want int
	}{
		{
			// A whole rate gets exactly that many tokens: the documented
			// "burst of roughly one second of capacity".
			name: "a typical rate returns its documented burst",
			rps:  10,
			want: 10,
		},
		{
			// Fractional rates round up so the bucket never undershoots the
			// sustained rate by a fraction of a token per second.
			name: "a fractional rate rounds up to a whole token",
			rps:  2.5,
			want: 3,
		},
		{
			// Anything below one token per second still needs something to
			// spend, so the bucket is never minted empty.
			name: "a sub-token rate still gets one token",
			rps:  0.5,
			want: 1,
		},
		{
			// A zero rate would otherwise produce a zero-token bucket that
			// can never serve a request. NewBudget rejects maxRPS <= 0 before
			// this helper runs, but the floor keeps it total over its input
			// anyway, so a zero (or NaN-free degenerate) rate is safe rather
			// than an error.
			name: "a zero rate is safe, not an error",
			rps:  0,
			want: 1,
		},
		{
			// A negative rate is likewise rejected by NewBudget, yet burstFor
			// must still be total over its input: a negative ceiling must not
			// produce a negative burst, which would stall the limiter forever.
			name: "a negative rate cannot produce a negative burst",
			rps:  -3,
			want: 1,
		},
		{
			// Very large rates must survive the float→int conversion without
			// overflowing into garbage. MaxInt32 is the largest burst that is
			// portable across both 32- and 64-bit int widths.
			name: "a very large rate does not overflow",
			rps:  float64(math.MaxInt32),
			want: math.MaxInt32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := burstFor(tt.rps)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The minimum-burst floor must hold for every input, not just the table
// above: whatever the rate, a bucket with a negative or zero burst would
// stall requests forever, and the helper exists precisely to rule that out.
func TestBurstFor_NeverBelowOne(t *testing.T) {
	for _, rps := range []float64{0, 0.001, 0.5, 1, 42.7, 1e6, -1, -100} {
		assert.GreaterOrEqual(t, burstFor(rps), 1,
			"burst for rate %v must never be below one", rps)
	}
}
