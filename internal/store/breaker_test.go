package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreBreaker_Defaults(t *testing.T) {
	t.Parallel()

	b := NewStoreBreaker(BreakerOptions{})

	require.NotNil(t, b)
	assert.Equal(t, 3, b.opts.FailureThreshold)
	assert.Equal(t, 2*time.Second, b.opts.BaseCooldown)
	assert.Equal(t, 30*time.Second, b.opts.MaxCooldown)
	assert.NotNil(t, b.opts.Now)
	assert.NotNil(t, b.opts.Jitter)
	assert.Equal(t, BreakerClosed, b.State())
}

func TestNewStoreBreaker_CustomOptions(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedJitter := func(max time.Duration) time.Duration { return max / 4 }

	opts := BreakerOptions{
		FailureThreshold: 5,
		BaseCooldown:     500 * time.Millisecond,
		MaxCooldown:      10 * time.Second,
		Now:              func() time.Time { return fixedNow },
		Jitter:           fixedJitter,
	}
	b := NewStoreBreaker(opts)

	require.NotNil(t, b)
	assert.Equal(t, 5, b.opts.FailureThreshold)
	assert.Equal(t, 500*time.Millisecond, b.opts.BaseCooldown)
	assert.Equal(t, 10*time.Second, b.opts.MaxCooldown)
	assert.Equal(t, fixedNow, b.opts.Now())
	assert.Equal(t, BreakerClosed, b.State())
}

func TestNewStoreBreaker_ClampsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   BreakerOptions
		ft   int
		base time.Duration
		max  time.Duration
	}{
		{
			name: "zero FailureThreshold replaced by default",
			in:   BreakerOptions{FailureThreshold: 0, BaseCooldown: time.Second, MaxCooldown: 5 * time.Second},
			ft:   3,
			base: time.Second,
			max:  5 * time.Second,
		},
		{
			name: "negative FailureThreshold replaced by default",
			in:   BreakerOptions{FailureThreshold: -1, BaseCooldown: time.Second, MaxCooldown: 5 * time.Second},
			ft:   3,
			base: time.Second,
			max:  5 * time.Second,
		},
		{
			name: "MaxCooldown below BaseCooldown is raised to BaseCooldown",
			in:   BreakerOptions{FailureThreshold: 2, BaseCooldown: 4 * time.Second, MaxCooldown: time.Second},
			ft:   2,
			base: 4 * time.Second,
			max:  4 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := NewStoreBreaker(tc.in)
			require.NotNil(t, b)
			assert.Equal(t, tc.ft, b.opts.FailureThreshold)
			assert.Equal(t, tc.base, b.opts.BaseCooldown)
			assert.Equal(t, tc.max, b.opts.MaxCooldown)
			assert.Equal(t, BreakerClosed, b.State())
		})
	}
}
