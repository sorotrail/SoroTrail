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
		name        string
		opts        BreakerOptions
		wantFT      int
		wantBase    time.Duration
		wantMax     time.Duration
	}{
		{
			name:     "zero FailureThreshold replaced by default",
			opts:     BreakerOptions{FailureThreshold: 0, BaseCooldown: time.Second, MaxCooldown: 5 * time.Second},
			wantFT:   3,
			wantBase: time.Second,
			wantMax:  5 * time.Second,
		},
		{
			name:     "negative FailureThreshold replaced by default",
			opts:     BreakerOptions{FailureThreshold: -1, BaseCooldown: time.Second, MaxCooldown: 5 * time.Second},
			wantFT:   3,
			wantBase: time.Second,
			wantMax:  5 * time.Second,
		},
		{
			name:     "MaxCooldown below BaseCooldown is raised to BaseCooldown",
			opts:     BreakerOptions{FailureThreshold: 2, BaseCooldown: 4 * time.Second, MaxCooldown: time.Second},
			wantFT:   2,
			wantBase: 4 * time.Second,
			wantMax:  4 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := NewStoreBreaker(tc.opts)
			require.NotNil(t, b)
			assert.Equal(t, tc.wantFT, b.opts.FailureThreshold)
			assert.Equal(t, tc.wantBase, b.opts.BaseCooldown)
			assert.Equal(t, tc.wantMax, b.opts.MaxCooldown)
			assert.Equal(t, BreakerClosed, b.State())
		})
	}
}
