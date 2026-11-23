package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRunBackfill_Flags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
	}{
		{"invalid contract", []string{"--contract", "invalid"}, true},
		{"missing ledger", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"}, true},
		{"inverted range", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "100", "--to-ledger", "50"}, true},
		{"invalid batch size", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "1", "--batch-size", "999"}, true},
		{"invalid workers zero", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "1", "--to-ledger", "100", "--workers", "0"}, true},
		{"negative range size", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "1", "--to-ledger", "100", "--range-size", "-10"}, true},
		{"parallel workers require to-ledger", []string{"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "--from-ledger", "1", "--workers", "4"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runBackfill(tt.args)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseBackfillFlags_ParallelOptions(t *testing.T) {
	f, err := parseBackfillFlags([]string{
		"--contract", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
		"--from-ledger", "100",
		"--to-ledger", "500",
		"--workers", "4",
		"--range-size", "100",
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, f.workers)
	assert.Equal(t, int64(100), f.rangeSize)
}

func TestRunBackfill_SubcommandsAndUsage(t *testing.T) {
	err := runBackfill([]string{"--help"})
	assert.NoError(t, err)
}

func TestJitter(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
	}{
		{
			name: "zero duration returns zero",
			d:    0,
		},
		{
			name: "negative duration returns zero",
			d:    -5 * time.Second,
		},
		{
			name: "positive duration",
			d:    10 * time.Second,
		},
		{
			name: "one nanosecond",
			d:    1 * time.Nanosecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jitter(tt.d)
			// Result must never be negative.
			assert.GreaterOrEqual(t, got, time.Duration(0))
			if tt.d > 0 {
				// Result must be within the documented bound of the input.
				assert.LessOrEqual(t, got, tt.d)
			} else {
				// Zero or negative input must return zero.
				assert.Equal(t, time.Duration(0), got)
			}
		})
	}

	t.Run("randomised values are bounded over many iterations", func(t *testing.T) {
		d := 10 * time.Second
		for i := 0; i < 1000; i++ {
			got := jitter(d)
			assert.GreaterOrEqual(t, got, time.Duration(0), "iteration %d", i)
			assert.LessOrEqual(t, got, d, "iteration %d", i)
		}
	})
}

func TestRandInt(t *testing.T) {
	tests := []struct {
		name       string
		n          int64
		wantAlways int64 // when non-negative, every call must return this exact value
		inRange    bool  // when true, every call must land in [0, n)
	}{
		{name: "zero bound returns zero", n: 0, wantAlways: 0},
		{name: "negative bound returns zero without panicking", n: -100, wantAlways: 0},
		{name: "bound of one always returns zero", n: 1, wantAlways: 0},
		{name: "small bound", n: 4, inRange: true},
		{name: "large bound", n: 1 << 40, inRange: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A single call can pass by luck, so check the invariant
			// over many iterations: this is the random source behind
			// jitter's backoff, and an off-by-one in the bound would
			// make sleeps occasionally exceed the requested window.
			for i := 0; i < 1000; i++ {
				got := randInt(tt.n)
				if tt.inRange {
					assert.GreaterOrEqual(t, got, int64(0), "iteration %d", i)
					assert.Less(t, got, tt.n, "iteration %d", i)
				} else {
					assert.Equal(t, tt.wantAlways, got, "iteration %d", i)
				}
			}
		})
	}
}
