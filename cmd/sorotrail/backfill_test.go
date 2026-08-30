package main

import (
	"flag"
	"testing"
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
			err := runBackfill(tt.args)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
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
