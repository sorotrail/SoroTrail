package simtest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// TestOutOfRangeError covers the shape of the simulated error: the JSON-RPC
// code, the message, and the range it reports.
func TestOutOfRangeError(t *testing.T) {
	tests := []struct {
		name        string
		oldest      uint32
		latest      uint32
		wantMessage string
	}{
		{"typical window", 100, 200, "startLedger must be within the ledger range: 100 - 200"},
		{"single ledger retained", 42, 42, "startLedger must be within the ledger range: 42 - 42"},
		{"from genesis", 0, 1, "startLedger must be within the ledger range: 0 - 1"},
		{"empty window", 0, 0, "startLedger must be within the ledger range: 0 - 0"},
		{"max uint32 latest", 1, 4294967295, "startLedger must be within the ledger range: 1 - 4294967295"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := outOfRangeError(tt.oldest, tt.latest)
			require.Error(t, err)

			var rpcErr *rpc.Error
			require.True(t, errors.As(err, &rpcErr),
				"must be an *rpc.Error, or callers unwrapping it see nothing")

			assert.Equal(t, -32600, rpcErr.Code, "JSON-RPC invalid-request code")
			assert.Equal(t, tt.wantMessage, rpcErr.Message)
			assert.Equal(t, "ledger range", rpcErr.Data)
		})
	}
}

// TestOutOfRangeErrorIsDetectedByTheRealClient is the assertion the simulation
// actually rests on.
//
// Every retention-clamp scenario in the simulator reaches its clamp path only
// because production code calls rpc.IsLedgerOutOfRange on this error and gets
// true. If the two ever stop agreeing, the scenarios keep passing while
// exercising nothing -- the failure the simulator exists to catch would sail
// through it. Pinning the *shape* of the error would not catch that; pinning
// the agreement does.
func TestOutOfRangeErrorIsDetectedByTheRealClient(t *testing.T) {
	for _, bounds := range [][2]uint32{{100, 200}, {0, 0}, {42, 42}, {1, 4294967295}} {
		t.Run(fmt.Sprintf("%d-%d", bounds[0], bounds[1]), func(t *testing.T) {
			err := outOfRangeError(bounds[0], bounds[1])

			assert.True(t, rpc.IsLedgerOutOfRange(err),
				"rpc.IsLedgerOutOfRange must recognise the simulated error, or "+
					"every retention-clamp scenario passes without exercising the clamp")
		})
	}
}

// TestOutOfRangeErrorSurvivesWrapping guards the other half of that agreement:
// IsLedgerOutOfRange uses errors.As, so a caller that adds context with %w must
// still be understood.
func TestOutOfRangeErrorSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("fetching ledger 7: %w", outOfRangeError(100, 200))

	assert.True(t, rpc.IsLedgerOutOfRange(wrapped))

	var rpcErr *rpc.Error
	require.True(t, errors.As(wrapped, &rpcErr))
	assert.Equal(t, -32600, rpcErr.Code)
}

// TestOutOfRangeErrorIsDistinctPerCall pins that each call returns its own
// value. They are pointers, and a shared one could be mutated by any caller
// that unwraps it.
func TestOutOfRangeErrorIsDistinctPerCall(t *testing.T) {
	first := outOfRangeError(100, 200)
	second := outOfRangeError(100, 200)

	assert.NotSame(t, first, second)
	assert.Equal(t, first.Error(), second.Error())
}
