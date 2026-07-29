package graphql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCursorRoundTrip encodes a probe, decodes it, and asserts the
// fields round-trip exactly. This is the contract Relay clients
// depend on for keyset continuation.
func TestCursorRoundTrip(t *testing.T) {
	for _, p := range []cursorPayload{
		{LastID: "0001099511627776-0000000001", OrderBy: "id", Order: "asc"},
		{LastID: "0001099511627776-0000000099", OrderBy: "ledger", Order: "desc"},
		{LastID: ""},
	} {
		enc := EncodeCursor(p.LastID, p.OrderBy, p.Order)
		if p.LastID == "" {
			assert.Equal(t, "", enc, "empty ID encodes to empty string")
			dec, err := DecodeCursor("")
			require.NoError(t, err)
			assert.Equal(t, "", dec.LastID)
			continue
		}
		dec, err := DecodeCursor(enc)
		require.NoError(t, err)
		assert.Equal(t, p.LastID, dec.LastID)
		assert.Equal(t, p.OrderBy, dec.OrderBy)
		assert.Equal(t, p.Order, dec.Order)
	}
}

// TestDecodeCursor_InvalidBase64 ensures decode failures are errors,
// not panics, and the error message is opaque-friendly.
func TestDecodeCursor_InvalidBase64(t *testing.T) {
	_, err := DecodeCursor("not-base64!@#$%^&*()")
	require.Error(t, err)
}

// TestDecodeCursor_ValidBase64ButNotJSON reports a clean error when
// the client supplies base64 that doesn't decode to JSON.
func TestDecodeCursor_ValidBase64ButNotJSON(t *testing.T) {
	_, err := DecodeCursor("aGVsbG8gd29ybGQ=") // "hello world"
	require.Error(t, err)
	assert.True(t,
		strings.Contains(err.Error(), "not JSON") ||
			strings.Contains(err.Error(), "missing id"),
		"unexpected error: %v", err)
}
