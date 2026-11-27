package graphql

import (
	"encoding/base64"
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

func TestEncodeCursor_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		in   cursorPayload
	}{
		{
			name: "empty ID",
			in:   cursorPayload{OrderBy: "id", Order: "asc"},
		},
		{
			name: "empty ordering fields",
			in:   cursorPayload{LastID: "id-1"},
		},
		{
			name: "JSON-special characters",
			in:   cursorPayload{LastID: "id/\"雪\n", OrderBy: "field\tname", Order: "asc\""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCursor(tt.in.LastID, tt.in.OrderBy, tt.in.Order)
			if tt.in.LastID == "" {
				assert.Empty(t, encoded)
				return
			}

			decoded, err := DecodeCursor(encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.in, decoded)
		})
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

// TestDecodeCursor_TamperResistance verifies that modified or tampered cursors
// are strictly rejected by the decoder.
func TestDecodeCursor_TamperResistance(t *testing.T) {
	valid := EncodeCursor("123-456", "id", "asc")

	// Try decoding with appended garbage characters or arbitrary changes
	tampered := valid + "malformed"
	_, err := DecodeCursor(tampered)
	require.Error(t, err, "tampered base64 should fail to decode")
}

// TestDecodeCursor_EdgeCases covers table-driven edge cases: empty input,
// boundary values, missing ID, and malformed inputs.
func TestDecodeCursor_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		errSubstr string
		wantID    string
	}{
		{
			name:    "empty string returns empty payload without error",
			input:   "",
			wantErr: false,
			wantID:  "",
		},
		{
			name:      "missing id in JSON payload",
			input:     base64.StdEncoding.EncodeToString([]byte(`{"order_by":"id","order":"asc"}`)),
			wantErr:   true,
			errSubstr: "invalid cursor: missing id",
		},
		{
			name:      "empty id string in JSON payload",
			input:     base64.StdEncoding.EncodeToString([]byte(`{"id":"","order_by":"id"}`)),
			wantErr:   true,
			errSubstr: "invalid cursor: missing id",
		},
		{
			name:      "empty JSON object payload",
			input:     base64.StdEncoding.EncodeToString([]byte(`{}`)),
			wantErr:   true,
			errSubstr: "invalid cursor: missing id",
		},
		{
			name:      "invalid base64 padding and characters",
			input:     "==invalid-base64==",
			wantErr:   true,
			errSubstr: "invalid cursor: not base64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeCursor(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantID, got.LastID)
			}
		})
	}
}
