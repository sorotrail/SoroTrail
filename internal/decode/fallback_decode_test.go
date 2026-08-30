package decode

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFallbackDecode_Table(t *testing.T) {
	tests := []struct {
		name      string
		base64XDR string
		decodeErr error
		wantShape string // expected JSON shape substring
		wantErr   string // expected error message substring in the output
	}{
		{
			name:      "valid base64 with decode error",
			base64XDR: "AAAAAQ==",
			decodeErr: errors.New("unsupported ScVal type"),
			wantShape: `"unknown"`,
			wantErr:   "unsupported ScVal type",
		},
		{
			name:      "empty base64 string",
			base64XDR: "",
			decodeErr: errors.New("empty input"),
			wantShape: `"unknown"`,
			wantErr:   "empty input",
		},
		{
			name:      "long base64 payload preserved",
			base64XDR: "c29tZS12ZXJ5LWxvbmctdmFsdWUtdGhhdC1maWxscy1pbm8tc2hvcnQtc3RyaW5nLWZpZWxk",
			decodeErr: errors.New("truncated"),
			wantShape: `"unknown"`,
			wantErr:   "truncated",
		},
		{
			name:      "special characters in base64",
			base64XDR: "aGVsbG8+d29ybGQ=",
			decodeErr: errors.New("parse failure"),
			wantShape: `"unknown"`,
			wantErr:   "parse failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fallbackDecode(tt.base64XDR, tt.decodeErr)
			require.True(t, json.Valid(got), "output must be valid JSON: %s", got)
			assert.Contains(t, string(got), tt.wantShape)

			// Verify the structure.
			var parsed map[string]any
			require.NoError(t, json.Unmarshal(got, &parsed))
			unknown, ok := parsed["unknown"].(map[string]any)
			require.True(t, ok, "output must have 'unknown' object")
			assert.Equal(t, "decode_error", unknown["type"])
			assert.Equal(t, tt.base64XDR, unknown["base64"])

			errStr, _ := unknown["error"].(string)
			assert.Contains(t, errStr, tt.wantErr)
		})
	}
}
