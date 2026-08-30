package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScalarValue(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    any
		wantErr bool
	}{
		{"symbol", `{"symbol":"transfer"}`, "transfer", false},
		{"address", `{"address":"G..."}`, "G...", false},
		{"i128", `{"i128":"1000"}`, "1000", false},
		{"u64", `{"u64":42}`, 42.0, false},
		{"bool", `{"bool":true}`, true, false},
		{"unknown type", `{"unknown":{"type":"foo","base64":"bar"}}`, map[string]any{"type": "foo", "base64": "bar"}, false},
		{"empty", `{}`, nil, true},
		{"invalid json", `invalid`, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScalarValue(json.RawMessage(tt.raw))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseTypeFromTag(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"symbol", `{"symbol":"transfer"}`, "symbol"},
		{"i128", `{"i128":"1000"}`, "i128"},
		{"empty", `{}`, ""},
		{"invalid", `invalid`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseTypeFromTag(json.RawMessage(tt.raw))
			assert.Equal(t, tt.want, got)
		})
	}
}
