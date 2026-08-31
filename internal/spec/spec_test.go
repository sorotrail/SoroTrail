package spec

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScSpecEntriesRawAndParseSpecEntries(t *testing.T) {
	// We construct valid/invalid raw xdr/json byte streams or use package structures
	// to exercise parseScSpecEntriesRaw and parseSpecEntries.
	// Since ScSpecEntry is defined via stellar/go or internal packages, let's test via JSON/XDR or mock helpers where applicable.
	
	t.Run("empty section yields empty spec", func(t *testing.T) {
		// An empty or zero-length raw slice should return an empty spec/entries without error.
		spec, err := parseSpecEntries([]byte{})
		require.NoError(t, err)
		assert.Empty(t, spec.Events)
	})

	t.Run("truncated stream is rejected without panicking", func(t *testing.T) {
		// Deliberately truncated byte sequence
		corruptBytes := []byte{0x01, 0x00, 0x00, 0x00, 0xff}
		_, err := parseScSpecEntriesRaw(corruptBytes)
		assert.Error(t, err)
	})

	t.Run("deliberately corrupt fixture is handled robustly", func(t *testing.T) {
		corruptFixture := bytes.Repeat([]byte{0xFF}, 64)
		_, err := parseScSpecEntriesRaw(corruptFixture)
		assert.Error(t, err)
	})
}

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
