package spec

import (
	"bytes"
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
		spec, err := parseSpecEntries([]byte{}, "dummy-hash", "dummy-id")
		require.NoError(t, err)
		assert.Equal(t, "dummy-hash", spec.WasmHash)
		assert.Equal(t, "dummy-id", spec.ContractID)
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

func TestScalarValueAndParseTypeFromTag(t *testing.T) {
	t.Run("scalar value extraction", func(t *testing.T) {
		val, err := ScalarValue([]byte(`{"symbol":"transfer"}`))">
		require.NoError(t, err)
		assert.Equal(t, "transfer", val)
	})

	t.Run("parse type from tag", func(t *testing.T) {
		typ := ParseTypeFromTag([]byte(`{"i128":"1000"}`))">
		assert.Equal(t, "i128", typ)
	})
}
