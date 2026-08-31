package decode

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTokenEvent_Transfer(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "transfer"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
		{"address": "GA7QNFHD3FJ3G7P46RRXNM6J6YBGQKJ3QLCZMVFCB5QRJEMJXVLVL3J5"}
	]`)
	value := json.RawMessage(`{"i128": "1000000000"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-1", 100, "testnet", topics, value)
	require.NotNil(t, te)
	assert.Equal(t, TokenTransfer, te.Kind)
	assert.Equal(t, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", te.From)
	assert.Equal(t, "GA7QNFHD3FJ3G7P46RRXNM6J6YBGQKJ3QLCZMVFCB5QRJEMJXVLVL3J5", te.To)
	assert.Equal(t, int64(100), te.Ledger)
	assert.Equal(t, "testnet", te.Network)
	assert.Equal(t, "1000000000", te.Amount.String())
	assert.Equal(t, "ev-1", te.EventID)
}

func TestParseTokenEvent_Mint(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "mint"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "5000000000"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-2", 101, "testnet", topics, value)
	require.NotNil(t, te)
	assert.Equal(t, TokenMint, te.Kind)
	assert.Equal(t, "", te.From)
	assert.Equal(t, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", te.To)
	assert.Equal(t, "5000000000", te.Amount.String())
}

func TestParseTokenEvent_Burn(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "burn"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "2500000000"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-3", 102, "mainnet", topics, value)
	require.NotNil(t, te)
	assert.Equal(t, TokenBurn, te.Kind)
	assert.Equal(t, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", te.From)
	assert.Equal(t, "", te.To)
	assert.Equal(t, "2500000000", te.Amount.String())
}

func TestParseTokenEvent_Clawback(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "clawback"},
		{"address": "GA7QNFHD3FJ3G7P46RRXNM6J6YBGQKJ3QLCZMVFCB5QRJEMJXVLVL3J5"}
	]`)
	value := json.RawMessage(`{"i128": "1000000"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-4", 103, "testnet", topics, value)
	require.NotNil(t, te)
	assert.Equal(t, TokenClawback, te.Kind)
	assert.Equal(t, "GA7QNFHD3FJ3G7P46RRXNM6J6YBGQKJ3QLCZMVFCB5QRJEMJXVLVL3J5", te.From)
	assert.Equal(t, "", te.To)
}

func TestParseTokenEvent_SelfTransferReturnsNil(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "transfer"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "1000000"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-5", 104, "testnet", topics, value)
	assert.Nil(t, te, "self-transfer should be nil (no net balance change)")
}

func TestParseTokenEvent_NonTokenEventReturnsNil(t *testing.T) {
	// A contract event with an unrecognised event name.
	topics := json.RawMessage(`[{"symbol": "some_other_event"}, {"address": "C..."}]`)
	value := json.RawMessage(`{"u64": 42}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-6", 105, "testnet", topics, value)
	assert.Nil(t, te, "non-token events should be silently skipped")
}

func TestParseTokenEvent_InvalidTopicsReturnsNil(t *testing.T) {
	// Empty topics array
	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-7", 106, "testnet", json.RawMessage(`[]`), json.RawMessage(`{"i128": "100"}`))
	assert.Nil(t, te)

	// Non-array topics
	te = ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-8", 107, "testnet", json.RawMessage(`"not-an-array"`), json.RawMessage(`{"i128": "100"}`))
	assert.Nil(t, te)
}

func TestParseTokenEvent_TransferMissingTopicsReturnsNil(t *testing.T) {
	// Transfer requires 3 topics, only provide 2
	topics := json.RawMessage(`[
		{"symbol": "transfer"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "100"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-9", 108, "testnet", topics, value)
	assert.Nil(t, te)
}

func TestParseTokenEvent_NegativeAmountReturnsNil(t *testing.T) {
	// i128 values should never be negative for valid token events.
	topics := json.RawMessage(`[
		{"symbol": "mint"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "-500"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-10", 109, "testnet", topics, value)
	assert.Nil(t, te)
}

func TestParseTokenEvent_LargeI128(t *testing.T) {
	// i128 max is ~2^127-1 ≈ 1.7e38; test a realistically large value.
	topics := json.RawMessage(`[
		{"symbol": "mint"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "170141183460469231731687303715884105727"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-11", 110, "testnet", topics, value)
	require.NotNil(t, te)
	expected := new(big.Int)
	expected.SetString("170141183460469231731687303715884105727", 10)
	assert.Equal(t, expected, te.Amount)
}

func TestParseTokenEvent_ZeroAmount(t *testing.T) {
	topics := json.RawMessage(`[
		{"symbol": "mint"},
		{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
	]`)
	value := json.RawMessage(`{"i128": "0"}`)

	te := ParseTokenEvent("CA3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJ3QJA", "ev-12", 111, "testnet", topics, value)
	require.NotNil(t, te)
	assert.Equal(t, "0", te.Amount.String())
}
