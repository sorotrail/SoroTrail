package decode

import (
	"encoding/json"
	"math/big"
	"strings"
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

func TestScvSymbol(t *testing.T) {
	// SCSymbol is string<32> in the Stellar XDR definition, so 32 is the
	// longest symbol the network can produce.
	maxSymbol := strings.Repeat("a", 32)

	tests := []struct {
		name   string
		input  json.RawMessage
		want   string
		wantOK bool
	}{
		{
			name:   "well-formed symbol yields its string",
			input:  json.RawMessage(`{"symbol":"transfer"}`),
			want:   "transfer",
			wantOK: true,
		},
		{
			name:   "non-symbol address is rejected rather than returning empty",
			input:  json.RawMessage(`{"address":"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "non-symbol integer is rejected rather than returning empty",
			input:  json.RawMessage(`{"u64":42}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "non-string symbol value is rejected",
			input:  json.RawMessage(`{"symbol":42}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "missing symbol key is rejected",
			input:  json.RawMessage(`{}`),
			want:   "",
			wantOK: false,
		},
		{
			name:  "empty symbol reports ok with an empty string",
			input: json.RawMessage(`{"symbol":""}`),
			// An empty string is still a well-formed symbol shape, so the
			// extractor reports success; the token allowlist downstream is
			// what rejects it as an unknown event name.
			want:   "",
			wantOK: true,
		},
		{
			name:   "symbol at maximum permitted length is returned intact",
			input:  json.RawMessage(`{"symbol":"` + maxSymbol + `"}`),
			want:   maxSymbol,
			wantOK: true,
		},
		{
			name:   "nil value does not panic and is rejected",
			input:  nil,
			want:   "",
			wantOK: false,
		},
		{
			name:   "invalid JSON is rejected",
			input:  json.RawMessage(`not json`),
			want:   "",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			var ok bool
			assert.NotPanics(t, func() {
				got, ok = scvSymbol(tt.input)
			})
			assert.Equal(t, tt.wantOK, ok, "ok flag")
			assert.Equal(t, tt.want, got, "extracted symbol")
		})
	}
}

func TestScvAmount(t *testing.T) {
	mustBig := func(s string) *big.Int {
		n, ok := new(big.Int).SetString(s, 10)
		require.True(t, ok, "test constant %q must parse as an integer", s)
		return n
	}

	// 2^53+1 is the smallest integer a float64 cannot represent exactly;
	// any decode path that fell back to float would silently lose it.
	aboveFloatPrec := "9007199254740993"
	// u64 max proves values all the way up to the u64 ceiling survive
	// the i128 string path intact.
	u64Max := "18446744073709551615"
	// i128 max is the largest amount a SEP-41 event can carry.
	i128Max := "170141183460469231731687303715884105727"

	tests := []struct {
		name   string
		input  json.RawMessage
		want   *big.Int
		wantOK bool
	}{
		{
			name:   "i128 string is extracted exactly",
			input:  json.RawMessage(`{"i128": "123456789"}`),
			want:   big.NewInt(123456789),
			wantOK: true,
		},
		{
			name:   "amount in the u64 range is extracted exactly",
			input:  json.RawMessage(`{"i128": "` + u64Max + `"}`),
			want:   mustBig(u64Max),
			wantOK: true,
		},
		{
			name:   "amount above 2^53 keeps full precision",
			input:  json.RawMessage(`{"i128": "` + aboveFloatPrec + `"}`),
			want:   mustBig(aboveFloatPrec),
			wantOK: true,
		},
		{
			name:   "i128 max is extracted untruncated",
			input:  json.RawMessage(`{"i128": "` + i128Max + `"}`),
			want:   mustBig(i128Max),
			wantOK: true,
		},
		{
			// Some RPC renderings deliver the amount as a bare JSON
			// number rather than a string; the fallback must still land
			// on the exact value rather than a float64.
			name:   "i128 delivered as a JSON number is still extracted",
			input:  json.RawMessage(`{"i128": 1000000}`),
			want:   big.NewInt(1000000),
			wantOK: true,
		},
		{
			// Extraction stays faithful to the payload; rejecting
			// negatives is the caller's job (ParseTokenEvent drops them,
			// covered by TestParseTokenEvent_NegativeAmountReturnsNil).
			name:   "negative amount keeps its sign",
			input:  json.RawMessage(`{"i128": "-500"}`),
			want:   big.NewInt(-500),
			wantOK: true,
		},
		{
			// scvAmount is documented to read the i128 key only, so a
			// u64-shaped value must surface as failure: a fabricated zero
			// here would make the auditor certify a wrong balance.
			name:   "u64-shaped value reports failure instead of a zero",
			input:  json.RawMessage(`{"u64": 42}`),
			want:   nil,
			wantOK: false,
		},
		{
			name:   "non-numeric value reports failure instead of zero",
			input:  json.RawMessage(`{"address": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}`),
			want:   nil,
			wantOK: false,
		},
		{
			name:   "non-numeric i128 payload is rejected",
			input:  json.RawMessage(`{"i128": "not-a-number"}`),
			want:   nil,
			wantOK: false,
		},
		{
			name:   "missing i128 key is rejected",
			input:  json.RawMessage(`{}`),
			want:   nil,
			wantOK: false,
		},
		{
			name:   "nil value does not panic and is rejected",
			input:  nil,
			want:   nil,
			wantOK: false,
		},
		{
			name:   "invalid JSON is rejected",
			input:  json.RawMessage(`not json`),
			want:   nil,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *big.Int
			var ok bool
			assert.NotPanics(t, func() {
				got, ok = scvAmount(tt.input)
			})
			assert.Equal(t, tt.wantOK, ok, "ok flag")
			if tt.wantOK {
				require.NotNil(t, got, "extracted amount")
				assert.Zero(t, tt.want.Cmp(got), "extracted amount must equal %s, got %s", tt.want, got)
			} else {
				assert.Nil(t, got, "a failed extraction must be nil, never a zero amount")
			}
		})
	}
}

func TestScvAddress(t *testing.T) {
	// Real, checksummed strkeys: GA... is an account, CD... a contract.
	const (
		accountStrkey  = "GA7QNFHD3FJ3G7P46RRXNM6J6YBGQKJ3QLCZMVFCB5QRJEMJXVLVL3J5"
		contractStrkey = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	)

	tests := []struct {
		name   string
		input  json.RawMessage
		want   string
		wantOK bool
	}{
		{
			// Address filtering and the address index both key off this
			// string, so a mangled prefix would silently split one
			// account's history in two; the G prefix must survive intact.
			name:   "account address yields a G-prefixed strkey",
			input:  json.RawMessage(`{"address":"` + accountStrkey + `"}`),
			want:   accountStrkey,
			wantOK: true,
		},
		{
			name:   "contract address yields a C-prefixed strkey",
			input:  json.RawMessage(`{"address":"` + contractStrkey + `"}`),
			want:   contractStrkey,
			wantOK: true,
		},
		{
			name:   "non-address symbol is rejected rather than returning a partial string",
			input:  json.RawMessage(`{"symbol":"transfer"}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "non-address integer is rejected rather than returning a partial string",
			input:  json.RawMessage(`{"u64":42}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "non-string address value is rejected",
			input:  json.RawMessage(`{"address":42}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "missing address key is rejected",
			input:  json.RawMessage(`{}`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "nil value does not panic and is rejected",
			input:  nil,
			want:   "",
			wantOK: false,
		},
		{
			name:   "zero-length value does not panic and is rejected",
			input:  json.RawMessage(``),
			want:   "",
			wantOK: false,
		},
		{
			name:   "invalid JSON is rejected",
			input:  json.RawMessage(`not json`),
			want:   "",
			wantOK: false,
		},
		{
			// A JSON null decodes into a nil map without error, so the
			// missing-key branch is what must reject it.
			name:   "JSON null value does not panic and is rejected",
			input:  json.RawMessage(`null`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "ScVal vector is rejected rather than returning its first element",
			input:  json.RawMessage(`[{"address":"` + accountStrkey + `"}]`),
			want:   "",
			wantOK: false,
		},
		{
			name:   "address nested in an object is rejected",
			input:  json.RawMessage(`{"address":{"account":"` + accountStrkey + `"}}`),
			want:   "",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			var ok bool
			assert.NotPanics(t, func() {
				got, ok = scvAddress(tt.input)
			})
			assert.Equal(t, tt.wantOK, ok, "ok flag")
			assert.Equal(t, tt.want, got, "extracted address")
			if tt.wantOK {
				// A strkey is a version byte, a 32-byte payload and a
				// 2-byte checksum base32-encoded, so every address the
				// decoder hands on is exactly 56 characters.
				assert.Len(t, got, 56, "strkey length")
			}
		})
	}
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
