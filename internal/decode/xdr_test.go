package decode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustBase64(t *testing.T, val xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(val)
	require.NoError(t, err)
	return s
}

func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scU64(n uint64) xdr.ScVal {
	u := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func TestXDRDecoder_DecodeScVal(t *testing.T) {
	boolVal := true
	u32 := xdr.Uint32(7)
	i32 := xdr.Int32(-7)
	i64 := xdr.Int64(-42)
	u128 := xdr.UInt128Parts{Hi: xdr.Uint64(1), Lo: xdr.Uint64(2)}
	// -1 expressed as two's complement across hi/lo.
	i128Neg := xdr.Int128Parts{Hi: xdr.Int64(-1), Lo: xdr.Uint64(0xFFFFFFFFFFFFFFFF)}
	falseVal := false
	bytesVal := xdr.ScBytes([]byte{0xde, 0xad, 0xbe, 0xef})
	strVal := xdr.ScString("hello")
	accountID := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")

	vec := xdr.ScVec{scSymbol("transfer"), scU64(9)}
	vecPtr := &vec
	scMap := xdr.ScMap{{Key: scSymbol("amount"), Val: scU64(100)}}
	mapPtr := &scMap

	tests := []struct {
		name string
		val  xdr.ScVal
		want string
	}{
		{"bool true", xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &boolVal}, `{"bool":true}`},
		{"bool false", xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &falseVal}, `{"bool":false}`},
		{"void", xdr.ScVal{Type: xdr.ScValTypeScvVoid}, `{"void":null}`},
		{"u32", xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u32}, `{"u32":7}`},
		{"i32", xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: &i32}, `{"i32":-7}`},
		{"u64", scU64(42), `{"u64":42}`},
		{"i64", xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: &i64}, `{"i64":-42}`},
		{
			"u128",
			xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &u128},
			`{"u128":"18446744073709551618"}`, // 1<<64 + 2
		},
		{
			"i128 negative",
			xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &i128Neg},
			`{"i128":"-1"}`,
		},
		{
			"bytes as hex",
			xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bytesVal},
			`{"bytes":"deadbeef"}`,
		},
		{"string", xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &strVal}, `{"string":"hello"}`},
		{"symbol", scSymbol("transfer"), `{"symbol":"transfer"}`},
		{
			"account address",
			xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &accountID,
			}},
			`{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"}`,
		},
		{
			"vec",
			xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr},
			`{"vec":[{"symbol":"transfer"},{"u64":9}]}`,
		},
		{
			"map",
			xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr},
			`{"map":[{"key":{"symbol":"amount"},"val":{"u64":100}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := XDRDecoder{}.DecodeScVal(mustBase64(t, tt.val))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

// TestXDRDecoder_ContractAddress covers the ScAddress contract-id variant,
// which is the shape actually emitted by most Soroban token/transfer events
// (as opposed to the ScValTypeScvAddress/account-id case already covered in
// the table above). The exact strkey encoding is the underlying xdr
// library's responsibility, so this asserts the decoded shape and the "C"
// contract-address prefix rather than a hand-computed checksum string.
func TestXDRDecoder_ContractAddress(t *testing.T) {
	contractHash := xdr.Hash{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}
	contractID := xdr.ContractId(contractHash)
	val := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}}

	got, err := XDRDecoder{}.DecodeScVal(mustBase64(t, val))
	require.NoError(t, err)

	var decoded map[string]string
	require.NoError(t, json.Unmarshal(got, &decoded))
	addr, ok := decoded["address"]
	require.True(t, ok, "contract ScAddress must decode to an {\"address\": ...} shape, got %s", got)
	assert.True(t, strings.HasPrefix(addr, "C"), "contract addresses use the C... strkey prefix, got %q", addr)
}

func TestXDRDecoder_UnknownTypeFallback(t *testing.T) {
	val := xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}
	raw := mustBase64(t, val)

	got, err := XDRDecoder{}.DecodeScVal(raw)
	require.NoError(t, err)

	var decoded map[string]map[string]string
	require.NoError(t, json.Unmarshal(got, &decoded))
	unknown := decoded["unknown"]
	require.NotNil(t, unknown, "unhandled types must decode to an {\"unknown\": ...} wrapper, got %s", got)
	assert.Equal(t, raw, unknown["base64"], "raw XDR must round-trip losslessly")
	assert.NotEmpty(t, unknown["type"])
}

func TestXDRDecoder_NestedCollections(t *testing.T) {
	tests := []struct {
		name string
		val  xdr.ScVal
		want string
	}{
		{
			"vec containing vec",
			func() xdr.ScVal {
				inner := xdr.ScVec{scSymbol("inner")}
				innerPtr := &inner
				outer := xdr.ScVec{scSymbol("outer"), {Type: xdr.ScValTypeScvVec, Vec: &innerPtr}}
				outerPtr := &outer
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &outerPtr}
			}(),
			`{"vec":[{"symbol":"outer"},{"vec":[{"symbol":"inner"}]}]}`,
		},
		{
			"vec containing map",
			func() xdr.ScVal {
				scMap := xdr.ScMap{{Key: scSymbol("key"), Val: scU64(42)}}
				mapPtr := &scMap
				vec := xdr.ScVec{scU64(1), {Type: xdr.ScValTypeScvMap, Map: &mapPtr}}
				vecPtr := &vec
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
			}(),
			`{"vec":[{"u64":1},{"map":[{"key":{"symbol":"key"},"val":{"u64":42}}]}]}`,
		},
		{
			"map containing vec",
			func() xdr.ScVal {
				inner := xdr.ScVec{scU64(1), scU64(2)}
				innerPtr := &inner
				entry := xdr.ScMapEntry{
					Key: scSymbol("items"),
					Val: xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &innerPtr},
				}
				scMap := xdr.ScMap{entry}
				mapPtr := &scMap
				return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}
			}(),
			`{"map":[{"key":{"symbol":"items"},"val":{"vec":[{"u64":1},{"u64":2}]}}]}`,
		},
		{
			"deep nesting: vec > map > vec",
			func() xdr.ScVal {
				deep := xdr.ScVec{scU64(99)}
				deepPtr := &deep
				entry := xdr.ScMapEntry{
					Key: scSymbol("data"),
					Val: xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &deepPtr},
				}
				scMap := xdr.ScMap{entry}
				mapPtr := &scMap
				outer := xdr.ScVec{{Type: xdr.ScValTypeScvMap, Map: &mapPtr}}
				outerPtr := &outer
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &outerPtr}
			}(),
			`{"vec":[{"map":[{"key":{"symbol":"data"},"val":{"vec":[{"u64":99}]}}]}]}`,
		},
		{
			"empty nested vec",
			func() xdr.ScVal {
				inner := xdr.ScVec{}
				innerPtr := &inner
				outer := xdr.ScVec{{Type: xdr.ScValTypeScvVec, Vec: &innerPtr}}
				outerPtr := &outer
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &outerPtr}
			}(),
			`{"vec":[{"vec":[]}]}`,
		},
		{
			"empty nested map",
			func() xdr.ScVal {
				scMap := xdr.ScMap{}
				mapPtr := &scMap
				return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}
			}(),
			`{"map":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := XDRDecoder{}.DecodeScVal(mustBase64(t, tt.val))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestXDRDecoder_InvalidBase64(t *testing.T) {
	_, err := XDRDecoder{}.DecodeScVal("not base64!!!")
	assert.Error(t, err)
}
