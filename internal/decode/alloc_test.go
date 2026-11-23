package decode

import (
	"encoding/json"
	"math"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

func TestAppendScValJSON_ByteForByteMatchesReference(t *testing.T) {
	boolTrue := true
	boolFalse := false
	u32 := xdr.Uint32(42)
	i32 := xdr.Int32(-42)
	u64 := xdr.Uint64(math.MaxUint64)
	i64 := xdr.Int64(math.MinInt64)
	tp := xdr.TimePoint(1700000000)
	dur := xdr.Duration(3600)
	u128Small := xdr.UInt128Parts{Hi: 0, Lo: 9999999}
	u128Wide := xdr.UInt128Parts{Hi: math.MaxUint64, Lo: math.MaxUint64}
	i128SmallPos := xdr.Int128Parts{Hi: 0, Lo: math.MaxInt64}
	i128SmallNeg := xdr.Int128Parts{Hi: -1, Lo: math.MaxUint64} // -1
	i128WideNeg := xdr.Int128Parts{Hi: math.MinInt64, Lo: 0}
	u256Small := xdr.UInt256Parts{HiHi: 0, HiLo: 0, LoHi: 0, LoLo: 123456789}
	u256Wide := xdr.UInt256Parts{HiHi: 1, HiLo: 2, LoHi: 3, LoLo: 4}
	i256SmallNeg := xdr.Int256Parts{HiHi: -1, HiLo: math.MaxUint64, LoHi: math.MaxUint64, LoLo: math.MaxUint64 - 99}
	i256WideNeg := xdr.Int256Parts{HiHi: math.MinInt64, HiLo: 0, LoHi: 0, LoLo: 1}
	rawBytes := xdr.ScBytes([]byte{0x00, 0x0f, 0xa5, 0xff})
	specialStr := xdr.ScString("quote\"slash\\html<>&\b\f\n\r\t\x01\x1f")
	accountID := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	contractID := xdr.ContractId{0xde, 0xad, 0xbe, 0xef}
	contractErrCode := xdr.Uint32(7)
	storageCode := xdr.ScErrorCodeScecExistingValue
	nonceVal := xdr.Int64(987654321)

	vec := xdr.ScVec{scSymbol("transfer"), scU64(100), {Type: xdr.ScValTypeScvString, Str: &specialStr}}
	vecPtr := &vec
	unsortedMap := xdr.ScMap{
		{Key: scSymbol("z_key"), Val: scU64(1)},
		{Key: scSymbol("a_key"), Val: scU64(2)},
		{Key: scSymbol("m<key>"), Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &boolTrue}},
	}
	mapPtr := &unsortedMap

	cases := []struct {
		name string
		val  xdr.ScVal
	}{
		{"bool_true", xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &boolTrue}},
		{"bool_false", xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &boolFalse}},
		{"void", xdr.ScVal{Type: xdr.ScValTypeScvVoid}},
		{"u32", xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u32}},
		{"i32", xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: &i32}},
		{"u64_max", xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u64}},
		{"i64_min", xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: &i64}},
		{"timepoint", xdr.ScVal{Type: xdr.ScValTypeScvTimepoint, Timepoint: &tp}},
		{"duration", xdr.ScVal{Type: xdr.ScValTypeScvDuration, Duration: &dur}},
		{"u128_small", xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &u128Small}},
		{"u128_wide", xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &u128Wide}},
		{"i128_small_pos", xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &i128SmallPos}},
		{"i128_small_neg", xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &i128SmallNeg}},
		{"i128_wide_neg", xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &i128WideNeg}},
		{"u256_small", xdr.ScVal{Type: xdr.ScValTypeScvU256, U256: &u256Small}},
		{"u256_wide", xdr.ScVal{Type: xdr.ScValTypeScvU256, U256: &u256Wide}},
		{"i256_small_neg", xdr.ScVal{Type: xdr.ScValTypeScvI256, I256: &i256SmallNeg}},
		{"i256_wide_neg", xdr.ScVal{Type: xdr.ScValTypeScvI256, I256: &i256WideNeg}},
		{"bytes", xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &rawBytes}},
		{"special_string", xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &specialStr}},
		{"symbol", scSymbol("fn_call")},
		{"vec", xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}},
		{"unsorted_map", xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}},
		{"address_account", xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &accountID,
		}}},
		{"address_contract", xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractID,
		}}},
		{"error_contract", xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &xdr.ScError{
			Type:         xdr.ScErrorTypeSceContract,
			ContractCode: &contractErrCode,
		}}},
		{"error_storage", xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &xdr.ScError{
			Type: xdr.ScErrorTypeSceStorage,
			Code: &storageCode,
		}}},
		{"contract_instance", scContractInstance(wasmExecutable(), mapPtr)},
		{"ledger_key_contract_instance", xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}},
		{"ledger_key_nonce", xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyNonce, NonceKey: &xdr.ScNonceKey{Nonce: nonceVal}}},
	}

	dec := XDRDecoder{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goShape, err := scValToGo(tc.val)
			require.NoError(t, err)
			wantBytes, err := json.Marshal(goShape)
			require.NoError(t, err)

			gotDirect, err := appendScValJSON(nil, tc.val)
			require.NoError(t, err)
			assert.Equal(t, string(wantBytes), string(gotDirect))

			b64 := mustBase64(t, tc.val)
			gotDecoded, err := dec.DecodeScVal(b64)
			require.NoError(t, err)
			assert.Equal(t, string(wantBytes), string(gotDecoded))
		})
	}
}

func TestDecodeScratchPool_ConcurrentAndNoSharedBufferAliasing(t *testing.T) {
	accountA := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	contractB := xdr.ContractId{1, 2, 3, 4, 5, 6, 7, 8}
	topic0 := mustBase64(t, scSymbol("transfer"))
	topic1 := mustBase64(t, xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &accountA,
		},
	})
	topic2 := mustBase64(t, xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractB,
		},
	})
	valXDR := mustBase64(t, xdr.ScVal{
		Type: xdr.ScValTypeScvU128,
		U128: &xdr.UInt128Parts{Hi: 0, Lo: 500000000},
	})

	ev := rpc.Event{
		ID:    "0000400000-0000000001",
		Topic: []string{topic0, topic1, topic2},
		Value: valXDR,
	}

	dec := XDRDecoder{}
	wantTopics, wantValue, err := EventTopicsValue(dec, ev)
	require.NoError(t, err)
	wantTopicsStr := string(wantTopics)
	wantValueStr := string(wantValue)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var refs []AddressRef
			for i := 0; i < 200; i++ {
				topics, value, err := EventTopicsValue(dec, ev)
				require.NoError(t, err)
				assert.Equal(t, wantTopicsStr, string(topics))
				assert.Equal(t, wantValueStr, string(value))

				refs = AppendAddresses(refs[:0], ev.ID, topics, value)
				require.Len(t, refs, 2)
				assert.Equal(t, "topic[1]", refs[0].Role)
				assert.Equal(t, "topic[2]", refs[1].Role)

				// Mutate returned slices in-place to prove they never alias
				// pooled decodeScratch buffers.
				for j := range topics {
					topics[j] = 'X'
				}
				for j := range value {
					value[j] = 'Y'
				}
			}
		}()
	}
	wg.Wait()
}
