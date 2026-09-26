package decode

// Exhaustive ScVal-type coverage for the decoder (#558).
//
// scValToGo has one arm per ScValType, so a type with no test is a type whose
// wire shape nobody owns — the JSON it produces could change or silently fall
// through to the lossless {"unknown": ...} wrapper without anything failing.
// The table below therefore keys on the XDR type itself rather than on the
// scenario, and TestScValTypeCoverage asserts the table reaches every value in
// the SDK's enum. A new ScValType in an SDK bump fails the guard until someone
// adds the row that pins what the decoder is supposed to do with it.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// highestKnownScValType is the last enumerator xdr.ScValType defines today.
// TestScValTypeEnumRange pins it, so the constant can never quietly fall behind
// the SDK and make the coverage guard below vacuously true.
const highestKnownScValType = int(xdr.ScValTypeScvExecutableTag)

func scBool(b bool) xdr.ScVal {
	v := b
	return xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &v}
}

func scU32(n uint32) xdr.ScVal {
	v := xdr.Uint32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &v}
}
func scI32(n int32) xdr.ScVal {
	v := xdr.Int32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: &v}
}
func scI64(n int64) xdr.ScVal {
	v := xdr.Int64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: &v}
}
func scString(s string) xdr.ScVal {
	v := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &v}
}

func scBytes(b []byte) xdr.ScVal {
	v := xdr.ScBytes(b)
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &v}
}

func scTimepoint(n uint64) xdr.ScVal {
	v := xdr.TimePoint(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvTimepoint, Timepoint: &v}
}

func scDuration(n uint64) xdr.ScVal {
	v := xdr.Duration(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvDuration, Duration: &v}
}

func scU128(hi, lo uint64) xdr.ScVal {
	v := xdr.UInt128Parts{Hi: xdr.Uint64(hi), Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &v}
}

func scI128(hi int64, lo uint64) xdr.ScVal {
	v := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &v}
}

func scU256(hihi, hilo, lohi, lolo uint64) xdr.ScVal {
	v := xdr.UInt256Parts{HiHi: xdr.Uint64(hihi), HiLo: xdr.Uint64(hilo), LoHi: xdr.Uint64(lohi), LoLo: xdr.Uint64(lolo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvU256, U256: &v}
}

func scI256(hihi int64, hilo, lohi, lolo uint64) xdr.ScVal {
	v := xdr.Int256Parts{HiHi: xdr.Int64(hihi), HiLo: xdr.Uint64(hilo), LoHi: xdr.Uint64(lohi), LoLo: xdr.Uint64(lolo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvI256, I256: &v}
}

// scContractError is the SCE_CONTRACT arm: the payload is a contract's own
// u32 error code, so ScError.Code stays nil.
func scContractError(code uint32) xdr.ScVal {
	c := xdr.Uint32(code)
	err := xdr.ScError{Type: xdr.ScErrorTypeSceContract, ContractCode: &c}
	return xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &err}
}

// scSystemError is every other ScErrorType arm: the payload is an
// ScErrorCode enum rendered by its own name, and ContractCode stays nil.
func scSystemError(typ xdr.ScErrorType, code xdr.ScErrorCode) xdr.ScVal {
	c := code
	err := xdr.ScError{Type: typ, Code: &c}
	return xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &err}
}

func scVec(items ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(items)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

func scMapOf(entries ...xdr.ScMapEntry) xdr.ScVal {
	m := xdr.ScMap(entries)
	mapPtr := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}
}

func scAccountAddress(strkey string) xdr.ScVal {
	account := xdr.MustAddress(strkey)
	addr := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &account,
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func scNonce(n int64) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyNonce, NonceKey: &xdr.ScNonceKey{Nonce: xdr.Int64(n)}}
}

func scExecutableTag(tag string) xdr.ScVal {
	v := xdr.ScString(tag)
	return xdr.ScVal{Type: xdr.ScValTypeScvExecutableTag, ExecutableTag: &v}
}

// TestScValTypeEnumRange pins the enum range the coverage guard iterates.
// Without it, an SDK that grows ScValType would leave the guard comparing
// against a stale high-water mark and reporting full coverage.
func TestScValTypeEnumRange(t *testing.T) {
	assert.Equal(t, "ScValTypeScvExecutableTag", xdr.ScValType(highestKnownScValType).String())
	assert.Emptyf(t, xdr.ScValType(highestKnownScValType+1).String(),
		"the SDK defines a new ScValType above %d; scValToGo needs an arm for it and the "+
			"coverage table needs a row, then raise highestKnownScValType", highestKnownScValType)
}

// TestScValTypeCoverage decodes one sample of every ScValType the SDK defines
// and asserts the exact JSON shape. The guard at the end of the run is the
// point of the test: a type missing from the table fails even though the
// decode itself would have passed through the lossless fallback.
func TestScValTypeCoverage(t *testing.T) {
	// SCV_LEDGER_KEY_CONTRACT_INSTANCE is a payload-less void marker. The
	// decoder deliberately leaves it to the fallback rather than emitting an
	// empty object, so its expected shape carries the re-encoded raw XDR.
	contractInstanceKey := xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}
	contractInstanceKeyRaw := mustBase64(t, contractInstanceKey)

	tests := []struct {
		name string
		typ  xdr.ScValType
		val  xdr.ScVal
		want string
	}{
		{"bool", xdr.ScValTypeScvBool, scBool(true), `{"bool":true}`},
		{"void", xdr.ScValTypeScvVoid, xdr.ScVal{Type: xdr.ScValTypeScvVoid}, `{"void":null}`},
		{
			"error: contract_code arm", xdr.ScValTypeScvError, scContractError(1),
			`{"error":{"type":"ScErrorTypeSceContract","contract_code":1}}`,
		},
		{
			// The two error arms are distinguished by which pointer is set, so
			// the absent key must stay absent rather than render as null or 0.
			"error: system code arm", xdr.ScValTypeScvError,
			scSystemError(xdr.ScErrorTypeSceValue, xdr.ScErrorCodeScecInvalidInput),
			`{"error":{"type":"ScErrorTypeSceValue","code":"ScErrorCodeScecInvalidInput"}}`,
		},
		{"u32", xdr.ScValTypeScvU32, scU32(7), `{"u32":7}`},
		{"u32 max", xdr.ScValTypeScvU32, scU32(4294967295), `{"u32":4294967295}`},
		{"i32", xdr.ScValTypeScvI32, scI32(-7), `{"i32":-7}`},
		{"u64", xdr.ScValTypeScvU64, scU64(42), `{"u64":42}`},
		{
			// u64 is small enough to stay a JSON number; only the 128/256-bit
			// types are rendered as strings.
			"u64 max stays a number", xdr.ScValTypeScvU64, scU64(18446744073709551615),
			`{"u64":18446744073709551615}`,
		},
		{"i64", xdr.ScValTypeScvI64, scI64(-42), `{"i64":-42}`},
		{
			"i64 min", xdr.ScValTypeScvI64, scI64(-9223372036854775808),
			`{"i64":-9223372036854775808}`,
		},
		{"timepoint", xdr.ScValTypeScvTimepoint, scTimepoint(1700000000), `{"timepoint":1700000000}`},
		{
			"timepoint max", xdr.ScValTypeScvTimepoint, scTimepoint(18446744073709551615),
			`{"timepoint":18446744073709551615}`,
		},
		{"duration", xdr.ScValTypeScvDuration, scDuration(3600), `{"duration":3600}`},
		{"duration zero", xdr.ScValTypeScvDuration, scDuration(0), `{"duration":0}`},
		{"u128", xdr.ScValTypeScvU128, scU128(1, 2), `{"u128":"18446744073709551618"}`},
		{
			"u128 max", xdr.ScValTypeScvU128, scU128(18446744073709551615, 18446744073709551615),
			`{"u128":"340282366920938463463374607431768211455"}`,
		},
		{"i128 zero", xdr.ScValTypeScvI128, scI128(0, 0), `{"i128":"0"}`},
		{"i128 positive", xdr.ScValTypeScvI128, scI128(0, 7), `{"i128":"7"}`},
		{
			// hi carries the sign, so a positive value with the high bit of the
			// whole 128-bit word set must still render positive.
			"i128 max", xdr.ScValTypeScvI128, scI128(9223372036854775807, 18446744073709551615),
			`{"i128":"170141183460469231731687303715884105727"}`,
		},
		{
			"i128 min", xdr.ScValTypeScvI128, scI128(-9223372036854775808, 0),
			`{"i128":"-170141183460469231731687303715884105728"}`,
		},
		{
			"u256", xdr.ScValTypeScvU256, scU256(0, 0, 0, 1), `{"u256":"1"}`,
		},
		{
			// Only the lowest word is set: the shift/or chain must not smear the
			// value across the upper 192 bits.
			"u256 low word only", xdr.ScValTypeScvU256, scU256(0, 0, 0, 18446744073709551615),
			`{"u256":"18446744073709551615"}`,
		},
		{
			"u256 max", xdr.ScValTypeScvU256,
			scU256(18446744073709551615, 18446744073709551615, 18446744073709551615, 18446744073709551615),
			`{"u256":"115792089237316195423570985008687907853269984665640564039457584007913129639935"}`,
		},
		{"i256 zero", xdr.ScValTypeScvI256, scI256(0, 0, 0, 0), `{"i256":"0"}`},
		{"i256 positive", xdr.ScValTypeScvI256, scI256(0, 0, 0, 1), `{"i256":"1"}`},
		{
			"i256 max", xdr.ScValTypeScvI256,
			scI256(9223372036854775807, 18446744073709551615, 18446744073709551615, 18446744073709551615),
			`{"i256":"57896044618658097711785492504343953926634992332820282019728792003956564819967"}`,
		},
		{
			"i256 min", xdr.ScValTypeScvI256, scI256(-9223372036854775808, 0, 0, 0),
			`{"i256":"-57896044618658097711785492504343953926634992332820282019728792003956564819968"}`,
		},
		{
			"i256 minus one", xdr.ScValTypeScvI256,
			scI256(-1, 18446744073709551615, 18446744073709551615, 18446744073709551615),
			`{"i256":"-1"}`,
		},
		{"bytes", xdr.ScValTypeScvBytes, scBytes([]byte{0xde, 0xad, 0xbe, 0xef}), `{"bytes":"deadbeef"}`},
		{"bytes empty", xdr.ScValTypeScvBytes, scBytes([]byte{}), `{"bytes":""}`},
		{"string", xdr.ScValTypeScvString, scString("hello"), `{"string":"hello"}`},
		{"symbol", xdr.ScValTypeScvSymbol, scSymbol("transfer"), `{"symbol":"transfer"}`},
		{"vec", xdr.ScValTypeScvVec, scVec(scU64(9)), `{"vec":[{"u64":9}]}`},
		{
			"map", xdr.ScValTypeScvMap,
			scMapOf(xdr.ScMapEntry{Key: scSymbol("amount"), Val: scU64(100)}),
			`{"map":[{"key":{"symbol":"amount"},"val":{"u64":100}}]}`,
		},
		{
			"address", xdr.ScValTypeScvAddress,
			scAccountAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
			`{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"}`,
		},
		{
			"contract instance", xdr.ScValTypeScvContractInstance,
			scContractInstance(wasmExecutable(), nil),
			`{"contract_instance":{"executable":{"wasm_hash":"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"},"storage":[]}}`,
		},
		{
			// The one ScValType that is intentionally *not* handled: it has no
			// payload, so it lands in the fallback wrapper instead.
			"ledger key contract instance falls back", xdr.ScValTypeScvLedgerKeyContractInstance,
			contractInstanceKey,
			fmt.Sprintf(`{"unknown":{"type":"ScValTypeScvLedgerKeyContractInstance","base64":%q}}`, contractInstanceKeyRaw),
		},
		{"ledger key nonce", xdr.ScValTypeScvLedgerKeyNonce, scNonce(42), `{"ledger_key_nonce":{"nonce":42}}`},
		{"executable tag", xdr.ScValTypeScvExecutableTag, scExecutableTag("soroban-tag"), `{"executable_tag":"soroban-tag"}`},
	}

	covered := make(map[xdr.ScValType]bool, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := XDRDecoder{}.DecodeScVal(mustBase64(t, tt.val))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
			covered[tt.typ] = true
		})
	}

	for typ := 0; typ <= highestKnownScValType; typ++ {
		scValType := xdr.ScValType(typ)
		assert.Truef(t, covered[scValType],
			"%s has no row in the coverage table, so nothing pins what it decodes to",
			scValType.String())
	}
}

// TestXDRDecoder_NilCollectionArms covers the two nil-payload arms of the
// Vec and Map cases. They are reachable on the wire (an ScVec/ScMap pointer
// that is itself nil) and must render as empty JSON arrays rather than null,
// which is what every consumer of the projection keys off.
func TestXDRDecoder_NilCollectionArms(t *testing.T) {
	tests := []struct {
		name string
		val  xdr.ScVal
		want string
	}{
		{
			"nil vec",
			func() xdr.ScVal {
				var nilVec *xdr.ScVec
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &nilVec}
			}(),
			`{"vec":[]}`,
		},
		{
			"nil map",
			func() xdr.ScVal {
				var nilMap *xdr.ScMap
				return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &nilMap}
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

// TestScValToGo_MapPointerItselfNil covers the defensive first half of the
// Map arm. It cannot be reached through DecodeScVal: the SDK's decoder always
// allocates the arm pointer, and an ScVal built without one panics when
// encoded, so there is no XDR that produces it. It is exercised directly
// because the guard is real code and the Vec arm beside it has no equivalent.
func TestScValToGo_MapPointerItselfNil(t *testing.T) {
	got, err := scValToGo(xdr.ScVal{Type: xdr.ScValTypeScvMap})
	require.NoError(t, err)
	out, err := json.Marshal(got)
	require.NoError(t, err)
	assert.JSONEq(t, `{"map":[]}`, string(out))
}

// TestXDRDecoder_FallbackShapesAreDistinct pins that the two ways a value can
// end up under {"unknown": ...} stay tellable apart. A value that could not be
// decoded at all reports type "decode_error" plus the message that explains it;
// a value that decoded fine but whose type the decoder does not handle reports
// the real ScValType name and no error. Collapsing them would make an
// unsupported-but-valid event indistinguishable from corrupt XDR.
func TestXDRDecoder_FallbackShapesAreDistinct(t *testing.T) {
	tests := []struct {
		name string
		in   func(t *testing.T) string
		// wantType is the unknown.type key, and wantError whether the
		// metadata carries a decode error message.
		wantType  string
		wantError bool
		// wantCounted is whether the decode must bump decodeErrors.
		wantCounted bool
	}{
		{
			name:        "undecodable XDR reports decode_error",
			in:          func(*testing.T) string { return "not base64!!!" },
			wantType:    "decode_error",
			wantError:   true,
			wantCounted: true,
		},
		{
			name: "unhandled but valid ScVal reports its own type name",
			in: func(t *testing.T) string {
				return mustBase64(t, xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance})
			},
			wantType: "ScValTypeScvLedgerKeyContractInstance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.in(t)
			before := DecodeErrorCount()

			got, err := XDRDecoder{}.DecodeScVal(input)
			require.NoError(t, err, "both fallback paths return a value, never an error")

			var decoded struct {
				Unknown map[string]any `json:"unknown"`
			}
			require.NoError(t, json.Unmarshal(got, &decoded))
			require.NotNil(t, decoded.Unknown, "output must be an {\"unknown\": ...} wrapper, got %s", got)
			assert.Equal(t, tt.wantType, decoded.Unknown["type"])
			assert.Equal(t, input, decoded.Unknown["base64"], "the raw XDR must survive intact")

			_, hasError := decoded.Unknown["error"]
			assert.Equal(t, tt.wantError, hasError,
				"only a genuine decode failure may carry an error message")

			delta := 0
			if tt.wantCounted {
				delta = 1
			}
			assert.Equal(t, before+uint64(delta), DecodeErrorCount(),
				"the decode-error counter must move only when decoding actually failed")
		})
	}
}

// TestContractExecutableUnhandledArm pins the default arm of
// contractExecutableToGo. An executable type outside the SDK's enum cannot be
// re-encoded, so the arm reports an error instead of a silent zero value.
// It is unreachable through DecodeScVal — the SDK rejects an out-of-range
// union discriminant while unmarshaling, before the decoder sees the value —
// which is why this calls the helper directly.
func TestContractExecutableUnhandledArm(t *testing.T) {
	_, err := contractExecutableToGo(xdr.ContractExecutable{Type: xdr.ContractExecutableType(99)})
	require.Error(t, err, "an executable that cannot be re-encoded must not yield a zero value")
	assert.Contains(t, err.Error(), "re-encoding unhandled ContractExecutable type")
}
