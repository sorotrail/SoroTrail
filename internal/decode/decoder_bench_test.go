package decode

import (
	"encoding/json"
	"testing"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Helper to encode an ScVal to base64 for testing.
func mustMarshalScValBase64(tb testing.TB, val xdr.ScVal) string {
	tb.Helper()
	out, err := xdr.MarshalBase64(val)
	if err != nil {
		tb.Fatalf("failed to marshal ScVal to base64: %v", err)
	}
	return out
}

func BenchmarkXDRDecode_Symbol(b *testing.B) {
	sym := xdr.ScSymbol("transfer")
	val := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &sym,
	}
	b64 := mustMarshalScValBase64(b, val)
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decoder.DecodeScVal(b64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXDRDecode_Address(b *testing.B) {
	accountID := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	addr := xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &accountID,
		},
	}
	b64 := mustMarshalScValBase64(b, addr)
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decoder.DecodeScVal(b64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXDRDecode_U128(b *testing.B) {
	val := xdr.ScVal{
		Type: xdr.ScValTypeScvU128,
		U128: &xdr.UInt128Parts{
			Hi: 123456789,
			Lo: 987654321,
		},
	}
	b64 := mustMarshalScValBase64(b, val)
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decoder.DecodeScVal(b64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXDRDecode_Vec(b *testing.B) {
	sym1 := xdr.ScSymbol("transfer")
	sym2 := xdr.ScSymbol("mint")
	uVal := xdr.Uint64(1000000)
	vec := xdr.ScVec{
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym1},
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym2},
		xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &uVal},
	}
	vecPtr := &vec
	val := xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec:  &vecPtr,
	}
	b64 := mustMarshalScValBase64(b, val)
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decoder.DecodeScVal(b64)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEventTopicsValue_XDR(b *testing.B) {
	sym := xdr.ScSymbol("transfer")
	valSym := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	b64Topic := mustMarshalScValBase64(b, valSym)

	u64Val := xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: (*xdr.Uint64)(ptrUint64(50000))}
	b64Val := mustMarshalScValBase64(b, u64Val)

	event := rpc.Event{
		Topic: []string{b64Topic, b64Topic},
		Value: b64Val,
	}
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := EventTopicsValue(decoder, event)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEventTopicsValue_JSONPassthrough(b *testing.B) {
	event := rpc.Event{
		TopicJSON: []json.RawMessage{
			json.RawMessage(`{"symbol":"transfer"}`),
			json.RawMessage(`{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}`),
		},
		ValueJSON: json.RawMessage(`{"u128":"1000000000"}`),
	}
	decoder := XDRDecoder{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := EventTopicsValue(decoder, event)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func ptrUint64(v uint64) *uint64 { return &v }
