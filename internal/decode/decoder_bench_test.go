package decode

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/sorotrail/sorotrail/internal/rpc"
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

// BenchmarkFullPageJSONSerialization benchmarks JSON marshaling of a full page of events
// to ensure serialization performance and allocation counts remain optimal.
func BenchmarkFullPageJSONSerialization(b *testing.B) {
	type Page struct {
		Events     []map[string]any `json:"events"`
		NextCursor string           `json:"next_cursor"`
		Limit      int              `json:"limit"`
	}

	items := make([]map[string]any, 50)
	for i := 0; i < 50; i++ {
		items[i] = map[string]any{
			"id":                 fmt.Sprintf("%019d-%010d", 1000000+i, 0),
			"contract_id":        "C0000000000000000000000000000000000000000000000000000001",
			"ledger":             1000000,
			"type":               "contract",
			"tx_hash":            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"tx_index":           0,
			"op_index":           0,
			"in_successful_call": true,
			"topics":             []string{"transfer"},
			"value":              map[string]any{"u64": 100000},
			"created_at":         "2026-01-01T00:00:00Z",
		}
	}

	page := Page{
		Events:     items,
		NextCursor: "0000001000000-0000000000",
		Limit:      50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(page)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExtractAddresses_Transfer(b *testing.B) {
	topics := json.RawMessage(`[{"symbol":"transfer"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`)
	value := json.RawMessage(`{"i128":"1000000000"}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		refs := ExtractAddresses(topics, value)
		if len(refs) != 2 {
			b.Fatalf("expected 2 address refs, got %d", len(refs))
		}
	}
}
