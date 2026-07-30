package decode

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"sync/atomic"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// decodeErrors counts ScVal decode failures. Incremented by DecodeScVal
// when unmarshaling or conversion fails. Reset only on process restart.
// Exposed via DecodeErrorCount for monitoring / stats aggregation.
var decodeErrors atomic.Uint64

// DecodeErrorCount returns the number of ScVal XDR decode failures since
// process start. A non-zero value indicates events whose raw XDR could not
// be decoded — they are stored in a lossless fallback form so ingestion
// never stalls.
func DecodeErrorCount() uint64 { return decodeErrors.Load() }

// XDRDecoder decodes base64 XDR ScVals into a typed-object JSON shape,
// e.g. {"symbol": "transfer"}, {"u64": 123}, {"address": "C..."}.
//
// The shape aims to be close to (but is not guaranteed identical to) the
// RPC's own xdrFormat "json" output. Topic filters match against whatever
// shape was stored at ingest time.
type XDRDecoder struct{}

var _ Decoder = XDRDecoder{}

func (XDRDecoder) DecodeScVal(base64XDR string) (json.RawMessage, error) {
	var val xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(base64XDR, &val); err != nil {
		decodeErrors.Add(1)
		slog.Warn("decode: unmarshaling ScVal XDR", "error", err, "base64_len", len(base64XDR))
		// Preserve the raw value in a lossless fallback so ingestion
		// never stalls on a single un-decodable value.
		return fallbackDecode(base64XDR, err), nil
	}
	v, err := scValToGo(val)
	if err != nil {
		decodeErrors.Add(1)
		slog.Warn("decode: converting ScVal", "type", val.Type.String(), "error", err)
		return fallbackDecode(base64XDR, err), nil
	}
	out, err := json.Marshal(v)
	if err != nil {
		decodeErrors.Add(1)
		slog.Warn("decode: marshaling ScVal", "error", err)
		return fallbackDecode(base64XDR, err), nil
	}
	return out, nil
}

// fallbackDecode returns a lossless {"unknown": ...} wrapper for a value
// that could not be decoded, preserving the raw base64 XDR so a future
// decoder version can reprocess it. The error is embedded in the metadata
// so the operator can inspect what went wrong.
func fallbackDecode(base64XDR string, decodeErr error) json.RawMessage {
	fallback, _ := json.Marshal(map[string]any{
		"unknown": map[string]any{
			"type":   "decode_error",
			"base64": base64XDR,
			"error":  decodeErr.Error(),
		},
	})
	return fallback
}

// scValToGo maps one ScVal onto plain Go values that marshal to the JSON
// shapes documented on XDRDecoder.
//
// contributors: add new ScVal type handling here. Unhandled types fall
// through to a lossless {"unknown": {...}} wrapper rather than an error, so
// ingestion never stalls on exotic values.
func scValToGo(val xdr.ScVal) (any, error) {
	switch val.Type {
	case xdr.ScValTypeScvBool:
		return map[string]any{"bool": *val.B}, nil
	case xdr.ScValTypeScvVoid:
		return map[string]any{"void": nil}, nil
	case xdr.ScValTypeScvU32:
		return map[string]any{"u32": uint32(*val.U32)}, nil
	case xdr.ScValTypeScvI32:
		return map[string]any{"i32": int32(*val.I32)}, nil
	case xdr.ScValTypeScvU64:
		return map[string]any{"u64": uint64(*val.U64)}, nil
	case xdr.ScValTypeScvI64:
		return map[string]any{"i64": int64(*val.I64)}, nil
	case xdr.ScValTypeScvTimepoint:
		return map[string]any{"timepoint": uint64(*val.Timepoint)}, nil
	case xdr.ScValTypeScvDuration:
		return map[string]any{"duration": uint64(*val.Duration)}, nil
	case xdr.ScValTypeScvU128:
		// 128/256-bit integers exceed JSON number precision; render decimal strings.
		return map[string]any{"u128": uint128String(*val.U128)}, nil
	case xdr.ScValTypeScvI128:
		return map[string]any{"i128": int128String(*val.I128)}, nil
	case xdr.ScValTypeScvU256:
		return map[string]any{"u256": uint256String(*val.U256)}, nil
	case xdr.ScValTypeScvI256:
		return map[string]any{"i256": int256String(*val.I256)}, nil
	case xdr.ScValTypeScvBytes:
		return map[string]any{"bytes": hex.EncodeToString(*val.Bytes)}, nil
	case xdr.ScValTypeScvString:
		return map[string]any{"string": string(*val.Str)}, nil
	case xdr.ScValTypeScvSymbol:
		return map[string]any{"symbol": string(*val.Sym)}, nil
	case xdr.ScValTypeScvAddress:
		addr, err := val.Address.String()
		if err != nil {
			return nil, fmt.Errorf("rendering address: %w", err)
		}
		return map[string]any{"address": addr}, nil
	case xdr.ScValTypeScvVec:
		vec := *val.Vec
		if vec == nil {
			return map[string]any{"vec": []any{}}, nil
		}
		items := make([]any, 0, len(*vec))
		for _, item := range *vec {
			v, err := scValToGo(item)
			if err != nil {
				return nil, err
			}
			items = append(items, v)
		}
		return map[string]any{"vec": items}, nil
	case xdr.ScValTypeScvMap:
		m := *val.Map
		if m == nil {
			return map[string]any{"map": []any{}}, nil
		}
		// Maps become ordered [{"key": K, "val": V}] pairs since ScMap keys
		// aren't restricted to strings.
		entries := make([]any, 0, len(*m))
		for _, entry := range *m {
			k, err := scValToGo(entry.Key)
			if err != nil {
				return nil, err
			}
			v, err := scValToGo(entry.Val)
			if err != nil {
				return nil, err
			}
			entries = append(entries, map[string]any{"key": k, "val": v})
		}
		return map[string]any{"map": entries}, nil
	case xdr.ScValTypeScvError:
		e := map[string]any{"type": val.Error.Type.String()}
		if val.Error.ContractCode != nil {
			e["contract_code"] = uint32(*val.Error.ContractCode)
		}
		if val.Error.Code != nil {
			e["code"] = val.Error.Code.String()
		}
		return map[string]any{"error": e}, nil
	default:
		// Lossless fallback: keep the type name and raw XDR so nothing is
		// dropped and a future decoder version can re-process it.
		raw, err := xdr.MarshalBase64(val)
		if err != nil {
			return nil, fmt.Errorf("re-encoding unhandled ScVal type %s: %w", val.Type, err)
		}
		return map[string]any{"unknown": map[string]any{
			"type":   val.Type.String(),
			"base64": raw,
		}}, nil
	}
}

func uint128String(p xdr.UInt128Parts) string {
	n := new(big.Int).SetUint64(uint64(p.Hi))
	n.Lsh(n, 64)
	n.Or(n, new(big.Int).SetUint64(uint64(p.Lo)))
	return n.String()
}

func int128String(p xdr.Int128Parts) string {
	n := big.NewInt(int64(p.Hi))
	n.Lsh(n, 64)
	n.Or(n, new(big.Int).SetUint64(uint64(p.Lo)))
	return n.String()
}

func uint256String(p xdr.UInt256Parts) string {
	n := new(big.Int).SetUint64(uint64(p.HiHi))
	for _, part := range []uint64{uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)} {
		n.Lsh(n, 64)
		n.Or(n, new(big.Int).SetUint64(part))
	}
	return n.String()
}

func int256String(p xdr.Int256Parts) string {
	n := big.NewInt(int64(p.HiHi))
	for _, part := range []uint64{uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)} {
		n.Lsh(n, 64)
		n.Or(n, new(big.Int).SetUint64(part))
	}
	return n.String()
}
