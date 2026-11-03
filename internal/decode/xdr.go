package decode

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

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

// decodeScratch holds reusable byte buffers and a bytes.Reader for base64 XDR
// decoding and JSON rendering.
//
// Lifetime rule: a decodeScratch is acquired from decodeScratchPool at the
// start of DecodeScVal or decodeTopicsXDR and returned via defer before the
// call returns. All returned json.RawMessage slices are copied into freshly
// allocated slices before the scratch struct is returned to the pool, so a
// pooled buffer never escapes into store.Event or across goroutines.
type decodeScratch struct {
	rawBuf  []byte
	jsonBuf []byte
	reader  bytes.Reader
	val     xdr.ScVal
}

var decodeScratchPool = sync.Pool{
	New: func() any {
		return &decodeScratch{
			rawBuf:  make([]byte, 0, 256),
			jsonBuf: make([]byte, 0, 256),
		}
	},
}

func (XDRDecoder) DecodeScVal(base64XDR string) (json.RawMessage, error) {
	sc := decodeScratchPool.Get().(*decodeScratch)
	defer decodeScratchPool.Put(sc)

	sc.jsonBuf = sc.jsonBuf[:0]
	out, err := sc.decodeScValAppend(sc.jsonBuf, base64XDR)
	if err != nil {
		return nil, err
	}
	sc.jsonBuf = out

	res := make(json.RawMessage, len(out))
	copy(res, out)
	return res, nil
}

func (sc *decodeScratch) decodeScValAppend(dst []byte, base64XDR string) ([]byte, error) {
	sc.val = xdr.ScVal{}
	if err := sc.unmarshalScValBase64(base64XDR, &sc.val); err != nil {
		decodeErrors.Add(1)
		slog.Warn("decode: unmarshaling ScVal XDR", "error", err, "base64_len", len(base64XDR))
		// Preserve the raw value in a lossless fallback so ingestion
		// never stalls on a single un-decodable value.
		return appendFallbackDecode(dst, base64XDR, err), nil
	}
	startLen := len(dst)
	out, err := appendScValJSON(dst, sc.val)
	if err != nil {
		decodeErrors.Add(1)
		slog.Warn("decode: converting ScVal", "type", sc.val.Type.String(), "error", err)
		return appendFallbackDecode(dst[:startLen], base64XDR, err), nil
	}
	return out, nil
}

func (sc *decodeScratch) unmarshalScValBase64(base64XDR string, val *xdr.ScVal) error {
	if len(base64XDR) == 0 {
		return io.EOF
	}
	// Delegate inputs containing newlines or carriage returns to the SDK helper
	// so multi-line base64 edge cases behave identically.
	if strings.IndexByte(base64XDR, '\n') >= 0 || strings.IndexByte(base64XDR, '\r') >= 0 {
		return xdr.SafeUnmarshalBase64(base64XDR, val)
	}
	maxDecoded := base64.StdEncoding.DecodedLen(len(base64XDR))
	if cap(sc.rawBuf) < maxDecoded {
		sc.rawBuf = make([]byte, maxDecoded)
	} else {
		sc.rawBuf = sc.rawBuf[:maxDecoded]
	}
	nDecoded, err := base64.StdEncoding.Decode(sc.rawBuf, []byte(base64XDR))
	if err != nil {
		return err
	}
	// Check that the base64 input had no unconsumed trailing characters after padding.
	if base64.StdEncoding.EncodedLen(nDecoded) != len(base64XDR) {
		return xdr.SafeUnmarshalBase64(base64XDR, val)
	}
	rawBytes := sc.rawBuf[:nDecoded]
	sc.reader.Reset(rawBytes)
	nRead, err := xdr.Unmarshal(&sc.reader, val)
	if err != nil {
		return err
	}
	if nRead != nDecoded {
		return fmt.Errorf("input not fully consumed. expected to read: %d, actual: %d", nDecoded, nRead)
	}
	return nil
}

// fallbackDecode returns a lossless {"unknown": ...} wrapper for a value
// that could not be decoded, preserving the raw base64 XDR so a future
// decoder version can reprocess it. The error is embedded in the metadata
// so the operator can inspect what went wrong.
func fallbackDecode(base64XDR string, decodeErr error) json.RawMessage {
	return appendFallbackDecode(nil, base64XDR, decodeErr)
}

func appendFallbackDecode(dst []byte, base64XDR string, decodeErr error) json.RawMessage {
	// Keys in alphabetical order to match encoding/json map marshaling:
	// "base64", "error", "type".
	dst = append(dst, `{"unknown":{"base64":`...)
	dst = appendJSONString(dst, base64XDR)
	dst = append(dst, `,"error":`...)
	dst = appendJSONString(dst, decodeErr.Error())
	dst = append(dst, `,"type":"decode_error"}}`...)
	return dst
}

// appendScValJSON appends the JSON representation of val directly to dst,
// producing byte-for-byte identical output to json.Marshal(scValToGo(val))
// without allocating intermediate map[string]any wrappers or invoking reflection.
func appendScValJSON(dst []byte, val xdr.ScVal) ([]byte, error) {
	switch val.Type {
	case xdr.ScValTypeScvBool:
		if *val.B {
			return append(dst, `{"bool":true}`...), nil
		}
		return append(dst, `{"bool":false}`...), nil
	case xdr.ScValTypeScvVoid:
		return append(dst, `{"void":null}`...), nil
	case xdr.ScValTypeScvU32:
		dst = append(dst, `{"u32":`...)
		dst = strconv.AppendUint(dst, uint64(*val.U32), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvI32:
		dst = append(dst, `{"i32":`...)
		dst = strconv.AppendInt(dst, int64(*val.I32), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvU64:
		dst = append(dst, `{"u64":`...)
		dst = strconv.AppendUint(dst, uint64(*val.U64), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvI64:
		dst = append(dst, `{"i64":`...)
		dst = strconv.AppendInt(dst, int64(*val.I64), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvTimepoint:
		dst = append(dst, `{"timepoint":`...)
		dst = strconv.AppendUint(dst, uint64(*val.Timepoint), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvDuration:
		dst = append(dst, `{"duration":`...)
		dst = strconv.AppendUint(dst, uint64(*val.Duration), 10)
		return append(dst, '}'), nil
	case xdr.ScValTypeScvU128:
		dst = append(dst, `{"u128":"`...)
		dst = appendUint128(dst, *val.U128)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvI128:
		dst = append(dst, `{"i128":"`...)
		dst = appendInt128(dst, *val.I128)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvU256:
		dst = append(dst, `{"u256":"`...)
		dst = appendUint256(dst, *val.U256)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvI256:
		dst = append(dst, `{"i256":"`...)
		dst = appendInt256(dst, *val.I256)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvBytes:
		raw := *val.Bytes
		dst = append(dst, `{"bytes":"`...)
		hexLen := hex.EncodedLen(len(raw))
		start := len(dst)
		if cap(dst)-start < hexLen {
			grown := make([]byte, start+hexLen, max(cap(dst)*2, start+hexLen+16))
			copy(grown, dst)
			dst = grown
		} else {
			dst = dst[:start+hexLen]
		}
		hex.Encode(dst[start:start+hexLen], raw)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvString:
		dst = append(dst, `{"string":`...)
		dst = appendJSONString(dst, string(*val.Str))
		return append(dst, '}'), nil
	case xdr.ScValTypeScvSymbol:
		dst = append(dst, `{"symbol":`...)
		dst = appendJSONString(dst, string(*val.Sym))
		return append(dst, '}'), nil
	case xdr.ScValTypeScvAddress:
		addr, err := val.Address.String()
		if err != nil {
			return nil, fmt.Errorf("rendering address: %w", err)
		}
		dst = append(dst, `{"address":"`...)
		dst = append(dst, addr...)
		return append(dst, `"}`...), nil
	case xdr.ScValTypeScvVec:
		dst = append(dst, `{"vec":[`...)
		if val.Vec != nil && *val.Vec != nil {
			for i, item := range **val.Vec {
				if i > 0 {
					dst = append(dst, ',')
				}
				var err error
				dst, err = appendScValJSON(dst, item)
				if err != nil {
					return nil, err
				}
			}
		}
		return append(dst, `]}`...), nil
	case xdr.ScValTypeScvMap:
		dst = append(dst, `{"map":[`...)
		if val.Map != nil && *val.Map != nil {
			var err error
			dst, err = appendScMapEntriesJSON(dst, **val.Map)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, `]}`...), nil
	case xdr.ScValTypeScvContractInstance:
		inst := *val.Instance
		dst = append(dst, `{"contract_instance":{"executable":`...)
		var err error
		dst, err = appendContractExecutableJSON(dst, inst.Executable)
		if err != nil {
			return nil, err
		}
		dst = append(dst, `,"storage":[`...)
		if inst.Storage != nil {
			dst, err = appendScMapEntriesJSON(dst, *inst.Storage)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, `]}}`...), nil
	case xdr.ScValTypeScvLedgerKeyNonce:
		dst = append(dst, `{"ledger_key_nonce":{"nonce":`...)
		dst = strconv.AppendInt(dst, int64(val.NonceKey.Nonce), 10)
		return append(dst, `}}`...), nil
	case xdr.ScValTypeScvExecutableTag:
		dst = append(dst, `{"executable_tag":`...)
		dst = appendJSONString(dst, string(*val.ExecutableTag))
		return append(dst, '}'), nil
	case xdr.ScValTypeScvError:
		// Keys sorted alphabetically to match encoding/json map ordering:
		// "code", "contract_code", "type".
		dst = append(dst, `{"error":{`...)
		needComma := false
		if val.Error.Code != nil {
			dst = append(dst, `"code":`...)
			dst = appendJSONString(dst, val.Error.Code.String())
			needComma = true
		}
		if val.Error.ContractCode != nil {
			if needComma {
				dst = append(dst, ',')
			}
			dst = append(dst, `"contract_code":`...)
			dst = strconv.AppendUint(dst, uint64(*val.Error.ContractCode), 10)
			needComma = true
		}
		if needComma {
			dst = append(dst, ',')
		}
		dst = append(dst, `"type":`...)
		dst = appendJSONString(dst, val.Error.Type.String())
		return append(dst, `}}`...), nil
	default:
		raw, err := xdr.MarshalBase64(val)
		if err != nil {
			return nil, fmt.Errorf("re-encoding unhandled ScVal type %s: %w", val.Type, err)
		}
		// Alphabetical key order: "base64", "type".
		dst = append(dst, `{"unknown":{"base64":`...)
		dst = appendJSONString(dst, raw)
		dst = append(dst, `,"type":`...)
		dst = appendJSONString(dst, val.Type.String())
		return append(dst, `}}`...), nil
	}
}

func appendScMapEntriesJSON(dst []byte, m xdr.ScMap) ([]byte, error) {
	for i, entry := range m {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"key":`...)
		var err error
		dst, err = appendScValJSON(dst, entry.Key)
		if err != nil {
			return nil, err
		}
		dst = append(dst, `,"val":`...)
		dst, err = appendScValJSON(dst, entry.Val)
		if err != nil {
			return nil, err
		}
		dst = append(dst, '}')
	}
	return dst, nil
}

func appendContractExecutableJSON(dst []byte, exec xdr.ContractExecutable) ([]byte, error) {
	switch exec.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		dst = append(dst, `{"wasm_hash":"`...)
		dst = append(dst, exec.WasmHash.HexString()...)
		return append(dst, `"}`...), nil
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		return append(dst, `{"stellar_asset":null}`...), nil
	case xdr.ContractExecutableTypeContractExecutableExternalRef:
		owner, err := exec.ExternalRef.ExecutableOwner.String()
		if err != nil {
			return nil, fmt.Errorf("rendering external-ref owner: %w", err)
		}
		dst = append(dst, `{"external_ref":{"executable_owner":`...)
		dst = appendJSONString(dst, owner)
		dst = append(dst, `,"tag":`...)
		dst = appendJSONString(dst, string(exec.ExternalRef.Tag))
		return append(dst, `}}`...), nil
	default:
		raw, err := xdr.MarshalBase64(exec)
		if err != nil {
			return nil, fmt.Errorf("re-encoding unhandled ContractExecutable type %s: %w", exec.Type, err)
		}
		dst = append(dst, `{"unknown":{"base64":`...)
		dst = appendJSONString(dst, raw)
		dst = append(dst, `,"type":`...)
		dst = appendJSONString(dst, exec.Type.String())
		return append(dst, `}}`...), nil
	}
}

// appendJSONString appends s as a JSON string literal matching encoding/json's
// escaping rules (including HTML escaping of '<', '>', '&'). Pure printable
// ASCII strings without special characters take a zero-allocation fast path.
func appendJSONString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= utf8.RuneSelf || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			b, _ := json.Marshal(s)
			return append(dst, b...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	return append(dst, '"')
}

// scValToGo maps one ScVal onto plain Go values that marshal to the JSON
// shapes documented on XDRDecoder.
//
// contributors: add new ScVal type handling here and in appendScValJSON.
// Unhandled types fall through to a lossless {"unknown": {...}} wrapper rather
// than an error, so ingestion never stalls on exotic values.
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
		if val.Map == nil || *val.Map == nil {
			return map[string]any{"map": []any{}}, nil
		}
		entries, err := scMapToGo(**val.Map)
		if err != nil {
			return nil, err
		}
		return map[string]any{"map": entries}, nil
	case xdr.ScValTypeScvContractInstance:
		// SCV_CONTRACT_INSTANCE carries the contract's instance struct: which
		// executable runs it plus its persistent storage map.
		inst := *val.Instance
		exec, err := contractExecutableToGo(inst.Executable)
		if err != nil {
			return nil, err
		}
		storage := []any{}
		if inst.Storage != nil {
			storage, err = scMapToGo(*inst.Storage)
			if err != nil {
				return nil, err
			}
		}
		return map[string]any{"contract_instance": map[string]any{
			"executable": exec,
			"storage":    storage,
		}}, nil
	case xdr.ScValTypeScvLedgerKeyNonce:
		// SCNonceKey struct: the nonce guarding a contract's auth entries.
		return map[string]any{"ledger_key_nonce": map[string]any{"nonce": int64(val.NonceKey.Nonce)}}, nil
	case xdr.ScValTypeScvExecutableTag:
		return map[string]any{"executable_tag": string(*val.ExecutableTag)}, nil
	// SCV_LEDGER_KEY_CONTRACT_INSTANCE is a payload-less void marker used only
	// as a special contract-data key; it is intentionally left to the lossless
	// {"unknown": ...} fallback below rather than emitting an empty object.
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

// scMapToGo converts an ScMap into an ordered list of {"key": K, "val": V}
// pairs. ScMap keys aren't restricted to strings, so collapsing them into a
// JSON object would lose information; the ordered pair form preserves it.
// Shared by ScVal maps and contract-instance storage.
func scMapToGo(m xdr.ScMap) ([]any, error) {
	entries := make([]any, 0, len(m))
	for _, entry := range m {
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
	return entries, nil
}

// contractExecutableToGo converts a ContractExecutable union — the executable
// arm of SCV_CONTRACT_INSTANCE — into JSON. Unknown arms fall back to the same
// lossless {"unknown": ...} wrapper as scValToGo.
func contractExecutableToGo(exec xdr.ContractExecutable) (any, error) {
	switch exec.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		return map[string]any{"wasm_hash": exec.WasmHash.HexString()}, nil
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		return map[string]any{"stellar_asset": nil}, nil
	case xdr.ContractExecutableTypeContractExecutableExternalRef:
		owner, err := exec.ExternalRef.ExecutableOwner.String()
		if err != nil {
			return nil, fmt.Errorf("rendering external-ref owner: %w", err)
		}
		return map[string]any{"external_ref": map[string]any{
			"executable_owner": owner,
			"tag":              string(exec.ExternalRef.Tag),
		}}, nil
	default:
		raw, err := xdr.MarshalBase64(exec)
		if err != nil {
			return nil, fmt.Errorf("re-encoding unhandled ContractExecutable type %s: %w", exec.Type, err)
		}
		return map[string]any{"unknown": map[string]any{
			"type":   exec.Type.String(),
			"base64": raw,
		}}, nil
	}
}

func appendUint128(dst []byte, p xdr.UInt128Parts) []byte {
	if p.Hi == 0 {
		return strconv.AppendUint(dst, uint64(p.Lo), 10)
	}
	var n, lo big.Int
	n.SetUint64(uint64(p.Hi))
	n.Lsh(&n, 64)
	n.Or(&n, lo.SetUint64(uint64(p.Lo)))
	return n.Append(dst, 10)
}

func appendInt128(dst []byte, p xdr.Int128Parts) []byte {
	if p.Hi == 0 {
		return strconv.AppendUint(dst, uint64(p.Lo), 10)
	}
	if p.Hi == -1 && int64(p.Lo) < 0 {
		return strconv.AppendInt(dst, int64(p.Lo), 10)
	}
	var n, lo big.Int
	n.SetInt64(int64(p.Hi))
	n.Lsh(&n, 64)
	n.Or(&n, lo.SetUint64(uint64(p.Lo)))
	return n.Append(dst, 10)
}

func appendUint256(dst []byte, p xdr.UInt256Parts) []byte {
	if p.HiHi == 0 && p.HiLo == 0 && p.LoHi == 0 {
		return strconv.AppendUint(dst, uint64(p.LoLo), 10)
	}
	var n, tmp big.Int
	n.SetUint64(uint64(p.HiHi))
	for _, part := range [3]uint64{uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)} {
		n.Lsh(&n, 64)
		n.Or(&n, tmp.SetUint64(part))
	}
	return n.Append(dst, 10)
}

func appendInt256(dst []byte, p xdr.Int256Parts) []byte {
	if p.HiHi == 0 && p.HiLo == 0 && p.LoHi == 0 {
		return strconv.AppendUint(dst, uint64(p.LoLo), 10)
	}
	var n, tmp big.Int
	n.SetInt64(int64(p.HiHi))
	for _, part := range [3]uint64{uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)} {
		n.Lsh(&n, 64)
		n.Or(&n, tmp.SetUint64(part))
	}
	return n.Append(dst, 10)
}

func uint128String(p xdr.UInt128Parts) string {
	if p.Hi == 0 {
		return strconv.FormatUint(uint64(p.Lo), 10)
	}
	var buf [40]byte
	return string(appendUint128(buf[:0], p))
}

func int128String(p xdr.Int128Parts) string {
	if p.Hi == 0 {
		return strconv.FormatUint(uint64(p.Lo), 10)
	}
	if p.Hi == -1 && int64(p.Lo) < 0 {
		return strconv.FormatInt(int64(p.Lo), 10)
	}
	var buf [40]byte
	return string(appendInt128(buf[:0], p))
}

func uint256String(p xdr.UInt256Parts) string {
	if p.HiHi == 0 && p.HiLo == 0 && p.LoHi == 0 {
		return strconv.FormatUint(uint64(p.LoLo), 10)
	}
	var buf [80]byte
	return string(appendUint256(buf[:0], p))
}

func int256String(p xdr.Int256Parts) string {
	if p.HiHi == 0 && p.HiLo == 0 && p.LoHi == 0 {
		return strconv.FormatUint(uint64(p.LoLo), 10)
	}
	var buf [80]byte
	return string(appendInt256(buf[:0], p))
}
