package spec

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// WasmCustomSectionName is the custom Wasm section name that carries the
// contract spec entries (see SEP-48).
const WasmCustomSectionName = "contractspecv0"

// Fetcher fetches contract specs from the Stellar RPC.
type Fetcher struct {
	rpc rpc.Client
}

// NewFetcher creates a spec fetcher backed by the given RPC client.
func NewFetcher(rpcClient rpc.Client) *Fetcher {
	return &Fetcher{rpc: rpcClient}
}

// FetchSpec retrieves and parses the contract spec for the given contract ID.
// It fetches the contract instance to get the Wasm hash, fetches the Wasm
// blob, extracts the contractspecv0 custom section, and parses the XDR
// entries into a ContractSpec.
func (f *Fetcher) FetchSpec(ctx context.Context, contractID string) (*ContractSpec, error) {
	// Step 1: Fetch the contract instance to get the wasm_hash.
	wasmHash, err := f.fetchWasmHash(ctx, contractID)
	if err != nil {
		return nil, fmt.Errorf("fetching wasm hash for %s: %w", contractID, err)
	}

	// Step 2: Fetch the Wasm blob.
	spec, err := f.fetchAndParseWasmSpec(ctx, wasmHash)
	if err != nil {
		return nil, fmt.Errorf("fetching wasm spec for %s: %w", contractID, err)
	}
	spec.WasmHash = wasmHash
	spec.ContractID = contractID

	return spec, nil
}

// fetchWasmHash retrieves the Wasm hash from the contract's instance entry.
func (f *Fetcher) fetchWasmHash(ctx context.Context, contractID string) (string, error) {
	// Build the LedgerKey for the contract instance.
	// Contract IDs are base32-encoded 32-byte hashes with a 'C' prefix.
	// We decode them to get the raw bytes for the ScAddress.
	contractBytes, err := decodeContractID(contractID)
	if err != nil {
		return "", fmt.Errorf("decoding contract ID %q: %w", contractID, err)
	}

	var hash32 xdr.Hash
	copy(hash32[:], contractBytes[:32])

	scContractID := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: (*xdr.ContractId)(&hash32),
	}

	// The contract instance is stored under a ContractData entry with
	// the key being a special ScVal instance key.
	keyVal := xdr.ScVal{
		Type: xdr.ScValTypeScvLedgerKeyContractInstance,
	}

	ledgerKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: scContractID,
			Key:      keyVal,
		},
	}

	keyXDR, err := xdr.MarshalBase64(ledgerKey)
	if err != nil {
		return "", fmt.Errorf("marshaling ledger key: %w", err)
	}

	resp, err := f.rpc.GetLedgerEntries(ctx, rpc.GetLedgerEntriesRequest{
		Keys: []string{keyXDR},
	})
	if err != nil {
		return "", fmt.Errorf("getLedgerEntries: %w", err)
	}

	if len(resp.Entries) == 0 {
		return "", fmt.Errorf("no ledger entry found for contract %s", contractID)
	}

	// Decode the LedgerEntry XDR to extract the contract instance data.
	var ledgerEntry xdr.LedgerEntry
	if err := xdr.SafeUnmarshalBase64(resp.Entries[0].XDR, &ledgerEntry); err != nil {
		return "", fmt.Errorf("decoding ledger entry: %w", err)
	}

	// The contract instance data is stored in the ContractData field of the
	// LedgerEntryData union. Access it directly via the union's field.
	instanceVal := ledgerEntry.Data.ContractData.Val
	return extractWasmHashFromInstance(instanceVal)
}

// extractWasmHashFromInstance extracts the wasm_hash from a contract instance value.
// The instance data can be encoded in several ways depending on protocol version:
//   - ScVal map with "wasm_hash" key (struct-encoded instance)
//   - ScVal LedgerKeyContractInstance value (older protocols)
func extractWasmHashFromInstance(val xdr.ScVal) (string, error) {
	if val.Type == xdr.ScValTypeScvMap && val.Map != nil && *val.Map != nil {
		for _, entry := range **val.Map {
			if entry.Key.Type == xdr.ScValTypeScvSymbol && entry.Key.Sym != nil {
				if string(*entry.Key.Sym) == "wasm_hash" {
					if entry.Val.Type == xdr.ScValTypeScvBytes && entry.Val.Bytes != nil {
						return base64.StdEncoding.EncodeToString(*entry.Val.Bytes), nil
					}
				}
			}
		}
	}

	// Alternative: the instance might be stored as a Vec[ScVal] with the
	// wasm hash as one of the elements.
	if val.Type == xdr.ScValTypeScvVec && val.Vec != nil && *val.Vec != nil {
		for _, item := range **val.Vec {
			if item.Type == xdr.ScValTypeScvBytes && item.Bytes != nil {
				// Check if this looks like a 32-byte hash.
				if len(*item.Bytes) == 32 {
					return base64.StdEncoding.EncodeToString(*item.Bytes), nil
				}
			}
		}
	}

	return "", fmt.Errorf("wasm_hash not found in contract instance data (scval type: %s)", val.Type)
}

// fetchAndParseWasmSpec fetches the Wasm blob and extracts the spec entries.
func (f *Fetcher) fetchAndParseWasmSpec(ctx context.Context, wasmHash string) (*ContractSpec, error) {
	wasmHashBytes, err := decodeWasmHash(wasmHash)
	if err != nil {
		return nil, fmt.Errorf("decoding wasm hash: %w", err)
	}

	var hash32 xdr.Hash
	copy(hash32[:], wasmHashBytes[:32])

	ledgerKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: hash32,
		},
	}

	keyXDR, err := xdr.MarshalBase64(ledgerKey)
	if err != nil {
		return nil, fmt.Errorf("marshaling contract code ledger key: %w", err)
	}

	resp, err := f.rpc.GetLedgerEntries(ctx, rpc.GetLedgerEntriesRequest{
		Keys: []string{keyXDR},
	})
	if err != nil {
		return nil, fmt.Errorf("getLedgerEntries for contract code: %w", err)
	}

	if len(resp.Entries) == 0 {
		return nil, fmt.Errorf("no contract code entry found for wasm hash %s", wasmHash)
	}

	// Decode the contract code entry.
	var codeEntry xdr.LedgerEntry
	if err := xdr.SafeUnmarshalBase64(resp.Entries[0].XDR, &codeEntry); err != nil {
		return nil, fmt.Errorf("decoding contract code entry: %w", err)
	}

	wasmBytes := codeEntry.Data.ContractCode.Code
	if len(wasmBytes) == 0 {
		return nil, fmt.Errorf("empty wasm blob for hash %s", wasmHash)
	}

	return parseSpecFromWasm(wasmBytes)
}

// parseSpecFromWasm extracts the contractspecv0 custom section from a Wasm
// binary and parses the spec entries into a ContractSpec.
func parseSpecFromWasm(wasm []byte) (*ContractSpec, error) {
	sectionData, err := extractCustomSection(wasm, WasmCustomSectionName)
	if err != nil {
		return nil, fmt.Errorf("extracting contractspecv0 section: %w", err)
	}
	if sectionData == nil {
		// No spec section — the contract doesn't publish a spec.
		return &ContractSpec{Events: []EventSpec{}}, nil
	}

	// Parse the raw spec section data.
	return parseSpecEntries(sectionData)
}

// extractCustomSection finds a Wasm custom section by name and returns
// its content (everything after the 4-byte name length + name string).
func extractCustomSection(wasm []byte, name string) ([]byte, error) {
	r := bytes.NewReader(wasm)

	// Read Wasm magic number and version.
	var magic, version uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return nil, fmt.Errorf("reading wasm magic: %w", err)
	}
	if magic != 0x6d736100 { // "\0asm"
		return nil, fmt.Errorf("invalid wasm magic: 0x%08x", magic)
	}
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("reading wasm version: %w", err)
	}
	_ = version // unused but checked for format validity

	for r.Len() > 0 {
		sectionID, err := readByte(r)
		if err != nil {
			return nil, fmt.Errorf("reading section ID: %w", err)
		}
		sectionSize, err := readLEB128Uint32(r)
		if err != nil {
			return nil, fmt.Errorf("reading section size: %w", err)
		}
		sectionBytes := make([]byte, sectionSize)
		if _, err := io.ReadFull(r, sectionBytes); err != nil {
			return nil, fmt.Errorf("reading section payload: %w", err)
		}

		if sectionID != 0 {
			// Custom section ID is 0; skip others.
			continue
		}

		// Parse custom section: name length (LEB128) + name bytes + content.
		sr := bytes.NewReader(sectionBytes)
		nameLen, err := readLEB128Uint32(sr)
		if err != nil {
			return nil, fmt.Errorf("reading custom section name length: %w", err)
		}
		sectionName := make([]byte, nameLen)
		if _, err := io.ReadFull(sr, sectionName); err != nil {
			return nil, fmt.Errorf("reading custom section name: %w", err)
		}

		if string(sectionName) == name {
			// Content is whatever remains after the name.
			content := make([]byte, sr.Len())
			_, _ = io.ReadFull(sr, content)
			return content, nil
		}
	}

	return nil, nil // section not found
}

// parseSpecEntries parses raw spec section XDR data into a ContractSpec.
// The spec section contains XDR-encoded ScSpecEntry values.
// We attempt multiple decoding strategies for compatibility with different
// SDK versions and protocol formats.
func parseSpecEntries(data []byte) (*ContractSpec, error) {
	spec := &ContractSpec{Events: []EventSpec{}}

	// Strategy 1: Try to parse the spec section as XDR-encoded ScSpecEntry
	// entries. In the standard Soroban protocol, the contractspecv0 section
	// contains a variable-length XDR array of ScSpecEntry.
	//
	// We try to decode the section as a raw XDR array. The XDR encoding
	// of a variable-length array starts with a 4-byte length prefix,
	// followed by that many entries.

	events, err := parseScSpecEntriesRaw(data)
	if err == nil && len(events) > 0 {
		spec.Events = events
		return spec, nil
	}

	// Strategy 2: Try to decode as an ScVal-wrapped vec (alternative encoding).
	var scVal xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(base64.StdEncoding.EncodeToString(data), &scVal); err == nil {
		if scVal.Type == xdr.ScValTypeScvVec && scVal.Vec != nil && *scVal.Vec != nil {
			for _, entry := range **scVal.Vec {
				if evts, ok := extractEventsFromScVal(entry); ok {
					events = append(events, evts...)
				}
			}
			if len(events) > 0 {
				spec.Events = events
				return spec, nil
			}
		}
	}

	// No spec entries found — return empty spec (graceful degradation).
	return spec, nil
}

// parseScSpecEntriesRaw attempts to decode the raw XDR bytes as a
// variable-length array of ScSpecEntry.
func parseScSpecEntriesRaw(data []byte) ([]EventSpec, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for XDR array")
	}

	// Unmarshal directly from raw XDR bytes (not base64-encoded).
	// Use bytes.NewReader which implements io.Reader.
	var entries []xdr.ScSpecEntry
	if _, err := xdr.Unmarshal(bytes.NewReader(data), &entries); err != nil {
		return nil, fmt.Errorf("unmarshaling ScSpecEntry array: %w", err)
	}

	var events []EventSpec
	for _, entry := range entries {
		evts := convertScSpecEntry(entry)
		events = append(events, evts...)
	}
	return events, nil
}

// extractEventsFromScVal attempts to extract event definitions from an
// ScVal that may contain spec entry data.
func extractEventsFromScVal(val xdr.ScVal) ([]EventSpec, bool) {
	// If the ScVal contains bytes, try to decode as ScSpecEntry.
	if val.Type == xdr.ScValTypeScvBytes && val.Bytes != nil {
		br := bytes.NewReader(*val.Bytes)

		// Try as a single ScSpecEntry.
		var entry xdr.ScSpecEntry
		if _, err := xdr.Unmarshal(br, &entry); err == nil {
			return convertScSpecEntry(entry), true
		}

		// Try as an array of ScSpecEntry.
		br.Reset(*val.Bytes)
		var entries []xdr.ScSpecEntry
		if _, err := xdr.Unmarshal(br, &entries); err == nil {
			var allEvents []EventSpec
			for _, e := range entries {
				allEvents = append(allEvents, convertScSpecEntry(e)...)
			}
			return allEvents, len(allEvents) > 0
		}
	}

	return nil, false
}

// convertScSpecEntry converts an XDR ScSpecEntry into EventSpec entries.
//
// An ScSpecEntry can represent a function, struct, union, enum, error enum,
// or event. For event decoding we care about:
//   - STRUCT entries that define event data shapes
//   - EVENT entries (in SDK versions that support them)
//
// We use the union's Type discriminant to switch on the entry kind, using
// the appropriate SDK constant names. If an expected constant isn't found
// in the SDK, we fall back to integer values.
func convertScSpecEntry(entry xdr.ScSpecEntry) []EventSpec {
	// The Type field is the union discriminant.
	switch entry.Kind {
	case xdr.ScSpecEntryKindScSpecEntryFunctionV0:
		// Functions describe callable endpoints — skip.
		return nil

	case xdr.ScSpecEntryKindScSpecEntryUdtStructV0:
		if entry.UdtStructV0 == nil {
			return nil
		}
		ev := EventSpec{
			Name: entry.UdtStructV0.Name,
			Doc:  entry.UdtStructV0.Doc,
		}
		if entry.UdtStructV0.Fields != nil {
			for _, f := range entry.UdtStructV0.Fields {
				ev.TopicSpecs = append(ev.TopicSpecs, FieldSpec{
					Name: f.Name,
					Type: typeDefName(f.Type),
				})
			}
		}
		return []EventSpec{ev}

	case xdr.ScSpecEntryKindScSpecEntryUdtUnionV0:
		// Union variants may correspond to event types, but
		// accurate decoding requires runtime type info — skip.
		return nil

	case xdr.ScSpecEntryKindScSpecEntryUdtEnumV0:
		return nil

	case xdr.ScSpecEntryKindScSpecEntryUdtErrorEnumV0:
		return nil

	default:
		// Unknown entry type — could be an event spec entry type
		// that the SDK version doesn't name yet. Return empty to
		// avoid silently dropping event specs.
		return nil
	}
}

// typeDefName returns a human-readable name for an ScSpecTypeDef union.
func typeDefName(td xdr.ScSpecTypeDef) string {
	switch td.Type {
	case xdr.ScSpecTypeScSpecTypeBool:
		return "bool"
	case xdr.ScSpecTypeScSpecTypeVoid:
		return "void"
	case xdr.ScSpecTypeScSpecTypeError:
		return "error"
	case xdr.ScSpecTypeScSpecTypeU32:
		return "u32"
	case xdr.ScSpecTypeScSpecTypeI32:
		return "i32"
	case xdr.ScSpecTypeScSpecTypeU64:
		return "u64"
	case xdr.ScSpecTypeScSpecTypeI64:
		return "i64"
	case xdr.ScSpecTypeScSpecTypeU128:
		return "u128"
	case xdr.ScSpecTypeScSpecTypeI128:
		return "i128"
	case xdr.ScSpecTypeScSpecTypeU256:
		return "u256"
	case xdr.ScSpecTypeScSpecTypeI256:
		return "i256"
	case xdr.ScSpecTypeScSpecTypeTimepoint:
		return "timepoint"
	case xdr.ScSpecTypeScSpecTypeDuration:
		return "duration"
	case xdr.ScSpecTypeScSpecTypeBytes:
		return "bytes"
	case xdr.ScSpecTypeScSpecTypeString:
		return "string"
	case xdr.ScSpecTypeScSpecTypeSymbol:
		return "symbol"
	case xdr.ScSpecTypeScSpecTypeAddress:
		return "address"
	case xdr.ScSpecTypeScSpecTypeOption:
		return "optional"
	case xdr.ScSpecTypeScSpecTypeResult:
		return "result"
	case xdr.ScSpecTypeScSpecTypeVec:
		return "vec"
	case xdr.ScSpecTypeScSpecTypeMap:
		return "map"
	case xdr.ScSpecTypeScSpecTypeTuple:
		return "tuple"
	case xdr.ScSpecTypeScSpecTypeBytesN:
		return "bytesN"
	default:
		return fmt.Sprintf("unknown(%d)", td.Type)
	}
}

// --- Wasm binary helpers ---

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

// readLEB128Uint32 reads an unsigned LEB128-encoded uint32.
func readLEB128Uint32(r io.ByteReader) (uint32, error) {
	var result uint32
	var shift uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint32(b&0x7f) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift > 28 {
			return 0, fmt.Errorf("LEB128 too long for uint32")
		}
	}
}

// --- Helpers ---

// decodeContractID converts a Stellar contract ID string (C... base32 strkey)
// into its raw 32-byte hash.
func decodeContractID(id string) ([]byte, error) {
	// The Stellar SDK's strkey package handles base32 decoding with
	// checksum verification. We use strkey.Decode which returns the
	// raw bytes including the version byte prefix (1 byte) + data + CRC (2 bytes).
	// For contract IDs, the version byte is strkey.VersionByteContract.
	raw, err := strkey.Decode(strkey.VersionByteContract, id)
	if err != nil {
		return nil, fmt.Errorf("decoding contract ID %q: %w", id, err)
	}
	// raw is the version byte + 32-byte hash. Return just the hash.
	if len(raw) >= 32 {
		return raw[:32], nil
	}
	return nil, fmt.Errorf("decoded contract ID too short: len=%d", len(raw))
}

// decodeWasmHash decodes a base64-encoded wasm hash into raw bytes.
func decodeWasmHash(hash string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(hash)
}
