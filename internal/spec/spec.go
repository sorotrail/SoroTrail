// Package spec fetches, caches, and applies contract spec entries from
// deployed Soroban Wasm blobs to enrich raw tagged-JSON events into
// named-field representations.
//
// Spec acquisition chain:
//
//	contract ID → instance entry (LedgerKeyContractData) → wasm_hash →
//	LedgerKeyContractCode → Wasm blob → "contractspecv0" custom section →
//	[]ScSpecEntry → internal types
//
// Specs are immutable per Wasm hash. The cache is keyed by Wasm hash;
// contract upgrades that change the hash automatically pick up the new spec.
package spec

import (
	"encoding/json"
	"fmt"
)

// ContractSpec holds the parsed contract specification — the set of event
// definitions that describe the event shapes a contract can emit.
type ContractSpec struct {
	// WasmHash is the SHA-256 hash of the contract's Wasm blob.
	WasmHash string `json:"wasm_hash"`
	// ContractID is the Stellar address of the contract this spec was
	// fetched for (the first contract that triggered the fetch).
	ContractID string `json:"contract_id"`
	// Events is the set of named event definitions derived from spec entries.
	Events []EventSpec `json:"events"`
}

// EventSpec describes one named event the contract can emit.
type EventSpec struct {
	// Name is the event name — the symbol value of topic[0].
	Name string `json:"name"`
	// Doc is the optional documentation string from the spec entry.
	Doc string `json:"doc,omitempty"`
	// TopicSpecs describes the types of each topic position.
	// topic[0] is always the event name symbol and is omitted here.
	// topic[1..n] correspond to the positional fields.
	TopicSpecs []FieldSpec `json:"topic_specs,omitempty"`
	// ValueSpec describes the type of the event value, if any.
	ValueSpec *FieldSpec `json:"value_spec,omitempty"`
}

// FieldSpec describes one named field in an event's topic or value.
type FieldSpec struct {
	// Name is the field name from the spec entry.
	Name string `json:"name"`
	// Type is the Soroban type name (e.g. "address", "i128", "symbol").
	Type string `json:"type"`
}

// DecodedEvent is the enriched representation of one event.
type DecodedEvent struct {
	// Event is the event name (from topic[0] symbol).
	Event string `json:"event"`
	// Decoded is true when the event was successfully mapped to a spec entry.
	Decoded bool `json:"decoded"`
	// Fields maps field names to their decoded values.
	// Only present when Decoded is true.
	Fields map[string]any `json:"fields,omitempty"`
}

// ScalarValue extracts the scalar payload from a shape-tagged JSON value.
// Input: {"symbol":"transfer"}, {"address":"G..."}, {"i128":"1000"}, {"u64":42}
// Output: "transfer", "G...", "1000", float64(42)
func ScalarValue(raw json.RawMessage) (any, error) {
	var tagged map[string]any
	if err := json.Unmarshal(raw, &tagged); err != nil {
		return nil, fmt.Errorf("unmarshaling tagged value %s: %w", string(raw), err)
	}
	// A tagged value has exactly one key.
	for _, v := range tagged {
		return v, nil
	}
	return nil, fmt.Errorf("empty tagged value %s", string(raw))
}

// ParseTypeFromTag extracts the type tag from a shape-tagged JSON value.
// Input: {"symbol":"transfer"} → "symbol", {"i128":"1000"} → "i128"
func ParseTypeFromTag(raw json.RawMessage) string {
	var tagged map[string]any
	if err := json.Unmarshal(raw, &tagged); err != nil || len(tagged) == 0 {
		return ""
	}
	// Return the first (and only) key.
	for k := range tagged {
		return k
	}
	return ""
}
