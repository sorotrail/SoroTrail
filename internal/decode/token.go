// Package decode turns Soroban ScVal payloads into queryable JSON.
//
// This file implements SEP-41 token event recognition on top of the already-decoded
// JSON topics and value, extracting per-address balance changes without needing
// the raw contract spec.
package decode

import (
	"encoding/json"
	"math/big"
	"strings"
)

// TokenEventKind is one of the SEP-41 token event variants.
type TokenEventKind string

const (
	TokenTransfer TokenEventKind = "transfer"
	TokenMint     TokenEventKind = "mint"
	TokenBurn     TokenEventKind = "burn"
	TokenClawback TokenEventKind = "clawback"
)

// TokenEvent holds the parsed fields from a SEP-41 token event.
type TokenEvent struct {
	Kind       TokenEventKind
	ContractID string
	From       string // empty for mint
	To         string // empty for burn, clawback
	Amount     *big.Int
	EventID    string
	Ledger     int64
	Network    string
}

// allowedTokenEvents lists every event name the decoder recognises.
// Only these produce TokenEvents; silent skip for everything else.
var allowedTokenEvents = map[string]TokenEventKind{
	"transfer": TokenTransfer,
	"mint":     TokenMint,
	"burn":     TokenBurn,
	"clawback": TokenClawback,
}

// scvSymbol extracts a symbol value from a decoded ScVal JSON object,
// e.g. {"symbol": "transfer"} -> "transfer".
func scvSymbol(v json.RawMessage) (string, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(v, &obj); err != nil {
		return "", false
	}
	sym, ok := obj["symbol"]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(sym, &s); err != nil {
		return "", false
	}
	return s, true
}

// scvAddress extracts an address value from a decoded ScVal JSON object,
// e.g. {"address": "C..."} -> "C...".
func scvAddress(v json.RawMessage) (string, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(v, &obj); err != nil {
		return "", false
	}
	addr, ok := obj["address"]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(addr, &s); err != nil {
		return "", false
	}
	return s, true
}

// scvAmount extracts an i128 amount from a decoded ScVal value JSON object,
// e.g. {"i128": "123456789"} -> 123456789.
func scvAmount(v json.RawMessage) (*big.Int, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(v, &obj); err != nil {
		return nil, false
	}
	raw, ok := obj["i128"]
	if !ok {
		return nil, false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		// Could be a JSON number too — try that.
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, false
		}
		s = n.String()
	}
	n := new(big.Int)
	n, ok = n.SetString(s, 10)
	if !ok {
		return nil, false
	}
	return n, true
}

// ParseTokenEvent attempts to parse a store.Event as a SEP-41 token event.
// Returns nil when the event is not a recognised token event (silent skip).
func ParseTokenEvent(contractID, eventID string, ledger int64, network string, topics json.RawMessage, value json.RawMessage) *TokenEvent {
	// Topics must be a JSON array with at least one element.
	var decoded []json.RawMessage
	if err := json.Unmarshal(topics, &decoded); err != nil || len(decoded) == 0 {
		return nil
	}

	// topic[0] must be a symbol naming the event.
	name, ok := scvSymbol(decoded[0])
	if !ok {
		return nil
	}
	kind, ok := allowedTokenEvents[strings.ToLower(name)]
	if !ok {
		return nil
	}

	amount, ok := scvAmount(value)
	if !ok || amount == nil {
		return nil
	}
	// Amount must be non-negative.
	if amount.Sign() < 0 {
		return nil
	}

	te := &TokenEvent{
		Kind:       kind,
		ContractID: contractID,
		Amount:     new(big.Int).Set(amount),
		EventID:    eventID,
		Ledger:     ledger,
		Network:    network,
	}

	switch kind {
	case TokenTransfer:
		// topics[1] = from, topics[2] = to
		if len(decoded) < 3 {
			return nil
		}
		from, ok := scvAddress(decoded[1])
		if !ok {
			return nil
		}
		to, ok := scvAddress(decoded[2])
		if !ok {
			return nil
		}
		if from == to {
			// Self-transfer: no net balance change.
			return nil
		}
		te.From = from
		te.To = to

	case TokenMint:
		// topics[1] = to
		if len(decoded) < 2 {
			return nil
		}
		to, ok := scvAddress(decoded[1])
		if !ok {
			return nil
		}
		te.To = to

	case TokenBurn:
		// topics[1] = from
		if len(decoded) < 2 {
			return nil
		}
		from, ok := scvAddress(decoded[1])
		if !ok {
			return nil
		}
		te.From = from

	case TokenClawback:
		// topics[1] = from
		if len(decoded) < 2 {
			return nil
		}
		from, ok := scvAddress(decoded[1])
		if !ok {
			return nil
		}
		te.From = from
	}

	return te
}
