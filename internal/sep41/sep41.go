// Package sep41 recognizes Soroban SEP-41 token contract events and
// emits a consumer-friendly normalized shape on top of the existing
// decoder's output.
//
// SoroTrail stores events as raw topic/value pairs decoded by
// internal/decode (e.g. [{"symbol":"transfer"},{"address":"G..."},{"address":"G..."}]
// and {"i128":"10000000"}). Lossless, but every consumer would otherwise
// have to re-implement SEP-41 / CAP-46-6 semantics themselves. This
// package layers a strict — never destructive — filter on top of that
// output: matching events get a normalized envelope, non-matching ones
// pass through untouched.
//
// Design notes that matter when extending this:
//
//   - The package never widens internal/decode. It reads what the
//     decoder produced and turns it into a typed envelope; anything
//     that doesn't quite fit a SEP-41 shape returns nil, so unrecognized
//     events are still served in their original form.
//   - Matching is conservative. A "transfer" symbol with the wrong
//     arity or a non-address topic position is NOT a SEP-41 event.
//   - Amounts are i128 and stay as decimal strings — never floats,
//     so callers preserve full precision.
//   - CAP-0067 evolution is supported: a trailing SEP-0011 asset
//     string topic is captured in the optional `asset` field. Approve
//     events reject this trailing topic because CAP-0067 never appends
//     one to approve.
//   - Muxed transfers (CAP-0067) emit a `to_muxed_id` number from the
//     data map, alongside the i128 amount.
//   - Contract addresses (C...) flow through `address` the same way
//     account addresses (G...) do; the type is preserved.
package sep41

import (
	"encoding/json"
	"strings"
)

// Standard is the value reported in Normalized.Standard. Exported so the
// API layer can compare without re-typing the literal.
const Standard = "sep41"

// Normalized is the SEP-41 normalized shape added on top of matching
// events. Field presence mirrors the SEP-41 / CAP-46-6 event semantics:
//   - transfer:  From, To, Amount, optional ToMuxedID, optional Asset
//   - mint:      To, Amount, optional Asset
//   - burn:      From, Amount, optional Asset
//   - clawback:  From, Amount, optional Asset
//   - approve:   From, Spender, Amount, ExpirationLedger
//
// Amounts are i128 rendered as decimal strings — never floats.
type Normalized struct {
	Standard         string  `json:"standard"`
	Event            string  `json:"event"`
	From             string  `json:"from,omitempty"`
	To               string  `json:"to,omitempty"`
	Spender          string  `json:"spender,omitempty"`
	Amount           string  `json:"amount"`
	ExpirationLedger uint32  `json:"expiration_ledger,omitempty"`
	Asset            string  `json:"asset,omitempty"`
	ToMuxedID        *uint64 `json:"to_muxed_id,omitempty"`
}

// txVal is the tagged-shape representation the existing decoder emits.
// All fields are pointers so a non-fitting event silently fails the match
// instead of panicking or being misread; a logged field that doesn't
// belong simply stays nil.
type txVal struct {
	Symbol  *string    `json:"symbol,omitempty"`
	Address *string    `json:"address,omitempty"`
	String  *string    `json:"string,omitempty"`
	I128    *string    `json:"i128,omitempty"`
	U32     *uint32    `json:"u32,omitempty"`
	U64     *uint64    `json:"u64,omitempty"`
	Vec     []txVal    `json:"vec,omitempty"`
	Map     []mapEntry `json:"map,omitempty"`
}

type mapEntry struct {
	Key txVal `json:"key"`
	Val txVal `json:"val"`
}

// Decode inspects a decoded event against the SEP-41 event shapes
// (transfer/mint/burn/clawback/approve) and returns the normalized view
// when the match is exact. Non-matches return nil; this is a strict
// filter — a "transfer" symbol with the wrong arity or a non-address
// topic position is not a SEP-41 event and the caller must pass it
// through untouched.
//
// The two arguments are the topic array and value already produced by
// internal/decode — the package deliberately does not require access
// to raw XDR or RPC responses, so it can be applied at API / webhook
// render time without an extra decode pass.
func Decode(topics, value json.RawMessage) *Normalized {
	if string(value) == "null" {
		// SEP-41 events always carry an amount (or amount+expiration for
		// approve). Null value → not SEP-41.
		return nil
	}
	var ts []txVal
	if err := json.Unmarshal(topics, &ts); err != nil || len(ts) < 2 {
		return nil
	}
	if ts[0].Symbol == nil {
		// topic[0] must be the event symbol — a bare address is not an
		// event name.
		return nil
	}
	var v txVal
	if err := json.Unmarshal(value, &v); err != nil {
		return nil
	}

	// CAP-0067 trailing asset topic. The last topic is recognized as an
	// asset only if it's a string-shaped value matching a SEP-0011
	// asset representation; unknown trailing strings are left in place
	// and break the arity check that follows — keeping the filter strict.
	var asset string
	if last := ts[len(ts)-1]; last.String != nil && looksLikeAsset(*last.String) {
		asset = *last.String
		ts = ts[:len(ts)-1]
	}

	switch *ts[0].Symbol {
	case "transfer":
		// Topics after optional asset stripping: [transfer, from, to].
		if len(ts) != 3 || ts[1].Address == nil || ts[2].Address == nil {
			return nil
		}
		amount, muxID := parseTransferValue(v)
		if amount == "" {
			return nil
		}
		return &Normalized{
			Standard:  Standard,
			Event:     "transfer",
			From:      *ts[1].Address,
			To:        *ts[2].Address,
			Amount:    amount,
			ToMuxedID: muxID,
			Asset:     asset,
		}
	case "mint":
		if len(ts) != 2 || ts[1].Address == nil || v.I128 == nil {
			return nil
		}
		return &Normalized{
			Standard: Standard,
			Event:    "mint",
			To:       *ts[1].Address,
			Amount:   *v.I128,
			Asset:    asset,
		}
	case "burn", "clawback":
		if len(ts) != 2 || ts[1].Address == nil || v.I128 == nil {
			return nil
		}
		return &Normalized{
			Standard: Standard,
			Event:    *ts[0].Symbol,
			From:     *ts[1].Address,
			Amount:   *v.I128,
			Asset:    asset,
		}
	case "approve":
		// Approve MUST NOT have a trailing SEP-0011 asset topic in
		// CAP-0067 — if one was stripped above, this is not approve.
		if asset != "" {
			return nil
		}
		if len(ts) != 3 || ts[1].Address == nil || ts[2].Address == nil {
			return nil
		}
		if len(v.Vec) != 2 || v.Vec[0].I128 == nil || v.Vec[1].U32 == nil {
			return nil
		}
		return &Normalized{
			Standard:         Standard,
			Event:            "approve",
			From:             *ts[1].Address,
			Spender:          *ts[2].Address,
			Amount:           *v.Vec[0].I128,
			ExpirationLedger: *v.Vec[1].U32,
		}
	default:
		return nil
	}
}

// Normalize calls Decode and renders the result as json.RawMessage, or
// returns nil for non-matches. Callers can rely on a single nil check:
// nil means "this event is not SEP-41", a non-nil value carries the
// normalized envelope ready to embed in a response.
func Normalize(topics, value json.RawMessage) json.RawMessage {
	n := Decode(topics, value)
	if n == nil {
		return nil
	}
	b, _ := json.Marshal(n)
	return b
}

// parseTransferValue accepts either the basic i128 amount or the
// CAP-0067 muxed data map ({"amount":i128, "to_muxed_id":u64}). Returns
// "" when the value shape is neither, so the caller can reject the
// event as not-SEP-41.
func parseTransferValue(v txVal) (amount string, muxID *uint64) {
	if v.I128 != nil {
		return *v.I128, nil
	}
	// Map order is non-deterministic on the wire — match by key, not
	// position.
	var gotAmount bool
	for _, e := range v.Map {
		if e.Key.Symbol == nil {
			continue
		}
		switch *e.Key.Symbol {
		case "amount":
			if e.Val.I128 == nil {
				return "", nil
			}
			amount = *e.Val.I128
			gotAmount = true
		case "to_muxed_id":
			if e.Val.U64 == nil {
				// Map declared a muxed id but didn't carry a u64 — not
				// a valid CAP-0067 muxed transfer; reject.
				return "", nil
			}
			v := *e.Val.U64
			muxID = &v
		}
	}
	if !gotAmount {
		return "", nil
	}
	return amount, muxID
}

// looksLikeAsset accepts the canonical SEP-0011 asset representations.
// "native" matches XLM; otherwise "<CODE>:<ISSUER>" with CODE 1–12 ASCII
// alphanumeric characters and ISSUER a 56-character Stellar account id
// (G…) — anonymous strings like "memo" or random text won't be stripped,
// so a stray trailing topic keeps the strict-arity check failing.
func looksLikeAsset(s string) bool {
	if s == "native" {
		return true
	}
	colon := strings.IndexByte(s, ':')
	if colon <= 0 {
		return false
	}
	code := s[:colon]
	if len(code) == 0 || len(code) > 12 {
		return false
	}
	for i := 0; i < len(code); i++ {
		c := code[i]
		// De Morgan'd: c is non-alphanumeric iff it's outside every
		// alphanumeric range. Easier to read than the negated disjunction.
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	issuer := s[colon+1:]
	return len(issuer) == 56 && issuer[0] == 'G'
}
