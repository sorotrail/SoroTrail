package decode

import (
	"encoding/json"
	"strings"
)

// AddressRef records one address occurrence in an event.
type AddressRef struct {
	Address string `json:"address"`
	// Role describes where the address appeared. Possible values:
	// "topic[N]" when it was the Nth (0-based) topic entry,
	// "value" when found somewhere in the value body,
	// "topic" when found in a topic but the exact index is unknown.
	Role string `json:"role"`
	// EventID is the TOID of the owning event. Populated by the caller.
	EventID string `json:"-"`
}

// isAddress reports whether s looks like a Stellar strkey (G... or C...).
// This checks shape only (prefix, length, base32 alphabet), not the checksum.
func isAddress(s string) bool {
	if len(s) != 56 {
		return false
	}
	prefix := s[0]
	if prefix != 'G' && prefix != 'C' {
		return false
	}
	for _, r := range s[1:] {
		if (r < 'A' || r > 'Z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}

// ExtractAddresses walks the decoded JSON topics and value of an event and
// returns every G... or C... address found, deduplicated per (address, role).
//
// It operates on decoded JSON (the output of EventTopicsValue / the decoder),
// not raw XDR. This respects the layering: decode produces JSON, and address
// extraction is a consumer of that JSON.
//
// Addresses inside {"unknown": ...} fallback shapes are deliberately skipped:
// the unknown shape represents a decoding failure, and extracting addresses
// from it would be unreliable (the field may not even be an address).
//
// The same address appearing in both a topic and the value produces two
// AddressRef rows (different roles), but duplicate (address, role) pairs are
// collapsed — each unique combination appears at most once.
func ExtractAddresses(topics, value json.RawMessage) []AddressRef {
	seen := map[[2]string]bool{} // (address, role) dedupe key
	var result []AddressRef

	add := func(addr, role string) {
		if !isAddress(addr) {
			return
		}
		key := [2]string{addr, role}
		if seen[key] {
			return
		}
		seen[key] = true
		result = append(result, AddressRef{Address: addr, Role: role})
	}

	// Walk topic array.
	if len(topics) > 0 {
		var topicArr []json.RawMessage
		if err := json.Unmarshal(topics, &topicArr); err == nil {
			for i, t := range topicArr {
				role := formatTopicRole(i)
				extractFromValue(t, role, add, 0)
			}
		}
	}

	// Walk value.
	if len(value) > 0 {
		extractFromValue(value, "value", add, 0)
	}

	return result
}

// extractFromValue recursively walks a decoded JSON value looking for string
// values that look like addresses. It calls add for each one found with the
// given role prefix.
//
// Depth is capped at maxExtractDepth to avoid pathological nesting blowing
// the stack (JSON depth is bounded by the Go decoder, but we cap anyway).
// Objects and arrays are recursed into; {"unknown": ...} shapes are skipped.
func extractFromValue(raw json.RawMessage, role string, add func(string, string), depth int) {
	const maxExtractDepth = 32
	if depth > maxExtractDepth || len(raw) == 0 {
		return
	}

	// Try object first.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil {
		// Skip {"unknown": ...} shapes — these are decoding fallbacks
		// and the value inside is not reliably an address.
		if _, ok := obj["unknown"]; ok && len(obj) == 1 {
			return
		}
		for _, v := range obj {
			extractFromValue(v, role, add, depth+1)
		}
		return
	}

	// Try array.
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err == nil {
		for _, v := range arr {
			extractFromValue(v, role, add, depth+1)
		}
		return
	}

	// Try string.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		add(s, role)
		return
	}
	// Numbers, booleans, null — no addresses here.
}

// Role constants.
const (
	RoleValue = "value"
	RoleTopic = "topic"

	RoleTopicPrefix = "topic[" // suffix is the 0-based index, e.g. "topic[0]"
)

func formatTopicRole(i int) string {
	var buf strings.Builder
	buf.Grow(12)
	buf.WriteString(RoleTopicPrefix)
	// i fits in a small int; Sprint is fine.
	buf.WriteString(itoa(i))
	buf.WriteByte(']')
	return buf.String()
}

// itoa is a tiny int→string for 0–99, which is the only range we use.
func itoa(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
