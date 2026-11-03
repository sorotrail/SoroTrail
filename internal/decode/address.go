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
	for i := 1; i < 56; i++ {
		r := s[i]
		if (r < 'A' || r > 'Z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}

func isAddressBytes(b []byte) bool {
	if len(b) != 56 {
		return false
	}
	prefix := b[0]
	if prefix != 'G' && prefix != 'C' {
		return false
	}
	for i := 1; i < 56; i++ {
		r := b[i]
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
	return AppendAddresses(nil, "", topics, value)
}

// AppendAddresses appends every deduplicated (address, role) occurrence from
// topics and value onto dst, tagging each appended AddressRef with eventID.
// Using AppendAddresses across a batch of events allows the caller to reuse or
// pre-size a single destination slice rather than allocating one slice per event.
func AppendAddresses(dst []AddressRef, eventID string, topics, value json.RawMessage) []AddressRef {
	// Fast check: a quoted 56-character Stellar address ("G..." or "C...")
	// requires at least 58 bytes of JSON.
	if len(topics) < 58 && len(value) < 58 {
		return dst
	}

	var seenSmall [8][2]string
	seenCount := 0
	var seenMap map[[2]string]bool

	add := func(addr, role string) {
		if !isAddress(addr) {
			return
		}
		key := [2]string{addr, role}
		if seenMap != nil {
			if seenMap[key] {
				return
			}
			seenMap[key] = true
		} else {
			for i := 0; i < seenCount; i++ {
				if seenSmall[i] == key {
					return
				}
			}
			if seenCount < len(seenSmall) {
				seenSmall[seenCount] = key
				seenCount++
			} else {
				seenMap = make(map[[2]string]bool, 16)
				for i := 0; i < seenCount; i++ {
					seenMap[seenSmall[i]] = true
				}
				seenMap[key] = true
			}
		}
		if cap(dst) == 0 {
			dst = make([]AddressRef, 0, 4)
		}
		dst = append(dst, AddressRef{Address: addr, Role: role, EventID: eventID})
	}

	if len(topics) >= 58 {
		walkTopLevelTopics(topics, add)
	}
	if len(value) >= 58 {
		extractFromValue(value, RoleValue, add, 0)
	}
	return dst
}

func walkTopLevelTopics(topics json.RawMessage, add func(string, string)) {
	i := skipWS(topics, 0)
	if i >= len(topics) || topics[i] != '[' {
		return
	}
	i++
	topicIdx := 0
	for {
		i = skipWS(topics, i)
		if i >= len(topics) || topics[i] == ']' {
			return
		}
		elemEnd, ok := scanJSONValueEnd(topics, i)
		if !ok {
			return
		}
		elem := topics[i:elemEnd]
		if len(elem) >= 58 {
			extractFromValue(elem, formatTopicRole(topicIdx), add, 0)
		}
		topicIdx++
		i = skipWS(topics, elemEnd)
		if i < len(topics) && topics[i] == ',' {
			i++
		}
	}
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
	if depth > maxExtractDepth {
		return
	}
	i := skipWS(raw, 0)
	if len(raw)-i < 58 {
		return
	}
	raw = raw[i:]

	switch raw[0] {
	case '"':
		end, hasEscape, ok := scanJSONStringEnd(raw, 0)
		if !ok {
			return
		}
		if !hasEscape {
			if end == 58 && isAddressBytes(raw[1:57]) {
				add(string(raw[1:57]), role)
			}
			return
		}
		var s string
		if err := json.Unmarshal(raw[:end], &s); err == nil {
			add(s, role)
		}

	case '[':
		pos := 1
		for {
			pos = skipWS(raw, pos)
			if pos >= len(raw) || raw[pos] == ']' {
				return
			}
			elemEnd, ok := scanJSONValueEnd(raw, pos)
			if !ok {
				return
			}
			if elemEnd-pos >= 58 {
				extractFromValue(raw[pos:elemEnd], role, add, depth+1)
			}
			pos = skipWS(raw, elemEnd)
			if pos < len(raw) && raw[pos] == ',' {
				pos++
			}
		}

	case '{':
		// Walk object key-value pairs. If the object is a single-key
		// {"unknown": ...} fallback shape, skip it entirely.
		pos := skipWS(raw, 1)
		if pos >= len(raw) || raw[pos] == '}' {
			return
		}
		firstKeyStart := pos
		firstKeyEnd, firstKeyEsc, ok := scanJSONStringEnd(raw, firstKeyStart)
		if !ok {
			return
		}
		pos = skipWS(raw, firstKeyEnd)
		if pos >= len(raw) || raw[pos] != ':' {
			return
		}
		pos = skipWS(raw, pos+1)
		firstValStart := pos
		firstValEnd, ok := scanJSONValueEnd(raw, firstValStart)
		if !ok {
			return
		}
		afterFirst := skipWS(raw, firstValEnd)
		isUnknownFirstKey := !firstKeyEsc && string(raw[firstKeyStart+1:firstKeyEnd-1]) == "unknown"
		if isUnknownFirstKey && afterFirst < len(raw) && raw[afterFirst] == '}' {
			return
		}
		if firstValEnd-firstValStart >= 58 {
			extractFromValue(raw[firstValStart:firstValEnd], role, add, depth+1)
		}
		pos = afterFirst
		for pos < len(raw) && raw[pos] == ',' {
			pos = skipWS(raw, pos+1)
			keyEnd, _, ok := scanJSONStringEnd(raw, pos)
			if !ok {
				return
			}
			pos = skipWS(raw, keyEnd)
			if pos >= len(raw) || raw[pos] != ':' {
				return
			}
			pos = skipWS(raw, pos+1)
			valStart := pos
			valEnd, ok := scanJSONValueEnd(raw, valStart)
			if !ok {
				return
			}
			if valEnd-valStart >= 58 {
				extractFromValue(raw[valStart:valEnd], role, add, depth+1)
			}
			pos = skipWS(raw, valEnd)
		}
	}
}

func skipWS(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func scanJSONStringEnd(b []byte, start int) (end int, hasEscape bool, ok bool) {
	if start >= len(b) || b[start] != '"' {
		return 0, false, false
	}
	for i := start + 1; i < len(b); i++ {
		switch b[i] {
		case '\\':
			hasEscape = true
			i++
		case '"':
			return i + 1, hasEscape, true
		}
	}
	return 0, hasEscape, false
}

func scanJSONValueEnd(b []byte, start int) (int, bool) {
	if start >= len(b) {
		return 0, false
	}
	switch b[start] {
	case '"':
		end, _, ok := scanJSONStringEnd(b, start)
		return end, ok
	case '{', '[':
		depth := 0
		for i := start; i < len(b); i++ {
			switch b[i] {
			case '"':
				strEnd, _, ok := scanJSONStringEnd(b, i)
				if !ok {
					return 0, false
				}
				i = strEnd - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	default:
		i := start
		for i < len(b) {
			switch b[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i, i > start
			}
			i++
		}
		return i, i > start
	}
}

// Role constants.
const (
	RoleValue = "value"
	RoleTopic = "topic"

	RoleTopicPrefix = "topic[" // suffix is the 0-based index, e.g. "topic[0]"
)

var precomputedTopicRoles = [16]string{
	"topic[0]", "topic[1]", "topic[2]", "topic[3]",
	"topic[4]", "topic[5]", "topic[6]", "topic[7]",
	"topic[8]", "topic[9]", "topic[10]", "topic[11]",
	"topic[12]", "topic[13]", "topic[14]", "topic[15]",
}

func formatTopicRole(i int) string {
	if i >= 0 && i < len(precomputedTopicRoles) {
		return precomputedTopicRoles[i]
	}
	var buf strings.Builder
	buf.Grow(12)
	buf.WriteString(RoleTopicPrefix)
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
