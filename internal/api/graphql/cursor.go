// Package graphql implements SoroTrail's read-only GraphQL API.
//
// The implementation runs gqlgen's tooling chain at build time via the
// gqlgen.yml config, plus a hand-written resolver layer (resolver.go)
// that delegates to the shared queries package — every event-filter
// / pagination rule lives in internal/api/queries and is shared between
// REST and GraphQL transports.
//
// Limits (enforced at compile time by extension.FixedComplexityLimit
// and at runtime by the limits.go depth/complexity guards):
//   - Query depth:    10   (hard cap; do not raise without rebaselining)
//   - Query complexity: 1000 (hard cap)
//
// Both limits are documented in the README.
package graphql

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// cursorPayload is the JSON we encode into the opaque cursor token. The
// store cursor (LastID) is already opaque to clients, but we layer the
// order metadata so a client replaying a stale cursor against a
// different ordering gets a clear validation failure rather than silent
// data corruption.
type cursorPayload struct {
	LastID  string `json:"id"`
	OrderBy string `json:"order_by,omitempty"`
	Order   string `json:"order,omitempty"`
}

// EncodeCursor produces a base64-encoded JSON object holding the keyset
// continuation state. The output is opaque to clients — they MUST treat
// it as a string and pass it back unchanged as `after:`.
func EncodeCursor(lastID, orderBy, order string) string {
	if lastID == "" {
		return ""
	}
	p := cursorPayload{LastID: lastID, OrderBy: orderBy, Order: order}
	raw, _ := json.Marshal(p)
	return base64.StdEncoding.EncodeToString(raw)
}

// DecodeCursor parses a base64 cursor back into the keyset state. An
// empty cursor is a no-op; any decode failure is reported so the
// resolver can surface it as a 400 (REST) / GraphQL error.
func DecodeCursor(s string) (cursorPayload, error) {
	if s == "" {
		return cursorPayload{}, nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return cursorPayload{}, fmt.Errorf("invalid cursor: not base64")
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return cursorPayload{}, fmt.Errorf("invalid cursor: not JSON")
	}
	if p.LastID == "" {
		return cursorPayload{}, fmt.Errorf("invalid cursor: missing id")
	}
	return p, nil
}
