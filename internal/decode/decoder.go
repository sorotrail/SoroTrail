// Package decode turns Soroban ScVal payloads into queryable JSON.
//
// Two shapes arrive from the RPC: when the server supports xdrFormat "json",
// topics/values are already JSON and pass through untouched; otherwise they
// are base64-encoded XDR and are decoded locally via the Decoder.
//
// contributors: this package is deliberately a thin, replaceable seam. Richer
// decoding — e.g. recognizing SEP-41 token transfer events and emitting
// normalized shapes — should be built as a new Decoder implementation or a
// layer on top, not by widening this interface.
package decode

import (
	"encoding/json"
	"fmt"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// Decoder converts a single base64-encoded XDR ScVal into JSON.
type Decoder interface {
	DecodeScVal(base64XDR string) (json.RawMessage, error)
}

// EventTopicsValue extracts an event's topics (as a JSON array) and value
// (as a JSON value), preferring the server-decoded JSON fields and falling
// back to local XDR decoding via d.
func EventTopicsValue(d Decoder, e rpc.Event) (topics, value json.RawMessage, err error) {
	switch {
	case e.TopicJSON != nil:
		topics, err = json.Marshal(e.TopicJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("re-marshaling topics: %w", err)
		}
	default:
		decoded := make([]json.RawMessage, 0, len(e.Topic))
		for i, t := range e.Topic {
			v, err := d.DecodeScVal(t)
			if err != nil {
				return nil, nil, fmt.Errorf("decoding topic %d: %w", i, err)
			}
			decoded = append(decoded, v)
		}
		topics, err = json.Marshal(decoded)
		if err != nil {
			return nil, nil, fmt.Errorf("marshaling topics: %w", err)
		}
	}

	switch {
	case e.ValueJSON != nil:
		value = e.ValueJSON
	case e.Value != "":
		value, err = d.DecodeScVal(e.Value)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding value: %w", err)
		}
	default:
		value = json.RawMessage("null")
	}
	return topics, value, nil
}
