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
	if _, isXDR := d.(XDRDecoder); isXDR && e.TopicJSON == nil {
		return decodeXDROnlyEvent(e)
	}

	switch {
	case e.TopicJSON != nil:
		topics, err = marshalTopicJSONSlice(e.TopicJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("re-marshaling topics: %w", err)
		}
	default:
		// Assemble the JSON array directly in a single pre-sized buffer rather
		// than allocating []json.RawMessage and calling json.Marshal.
		buf := make([]byte, 0, max(2, len(e.Topic)*64))
		buf = append(buf, '[')
		written := 0
		for _, t := range e.Topic {
			v, decErr := d.DecodeScVal(t)
			if decErr != nil {
				// If the Decoder returns an error, skip this topic
				// rather than failing the whole event. DecodeScVal has
				// already logged and counted the failure; we just
				// don't include the un-decodable topic in the output.
				continue
			}
			if written > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, v...)
			written++
		}
		buf = append(buf, ']')
		topics = json.RawMessage(buf)
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

// decodeXDROnlyEvent decodes an event's topics and value using a single pooled
// decodeScratch so that all topics and the value are rendered without per-topic
// slice allocations, copying out two freshly owned slices at the end.
func decodeXDROnlyEvent(e rpc.Event) (topics, value json.RawMessage, err error) {
	sc := decodeScratchPool.Get().(*decodeScratch)
	defer decodeScratchPool.Put(sc)

	sc.jsonBuf = sc.jsonBuf[:0]
	sc.jsonBuf = append(sc.jsonBuf, '[')
	written := 0
	for _, t := range e.Topic {
		before := len(sc.jsonBuf)
		if written > 0 {
			sc.jsonBuf = append(sc.jsonBuf, ',')
		}
		out, decErr := sc.decodeScValAppend(sc.jsonBuf, t)
		if decErr != nil {
			sc.jsonBuf = sc.jsonBuf[:before]
			continue
		}
		sc.jsonBuf = out
		written++
	}
	sc.jsonBuf = append(sc.jsonBuf, ']')
	topics = make(json.RawMessage, len(sc.jsonBuf))
	copy(topics, sc.jsonBuf)

	switch {
	case e.ValueJSON != nil:
		value = e.ValueJSON
	case e.Value != "":
		sc.jsonBuf = sc.jsonBuf[:0]
		out, decErr := sc.decodeScValAppend(sc.jsonBuf, e.Value)
		if decErr != nil {
			return nil, nil, fmt.Errorf("decoding value: %w", decErr)
		}
		sc.jsonBuf = out
		value = make(json.RawMessage, len(out))
		copy(value, out)
	default:
		value = json.RawMessage("null")
	}
	return topics, value, nil
}

func marshalTopicJSONSlice(topicJSON []json.RawMessage) (json.RawMessage, error) {
	if len(topicJSON) == 0 {
		return json.RawMessage("[]"), nil
	}
	totalLen := 2 + len(topicJSON) - 1
	for _, raw := range topicJSON {
		if len(raw) == 0 || !isCompactValidSimpleJSON(raw) {
			return json.Marshal(topicJSON)
		}
		totalLen += len(raw)
	}
	out := make(json.RawMessage, 0, totalLen)
	out = append(out, '[')
	for i, raw := range topicJSON {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, raw...)
	}
	out = append(out, ']')
	return out, nil
}

// isCompactValidSimpleJSON reports whether raw is already a compact, valid
// JSON value without unquoted whitespace that json.Marshal would strip.
func isCompactValidSimpleJSON(raw []byte) bool {
	inString := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if inString {
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case ' ', '\t', '\n', '\r':
			return false
		}
	}
	return !inString && json.Valid(raw)
}
