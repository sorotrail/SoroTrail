package decode

import (
	"github.com/sorotrail/sorotrail/internal/spec"
	"fmt"
	"math/big"
)


// SpecDecodedEvent wraps structural event decoding with spec-derived field names and types.
type SpecDecodedEvent struct {
	Structural    interface{}            `json:"structural"`
	SpecFields    map[string]interface{} `json:"spec_fields,omitempty"`
	Mismatch      bool                   `json:"mismatch"`
	MismatchError string                 `json:"mismatch_error,omitempty"`
}

// DecodeEventWithSpec decodes an event payload using the provided contract spec while keeping structural decoding as truth.
func DecodeEventWithSpec(structural interface{}, eventSpec *spec.ContractSpec, eventName string) SpecDecodedEvent {
	result := SpecDecodedEvent{
		Structural: structural,
		SpecFields: make(map[string]interface{}),
	}

	if eventSpec == nil {
		return result
	}

	var targetEvent *spec.EventSpec
	for _, ev := range eventSpec.Events {
		if ev.Name == eventName {
			targetEvent = &ev
			break
		}
	}

	if targetEvent == nil && len(eventSpec.Events) > 0 {
		targetEvent = &eventSpec.Events[0]
	}

	if targetEvent == nil {
		return result
	}

	fields := targetEvent.TopicSpecs
	// Also include value spec if present
	if targetEvent.ValueSpec != nil {
		fields = append(fields, *targetEvent.ValueSpec)
	}

	switch st := structural.(type) {
	case map[string]interface{}:
		for i, f := range fields {
			mapKey := fmt.Sprintf("field_%d", i)
			if val, ok := st[mapKey]; ok {
				result.SpecFields[f.Name] = formatFieldValue(val, f.Type)
			} else if val, ok := st[f.Name]; ok {
				result.SpecFields[f.Name] = formatFieldValue(val, f.Type)
			}
		}
		if len(result.SpecFields) == 0 {
			for k, v := range st {
				result.SpecFields[k] = v
			}
		}
	case []interface{}:
		for i, v := range st {
			if i < len(fields) {
				f := fields[i]
				result.SpecFields[f.Name] = formatFieldValue(v, f.Type)
			} else {
				result.SpecFields[fmt.Sprintf("field_%d", i)] = v
			}
		}
		if len(st) != len(fields) && len(fields) > 0 {
			result.Mismatch = true
			result.MismatchError = fmt.Sprintf("field count mismatch: payload has %d items, spec has %d fields", len(st), len(fields))
		}
	default:
		if len(fields) > 0 {
			result.SpecFields[fields[0].Name] = formatFieldValue(structural, fields[0].Type)
		} else {
			result.SpecFields["value"] = structural
		}
	}

	return result
}

func formatFieldValue(val interface{}, typ string) interface{} {
	switch typ {
	case "i128", "u128", "i256", "u256":
		switch v := val.(type) {
		case string:
			return v
		case int, int64, uint64, float64:
			return fmt.Sprintf("%v", v)
		case *big.Int:
			return v.String()
		default:
			return fmt.Sprintf("%v", val)
		}
	default:
		return val
	}
}
// SpecDecoder provides helper utilities for decoding event payloads against a contract spec.
type SpecDecoder struct {
	spec *spec.ContractSpec
}

// NewSpecDecoder creates a new SpecDecoder instance.
func NewSpecDecoder(s *spec.ContractSpec) *SpecDecoder {
	return &SpecDecoder{spec: s}
}
