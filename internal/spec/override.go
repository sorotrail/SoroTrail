package spec

import (
	"encoding/json"
	"fmt"
)

// ParseOverrideSpec parses and validates a user-supplied spec JSON intended
// to override the spec fetched from the RPC for a given contract.
//
// The accepted shape mirrors ContractSpec:
//
//	{
//	  "events": [
//	    {
//	      "name": "transfer",                                 // required, non-empty
//	      "doc": "optional documentation",                     // optional
//	      "topic_specs": [ {"name":"to","type":"address"} ],   // optional
//	      "value_spec":  {"name":"amount","type":"i128"}       // optional
//	    }
//	  ]
//	}
//
// Validation rules:
//   - the payload must be a valid JSON object;
//   - "events" is required and must be an array (possibly empty);
//   - every event entry must be a JSON object with a non-empty string "name";
//   - duplicate event names are rejected;
//   - every field spec (in topic_specs and value_spec) must be a JSON object
//     with non-empty string "name" and "type".
//
// Invalid specs are rejected with an error; the returned ContractSpec is only
// usable when err == nil.
func ParseOverrideSpec(data []byte) (*ContractSpec, error) {
	var raw struct {
		Events []struct {
			Name       string       `json:"name"`
			Doc        string       `json:"doc"`
			TopicSpecs []FieldSpec  `json:"topic_specs"`
			ValueSpec  *FieldSpec   `json:"value_spec"`
		} `json:"events"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing override spec JSON: %w", err)
	}
	if raw.Events == nil {
		return nil, fmt.Errorf("override spec: \"events\" array is required")
	}

	spec := &ContractSpec{Events: make([]EventSpec, 0, len(raw.Events))}
	seen := make(map[string]struct{}, len(raw.Events))

	for i, ev := range raw.Events {
		if ev.Name == "" {
			return nil, fmt.Errorf("override spec: event #%d: \"name\" is required and must be a non-empty string", i+1)
		}
		if _, dup := seen[ev.Name]; dup {
			return nil, fmt.Errorf("override spec: duplicate event name %q", ev.Name)
		}
		seen[ev.Name] = struct{}{}

		out := EventSpec{
			Name: ev.Name,
			Doc:  ev.Doc,
		}
		for j, fs := range ev.TopicSpecs {
			if err := validateFieldSpec(fs, fmt.Sprintf("event %q: topic_specs[%d]", ev.Name, j)); err != nil {
				return nil, err
			}
			out.TopicSpecs = append(out.TopicSpecs, fs)
		}
		if ev.ValueSpec != nil {
			if err := validateFieldSpec(*ev.ValueSpec, fmt.Sprintf("event %q: value_spec", ev.Name)); err != nil {
				return nil, err
			}
			vs := *ev.ValueSpec
			out.ValueSpec = &vs
		}
		spec.Events = append(spec.Events, out)
	}

	return spec, nil
}

func validateFieldSpec(fs FieldSpec, where string) error {
	if fs.Name == "" {
		return fmt.Errorf("override spec: %s: \"name\" is required and must be a non-empty string", where)
	}
	if fs.Type == "" {
		return fmt.Errorf("override spec: %s: \"type\" is required and must be a non-empty string", where)
	}
	return nil
}
