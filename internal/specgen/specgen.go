// Package specgen renders the authored OpenAPI YAML into the JSON copy
// that internal/api embeds and serves at /openapi.json.
//
// The two files exist because the spec is authored as YAML — comments,
// block scalars, folded descriptions — while the binary has to hand a
// browser something Swagger UI can fetch without a YAML parser. Keeping the
// JSON generated rather than hand-maintained is what stops the two from
// drifting apart, and it is why this lives in a package rather than inside
// cmd/specgen: pkg/docs.TestSpecCopiesAreIdentical calls Render and fails
// the build when the committed JSON is not what the YAML produces.
package specgen

import (
	"bytes"
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Render converts an OpenAPI YAML document into the indented JSON form the
// repository commits. The output ends in a newline, so it can be compared
// byte-for-byte against the file on disk.
func Render(src []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return nil, fmt.Errorf("parsing spec: %w", err)
	}
	value, err := convert(&doc)
	if err != nil {
		return nil, fmt.Errorf("converting spec: %w", err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// A spec full of "<" in descriptions or "&" in URLs must survive the
	// round trip readably; the escaped forms are valid JSON but make the
	// generated file needlessly hard to review.
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		return nil, fmt.Errorf("encoding spec: %w", err)
	}
	return buf.Bytes(), nil
}

// orderedMap preserves a YAML mapping's authored key order through the JSON
// encoder. Decoding into map[string]any would sort the keys alphabetically
// and turn every regeneration into a whole-file diff.
type orderedMap struct {
	keys   []string
	values map[string]any
}

func (m *orderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range m.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := marshalValue(k)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		val, err := marshalValue(m.values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// marshalValue encodes one value with HTML escaping off, so nested values
// are written the same way the top-level encoder writes them.
func marshalValue(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// convert turns a yaml.Node tree into ordinary Go values, mapping YAML
// scalars onto the JSON types their tags imply.
func convert(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) != 1 {
			return nil, fmt.Errorf("expected exactly one document, got %d", len(n.Content))
		}
		return convert(n.Content[0])
	case yaml.MappingNode:
		m := &orderedMap{values: make(map[string]any, len(n.Content)/2)}
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, ok := scalarKey(n.Content[i])
			if !ok {
				return nil, fmt.Errorf("line %d: mapping keys must be scalars", n.Content[i].Line)
			}
			value, err := convert(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			if _, dup := m.values[key]; dup {
				return nil, fmt.Errorf("line %d: duplicate key %q", n.Content[i].Line, key)
			}
			m.keys = append(m.keys, key)
			m.values[key] = value
		}
		return m, nil
	case yaml.SequenceNode:
		// Non-nil so an empty YAML sequence renders as [] rather than null.
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			value, err := convert(c)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case yaml.AliasNode:
		return convert(n.Alias)
	case yaml.ScalarNode:
		return scalar(n)
	default:
		return nil, fmt.Errorf("line %d: unsupported YAML node kind %d", n.Line, n.Kind)
	}
}

// scalarKey renders a mapping key as the string JSON needs. Response codes
// are the reason this cannot just read n.Value: "200" quoted and 200 bare
// both have to come out as the string key "200".
func scalarKey(n *yaml.Node) (string, bool) {
	if n.Kind != yaml.ScalarNode {
		return "", false
	}
	return n.Value, true
}

func scalar(n *yaml.Node) (any, error) {
	switch n.Tag {
	case "!!null":
		return nil, nil
	case "!!bool":
		var b bool
		if err := n.Decode(&b); err != nil {
			return nil, err
		}
		return b, nil
	case "!!int":
		var i int64
		if err := n.Decode(&i); err != nil {
			return nil, err
		}
		return i, nil
	case "!!float":
		var f float64
		if err := n.Decode(&f); err != nil {
			return nil, err
		}
		return f, nil
	case "!!str":
		return n.Value, nil
	default:
		return nil, fmt.Errorf("line %d: unsupported scalar tag %q", n.Line, n.Tag)
	}
}
