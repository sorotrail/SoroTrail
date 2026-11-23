package api

// Fuzz targets for the JSON request-body reader and decoder. Every write
// endpoint passes untrusted bytes through them, so they check what a handler
// relies on: an accepted body is exactly one JSON value that decodes to what
// the standard library decodes, a rejected body never leaks a half-filled
// destination, and the size limit is exact.

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type fuzzBody struct {
	Name  string   `json:"name"`
	Limit int      `json:"limit"`
	Tags  []string `json:"tags"`
}

func FuzzDecodeJSONBytes(f *testing.F) {
	f.Add(`{"name":"a","limit":3,"tags":["x","y"]}`)
	f.Add(`{"name":"a"}`)
	f.Add(`{}`)
	f.Add(``)
	f.Add(`null`)
	f.Add(`[]`)
	f.Add(`{"name":"a"} {"name":"b"}`)
	f.Add(`{"name":"a"}garbage`)
	f.Add(`{"name":"a","extra":1}`)
	f.Add(`{"NAME":"case-insensitive"}`)
	f.Add(`{"name":1}`)
	f.Add(`{"name":"ok","limit":"not a number"}`)
	f.Add(`{"name":"a","name":"b"}`)
	f.Add("{\"name\":\"\xff\"}")
	f.Add(`{"limit":99999999999999999999}`)

	f.Fuzz(func(t *testing.T, body string) {
		sentinel := fuzzBody{Name: "untouched", Limit: -1, Tags: []string{"keep"}}
		dst := sentinel

		err := decodeJSONBytes([]byte(body), &dst)
		if err != nil {
			if !reflect.DeepEqual(dst, sentinel) {
				t.Fatalf("a rejected body %q left the destination half-populated: %+v", body, dst)
			}
			return
		}

		// Accepted: the body is one JSON value (Unmarshal rejects trailing
		// data) and decodes to the same thing the standard library produces.
		var want fuzzBody
		if uerr := json.Unmarshal([]byte(body), &want); uerr != nil {
			t.Fatalf("decodeJSONBytes accepted %q but json.Unmarshal rejects it: %v", body, uerr)
		}
		if !reflect.DeepEqual(dst, want) {
			t.Fatalf("decodeJSONBytes(%q) = %+v, json.Unmarshal = %+v", body, dst, want)
		}

		// Unknown fields must have been rejected, so every key is a known one.
		var keys map[string]json.RawMessage
		if json.Unmarshal([]byte(body), &keys) == nil {
			for k := range keys {
				switch strings.ToLower(k) {
				case "name", "limit", "tags":
				default:
					t.Fatalf("decodeJSONBytes accepted the unknown field %q in %q", k, body)
				}
			}
		}
	})
}

func FuzzReadJSONBody(f *testing.F) {
	f.Add([]byte(`{"a":1}`), int64(100))
	f.Add([]byte(`{"a":1}`), int64(7))
	f.Add([]byte(`{"a":1}`), int64(6))
	f.Add([]byte(`{"a":1}`), int64(0))
	f.Add([]byte(`{"a":1}`), int64(-5))
	f.Add([]byte(``), int64(100))
	f.Add([]byte(" \n\t "), int64(100))
	f.Add(bytes.Repeat([]byte("a"), 300), int64(256))

	f.Fuzz(func(t *testing.T, body []byte, limit int64) {
		// Keep the limit in a range where the inputs can actually straddle it.
		limit %= 1 << 12

		req, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		got, err := readJSONBody(req, limit)

		blank := len(bytes.TrimSpace(body)) == 0
		tooLarge := limit > 0 && int64(len(body)) > limit

		switch {
		case tooLarge:
			if !errors.Is(err, errBodyTooLarge) {
				t.Fatalf("a %d-byte body over a %d-byte limit gave %v, want errBodyTooLarge", len(body), limit, err)
			}
		case blank:
			if err == nil {
				t.Fatalf("a blank body %q was accepted", body)
			}
		default:
			if err != nil {
				t.Fatalf("a %d-byte body within limit %d was rejected: %v", len(body), limit, err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("readJSONBody changed the body: %q -> %q", body, got)
			}
			return
		}
		if got != nil {
			t.Fatalf("a rejected body must come back nil, got %q", got)
		}
	})
}
