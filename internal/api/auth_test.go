package api

import (
	"strings"
	"testing"
)

func TestGenerateAPIKey(t *testing.T) {
	tests := []struct {
		name     string
		validate func(t *testing.T, plaintext, prefix string, digest []byte, err error)
	}{
		{
			name: "happy path and format validation",
			validate: func(t *testing.T, plaintext, prefix string, digest []byte, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.HasPrefix(plaintext, "st_") {
					t.Errorf("expected plaintext to start with 'st_', got %q", plaintext)
				}
				if len(prefix) != keyPrefixLen {
					t.Errorf("expected prefix length %d, got %d (%q)", keyPrefixLen, len(prefix), prefix)
				}
				if len(digest) != 32 {
					t.Errorf("expected SHA-256 digest length 32, got %d", len(digest))
				}
				pfx, dig, ok := parseAPIKey(plaintext)
				if !ok {
					t.Errorf("failed to parse generated API key %q", plaintext)
				}
				if pfx != prefix {
					t.Errorf("parsed prefix %q does not match generated prefix %q", pfx, prefix)
				}
				if len(dig) != len(digest) {
					t.Errorf("parsed digest length mismatch")
				}
			},
		},
		{
			name: "parseAPIKey invalid empty input",
			validate: func(t *testing.T, _, _ string, _ []byte, _ error) {
				_, _, ok := parseAPIKey("")
				if ok {
					t.Error("expected empty API key to fail parsing")
				}
			},
		},
		{
			name: "parseAPIKey malformed inputs boundary checks",
			validate: func(t *testing.T, _, _ string, _ []byte, _ error) {
				malformed := []string{
					"st_",
					"st_short",
					"wrongscheme_1234567890123456_secret",
					"st__secret",
					"st_1234567890123456_",
					"st_toolongprefix12345_secret",
				}
				for _, m := range malformed {
					if _, _, ok := parseAPIKey(m); ok {
						t.Errorf("expected malformed API key %q to fail parsing", m)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plaintext, prefix, digest, err := GenerateAPIKey()
			tt.validate(t, plaintext, prefix, digest, err)
		})
	}
}
