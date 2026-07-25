package plugins

import (
	"testing"
)

// ---------------------------------------------------------------
// Pure-Go unit tests (no wazero runtime, no _test fixture files).
// ---------------------------------------------------------------

func TestEventSymbolFromTopics(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no array", `{"symbol":"x"}`, ""},
		{"plain string", `["transfer"]`, ""},
		{"symbol object", `[{"symbol":"swap"}]`, "swap"},
		{"symbol object with comma", `[{"symbol":"swap"}, {"address":"C..."}]`, "swap"},
		{"missing symbol key", `[{"address":"C..."}]`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventSymbolFromTopics([]byte(tt.in))
			if got != tt.want {
				t.Errorf("EventSymbolFromTopics(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestClaimsMatches(t *testing.T) {
	c := Claims{Contracts: []string{"CA"}, Topics: []string{"swap"}}
	if !c.matches("CA", "swap") {
		t.Error("expected match on (CA, swap)")
	}
	if c.matches("CB", "swap") {
		t.Error("did not expect match on (CB, swap)")
	}
	if c.matches("CA", "transfer") {
		t.Error("did not expect match on (CA, transfer)")
	}
	wildcard := Claims{Contracts: nil, Topics: nil}
	if !wildcard.matches("CX", "anything") {
		t.Error("empty claims should match everything")
	}
}

func TestMatchField(t *testing.T) {
	if !matchField(nil, "x") {
		t.Error("nil field should match (wildcard)")
	}
	if !matchField([]string{"a"}, "a") {
		t.Error("expected match on 'a'")
	}
	if matchField([]string{"a"}, "b") {
		t.Error("did not expect match on 'b'")
	}
}

func TestIsTrap(t *testing.T) {
	if isTrap(nil) {
		t.Error("nil should not be trap")
	}
	if !isTrap(&fakeErr{"wasm unreachable reached"}) {
		t.Error("expected trap for unreachable")
	}
	if !isTrap(&fakeErr{"invalid memory access"}) {
		t.Error("expected trap for invalid memory")
	}
	if !isTrap(&fakeErr{"wasm trap"}) {
		t.Error("expected trap for generic trap")
	}
	if isTrap(&fakeErr{"some other error"}) {
		t.Error("did not expect some-other-error to be a trap")
	}
}

func TestNewInputMarshalsCanonicalShape(t *testing.T) {
	in, err := NewInput("0001", "CA...", 42,
		[]byte(`[{"symbol":"swap"}]`),
		[]byte(`{"u64":7}`),
	)
	if err != nil {
		t.Fatalf("NewInput: %v", err)
	}
	if in.ContractID != "CA..." {
		t.Errorf("ContractID=%q", in.ContractID)
	}
	if string(in.TopicsJSON) != `[{"symbol":"swap"}]` {
		t.Errorf("TopicsJSON=%q", in.TopicsJSON)
	}
	if !contains(string(in.EventJSON), `"id":"0001"`) {
		t.Errorf("EventJSON missing id: %s", in.EventJSON)
	}
	if !contains(string(in.EventJSON), `"contract":"CA..."`) {
		t.Errorf("EventJSON missing contract: %s", in.EventJSON)
	}
	if !contains(string(in.EventJSON), `"ledger":42`) {
		t.Errorf("EventJSON missing ledger: %s", in.EventJSON)
	}
}

// NewInput with nil value falls back to JSON null.
func TestNewInputNilValue(t *testing.T) {
	in, err := NewInput("e", "C", 1, []byte("[]"), nil)
	if err != nil {
		t.Fatalf("NewInput: %v", err)
	}
	if !contains(string(in.EventJSON), `"value":null`) {
		t.Errorf("EventJSON did not serialize nil value as null: %s", in.EventJSON)
	}
}

// contains is a tiny stdlib-free substring search used here to keep the
// test file's dependency surface small.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// fakeErr is the cheapest error satisfying error; used for isTrap tests.
type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }
