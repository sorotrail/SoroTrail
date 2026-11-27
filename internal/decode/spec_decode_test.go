package decode

import (
	"testing"
)

func TestDecodeEventWithSpec_MatchingAndMismatched(t *testing.T) {
	eventSpec := &ContractSpecView{
		ContractID: "C123",
		WasmHash:   "hash123",
		Events: []EventSpecView{
			{
				Name: "transfer",
				TopicSpecs: []FieldSpecView{
					{Name: "from", Type: "address"},
					{Name: "to", Type: "address"},
					{Name: "amount", Type: "i128"},
				},
			},
		},
	}

	// Matching slice payload
	payload := []interface{}{
		"G123",
		"G456",
		"1000000000000000000",
	}

	res := DecodeEventWithSpec(payload, eventSpec, "transfer")
	if res.Mismatch {
		t.Errorf("expected no mismatch, got true: %s", res.MismatchError)
	}
	if res.SpecFields["amount"] != "1000000000000000000" {
		fn, ok := res.SpecFields["amount"].(string)
		if !ok || fn != "1000000000000000000" {
			t.Errorf("expected wide integer string representation, got %v", res.SpecFields["amount"])
		}
	}

	// Mismatched count payload
	mismatchedPayload := []interface{}{
		"G123",
	}
	resMismatch := DecodeEventWithSpec(mismatchedPayload, eventSpec, "transfer")
	if !resMismatch.Mismatch {
		t.Errorf("expected mismatch flag to be true")
	}
}

func TestDecodeEventWithSpec_MissingSpec(t *testing.T) {
	payload := "some-value"
	res := DecodeEventWithSpec(payload, nil, "transfer")
	if len(res.SpecFields) > 0 {
		t.Errorf("expected empty spec fields for missing spec")
	}
}

func TestDecodeEventWithSpec_VersionBoundary(t *testing.T) {
	specV1 := &ContractSpecView{
		ContractID: "C123",
		Events: []EventSpecView{
			{
				Name: "transfer",
				TopicSpecs: []FieldSpecView{
					{Name: "from", Type: "address"},
				},
			},
		},
	}
	payload := []interface{}{"G123"}
	res := DecodeEventWithSpec(payload, specV1, "transfer")
	if res.Mismatch {
		t.Errorf("unexpected mismatch on version boundary test")
	}
	if res.SpecFields["from"] != "G123" {
		t.Errorf("expected field 'from' to be decoded correctly")
	}
}
