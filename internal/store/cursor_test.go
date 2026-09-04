package store

import (
	"errors"
	"testing"
	"time"
)

func TestEncodeDecodeCompositeCursorRoundTrip(t *testing.T) {
	cases := []struct{ sortValue, id string }{
		{"123", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{"2024-01-02T03:04:05.000000000Z", "00aabbccdd002233aabbccdd002233aabbccdd002233aabbccdd002233"},
		{"token#1", "c"},
	}
	for _, c := range cases {
		enc := encodeCompositeCursor(c.sortValue, c.id)
		sv, id, err := decodeCompositeCursor(enc)
		if err != nil {
			t.Fatalf("decode(%q): %v", enc, err)
		}
		if sv != c.sortValue || id != c.id {
			t.Fatalf("round trip = (%q,%q), want (%q,%q)", sv, id, c.sortValue, c.id)
		}
	}
}

func TestDecodeCompositeCursorRejectsInvalid(t *testing.T) {
	for _, c := range []string{"!!!not-base64!!!", "abcdef", "|", "lead|", "|tail"} {
		if _, _, err := decodeCompositeCursor(c); err == nil {
			t.Fatalf("decode(%q) expected error", c)
		} else if !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("decode(%q) error = %v, want ErrInvalidCursor", c, err)
		}
	}
}

func TestEncodeDecodeContractsCursor(t *testing.T) {
	enc := EncodeContractsCursor("value", "99", "CA000000000000000000000000000000000000000000000000000000")
	sv, id, err := DecodeContractsCursor(enc)
	if err != nil {
		t.Fatalf("DecodeContractsCursor: %v", err)
	}
	if sv != "99" || id != "CA000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("DecodeContractsCursor = (%q,%q)", sv, id)
	}
	if _, _, err := DecodeContractsCursor("!!!not-base64!!!"); err == nil || !errors.Is(err, ErrInvalidContractsCursor) {
		t.Fatalf("expected ErrInvalidContractsCursor, got %v", err)
	}
}

func TestEncodeCursorOrderByID(t *testing.T) {
	if got := EncodeCursor(OrderByID, Event{ID: "abc123"}); got != "abc123" {
		t.Fatalf("EncodeCursor(OrderByID) = %q, want abc123", got)
	}
}

func TestEncodeCursorCompositeOrdersRoundTrip(t *testing.T) {
	e := Event{ID: "abc123", Ledger: 99, CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	for _, ob := range []string{OrderByLedger, OrderByCreatedAt} {
		cursor := EncodeCursor(ob, e)
		sv, id, err := decodeCompositeCursor(cursor)
		if err != nil {
			t.Fatalf("decode %s cursor: %v", ob, err)
		}
		if id != "abc123" {
			t.Fatalf("id = %q, want abc123", id)
		}
		if ob == OrderByLedger && sv != "99" {
			t.Fatalf("sort = %q, want 99", sv)
		}
	}
}
