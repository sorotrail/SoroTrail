package store

import (
	"encoding/json"
	"errors"
	"strings"
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

func TestFormatTOIDCursorAndCursorLedger(t *testing.T) {
	c1 := FormatTOIDCursor(100_000, 1, 0, 0)
	c2 := FormatTOIDCursor(100_000, 1, 0, 1)
	c3 := FormatTOIDCursor(100_000, 2, 0, 0)
	c4 := FormatTOIDCursor(250_000, 0, 0, 0)

	if c1 >= c2 || c2 >= c3 || c3 >= c4 {
		t.Fatalf("TOID cursors not ordered chronologically: %q, %q, %q, %q", c1, c2, c3, c4)
	}

	for _, tc := range []struct {
		cursor     string
		wantLedger int64
		wantOK     bool
	}{
		{c1, 100_000, true},
		{c4, 250_000, true},
		{FormatTOIDCursor(1, 0, 0, 0), 1, true},
		{"0001099511627776-0000000001", 256, true},
		// Small synthetic test IDs (toid < 1<<32) must return (0, false) so
		// unit tests with synthetic counters decoupled from Event.Ledger are
		// never mis-pruned.
		{"00000000000000000001-0000000000", 0, false},
		{"00000000000000000999-0000000010", 0, false},
		{"abc123", 0, false},
		{"12345-", 0, false},
		{"-0000000001", 0, false},
		{EncodeCursor(OrderByLedger, Event{Ledger: 500, ID: c1}), 0, false},
	} {
		gotLedger, gotOK := CursorLedger(tc.cursor)
		if gotOK != tc.wantOK || gotLedger != tc.wantLedger {
			t.Fatalf("CursorLedger(%q) = (%d, %v), want (%d, %v)",
				tc.cursor, gotLedger, gotOK, tc.wantLedger, tc.wantOK)
		}
	}
}

func TestValidateSparseFilterRange(t *testing.T) {
	topic := json.RawMessage(`{"symbol":"transfer"}`)

	// Non-sparse filter is always permitted even over an unbounded ledger range.
	if err := ValidateSparseFilterRange(EventFilter{ContractID: "C123"}, DefaultMaxSparseFilterLedgerSpan); err != nil {
		t.Fatalf("expected non-sparse filter to pass, got %v", err)
	}

	// Sparse filter with maxSpan <= 0 (disabled) is permitted.
	if err := ValidateSparseFilterRange(EventFilter{Topic0: topic}, 0); err != nil {
		t.Fatalf("expected disabled maxSpan to pass, got %v", err)
	}

	// Sparse filter without both FromLedger and ToLedger fails with ErrSparseFilterRangeTooWide.
	for _, f := range []EventFilter{
		{Topic: topic},
		{TopicContains: json.RawMessage(`[{"symbol":"transfer"}]`), FromLedger: 100},
		{Topic0: topic, ToLedger: 500},
		{Topic3: topic},
	} {
		err := ValidateSparseFilterRange(f, DefaultMaxSparseFilterLedgerSpan)
		if !errors.Is(err, ErrSparseFilterRangeTooWide) {
			t.Fatalf("expected ErrSparseFilterRangeTooWide for %+v, got %v", f, err)
		}
		if !strings.HasPrefix(err.Error(), "ledger range [") {
			t.Fatalf("expected error message to start with 'ledger range [' for API classification, got %q", err.Error())
		}
	}

	// Sparse filter exceeding maxSpan fails.
	wide := EventFilter{
		Topic0:     topic,
		FromLedger: 100_000,
		ToLedger:   100_000 + DefaultMaxSparseFilterLedgerSpan + 1,
	}
	if err := ValidateSparseFilterRange(wide, DefaultMaxSparseFilterLedgerSpan); !errors.Is(err, ErrSparseFilterRangeTooWide) {
		t.Fatalf("expected ErrSparseFilterRangeTooWide for wide span, got %v", err)
	}

	// Sparse filter within maxSpan passes.
	bounded := EventFilter{
		Topic0:     topic,
		FromLedger: 100_000,
		ToLedger:   100_000 + DefaultMaxSparseFilterLedgerSpan,
	}
	if err := ValidateSparseFilterRange(bounded, DefaultMaxSparseFilterLedgerSpan); err != nil {
		t.Fatalf("expected bounded sparse filter to pass, got %v", err)
	}
}

func TestBuildQueryEventsSQL_PartitionPruningPredicates(t *testing.T) {
	toidCursor := FormatTOIDCursor(240_000, 3, 1, 2)

	t.Run("OrderByID with canonical TOID cursor injects scalar ledger >= bound", func(t *testing.T) {
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: toidCursor,
			Order:  "asc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger >= $1") || !strings.Contains(q, "id > $2") {
			t.Fatalf("expected scalar ledger bound and id keyset predicate in query, got: %s (args=%v)", q, args)
		}
		if len(args) < 2 || args[0] != int64(240_000) || args[1] != toidCursor {
			t.Fatalf("unexpected args: %v", args)
		}
	})

	t.Run("OrderByID DESC with canonical TOID cursor injects scalar ledger <= bound", func(t *testing.T) {
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: toidCursor,
			Order:  "desc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger <= $1") || !strings.Contains(q, "id < $2") {
			t.Fatalf("expected scalar ledger <= bound and id < predicate in query, got: %s (args=%v)", q, args)
		}
	})

	t.Run("OrderByID with synthetic small-integer cursor omits ledger bound", func(t *testing.T) {
		q, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: "00000000000000000005-0000000000",
			Order:  "asc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if strings.Contains(q, "ledger >=") {
			t.Fatalf("synthetic test ID must not inject ledger bound: %s", q)
		}
	})

	t.Run("OrderByLedger injects scalar ledger bound alongside row-value comparison", func(t *testing.T) {
		ledgerCursor := EncodeCursor(OrderByLedger, Event{Ledger: 360_000, ID: toidCursor})
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByLedger,
			Order:   "asc",
			Cursor:  ledgerCursor,
			Limit:   25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger >= $1::bigint") || !strings.Contains(q, "(ledger, id) > ($1::bigint, $2)") {
			t.Fatalf("expected both scalar ledger bound and row-value predicate, got: %s (args=%v)", q, args)
		}
	})

	t.Run("Cross-order composite cursor replay returns ErrInvalidCursor", func(t *testing.T) {
		createdAtCursor := EncodeCursor(OrderByCreatedAt, Event{
			ID:        toidCursor,
			CreatedAt: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
		})
		if _, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByLedger,
			Cursor:  createdAtCursor,
		}, 0); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor when replaying created_at cursor on ledger order, got %v", err)
		}

		ledgerCursor := EncodeCursor(OrderByLedger, Event{Ledger: 360_000, ID: toidCursor})
		if _, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByCreatedAt,
			Cursor:  ledgerCursor,
		}, 0); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor when replaying ledger cursor on created_at order, got %v", err)
		}
	})
}
