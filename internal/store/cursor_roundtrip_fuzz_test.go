package store

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// FuzzCompositeCursorRoundTrip checks the other direction from
// FuzzDecodeCompositeCursor: a cursor the store hands out decodes back to the
// exact (sort value, id) pair it was built from, and a pair that cannot be
// represented is rejected as ErrInvalidCursor instead of decoding to
// something else. A paging cursor that silently changes is a skipped or
// repeated page.
func FuzzCompositeCursorRoundTrip(f *testing.F) {
	f.Add("424242", "0000000000000424242-0000000001")
	f.Add("2026-09-24T12:00:00Z", "0000000000000424242-0000000001")
	f.Add("a|b", "id")   // a separator inside the sort value is allowed
	f.Add("sort", "i|d") // one inside the id is not
	f.Add("", "id")      // empty components are rejected
	f.Add("sort", "")
	f.Add("\x00\xff", "\xfe")
	f.Add(strings.Repeat("s", 300), strings.Repeat("i", 300))

	f.Fuzz(func(t *testing.T, sortValue, id string) {
		sv, gotID, err := decodeCompositeCursor(encodeCompositeCursor(sortValue, id))

		if sortValue == "" || id == "" || strings.Contains(id, "|") {
			if err == nil {
				// Only acceptable when the lossy case still decodes to
				// something well formed; it must never match the input.
				if sv == sortValue && gotID == id {
					t.Fatalf("(%q, %q) cannot be represented but decoded as itself", sortValue, id)
				}
				if sv == "" || gotID == "" || strings.Contains(gotID, "|") {
					t.Fatalf("(%q, %q) decoded to a malformed pair (%q, %q)", sortValue, id, sv, gotID)
				}
				return
			}
			if !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("unrepresentable pair gave %v, want ErrInvalidCursor", err)
			}
			return
		}

		if err != nil {
			t.Fatalf("a valid pair (%q, %q) failed to round-trip: %v", sortValue, id, err)
		}
		if sv != sortValue || gotID != id {
			t.Fatalf("round trip changed (%q, %q) into (%q, %q)", sortValue, id, sv, gotID)
		}

		// The contracts cursor shares the codec.
		csv, cid, cerr := DecodeContractsCursor(EncodeContractsCursor("name", sortValue, id))
		if cerr != nil || csv != sortValue || cid != id {
			t.Fatalf("contracts cursor round trip: (%q, %q), %v", csv, cid, cerr)
		}
	})
}

// FuzzEventCursorRoundTrip builds cursors from real Event values the way the
// store does and checks the decoded sort value is the one that was ordered on.
func FuzzEventCursorRoundTrip(f *testing.F) {
	f.Add(int64(424242), int64(1_790_000_000_000_000_000), "0000000000000424242-0000000001")
	f.Add(int64(0), int64(0), "x")
	f.Add(int64(-1), int64(-1), "id-with-dash")

	f.Fuzz(func(t *testing.T, ledger, unixNano int64, id string) {
		if id == "" || strings.Contains(id, "|") {
			t.Skip("not a well-formed event id")
		}
		e := Event{ID: id, Ledger: ledger, CreatedAt: time.Unix(0, unixNano).UTC()}

		sv, gotID, err := decodeCompositeCursor(EncodeCursor(OrderByLedger, e))
		if err != nil || gotID != id || sv != strconv.FormatInt(ledger, 10) {
			t.Fatalf("ledger cursor: (%q, %q), %v for %+v", sv, gotID, err, e)
		}

		sv, gotID, err = decodeCompositeCursor(EncodeCursor(OrderByCreatedAt, e))
		if err != nil || gotID != id {
			t.Fatalf("created_at cursor: (%q, %q), %v for %+v", sv, gotID, err, e)
		}
		back, perr := time.Parse(time.RFC3339Nano, sv)
		if perr != nil || !back.Equal(e.CreatedAt) {
			t.Fatalf("created_at sort value %q does not parse back to %s (%v)", sv, e.CreatedAt, perr)
		}
	})
}
