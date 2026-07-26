package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedOrderingEvents stores events whose sort columns deliberately contain
// duplicates: three events per ledger, and created_at shared across whole
// ledgers (a batch insert stamps one timestamp on many rows). Ordering by a
// column with ties is exactly where keyset pagination breaks if the sort
// isn't made total, so the fixture has ties by construction.
func seedOrderingEvents(t *testing.T, p *Postgres) []Event {
	t.Helper()
	base := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	var events []Event
	n := 0
	for ledger := int64(100); ledger < 106; ledger++ {
		for range 3 {
			n++
			e := testEvent(eventID(n), ledger, contractA)
			// Whole ledgers share a created_at, so both non-id orderings
			// have runs of equal values wider than the page size below.
			e.CreatedAt = base.Add(time.Duration(ledger-100) * time.Minute)
			events = append(events, e)
		}
	}
	_, err := p.UpsertEvents(context.Background(), events)
	require.NoError(t, err)
	return events
}

// walkPages pages through every result with a page size smaller than the
// runs of duplicate sort values, returning the rows in the order seen.
func walkPages(t *testing.T, p *Postgres, f EventFilter) []Event {
	t.Helper()
	var all []Event
	cursor := ""
	for range 100 { // bounded so a broken cursor loops the test, not forever
		f.Cursor = cursor
		page, next, err := p.QueryEvents(context.Background(), f)
		require.NoError(t, err)
		all = append(all, page...)
		if next == "" {
			return all
		}
		cursor = next
	}
	t.Fatal("pagination did not terminate")
	return nil
}

// Each ordering returns correctly sorted results, and paginating through it
// yields every row exactly once — the acceptance criterion for #139.
func TestQueryEvents_OrderByPagination(t *testing.T) {
	p := testStore(t)
	seeded := seedOrderingEvents(t, p)

	seededIDs := make([]string, 0, len(seeded))
	for _, e := range seeded {
		seededIDs = append(seededIDs, e.ID)
	}
	sort.Strings(seededIDs)

	for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			name := orderBy + "/" + order
			if orderBy == "" {
				name = "default/" + order
			}
			t.Run(name, func(t *testing.T) {
				// Page size 2 against runs of 3 equal values, so page
				// boundaries land inside a tie.
				got := walkPages(t, p, EventFilter{OrderBy: orderBy, Order: order, Limit: 2})

				gotIDs := make([]string, 0, len(got))
				for _, e := range got {
					gotIDs = append(gotIDs, e.ID)
				}
				sorted := append([]string(nil), gotIDs...)
				sort.Strings(sorted)
				assert.Equal(t, seededIDs, sorted,
					"every seeded row returned exactly once, none skipped or repeated")

				assertSorted(t, got, orderBy, order)
			})
		}
	}
}

// assertSorted checks the returned order matches the requested one, with id
// as the documented tiebreaker.
func assertSorted(t *testing.T, got []Event, orderBy, order string) {
	t.Helper()
	desc := order == "desc"
	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		switch orderBy {
		case OrderByLedger:
			if prev.Ledger == cur.Ledger {
				assertIDOrder(t, prev.ID, cur.ID, desc)
				continue
			}
			if desc {
				assert.Greater(t, prev.Ledger, cur.Ledger)
			} else {
				assert.Less(t, prev.Ledger, cur.Ledger)
			}
		case OrderByCreatedAt:
			if prev.CreatedAt.Equal(cur.CreatedAt) {
				assertIDOrder(t, prev.ID, cur.ID, desc)
				continue
			}
			if desc {
				assert.True(t, prev.CreatedAt.After(cur.CreatedAt))
			} else {
				assert.True(t, prev.CreatedAt.Before(cur.CreatedAt))
			}
		default:
			assertIDOrder(t, prev.ID, cur.ID, desc)
		}
	}
}

func assertIDOrder(t *testing.T, prev, cur string, desc bool) {
	t.Helper()
	if desc {
		assert.Greater(t, prev, cur)
	} else {
		assert.Less(t, prev, cur)
	}
}

// An unsupported sort column is rejected rather than silently ignored — a
// typo must not quietly return default-ordered results.
func TestQueryEvents_RejectsUnsupportedOrderBy(t *testing.T) {
	p := testStore(t)
	_, _, err := p.QueryEvents(context.Background(), EventFilter{OrderBy: "tx_hash"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported order_by")
}

// A cursor minted under one ordering must not be silently accepted by
// another: it decodes to the wrong shape, and the caller gets ErrInvalidCursor
// (400) rather than a wrong page.
func TestQueryEvents_RejectsMismatchedCursor(t *testing.T) {
	p := testStore(t)
	seedOrderingEvents(t, p)
	ctx := context.Background()

	_, idCursor, err := p.QueryEvents(ctx, EventFilter{Limit: 2})
	require.NoError(t, err)
	require.NotEmpty(t, idCursor)

	_, _, err = p.QueryEvents(ctx, EventFilter{OrderBy: OrderByLedger, Limit: 2, Cursor: idCursor})
	assert.ErrorIs(t, err, ErrInvalidCursor)
}

// Default ordering still emits a bare event ID as its cursor, so cursors
// issued before order_by existed keep working.
func TestQueryEvents_DefaultCursorStaysPlainID(t *testing.T) {
	p := testStore(t)
	seeded := seedOrderingEvents(t, p)
	ctx := context.Background()

	page, cursor, err := p.QueryEvents(ctx, EventFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, page[1].ID, cursor, "default cursor is the last event ID verbatim")

	// And that bare cursor resumes correctly.
	next, _, err := p.QueryEvents(ctx, EventFilter{Limit: 2, Cursor: cursor})
	require.NoError(t, err)
	require.NotEmpty(t, next)
	assert.Equal(t, seeded[2].ID, next[0].ID)
}

func TestEncodeDecodeCursor(t *testing.T) {
	e := testEvent(eventID(7), 4242, contractA)
	e.CreatedAt = time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.UTC)

	t.Run("id ordering returns the raw id", func(t *testing.T) {
		assert.Equal(t, e.ID, EncodeCursor(OrderByID, e))
		assert.Equal(t, e.ID, EncodeCursor("", e))
	})

	t.Run("ledger round trip", func(t *testing.T) {
		v, id, err := decodeCompositeCursor(EncodeCursor(OrderByLedger, e))
		require.NoError(t, err)
		assert.Equal(t, "4242", v)
		assert.Equal(t, e.ID, id)
	})

	t.Run("created_at round trip keeps nanoseconds", func(t *testing.T) {
		v, id, err := decodeCompositeCursor(EncodeCursor(OrderByCreatedAt, e))
		require.NoError(t, err)
		assert.Equal(t, "2026-07-01T12:30:45.123456789Z", v)
		assert.Equal(t, e.ID, id)
	})

	for _, bad := range []string{"not base64!!", "", "cGxhaW4"} { // last: "plain", no separator
		t.Run("rejects "+bad, func(t *testing.T) {
			_, _, err := decodeCompositeCursor(bad)
			assert.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}
