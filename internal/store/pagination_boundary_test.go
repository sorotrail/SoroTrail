//go:build integration

package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedEvents inserts n sequential events starting at ledger 100, one per
// ledger, all for contractA. It returns the events for assertion.
func seedEvents(t *testing.T, p *Postgres, n int) []Event {
	t.Helper()
	events := make([]Event, n)
	for i := range n {
		events[i] = testEvent(eventID(i+1), int64(100+i), contractA)
	}
	_, err := p.UpsertEvents(context.Background(), events)
	require.NoError(t, err)
	return events
}

// TestPaginationBoundary covers three edge-case families for
// keyset pagination: empty results, single-page results, and results
// where the total count is an exact multiple of the page size (so the
// last page is full and the cursor is "").
func TestPaginationBoundary(t *testing.T) {
	t.Run("empty store", func(t *testing.T) {
		p := testStore(t)
		for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
			for _, order := range []string{"", "asc", "desc"} {
				t.Run(orderBy+"/"+order, func(t *testing.T) {
					events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
						Scope:   WildcardScope(),
						OrderBy: orderBy,
						Order:   order,
						Limit:   10,
					})
					require.NoError(t, err)
					assert.Empty(t, events)
					assert.Empty(t, cursor, "empty result must have empty cursor")
				})
			}
		}
	})

	t.Run("filter matches nothing", func(t *testing.T) {
		p := testStore(t)
		_ = seedEvents(t, p, 5)

		events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
			Scope:      WildcardScope(),
			ContractID: contractB, // no events for this contract
			Limit:      10,
		})
		require.NoError(t, err)
		assert.Empty(t, events)
		assert.Empty(t, cursor, "no-match filter must have empty cursor")
	})

	t.Run("single page", func(t *testing.T) {
		// 5 events, limit=10 → everything fits in one page, cursor is "".
		p := testStore(t)
		seeded := seedEvents(t, p, 5)

		for _, tc := range []struct {
			name    string
			orderBy string
			order   string
		}{
			{"id/asc", OrderByID, "asc"},
			{"id/desc", OrderByID, "desc"},
			{"ledger/asc", OrderByLedger, "asc"},
			{"ledger/desc", OrderByLedger, "desc"},
			{"created_at/asc", OrderByCreatedAt, "asc"},
			{"created_at/desc", OrderByCreatedAt, "desc"},
			{"default/asc", "", "asc"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
					Scope:   WildcardScope(),
					OrderBy: tc.orderBy,
					Order:   tc.order,
					Limit:   10,
				})
				require.NoError(t, err)
				require.Len(t, events, len(seeded))
				assert.Empty(t, cursor, "all results fit on one page")
				assertSorted(t, events, tc.orderBy, tc.order)
			})
		}
	})

	t.Run("single page limit equals count", func(t *testing.T) {
		// 5 events, limit=5 → exact fit, cursor is "".
		p := testStore(t)
		seeded := seedEvents(t, p, 5)

		events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
			Scope: WildcardScope(),
			Limit: 5,
		})
		require.NoError(t, err)
		require.Len(t, events, len(seeded))
		assert.Empty(t, cursor, "limit equals total → no next page")
	})

	t.Run("exact multiple of limit", func(t *testing.T) {
		// 6 events, limit=3 → exactly 2 pages, last page is full.
		p := testStore(t)
		seeded := seedEvents(t, p, 6)

		for _, tc := range []struct {
			name    string
			orderBy string
			order   string
		}{
			{"id/asc", OrderByID, "asc"},
			{"id/desc", OrderByID, "desc"},
			{"ledger/asc", OrderByLedger, "asc"},
			{"ledger/desc", OrderByLedger, "desc"},
			{"created_at/asc", OrderByCreatedAt, "asc"},
			{"created_at/desc", OrderByCreatedAt, "desc"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var all []Event
				cursor := ""
				for range 10 { // bounded safety
					page, next, err := p.QueryEvents(context.Background(), EventFilter{
						Scope:   WildcardScope(),
						OrderBy: tc.orderBy,
						Order:   tc.order,
						Limit:   3,
						Cursor:  cursor,
					})
					require.NoError(t, err)
					require.Len(t, page, 3, "each page is full")
					all = append(all, page...)
					if next == "" {
						cursor = next
						break
					}
					cursor = next
				}
				require.Len(t, all, len(seeded),
					"every seeded row returned exactly once")
				assert.Empty(t, cursor,
					"last page is full → cursor must be empty (end-of-data)")
				assertSorted(t, all, tc.orderBy, tc.order)
			})
		}
	})

	t.Run("exact multiple of limit across filter", func(t *testing.T) {
		// Filter to contractA (3 events) with limit=3, then
		// contractB with limit=3. Both should be exact fits.
		p := testStore(t)
		// Seed 6 events: odd for contractA, even for contractB.
		var events []Event
		for i := range 6 {
			contract := contractA
			if i%2 == 1 {
				contract = contractB
			}
			events = append(events, testEvent(eventID(i+1), int64(100+i), contract))
		}
		_, err := p.UpsertEvents(context.Background(), events)
		require.NoError(t, err)

		for _, contract := range []string{contractA, contractB} {
			t.Run(contract, func(t *testing.T) {
				var all []Event
				cursor := ""
				for range 10 {
					page, next, err := p.QueryEvents(context.Background(), EventFilter{
						Scope:      WildcardScope(),
						ContractID: contract,
						Limit:      3,
						Cursor:     cursor,
					})
					require.NoError(t, err)
					all = append(all, page...)
					if next == "" {
						break
					}
					cursor = next
				}
				// contractA has 3 events, contractB has 3 events.
				require.Len(t, all, 3,
					"all matching events returned")
			})
		}
	})
}

// TestPaginationBoundary_CountEvents verifies that CountEvents returns the
// same total regardless of pagination parameters, for boundary cases.
func TestPaginationBoundary_CountEvents(t *testing.T) {
	p := testStore(t)
	_ = seedEvents(t, p, 6)

	t.Run("count ignores cursor and limit", func(t *testing.T) {
		total, err := p.CountEvents(context.Background(), EventFilter{
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(6), total)
	})

	t.Run("count after first page", func(t *testing.T) {
		_, cursor, err := p.QueryEvents(context.Background(), EventFilter{
			Scope: WildcardScope(),
			Limit: 3,
		})
		require.NoError(t, err)
		require.NotEmpty(t, cursor)

		total, err := p.CountEvents(context.Background(), EventFilter{
			Scope:  WildcardScope(),
			Cursor: cursor,
			Limit:  3,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(6), total,
			"count must return total matching rows, not remaining")
	})

	t.Run("count for no-match filter", func(t *testing.T) {
		total, err := p.CountEvents(context.Background(), EventFilter{
			Scope:      WildcardScope(),
			ContractID: contractB,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
	})
}

// TestPaginationBoundary_ExactMultipleWithDesc verifies the exact-multiple
// boundary with descending order and keyset cursors, confirming the cursor
// correctly points to the last row of the penultimate page.
func TestPaginationBoundary_ExactMultipleWithDesc(t *testing.T) {
	p := testStore(t)
	seeded := seedEvents(t, p, 4) // 4 events, limit=2 → 2 full pages

	page1, cursor1, err := p.QueryEvents(context.Background(), EventFilter{
		Scope: WildcardScope(),
		Order: "desc",
		Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, cursor1)

	// page1 should have the 2 newest (highest IDs).
	assert.Equal(t, seeded[3].ID, page1[0].ID)
	assert.Equal(t, seeded[2].ID, page1[1].ID)

	page2, cursor2, err := p.QueryEvents(context.Background(), EventFilter{
		Scope:  WildcardScope(),
		Order:  "desc",
		Limit:  2,
		Cursor: cursor1,
	})
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Empty(t, cursor2, "second page is full → no next page")

	assert.Equal(t, seeded[1].ID, page2[0].ID)
	assert.Equal(t, seeded[0].ID, page2[1].ID)
}

// TestPaginationBoundary_SingleEvent verifies pagination when there is
// exactly one event in the store.
func TestPaginationBoundary_SingleEvent(t *testing.T) {
	p := testStore(t)
	_ = seedEvents(t, p, 1)

	for _, tc := range []struct {
		name    string
		orderBy string
		order   string
		limit   int
	}{
		{"id/asc/limit10", OrderByID, "asc", 10},
		{"id/desc/limit10", OrderByID, "desc", 10},
		{"id/asc/limit1", OrderByID, "asc", 1},
		{"id/asc/limit200", OrderByID, "asc", MaxQueryLimit},
		{"ledger/asc/limit10", OrderByLedger, "asc", 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
				Scope:   WildcardScope(),
				OrderBy: tc.orderBy,
				Order:   tc.order,
				Limit:   tc.limit,
			})
			require.NoError(t, err)
			require.Len(t, events, 1)
			assert.Empty(t, cursor, "single event → no next page")
		})
	}
}

// TestPaginationBoundary_NonEmptyTopics verifies pagination boundary
// behavior when events have JSON topics (the default testEvent setup).
func TestPaginationBoundary_NonEmptyTopics(t *testing.T) {
	p := testStore(t)
	// Seed events with varying topics, 4 events total.
	var events []Event
	for i := range 4 {
		e := testEvent(eventID(i+1), int64(100+i), contractA)
		// Alternate topics so filtering can exercise boundaries.
		if i%2 == 0 {
			e.Topics = json.RawMessage(`[{"symbol":"transfer"}]`)
		} else {
			e.Topics = json.RawMessage(`[{"symbol":"mint"}]`)
		}
		events = append(events, e)
	}
	_, err := p.UpsertEvents(context.Background(), events)
	require.NoError(t, err)

	t.Run("topic filter exact multiple of limit", func(t *testing.T) {
		// 2 events with {"symbol":"transfer"}, limit=2 → exact fit.
		var all []Event
		cursor := ""
		for range 5 {
			page, next, err := p.QueryEvents(context.Background(), EventFilter{
				Scope: WildcardScope(),
				Topic: json.RawMessage(`{"symbol":"transfer"}`),
				Limit: 2,
				Cursor: cursor,
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		assert.Len(t, all, 2, "exactly 2 transfer events")
		assert.Empty(t, cursor, "exact fit → no next page")
	})

	t.Run("topic filter no match", func(t *testing.T) {
		events, cursor, err := p.QueryEvents(context.Background(), EventFilter{
			Scope: WildcardScope(),
			Topic: json.RawMessage(`{"symbol":"burn"}`),
			Limit: 10,
		})
		require.NoError(t, err)
		assert.Empty(t, events)
		assert.Empty(t, cursor)
	})
}
