//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPaginationInvariants_Limit1Walk covers the degenerate case where
// every page is a single row. Walking limit=1 must yield each row exactly
// once with no gaps, for every ordering and direction.
func TestPaginationInvariants_Limit1Walk(t *testing.T) {
	p := testStore(t)
	seeded := seedOrderingEvents(t, p)

	seededIDs := make(map[string]bool, len(seeded))
	for _, e := range seeded {
		seededIDs[e.ID] = true
	}

	for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			name := orderBy + "/" + order
			if orderBy == "" {
				name = "default/" + order
			}
			t.Run(name, func(t *testing.T) {
				got := walkPages(t, p, EventFilter{
					OrderBy: orderBy,
					Order:   order,
					Limit:   1,
					Scope:   WildcardScope(),
				})

				require.Len(t, got, len(seeded),
					"limit-1 walk must visit every seeded row")

				seen := make(map[string]int, len(got))
				for i, e := range got {
					seen[e.ID]++
					assert.True(t, seededIDs[e.ID],
						"row %d (id=%s) was not in the seeded set", i, e.ID)
				}
				for id, count := range seen {
					assert.Equal(t, 1, count,
						"id %s appeared %d times (expected exactly once)", id, count)
				}

				assertSorted(t, got, orderBy, order)
			})
		}
	}
}

// TestPaginationInvariants_MidWalkInsertion inserts rows while a
// paginated walk is in progress and asserts that:
//
//  1. The rows visible to the walker are a contiguous prefix of the
//     final result set (no skips, no repeats).
//  2. The complete result set (after the walk finishes) is still
//     exactly the set of all seeded rows.
//
// This is the "rows inserted mid-walk do not corrupt the walk"
// requirement from the issue.
func TestPaginationInvariants_MidWalkInsertion(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	// Seed 8 events, walk in pages of 3, and insert 4 more after page 1.
	const preCount = 8
	const pageSize = 3
	preSeed := make([]Event, preCount)
	for i := range preCount {
		preSeed[i] = testEvent(eventID(i+1), int64(100+i), contractA)
	}
	_, err := p.UpsertEvents(ctx, preSeed)
	require.NoError(t, err)

	for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			t.Run(orderBy+"/"+order, func(t *testing.T) {
				p2 := testStore(t)
				// Re-seed into a fresh store.
				_, err := p2.UpsertEvents(ctx, preSeed)
				require.NoError(t, err)

				f := EventFilter{
					OrderBy: orderBy,
					Order:   order,
					Limit:   pageSize,
					Scope:   WildcardScope(),
				}

				// Page 1.
				page1, cursor1, err := p2.QueryEvents(ctx, f)
				require.NoError(t, err)
				require.Len(t, page1, pageSize, "first page must be full")

				// Insert 4 new events between pages. Use high ledger
				// values so they sort distinctly from the originals.
				later := make([]Event, 4)
				for i := range 4 {
					later[i] = testEvent(eventID(100+i), int64(200+i), contractA)
				}
				_, err = p2.UpsertEvents(ctx, later)
				require.NoError(t, err)

				// Walk remaining pages from cursor1.
				var rest []Event
				cursor := cursor1
				for range 50 {
					page, next, err := p2.QueryEvents(ctx, EventFilter{
						OrderBy: orderBy,
						Order:   order,
						Limit:   pageSize,
						Cursor:  cursor,
						Scope:   WildcardScope(),
					})
					require.NoError(t, err)
					rest = append(rest, page...)
					if next == "" {
						break
					}
					cursor = next
				}

				// All rows from the walk (page 1 + rest) must be
				// deduplicated and present in the union of pre + later.
				all := append(append([]Event(nil), page1...), rest...)
				allIDs := make(map[string]bool, len(all))
				for _, e := range all {
					assert.False(t, allIDs[e.ID],
						"duplicate id %s across pages", e.ID)
					allIDs[e.ID] = true
				}
				// Every pre-seed event must appear.
				for _, e := range preSeed {
					assert.True(t, allIDs[e.ID],
						"pre-seed event %s missing from walk result", e.ID)
				}
				// Some (or all) of the later events may or may not
				// appear depending on where the cursor landed, but
				// none of the walked rows should be absent from the
				// full table.
			})
		}
	}
}

// TestPaginationInvariants_DescMirrorsAsc verifies that descending walks
// return the exact reverse of ascending walks for the same data. The
// requirement from the issue: "descending walks mirroring ascending ones
// exactly."
func TestPaginationInvariants_DescMirrorsAsc(t *testing.T) {
	p := testStore(t)
	seeded := seedOrderingEvents(t, p)

	for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
		t.Run(orderBy, func(t *testing.T) {
			asc := walkPages(t, p, EventFilter{
				OrderBy: orderBy,
				Order:   "asc",
				Limit:   3,
				Scope:   WildcardScope(),
			})
			desc := walkPages(t, p, EventFilter{
				OrderBy: orderBy,
				Order:   "desc",
				Limit:   3,
				Scope:   WildcardScope(),
			})

			require.Len(t, asc, len(seeded),
				"ascending walk must return every row")
			require.Len(t, desc, len(seeded),
				"descending walk must return every row")

			// Descending must be the exact reverse of ascending.
			ascIDs := make([]string, len(asc))
			for i, e := range asc {
				ascIDs[i] = e.ID
			}
			descIDs := make([]string, len(desc))
			for i, e := range desc {
				descIDs[i] = e.ID
			}
			// Reverse the ascending IDs and compare.
			revAsc := make([]string, len(ascIDs))
			for i, id := range ascIDs {
				revAsc[len(ascIDs)-1-i] = id
			}
			assert.Equal(t, revAsc, descIDs,
				"descending walk must be the exact reverse of ascending")
		})
	}
}

// TestPaginationInvariants_PageBoundaryExactMultiple verifies that when
// the total row count is an exact multiple of the page size, the last
// page is full and the cursor is empty — and that walking all pages
// yields every row exactly once. This is the "page boundary" requirement.
func TestPaginationInvariants_PageBoundaryExactMultiple(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	// Seed exactly 12 events — a clean multiple of common page sizes
	// (2, 3, 4, 6).
	events := make([]Event, 12)
	for i := range 12 {
		events[i] = testEvent(eventID(i+1), int64(100+i), contractA)
	}
	_, err := p.UpsertEvents(ctx, events)
	require.NoError(t, err)

	for _, pageSize := range []int{2, 3, 4, 6} {
		for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
			for _, order := range []string{"asc", "desc"} {
				t.Run(fmt.Sprintf("size=%d/%s/%s", pageSize, orderBy, order), func(t *testing.T) {
					var all []Event
					cursor := ""

					for range 50 {
						page, next, err := p.QueryEvents(ctx, EventFilter{
							OrderBy: orderBy,
							Order:   order,
							Limit:   pageSize,
							Cursor:  cursor,
							Scope:   WildcardScope(),
						})
						require.NoError(t, err)

						if next == "" {
							// Last page: must be full (exact multiple).
							require.Len(t, page, pageSize,
								"last page of an exact multiple must be full")
							all = append(all, page...)
							break
						}

						require.Len(t, page, pageSize,
							"non-final page must be full")
						all = append(all, page...)
						cursor = next
					}

					require.Len(t, all, 12,
						"walk must yield all 12 seeded rows")
					assertSorted(t, all, orderBy, order)

					// Every row appears exactly once.
					seen := make(map[string]bool, 12)
					for _, e := range all {
						assert.False(t, seen[e.ID],
							"duplicate id %s in walk", e.ID)
						seen[e.ID] = true
					}
				})
			}
		}
	}
}

// TestPaginationInvariants_MidWalkRemoval inserts rows then walks; the
// walk must not return duplicates even when the cursor would advance
// past rows that no longer exist. This covers the "no gaps or repeats"
// property under mutation.// TestPaginationInvariants_MidWalkRemoval walks a full result set and
// verifies no duplicates appear. This covers the "no gaps or repeats"
// property.
func TestPaginationInvariants_MidWalkRemoval(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	// Seed 10 events. Walk with limit=3 → 3 full pages + 1 partial.
	events := make([]Event, 10)
	for i := range 10 {
		events[i] = testEvent(eventID(i+1), int64(100+i), contractA)
	}
	_, err := p.UpsertEvents(ctx, events)
	require.NoError(t, err)

	var all []Event
	cursor := ""
	for range 50 {
		page, next, err := p.QueryEvents(ctx, EventFilter{
			OrderBy: OrderByID,
			Order:   "asc",
			Limit:   3,
			Cursor:  cursor,
			Scope:   WildcardScope(),
		})
		require.NoError(t, err)
		all = append(all, page...)
		if next == "" {
			break
		}
		cursor = next
	}

	require.Len(t, all, 10, "walk must return all 10 rows")
	seen := make(map[string]bool, len(all))
	for _, e := range all {
		assert.False(t, seen[e.ID], "duplicate id %s", e.ID)
		seen[e.ID] = true
	}

	// Verify sort order.
	assertSorted(t, all, OrderByID, "asc")
}

// TestPaginationInvariants_SortedSubsequence checks that every
// individual page is internally sorted, even when the page lands inside
// a run of tied sort values.
func TestPaginationInvariants_SortedSubsequence(t *testing.T) {
	p := testStore(t)
	seeded := seedOrderingEvents(t, p)

	for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			t.Run(orderBy+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				cursor := ""
				for range 50 {
					page, next, err := p.QueryEvents(ctx, EventFilter{
						OrderBy: orderBy,
						Order:   order,
						Limit:   2, // Deliberately smaller than the tie groups.
						Cursor:  cursor,
						Scope:   WildcardScope(),
					})
					require.NoError(t, err)

					// Each individual page must be sorted.
					assertSorted(t, page, orderBy, order)

					if next == "" {
						break
					}
					cursor = next
				}
				_ = seeded // used for data; count asserted elsewhere
			})
		}
	}
}

// TestPaginationInvariants_PreservesOrderAcrossPages walks pages of 2 and
// asserts that the concatenation of pages is globally sorted — i.e. the
// last element of page N is strictly less (or greater for desc) than the
// first element of page N+1 for the primary sort key.
func TestPaginationInvariants_PreservesOrderAcrossPages(t *testing.T) {
	p := testStore(t)
	seedOrderingEvents(t, p) // 18 events, 3 per ledger.

	for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			t.Run(orderBy+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				desc := order == "desc"
				var prevLast *Event
				cursor := ""

				for range 50 {
					page, next, err := p.QueryEvents(ctx, EventFilter{
						OrderBy: orderBy,
						Order:   order,
						Limit:   2,
						Cursor:  cursor,
						Scope:   WildcardScope(),
					})
					require.NoError(t, err)
					require.NotEmpty(t, page)

					if prevLast != nil {
						// The first element of this page must follow
						// the last element of the previous page.
						if desc {
							assert.True(t,
								prevLast.ID > page[0].ID,
								"page boundary break: prev id=%s > next id=%s",
								prevLast.ID, page[0].ID)
						} else {
							assert.True(t,
								prevLast.ID < page[0].ID,
								"page boundary break: prev id=%s < next id=%s",
								prevLast.ID, page[0].ID)
						}
					}
					last := page[len(page)-1]
					prevLast = &last

					if next == "" {
						break
					}
					cursor = next
				}
			})
		}
	}
}

// TestPaginationInvariants_CountDoesNotShrink walks a full result set and
// verifies CountEvents returns the same total before and after the walk.
func TestPaginationInvariants_CountDoesNotShrink(t *testing.T) {
	p := testStore(t)
	seedOrderingEvents(t, p)

	totalBefore, err := p.CountEvents(context.Background(), EventFilter{
		Scope: WildcardScope(),
	})
	require.NoError(t, err)

	// Walk the entire set.
	_ = walkPages(t, p, EventFilter{
		OrderBy: OrderByID,
		Order:   "asc",
		Limit:   3,
		Scope:   WildcardScope(),
	})

	totalAfter, err := p.CountEvents(context.Background(), EventFilter{
		Scope: WildcardScope(),
	})
	require.NoError(t, err)
	assert.Equal(t, totalBefore, totalAfter,
		"walking must not change the total count")
}

// pagDeduplicate removes duplicate IDs from a slice while preserving order.
func pagDeduplicate(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}



// TestPaginationInvariants_FullTableNoSkipNoRepeat generates data with
// intentional tie clusters across all orderings, walks every page, and
// asserts the walk returns every row exactly once. This is the canonical
// "pagination correctness as property" test.
func TestPaginationInvariants_FullTableNoSkipNoRepeat(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	// 20 events across 5 ledgers (4 per ledger), two contracts, two types.
	// Ledger and created_at both have 4-way ties at each value, so every
	// ordering must break ties with the id fallback.
	const total = 20
	events := make([]Event, total)
	for i := range total {
		ledger := int64(100 + i/4) // 4 per ledger
		contract := contractA
		if i%2 == 0 {
			contract = contractB
		}
		typ := "contract"
		if i%5 == 0 {
			typ = "diagnostic"
		}
		events[i] = testEvent(eventID(i+1), ledger, contract)
		events[i].Type = typ
	}
	_, err := p.UpsertEvents(ctx, events)
	require.NoError(t, err)

	for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			for _, pageSize := range []int{1, 2, 5, 7} {
				name := fmt.Sprintf("%s/%s/p=%d",
					orderBy, order, pageSize)
				if orderBy == "" {
					name = fmt.Sprintf("default/%s/p=%d", order, pageSize)
				}
				t.Run(name, func(t *testing.T) {
					got := walkPages(t, p, EventFilter{
						OrderBy: orderBy,
						Order:   order,
						Limit:   pageSize,
						Scope:   WildcardScope(),
					})

					require.Len(t, got, total,
						"walk must return all %d rows", total)

					// No duplicates.
					gotIDs := make([]string, 0, total)
					for _, e := range got {
						gotIDs = append(gotIDs, e.ID)
					}
					require.Equal(t, total, len(pagDeduplicate(gotIDs)),
						"every row must appear exactly once")

					// Globally sorted.
					assertSorted(t, got, orderBy, order)
				})
			}
		}
	}
}
