//go:build integration

package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sorotrail/sorotrail/internal/testdb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryEvents_FilterCombinations covers the complete QueryEvents filter
// surface against a real Postgres database via testdb.Setup:
//
//   - every individual filter,
//   - pairwise intersections of filters,
//   - pagination in both directions with all order_by modes,
//   - limit boundaries (1, Cap, 0/under, above Cap),
//   - empty results from impossible windows and empty selectors,
//   - ordering totalness via duplicate sort values.
//
// The tests are untagged from the shared goroutine (not wrapped in a loop over
// backends) so they run against Postgres only and can use testdb.Setup and
// the helpers shared by store's integration tests.
func TestQueryEvents_FilterCombinations(t *testing.T) {
	ctx := context.Background()

	pool := testdb.Setup(t, Migrate)
	st := NewPostgres(pool)

	// One event per contract, a couple of event types, and a mix of successful
	// and failed calls so every filter has a distinguishable row set.
	seed := func() []Event {
		var events []Event
		for i := int64(1); i <= 12; i++ {
			contract := contractA
			if i%3 == 0 {
				contract = contractB
			}
			ev := testEvent(eventID(i), 100+i, contract)
			ev.Type = "contract"
			ev.TxHash = "tx" + string(rune('a'+i))
			if i%4 == 0 {
				ev.InSuccessfulCall = false
			}
			if i == 5 {
				ev.Type = "diagnostic"
				ev.Topics = json.RawMessage(`[{"symbol":"mint"}]`)
			}
			if i == 7 {
				// A distinct time window so the time filters are meaningful.
				ev.CreatedAt = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
			}
			events = append(events, ev)
		}
		_, err := st.UpsertEvents(ctx, events)
		require.NoError(t, err)
		return events
	}

	t.Run("no filters returns all rows in order", func(t *testing.T) {
		seed()
		got, cursor, err := st.QueryEvents(ctx, EventFilter{Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 12)
		assert.Empty(t, cursor, "full result set has no next page")
	})

	t.Run("single filter: contract_id", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{ContractID: contractB, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 4)
	})

	t.Run("single filter: contract_id_prefix", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{ContractIDPrefix: "CB", Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 4)
	})

	t.Run("single filter: types", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"diagnostic"}, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("single filter: topic", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{Topic: json.RawMessage(`{"symbol":"mint"}`), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("single filter: topic0 / topic1 positionally", func(t *testing.T) {
		seed()
		extra := []Event{
			testEvent(eventID(20), 200, contractA),
			testEvent(eventID(21), 201, contractA),
		}
		extra[0].Topics = json.RawMessage(`[{"symbol":"transfer"},{"address":"GABC"}]`)
		extra[1].Topics = json.RawMessage(`[{"symbol":"transfer"},{"address":"GDEF"}]`)
		_, err := st.UpsertEvents(ctx, extra)
		require.NoError(t, err)

		got, _, err := st.QueryEvents(ctx, EventFilter{
			Topic0: json.RawMessage(`{"symbol":"transfer"}`),
			Topic1: json.RawMessage(`{"address":"GABC"}`),
			Scope:  WildcardScope(),
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, eventID(20), got[0].ID)
	})

	t.Run("single filter: topic_contains", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{TopicContains: json.RawMessage(`[{"u64":7}]`), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 10, "every event whose topics array contains {u64:7}")
	})

	t.Run("single filter: tx_hash", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{TxHash: "txb", Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 4)
	})

	t.Run("single filter: in_successful_call", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{InSuccessfulCall: ptr(true), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 9)
	})

	t.Run("single filter: has_value", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{HasValue: ptr(true), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 12)
	})

	t.Run("single filter: tx_index", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{TxIndex: ptr(int32(1)), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 4)
	})

	t.Run("single filter: from_ledger / to_ledger", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{FromLedger: 103, ToLedger: 105, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("single filter: from_time / to_time", func(t *testing.T) {
		seed()
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			ToTime:   time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1, "only the seeded 2026-08-01 event falls in the window")
	})

	t.Run("pairwise intersections", func(t *testing.T) {
		seed()

		t.Run("contract_id + from_ledger", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				ContractID: contractB,
				FromLedger: 103,
				Scope:      WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 2)
		})

		t.Run("type + topic_contains", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				Types:         []string{"diagnostic"},
				TopicContains: json.RawMessage(`[{"u64":7}]`),
				Scope:         WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 1)
		})

		t.Run("tx_hash + in_successful_call", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				TxHash:           "txb",
				InSuccessfulCall: ptr(true),
				Scope:            WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 3)
		})

		t.Run("from_ledger + in_successful_call", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				FromLedger:       103,
				InSuccessfulCall: ptr(true),
				Scope:            WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 2)
		})

		t.Run("topic0 + contract_id", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				ContractID: contractA,
				Topic0:     json.RawMessage(`{"symbol":"transfer"}`),
				Scope:      WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 6)
		})
	})

	t.Run("limit boundaries", func(t *testing.T) {
		seed()

		t.Run("limit one", func(t *testing.T) {
			got, next, err := st.QueryEvents(ctx, EventFilter{Limit: 1, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, 1)
			assert.NotEmpty(t, next, "a partial page carries a cursor")
		})

		t.Run("limit equal to page cap", func(t *testing.T) {
			got, next, err := st.QueryEvents(ctx, EventFilter{Limit: MaxQueryLimit, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, MaxQueryLimit)
			assert.Empty(t, next, "a full max-size page has no next page")
		})

		t.Run("limit zero falls back to the default", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{Limit: 0, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, DefaultQueryLimit)
		})

		t.Run("limit above the cap is clamped", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{Limit: MaxQueryLimit + 1, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, MaxQueryLimit)
		})

		t.Run("limit zero with empty result", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{Limit: 0, FromLedger: 9999, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, 0)
		})
	})

	// Seed once with duplicate sort values so the ordering-totalness checks
	// are meaningful: three events per ledger, shared created_at per ledger.
	seededOrder := seedOrderingEvents(t, st)
	n := len(seededOrder)

	t.Run("pagination direction and order_by", func(t *testing.T) {
		for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
			for _, order := range []string{"asc", "desc"} {
				name := orderBy
				if name == "" {
					name = "default"
				}
				name += "/" + order

				t.Run(name, func(t *testing.T) {
					got := walkPages(t, st, EventFilter{
						OrderBy: orderBy,
						Order:   order,
						Limit:   2,
						Scope:   WildcardScope(),
					})

					assert.Equal(t, n, len(got),
						"every seeded row must be visited exactly once")
					for i := 1; i < len(got); i++ {
						assertIDOrder(t, got[i-1].ID, got[i].ID, order == "desc")
					}
					assertSorted(t, got, orderBy, order)
				})
			}
		}
	})

	t.Run("cursor validity across orderings", func(t *testing.T) {
		// Seed once so cursor assertions are bound to the actual fixture,
		// not to a hand-written ID constant.
		seeded := seedOrderingEvents(t, st)

		// A bare id cursor from the first page must resume at the third
		// seeded event when followed.
		t.Run("bare id cursor resumes after the first page", func(t *testing.T) {
			page, cursor, err := st.QueryEvents(ctx, EventFilter{Limit: 2, Scope: WildcardScope()})
			require.NoError(t, err)
			require.Len(t, page, 2)
			require.NotEmpty(t, cursor)

			next, _, err := st.QueryEvents(ctx, EventFilter{Limit: 2, Cursor: cursor, Scope: WildcardScope()})
			require.NoError(t, err)
			// A two-wide page resumes at the third row, so the resumed page
			// has two events, not ten.
			require.Len(t, next, 2)
			assert.Equal(t, seeded[2].ID, next[0].ID)
		})

		t.Run("mismatched cursor rejects", func(t *testing.T) {
			// A cursor minted under the default ordering is a bare id; ask
			// for it under a ledger sort and decodeCompositeCursor cannot
			// parse the bare id as a ledger.
			_, _, err := st.QueryEvents(ctx, EventFilter{
				OrderBy: OrderByLedger,
				Limit:   2,
				Cursor:  idCursor,
				Scope:   WildcardScope(),
			})
			assert.ErrorIs(t, err, ErrInvalidCursor)
		})
	})

	t.Run("empty results", func(t *testing.T) {
		t.Run("impossible time window", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				FromTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
				ToTime:   time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC),
				Scope:    WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 0)
		})

		t.Run("impossible role filter", func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				InSuccessfulCall: ptr(false),
				FromLedger:       9999,
				Scope:            WildcardScope(),
			})
			require.NoError(t, err)
			assert.Len(t, got, 0)
		})

		t.Run("empty selector has no next page", func(t *testing.T) {
			got, cursor, err := st.QueryEvents(ctx, EventFilter{ContractID: contractA, Types: []string{"diagnostic"}, Scope: WildcardScope()})
			require.NoError(t, err)
			assert.Len(t, got, 1)
			assert.Empty(t, cursor)
		})
	})

	t.Run("error handling", func(t *testing.T) {
		t.Run("unsupported order_by", func(t *testing.T) {
			_, _, err := st.QueryEvents(ctx, EventFilter{OrderBy: "tx_hash", Scope: WildcardScope()})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported order_by")
		})
	})

	t.Cleanup(func() {
		_, err := pool.Exec(ctx, `TRUNCATE events, ingestion_state, watched_contracts, replay_state, api_keys CASCADE`)
		if err != nil {
			t.Logf("cleaning up shared Postgres: %v", err)
		}
	})
}

// assertIDOrder checks that two event IDs are in the requested traversal
// direction, which is the definition of "sorted" for every order_by mode in
// this package.
func assertIDOrder(t *testing.T, prev, cur string, desc bool) {
	t.Helper()
	if desc {
		assert.Greater(t, prev, cur)
	} else {
		assert.Less(t, prev, cur)
	}
}
