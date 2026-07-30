package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func eventID(n int) string {
	return fmt.Sprintf("%020d-%010d", n, 0)
}

func testEvent(id string, ledger int64, contractID string) Event {
	return Event{
		ID:               id,
		ContractID:       contractID,
		Ledger:           ledger,
		Type:             "contract",
		TxHash:           "deadbeef",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`),
		Value:            json.RawMessage(`{"i128":"1000"}`),
	}
}

// runStoreTests runs every backend-agnostic conformance test against the
// store created by factory. factory is called once per test function and
// must return a fresh, migrated store.
func runStoreTests(t *testing.T, factory func(t *testing.T) Store) {
	t.Run("UpsertEvents_Idempotent", func(t *testing.T) {
		testUpsertEventsIdempotent(t, factory(t))
	})
	t.Run("GetEvent_NotFound", func(t *testing.T) {
		testGetEventNotFound(t, factory(t))
	})
	t.Run("QueryEvents_FiltersAndPagination", func(t *testing.T) {
		testQueryEventsFiltersAndPagination(t, factory(t))
	})
	t.Run("QueryEvents_TimeRange", func(t *testing.T) {
		testQueryEventsTimeRange(t, factory(t))
	})
	t.Run("IngestionStateRoundTrip", func(t *testing.T) {
		testIngestionStateRoundTrip(t, factory(t))
	})
	t.Run("WatchedContracts", func(t *testing.T) {
		testWatchedContracts(t, factory(t))
	})
	t.Run("Stats", func(t *testing.T) {
		testStats(t, factory(t))
	})
	t.Run("RawXDRRoundTrip", func(t *testing.T) {
		testRawXDRRoundTrip(t, factory(t))
	})
	t.Run("ReplaceEventsInRangeKeepsRawXDR", func(t *testing.T) {
		testReplaceEventsInRangeKeepsRawXDR(t, factory(t))
	})
}

func testUpsertEventsIdempotent(t *testing.T, st Store) {
	ctx := context.Background()
	events := []Event{testEvent(eventID(1), 100, contractA), testEvent(eventID(2), 101, contractA)}
	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	inserted, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted, "duplicate IDs are ignored")

	got, err := st.GetEvent(ctx, eventID(1), WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, contractA, got.ContractID)
	assert.JSONEq(t, `[{"symbol":"transfer"},{"u64":7}]`, string(got.Topics))
	assert.JSONEq(t, `{"i128":"1000"}`, string(got.Value))
}

func testGetEventNotFound(t *testing.T, st Store) {
	_, err := st.GetEvent(context.Background(), "missing", WildcardScope())
	assert.ErrorIs(t, err, ErrNotFound)
}

func testQueryEventsFiltersAndPagination(t *testing.T, st Store) {
	ctx := context.Background()

	var events []Event
	for i := 1; i <= 10; i++ {
		contract := contractA
		if i%2 == 0 {
			contract = contractB
		}
		e := testEvent(eventID(i), int64(100+i), contract)
		if i == 3 {
			e.Topics = json.RawMessage(`[{"symbol":"mint"}]`)
			e.Type = "diagnostic"
		}
		events = append(events, e)
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	t.Run("by contract", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{ContractID: contractB})
		require.NoError(t, err)
		assert.Len(t, got, 5)
	})

	t.Run("by ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{FromLedger: 103, ToLedger: 105})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("by type", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"diagnostic"}})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, eventID(3), got[0].ID)
	})

	t.Run("by topic at any position", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Topic: json.RawMessage(`{"u64":7}`)})
		require.NoError(t, err)
		assert.Len(t, got, 9, "second-position topic matches too")

		got, _, err = st.QueryEvents(ctx, EventFilter{Topic: json.RawMessage(`{"symbol":"mint"}`)})
		require.NoError(t, err)
		assert.Len(t, got, 1)
	})

	t.Run("by topic0 and topic1 positionally", func(t *testing.T) {
		e1 := testEvent(eventID(100), 200, contractA)
		e1.Topics = json.RawMessage(`[{"symbol":"transfer"},{"address":"GABC"},{"address":"GDEF"}]`)
		e2 := testEvent(eventID(101), 201, contractA)
		e2.Topics = json.RawMessage(`[{"symbol":"transfer"},{"address":"GDEF"},{"address":"GABC"}]`)
		_, err := st.UpsertEvents(ctx, []Event{e1, e2})
		require.NoError(t, err)

		got, _, err := st.QueryEvents(ctx, EventFilter{
			Topic0: json.RawMessage(`{"symbol":"transfer"}`),
			Topic1: json.RawMessage(`{"address":"GABC"}`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, e1.ID, got[0].ID)
	})

	t.Run("keyset pagination walks all rows in order", func(t *testing.T) {
		var all []Event
		cursor := ""
		for {
			page, next, err := st.QueryEvents(ctx, EventFilter{Limit: 3, Cursor: cursor})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		require.Len(t, all, 12)
		for i := 1; i < len(all); i++ {
			assert.Less(t, all[i-1].ID, all[i].ID, "ascending ID order across pages")
		}
	})

	t.Run("by topic_contains with object in array (containment)", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`[{"u64":7}]`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 9, "all events with u64:7 (9 out of 10)")
	})

	t.Run("by topic_contains with object directly does not match array", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`{"u64":7}`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0, "object not in array => no match")
	})

	t.Run("by topic_contains combined with contract_id", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ContractID:    contractB,
			TopicContains: json.RawMessage(`[{"u64":7}]`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 5)
	})

	t.Run("by topic_contains no match", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`[{"symbol":"nonexistent"}]`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})

	t.Run("keyset pagination desc returns newest-first", func(t *testing.T) {
		var all []Event
		cursor := ""
		for {
			page, next, err := st.QueryEvents(ctx, EventFilter{
				Limit:  3,
				Cursor: cursor,
				Order:  "desc",
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		require.Len(t, all, 12)
		for i := 1; i < len(all); i++ {
			assert.Greater(t, all[i-1].ID, all[i].ID, "descending ID order across pages")
		}
	})
}

func testQueryEventsTimeRange(t *testing.T, st Store) {
	ctx := context.Background()

	var events []Event
	for i := 1; i <= 5; i++ {
		e := testEvent(eventID(i), int64(100+i), contractA)
		e.CreatedAt = time.Date(2026, 7, 20+i, 12, 0, 0, 0, time.UTC)
		events = append(events, e)
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	t.Run("from_time only", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("to_time only", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ToTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})

	t.Run("both bounds inclusive", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			ToTime:   time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("intersection with ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromLedger: 104,
			ToLedger:   106,
			FromTime:   time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})

	t.Run("empty window returns nothing", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})
}

func testIngestionStateRoundTrip(t *testing.T, st Store) {
	ctx := context.Background()

	_, err := st.GetIngestionState(ctx)
	assert.ErrorIs(t, err, ErrNotFound, "fresh database has no state")

	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 42, LastCursor: "c1"}))
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 43}))

	got, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(43), got.LastIngestedLedger)
	assert.Empty(t, got.LastCursor, "state is a single row, fully replaced")
}

func testWatchedContracts(t *testing.T, st Store) {
	ctx := context.Background()

	require.NoError(t, st.AddWatchedContract(ctx, contractA))
	require.NoError(t, st.AddWatchedContract(ctx, contractA), "re-adding is a no-op")
	require.NoError(t, st.AddWatchedContract(ctx, contractB))

	got, err := st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{contractA, contractB}, got)
}

func testStats(t *testing.T, st Store) {
	ctx := context.Background()

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	})
	require.NoError(t, err)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 101}))
	require.NoError(t, st.AddWatchedContract(ctx, contractA))

	stats, err := st.Stats(ctx, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.TotalEvents)
	assert.Equal(t, int64(101), stats.LastIngestedLedger)
	assert.Equal(t, int64(100), stats.OldestStoredLedger)
	assert.Equal(t, int64(2), stats.ContractCount)
	assert.Equal(t, int64(1), stats.WatchedContracts)
}

func testRawXDRRoundTrip(t *testing.T, st Store) {
	ctx := context.Background()

	withXDR := testEvent(eventID(1), 100, contractA)
	withXDR.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg==", "AAAAEAAAAA=="}
	withXDR.RawValueXDR = "AAAACgAAAAAAAAAB"

	legacy := testEvent(eventID(2), 100, contractA)

	_, err := st.UpsertEvents(ctx, []Event{withXDR, legacy})
	require.NoError(t, err)

	got, err := st.GetEvent(ctx, withXDR.ID, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, withXDR.RawTopicXDR, got.RawTopicXDR)
	assert.Equal(t, withXDR.RawValueXDR, got.RawValueXDR)

	gotLegacy, err := st.GetEvent(ctx, legacy.ID, WildcardScope())
	require.NoError(t, err)
	assert.Empty(t, gotLegacy.RawTopicXDR)
	assert.Empty(t, gotLegacy.RawValueXDR)
}

func testReplaceEventsInRangeKeepsRawXDR(t *testing.T, st Store) {
	ctx := context.Background()

	original := testEvent(eventID(1), 100, contractA)
	original.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg=="}
	original.RawValueXDR = "AAAACgAAAAAAAAAB"
	_, err := st.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)

	repaired := original
	repaired.RawValueXDR = "AAAACgAAAAAAAAAC"
	require.NoError(t, st.ReplaceEventsInRange(ctx, []Event{repaired}, 100, 100))

	got, err := st.GetEvent(ctx, original.ID, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, "AAAACgAAAAAAAAAC", got.RawValueXDR)

	noXDR := original
	noXDR.RawTopicXDR, noXDR.RawValueXDR = nil, ""
	require.NoError(t, st.ReplaceEventsInRange(ctx, []Event{noXDR}, 100, 100))

	got, err = st.GetEvent(ctx, original.ID, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAAADwAAAAh0cmFuc2Zlcg=="}, got.RawTopicXDR,
		"a JSON-only repair must not strip stored raw XDR")
	assert.Equal(t, "AAAACgAAAAAAAAAC", got.RawValueXDR)
}
