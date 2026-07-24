package store

// Integration tests for the Postgres store. They need a real database and
// are skipped unless TEST_DATABASE_URL is set, e.g.:
//
//	docker compose up -d postgres
//	make test-db
//
// Each run migrates the schema and truncates the tables it touches.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testStore(t *testing.T) *Postgres {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration tests (see CONTRIBUTING.md)")
	}
	require.NoError(t, Migrate(dbURL))

	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(context.Background(),
		`TRUNCATE events, ingestion_state, watched_contracts, replay_state`)
	require.NoError(t, err)
	return NewPostgres(pool)
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

const (
	contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

// eventID builds IDs whose lexicographic order matches insertion order, like
// real TOIDs.
func eventID(n int) string { return fmt.Sprintf("%020d-%010d", n, 0) }

func TestUpsertEvents_Idempotent(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	events := []Event{testEvent(eventID(1), 100, contractA), testEvent(eventID(2), 101, contractA)}
	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	inserted, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted, "duplicate IDs are ignored")

	got, err := st.GetEvent(ctx, eventID(1))
	require.NoError(t, err)
	assert.Equal(t, contractA, got.ContractID)
	assert.JSONEq(t, `[{"symbol":"transfer"},{"u64":7}]`, string(got.Topics))
	assert.JSONEq(t, `{"i128":"1000"}`, string(got.Value))
}

func TestGetEvent_NotFound(t *testing.T) {
	st := testStore(t)
	_, err := st.GetEvent(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestQueryEvents_FiltersAndPagination(t *testing.T) {
	st := testStore(t)
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
		got, _, err := st.QueryEvents(ctx, EventFilter{Type: "diagnostic"})
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
		require.Len(t, all, 10)
		for i := 1; i < len(all); i++ {
			assert.Less(t, all[i-1].ID, all[i].ID, "ascending ID order across pages")
		}
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
		require.Len(t, all, 10)
		for i := 1; i < len(all); i++ {
			assert.Greater(t, all[i-1].ID, all[i].ID, "descending ID order across pages")
		}
	})
}

func TestQueryEvents_TimeRange(t *testing.T) {
	st := testStore(t)
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
		assert.Len(t, got, 3) // Jul 23, 24, 25 inclusive
	})

	t.Run("to_time only", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ToTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2) // Jul 21, 22 inclusive
	})

	t.Run("both bounds inclusive", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			ToTime:   time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3) // Jul 22, 23, 24
	})

	t.Run("intersection with ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromLedger: 104,
			ToLedger:   106,
			FromTime:   time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2) // ledger 104+106, time >= Jul23 -> events 4,5 (ledger 104,105 -> Jul24,25)
	})

	t.Run("empty window returns nothing", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})
}

func TestIngestionStateRoundTrip(t *testing.T) {
	st := testStore(t)
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

func TestWatchedContracts(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	require.NoError(t, st.AddWatchedContract(ctx, contractA))
	require.NoError(t, st.AddWatchedContract(ctx, contractA), "re-adding is a no-op")
	require.NoError(t, st.AddWatchedContract(ctx, contractB))

	got, err := st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{contractA, contractB}, got)
}

func TestStats(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	})
	require.NoError(t, err)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 101}))
	require.NoError(t, st.AddWatchedContract(ctx, contractA))

	stats, err := st.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.TotalEvents)
	assert.Equal(t, int64(101), stats.LastIngestedLedger)
	assert.Equal(t, int64(2), stats.ContractCount)
	assert.Equal(t, int64(1), stats.WatchedContracts)
}
