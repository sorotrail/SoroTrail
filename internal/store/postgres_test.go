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
	"database/sql"
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
	return testStoreWithPartitionSpan(t, int64(DefaultEventPartitionSpan))
}

func testStoreWithPartitionSpan(t *testing.T, span int64) *Postgres {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration tests (see CONTRIBUTING.md)")
	}
	require.NoError(t, Migrate(dbURL))

	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Range partitions created by ensure_event_partitions are DDL, not
	// data — TRUNCATE below clears rows but leaves them behind. Since
	// different tests in this package use different partition spans over
	// overlapping ledger ranges (e.g. span=10 vs the production default
	// 120960), a partition a prior test left in place can collide with
	// the one this test's span is about to create ("partition would
	// overlap partition"). Drop every non-default events_* child first so
	// each test starts from a clean partition layout.
	_, err = pool.Exec(context.Background(), `
		DO $$
		DECLARE
		    child text;
		BEGIN
		    FOR child IN
		        SELECT c.relname
		        FROM pg_inherits i
		        JOIN pg_class c ON c.oid = i.inhrelid
		        JOIN pg_class p ON p.oid = i.inhparent
		        WHERE p.relname = 'events' AND c.relname <> 'events_default'
		    LOOP
		        EXECUTE format('DROP TABLE IF EXISTS %I', child);
		    END LOOP;
		END $$;`)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(),
		`TRUNCATE events, ingestion_state, watched_contracts, replay_state`)
	require.NoError(t, err)
	return NewPostgres(pool, span)
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

// legacySchemaMigrationsVersion is the schema_migrations version the
// legacy test simulates "already applied" by forcing it via UPDATE.
// The test hand-ruptures the events table to non-partitioned then
// re-runs Migrate, which applies every migration whose version is
// strictly greater than this value. It must therefore be < the
// partition slot (currently 0008_partition_events). The original
// value 3 happened to be the just-before-partition migration pre-#68
// (0003_add_created_at_index); post-#68, `= 3` resolves to
// 0003_topic_position_indexes, and the re-applied chain
// (0004…0008) is idempotent enough that 3 still works. If you
// renumber migrations and the partition slot moves, update this
// constant so `value < partitionSlot` stays true.
//
// Held as a named const (not an inline literal) so it is interpolated
// via fmt.Sprintf into the SQL below — golangci-lint's `unused` rule
// would otherwise flag it as unused because the SQL body is one opaque
// string literal to Go's analyzer. The const is a compile-time int, so
// `%d` interpolation here carries no SQL-injection surface.
const legacySchemaMigrationsVersion = 3

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

func TestUpsertEvents_CreatesPartitionsAndIsIdempotent(t *testing.T) {
	st := testStoreWithPartitionSpan(t, 10)
	ctx := context.Background()

	events := []Event{
		testEvent(eventID(1), 10, contractA),
		testEvent(eventID(2), 19, contractB),
	}

	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	inserted, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted, "duplicate IDs are ignored across partitions")

	var plan string
	rows, err := st.pool.Query(ctx, `EXPLAIN (COSTS OFF) SELECT id FROM events WHERE ledger BETWEEN $1 AND $2 ORDER BY id`, 10, 19)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan += line + "\n"
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan, "events_10_19")
	assert.NotContains(t, plan, "events_20_29")
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
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"diagnostic"}})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, eventID(3), got[0].ID)
	})

	t.Run("by multiple types", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"contract", "diagnostic"}})
		require.NoError(t, err)
		require.Len(t, got, 10)
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
			// Bounded to the original 10 events' ledger range so the extra
			// rows the "topic0 and topic1 positionally" subtest inserts
			// above (ledgers 200/201) don't inflate this count.
			page, next, err := st.QueryEvents(ctx, EventFilter{Limit: 3, Cursor: cursor, FromLedger: 101, ToLedger: 110})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		// Count what is actually in the table rather than hardcoding it:
		// sibling subtests above insert rows of their own, so a literal
		// makes this assertion depend on subtest execution order.
		require.Len(t, all, countEventsInRange(t, st, 101, 110))
		for i := 1; i < len(all); i++ {
			assert.Less(t, all[i-1].ID, all[i].ID, "ascending ID order across pages")
		}
	})

	t.Run("by topic_contains with object in array (containment)", func(t *testing.T) {
		// topic_contains=[{"u64":7}] should match events whose topics array
		// contains an element that jsonb-contains {"u64":7}.
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`[{"u64":7}]`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 9, "all events with u64:7 (9 out of 10)")
	})

	t.Run("by topic_contains with object directly does not match array", func(t *testing.T) {
		// Passing an object directly (not wrapped in array) won't match an
		// array column — jsonb array @> object is always false in Postgres.
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
		// contractB has 5 events (even indexes), all of which contain {"u64":7}.
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
				Limit:      3,
				Cursor:     cursor,
				Order:      "desc",
				FromLedger: 101,
				ToLedger:   110,
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		require.Len(t, all, countEventsInRange(t, st, 101, 110))
		for i := 1; i < len(all); i++ {
			assert.Greater(t, all[i-1].ID, all[i].ID, "descending ID order across pages")
		}
	})
}

// countEventsInRange reports how many events the store holds in a ledger
// range, so a pagination assertion states the range it walked instead of a
// literal that silently depends on which sibling subtests ran first.
//
// The range matters: sibling subtests insert their own rows outside
// [101,110], and the keyset walks below are bounded to that window. Counting
// the whole table instead would compare a bounded walk against an unbounded
// total.
func countEventsInRange(t *testing.T, st *Postgres, fromLedger, toLedger int64) int {
	t.Helper()
	got, next, err := st.QueryEvents(context.Background(), EventFilter{
		Limit:      MaxQueryLimit,
		FromLedger: fromLedger,
		ToLedger:   toLedger,
	})
	require.NoError(t, err)
	require.Empty(t, next, "fixture must fit in one max-size page")
	return len(got)
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

func TestMigrate_UpgradesLegacyEventsTable(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration tests (see CONTRIBUTING.md)")
	}
	require.NoError(t, Migrate(dbURL))

	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// See testStoreWithPartitionSpan: drop non-default events_* children
	// left by earlier tests' partition spans before creating our own, or
	// the narrow span=10 partition below can collide with one of theirs.
	_, err = pool.Exec(context.Background(), `
		DO $$
		DECLARE
		    child text;
		BEGIN
		    FOR child IN
		        SELECT c.relname
		        FROM pg_inherits i
		        JOIN pg_class c ON c.oid = i.inhrelid
		        JOIN pg_class p ON p.oid = i.inhparent
		        WHERE p.relname = 'events' AND c.relname <> 'events_default'
		    LOOP
		        EXECUTE format('DROP TABLE IF EXISTS %I', child);
		    END LOOP;
		END $$;`)
	require.NoError(t, err)

	st := NewPostgres(pool, 10)
	ctx := context.Background()

	original := testEvent(eventID(1), 100, contractA)
	original.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg=="}
	original.RawValueXDR = "AAAACgAAAAAAAAAB"
	_, err = st.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)

	sqlRerun := fmt.Sprintf(`
		ALTER TABLE events RENAME TO events_partitioned;
		-- Renaming the table doesn't rename its indexes/constraints — their
		-- names (events_pkey, idx_events_*) are global to the schema and
		-- still point at events_partitioned. Free them before the plain
		-- replacement events table below recreates the same names, exactly
		-- as 0008_partition_events.up.sql does for events_legacy.
		ALTER TABLE events_partitioned DROP CONSTRAINT IF EXISTS events_pkey CASCADE;
		DROP INDEX IF EXISTS idx_events_id;
		DROP INDEX IF EXISTS idx_events_contract_id;
		DROP INDEX IF EXISTS idx_events_ledger;
		DROP INDEX IF EXISTS idx_events_contract_ledger;
		DROP INDEX IF EXISTS idx_events_topics;
		DROP INDEX IF EXISTS idx_events_created_at;
		DROP INDEX IF EXISTS idx_events_topic0;
		DROP INDEX IF EXISTS idx_events_topic1;
		DROP INDEX IF EXISTS idx_events_topic2;
		DROP INDEX IF EXISTS idx_events_topic3;
		CREATE TABLE events (
			id                 text PRIMARY KEY,
			contract_id        text NOT NULL,
			ledger             bigint NOT NULL,
			type               text NOT NULL,
			tx_hash            text NOT NULL,
			tx_index           int NOT NULL DEFAULT 0,
			op_index           int NOT NULL DEFAULT 0,
			in_successful_call boolean NOT NULL DEFAULT true,
			topics             jsonb NOT NULL DEFAULT '[]'::jsonb,
			value              jsonb,
			created_at         timestamptz NOT NULL DEFAULT now(),
			topics_xdr         jsonb CHECK (topics_xdr IS NULL OR jsonb_typeof(topics_xdr) = 'array'),
			value_xdr          text,
			raw_topic_xdr      text[],
			raw_value_xdr      text
		);
		CREATE INDEX idx_events_contract_id ON events (contract_id);
		CREATE INDEX idx_events_ledger ON events (ledger);
		CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
		CREATE INDEX idx_events_topics ON events USING gin (topics);
		CREATE INDEX idx_events_created_at ON events (created_at);
		INSERT INTO events (
			id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			in_successful_call, topics, value, created_at,
			topics_xdr, value_xdr, raw_topic_xdr, raw_value_xdr
		)
		SELECT
			id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			in_successful_call, topics, value, created_at,
			to_jsonb(raw_topic_xdr) AS topics_xdr,
			raw_value_xdr            AS value_xdr,
			raw_topic_xdr,
			raw_value_xdr
		FROM events_partitioned
		ORDER BY ledger, id;
		DROP TABLE events_partitioned CASCADE;
		DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);
		UPDATE schema_migrations SET version = %d, dirty = false;
	`, legacySchemaMigrationsVersion)
	_, err = pool.Exec(ctx, sqlRerun)
	require.NoError(t, err)

	require.NoError(t, Migrate(dbURL))

	got, err := st.GetEvent(ctx, original.ID)
	require.NoError(t, err)
	assert.Equal(t, original.ContractID, got.ContractID)
	assert.Equal(t, original.RawTopicXDR, got.RawTopicXDR)
	assert.Equal(t, original.RawValueXDR, got.RawValueXDR)

	// 0008's events_default catch-all now holds the migrated row (ledger
	// 100). Exercise the runtime partition router (this test was created
	// with st = NewPostgres(pool, 10), so partitionSpan=10) on a ledger
	// events_default has no rows for yet. Postgres refuses to attach a new
	// range partition whose bounds already contain rows sitting in the
	// DEFAULT partition ("updated partition constraint for default
	// partition would be violated"), so this must be a fresh ledger, not
	// the just-migrated one — this is purely about proving the narrow
	// partition gets created post-migration.
	fresh := testEvent(eventID(2), 150, contractA)
	_, err = st.UpsertEvents(ctx, []Event{fresh})
	require.NoError(t, err)

	partitions, err := pool.Query(ctx, `SELECT to_regclass('events_150_159'), to_regclass('events_160_169')`)
	require.NoError(t, err)
	defer partitions.Close()
	require.True(t, partitions.Next())
	var firstPartition, secondPartition sql.NullString
	require.NoError(t, partitions.Scan(&firstPartition, &secondPartition))
	assert.True(t, firstPartition.Valid)
	assert.False(t, secondPartition.Valid)
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
	require.Len(t, got, 2)
	assert.Equal(t, contractA, got[0].ContractID)
	assert.Equal(t, contractB, got[1].ContractID)
	assert.False(t, got[0].AddedAt.IsZero(), "added_at is set on insert")
}

// RemoveWatchedContract drops the watch-list row but leaves stored events
// intact: the contract's history must remain queryable after removal.
func TestRemoveWatchedContract_PreservesEvents(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// Seed the watch list and ingest two events for that contract.
	require.NoError(t, st.AddWatchedContract(ctx, contractA))
	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
	})
	require.NoError(t, err)

	// Remove it — the response semantics are "stop future ingestion".
	require.NoError(t, st.RemoveWatchedContract(ctx, contractA))

	// The watch-list row is gone.
	remaining, err := st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	assert.Empty(t, remaining, "remove must clear the watch list entry")

	// Removing again with no row is a 404-style error: the API uses this
	// to surface a typo as 404.
	err = st.RemoveWatchedContract(ctx, contractA)
	assert.ErrorIs(t, err, ErrNotFound)

	// Stored events for the removed contract are intact and queryable.
	got, _, err := st.QueryEvents(ctx, EventFilter{ContractID: contractA})
	require.NoError(t, err)
	require.Len(t, got, 2, "removal NEVER deletes event rows — history is preserved")
	assert.Equal(t, int64(100), got[0].Ledger)
	assert.Equal(t, int64(101), got[1].Ledger)
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
	assert.Equal(t, int64(100), stats.OldestStoredLedger)
	assert.Equal(t, int64(2), stats.ContractCount)
	assert.Equal(t, int64(1), stats.WatchedContracts)

	var plan string
	rows, err := st.pool.Query(ctx, `EXPLAIN (COSTS OFF) SELECT coalesce(min(ledger), 0) FROM events`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan += line + "\n"
	}
	require.NoError(t, rows.Err())
	// The exact partition index name is Postgres-generated; the important
	// property is that min(ledger) stays index-backed.
	assert.Contains(t, plan, "Index")
	assert.Contains(t, plan, "ledger")
}

// TestQueryEvents_PositionalTopics exercises the topic0..topic3 positional
// filters in their own truncated DB so the extra rows do not leak into the
// shared-dataset assertions in TestQueryEvents_FiltersAndPagination.
func TestQueryEvents_PositionalTopics(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

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
}
