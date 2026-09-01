//go:build integration

package store

// Integration tests for the Postgres store. Gated behind the `integration`
// build tag so `go test ./...` stays fast; run via `make test-integration`
// or with `go test -tags=integration ./...`. The runner must provide a
// Postgres reachable via TEST_DATABASE_URL (or testcontainers-go will start
// one — see CONTRIBUTING.md).
//
// Each run migrates the schema via store.Migrate and truncates the
// tables it touches. -p 1 keeps packages from racing on the same DB.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

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

	_, err = pool.Exec(context.Background(), `
		TRUNCATE events, ingestion_state, watched_contracts, replay_state, api_keys;
		-- Each test starts with a clean partition set: partitions persist
		-- across tests (TRUNCATE only empties them), and a leftover
		-- default-span partition would overlap the span-N partition a test
		-- with a custom span tries to create.
		DO $$
		DECLARE r record;
		BEGIN
			FOR r IN SELECT tablename FROM pg_tables WHERE tablename LIKE 'events\_%' LOOP
				EXECUTE format('DROP TABLE %I CASCADE', r.tablename);
			END LOOP;
		END $$;
	`)
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

	// Detach and drop all existing event partitions so a store with a different
	// partition span can create fresh partitions without overlap. This is
	// needed because TestMigrate_UpgradesLegacyEventsTable re-runs migrations
	// on the shared database, which may create the default (span=120960)
	// partition covering a very wide range.
	_, err = pool.Exec(context.Background(), `
		DO $block$
		DECLARE
			part text;
		BEGIN
			FOR part IN SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'events'::regclass
			LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || part || ' CASCADE';
			END LOOP;
		END $block$;
	`)
	require.NoError(t, err)

	return NewPostgres(pool, span)
}

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

// TestPartialIndexForSuccessfulCalls covers the partial index over
// (contract_id, ledger, id) restricted to in_successful_call = true,
// originally introduced as (contract_id, ledger) by migration 0019 and
// widened to include the id tiebreaker by 0022.
//
// It asserts two things. First, that the index exists with the expected
// shape — indexed columns and partial predicate — by reading its
// definition straight from the catalog (table-driven over the substrings
// the definition must contain). Second, that the predicate the index
// encodes matches query intent: a contract-scoped read filtered to
// successful calls returns only the successful-call rows and none of the
// failed-call rows.
func TestPartialIndexForSuccessfulCalls(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// The migration creates the index on the partitioned parent; read its
	// definition once and assert the pieces that must be present. Reading
	// pg_indexes.indexdef (rather than assembling from pg_index columns)
	// keeps the assertions close to how an operator would eyeball the
	// schema.
	var indexDef string
	err := st.pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = $1`,
		"idx_events_contract_ledger_id_successful",
	).Scan(&indexDef)
	require.NoError(t, err, "partial index should exist after migration")

	defWantSubstrings := []struct {
		name string
		want string
	}{
		{"indexed on events", " ON "},
		{"covers contract_id and ledger in order", "(contract_id, ledger, id)"},
		{"partial predicate scopes to successful calls", "WHERE (in_successful_call = true)"},
	}
	for _, tc := range defWantSubstrings {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, indexDef, tc.want)
		})
	}

	// The index must also register as a valid, partial index in the
	// catalog — a plain (non-partial) index on the same columns would
	// satisfy the substring checks above if the definition string ever
	// changed shape, so assert indpred is populated directly.
	var isValid, isPartial bool
	err = st.pool.QueryRow(ctx, `
		SELECT i.indisvalid, i.indpred IS NOT NULL
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`,
		"idx_events_contract_ledger_id_successful",
	).Scan(&isValid, &isPartial)
	require.NoError(t, err)
	assert.True(t, isValid, "index should be valid")
	assert.True(t, isPartial, "index should be partial (have a predicate)")

	// Functional check: seed a mix of successful and failed-call events and
	// confirm the partial predicate selects exactly the successful subset
	// for a contract. This is the query shape the index exists to serve.
	seed := []struct {
		id         string
		ledger     int64
		successful bool
	}{
		{eventID(1), 100, true},
		{eventID(2), 101, false},
		{eventID(3), 102, true},
		{eventID(4), 103, false},
		{eventID(5), 104, true},
	}
	events := make([]Event, 0, len(seed))
	wantSuccessful := make(map[string]bool)
	for _, s := range seed {
		e := testEvent(s.id, s.ledger, contractA)
		e.InSuccessfulCall = s.successful
		events = append(events, e)
		if s.successful {
			wantSuccessful[s.id] = true
		}
	}
	_, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	rows, err := st.pool.Query(ctx,
		`SELECT id FROM events WHERE contract_id = $1 AND in_successful_call = true ORDER BY id`,
		contractA)
	require.NoError(t, err)
	defer rows.Close()

	got := make(map[string]bool)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		got[id] = true
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, wantSuccessful, got,
		"query filtered to successful calls should return only successful-call rows")
}

// TestTxHashIndex covers migration 0015_add_tx_hash_index: the btree index
// over events(tx_hash) backing tx-hash lookups (EventFilter.TxHash and
// GetEventsByTxHash). Without it every tx_hash predicate degrades to a scan
// across all partitions.
//
// Like TestPartialIndexForSuccessfulCalls, the index's existence and shape
// are read straight from the catalog. The planner is deliberately not asked
// whether it would pick the index: on the handful of rows a test seeds, a
// seq scan is genuinely cheaper, so such an assertion would be flaky by
// construction.
func TestTxHashIndex(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// events is partitioned, so indexdef renders the target as
	// "ON ONLY public.events"; assert the table via the catalog's
	// tablename column instead of a substring of the definition.
	var tableName, indexDef string
	err := st.pool.QueryRow(ctx,
		`SELECT tablename, indexdef FROM pg_indexes WHERE indexname = $1`,
		"idx_events_tx_hash",
	).Scan(&tableName, &indexDef)
	require.NoError(t, err, "tx_hash index should exist after migrations")
	assert.Equal(t, "events", tableName, "index should be on the events table")

	defWantSubstrings := []struct {
		name string
		want string
	}{
		{"covers tx_hash", "(tx_hash)"},
	}
	for _, tc := range defWantSubstrings {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, indexDef, tc.want)
		})
	}

	var isValid bool
	err = st.pool.QueryRow(ctx, `
		SELECT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`,
		"idx_events_tx_hash",
	).Scan(&isValid)
	require.NoError(t, err)
	assert.True(t, isValid, "index should be valid")
}

func TestGetEvent_NotFound(t *testing.T) {
	st := testStore(t)
	_, err := st.GetEvent(context.Background(), "missing", SystemScope())
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
		got, _, err := st.QueryEvents(ctx, EventFilter{ContractID: contractB, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 5)
	})

	t.Run("by ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{FromLedger: 103, ToLedger: 105, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("by type", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"diagnostic"}, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, eventID(3), got[0].ID)
	})

	t.Run("by multiple types", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Types: []string{"contract", "diagnostic"}, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 10)
	})

	t.Run("by topic at any position", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Topic: json.RawMessage(`{"u64":7}`), Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Len(t, got, 9, "second-position topic matches too")

		got, _, err = st.QueryEvents(ctx, EventFilter{Topic: json.RawMessage(`{"symbol":"mint"}`), Scope: WildcardScope()})
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
			Scope:  WildcardScope(),
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
			page, next, err := st.QueryEvents(ctx, EventFilter{
				Limit:      3,
				Cursor:     cursor,
				FromLedger: 101,
				ToLedger:   110,
				Scope:      WildcardScope(),
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		// 12 rows total: the 10 seeded above plus e1/e2 inserted by the
		// "by topic0 and topic1 positionally" subtest.
		require.Len(t, all, 12)
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
			Scope:         WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 9, "all events with u64:7 (9 out of 10)")
	})

	t.Run("by topic_contains with object directly does not match array", func(t *testing.T) {
		// Passing an object directly (not wrapped in array) won't match an
		// array column — jsonb array @> object is always false in Postgres.
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`{"u64":7}`),
			Scope:         WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0, "object not in array => no match")
	})

	t.Run("by topic_contains combined with contract_id", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ContractID:    contractB,
			TopicContains: json.RawMessage(`[{"u64":7}]`),
			Scope:         WildcardScope(),
		})
		require.NoError(t, err)
		// contractB has 5 events (even indexes), all of which contain {"u64":7}.
		assert.Len(t, got, 5)
	})

	t.Run("by topic_contains no match", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			TopicContains: json.RawMessage(`[{"symbol":"nonexistent"}]`),
			Scope:         WildcardScope(),
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
				Scope:      WildcardScope(),
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		require.Len(t, all, 12) // 10 seeded + e1/e2 from the positional subtest
		require.Len(t, all, countEventsInRange(t, st, 101, 110))
		for i := 1; i < len(all); i++ {
			assert.Greater(t, all[i-1].ID, all[i].ID, "descending ID order across pages")
		}
	})

	t.Run("by tx_hash", func(t *testing.T) {
		e1 := testEvent(eventID(200), 300, contractA)
		e1.TxHash = "txhash1"
		e2 := testEvent(eventID(201), 301, contractA)
		e2.TxHash = "txhash2"
		e3 := testEvent(eventID(202), 302, contractA)
		e3.TxHash = "txhash1"
		_, err := st.UpsertEvents(ctx, []Event{e1, e2, e3})
		require.NoError(t, err)

		got, _, err := st.QueryEvents(ctx, EventFilter{TxHash: "txhash1",
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2)

		got, _, err = st.QueryEvents(ctx, EventFilter{TxHash: "txhash2",
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)

		got, _, err = st.QueryEvents(ctx, EventFilter{TxHash: "nonexistent",
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})

	t.Run("by in_successful_call", func(t *testing.T) {
		e1 := testEvent(eventID(300), 400, contractA)
		e1.InSuccessfulCall = true
		e1.TxHash = "isc_test_a"
		e2 := testEvent(eventID(301), 401, contractA)
		e2.InSuccessfulCall = false
		e2.TxHash = "isc_test_b"
		e3 := testEvent(eventID(302), 402, contractA)
		e3.InSuccessfulCall = true
		e3.TxHash = "isc_test_c"
		_, err := st.UpsertEvents(ctx, []Event{e1, e2, e3})
		require.NoError(t, err)

		got, _, err := st.QueryEvents(ctx, EventFilter{
			TxHash:           "isc_test_a",
			InSuccessfulCall: ptr(true),
			Scope:            WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, e1.ID, got[0].ID)

		got, _, err = st.QueryEvents(ctx, EventFilter{
			TxHash:           "isc_test_b",
			InSuccessfulCall: ptr(false),
			Scope:            WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, e2.ID, got[0].ID)

		got, _, err = st.QueryEvents(ctx, EventFilter{TxHash: "isc_test_c",
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.True(t, got[0].InSuccessfulCall)
	})

	t.Run("by tx_hash and in_successful_call combined", func(t *testing.T) {
		e1 := testEvent(eventID(400), 500, contractA)
		e1.TxHash = "combo1"
		e1.InSuccessfulCall = true
		e2 := testEvent(eventID(401), 501, contractA)
		e2.TxHash = "combo1"
		e2.InSuccessfulCall = false
		e3 := testEvent(eventID(402), 502, contractA)
		e3.TxHash = "combo2"
		e3.InSuccessfulCall = true
		_, err := st.UpsertEvents(ctx, []Event{e1, e2, e3})
		require.NoError(t, err)

		got, _, err := st.QueryEvents(ctx, EventFilter{
			TxHash:           "combo1",
			InSuccessfulCall: ptr(true),
			Scope:            WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, e1.ID, got[0].ID)

		got, _, err = st.QueryEvents(ctx, EventFilter{
			TxHash:           "combo1",
			InSuccessfulCall: ptr(false),
			Scope:            WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, e2.ID, got[0].ID)
	})
}

func TestListContracts(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// Three contracts with distinct activity footprints:
	//   A: 3 events across ledgers 100–102
	//   B: 1 event in ledger 200 (min == max)
	//   C: 2 events across ledgers 300–301
	events := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
		testEvent(eventID(3), 102, contractA),
		testEvent(eventID(4), 200, contractB),
		testEvent(eventID(5), 300, contractC),
		testEvent(eventID(6), 301, contractC),
	}
	// UpsertEvents writes created_at as given (no DEFAULT now() kick-in),
	// so stamp the fixtures or last_seen comes back as the zero time.
	ingestedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for i := range events {
		events[i].CreatedAt = ingestedAt
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	wantIDs := []string{contractA, contractB, contractC} // ascending

	t.Run("groups counts and ledger ranges ordered by contract_id", func(t *testing.T) {
		got, cursor, err := st.ListContracts(ctx, ContractsFilter{Limit: 10})
		require.NoError(t, err)
		require.Len(t, got, len(wantIDs))
		assert.Empty(t, cursor, "no next page when everything fits")

		for i, c := range got {
			assert.Equal(t, wantIDs[i], c.ContractID)
			assert.True(t, c.LastSeen.Equal(ingestedAt), "last_seen = %v, want %v", c.LastSeen, ingestedAt)
		}
		assert.Equal(t, int64(3), got[0].EventCount)
		assert.Equal(t, int64(100), got[0].FirstLedger)
		assert.Equal(t, int64(102), got[0].LastLedger)

		assert.Equal(t, int64(1), got[1].EventCount)
		assert.Equal(t, int64(200), got[1].FirstLedger)
		assert.Equal(t, int64(200), got[1].LastLedger)

		assert.Equal(t, int64(2), got[2].EventCount)
		assert.Equal(t, int64(300), got[2].FirstLedger)
		assert.Equal(t, int64(301), got[2].LastLedger)
	})

	t.Run("keyset pagination walks every contract exactly once", func(t *testing.T) {
		page1, cursor, err := st.ListContracts(ctx, ContractsFilter{Limit: 2})
		require.NoError(t, err)
		require.Len(t, page1, 2)
		require.NotEmpty(t, cursor, "a full page means more rows follow")
		assert.Equal(t, wantIDs[:2], []string{page1[0].ContractID, page1[1].ContractID})

		page2, cursor2, err := st.ListContracts(ctx, ContractsFilter{Limit: 2, Cursor: cursor})
		require.NoError(t, err)
		require.Len(t, page2, 1)
		assert.Empty(t, cursor2, "cursor omitted on the last page")
		assert.Equal(t, wantIDs[2], page2[0].ContractID)
	})

	t.Run("prefix filter narrows to matching contracts", func(t *testing.T) {
		got, _, err := st.ListContracts(ctx, ContractsFilter{ContractIDPrefix: "CB", Limit: 10})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, contractB, got[0].ContractID)
	})

	t.Run("activity sort ranks busiest first", func(t *testing.T) {
		got, _, err := st.ListContracts(ctx, ContractsFilter{SortKey: SortByActivity, Limit: 10})
		require.NoError(t, err)
		require.Len(t, got, len(wantIDs))
		assert.Equal(t, contractA, got[0].ContractID) // 3 events
		assert.Equal(t, contractC, got[1].ContractID) // 2 events
		assert.Equal(t, contractB, got[2].ContractID) // 1 event
	})

	t.Run("count matches the listing and honors the prefix", func(t *testing.T) {
		total, err := st.CountContracts(ctx, ContractsFilter{})
		require.NoError(t, err)
		assert.Equal(t, int64(len(wantIDs)), total)

		narrowed, err := st.CountContracts(ctx, ContractsFilter{ContractIDPrefix: "CB"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), narrowed)
	})
}

func TestQueryEvents_InSuccessfulCallFilter(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	trueEvent := testEvent(eventID(500), 600, contractA)
	trueEvent.InSuccessfulCall = true

	falseEvent := testEvent(eventID(501), 601, contractA)
	falseEvent.InSuccessfulCall = false

	_, err := st.UpsertEvents(ctx, []Event{trueEvent, falseEvent})
	require.NoError(t, err)

	t.Run("filter by true alone", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{InSuccessfulCall: ptr(true),
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.True(t, got[0].InSuccessfulCall)
	})

	t.Run("filter by false alone", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{InSuccessfulCall: ptr(false),
			Scope: WildcardScope(),
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.False(t, got[0].InSuccessfulCall)
	})

	t.Run("nil returns all", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 2)
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
		Scope:      WildcardScope(),
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
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3) // Jul 23, 24, 25 inclusive
	})

	t.Run("to_time only", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ToTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			Scope:  WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2) // Jul 21, 22 inclusive
	})

	t.Run("both bounds inclusive", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			ToTime:   time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3) // Jul 22, 23, 24
	})

	t.Run("intersection with ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromLedger: 104,
			ToLedger:   106,
			FromTime:   time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
			Scope:      WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2) // ledger 104+106, time >= Jul23 -> events 4,5 (ledger 104,105 -> Jul24,25)
	})

	t.Run("empty window returns nothing", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 0)
	})
}

func TestAggregateEvents_ByLedger(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	var events []Event
	for i := 1; i <= 5; i++ {
		e := testEvent(eventID(i), int64(100+(i-1)/2), contractA)
		e.CreatedAt = time.Date(2026, 7, 20+i, 12, 0, 0, 0, time.UTC)
		events = append(events, e)
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	got, err := st.AggregateEvents(ctx, EventFilter{FromLedger: 100, ToLedger: 102, Scope: WildcardScope()}, "ledger")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "100", got[0].Bucket)
	assert.Equal(t, int64(2), got[0].Count) // events 1,2 on ledger 100
	assert.Equal(t, "101", got[1].Bucket)
	assert.Equal(t, int64(2), got[1].Count) // events 3,4 on ledger 101
	assert.Equal(t, "102", got[2].Bucket)
	assert.Equal(t, int64(1), got[2].Count) // event 5 on ledger 102
}

func TestAggregateEvents_ByTime(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	var events []Event
	for i := 1; i <= 4; i++ {
		e := testEvent(eventID(i), int64(100+i), contractA)
		// (i-1)*6 keeps all four inside one 24h bucket: i*6 made the fourth
		// event 24:00, which Go normalises to the next day, so the query
		// correctly returned two buckets and the assertion below failed.
		e.CreatedAt = time.Date(2026, 7, 20, (i-1)*6, 0, 0, 0, time.UTC) // 00:00, 06:00, 12:00, 18:00
		events = append(events, e)
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	got, err := st.AggregateEvents(ctx, EventFilter{FromLedger: 101, ToLedger: 104, Scope: WildcardScope()}, "24h")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "2026-07-20T00:00:00", got[0].Bucket)
	assert.Equal(t, int64(4), got[0].Count)
}

func TestAggregateEvents_Filters(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	events := []Event{
		testEvent(eventID(1), 100, contractA), // contract A
		testEvent(eventID(2), 101, contractB), // contract B
		testEvent(eventID(3), 101, contractA), // contract A again
	}
	for i := range events {
		events[i].Type = "contract"
	}
	events[1].Type = "system"

	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	// contract_id filter
	got, err := st.AggregateEvents(ctx, EventFilter{ContractID: contractA, Scope: WildcardScope()}, "ledger")
	require.NoError(t, err)
	assert.Len(t, got, 2) // ledgers 100 and 101
	totalA := int64(0)
	for _, b := range got {
		totalA += b.Count
	}
	assert.Equal(t, int64(2), totalA)

	// type filter
	got, err = st.AggregateEvents(ctx, EventFilter{Types: []string{"system"}, Scope: WildcardScope()}, "ledger")
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, int64(1), got[0].Count)
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

	// Simulate a pre-partition deployment: drop the partitioned table the
	// initial Migrate built, recreate the legacy (0001 + raw XDR) schema
	// with one event carrying raw XDR, and rewind the migration counter to
	// version 5 so Migrate only re-applies the partitioning migration and
	// later ones. The legacy table must be dropped outright rather than
	// renamed: index names are schema-wide in Postgres, so a rename would
	// leave idx_events_* behind and collide with the legacy CREATE INDEX.
	// Drop the default partition that Migrate() created with the production
	// span so the test's custom span-10 partition setup doesn't collide.
	_, err = pool.Exec(ctx, `
		DO $$DECLARE
			part text;
		BEGIN
			FOR part IN SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'events'::regclass LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || part || ' CASCADE';
			END LOOP;
		END$$`)
	require.NoError(t, err)

	original := testEvent(eventID(1), 100, contractA)
	original.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg=="}
	original.RawValueXDR = "AAAACgAAAAAAAAAB"

	_, err = pool.Exec(ctx, `
		-- Drop the tables migrations 6+ own so the rewinded Migrate can
		-- recreate them; tables from migrations 1-5 stay as they are.
		DROP TABLE IF EXISTS api_keys CASCADE;
		DROP TABLE IF EXISTS delivery_attempts CASCADE;
		DROP TABLE IF EXISTS subscriptions CASCADE;
		DROP TABLE IF EXISTS contract_specs CASCADE;
		DROP TABLE IF EXISTS events CASCADE;
		DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);
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
		UPDATE schema_migrations SET version = 5, dirty = false;
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO events (
			id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			in_successful_call, topics, value, created_at, topics_xdr, value_xdr
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)`,
		original.ID, original.ContractID, original.Ledger, original.Type,
		original.TxHash, original.TxIndex, original.OpIndex,
		original.InSuccessfulCall, original.Topics, original.Value,
		original.CreatedAt, original.RawTopicXDR, original.RawValueXDR,
	)
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
		-- Every table created above legacySchemaMigrationsVersion has to go.
		-- Replaying the series re-runs each CREATE statement, and a plain
		-- CREATE TABLE or CREATE INDEX against a surviving object fails the
		-- migration and leaves the series dirty. CREATE TABLE IF NOT EXISTS
		-- is not enough on its own either: contract_cursors is idempotent but
		-- the index beside it is not. CASCADE takes the indexes with the
		-- table, so dropping every table above v3 covers both.
		--
		-- events is deliberately absent: this test rebuilds it from
		-- events_legacy to prove the upgrade preserves rows.
		DROP TABLE IF EXISTS contract_meta CASCADE;
		DROP TABLE IF EXISTS contract_specs CASCADE;
		DROP TABLE IF EXISTS backfill_state CASCADE;
		DROP TABLE IF EXISTS contract_cursors CASCADE;
		DROP TABLE IF EXISTS dead_letters CASCADE;
		DROP TABLE IF EXISTS replay_state CASCADE;
		DROP TABLE IF EXISTS subscriptions CASCADE;
		DROP TABLE IF EXISTS delivery_attempts CASCADE;
		DROP TABLE IF EXISTS tenant_usage CASCADE;
		DROP TABLE IF EXISTS tenant_watched_contracts CASCADE;
		DROP TABLE IF EXISTS tenant_contract_grants CASCADE;
		DROP TABLE IF EXISTS api_keys CASCADE;
		DROP TABLE IF EXISTS tenants CASCADE;
		DROP TABLE IF EXISTS event_addresses CASCADE;
		DROP TABLE IF EXISTS token_balances CASCADE;
		DROP TABLE IF EXISTS token_balance_state CASCADE;
		DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);
		UPDATE schema_migrations SET version = %d, dirty = false;
	`, legacySchemaMigrationsVersion)
	_, err = pool.Exec(ctx, sqlRerun)
	require.NoError(t, err)

	require.NoError(t, Migrate(dbURL))

	got, err := st.GetEvent(ctx, original.ID, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, original.ContractID, got.ContractID)
	assert.Equal(t, original.RawTopicXDR, got.RawTopicXDR)
	assert.Equal(t, original.RawValueXDR, got.RawValueXDR)

	// The partition migration bakes in the default span (120960 ledgers),
	// so ledger 100 lands in the events_0_120959 partition.
	partitions, err := pool.Query(ctx, `SELECT to_regclass('events_0_120959'), to_regclass('events_100_109')`)
	require.NoError(t, err)
	defer partitions.Close()
	require.True(t, partitions.Next())
	var defaultSpanPartition, tinySpanPartition sql.NullString
	require.NoError(t, partitions.Scan(&defaultSpanPartition, &tinySpanPartition))
	assert.True(t, defaultSpanPartition.Valid, "ledger 100 must be inside the default-span partition")
	assert.False(t, tinySpanPartition.Valid, "the migration does not use the store's test partition span")
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

	// Two destinations for two selected columns: a merge previously left
	// this scanning one, which failed before it could assert anything.
	partitions, err := pool.Query(ctx, `SELECT to_regclass('events_150_159'), to_regclass('events_160_169')`)
	require.NoError(t, err)
	defer partitions.Close()
	require.True(t, partitions.Next())
	var legacyPartition, freshPartition sql.NullString
	require.NoError(t, partitions.Scan(&legacyPartition, &freshPartition))
	assert.True(t, legacyPartition.Valid || freshPartition.Valid,
		"upserting across the span boundary should have created a partition for at least one of the two ranges")
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

func TestIngestionState_LastSuccessfulPoll(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// Initial save without LastSuccessfulPoll should have nil
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 10, LastCursor: "c1"}))
	got, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.LastSuccessfulPoll, "LastSuccessfulPoll is nil when not set")

	// Save with LastSuccessfulPoll set
	now := time.Now().Truncate(time.Millisecond)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: 20,
		LastCursor:         "c2",
		LastSuccessfulPoll: &now,
	}))

	got, err = st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(20), got.LastIngestedLedger)
	require.NotNil(t, got.LastSuccessfulPoll)
	assert.Equal(t, now, *got.LastSuccessfulPoll)

	// Overwrite without LastSuccessfulPoll should keep the old value? No, it should update to nil
	// The UPSERT sets last_successful_poll = EXCLUDED.last_successful_poll, so nil overwrites
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 30, LastCursor: "c3"}))
	got, err = st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.LastSuccessfulPoll, "LastSuccessfulPoll is nil when not provided in save")
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
	got, _, err := st.QueryEvents(ctx, EventFilter{ContractID: contractA, Scope: WildcardScope()})
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

	// Save ingestion state with LastSuccessfulPoll
	now := time.Now().Truncate(time.Millisecond)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: 101,
		LastSuccessfulPoll: &now,
	}))
	require.NoError(t, st.AddWatchedContract(ctx, contractA))

	stats, err := st.Stats(ctx, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.TotalEvents)
	assert.Equal(t, int64(101), stats.LastIngestedLedger)
	assert.Equal(t, int64(100), stats.OldestStoredLedger)
	assert.Equal(t, int64(2), stats.ContractCount)
	assert.Equal(t, int64(1), stats.WatchedContracts)
	assert.Greater(t, stats.TableSizeBytes, int64(0), "table_size_bytes should report the on-disk size of the events table")

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
		Scope:  WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, e1.ID, got[0].ID)
}

// TestCountDeadLetters verifies that CountDeadLetters mirrors the
// ListDeadLetters contract filter: the total is the full match set, with
// and without the per-contract filter, and is what the X-Total-Count
// header is built from.
func TestCountDeadLetters(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	seed := []DeadLetterInput{
		{EventID: "ev-a1", ContractID: contractA, Ledger: 1, Type: "contract", TxHash: "hash-1", Err: errors.New("decode")},
		{EventID: "ev-a2", ContractID: contractA, Ledger: 2, Type: "contract", TxHash: "hash-2", Err: errors.New("decode")},
		{EventID: "ev-b1", ContractID: contractB, Ledger: 1, Type: "contract", TxHash: "hash-3", Err: errors.New("decode")},
	}
	for _, in := range seed {
		_, err := st.DeadLetterEvent(ctx, in)
		require.NoError(t, err)
	}

	total, err := st.CountDeadLetters(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	scoped, err := st.CountDeadLetters(ctx, contractA)
	require.NoError(t, err)
	assert.Equal(t, int64(2), scoped)

	none, err := st.CountDeadLetters(ctx, contractC)
	require.NoError(t, err)
	assert.Equal(t, int64(0), none)
}

// TestCountDeliveryAttempts verifies that CountDeliveryAttempts totals a
// subscription's attempts regardless of the list limit, and that it is
// owner-gated the same way the list is: an owner that cannot see the
// subscription gets ErrNotFound rather than a count.
func TestCountDeliveryAttempts(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	sub, err := st.CreateSubscription(ctx, Subscription{
		URL:     "https://example.com/hook",
		Filters: SubscriptionFilter{ContractID: contractA},
		Secret:  "s",
		Enabled: true,
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := st.RecordDeliveryAttempt(ctx, DeliveryAttempt{
			SubscriptionID: sub.ID,
			EventID:        "ev-1",
			Status:         DeliveryFailed,
			Error:          "timeout",
		})
		require.NoError(t, err)
	}

	total, err := st.CountDeliveryAttempts(ctx, sub.ID, AllSubscriptions())
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)

	_, err = st.CountDeliveryAttempts(ctx, sub.ID, OwnedBy(999))
	require.ErrorIs(t, err, ErrNotFound,
		"an owner that cannot see the subscription must not be able to count its attempts")
}
