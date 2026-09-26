//go:build integration || !integration

package store

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

// conformanceTestNames lists the behavioural conformance tests, in the order
// they run. A backend's registration refers to these names when it declares
// an operation unsupported, so the two cannot drift out of sync.
var conformanceTestNames = []string{
	"UpsertEvents_Idempotent",
	"GetEvent_NotFound",
	"QueryEvents_FiltersAndPagination",
	"QueryEvents_TimeRange",
	"IngestionStateRoundTrip",
	"WatchedContracts",
	"Stats",
	"RawXDRRoundTrip",
	"ReplaceEventsInRangeKeepsRawXDR",
}

// conformanceBackend is one Store implementation the shared suite runs
// against. Adding a backend is one entry in conformanceBackends; nothing in
// the test bodies below is backend-specific.
type conformanceBackend struct {
	// name is the subtest name and the backend's label in failure output.
	name string
	// factory returns a fresh, migrated store. It is called once per
	// conformance test so each starts from empty tables.
	factory func(t *testing.T) Store
	// skipEnv is the environment variable that must be set for a
	// server-backed backend to be exercised. When it is unset the whole
	// backend is skipped cleanly, never failed, mirroring the Postgres
	// integration tests' TEST_DATABASE_URL convention.
	skipEnv string
	// unsupported names the conformance tests this backend deliberately
	// does not implement. The suite skips them with the backend's own
	// declaration rather than running behavioural assertions against a
	// silent no-op. TestStoreUnsupportedOperations then asserts each such
	// operation returns ErrUnsupported.
	unsupported map[string]bool
}

// conformanceBackends is the single place a new Store implementation is
// registered with the shared suite.
func conformanceBackends() []conformanceBackend {
	// ClickHouse is registered but currently declares the whole behavioural
	// suite unsupported: its Store methods are stubs that return zero values
	// rather than results (tracked separately). Wiring it in here is what
	// makes those gaps visible and reviewable.
	unsupportedAll := make(map[string]bool, len(conformanceTestNames))
	for _, name := range conformanceTestNames {
		unsupportedAll[name] = true
	}

	return []conformanceBackend{
		{
			name:    "sqlite",
			factory: newSQLiteStore,
		},
		{
			name:    "postgres",
			factory: newPostgresConformanceStore,
			skipEnv: "TEST_DATABASE_URL",
		},
		{
			name:        "clickhouse",
			factory:     newClickHouseConformanceStore,
			skipEnv:     "TEST_CLICKHOUSE_URL",
			unsupported: unsupportedAll,
		},
	}
}

// TestStoreConformance runs every backend-agnostic Store test against every
// registered backend. It is the harness that holds each implementation to the
// same semantics, so a backend cannot silently diverge from the reference
// implementation.
func TestStoreConformance(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			runStoreTests(t, backend)
		})
	}
}

// runStoreTests runs the conformance suite against one backend. A backend
// with a required server is skipped as a whole when its URL is unset, and any
// operation it declares unsupported is skipped per test with that declaration.
func runStoreTests(t *testing.T, backend conformanceBackend) {
	t.Helper()

	if backend.skipEnv != "" && os.Getenv(backend.skipEnv) == "" {
		t.Skipf("%s: %s is not set; skipping this backend", backend.name, backend.skipEnv)
	}

	tests := []struct {
		name string
		run  func(t *testing.T, st Store)
	}{
		{"UpsertEvents_Idempotent", testUpsertEventsIdempotent},
		{"GetEvent_NotFound", testGetEventNotFound},
		{"QueryEvents_FiltersAndPagination", testQueryEventsFiltersAndPagination},
		{"QueryEvents_TimeRange", testQueryEventsTimeRange},
		{"IngestionStateRoundTrip", testIngestionStateRoundTrip},
		{"WatchedContracts", testWatchedContracts},
		{"Stats", testStats},
		{"RawXDRRoundTrip", testRawXDRRoundTrip},
		{"ReplaceEventsInRangeKeepsRawXDR", testReplaceEventsInRangeKeepsRawXDR},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if backend.unsupported[tc.name] {
				t.Skipf("%s declares %s unsupported", backend.name, tc.name)
			}
			tc.run(t, backend.factory(t))
		})
	}
}

// newPostgresConformanceStore returns a fresh Postgres store for the shared
// suite. Unlike the integration-tagged helpers it is available to the
// untagged binary, so CI's Postgres-backed job (which sets TEST_DATABASE_URL)
// exercises the suite against Postgres without a second build tag.
func newPostgresConformanceStore(t *testing.T) Store {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres conformance (see CONTRIBUTING.md)")
	}
	require.NoError(t, Migrate(dbURL))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Every conformance test asserts against a known, empty result set, so
	// clear the tables each test reads or writes. Drop every event partition
	// first: sibling tests (especially the integration-tagged ones) create
	// partitions with narrower spans, and a leftover one makes the default
	// span's partition overlap it ("partition would overlap partition").
	_, err = pool.Exec(ctx, `
		DO $$ DECLARE part text;
		BEGIN
			FOR part IN SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'events'::regclass
			LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || part || ' CASCADE';
			END LOOP;
		END $$;`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `TRUNCATE events, ingestion_state, watched_contracts, audit_state`)
	require.NoError(t, err)

	return NewPostgres(pool)
}

// newClickHouseConformanceStore returns a fresh ClickHouse store when a server
// is configured. The backend currently declares the behavioural suite
// unsupported, so this is only reached once those stubs grow real
// implementations; the gate keeps a missing server a skip, never a failure.
func newClickHouseConformanceStore(t *testing.T) Store {
	t.Helper()
	url := os.Getenv("TEST_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("TEST_CLICKHOUSE_URL not set; skipping ClickHouse conformance")
	}
	require.NoError(t, Migrate(url))
	st, err := NewStoreFromURL(url)
	require.NoError(t, err)
	require.NoError(t, st.Ping(context.Background()))
	return st
}

func testUpsertEventsIdempotent(t *testing.T, st Store) {
	ctx := context.Background()
	events := []Event{testEvent(eventID(1), 100, contractA), testEvent(eventID(2), 101, contractA)}
	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	inserted, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(0), inserted)
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
			page, next, err := st.QueryEvents(ctx, EventFilter{Limit: 3, Cursor: cursor, Scope: WildcardScope()})
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
			Scope:         WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 9, "all events with u64:7 (9 out of 10)")
	})

	t.Run("by topic_contains with object directly does not match array", func(t *testing.T) {
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
				Limit:  3,
				Cursor: cursor,
				Order:  "desc",
				Scope:  WildcardScope(),
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
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("to_time only", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			ToTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			Scope:  WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})

	t.Run("both bounds inclusive", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromTime: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
			ToTime:   time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC),
			Scope:    WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("intersection with ledger range", func(t *testing.T) {
		got, _, err := st.QueryEvents(ctx, EventFilter{
			FromLedger: 104,
			ToLedger:   106,
			FromTime:   time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
			Scope:      WildcardScope(),
		})
		require.NoError(t, err)
		assert.Len(t, got, 2)
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
	ids := make([]string, 0, len(got))
	for _, wc := range got {
		ids = append(ids, wc.ContractID)
	}
	assert.Equal(t, []string{contractA, contractB}, ids)
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

// TestStoreUnsupportedOperations asserts that operations a backend does not
// implement fail loudly with ErrUnsupported rather than returning a zero
// value that reads as success. The behavioural suite above exercises what a
// backend does support; this covers what it declares it does not, so a
// capability gap cannot regress back into a silent empty result.
func TestStoreUnsupportedOperations(t *testing.T) {
	ctx := context.Background()

	// SQLite is the backend that declares Postgres-only operations. It needs
	// no server, so this runs unconditionally in CI.
	st := newSQLiteStore(t)

	tests := []struct {
		name  string
		probe func() error
	}{
		{"ListContracts", func() error {
			_, _, err := st.ListContracts(ctx, ContractsFilter{})
			return err
		}},
		{"GetContractSummary", func() error {
			_, err := st.GetContractSummary(ctx, contractA)
			return err
		}},
		{"ContractEventTypeCounts", func() error {
			_, err := st.ContractEventTypeCounts(ctx, contractA)
			return err
		}},
		{"CreateAPIKey", func() error {
			_, err := st.CreateAPIKey(ctx, APIKey{})
			return err
		}},
		{"GetAPIKey", func() error {
			_, err := st.GetAPIKey(ctx, 1)
			return err
		}},
		{"LookupAPIKeyByPrefix", func() error {
			_, err := st.LookupAPIKeyByPrefix(ctx, "prefix")
			return err
		}},
		{"ListAPIKeys", func() error {
			_, err := st.ListAPIKeys(ctx)
			return err
		}},
		{"RevokeAPIKey", func() error {
			return st.RevokeAPIKey(ctx, 1)
		}},
		{"ListContractIDs", func() error {
			_, err := st.ListContractIDs(ctx)
			return err
		}},
		{"GetContractMeta", func() error {
			_, err := st.GetContractMeta(ctx, contractA)
			return err
		}},
		{"UpsertContractMeta", func() error {
			return st.UpsertContractMeta(ctx, ContractMeta{ContractID: contractA})
		}},
		{"CountContractEvents", func() error {
			_, err := st.CountContractEvents(ctx, contractA)
			return err
		}},
		{"ListContractsNeedingRefresh", func() error {
			_, err := st.ListContractsNeedingRefresh(ctx, time.Now())
			return err
		}},
		{"GetContractCursor", func() error {
			_, err := st.GetContractCursor(ctx, contractA)
			return err
		}},
		{"SaveContractCursor", func() error {
			return st.SaveContractCursor(ctx, ContractCursor{ContractID: contractA})
		}},
		{"DeleteContractCursor", func() error {
			return st.DeleteContractCursor(ctx, contractA)
		}},
		{"ListContractCursors", func() error {
			_, err := st.ListContractCursors(ctx)
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.probe()
			require.Error(t, err, "an unimplemented operation must not succeed silently")
			assert.ErrorIs(t, err, ErrUnsupported,
				"the error must wrap ErrUnsupported so callers can react to it")
		})
	}
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

// RunStoreConformanceSuite runs a standard set of error semantic and behavior
// assertions against any Store implementation to guarantee that backends agree
// on ErrNotFound, empty collections, and error handling.
func RunStoreConformanceSuite(t *testing.T, st Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("MissingSingleResourceReturnsErrNotFound", func(t *testing.T) {
		// Querying a non-existent single resource or state must consistently return ErrNotFound.
		_, err := st.GetEvent(ctx, "nonexistent-event-id", WildcardScope())
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("EmptyCollectionReturnsEmptySliceNeverErrNotFound", func(t *testing.T) {
		// Querying collections with no matching records must return an empty result set and nil error.
		contracts, err := st.ListWatchedContracts(ctx)
		require.NoError(t, err)
		assert.Empty(t, contracts)

		// QueryEvents with a scope that matches nothing should return an empty slice and nil error.
		events, _, err := st.QueryEvents(ctx, EventFilter{Scope: NewScope([]string{"nonexistent-contract-id"}), Limit: 10})
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("UnsupportedOrInvalidOperationReturnsExplicitError", func(t *testing.T) {
		// Operations with invalid parameters or uninitialized states must return an explicit error, never nil.
		err := st.AddWatchedContract(ctx, "")
		assert.Error(t, err)
	})
}
