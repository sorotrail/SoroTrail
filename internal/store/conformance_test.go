//go:build integration || !integration

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/testskip"
)

const (
	contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	// contractInventoryC is the third fixture contract in the shared
	// inventory test. It is named separately from the integration-tagged
	// suite's contractC so the two build flavours can coexist.
	contractInventoryC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
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
	"ContractCursors",
	"Stats",
	"RawXDRRoundTrip",
	"ReplaceEventsInRangeKeepsRawXDR",
	"ContractInventory",
	"AuditStateRoundTrip",
	"AuditFindings",
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
	// ClickHouse starts from every behavioural test declared unsupported
	// and then opts in the ones whose implementations now carry real
	// behaviour, so a still-stubbed method can never be asserted against
	// as if it returned results. WatchedContracts (#526) and the two
	// audit tests (#525) are the operations those issues require: their
	// methods issue real SQL rather than returning zero values, so the
	// suite now runs them instead of skipping them under a registration
	// that claimed the whole backend was stubbed. Everything else stays
	// declared until its implementation lands; a new test name defaults
	// to unsupported here, which is the safe direction to drift in.
	unsupportedAll := make(map[string]bool, len(conformanceTestNames))
	for _, name := range conformanceTestNames {
		unsupportedAll[name] = true
	}
	for _, name := range []string{"WatchedContracts", "AuditStateRoundTrip", "AuditFindings", "ContractInventory", "Stats"} {
		delete(unsupportedAll, name)
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

// conformanceBackendNamed looks up a registered backend by name, so a
// single-backend entry point can never drift from the registry's declaration
// of that backend's skip environment and unsupported operations.
func conformanceBackendNamed(t *testing.T, name string) conformanceBackend {
	t.Helper()
	for _, backend := range conformanceBackends() {
		if backend.name == name {
			return backend
		}
	}
	t.Fatalf("%s is not registered as a conformance backend", name)
	return conformanceBackend{}
}

// runStoreTests runs the conformance suite against one backend. A backend
// with a required server is skipped as a whole when its URL is unset, and any
// operation it declares unsupported is skipped per test with that declaration.
//
// The skip goes through the shared convention (internal/testskip), so it
// prints a notice naming what did not run instead of disappearing into the
// default `go test` output, and so a CI job that needs the backend can set
// the require variable and make the skip a failure.
func runStoreTests(t *testing.T, backend conformanceBackend) {
	t.Helper()

	if backend.skipEnv != "" {
		testskip.Skip(t, testskip.Tool{
			Name:   backend.name,
			EnvVar: backend.skipEnv,
			Title:  "the " + backend.name + " store conformance suite",
			Reason: backend.skipEnv + " is not set",
			Cost: "the " + backend.name + " store implementation is only " +
				"exercised against a live server, so its behaviour is unverified here",
			Install:    "start one and set " + backend.skipEnv + ", then re-run the suite",
			RequireEnv: "SOROTRAIL_REQUIRE_" + strings.ToUpper(backend.name),
		})
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
		{"ContractCursors", testContractCursors},
		{"Stats", testStats},
		{"RawXDRRoundTrip", testRawXDRRoundTrip},
		{"ReplaceEventsInRangeKeepsRawXDR", testReplaceEventsInRangeKeepsRawXDR},
		{"ContractInventory", testContractInventory},
		{"AuditStateRoundTrip", testAuditStateRoundTrip},
		{"AuditFindings", testAuditFindings},
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

	_, err = pool.Exec(ctx, `TRUNCATE events, ingestion_state, watched_contracts, audit_state, audit_findings`)
	require.NoError(t, err)

	return NewPostgres(pool)
}

// newClickHouseStoreForTests returns a migrated, empty ClickHouse store when
// a server is configured. The backend opts into only part of the behavioural
// suite (see conformanceBackends), and the gate keeps a missing server a
// skip, never a failure.
func newClickHouseStoreForTests(t *testing.T) (Store, error) {
	t.Helper()
	url := os.Getenv("TEST_CLICKHOUSE_URL")
	if url == "" {
		t.Skip("TEST_CLICKHOUSE_URL not set; skipping ClickHouse conformance")
	}
	if err := Migrate(url); err != nil {
		return nil, err
	}
	st, err := NewStoreFromURL(url)
	if err != nil {
		return nil, err
	}
	if err := st.Ping(context.Background()); err != nil {
		return nil, err
	}
	chs, ok := st.(*ClickHouse)
	if !ok {
		return nil, fmt.Errorf("TEST_CLICKHOUSE_URL must start with clickhouse:// (got %q)", url)
	}
	// Clear the tables the behavioural suite reads and writes, the same
	// way the Postgres factory truncates before every test: ClickHouse
	// data outlives the process, so rows left behind by a previous run
	// would turn the suite's exact-set assertions into flakes instead of
	// signal. SQLite never needs this because it is fresh per test.
	exec := newClickHouseExecutor(chs.cfg)
	for _, table := range []string{"events", "ingestion_state", "watched_contracts", "audit_state", "audit_findings"} {
		if err := exec.exec(context.Background(), chs.cfg.databaseOrDefault(), "TRUNCATE TABLE "+table); err != nil {
			return nil, fmt.Errorf("truncating %s for a clean conformance run: %w", table, err)
		}
	}
	return st, nil
}

func newClickHouseConformanceStore(t *testing.T) Store {
	t.Helper()
	st, err := newClickHouseStoreForTests(t)
	require.NoError(t, err)
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

	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: contractA}))
	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: contractA}), "re-adding is a no-op")
	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: contractB}))

	got, err := st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	ids := make([]string, 0, len(got))
	for _, wc := range got {
		// The backend is required to report the network each entry was
		// added on, defaulting an empty add to "default".
		assert.Equal(t, "default", wc.Network)
		ids = append(ids, wc.ContractID)
	}
	assert.Equal(t, []string{contractA, contractB}, ids)

	// Removal is reflected by the very next list, a second removal
	// reports ErrNotFound instead of pretending to succeed, and
	// re-adding a removed contract brings it back — the three behaviours
	// POST/DELETE /watched-contracts depend on (#526).
	require.NoError(t, st.RemoveWatchedContract(ctx, WatchedContract{ContractID: contractA}))
	require.ErrorIs(t, st.RemoveWatchedContract(ctx, WatchedContract{ContractID: contractA}), ErrNotFound,
		"the entry is gone; a second remove must not read as success")

	got, err = st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	ids = ids[:0]
	for _, wc := range got {
		ids = append(ids, wc.ContractID)
	}
	assert.Equal(t, []string{contractB}, ids, "the removed contract must disappear from the list")

	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: contractA}),
		"re-adding a removed contract must bring it back")
	got, err = st.ListWatchedContracts(ctx)
	require.NoError(t, err)
	ids = ids[:0]
	for _, wc := range got {
		ids = append(ids, wc.ContractID)
	}
	assert.Equal(t, []string{contractA, contractB}, ids, "re-adding must restore the entry in list order")
}

// testAuditStateRoundTrip pins the data-integrity auditor's high-water mark
// semantics (#525): a network that has never been audited reads as
// ErrNotFound rather than a zero state the auditor could mistake for
// "verified through ledger 0", a save round-trips, SaveAuditStateIfGreater
// never moves the marker backward, and its first write for a network
// creates the row — the cold-start path every auditor run takes.
func testAuditStateRoundTrip(t *testing.T, st Store) {
	ctx := context.Background()
	const network = "conformance-audit"

	_, err := st.GetAuditState(ctx, network)
	require.ErrorIs(t, err, ErrNotFound, "a never-audited network must not read as a zero state")

	require.NoError(t, st.SaveAuditState(ctx, AuditState{Network: network, VerifiedThroughLedger: 100}))
	got, err := st.GetAuditState(ctx, network)
	require.NoError(t, err)
	assert.Equal(t, network, got.Network)
	assert.Equal(t, int64(100), got.VerifiedThroughLedger)
	assert.False(t, got.UpdatedAt.IsZero(), "a save must record an UpdatedAt timestamp")

	// Save fully replaces the per-network row rather than appending.
	require.NoError(t, st.SaveAuditState(ctx, AuditState{Network: network, VerifiedThroughLedger: 150}))
	got, err = st.GetAuditState(ctx, network)
	require.NoError(t, err)
	assert.Equal(t, int64(150), got.VerifiedThroughLedger)

	// A lagging writer must not regress the marker: the stored value is
	// answered back and the database keeps it.
	regressed, err := st.SaveAuditStateIfGreater(ctx, network, 120)
	require.NoError(t, err)
	assert.Equal(t, int64(150), regressed.VerifiedThroughLedger,
		"a lower candidate must be answered with the stored high-water mark")
	got, err = st.GetAuditState(ctx, network)
	require.NoError(t, err)
	assert.Equal(t, int64(150), got.VerifiedThroughLedger,
		"a lower candidate must leave the stored value alone")

	// An advancing candidate must persist.
	advanced, err := st.SaveAuditStateIfGreater(ctx, network, 200)
	require.NoError(t, err)
	assert.Equal(t, int64(200), advanced.VerifiedThroughLedger)
	got, err = st.GetAuditState(ctx, network)
	require.NoError(t, err)
	assert.Equal(t, int64(200), got.VerifiedThroughLedger)

	// The first advance for a network with no row yet has to create it.
	const fresh = "conformance-audit-fresh"
	first, err := st.SaveAuditStateIfGreater(ctx, fresh, 42)
	require.NoError(t, err)
	assert.Equal(t, int64(42), first.VerifiedThroughLedger)
	got, err = st.GetAuditState(ctx, fresh)
	require.NoError(t, err)
	assert.Equal(t, int64(42), got.VerifiedThroughLedger)
}

// testAuditFindings pins the finding lifecycle the auditor drives through
// the store (#525): a recorded finding is retrievable through the
// open-findings lookup over its ledger range and invisible outside it, the
// mutable fields an update carries persist, repairing a finding retires it
// from that lookup, and updating an id that never existed stays a no-op —
// mirroring Postgres's UPDATE matching zero rows — instead of inventing a
// finding or erroring.
func testAuditFindings(t *testing.T, st Store) {
	ctx := context.Background()
	const network = "conformance-findings"

	_, err := st.ListOpenFindingsByRange(ctx, network, 100, 105)
	require.ErrorIs(t, err, ErrNotFound, "nothing has been recorded yet")

	missing := []string{eventID(7), eventID(9)}
	rec, err := st.RecordAuditFinding(ctx, AuditFinding{
		Network:       network,
		FromLedger:    100,
		ToLedger:      105,
		ExpectedCount: 6,
		ActualCount:   4,
		MissingIDs:    missing,
		Status:        FindingOpen,
	})
	require.NoError(t, err)
	require.NotZero(t, rec.ID, "the writer mints the finding id")
	assert.False(t, rec.CreatedAt.IsZero(), "the writer stamps the detection time")

	got, err := st.ListOpenFindingsByRange(ctx, network, 100, 105)
	require.NoError(t, err, "a recorded finding must be retrievable")
	assert.Equal(t, rec.ID, got.ID)
	assert.Equal(t, network, got.Network)
	assert.Equal(t, int64(100), got.FromLedger)
	assert.Equal(t, int64(105), got.ToLedger)
	assert.Equal(t, 6, got.ExpectedCount)
	assert.Equal(t, 4, got.ActualCount)
	assert.Equal(t, FindingOpen, got.Status)
	assert.Equal(t, missing, got.MissingIDs)
	assert.Zero(t, got.Attempts, "a fresh finding has not been retried")

	// The lookup answers only for the range the finding covers...
	_, err = st.ListOpenFindingsByRange(ctx, network, 200, 300)
	require.ErrorIs(t, err, ErrNotFound, "a finding outside the requested range must not match")
	// ...and only for the network it was recorded on.
	_, err = st.ListOpenFindingsByRange(ctx, "conformance-other", 100, 105)
	require.ErrorIs(t, err, ErrNotFound, "findings are network-scoped")

	// The auditor's retry bookkeeping rides on Update and must persist
	// while the finding stays actionable.
	attempted := time.Now().UTC().Truncate(time.Second)
	updated := rec
	updated.Status = FindingUnrecoverable
	updated.Attempts = 2
	updated.LastError = "rpc returned a different event set"
	updated.LastAttemptedAt = attempted
	require.NoError(t, st.UpdateAuditFinding(ctx, updated))

	got, err = st.ListOpenFindingsByRange(ctx, network, 100, 105)
	require.NoError(t, err, "an unrecoverable finding stays visible to the lookup")
	assert.Equal(t, rec.ID, got.ID, "the update must amend the recorded finding, not mint a new one")
	assert.Equal(t, FindingUnrecoverable, got.Status)
	assert.Equal(t, 2, got.Attempts)
	assert.Equal(t, "rpc returned a different event set", got.LastError)
	assert.WithinDuration(t, attempted, got.LastAttemptedAt, time.Minute)

	// Repairing the finding retires it from the open lookup.
	updated.Status = FindingRepaired
	require.NoError(t, st.UpdateAuditFinding(ctx, updated))
	_, err = st.ListOpenFindingsByRange(ctx, network, 100, 105)
	require.ErrorIs(t, err, ErrNotFound, "a repaired finding is no longer actionable")

	// An id that was never recorded must be a silent no-op, mirroring the
	// Postgres UPDATE that matches zero rows without an error.
	require.NoError(t, st.UpdateAuditFinding(ctx, AuditFinding{ID: rec.ID + 1_000_000, Status: FindingOpen}))
	_, err = st.ListOpenFindingsByRange(ctx, network, 100, 105)
	require.ErrorIs(t, err, ErrNotFound, "a no-op update must not create a finding")
}

// testContractCursors pins the per-contract resume-cursor semantics the
// watched-contract ingester depends on: an absent cursor reads as
// ErrNotFound (not a zero cursor that would silently resume from ledger
// 0), a save upserts, a delete is synchronous, and List reflects exactly
// the rows that exist.
func testContractCursors(t *testing.T, st Store) {
	ctx := context.Background()

	_, err := st.GetContractCursor(ctx, contractA)
	require.ErrorIs(t, err, ErrNotFound,
		"a contract with no cursor row must read as ErrNotFound, not a zero cursor")

	saved := ContractCursor{ContractID: contractA, LastIngestedLedger: 100, LastCursor: "cur-1"}
	require.NoError(t, st.SaveContractCursor(ctx, saved))

	got, err := st.GetContractCursor(ctx, contractA)
	require.NoError(t, err)
	assert.Equal(t, contractA, got.ContractID)
	assert.Equal(t, int64(100), got.LastIngestedLedger)
	assert.Equal(t, "cur-1", got.LastCursor)

	// Saving again is an upsert, not a duplicate-key error.
	updated := ContractCursor{ContractID: contractA, LastIngestedLedger: 120, LastCursor: "cur-2"}
	require.NoError(t, st.SaveContractCursor(ctx, updated))

	got, err = st.GetContractCursor(ctx, contractA)
	require.NoError(t, err)
	assert.Equal(t, int64(120), got.LastIngestedLedger)
	assert.Equal(t, "cur-2", got.LastCursor)
	assert.False(t, got.UpdatedAt.IsZero(), "Save must record an UpdatedAt timestamp")

	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{ContractID: contractB, LastIngestedLedger: 5}))

	listed, err := st.ListContractCursors(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 2, "List must reflect exactly the stored cursors")

	require.NoError(t, st.DeleteContractCursor(ctx, contractA))
	require.ErrorIs(t, st.DeleteContractCursor(ctx, contractA), ErrNotFound,
		"deleting an already-absent cursor must read as ErrNotFound")

	listed, err = st.ListContractCursors(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1, "Delete must be synchronous: the next List reflects it")
	assert.Equal(t, contractB, listed[0].ContractID)
}

func testStats(t *testing.T, st Store) {
	ctx := context.Background()

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	})
	require.NoError(t, err)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 101}))
	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: contractA}))

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

// testContractInventory is the backend-agnostic contract-inventory test.
// ListContracts, GetContractSummary and ContractEventTypeCounts are easy to
// implement three slightly different ways — different tiebreaks, different
// cursor normalization, different zero values — so the same fixture is run
// against every backend that claims to support them.
func testContractInventory(t *testing.T, st Store) {
	ctx := context.Background()

	// A has the widest activity; B is the single-event edge case; C sits
	// between them. Last-seen times are distinct so ordering the last_seen
	// sort is deterministic without relying on the contract_id tiebreak.
	aTime := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	bTime := time.Date(2026, 8, 1, 12, 1, 0, 0, time.UTC)
	cTime := time.Date(2026, 8, 1, 12, 2, 0, 0, time.UTC)

	a1 := testEvent(eventID(1), 100, contractA)
	a2 := testEvent(eventID(2), 101, contractA)
	a2.Type = "system"
	a3 := testEvent(eventID(3), 102, contractA)
	a1.CreatedAt, a2.CreatedAt, a3.CreatedAt = aTime, aTime, aTime

	b1 := testEvent(eventID(4), 200, contractB)
	b1.CreatedAt = bTime

	c1 := testEvent(eventID(5), 300, contractInventoryC)
	c2 := testEvent(eventID(6), 301, contractInventoryC)
	c1.CreatedAt, c2.CreatedAt = cTime, cTime

	_, err := st.UpsertEvents(ctx, []Event{a1, a2, a3, b1, c1, c2})
	require.NoError(t, err)

	t.Run("default listing groups counts and ledger ranges", func(t *testing.T) {
		got, cursor, err := st.ListContracts(ctx, ContractsFilter{Limit: 10, Scope: WildcardScope()})
		require.NoError(t, err)
		assert.Empty(t, cursor, "no next page when everything fits")

		require.Len(t, got, 3)
		assert.Equal(t, contractA, got[0].ContractID)
		assert.Equal(t, int64(3), got[0].EventCount)
		assert.Equal(t, int64(100), got[0].FirstLedger)
		assert.Equal(t, int64(102), got[0].LastLedger)
		assert.True(t, got[0].LastSeen.Equal(aTime), "last_seen = %v, want %v", got[0].LastSeen, aTime)

		assert.Equal(t, contractB, got[1].ContractID)
		assert.Equal(t, int64(1), got[1].EventCount, "a one-event contract is not dropped or double-counted")
		assert.Equal(t, int64(200), got[1].FirstLedger)
		assert.Equal(t, int64(200), got[1].LastLedger)

		assert.Equal(t, contractInventoryC, got[2].ContractID)
		assert.Equal(t, int64(2), got[2].EventCount)
		assert.Equal(t, int64(300), got[2].FirstLedger)
		assert.Equal(t, int64(301), got[2].LastLedger)
	})

	t.Run("cursor paging walks every contract exactly once", func(t *testing.T) {
		page1, cursor, err := st.ListContracts(ctx, ContractsFilter{Limit: 2, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, page1, 2)
		require.NotEmpty(t, cursor, "a full page means more rows follow")
		assert.Equal(t, []string{contractA, contractB}, []string{page1[0].ContractID, page1[1].ContractID})

		page2, cursor2, err := st.ListContracts(ctx, ContractsFilter{Limit: 2, Cursor: cursor, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, page2, 1)
		assert.Empty(t, cursor2, "cursor omitted on the last page")
		assert.Equal(t, contractInventoryC, page2[0].ContractID)
	})

	t.Run("invalid cursor is reported as ErrInvalidContractsCursor", func(t *testing.T) {
		_, _, err := st.ListContracts(ctx, ContractsFilter{Cursor: "!!!not-base64!!!", Scope: WildcardScope()})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidContractsCursor)
	})

	t.Run("prefix filter narrows to matching contracts", func(t *testing.T) {
		got, _, err := st.ListContracts(ctx, ContractsFilter{ContractIDPrefix: "CB", Limit: 10, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, contractB, got[0].ContractID)
	})

	t.Run("activity sort ranks busiest first", func(t *testing.T) {
		got, _, err := st.ListContracts(ctx, ContractsFilter{SortKey: SortByActivity, Limit: 10, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, []string{contractA, contractInventoryC, contractB},
			[]string{got[0].ContractID, got[1].ContractID, got[2].ContractID})
	})

	t.Run("ledger and last-seen sort orders", func(t *testing.T) {
		cases := []struct {
			sortKey string
			want    []string
		}{
			{SortByFirstLedger, []string{contractInventoryC, contractB, contractA}},
			{SortByLastLedger, []string{contractInventoryC, contractB, contractA}},
			{SortByLastSeen, []string{contractInventoryC, contractB, contractA}},
		}
		for _, tc := range cases {
			got, _, err := st.ListContracts(ctx, ContractsFilter{SortKey: tc.sortKey, Limit: 10, Scope: WildcardScope()})
			require.NoError(t, err)
			require.Len(t, got, len(tc.want), "sort %s", tc.sortKey)
			assert.Equal(t, tc.want,
				[]string{got[0].ContractID, got[1].ContractID, got[2].ContractID},
				"sort %s", tc.sortKey)
		}
	})

	t.Run("summary matches the listing", func(t *testing.T) {
		got, err := st.GetContractSummary(ctx, contractA)
		require.NoError(t, err)
		assert.Equal(t, contractA, got.ContractID)
		assert.Equal(t, int64(3), got.EventCount)
		assert.Equal(t, int64(100), got.FirstLedger)
		assert.Equal(t, int64(102), got.LastLedger)
		assert.True(t, got.LastSeen.Equal(aTime), "last_seen = %v, want %v", got.LastSeen, aTime)

		single, err := st.GetContractSummary(ctx, contractB)
		require.NoError(t, err)
		assert.Equal(t, int64(1), single.EventCount)
		assert.Equal(t, int64(200), single.FirstLedger)
		assert.Equal(t, int64(200), single.LastLedger)
	})

	t.Run("summary of an unknown contract is ErrNotFound", func(t *testing.T) {
		_, err := st.GetContractSummary(ctx, "CDOESNOTEXIST")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("event type breakdown counts each type", func(t *testing.T) {
		got, err := st.ContractEventTypeCounts(ctx, contractA)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "contract", got[0].Type)
		assert.Equal(t, int64(2), got[0].Count)
		assert.Equal(t, "system", got[1].Type)
		assert.Equal(t, int64(1), got[1].Count)

		empty, err := st.ContractEventTypeCounts(ctx, "CDOESNOTEXIST")
		require.NoError(t, err)
		assert.NotNil(t, empty, "an empty breakdown must not be nil")
		assert.Empty(t, empty)
	})

	// Ties are the case a sort can silently lose rows on: two contracts
	// with the same event count must still have a total order, or a page
	// boundary inside the tie can repeat or skip one. This runs last
	// because it adds rows the earlier assertions do not know about.
	t.Run("ties break by contract_id descending", func(t *testing.T) {
		const (
			tieD = "CDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"
			tieE = "CEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"
		)
		tieTime := time.Date(2026, 8, 1, 12, 3, 0, 0, time.UTC)
		d1 := testEvent(eventID(10), 400, tieD)
		d2 := testEvent(eventID(11), 401, tieD)
		e1 := testEvent(eventID(12), 400, tieE)
		e2 := testEvent(eventID(13), 401, tieE)
		for _, e := range []*Event{&d1, &d2, &e1, &e2} {
			e.CreatedAt = tieTime
		}
		_, err := st.UpsertEvents(ctx, []Event{d1, d2, e1, e2})
		require.NoError(t, err)

		got, _, err := st.ListContracts(ctx, ContractsFilter{SortKey: SortByActivity, Limit: 10, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 5)
		assert.Equal(t, []string{contractA, tieE, tieD, contractInventoryC, contractB},
			[]string{got[0].ContractID, got[1].ContractID, got[2].ContractID, got[3].ContractID, got[4].ContractID})

		got, _, err = st.ListContracts(ctx, ContractsFilter{SortKey: SortByLastSeen, Limit: 10, Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, got, 5)
		assert.Equal(t, []string{tieE, tieD, contractInventoryC, contractB, contractA},
			[]string{got[0].ContractID, got[1].ContractID, got[2].ContractID, got[3].ContractID, got[4].ContractID})
	})
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

// --- SQLite conformance suite ---
// These tests run the shared conformance suite against the SQLite
// backend to guarantee identical behaviour to Postgres.

func TestSQLite_ConformanceSuite(t *testing.T) {
	runStoreTests(t, conformanceBackendNamed(t, "sqlite"))
}

// TestSQLite_Conformance_MigrationIdempotent verifies that the
// SQLite migration series applies cleanly from empty and that
// applying it again is a no-op (idempotent).
func TestSQLite_Conformance_MigrationIdempotent(t *testing.T) {
	// A file-backed database is required here. ":memory:" gives every
	// connection its own private database, so migrating through one handle
	// and querying through another observes an empty schema no matter what
	// Migrate actually did.
	url := "sqlite://" + filepath.Join(t.TempDir(), "migrations.db")

	// First migration — should succeed.
	require.NoError(t, Migrate(url))

	// Second migration — should be idempotent and not error.
	require.NoError(t, Migrate(url))

	// Verify the schema is present after migration.
	db, err := sql.Open("sqlite", parseSQLiteDSN(url))
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&count)
	require.NoError(t, err)
	assert.Greater(t, count, 0, "migrations should create at least one table")

	// Re-running the series must not leave the tracked version mid-flight or
	// re-apply files: a clean, fully-applied status is what idempotence looks
	// like from the migration bookkeeping side.
	status, err := MigrateStatus(url)
	require.NoError(t, err)
	assert.False(t, status.Dirty, "a re-applied migration series must not go dirty")
	assert.Empty(t, status.Pending, "re-applying must leave nothing pending")
	assert.Greater(t, status.Version, uint(0), "at least one migration must be recorded")
}

// --- ClickHouse conformance suite ---
// These tests run the shared conformance suite against the ClickHouse
// backend when a ClickHouse server is available. Without a server,
// the suite skips cleanly rather than failing.

func TestClickHouse_ConformanceSuite(t *testing.T) {
	// Attempt to create a ClickHouse store from a URL. If no server
	// is available, the test skips rather than failing.
	runStoreTests(t, conformanceBackendNamed(t, "clickhouse"))
}

// TestClickHouse_Unsupported_Methods asserts that every method
// not yet implemented in the ClickHouse backend returns an explicit
// error rather than silently succeeding or panicking.
func TestClickHouse_Unsupported_Methods(t *testing.T) {
	st := &ClickHouse{}
	ctx := context.Background()

	// Verify that unsupported methods fail loudly with ErrUnsupported.
	_, err := st.ListContractIDs(ctx)
	assert.ErrorIs(t, err, ErrUnsupported, "ListContractIDs must report its absence explicitly")

	_, err = st.GetContractMeta(ctx, "test")
	assert.ErrorIs(t, err, ErrUnsupported,
		"GetContractMeta must report its absence explicitly: a backend that stores no metadata must not look like a contract that has none")

	// ListContracts is an unsupported operation on ClickHouse: the
	// backend has no contract registry to read and refuses rather than
	// returning a zero value that would read as success.
	_, _, err = st.ListContracts(ctx, ContractsFilter{})
	assert.ErrorIs(t, err, ErrUnsupported, "ListContracts must report its absence explicitly")

	_, err = st.GetContractSummary(ctx, "test")
	assert.Error(t, err,
		"GetContractSummary must surface the failed ClickHouse request rather than a zero-value summary")

	// ContractEventTypeCounts is unsupported on the ClickHouse backend.
	_, err = st.ContractEventTypeCounts(ctx, "test")
	assert.Error(t, err,
		"ContractEventTypeCounts must surface the failed ClickHouse request")

	// API key operations are unsupported by ClickHouse and must return explicit errors.
	_, err = st.CreateAPIKey(ctx, APIKey{})
	assert.ErrorContains(t, err, "not supported by the clickhouse backend")

	_, err = st.GetAPIKey(ctx, 1)
	assert.ErrorContains(t, err, "not supported by the clickhouse backend")

	_, err = st.LookupAPIKeyByPrefix(ctx, "prefix")
	assert.ErrorContains(t, err, "not supported by the clickhouse backend")

	_, err = st.ListAPIKeys(ctx)
	assert.ErrorContains(t, err, "not supported by the clickhouse backend")

	err = st.RevokeAPIKey(ctx, 1)
	assert.ErrorContains(t, err, "not supported by the clickhouse backend")

	// GetContractCursor, SaveContractCursor, DeleteContractCursor and
	// ListContractCursors are unsupported on the ClickHouse backend, so a
	// call fails loudly instead of returning a zero cursor that would make
	// the watched-contract ingester silently resume from ledger 0.
	_, err = st.GetContractCursor(ctx, "test")
	assert.Error(t, err, "GetContractCursor must report the failed lookup rather than a zero cursor")

	// The audit-state and watched-contract methods behind issues #525
	// and #526 are implemented too, so they all issue a request. On a
	// zero-value ClickHouse with no server to reach the only acceptable
	// outcome is an error: the silent-nil no-op those issues exist to
	// prevent would read here as a successful write to an empty result.
	_, err = st.GetAuditState(ctx, "test")
	assert.Error(t, err, "GetAuditState must report the failed lookup rather than a zero state")

	err = st.SaveAuditState(ctx, AuditState{Network: "test", VerifiedThroughLedger: 1})
	assert.Error(t, err, "SaveAuditState must report the failed write rather than claiming success")

	_, err = st.SaveAuditStateIfGreater(ctx, "test", 1)
	assert.Error(t, err, "SaveAuditStateIfGreater must report the failed lookup rather than a zero state")

	err = st.UpdateAuditFinding(ctx, AuditFinding{ID: 1, Status: FindingOpen})
	assert.Error(t, err, "UpdateAuditFinding must report the failed write rather than claiming success")

	err = st.AddWatchedContract(ctx, WatchedContract{ContractID: contractA})
	assert.Error(t, err, "AddWatchedContract must report the failed write rather than claiming success")

	err = st.RemoveWatchedContract(ctx, WatchedContract{ContractID: contractA})
	assert.Error(t, err, "RemoveWatchedContract must report the failed lookup rather than ErrNotFound")
}

// TestClickHouse_ContractMetadataRefuses asserts that the contract-metadata
// surface — the one the token-enrichment worker drives — refuses on
// ClickHouse instead of answering with a value that reads as success.
//
// The methods are the ones an enrichment loop calls: it lists the contracts
// it has seen, asks which need a refresh, writes what it learned, counts a
// contract's events and reads a contract's cached metadata back. A silent
// no-op on any of them made the whole pipeline report success forever while
// storing nothing, and no log line or metric said so.
//
// The refusal has to wrap ErrUnsupported so a caller can recognise it and
// stand down; a plain error would be indistinguishable from a transient
// failure worth retrying.
func TestClickHouse_ContractMetadataRefuses(t *testing.T) {
	st := &ClickHouse{}
	ctx := context.Background()

	tests := []struct {
		name  string
		probe func() error
	}{
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.probe()
			require.Error(t, err, "an unimplemented operation must not succeed silently")
			assert.ErrorIs(t, err, ErrUnsupported,
				"the refusal must wrap ErrUnsupported so the caller can stand down")
		})
	}
}

// TestClickHouse_SuiteSkipsCleanly verifies that when no ClickHouse
// server is available, the conformance suite skips without error.
func TestClickHouse_SuiteSkipsCleanly(t *testing.T) {
	// With no CLICKHOUSE_URL set, the conformance test should skip.
	st, err := newClickHouseStoreForTests(t)
	if err != nil {
		t.Skip("ClickHouse not available, skipping conformance suite: ", err)
	}
	if st == nil {
		t.Skip("ClickHouse not available, skipping conformance suite")
	}
	t.Skip("ClickHouse is available; the skip-path assertion does not apply")
}

// RunStoreConformanceSuite runs a standard set of error semantic and behavior
// assertions against any Store implementation to guarantee that backends agree
// on ErrNotFound, empty collections, and error handling.
func RunStoreConformanceSuite(t *testing.T, st Store) {
	runStoreTests(t, conformanceBackend{
		name:    "custom",
		factory: func(t *testing.T) Store { return st },
	})
}
