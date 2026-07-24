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
	"os"
	"testing"

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

	_, err = pool.Exec(context.Background(),
		`TRUNCATE events, ingestion_state, watched_contracts, replay_state`)
	require.NoError(t, err)
	return NewPostgres(pool, span)
}

func TestPostgres_Conformance(t *testing.T) {
	runStoreTests(t, func(t *testing.T) Store {
		return testStore(t)
	})
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

func TestMigrate_UpgradesLegacyEventsTable(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration tests (see CONTRIBUTING.md)")
	}
	require.NoError(t, Migrate(dbURL))

	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	st := NewPostgres(pool, 10)
	ctx := context.Background()

	original := testEvent(eventID(1), 100, contractA)
	original.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg=="}
	original.RawValueXDR = "AAAACgAAAAAAAAAB"
	_, err = st.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		ALTER TABLE events RENAME TO events_partitioned;
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
			value_xdr          text
		);
		CREATE INDEX idx_events_contract_id ON events (contract_id);
		CREATE INDEX idx_events_ledger ON events (ledger);
		CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
		CREATE INDEX idx_events_topics ON events USING gin (topics);
		CREATE INDEX idx_events_created_at ON events (created_at);
		INSERT INTO events (
			id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			in_successful_call, topics, value, created_at, topics_xdr, value_xdr
		)
		SELECT
			id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			in_successful_call, topics, value, created_at, topics_xdr, value_xdr
		FROM events_partitioned
		ORDER BY ledger, id;
		DROP TABLE events_partitioned CASCADE;
		DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);
		UPDATE schema_migrations SET version = 3, dirty = false;
	`)
	require.NoError(t, err)

	require.NoError(t, Migrate(dbURL))

	got, err := st.GetEvent(ctx, original.ID)
	require.NoError(t, err)
	assert.Equal(t, original.ContractID, got.ContractID)
	assert.Equal(t, original.RawTopicXDR, got.RawTopicXDR)
	assert.Equal(t, original.RawValueXDR, got.RawValueXDR)

	partitions, err := pool.Query(ctx, `SELECT to_regclass('events_100_109'), to_regclass('events_110_119')`)
	require.NoError(t, err)
	defer partitions.Close()
	require.True(t, partitions.Next())
	var firstPartition, secondPartition sql.NullString
	require.NoError(t, partitions.Scan(&firstPartition, &secondPartition))
	assert.True(t, firstPartition.Valid)
	assert.False(t, secondPartition.Valid)
}

func TestPostgres_StatsWithExplain(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	})
	require.NoError(t, err)

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
	assert.Contains(t, plan, "Index")
	assert.Contains(t, plan, "ledger")
}
