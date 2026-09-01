package store

import (
	"context"
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestSQLite_Conformance(t *testing.T) {
	runStoreTests(t, newSQLiteStore)
}

func newSQLiteStore(t *testing.T) Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Apply the full SQLite migration series in version order, mirroring
	// migrateSQLite. Applying only 0001_init (as this helper historically
	// did) would leave later schema additions out — in particular
	// 0003_last_successful_poll's ingestion_state column, which the
	// shared ingestion-state code now reads and writes.
	entries, err := sqliteMigrationsFS.ReadDir("migrations/sqlite")
	if err != nil {
		t.Fatalf("reading sqlite migrations: %v", err)
	}
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)
	for _, name := range upFiles {
		schema, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/" + name)
		if err != nil {
			t.Fatalf("reading sqlite migration %s: %v", name, err)
		}
		if _, err := db.ExecContext(context.Background(), string(schema)); err != nil {
			t.Fatalf("applying sqlite migration %s: %v", name, err)
		}
	}

	// Set up WAL mode for concurrent reads during writes
	if _, err := db.ExecContext(context.Background(), `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("setting WAL mode: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA busy_timeout=5000`); err != nil {
		t.Fatalf("setting busy timeout: %v", err)
	}

	return NewSQLite(db)
}

// TestSQLite_OpenExisting tests that a SQLite database can be opened from a
// file path. Uses a temporary file.
// TestSQLite_IngestionState_LastSuccessfulPoll verifies the SQLite backend
// persists and round-trips the ingester's last-successful-poll timestamp,
// mirroring the Postgres behavior exercised in
// TestIngestionState_LastSuccessfulPoll (postgres_test.go).
func TestSQLite_IngestionState_LastSuccessfulPoll(t *testing.T) {
	st := newSQLiteStore(t)
	ctx := context.Background()

	// Fresh state has no last-successful-poll yet.
	_, err := st.GetIngestionState(ctx)
	require.ErrorIs(t, err, ErrNotFound, "fresh database has no ingestion state")

	// Save without a poll timestamp -> nil on read.
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 10, LastCursor: "c1"}))
	got, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.LastSuccessfulPoll, "LastSuccessfulPoll is nil when not set")

	// Save with a poll timestamp -> round-trips exactly.
	truncated := time.Now().Truncate(time.Millisecond)
	poll := truncated.UTC()
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: 20,
		LastCursor:         "c2",
		LastSuccessfulPoll: &poll,
	}))
	got, err = st.GetIngestionState(ctx)
	require.NoError(t, err)
	require.NotNil(t, got.LastSuccessfulPoll)
	assert.Equal(t, poll, *got.LastSuccessfulPoll)

	// The UPSERT sets last_successful_poll = excluded.last_successful_poll,
	// so a save without it clears the value (never leaves a stale one).
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{LastIngestedLedger: 30, LastCursor: "c3"}))
	got, err = st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.LastSuccessfulPoll, "LastSuccessfulPoll is nil when not provided in save")
}

func TestSQLite_OpenExisting(t *testing.T) {
	f, err := os.CreateTemp("", "sorotrail-*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	if err := Migrate("sqlite:" + path); err != nil {
		t.Fatalf("migrating sqlite: %v", err)
	}

	st := NewSQLite(db)
	ctx := context.Background()

	if err := st.Ping(ctx); err != nil {
		t.Fatalf("pinging sqlite: %v", err)
	}

	events, next, err := st.QueryEvents(ctx, EventFilter{Limit: 10})
	if err != nil {
		t.Fatalf("querying events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
	if next != "" {
		t.Fatalf("expected empty next cursor, got %q", next)
	}
}

// TestSQLite_MigrationsCreateTxHashIndex verifies the sqlite migration
// series lands idx_events_tx_hash, mirroring the Postgres-side index from
// 0015_add_tx_hash_index so tx-hash lookups don't scan the whole events
// table on either backend.
func TestSQLite_MigrationsCreateTxHashIndex(t *testing.T) {
	f, err := os.CreateTemp("", "sorotrail-*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	if err := Migrate("sqlite:" + path); err != nil {
		t.Fatalf("migrating sqlite: %v", err)
	}

	var name string
	err = db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_events_tx_hash'`,
	).Scan(&name)
	if err != nil {
		t.Fatalf("idx_events_tx_hash should exist after migrations: %v", err)
	}
}
