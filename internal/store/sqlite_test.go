package store

import (
	"context"
	"database/sql"
	"os"
	"testing"

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

	// Apply the schema directly
	schema, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/0001_init.up.sql")
	if err != nil {
		t.Fatalf("reading sqlite schema: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), string(schema)); err != nil {
		t.Fatalf("applying sqlite schema: %v", err)
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
