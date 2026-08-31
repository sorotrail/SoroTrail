//go:build integration

// Package testdb offers a shared Postgres helper for integration tests.
//
// Resolution rules:
//
//   - TEST_DATABASE_URL set: migrates and truncates against the shared
//     database. Tests across packages share the DB; run with `go test -p 1`
//     (already the case in `make test-integration`).
//   - TEST_DATABASE_URL unset: spins up an ephemeral Postgres 16-alpine
//     container per Setup call via testcontainers-go. Each test gets its
//     own container; truncation is unnecessary because nothing is shared.
//   - Neither path works (no Docker for testcontainers, env var unset):
//     the helper `t.Skip` with a message that points at CONTRIBUTING.md.
//
// Never point TEST_DATABASE_URL at a database you care about — the
// shared path truncates the events / ingestion_state / watched_contracts
// / replay_state / audit_* tables.
//
// The cycle that this helper would otherwise create — package store
// tests importing testdb while testdb imports store for store.Migrate —
// is avoided because `Setup` takes the migration step as an injected
// function (`migrate func(url string) error`). The caller supplies
// `store.Migrate`; the helper never imports internal/store directly.
package testdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Setup returns a migrated *pgxpool.Pool scoped to the test. If migrate is
// nil the caller accepts responsibility for having migrated the database;
// if it's set, run it before the (optional) troncation.
//
// On hosts without Docker / testcontainers support, t.Skip is called.
func Setup(t *testing.T, migrate func(url string) error) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	url := os.Getenv("TEST_DATABASE_URL")
	if url != "" {
		return setupShared(t, ctx, url, migrate)
	}
	return setupContainer(t, ctx, migrate)
}

// setupShared uses a runner-provided Postgres. Apply migrate() first so a
// store built fresh from migrations — covering the schema-up property the
// issue asks for — works, then TRUNCATE wipes any earlier test's data.
func setupShared(t *testing.T, ctx context.Context, url string, migrate func(url string) error) *pgxpool.Pool {
	t.Helper()
	if migrate != nil {
		if err := migrate(url); err != nil {
			t.Fatalf("testdb: migrating shared Postgres: %v", err)
		}
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("testdb: connecting to shared Postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := truncateAll(ctx, pool); err != nil {
		t.Fatalf("testdb: truncating shared Postgres: %v", err)
	}
	t.Cleanup(func() {
		// Use a background context so cleanup runs even if the test's
		// context timed out.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = truncateAll(ctx, pool)
	})
	return pool
}

// setupContainer brings up an ephemeral Postgres 16-alpine container with
// a fresh database. The postgres module ships its own default readiness
// wait strategy; we don't redeclare it here so a startup failure surfaces
// the underlying error rather than masking it behind a duplicate probe.
func setupContainer(t *testing.T, ctx context.Context, migrate func(url string) error) *pgxpool.Pool {
	t.Helper()
	pgC, err := tcpostgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("testdb: Docker not available for testcontainers; set TEST_DATABASE_URL to run this test against an existing Postgres (see CONTRIBUTING.md). Cause: %v", err)
		}
		t.Fatalf("testdb: starting Postgres container: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := pgC.Terminate(stopCtx); err != nil {
			t.Logf("testdb: terminating Postgres container: %v", err)
		}
	})

	url, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("testdb: reading Postgres connection string: %v", err)
	}
	if migrate != nil {
		if err := migrate(url); err != nil {
			t.Fatalf("testdb: migrating ephemeral Postgres: %v", err)
		}
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("testdb: connecting to ephemeral Postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// isDockerUnavailable matches the error shape testcontainers-go returns
// when Docker is missing or the caller has no permission to talk to it.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"could not find a working docker socket",
		"failed to create docker client",
		"permission denied",
		"cannot connect to the docker daemon",
	} {
		if strings.Contains(strings.ToLower(msg), strings.ToLower(needle)) {
			return true
		}
	}
	return false
}

// truncateAll wipes the tables every package's tests touch. New tables
// added in later migrations belong here.
//
// It also drops every non-default events partition: range partitions are
// DDL, so TRUNCATE clears their rows but leaves them behind, and the
// store package's partition-span experiments (e.g. span=10) can leave a
// narrow child that a later package's default-span (120960) partition
// would overlap ("partition would overlap partition", SQLSTATE 42P17).
// Dropping them here keeps every package's DB-backed tests starting from
// the clean layout Migrate produced, regardless of what a previous
// package did.
func truncateAll(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `TRUNCATE
		events,
		ingestion_state,
		audit_state,
		audit_findings,
		watched_contracts,
		replay_state
		RESTART IDENTITY`)
	if err != nil {
		return fmt.Errorf("truncating tables: %w", err)
	}
	_, err = pool.Exec(ctx, `
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
	if err != nil {
		return fmt.Errorf("dropping leftover event partitions: %w", err)
	}
	return nil
}
