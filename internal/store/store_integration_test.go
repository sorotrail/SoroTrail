//go:build integration

package store

// "Migrations up from empty" integration coverage. Calls `testdb.Setup`
// with `store.Migrate` as the migration step, then inspects every
// table the Go code reads or writes. Migration drift from a future
// contributor adding a column is caught here the moment the migration
// is merged.
//
// Idempotence (running Migrate twice against an up-to-date DB) is
// exercised at boot by `cmd/sorotrail/main.go`'s `if err := store.Migrate(...);`
// line; we don't repeat that here so the helper's per-test migration
// is the only migrate call.

import (
	"context"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/testdb"
)

// TestMigrations_ApplyFromEmptyLand asserts that every embedded
// migration applies cleanly against a brand-new database and lands
// the schema the Go codebase depends on.
func TestMigrations_ApplyFromEmptyLand(t *testing.T) {
	pool := testdb.Setup(t, Migrate)
	requireSchema(t, pool)
}

func requireSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	type schemaCheck struct {
		table   string
		columns []string
		indexes []string
	}
	expected := []schemaCheck{
		{
			table: "events",
			columns: []string{
				"id", "contract_id", "ledger", "type", "tx_hash",
				"tx_index", "op_index", "in_successful_call",
				"topics", "value", "created_at",
				"raw_topic_xdr", "raw_value_xdr",
			},
			indexes: []string{
				"idx_events_contract_id",
				"idx_events_ledger",
			},
		},
		{
			table:   "ingestion_state",
			columns: []string{"id", "last_ingested_ledger", "last_cursor", "updated_at"},
		},
		{
			// audit_state is keyed by network, not by the singleton id the
			// table was created with: GetAuditState selects WHERE network = $1
			// and SaveAuditState upserts ON CONFLICT (network). The original
			// id column had CHECK (id = 1) and no default, so it would have
			// rejected those inserts, and 0005_network_column drops it.
			// sqlite.go still uses id = 1 against its own migration series.
			table:   "audit_state",
			columns: []string{"network", "verified_through_ledger", "updated_at"},
		},
		{
			table: "audit_findings",
			columns: []string{
				"id", "from_ledger", "to_ledger",
				"expected_count", "actual_count", "missing_ids",
				"status", "attempts", "last_attempted_at",
				"last_error", "created_at",
			},
		},
		{
			table:   "watched_contracts",
			columns: []string{"contract_id", "added_at"},
		},
		{
			table: "replay_state",
			columns: []string{
				"id", "from_ledger", "to_ledger",
				"last_event_id", "processed", "changed", "skipped",
				"started_at", "updated_at", "completed_at",
			},
		},
	}

	for _, want := range expected {
		t.Run(want.table, func(t *testing.T) {
			got, err := columnNames(ctx, pool, want.table)
			require.NoError(t, err)
			for _, c := range want.columns {
				assert.Contains(t, got, c,
					"migration drift: column %q missing from %q", c, want.table)
			}
			for _, idx := range want.indexes {
				assert.True(t, indexExists(ctx, pool, idx),
					"index %q should exist after migrations", idx)
			}
		})
	}
}

func columnNames(ctx context.Context, pool *pgxpool.Pool, table string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 16)
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func indexExists(ctx context.Context, pool *pgxpool.Pool, index string) bool {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, index,
	).Scan(&exists); err != nil {
		return false
	}
	return exists
}
