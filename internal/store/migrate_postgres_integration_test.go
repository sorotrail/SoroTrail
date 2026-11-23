//go:build integration

package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresIntegration_MigrateWithOptionsAndBatchedMigration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres live-migration integration test")
	}

	opts := MigrationOptions{
		LockTimeout:      5 * time.Second,
		StatementTimeout: 30 * time.Second,
	}
	require.NoError(t, MigrateWithOptions(dsn, opts))

	connCfg, err := buildPostgresMigrateConnConfig(dsn, opts)
	require.NoError(t, err)
	db := stdlib.OpenDB(*connCfg)
	defer db.Close()

	ctx := context.Background()
	var lockTimeout, stmtTimeout string
	require.NoError(t, db.QueryRowContext(ctx, `SHOW lock_timeout`).Scan(&lockTimeout))
	require.NoError(t, db.QueryRowContext(ctx, `SHOW statement_timeout`).Scan(&stmtTimeout))
	assert.Equal(t, "5s", lockTimeout)
	assert.Equal(t, "30s", stmtTimeout)

	_, _ = db.ExecContext(ctx, `DELETE FROM migration_backfill_state WHERE migration_name = 'pg_integration_backfill'`)
	prog, err := RunBatchedMigration(ctx, db, BatchedMigrationConfig{
		Name:             "pg_integration_backfill",
		BatchSize:        25,
		StatementTimeout: 10 * time.Second,
		Step: func(_ context.Context, _ *sql.Tx, cursor string, _ int) (string, int64, bool, error) {
			if cursor == "" {
				return "batch-1", 25, false, nil
			}
			return "batch-2", 10, true, nil
		},
	})
	require.NoError(t, err)
	assert.True(t, prog.Completed)
	assert.Equal(t, 2, prog.BatchesRun)
	assert.Equal(t, int64(35), prog.RowsProcessed)
	assert.Equal(t, "batch-2", prog.LastCursor)
}
