//go:build integration

package store

// Database-backed tests for GetMigrationStatus, the reporting an operator
// reads after a failed deploy: which migration version is applied and whether
// the schema is dirty. They need a real Postgres and skip without
// TEST_DATABASE_URL, like the rest of this package's integration suite
// (see postgres_test.go). Run with:
//
//	go test -tags=integration -run 'TestGetMigrationStatus' ./internal/store/

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func headEmbeddedVersion(t *testing.T) uint {
	t.Helper()
	all := embeddedVersions(t)
	require.NotEmpty(t, all)
	return all[len(all)-1]
}

// TestGetMigrationStatus_FullyMigrated covers the healthy case: after Migrate
// has applied everything, the head version is reported clean and nothing is
// pending.
func TestGetMigrationStatus_FullyMigrated(t *testing.T) {
	url := dbURL(t)
	require.NoError(t, Migrate(url))
	head := headEmbeddedVersion(t)

	status, err := GetMigrationStatus(url)
	require.NoError(t, err)
	assert.Equal(t, head, status.Version, "a fully migrated database reports the embedded head version")
	assert.False(t, status.Dirty, "a completed migration run leaves the version clean")
	assert.Empty(t, status.Pending, "no embedded version remains once the head is applied")
}

// TestGetMigrationStatus_Dirty covers the state a failed migration leaves
// behind: the version is reported together with the dirty flag so an operator
// knows manual intervention is required.
func TestGetMigrationStatus_Dirty(t *testing.T) {
	url := dbURL(t)
	require.NoError(t, Migrate(url))
	head := headEmbeddedVersion(t)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `UPDATE schema_migrations SET dirty = true WHERE version = $1`, head)
	require.NoError(t, err)
	// A dirty schema makes every later Migrate fail, so clear it before any
	// other test runs.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE schema_migrations SET dirty = false WHERE version = $1`, head)
	})

	status, err := GetMigrationStatus(url)
	require.NoError(t, err)
	assert.True(t, status.Dirty, "a version flagged dirty must be reported as dirty")
	assert.Equal(t, head, status.Version, "the dirty version is still reported")
}

// TestGetMigrationStatus_Unmigrated is the other half of the distinction in
// TestGetMigrationStatus_UnreachableDatabaseIsAnError: a reachable database
// with nothing applied is a status (version 0, all versions pending), not an
// error.
func TestGetMigrationStatus_Unmigrated(t *testing.T) {
	url := dbURL(t)
	dropAllObjects(t, url)
	// Leave the shared database migrated so the tests that follow see a schema.
	t.Cleanup(func() {
		if err := Migrate(url); err != nil {
			t.Errorf("re-migrating after the unmigrated-database test: %v", err)
		}
	})

	status, err := GetMigrationStatus(url)
	require.NoError(t, err, "an unmigrated database is a status, not an error")
	assert.Equal(t, uint(0), status.Version, "nothing applied reports version 0")
	assert.False(t, status.Dirty)
	assert.Equal(t, embeddedVersions(t), status.Pending, "every embedded version is pending")
}
