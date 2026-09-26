package store

// Tests for the migration status helpers that run without a database:
// migrationVersions (the pending-version calculation GetMigrationStatus is
// built on) and CountEmbeddedMigrations. The database-backed half of
// GetMigrationStatus lives in migration_status_integration_test.go.

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/golang-migrate/migrate/v4/source/stub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetMigrationStatus resolves its database driver from the URL scheme, but the
// store only registers the "pgx5" scheme that Migrate names explicitly. Tests
// drive GetMigrationStatus with the postgres:// URL form the migrate-status
// command passes, so register the same pgx driver under the postgres scheme for
// this test binary. The guard keeps the init idempotent if production ever adds
// the database/postgres import.
func init() {
	for _, name := range database.List() {
		if name == "postgres" {
			return
		}
	}
	database.Register("postgres", &pgxmigrate.Postgres{})
}

// embeddedVersions returns every embedded migration version, in load order.
func embeddedVersions(t *testing.T) []uint {
	t.Helper()
	src, err := iofs.New(postgresMigrationsFS, "migrations")
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	versions, err := migrationVersions(src, 0)
	require.NoError(t, err)
	return versions
}

// embeddedSource returns a fresh driver over the embedded migration set.
func embeddedSource(t *testing.T) source.Driver {
	t.Helper()
	src, err := iofs.New(postgresMigrationsFS, "migrations")
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// emptySource returns a driver with no migrations at all, standing in for a
// build whose embedded set failed to include anything.
func emptySource(t *testing.T) source.Driver {
	t.Helper()
	src, err := stub.WithInstance(nil, &stub.Config{})
	require.NoError(t, err)
	return src
}

// faultySource is a source.Driver whose First fails with a non-ErrNotExist
// error, standing in for an unreadable migration source. migrationVersions
// must surface that error rather than reporting "nothing pending".
type faultySource struct{ err error }

func (f faultySource) Open(string) (source.Driver, error) { return f, nil }
func (f faultySource) Close() error                       { return nil }
func (f faultySource) First() (uint, error)               { return 0, f.err }
func (f faultySource) Prev(uint) (uint, error)            { return 0, os.ErrNotExist }
func (f faultySource) Next(uint) (uint, error)            { return 0, os.ErrNotExist }
func (f faultySource) ReadUp(uint) (io.ReadCloser, string, error) {
	return nil, "", os.ErrNotExist
}
func (f faultySource) ReadDown(uint) (io.ReadCloser, string, error) {
	return nil, "", os.ErrNotExist
}

func TestMigrationVersions(t *testing.T) {
	all := embeddedVersions(t)
	require.GreaterOrEqual(t, len(all), 2, "need at least two embedded migrations to exercise the remainder")
	head := all[len(all)-1]

	tests := []struct {
		name    string
		source  func(*testing.T) source.Driver
		current uint
		want    []uint
	}{
		{
			name:    "a fully migrated database has no pending versions",
			source:  embeddedSource,
			current: head,
			want:    []uint{},
		},
		{
			name:    "a partially migrated database reports the remainder in order",
			source:  embeddedSource,
			current: all[len(all)-3],
			want:    all[len(all)-2:],
		},
		{
			name:    "an unmigrated database reports every embedded version",
			source:  embeddedSource,
			current: 0,
			want:    all,
		},
		{
			name:    "a source with no migrations has nothing pending",
			source:  emptySource,
			current: 0,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := migrationVersions(tt.source(t), tt.current)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a source read error is surfaced, not swallowed", func(t *testing.T) {
		_, err := migrationVersions(faultySource{err: errors.New("source unavailable")}, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source unavailable")
	})
}

// TestCountEmbeddedMigrations pins the count that the schema-inspect command
// compares against the applied version, and checks it agrees with the source
// driver's view of the same embedded series.
func TestCountEmbeddedMigrations(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	upFiles := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles++
		}
	}

	count, err := CountEmbeddedMigrations()
	require.NoError(t, err)
	assert.Greater(t, count, 0, "there must be embedded migrations to count")
	assert.Equal(t, upFiles, count, "the count must match the embedded .up.sql files")

	all := embeddedVersions(t)
	assert.Equal(t, len(all), count,
		"the count must match the versions the embedded source driver reports")
}

// TestGetMigrationStatus_UnreachableDatabaseIsAnError covers the errored half
// of "an unmigrated database is reported distinctly from an errored one": a
// database that cannot be reached must surface an error rather than being
// mistaken for a schema with nothing applied. It needs no running database —
// the connection is refused immediately on loopback.
func TestGetMigrationStatus_UnreachableDatabaseIsAnError(t *testing.T) {
	status, err := GetMigrationStatus("postgres://sorotrail:secret@127.0.0.1:1/sorotrail?sslmode=disable&connect_timeout=1")
	require.Error(t, err, "an unreachable database must be an error, not an unmigrated status")
	assert.Contains(t, err.Error(), "creating migration client")
	assert.Equal(t, MigrationStatus{}, status, "no partial status is returned alongside the error")
}
