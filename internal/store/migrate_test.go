package store

import (
	"io/fs"
	"path"
	"regexp"
	"strconv"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/stretchr/testify/require"
)

// TestMigrationsLoad checks the embedded migration set the way Migrate does,
// without a database.
//
// Two migrations claiming the same version number is rejected by
// golang-migrate at source-load time, before any connection is opened — so
// every single integration test in this package fails at once, all of them
// reporting a migration error rather than anything about themselves. That is
// a lot of noise for one renumbering, and none of it reproduces locally,
// because the tests that would surface it skip without TEST_DATABASE_URL.
//
// This test needs no database, so it runs everywhere and names the actual
// problem. Two branches adding a migration concurrently is the normal way to
// hit this, and it is invisible in either diff alone.
func TestMigrationsLoad(t *testing.T) {
	_, err := iofs.New(migrationsFS, "migrations")
	require.NoError(t, err, "embedded migrations must load; a duplicate version number fails every integration test in this package at once")
}

// TestMigrationVersionsAreUniqueAndPaired checks the two structural rules
// that golang-migrate depends on and reports the offender by name.
//
// The pairing half matters because a missing .down.sql is not a load error —
// it only surfaces when someone tries to roll back, which is exactly the
// moment nobody wants to discover it.
func TestMigrationVersionsAreUniqueAndPaired(t *testing.T) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	require.NoError(t, err)

	// golang-migrate's iofs source parses <version>_<name>.<direction>.sql.
	namePattern := regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

	type migration struct{ up, down string }
	byVersion := map[int]*migration{}
	nameOf := map[int]string{}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := namePattern.FindStringSubmatch(e.Name())
		require.NotNil(t, m, "%s does not match <version>_<name>.(up|down).sql", e.Name())

		version, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		name, direction := m[2], m[3]

		if existing, ok := nameOf[version]; ok {
			require.Equal(t, existing, name,
				"version %04d is claimed by two different migrations (%s and %s); "+
					"renumber the newer one to the next free slot above the highest existing version",
				version, existing, name)
		}
		nameOf[version] = name

		if byVersion[version] == nil {
			byVersion[version] = &migration{}
		}
		if direction == "up" {
			byVersion[version].up = e.Name()
		} else {
			byVersion[version].down = e.Name()
		}
	}

	require.NotEmpty(t, byVersion, "no migrations found under %s", path.Join("migrations"))

	for version, m := range byVersion {
		require.NotEmpty(t, m.up, "version %04d (%s) has a down migration but no up", version, nameOf[version])
		require.NotEmpty(t, m.down, "version %04d (%s) has an up migration but no down", version, nameOf[version])
	}
}
