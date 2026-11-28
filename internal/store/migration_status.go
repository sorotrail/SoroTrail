package store

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
)

// MigrationStatus describes the state of the database migration set.
type MigrationStatus struct {
	Version uint
	Dirty   bool
	Pending []uint
}

// GetMigrationStatus reports the database's current migration version and the
// migrations that have not yet been applied. It does not modify the database.
//
// It is the historical entry point for this information and now delegates to
// MigrateStatus, which works for both the Postgres and SQLite series (the
// previous implementation only handled a URL registered with the default
// golang-migrate database drivers).
func GetMigrationStatus(databaseURL string) (MigrationStatus, error) {
	return MigrateStatus(databaseURL)
}

// CountEmbeddedMigrations returns the total number of embedded migration
// versions (the count of .up.sql files). This is used by the schema-inspect
// command to compare the applied version against the embedded set without
// opening a database connection.
func CountEmbeddedMigrations() (int, error) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		return 0, fmt.Errorf("reading embedded migrations: %w", err)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			count++
		}
	}
	return count, nil
}

// EmbeddedMigrationVersions returns the actual version numbers of embedded
// Postgres .up.sql migration files in ascending order. Unlike
// CountEmbeddedMigrations, this correctly handles non-sequential version
// numbers (gaps introduced when multiple PRs branch from the same base and
// each adds a migration with a different version number).
func EmbeddedMigrationVersions() ([]uint, error) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("reading embedded migrations: %w", err)
	}
	var versions []uint
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			var v uint
			if _, scanErr := fmt.Sscanf(e.Name(), "%d_", &v); scanErr == nil {
				versions = append(versions, v)
			}
		}
	}
	return versions, nil
}

func migrationVersions(migrationSource source.Driver, current uint) ([]uint, error) {
	first, err := migrationSource.First()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	versions := make([]uint, 0)
	for version := first; ; {
		if version > current {
			versions = append(versions, version)
		}

		next, err := migrationSource.Next(version)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return nil, err
		}
		version = next
	}
	return versions, nil
}
