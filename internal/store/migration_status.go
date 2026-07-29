package store

import (
	"errors"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// MigrationStatus describes the state of the database migration set.
type MigrationStatus struct {
	Version uint
	Dirty   bool
	Pending []uint
}

// GetMigrationStatus reports the database's current migration version and
// migrations that have not yet been applied. It does not modify the database.
func GetMigrationStatus(databaseURL string) (MigrationStatus, error) {
	migrationSource, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("opening migrations: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", migrationSource, databaseURL)
	if err != nil {
		_ = migrationSource.Close()
		return MigrationStatus{}, fmt.Errorf("creating migration client: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()

	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return MigrationStatus{}, fmt.Errorf("reading migration version: %w", err)
	}
	if errors.Is(err, migrate.ErrNilVersion) {
		version = 0
		dirty = false
	}

	pending, err := migrationVersions(migrationSource, version)
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("reading available migrations: %w", err)
	}

	return MigrationStatus{Version: version, Dirty: dirty, Pending: pending}, nil
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
