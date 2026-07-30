package store

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var postgresMigrationsFS embed.FS

//go:embed migrations/sqlite/*.sql
var sqliteMigrationsFS embed.FS

// Migrate applies all pending migrations to the database at databaseURL.
// It detects the dialect from the URL scheme and runs the appropriate
// migration series. Safe to call on every startup; up-to-date schema is a no-op.
func Migrate(databaseURL string) error {
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		return nil
	}
	if !strings.HasPrefix(databaseURL, "postgres://") && !strings.HasPrefix(databaseURL, "postgresql://") {
		return fmt.Errorf("unsupported database url scheme")
	}

	src, err := iofs.New(postgresMigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("loading embedded migrations: %w", err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("opening migration connection: %w", err)
	}
	defer db.Close()

	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return fmt.Errorf("initializing migration driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("initializing migrations: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("applying migrations: %w", err)
	}
	return nil
}

// migrateSQLite runs SQLite migrations directly without golang-migrate.
// The golang-migrate sqlite3 driver pulls in mattn/go-sqlite3 (CGO), so we
// run the embedded SQL files using modernc.org/sqlite (pure Go, CGO-free).
//
// SQLite is only used for fresh deployments, so a single init migration is
// sufficient. The migration tracks applied files via a schema_migrations
// table for consistency with the Postgres path, but version-level rollback
// is not supported — SQLite is not the production target.
func migrateSQLite(databaseURL string) error {
	dsn := parseSQLiteDSN(databaseURL)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("opening sqlite for migration: %w", err)
	}
	defer db.Close()

	// Ensure the migration tracking table exists.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, dirty INTEGER NOT NULL DEFAULT 0)`); err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	entries, err := sqliteMigrationsFS.ReadDir("migrations/sqlite")
	if err != nil {
		return fmt.Errorf("reading sqlite migrations: %w", err)
	}

	// Collect and sort up-migration files.
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)

	for _, f := range upFiles {
		// Extract version from filename (e.g. "0001_init.up.sql").
		version := 0
		if _, err := fmt.Sscanf(f, "%d", &version); err != nil || version == 0 {
			continue
		}

		// Check if already applied.
		var existing int
		if err := db.QueryRow(`SELECT version FROM schema_migrations WHERE version = ?`, version).Scan(&existing); err == nil {
			continue
		}

		content, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/" + f)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", f, err)
		}

		if _, err := db.Exec(string(content)); err != nil {
			return fmt.Errorf("applying migration %s: %w", f, err)
		}

		if _, err := db.Exec(`INSERT INTO schema_migrations (version, dirty) VALUES (?, 0)`, version); err != nil {
			return fmt.Errorf("recording migration %s: %w", f, err)
		}
	}
	return nil
}

// parseSQLiteDSN strips the sqlite:// scheme prefix from a
// database URL, returning the file path for modernc.org/sqlite.
func parseSQLiteDSN(databaseURL string) string {
	const prefix = "sqlite://"
	if strings.HasPrefix(databaseURL, prefix) {
		return databaseURL[len(prefix):]
	}
	return databaseURL
}
