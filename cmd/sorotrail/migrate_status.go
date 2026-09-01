package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// runMigrateStatus implements `sorotrail migrate-status`: a read-only
// command that reports pending migrations without applying them. This
// is the safety check an operator runs before deploying to confirm the
// migration set is stable.
func runMigrateStatus(args []string) error {
	fs := flag.NewFlagSet("migrate-status", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail migrate-status

Reports pending database migrations without applying them. Read-only —
no writes are performed. Exit code 0 means the schema is up to date;
exit code 1 means migrations are pending or the database is dirty.

The command also validates the embedded migration set for structural
problems: duplicate version numbers, missing down files, and migrations
that rebuild a table a later migration depends on.
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed
		}
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	status, err := store.GetMigrationStatus(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("reading migration status: %w", err)
	}

	fmt.Printf("Applied version: %d\n", status.Version)
	fmt.Printf("Dirty:           %v\n", status.Dirty)

	if status.Dirty {
		fmt.Println("\nThe database is in a dirty state. This means a migration")
		fmt.Println("partially applied and then failed. Recovery steps:")
		fmt.Println("")
		fmt.Println("  1. Inspect the schema_migrations table:")
		fmt.Println("     SELECT version, dirty FROM schema_migrations;")
		fmt.Println("")
		fmt.Println("  2. If the version is correct but dirty, mark it clean:")
		fmt.Println("     UPDATE schema_migrations SET dirty = false;")
		fmt.Println("")
		fmt.Println("  3. If the version is wrong, manually set it to the correct")
		fmt.Println("     value and clear dirty:")
		fmt.Println("     UPDATE schema_migrations SET version = N, dirty = false;")
		fmt.Println("")
		fmt.Println("  4. Re-run sorotrail to verify it starts cleanly.")
		fmt.Println("")
		return fmt.Errorf("database is dirty; manual intervention required")
	}

	if len(status.Pending) == 0 {
		fmt.Println("\nNo pending migrations — schema is up to date.")
		return nil
	}

	fmt.Printf("\nPending migrations: %d\n", len(status.Pending))
	for _, v := range status.Pending {
		fmt.Printf("  %04d\n", v)
	}
	fmt.Println("\nRun sorotrail to apply pending migrations (they apply automatically on startup).")
	return fmt.Errorf("%d pending migration(s)", len(status.Pending))
}
