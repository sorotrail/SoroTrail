//go:build integration

package store

// Migration reversibility and idempotency tests.
//
// These integration tests exercise every Postgres migration end-to-end and
// verify the invariants listed in issue #818:
//
//   - up from empty to head succeeding
//   - down from head to zero succeeding
//   - up again after down producing an identical schema
//   - each down reversing exactly what its up did
//   - no migration dropping or rebuilding a table a later one depends on
//
// Run:
//
//	go test -tags=integration -run 'TestMigrat.*' ./internal/store/

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- helpers ----------

// migrateVersions extracts sorted unique version numbers from the embedded
// migration filenames.
func migrateVersions(t *testing.T) []int {
	t.Helper()
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	require.NoError(t, err)

	seen := map[int]bool{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		var v int
		_, err := fmt.Sscanf(e.Name(), "%d", &v)
		require.NoError(t, err)
		seen[v] = true
	}

	versions := make([]int, 0, len(seen))
	for v := range seen {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	return versions
}

// dbURL returns the test database URL, skipping the test if unset.
func dbURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("TEST_DATABASE_URL")
	if u == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping migration reversibility tests")
	}
	return u
}

// newMigrateToVersion creates a fresh *migrate.Migrate pointing at the given
// database URL. Every call creates independent source and database drivers,
// so the caller can close without affecting other instances.
func newMigrateToVersion(t *testing.T, url string) *migrate.Migrate {
	t.Helper()
	src, err := iofs.New(postgresMigrationsFS, "migrations")
	require.NoError(t, err)

	db, err := sql.Open("pgx", url)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	drv, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	require.NoError(t, err)

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", drv)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = m.Close()
	})
	return m
}

// applyToVersion brings the database to the given migration version. It
// applies all migrations first, then steps down to the target if necessary.
// On an up-to-date DB it returns ErrNoChange, which is fine.
func applyToVersion(t *testing.T, url string, targetVersion uint) {
	t.Helper()
	m := newMigrateToVersion(t, url)
	// Migrate applies all up migrations.
	err := m.Up()
	if err != nil && err != migrate.ErrNoChange {
		require.NoError(t, err, "applying all migrations")
	}

	currentVer, _, err := m.Version()
	require.NoError(t, err, "reading current version")

	if currentVer > targetVersion {
		steps := int(currentVer - targetVersion)
		require.NoError(t, m.Steps(-steps), "stepping down to version %d", targetVersion)
	} else if currentVer < targetVersion {
		steps := int(targetVersion - currentVer)
		require.NoError(t, m.Steps(steps), "stepping up to version %d", targetVersion)
	}
}

// resetDatabase drops every user-created table and function so each test
// starts from a completely clean state. It also cleans up partitioned-table
// children so later tests don't hit "partition would overlap" errors.
func resetDatabase(t *testing.T, url string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Drop all non-system tables in the public schema.
	rows, err := pool.Query(context.Background(),
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	require.NoError(t, err)

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())

	for _, tbl := range tables {
		_, err := pool.Exec(context.Background(),
			fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, tbl))
		require.NoError(t, err)
	}

	// Drop functions created by migrations (e.g. ensure_event_partitions).
	_, err = pool.Exec(context.Background(),
		`DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint)`)
	require.NoError(t, err)

	// Clean the migration tracking table.
	_, err = pool.Exec(context.Background(),
		`DROP TABLE IF EXISTS schema_migrations`)
	require.NoError(t, err)

	// Drop indexes that may remain from partitioned tables.
	idxRows, err := pool.Query(context.Background(),
		`SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
		 AND indexname LIKE 'idx_%'`)
	require.NoError(t, err)
	for idxRows.Next() {
		var idx string
		require.NoError(t, idxRows.Scan(&idx))
		_, _ = pool.Exec(context.Background(),
			fmt.Sprintf(`DROP INDEX IF EXISTS %s CASCADE`, idx))
	}
	require.NoError(t, idxRows.Err())
}

// dbSnapshot captures a deterministic hash of the database schema. It hashes
// tables (sorted), their columns (sorted), and indexes (sorted) so two
// snapshots are comparable via simple byte comparison. The hash is stable
// across runs on the same schema state and changes when the schema differs.
func dbSnapshot(t *testing.T, url string) []byte {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var parts []string

	// Tables and columns, ordered deterministically.
	tblRows, err := pool.Query(context.Background(), `
		SELECT c.table_name, c.column_name, c.data_nullable,
		       c.column_default, c.character_maximum_length
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
		WHERE c.table_schema = 'public'
		ORDER BY c.table_name, c.ordinal_position`)
	require.NoError(t, err)
	for tblRows.Next() {
		var tbl, col, nullable string
		var defaultVal, charMax sql.NullString
		require.NoError(t, tblRows.Scan(&tbl, &col, &nullable, &defaultVal, &charMax))
		entry := fmt.Sprintf("T:%s/%s/%s/%v/%v", tbl, col, nullable, defaultVal, charMax)
		parts = append(parts, entry)
	}
	require.NoError(t, tblRows.Err())

	// Table constraints (PK, unique, check).
	conRows, err := pool.Query(context.Background(), `
		SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
		       kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = 'public'
		ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`)
	require.NoError(t, err)
	for conRows.Next() {
		var tbl, cname, ctype, col string
		require.NoError(t, conRows.Scan(&tbl, &cname, &ctype, &col))
		parts = append(parts, fmt.Sprintf("C:%s/%s/%s/%s", tbl, cname, ctype, col))
	}
	require.NoError(t, conRows.Err())

	// Indexes with column lists.
	idxRows, err := pool.Query(context.Background(), `
		SELECT tablename, indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = 'public'
		ORDER BY tablename, indexname`)
	require.NoError(t, err)
	for idxRows.Next() {
		var tbl, idxName, idxDef string
		require.NoError(t, idxRows.Scan(&tbl, &idxName, &idxDef))
		parts = append(parts, fmt.Sprintf("I:%s/%s/%s", tbl, idxName, idxDef))
	}
	require.NoError(t, idxRows.Err())

	// Functions (e.g. ensure_event_partitions).
	fnRows, err := pool.Query(context.Background(), `
		SELECT p.proname, pg_get_function_arguments(p.oid)
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'public'
		ORDER BY p.proname`)
	require.NoError(t, err)
	for fnRows.Next() {
		var fname, fargs string
		require.NoError(t, fnRows.Scan(&fname, &fargs))
		parts = append(parts, fmt.Sprintf("F:%s(%s)", fname, fargs))
	}
	require.NoError(t, fnRows.Err())

	if len(parts) == 0 {
		return []byte("empty-schema")
	}

	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return h[:]
}

// ---------- non-integration tests ----------

// TestMigrate_EachUpHasMatchingDown verifies that every .up.sql migration has
// a matching .down.sql file and vice versa.
func TestMigrate_EachUpHasMatchingDown(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	require.NoError(t, err)

	type migFile struct {
		version int
		dir     string // "up" or "down"
	}

	var files []migFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		files = append(files, migFile{
			version: mustAtoi(m[1]),
			dir:     m[3],
		})
	}

	// Group by version.
	byVersion := map[int][2]string{} // [0]=up, [1]=down
	for _, f := range files {
		pair := byVersion[f.version]
		if f.dir == "up" {
			pair[0] = "up"
		} else {
			pair[1] = "down"
		}
		byVersion[f.version] = pair
	}

	// Every version must have both up and down.
	for v, pair := range byVersion {
		assert.NotEmpty(t, pair[0], "version %04d missing up file", v)
		assert.NotEmpty(t, pair[1], "version %04d missing down file", v)
	}
}

// TestMigrate_NoUpDropsTableDependency verifies that no up-migration drops
// or replaces a table that a later up-migration depends on. This is a static
// analysis check that catches obvious dependency violations.
func TestMigrate_NoUpDropsTableDependency(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	require.NoError(t, err)

	type migContent struct {
		version int
		content string
	}
	var migs []migContent
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		data, err := postgresMigrationsFS.ReadFile("migrations/" + e.Name())
		require.NoError(t, err)
		var v int
		_, err = fmt.Sscanf(e.Name(), "%d", &v)
		require.NoError(t, err)
		migs = append(migs, migContent{version: v, content: string(data)})
	}

	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })

	// Track tables created/dropped by each version.
	created := map[int][]string{}
	dropped := map[int][]string{}

	createRe := regexp.MustCompile(`(?i)CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	dropRe := regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	renameRe := regexp.MustCompile(`(?i)RENAME\s+TABLE\s+(\w+)\s+TO\s+(\w+)`)

	for _, m := range migs {
		for _, match := range createRe.FindAllStringSubmatch(m.content, -1) {
			created[m.version] = append(created[m.version], strings.ToLower(match[1]))
		}
		for _, match := range dropRe.FindAllStringSubmatch(m.content, -1) {
			dropped[m.version] = append(dropped[m.version], strings.ToLower(match[1]))
		}
		for _, match := range renameRe.FindAllStringSubmatch(m.content, -1) {
			dropped[m.version] = append(dropped[m.version], strings.ToLower(match[1]))
			created[m.version] = append(created[m.version], strings.ToLower(match[2]))
		}
	}

	// For each dropped table, check that no later version creates it.
	allVersions := make([]int, 0, len(migs))
	for _, m := range migs {
		allVersions = append(allVersions, m.version)
	}

	for dropVer, tables := range dropped {
		for _, tbl := range tables {
			if tbl == "schema_migrations" {
				continue
			}
			for _, createVer := range allVersions {
				if createVer <= dropVer {
					continue
				}
				for _, ct := range created[createVer] {
					if ct == tbl {
						t.Errorf(
							"version %04d drops table %q which version %04d creates; "+
								"running down from version %d would destroy a table that version %d depends on",
							dropVer, tbl, createVer, dropVer, createVer,
						)
					}
				}
			}
		}
	}
}

// ---------- integration tests ----------

// TestMigrate_UpFromEmpty verifies that applying every migration to a fresh
// database succeeds. This is the most basic forward-path smoke test.
func TestMigrate_UpFromEmpty(t *testing.T) {
	url := dbURL(t)
	resetDatabase(t, url)

	err := Migrate(url)
	require.NoError(t, err, "Migrate must succeed on a fresh database")
}

// TestMigrate_DownFromHead verifies that every down-migration succeeds when
// applied in sequence from the latest version to zero.
func TestMigrate_DownFromHead(t *testing.T) {
	url := dbURL(t)
	resetDatabase(t, url)

	versions := migrateVersions(t)
	require.NotEmpty(t, versions)
	maxVersion := versions[len(versions)-1]

	// Apply all migrations to reach head.
	applyToVersion(t, url, uint(maxVersion))

	// Step down from head to zero.
	for v := maxVersion; v >= 1; v-- {
		m := newMigrateToVersion(t, url)
		require.NoError(t, m.Steps(-1),
			"stepping down from version %d must succeed", v)
	}

	// Verify we are at version zero.
	m := newMigrateToVersion(t, url)
	_, _, err := m.Version()
	require.Error(t, err, "after rolling back all migrations, Version must error")
}

// TestMigrate_UpDownUpRoundTrip verifies that applying all migrations, then
// rolling them all back, then re-applying them produces an identical schema.
// This catches down-migrations that leave behind stale objects (orphaned
// indexes, leftover columns, etc.) that the subsequent up-apply doesn't
// clean up.
func TestMigrate_UpDownUpRoundTrip(t *testing.T) {
	url := dbURL(t)
	resetDatabase(t, url)

	versions := migrateVersions(t)
	require.NotEmpty(t, versions)
	maxVersion := versions[len(versions)-1]

	// Apply all migrations.
	applyToVersion(t, url, uint(maxVersion))
	schemaAfterFirstUp := dbSnapshot(t, url)

	// Roll back to zero.
	applyToVersion(t, url, 0)

	// Re-apply all migrations.
	applyToVersion(t, url, uint(maxVersion))
	schemaAfterSecondUp := dbSnapshot(t, url)

	assert.Equal(t, schemaAfterFirstUp, schemaAfterSecondUp,
		"re-applying all migrations after a full rollback must produce the same schema")
}

// TestMigrate_Idempotent verifies that Migrate() can be called twice in
// succession and both succeed. A schema that isn't idempotent would fail on
// the second call (e.g. a CREATE TABLE without IF NOT EXISTS).
func TestMigrate_Idempotent(t *testing.T) {
	url := dbURL(t)
	resetDatabase(t, url)

	require.NoError(t, Migrate(url), "first Migrate call must succeed")
	require.NoError(t, Migrate(url), "second Migrate call must be a no-op (idempotent)")

	schemaAfterFirst := dbSnapshot(t, url)
	require.NoError(t, Migrate(url), "third Migrate call must also succeed")
	schemaAfterThird := dbSnapshot(t, url)
	assert.Equal(t, schemaAfterFirst, schemaAfterThird,
		"repeated Migrate calls must not change the schema")
}

// TestMigrate_EachDownReversesItsUp verifies, for every migration version,
// that applying that version's up migration and then immediately rolling it
// back produces the same schema as before the up. This catches down-migrations
// that forget to drop a column, leave behind a constraint, or recreate an
// object with a different definition.
func TestMigrate_EachDownReversesItsUp(t *testing.T) {
	url := dbURL(t)
	versions := migrateVersions(t)
	require.NotEmpty(t, versions)

	for _, targetVersion := range versions {
		targetVersion := targetVersion // capture loop var for subtest
		t.Run(fmt.Sprintf("v%04d", targetVersion), func(t *testing.T) {
			resetDatabase(t, url)

			// Apply all migrations up to the version just before this one.
			if targetVersion > 1 {
				applyToVersion(t, url, uint(targetVersion-1))
			}
			schemaBefore := dbSnapshot(t, url)

			// Apply the target version's up migration.
			m := newMigrateToVersion(t, url)
			require.NoError(t, m.Migrate(uint(targetVersion)),
				"applying up migration v%04d must succeed", targetVersion)

			// Roll it back.
			m2 := newMigrateToVersion(t, url)
			require.NoError(t, m2.Steps(-1),
				"rolling back down migration v%04d must succeed", targetVersion)

			// Schema must match what it was before the up.
			schemaAfter := dbSnapshot(t, url)
			assert.Equal(t, schemaBefore, schemaAfter,
				fmt.Sprintf("down migration v%04d must produce the same schema as before its up", targetVersion))
		})
	}
}
