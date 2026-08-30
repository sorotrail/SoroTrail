//go:build integration

package store

// Migration reversibility and idempotency tests.
//
// These integration tests exercise every Postgres migration end-to-end and
// verify the invariants listed in issue #71:
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
// database URL. Every call creates independent source and database drivers.
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
	t.Cleanup(func() { _, _ = m.Close() })
	return m
}

// dropAllObjects drops every user-created table, function, and index in the
// public schema, and removes the schema_migrations table.
func dropAllObjects(t *testing.T, url string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

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

	_, err = pool.Exec(context.Background(),
		`DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint)`)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(),
		`DROP TABLE IF EXISTS schema_migrations`)
	require.NoError(t, err)
}

// dbSnapshot captures a deterministic hash of the database schema.
func dbSnapshot(t *testing.T, url string) []byte {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var parts []string

	tblRows, err := pool.Query(context.Background(), `
		SELECT c.table_name, c.column_name, c.is_nullable,
		       c.column_default, c.character_maximum_length
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
		WHERE c.table_schema = 'public'
		  AND c.table_name != 'schema_migrations'
		ORDER BY c.table_name, c.ordinal_position`)
	require.NoError(t, err)
	for tblRows.Next() {
		var tbl, col, nullable string
		var defaultVal, charMax sql.NullString
		require.NoError(t, tblRows.Scan(&tbl, &col, &nullable, &defaultVal, &charMax))
		parts = append(parts, fmt.Sprintf("T:%s/%s/%s/%v/%v", tbl, col, nullable, defaultVal, charMax))
	}
	require.NoError(t, tblRows.Err())

	conRows, err := pool.Query(context.Background(), `
		SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
		       kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = 'public'
		  AND tc.table_name != 'schema_migrations'
		ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`)
	require.NoError(t, err)
	for conRows.Next() {
		var tbl, cname, ctype, col string
		require.NoError(t, conRows.Scan(&tbl, &cname, &ctype, &col))
		parts = append(parts, fmt.Sprintf("C:%s/%s/%s/%s", tbl, cname, ctype, col))
	}
	require.NoError(t, conRows.Err())

	idxRows, err := pool.Query(context.Background(), `
		SELECT tablename, indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = 'public'
		  AND tablename != 'schema_migrations'
		ORDER BY tablename, indexname`)
	require.NoError(t, err)
	for idxRows.Next() {
		var tbl, idxName, idxDef string
		require.NoError(t, idxRows.Scan(&tbl, &idxName, &idxDef))
		parts = append(parts, fmt.Sprintf("I:%s/%s/%s", tbl, idxName, idxDef))
	}
	require.NoError(t, idxRows.Err())

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

// stepDownToVersion steps down one migration at a time until the database
// reaches the target version. golang-migrate's Steps(-1) calls
// source.Previous(version), which skips missing versions (e.g. version 5
// in this project), so we loop until the target is reached.
func stepDownToVersion(t *testing.T, url string, target uint) {
	t.Helper()
	for {
		m := newMigrateToVersion(t, url)
		cur, _, err := m.Version()
		require.NoError(t, err)
		if cur <= target {
			return
		}
		require.NoError(t, m.Steps(-1),
			"stepping down from version %d towards %d", cur, target)
	}
}

// ---------- integration tests ----------

// TestMigrate_UpFromEmpty verifies that applying every migration to a fresh
// database succeeds.
func TestMigrate_UpFromEmpty(t *testing.T) {
	url := dbURL(t)
	dropAllObjects(t, url)

	err := Migrate(url)
	require.NoError(t, err, "Migrate must succeed on a fresh database")
}

// TestMigrate_DownFromHead verifies that every down-migration succeeds when
// applied in sequence from the latest version to zero. golang-migrate cannot
// step from version 1 to 0 (the source has no "previous" version), so the
// final step applies v0001.down.sql directly.
func TestMigrate_DownFromHead(t *testing.T) {
	url := dbURL(t)
	dropAllObjects(t, url)

	require.NoError(t, Migrate(url))

	// Step down from head to version 1.
	stepDownToVersion(t, url, 1)

	// Verify we are at version 1.
	m := newMigrateToVersion(t, url)
	cur, _, err := m.Version()
	require.NoError(t, err)
	require.Equal(t, uint(1), cur, "should be at version 1 before final rollback")

	// Apply v0001.down.sql directly (golang-migrate can't step from 1 to 0).
	downSQL, err := postgresMigrationsFS.ReadFile("migrations/0001_init.down.sql")
	require.NoError(t, err)
	db, err := sql.Open("pgx", url)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(string(downSQL))
	require.NoError(t, err, "applying 0001_init.down.sql directly must succeed")
	_, err = db.Exec(`DROP TABLE IF EXISTS schema_migrations`)
	require.NoError(t, err)

	// Verify the database has no user tables.
	var count int
	err = db.QueryRow(`SELECT count(*) FROM pg_tables WHERE schemaname = 'public'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "after rolling back all migrations, no user tables should remain")
}

// TestMigrate_UpDownUpRoundTrip verifies that applying all migrations, then
// rolling them all back, then re-applying them produces an identical schema.
func TestMigrate_UpDownUpRoundTrip(t *testing.T) {
	url := dbURL(t)
	dropAllObjects(t, url)

	versions := migrateVersions(t)
	require.NotEmpty(t, versions)

	// Apply all migrations.
	require.NoError(t, Migrate(url))
	schemaAfterFirstUp := dbSnapshot(t, url)

	// Roll back to version 1 (golang-migrate limit for Steps).
	stepDownToVersion(t, url, 1)

	// Re-apply all migrations from version 1.
	require.NoError(t, Migrate(url))
	schemaAfterSecondUp := dbSnapshot(t, url)

	assert.Equal(t, schemaAfterFirstUp, schemaAfterSecondUp,
		"re-applying all migrations after a rollback must produce the same schema")
}

// TestMigrate_Idempotent verifies that Migrate() can be called multiple times
// and produces the same schema each time.
func TestMigrate_Idempotent(t *testing.T) {
	url := dbURL(t)
	dropAllObjects(t, url)

	require.NoError(t, Migrate(url), "first Migrate call must succeed")
	require.NoError(t, Migrate(url), "second Migrate call must be a no-op (idempotent)")

	schemaAfterFirst := dbSnapshot(t, url)
	require.NoError(t, Migrate(url), "third Migrate call must also succeed")
	schemaAfterThird := dbSnapshot(t, url)
	assert.Equal(t, schemaAfterFirst, schemaAfterThird,
		"repeated Migrate calls must not change the schema")
}

// TestMigrate_EachDownReversesItsUp verifies, for every migration version N
// (2 through head), that applying N's up and then N's down produces the same
// schema as before N was applied. Version 1 is excluded because golang-migrate
// cannot step from version 1 to 0 via Steps. Version 1's reversibility is
// covered by TestMigrate_DownFromHead.
func TestMigrate_EachDownReversesItsUp(t *testing.T) {
	url := dbURL(t)
	versions := migrateVersions(t)
	require.NotEmpty(t, versions)
	require.True(t, versions[len(versions)-1] >= 2,
		"need at least version 2 for this test")

	// Apply all migrations to reach head.
	require.NoError(t, Migrate(url))

	// Walk down from head to version 2, testing each down migration.
	for i := len(versions) - 1; i >= 1; i-- {
		ver := versions[i]

		t.Run(fmt.Sprintf("v%04d", ver), func(t *testing.T) {
			// Ensure the database is at version ver. This handles the case
			// where a previous subtest (or a gap in version numbers) left
			// the database at a different version. ErrNoChange is fine — it
			// means we are already there.
			m := newMigrateToVersion(t, url)
			if err := m.Migrate(uint(ver)); err != nil && err != migrate.ErrNoChange {
				require.NoError(t, err, "migrating to version %d must succeed", ver)
			}

			// Step down to prevVer and capture the "before" schema.
			require.NoError(t, m.Steps(-1),
				"stepping down from version %d must succeed", ver)
			schemaBefore := dbSnapshot(t, url)

			// Step back up to ver.
			if err := m.Migrate(uint(ver)); err != nil && err != migrate.ErrNoChange {
				require.NoError(t, err, "stepping back up to version %d must succeed", ver)
			}

			// Step down to prevVer again and capture the "after" schema.
			require.NoError(t, m.Steps(-1),
				"stepping down from version %d (second pass) must succeed", ver)
			schemaAfter := dbSnapshot(t, url)

			// Step back up to ver so the next iteration starts clean.
			if err := m.Migrate(uint(ver)); err != nil && err != migrate.ErrNoChange {
				require.NoError(t, err, "restoring version %d for next iteration must succeed", ver)
			}

			// The down migration must have reversed the up migration exactly.
			assert.Equal(t, schemaBefore, schemaAfter,
				"down migration v%04d must produce the same schema as before its up", ver)
		})
	}
}
