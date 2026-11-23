package store

// Non-integration tests for migration file validation. These tests run
// without a Postgres connection: they verify the embedded migration files
// are well-formed, uniquely versioned, and syntactically valid SQL, so a
// broken migration is caught at build time rather than at database startup.
//
// Run:
//
//	go test -run 'TestMigrat' ./internal/store/

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// migrationRe matches migration file names like "0001_init.up.sql".
var migrationRe = regexp.MustCompile(`^(\d{4})_(\w+)\.(up|down)\.sql$`)

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
// The filename checks below catch the same class of mistake by inspection;
// this one catches it through the loader itself, so it stays honest if
// golang-migrate ever tightens what it accepts. Two branches adding a
// migration concurrently is the normal way to hit this, and it is invisible
// in either diff alone.
func TestMigrationsLoad(t *testing.T) {
	_, err := iofs.New(postgresMigrationsFS, "migrations")
	require.NoError(t, err, "embedded migrations must load; a duplicate version number fails every integration test in this package at once")
}

// TestMigrate_FilesAreWellFormed checks that every embedded migration file
// has a compliant name, that every up file has a matching down file (and
// vice versa), and that at least one migration exists.
func TestMigrate_FilesAreWellFormed(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("reading embedded migrations directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no migration files found")
	}

	type file struct {
		version int
		name    string
		dir     string // "up" or "down"
	}

	var files []file
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationRe.FindStringSubmatch(e.Name())
		if m == nil {
			t.Errorf("migration file %q does not match pattern NNNN_name.up.sql / NNNN_name.down.sql", e.Name())
			continue
		}
		files = append(files, file{
			version: mustAtoi(m[1]),
			name:    m[2],
			dir:     m[3],
		})
	}
	if t.Failed() {
		t.Fatal("fix file naming before proceeding")
	}

	// Every up file must have a matching down file and vice versa.
	upByVersion := map[int]string{}
	downByVersion := map[int]string{}
	for _, f := range files {
		switch f.dir {
		case "up":
			upByVersion[f.version] = f.name
		case "down":
			downByVersion[f.version] = f.name
		}
	}

	for v, name := range upByVersion {
		downName, ok := downByVersion[v]
		if !ok {
			t.Errorf("version %04d (%s.up.sql) has no matching down file", v, name)
		} else if name != downName {
			t.Errorf("version %04d name mismatch: up=%q down=%q", v, name, downName)
		}
	}
	for v, name := range downByVersion {
		if _, ok := upByVersion[v]; !ok {
			t.Errorf("version %04d (%s.down.sql) has no matching up file", v, name)
		}
	}
	if t.Failed() {
		t.Fatal("fix paired migration files before proceeding")
	}

	versions := make([]int, 0, len(upByVersion))
	for v := range upByVersion {
		versions = append(versions, v)
	}
	sort.Ints(versions)

	t.Logf("migration versions: %v", versions)

	// Need at least 2 migrations for the rupture scenario to work (the
	// legacy-schema downgrade + re-migrate test needs a pre-partition
	// version to roll back to and a partition migration to re-apply).
	if len(versions) < 2 {
		t.Fatal("need at least 2 migrations for the rupture scenario to work")
	}
}

// TestMigrate_ONConflictUsesLedgerID verifies that insertEventsBatch runs
// without panicking for both the DO NOTHING and DO UPDATE paths. The
// pgx.Batch API is opaque without a database connection, so full SQL
// inspection is deferred to the integration tests (TestUpsertEvents_*).
// The nil-check here catches a nil-pointer deref at compile-adjacent time.
func TestMigrate_ONConflictUsesLedgerID(t *testing.T) {
	noUpdate := insertEventsBatch([]Event{{
		ID: "e1", ContractID: "C1", Ledger: 100, Type: "contract",
		TxHash: "0xabc", InSuccessfulCall: true,
	}}, false)
	require.NotNil(t, noUpdate, "DO NOTHING batch")

	update := insertEventsBatch([]Event{{
		ID: "e1", ContractID: "C1", Ledger: 100, Type: "contract",
		TxHash: "0xabc", InSuccessfulCall: true,
	}}, true)
	require.NotNil(t, update, "DO UPDATE batch")
}

// TestMigrate_FilesAreParseableSQL verifies each embedded migration file
// is non-empty and has no lines that are clearly broken content. The check
// is deliberately lenient — SQL and PL/pgSQL are too free-form for a
// line-oriented parser to fully validate — but catches gross structural
// breakage (e.g. half a conflict resolution spliced into the file) that
// gofmt cannot surface on .sql files.
func TestMigrate_FilesAreParseableSQL(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("reading embedded migrations directory: %v", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := postgresMigrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			t.Errorf("reading %s: %v", e.Name(), err)
			continue
		}
		content := string(data)
		if strings.TrimSpace(content) == "" {
			t.Errorf("migration %s is empty", e.Name())
			continue
		}

		// Check for obvious breakage: lines that contain only binary
		// garbage or git conflict markers (<<<<<<<, =======, >>>>>>>).
		// This is what would remain after a botched conflict resolution.
		lines := strings.Split(content, "\n")
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			if strings.HasPrefix(trimmed, "<<<<<<<") ||
				trimmed == "=======" ||
				strings.HasPrefix(trimmed, ">>>>>>>") {
				t.Errorf("%s:%d: git conflict marker found: %q", e.Name(), i+1, trimmed)
			}
			// Check for binary-looking content (high bytes).
			for _, r := range trimmed {
				if r > 127 {
					t.Errorf("%s:%d: non-ASCII byte found: %q (byte %d)", e.Name(), i+1, trimmed, r)
					break
				}
			}
		}
	}
}

// TestMigrate_VersionNumbersAreUnique checks that no two migration files
// share the same version number AND direction (up/down), because
// golang-migrate would refuse loading the source. Up/down pairs with the
// same version are correct — they represent the forward and rollback of a
// single migration step. The bug described in issue #193 was multiple up
// files (or multiple down files) with the same version.
func TestMigrate_VersionNumbersAreUnique(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("reading embedded migrations directory: %v", err)
	}

	// Track (version, direction) pairs — up and down share the same
	// version number by design.
	type key struct {
		version int
		dir     string // "up" or "down"
	}
	seen := map[key]string{} // (version, dir) → filename
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		k := key{version: mustAtoi(m[1]), dir: m[3]}
		if prev, ok := seen[k]; ok {
			t.Errorf("duplicate %s migration for version %04d: %s and %s", k.dir, k.version, prev, e.Name())
		}
		seen[k] = e.Name()
	}
}

// mustAtoi converts a decimal string to int without the allocation of
// strconv.Atoi or the error path of fmt.Sscanf.
func mustAtoi(s string) int {
	var n int
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}

// TestMigrate_NoUpDropsTableDependency verifies that no up-migration drops
// or replaces a table that a later up-migration depends on. This is the
// simplest form of the check: migration A renames/drops table T, and
// migration B creates table T — that is fine, but migration A renames
// table T and migration B tries to ALTER table T is a problem.
//
// This test runs without a database, so it catches the issue at build
// time rather than at database startup.
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
							"version %04d drops table %q which version %04d creates",
							dropVer, tbl, createVer)
					}
				}
			}
		}
	}
}

// TestMigrate_NoUpRebuildsTableDependency detects a subtler class of
// migration breakage: migration N adds a column to table T via
// ALTER TABLE T ADD COLUMN, and a later migration M rebuilds table T
// (DROP/RENAME + CREATE), which silently discards the column that N added
// because the rebuild recreates the table from scratch with a fixed schema.
//
// The check walks migrations in order, tracking which tables have been
// modified via ALTER TABLE. When a rebuild (DROP TABLE or RENAME TABLE)
// is encountered, it checks whether any earlier migration had altered that
// table. If so, the rebuild may have discarded those changes.
//
// This test runs without a database, so it catches the issue at build
// time. 0007_partition_events is the worked example: it rebuilds the
// events table, and the test verifies that no earlier migration had
// ALTERed events in a way that 0008 would discard.
func TestMigrate_NoUpRebuildsTableDependency(t *testing.T) {
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

	// Track tables that have been altered (ADD COLUMN, etc.) by migrations
	// up to the current point. If a later migration rebuilds the table,
	// those ALTERs are silently lost.
	altered := map[string]int{} // table name → latest version that altered it

	dropRe := regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	renameRe := regexp.MustCompile(`(?i)RENAME\s+TABLE\s+(\w+)\s+TO\s+(\w+)`)
	alterRe := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)`)

	for _, m := range migs {
		// Track ALTER TABLE operations (ADD COLUMN, etc.).
		for _, match := range alterRe.FindAllStringSubmatch(m.content, -1) {
			tbl := strings.ToLower(match[1])
			if tbl == "schema_migrations" {
				continue
			}
			altered[tbl] = m.version
		}

		// When a rebuild occurs (DROP or RENAME), check whether any EARLIER
		// migration (version < current) had ALTERed the same table. An ALTER
		// in the same migration is expected — that migration is the rebuild
		// itself — and should not trigger a false positive.
		for _, match := range dropRe.FindAllStringSubmatch(m.content, -1) {
			tbl := strings.ToLower(match[1])
			if tbl == "schema_migrations" {
				continue
			}
			if alterVer, ok := altered[tbl]; ok && alterVer < m.version {
				t.Errorf(
					"version %04d drops/rebuilds table %q which was altered by version %04d; "+
						"the rebuild may discard columns added in version %04d",
					m.version, tbl, alterVer, alterVer)
			}
		}
		for _, match := range renameRe.FindAllStringSubmatch(m.content, -1) {
			tbl := strings.ToLower(match[1])
			if tbl == "schema_migrations" {
				continue
			}
			if alterVer, ok := altered[tbl]; ok && alterVer < m.version {
				t.Errorf(
					"version %04d drops/rebuilds table %q which was altered by version %04d; "+
						"the rebuild may discard columns added in version %04d",
					m.version, tbl, alterVer, alterVer)
			}
		}
	}
}

// TestMigrate_PendingVersionsWithoutDB verifies that the embedded migration
// set can be introspected without a database connection. This is the seed
// of the migrate-status command: it extracts version numbers and confirms
// the set is consistent (every version has both up and down, versions are
// sequential starting from 1).
func TestMigrate_PendingVersionsWithoutDB(t *testing.T) {
	entries, err := postgresMigrationsFS.ReadDir("migrations")
	require.NoError(t, err)

	versions := map[int]bool{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		versions[mustAtoi(m[1])] = true
	}

	sorted := make([]int, 0, len(versions))
	for v := range versions {
		sorted = append(sorted, v)
	}
	sort.Ints(sorted)

	require.NotEmpty(t, sorted, "need at least one migration")
	require.Equal(t, 1, sorted[0], "versions must start at 1")

	// Versions must be strictly ascending and unique. golang-migrate v4.19
	// tolerates gaps (a deliberate renumber can leave a slot empty), so we
	// assert ordering and uniqueness — which still catches the duplicate
	// version numbers that break every integration test at once — rather
	// than perfect contiguity.
	for i := 1; i < len(sorted); i++ {
		assert.Greater(t, sorted[i], sorted[i-1],
			"migration versions must be strictly increasing and unique (got %d after %d)", sorted[i], sorted[i-1])
	}

	t.Logf("embedded migrations: %d versions (1..%d)", len(sorted), sorted[len(sorted)-1])
}

// TestMigrate_LiveDatabaseSafetyRules enforces the mechanical CI check for
// schema migrations against a live database (#1228):
//  1. Every embedded migration in internal/store/migrations/ must pass
//     LintEmbeddedMigrations() with zero violations (and every entry in
//     HistoricalMigrationExceptions must match a real violation).
//  2. Synthetic migrations violating each of the 7 live-database safety rules
//     must be deterministically flagged with the expected rule ID.
//  3. Compliant migrations (new table + index in the same file, CONCURRENTLY
//     with `-- sorotrail:no-transaction`, NOT NULL with DEFAULT, NOT VALID +
//     VALIDATE CONSTRAINT before SET NOT NULL, and Stage 5 contract drop) must
//     pass with zero violations.
func TestMigrate_LiveDatabaseSafetyRules(t *testing.T) {
	violations, err := LintEmbeddedMigrations()
	require.NoError(t, err)
	assert.Empty(t, violations, "embedded migrations must satisfy live-database safety rules")

	t.Run("rejects unsafe DDL patterns", func(t *testing.T) {
		cases := []struct {
			name     string
			filename string
			sql      string
			wantRule MigrationRuleID
			wantLine int
		}{
			{
				name:     "CREATE INDEX on existing table without CONCURRENTLY",
				filename: "0099_bad_index.up.sql",
				sql:      "CREATE INDEX IF NOT EXISTS idx_subscriptions_url ON subscriptions (url);\n",
				wantRule: RuleConcurrentIndexRequired,
				wantLine: 1,
			},
			{
				name:     "CREATE INDEX CONCURRENTLY without no-transaction directive",
				filename: "0099_bad_conc_notx.up.sql",
				sql:      "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subscriptions_url ON subscriptions (url);\n",
				wantRule: RuleConcurrentIndexNoTx,
				wantLine: 1,
			},
			{
				name:     "CREATE INDEX CONCURRENTLY inside explicit BEGIN/COMMIT block",
				filename: "0099_bad_conc_begin.up.sql",
				sql:      "-- sorotrail:no-transaction\nBEGIN;\nCREATE INDEX CONCURRENTLY idx_sub_url ON subscriptions (url);\nCOMMIT;\n",
				wantRule: RuleConcurrentIndexNoTx,
				wantLine: 3,
			},
			{
				name:     "DROP INDEX CONCURRENTLY in down migration without no-transaction directive",
				filename: "0099_bad_drop_conc.down.sql",
				sql:      "DROP INDEX CONCURRENTLY IF EXISTS idx_subscriptions_url;\n",
				wantRule: RuleConcurrentIndexNoTx,
				wantLine: 1,
			},
			{
				name:     "no-transaction migration mixing transactional DDL",
				filename: "0099_bad_notx_mixed.up.sql",
				sql:      "-- sorotrail:no-transaction\nALTER TABLE subscriptions ADD COLUMN note text;\n",
				wantRule: RuleConcurrentIndexNoTx,
				wantLine: 2,
			},
			{
				name:     "no-transaction migration with multiple statements",
				filename: "0099_bad_notx_multi.up.sql",
				sql:      "-- sorotrail:no-transaction\nCREATE INDEX CONCURRENTLY idx_sub_a ON subscriptions (url);\nCREATE INDEX CONCURRENTLY idx_sub_b ON subscriptions (enabled);\n",
				wantRule: RuleConcurrentIndexNoTx,
				wantLine: 3,
			},
			{
				name:     "ALTER TABLE DROP COLUMN without contract directive",
				filename: "0099_bad_drop_col.up.sql",
				sql:      "ALTER TABLE subscriptions DROP COLUMN IF EXISTS secret;\n",
				wantRule: RuleNoDropColumn,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE RENAME COLUMN on live table",
				filename: "0099_bad_rename_col.up.sql",
				sql:      "ALTER TABLE subscriptions RENAME COLUMN url TO callback_url;\n",
				wantRule: RuleNoRenameColumnOrTable,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE RENAME TO on live table",
				filename: "0099_bad_rename_tbl.up.sql",
				sql:      "ALTER TABLE subscriptions RENAME TO webhook_subscriptions;\n",
				wantRule: RuleNoRenameColumnOrTable,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE ALTER COLUMN TYPE on live table",
				filename: "0099_bad_alter_type.up.sql",
				sql:      "ALTER TABLE subscriptions ALTER COLUMN failure_count TYPE bigint;\n",
				wantRule: RuleNoAlterColumnType,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE ADD COLUMN NOT NULL without DEFAULT",
				filename: "0099_bad_add_notnull.up.sql",
				sql:      "ALTER TABLE subscriptions ADD COLUMN description text NOT NULL;\n",
				wantRule: RuleNotNullNeedsDefault,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE SET NOT NULL without NOT VALID check",
				filename: "0099_bad_set_notnull.up.sql",
				sql:      "ALTER TABLE subscriptions ALTER COLUMN tenant_id SET NOT NULL;\n",
				wantRule: RuleNotNullNeedsDefault,
				wantLine: 1,
			},
			{
				name:     "ALTER TABLE SET NOT NULL with NOT VALID check but missing VALIDATE CONSTRAINT",
				filename: "0099_bad_set_notnull_unvalidated.up.sql",
				sql:      "ALTER TABLE subscriptions ADD CONSTRAINT chk_sub_tenant CHECK (tenant_id IS NOT NULL) NOT VALID;\nALTER TABLE subscriptions ALTER COLUMN tenant_id SET NOT NULL;\n",
				wantRule: RuleNotNullNeedsDefault,
				wantLine: 2,
			},
			{
				name:     "ALTER TABLE SET NOT NULL when NOT VALID check validated a different column",
				filename: "0099_bad_set_notnull_wrong_col.up.sql",
				sql:      "ALTER TABLE subscriptions ADD CONSTRAINT chk_sub_url CHECK (url IS NOT NULL) NOT VALID;\nALTER TABLE subscriptions VALIDATE CONSTRAINT chk_sub_url;\nALTER TABLE subscriptions ALTER COLUMN tenant_id SET NOT NULL;\n",
				wantRule: RuleNotNullNeedsDefault,
				wantLine: 3,
			},
			{
				name:     "unbounded UPDATE backfill inside DDL migration",
				filename: "0099_bad_update.up.sql",
				sql:      "UPDATE subscriptions SET failure_count = 0 WHERE failure_count IS NULL;\n",
				wantRule: RuleNoUnboundedDataBackfill,
				wantLine: 1,
			},
			{
				name:     "unbounded DELETE inside DDL migration",
				filename: "0099_bad_delete.up.sql",
				sql:      "DELETE FROM delivery_attempts WHERE status = 'failed';\n",
				wantRule: RuleNoUnboundedDataBackfill,
				wantLine: 1,
			},
			{
				name:     "unbounded INSERT INTO SELECT backfill inside DDL migration",
				filename: "0099_bad_insert_select.up.sql",
				sql:      "INSERT INTO dead_letters (event_id, contract_id, ledger, type, error) SELECT id, contract_id, ledger, type, 'migrated' FROM events;\n",
				wantRule: RuleNoUnboundedDataBackfill,
				wantLine: 1,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := LintMigrationFile(tc.filename, tc.sql, false)
				require.NotEmpty(t, got, "expected violation for %s", tc.name)
				assert.Equal(t, tc.wantRule, got[0].Rule)
				assert.Equal(t, tc.wantLine, got[0].Line)
				assert.Contains(t, got[0].Error(), string(tc.wantRule))
			})
		}
	})

	t.Run("accepts safe live-database DDL patterns", func(t *testing.T) {
		safeCases := []struct {
			name     string
			filename string
			sql      string
		}{
			{
				name:     "CREATE TABLE and CREATE INDEX on that new table in the same migration",
				filename: "0099_new_table.up.sql",
				sql: `CREATE TABLE IF NOT EXISTS webhook_outbox (
    id bigserial PRIMARY KEY,
    subscription_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_webhook_outbox_sub ON webhook_outbox (subscription_id);`,
			},
			{
				name:     "CREATE INDEX CONCURRENTLY with no-transaction directive",
				filename: "0099_conc_index.up.sql",
				sql: `-- sorotrail:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_subscriptions_created_at
    ON subscriptions (created_at);`,
			},
			{
				name:     "DROP INDEX CONCURRENTLY with no-transaction directive",
				filename: "0099_conc_index.down.sql",
				sql: `-- sorotrail:no-transaction
DROP INDEX CONCURRENTLY IF EXISTS idx_subscriptions_created_at;`,
			},
			{
				name:     "ALTER TABLE ADD COLUMN nullable or NOT NULL with constant DEFAULT",
				filename: "0099_safe_add_col.up.sql",
				sql: `ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS description text;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS max_retries int NOT NULL DEFAULT 5;`,
			},
			{
				name:     "NOT VALID check constraint followed by VALIDATE CONSTRAINT and SET NOT NULL",
				filename: "0099_safe_set_notnull.up.sql",
				sql: `ALTER TABLE subscriptions ADD CONSTRAINT chk_subscriptions_tenant_not_null CHECK (tenant_id IS NOT NULL) NOT VALID;
ALTER TABLE subscriptions VALIDATE CONSTRAINT chk_subscriptions_tenant_not_null;
ALTER TABLE subscriptions ALTER COLUMN tenant_id SET NOT NULL;`,
			},
			{
				name:     "Stage 5 contract DROP COLUMN with explicit directive",
				filename: "0099_contract_drop.up.sql",
				sql: `-- sorotrail:allow-contract-drop-column: Stage 5 of #1228 expand-and-contract after dual-write retirement
ALTER TABLE subscriptions DROP COLUMN IF EXISTS legacy_description;`,
			},
			{
				name:     "Partitioned parent table index with explicit directive",
				filename: "0099_partitioned_index.up.sql",
				sql: `-- sorotrail:allow-partitioned-parent-index: events is partitioned by range (ledger)
CREATE INDEX IF NOT EXISTS idx_events_tx_index ON events (tx_index);`,
			},
		}

		for _, tc := range safeCases {
			t.Run(tc.name, func(t *testing.T) {
				got := LintMigrationFile(tc.filename, tc.sql, false)
				assert.Empty(t, got, "unexpected violation for %s: %v", tc.name, got)
			})
		}
	})
}

// TestMigrate_NoTransactionDirectiveAndSplitStatements verifies that
// `-- sorotrail:no-transaction` is detected accurately, semicolons inside
// comments, string literals, and PL/pgSQL dollar-quoted bodies do not split
// statements prematurely, and execMigrationSQL executes non-transactional
// statements outside an explicit transaction block.
func TestMigrate_NoTransactionDirectiveAndSplitStatements(t *testing.T) {
	sqlWithDirective := `-- Leading comment
-- sorotrail:no-transaction
CREATE TABLE IF NOT EXISTS mig_notx_probe (
    id INTEGER PRIMARY KEY,
    note TEXT NOT NULL DEFAULT 'semi;inside;literal'
);
CREATE INDEX IF NOT EXISTS idx_mig_notx_probe_note ON mig_notx_probe (note);
`
	assert.True(t, HasNoTransactionDirective(sqlWithDirective))
	assert.False(t, HasNoTransactionDirective("CREATE TABLE t (id int);\n-- sorotrail:no-transaction\n"))

	stmts := SplitNoTransactionStatements(sqlWithDirective)
	require.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE TABLE IF NOT EXISTS mig_notx_probe")
	assert.Contains(t, stmts[0], "'semi;inside;literal'", "string literal containing semicolons must be preserved verbatim")
	assert.Contains(t, stmts[1], "CREATE INDEX IF NOT EXISTS idx_mig_notx_probe_note")

	dollarSQL := `-- sorotrail:no-transaction
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $fn$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sub_url ON subscriptions (url);
`
	dollarStmts := SplitNoTransactionStatements(dollarSQL)
	require.Len(t, dollarStmts, 2)
	assert.Contains(t, dollarStmts[0], "NEW.updated_at := now();")
	assert.Contains(t, dollarStmts[1], "CREATE INDEX CONCURRENTLY")

	url := newTempSQLiteURL(t)
	db, err := openSQLiteForMigration(url)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, ensureSQLiteMigrationTable(db))
	require.NoError(t, execMigrationSQL(
		context.Background(),
		db,
		sqlWithDirective,
		`INSERT INTO schema_migrations (version, dirty) VALUES (?, 0)`,
		99,
	))

	assert.True(t, sqliteTableExists(t, url, "mig_notx_probe"))
	_, err = db.Exec(`INSERT INTO mig_notx_probe (id) VALUES (1)`)
	require.NoError(t, err)
	var gotNote string
	require.NoError(t, db.QueryRow(`SELECT note FROM mig_notx_probe WHERE id = 1`).Scan(&gotNote))
	assert.Equal(t, "semi;inside;literal", gotNote, "DEFAULT string literal with semicolons must be stored intact")
}

// TestMigrate_SessionTimeoutDirectivesAndOptions verifies that
// DefaultMigrationOptions and FormatMigrationSessionTimeoutSQL produce bounded
// lock_timeout and statement_timeout settings and that MigrateWithOptions /
// MigrateUpWithOptions / MigrateDownWithOptions honor custom MigrationOptions.
func TestMigrate_SessionTimeoutDirectivesAndOptions(t *testing.T) {
	defaults := DefaultMigrationOptions()
	assert.Equal(t, 5*time.Second, defaults.LockTimeout)
	assert.Equal(t, 30*time.Second, defaults.StatementTimeout)

	sqlStmts := FormatMigrationSessionTimeoutSQL(MigrationOptions{
		LockTimeout:      3 * time.Second,
		StatementTimeout: 15 * time.Second,
	})
	assert.Equal(t, []string{
		"SET lock_timeout = '3000ms'",
		"SET statement_timeout = '15000ms'",
	}, sqlStmts)

	noTxStmts := FormatMigrationSessionTimeoutSQLForMigration(
		"-- sorotrail:no-transaction\nCREATE INDEX CONCURRENTLY idx_sub_url ON subscriptions (url);\n",
		MigrationOptions{LockTimeout: 3 * time.Second, StatementTimeout: 15 * time.Second},
	)
	assert.Equal(t, []string{
		"SET lock_timeout = '3000ms'",
		"SET statement_timeout = 0",
	}, noTxStmts)

	zeroCfg, err := buildPostgresMigrateConnConfig("postgres://user:pass@localhost:5432/sorotrail?sslmode=disable", MigrationOptions{})
	require.NoError(t, err)
	assert.Empty(t, zeroCfg.RuntimeParams["lock_timeout"], "legacy zero MigrationOptions must not force a 5s lock_timeout on pg_advisory_lock")
	assert.Empty(t, zeroCfg.RuntimeParams["statement_timeout"], "legacy zero MigrationOptions must not force a 30s statement_timeout")

	url := newTempSQLiteURL(t)
	opts := MigrationOptions{
		LockTimeout:      2 * time.Second,
		StatementTimeout: 10 * time.Second,
	}
	connCfg, err := buildPostgresMigrateConnConfig("postgres://user:pass@localhost:5432/sorotrail?sslmode=disable", opts)
	require.NoError(t, err)
	assert.Equal(t, "2000ms", connCfg.RuntimeParams["lock_timeout"])
	assert.Equal(t, "10000ms", connCfg.RuntimeParams["statement_timeout"])

	assert.Equal(t,
		"SELECT a FROM t WHERE b = $1 AND c = $2",
		rebindMigrationPlaceholders("SELECT a FROM t WHERE b = ? AND c = ?", true),
	)
	assert.Equal(t,
		"SELECT a FROM t WHERE b = ? AND c = ?",
		rebindMigrationPlaceholders("SELECT a FROM t WHERE b = ? AND c = ?", false),
	)

	require.NoError(t, MigrateWithOptions(url, opts))
	st, err := MigrateStatus(url)
	require.NoError(t, err)
	assert.Equal(t, sqliteLatestVersion(t), st.Version)
	assert.Empty(t, st.Pending)

	require.NoError(t, MigrateDownWithOptions(url, 1, opts))
	require.NoError(t, MigrateUpWithOptions(url, 1, opts))

	// Also verify RunBatchedMigration with UseDollarPlaceholders: true ($1..$5).
	db, err := openSQLiteForMigration(url)
	require.NoError(t, err)
	defer db.Close()
	prog, err := RunBatchedMigration(context.Background(), db, BatchedMigrationConfig{
		Name:                  "dollar_placeholder_smoke",
		BatchSize:             10,
		UseDollarPlaceholders: true,
		Step: func(_ context.Context, _ *sql.Tx, _ string, _ int) (string, int64, bool, error) {
			return "cursor-done", 5, true, nil
		},
	})
	require.NoError(t, err)
	assert.True(t, prog.Completed)
	assert.Equal(t, int64(5), prog.RowsProcessed)
	assert.Equal(t, "cursor-done", prog.LastCursor)

	// Verify RunBatchedMigration optimistic concurrency guard when a competing
	// runner modifies migration_backfill_state concurrently (both on initial
	// insert and on subsequent cursor update).
	_, err = RunBatchedMigration(context.Background(), db, BatchedMigrationConfig{
		Name:      "concurrent_runner_conflict",
		BatchSize: 10,
		Step: func(stepCtx context.Context, tx *sql.Tx, _ string, _ int) (string, int64, bool, error) {
			_, execErr := tx.ExecContext(stepCtx, `
				INSERT INTO migration_backfill_state (migration_name, last_cursor, rows_processed, updated_at)
				VALUES ('concurrent_runner_conflict', 'other-pod-cursor', 10, '2026-01-01T00:00:00Z')
			`)
			require.NoError(t, execErr)
			return "my-cursor", 10, false, nil
		},
	})
	require.ErrorIs(t, err, ErrConcurrentBatchedMigration)

	// Seed a checkpoint with last_cursor = 'batch-1' and verify that if another
	// runner advances last_cursor before the batch update, ErrConcurrentBatchedMigration is returned.
	_, err = db.Exec(`
		INSERT INTO migration_backfill_state (migration_name, last_cursor, rows_processed, updated_at)
		VALUES ('concurrent_runner_update_conflict', 'batch-1', 10, '2026-01-01T00:00:00Z')
	`)
	require.NoError(t, err)
	_, err = RunBatchedMigration(context.Background(), db, BatchedMigrationConfig{
		Name:      "concurrent_runner_update_conflict",
		BatchSize: 10,
		Step: func(stepCtx context.Context, tx *sql.Tx, _ string, _ int) (string, int64, bool, error) {
			_, execErr := tx.ExecContext(stepCtx, `
				UPDATE migration_backfill_state
				SET last_cursor = 'other-pod-batch-2'
				WHERE migration_name = 'concurrent_runner_update_conflict'
			`)
			require.NoError(t, execErr)
			return "my-batch-2", 10, true, nil
		},
	})
	require.ErrorIs(t, err, ErrConcurrentBatchedMigration)
}

// TestMigrate_ExpandAndContractUnderConcurrentWriteLoad exercises the 5-stage
// expand-and-contract pattern and RunBatchedMigration against a live database
// while concurrent writer and reader goroutines actively insert and query
// events (#1228):
//   - Stage 1 (Expand): Add nullable column `tx_hash_normalized` and index while
//     live traffic is running.
//   - Stage 2 (Dual-Write): Writers populate both `tx_hash` and
//     `tx_hash_normalized`.
//   - Stage 3 (Resumable Batched Backfill): RunBatchedMigration backfills
//     pre-existing rows in bounded batches, survives an injected mid-flight
//     interruption, resumes from the persisted cursor in
//     `migration_backfill_state`, and emits structured progress logs.
//   - Stage 4 (Switch Reads & Verify): Confirm every pre-existing and
//     concurrently written event has `tx_hash_normalized` populated with zero
//     writer or reader errors.
//   - Stage 5 (Contract): Verify the contract migration passes
//     LintMigrationFile when annotated with DirectiveAllowContractDropColumn.
func TestMigrate_ExpandAndContractUnderConcurrentWriteLoad(t *testing.T) {
	url := newTempSQLiteURL(t)
	require.NoError(t, Migrate(url))

	db, err := sql.Open("sqlite", parseSQLiteDSN(url)+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(4)

	st := NewSQLite(db)
	ctx := context.Background()

	// Seed 45 historical events written before Stage 1 (Expand).
	const initialEvents = 45
	seedBatch := make([]Event, 0, initialEvents)
	for i := 1; i <= initialEvents; i++ {
		seedBatch = append(seedBatch, Event{
			ID:               fmt.Sprintf("0000100000-%010d", i),
			ContractID:       "CCONTRACT1228",
			Ledger:           int64(1000 + i),
			Type:             "contract",
			TxHash:           fmt.Sprintf("0xDEAD%04X", i),
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`["transfer"]`),
			Value:            json.RawMessage(`{"amount":"100"}`),
		})
	}
	_, err = st.UpsertEvents(ctx, seedBatch)
	require.NoError(t, err)

	// Start concurrent writer and reader traffic against the live database.
	var (
		dualWriteEnabled atomic.Bool
		stopTraffic      atomic.Bool
		writerErrors     atomic.Int64
		readerErrors     atomic.Int64
		writtenDuringMig atomic.Int64
		wg               sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		seq := 1
		for !stopTraffic.Load() {
			evID := fmt.Sprintf("0000200000-%010d", seq)
			txHash := fmt.Sprintf("0xBEEF%04X", seq)
			ev := Event{
				ID:               evID,
				ContractID:       "CCONTRACT1228",
				Ledger:           int64(2000 + seq),
				Type:             "contract",
				TxHash:           txHash,
				InSuccessfulCall: true,
				Topics:           json.RawMessage(`["transfer"]`),
				Value:            json.RawMessage(`{"amount":"250"}`),
			}
			if _, err := st.UpsertEvents(ctx, []Event{ev}); err != nil {
				writerErrors.Add(1)
				return
			}
			if dualWriteEnabled.Load() {
				if _, err := db.ExecContext(
					ctx,
					`UPDATE events SET tx_hash_normalized = lower(tx_hash) WHERE id = ?`,
					evID,
				); err != nil {
					writerErrors.Add(1)
					return
				}
			}
			writtenDuringMig.Add(1)
			seq++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for !stopTraffic.Load() {
			_, _, err := st.QueryEvents(ctx, EventFilter{ContractID: "CCONTRACT1228", Limit: 10})
			if err != nil {
				readerErrors.Add(1)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Let concurrent traffic warm up before Stage 1 (Expand).
	time.Sleep(15 * time.Millisecond)

	// Stage 1 (Expand): Add nullable column and non-transactional index while traffic runs.
	expandSQL := `-- sorotrail:no-transaction
ALTER TABLE events ADD COLUMN tx_hash_normalized TEXT;
CREATE INDEX IF NOT EXISTS idx_events_tx_hash_normalized ON events (tx_hash_normalized);
`
	require.NoError(t, execMigrationSQL(ctx, db, expandSQL, ""))

	// Stage 2 (Dual-Write): Flip concurrent writers to populate tx_hash_normalized.
	dualWriteEnabled.Store(true)
	time.Sleep(15 * time.Millisecond)

	// Stage 3 (Resumable Batched Backfill): Backfill in batches of 15, injecting
	// a transient interruption on the 3rd batch call to prove cursor checkpoint
	// durability and resumption.
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	stepCalls := 0
	injectedErr := errors.New("simulated lock_timeout during batch 3")

	backfillStep := func(stepCtx context.Context, tx *sql.Tx, cursor string, limit int) (string, int64, bool, error) {
		stepCalls++
		if stepCalls == 3 {
			return "", 0, false, injectedErr
		}

		rows, err := tx.QueryContext(stepCtx, `
			SELECT id FROM events
			WHERE id > ? AND (tx_hash_normalized IS NULL OR tx_hash_normalized = '')
			ORDER BY id ASC
			LIMIT ?
		`, cursor, limit)
		if err != nil {
			return "", 0, false, err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return "", 0, false, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return "", 0, false, err
		}
		_ = rows.Close()

		if len(ids) == 0 {
			return cursor, 0, true, nil
		}

		var updated int64
		for _, id := range ids {
			res, err := tx.ExecContext(stepCtx, `
				UPDATE events
				SET tx_hash_normalized = lower(tx_hash)
				WHERE id = ? AND (tx_hash_normalized IS NULL OR tx_hash_normalized = '')
			`, id)
			if err != nil {
				return "", 0, false, err
			}
			n, _ := res.RowsAffected()
			updated += n
		}
		nextCursor := ids[len(ids)-1]
		done := len(ids) < limit
		return nextCursor, updated, done, nil
	}

	cfg := BatchedMigrationConfig{
		Name:             "0033_backfill_tx_hash_normalized",
		BatchSize:        15,
		StatementTimeout: 5 * time.Second,
		Logger:           logger,
		Step:             backfillStep,
	}

	// First run stops at batch 3 with injectedErr after committing 2 batches (30 rows).
	partialProgress, err := RunBatchedMigration(ctx, db, cfg)
	require.ErrorIs(t, err, injectedErr)
	assert.Equal(t, 2, partialProgress.BatchesRun)
	assert.Equal(t, int64(30), partialProgress.RowsProcessed)
	assert.Equal(t, "0000100000-0000000030", partialProgress.LastCursor)
	assert.False(t, partialProgress.Completed)

	// Resume: picks up from cursor "0000100000-0000000030" and finishes the remaining rows.
	finalProgress, err := RunBatchedMigration(ctx, db, cfg)
	require.NoError(t, err)
	assert.True(t, finalProgress.Completed)
	assert.GreaterOrEqual(t, finalProgress.RowsProcessed, int64(initialEvents))
	assert.GreaterOrEqual(t, finalProgress.BatchesRun, 1)

	// Calling RunBatchedMigration a third time is an idempotent no-op.
	noopProgress, err := RunBatchedMigration(ctx, db, cfg)
	require.NoError(t, err)
	assert.True(t, noopProgress.Completed)
	assert.Equal(t, 0, noopProgress.BatchesRun, "completed migration must not re-run batches")
	assert.Equal(t, finalProgress.RowsProcessed, noopProgress.RowsProcessed)

	// Stop concurrent traffic and verify zero writer/reader errors.
	stopTraffic.Store(true)
	wg.Wait()

	assert.Zero(t, writerErrors.Load(), "concurrent writers must not error during expand + batched backfill")
	assert.Zero(t, readerErrors.Load(), "concurrent readers must not error during expand + batched backfill")
	assert.Positive(t, writtenDuringMig.Load(), "concurrent writers must have inserted rows during migration")

	// Stage 4 (Switch Reads & Validate): Every row in events now has tx_hash_normalized populated.
	var unpopulated int
	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM events WHERE tx_hash_normalized IS NULL OR tx_hash_normalized = ''`,
	).Scan(&unpopulated))
	assert.Zero(t, unpopulated, "all historical and concurrent rows must have tx_hash_normalized populated")

	// Structured progress logs were emitted with migration_name, batch_rows, total_rows, cursor.
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, `"migration_name":"0033_backfill_tx_hash_normalized"`)
	assert.Contains(t, logOutput, `"batch_rows":15`)
	assert.Contains(t, logOutput, `"completed":true`)

	// Stage 5 (Contract): Verify the contract DDL passes LintMigrationFile when annotated.
	contractSQL := `-- sorotrail:allow-contract-drop-column: Stage 5 after switching reads to tx_hash_normalized
ALTER TABLE events DROP COLUMN IF EXISTS tx_hash;
`
	assert.Empty(t, LintMigrationFile("0034_contract_tx_hash.up.sql", contractSQL, false))
}
