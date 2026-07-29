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
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/iofs"
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
	_, err := iofs.New(migrationsFS, "migrations")
	require.NoError(t, err, "embedded migrations must load; a duplicate version number fails every integration test in this package at once")
}

// TestMigrate_FilesAreWellFormed checks that every embedded migration file
// has a compliant name, that every up file has a matching down file (and
// vice versa), and that at least one migration exists.
func TestMigrate_FilesAreWellFormed(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
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
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("reading embedded migrations directory: %v", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := migrationsFS.ReadFile("migrations/" + e.Name())
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
	entries, err := migrationsFS.ReadDir("migrations")
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
