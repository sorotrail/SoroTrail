package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var postgresMigrationsFS embed.FS

//go:embed migrations/sqlite/*.sql
var sqliteMigrationsFS embed.FS

const (
	// DefaultMigrationLockTimeout bounds how long a DDL statement waits to
	// acquire a table or index lock against live traffic before failing fast.
	// A short lock_timeout prevents an ALTER TABLE waiting on a long-running
	// query from queueing all subsequent live reads and writes behind its
	// ACCESS EXCLUSIVE lock request.
	DefaultMigrationLockTimeout = 5 * time.Second

	// DefaultMigrationStatementTimeout bounds the wall-clock execution time of
	// an individual migration statement so an accidental table rewrite or
	// runaway query aborts cleanly instead of holding locks indefinitely.
	DefaultMigrationStatementTimeout = 30 * time.Second

	// DefaultBatchedMigrationSize is the default number of rows processed per
	// committed batch in RunBatchedMigration.
	DefaultBatchedMigrationSize = 1000
)

// MigrationOptions configures lock timeouts, statement timeouts, and progress
// logging for schema migration execution against a live database.
type MigrationOptions struct {
	LockTimeout      time.Duration
	StatementTimeout time.Duration
	Logger           *slog.Logger
}

// DefaultMigrationOptions returns the production-safe defaults for running
// schema migrations against a live database.
func DefaultMigrationOptions() MigrationOptions {
	return MigrationOptions{
		LockTimeout:      DefaultMigrationLockTimeout,
		StatementTimeout: DefaultMigrationStatementTimeout,
	}
}

func (o MigrationOptions) withDefaults() MigrationOptions {
	out := o
	if out.LockTimeout <= 0 {
		out.LockTimeout = DefaultMigrationLockTimeout
	}
	if out.StatementTimeout <= 0 {
		out.StatementTimeout = DefaultMigrationStatementTimeout
	}
	if out.Logger == nil {
		out.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return out
}

// FormatMigrationSessionTimeoutSQL returns the PostgreSQL session SET
// statements that enforce LockTimeout and StatementTimeout on a migration
// connection.
func FormatMigrationSessionTimeoutSQL(opts MigrationOptions) []string {
	resolved := opts.withDefaults()
	return []string{
		fmt.Sprintf("SET lock_timeout = '%dms'", resolved.LockTimeout.Milliseconds()),
		fmt.Sprintf("SET statement_timeout = '%dms'", resolved.StatementTimeout.Milliseconds()),
	}
}

// FormatMigrationSessionTimeoutSQLForMigration returns the PostgreSQL session
// SET statements for a specific migration payload. Non-transactional
// migrations (`-- sorotrail:no-transaction`, used for CREATE/DROP INDEX
// CONCURRENTLY) keep the short lock_timeout so initial/final catalog lock
// waits remain bounded, but set `statement_timeout = 0` so multi-pass
// concurrent index builds on large tables do not abort after 30s.
func FormatMigrationSessionTimeoutSQLForMigration(sqlContent string, opts MigrationOptions) []string {
	resolved := opts.withDefaults()
	if HasNoTransactionDirective(sqlContent) {
		return []string{
			fmt.Sprintf("SET lock_timeout = '%dms'", resolved.LockTimeout.Milliseconds()),
			"SET statement_timeout = 0",
		}
	}
	return FormatMigrationSessionTimeoutSQL(resolved)
}

// Migrate applies all pending migrations to the database at databaseURL.
// It detects the dialect from the URL scheme and runs the appropriate
// migration series. Safe to call on every startup; up-to-date schema is a no-op.
//
// It is the startup-path alias for MigrateUp(url, 0) and is kept so existing
// callers (main, replay, backfill, …) are unchanged.
func Migrate(databaseURL string) error {
	return MigrateUp(databaseURL, 0)
}

// MigrateWithOptions applies all pending migrations using explicit
// MigrationOptions (lock_timeout, statement_timeout, logger).
func MigrateWithOptions(databaseURL string, opts MigrationOptions) error {
	return MigrateUpWithOptions(databaseURL, 0, opts)
}

// MigrateUp applies pending migrations. steps > 0 applies at most that many
// migrations; steps <= 0 applies every pending migration.
//
// ClickHouse runs the embedded ClickHouse series (apply-only). SQLite runs the
// embedded SQLite series; Postgres runs the embedded Postgres series via
// golang-migrate (and honors steps).
func MigrateUp(databaseURL string, steps int) error {
	return migrateUpInternal(databaseURL, steps, MigrationOptions{}, false)
}

// MigrateUpWithOptions applies pending migrations with explicit lock and
// statement timeout safeguards.
func MigrateUpWithOptions(databaseURL string, steps int, opts MigrationOptions) error {
	return migrateUpInternal(databaseURL, steps, opts.withDefaults(), true)
}

func migrateUpInternal(databaseURL string, steps int, opts MigrationOptions, applyTimeouts bool) error {
	resolved := opts.withDefaults()
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		return migrateClickHouse(databaseURL)
	}
	if isSQLiteURL(databaseURL) {
		return migrateSQLiteUpWithOptions(databaseURL, steps, resolved)
	}
	if !isPostgresURL(databaseURL) {
		return fmt.Errorf("unsupported database url scheme")
	}

	pgOpts := MigrationOptions{}
	if applyTimeouts {
		pgOpts = resolved
	}
	m, err := newPostgresMigrateWithOptions(databaseURL, pgOpts)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if steps > 0 {
		if err := m.Steps(steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("applying %d migration step(s): %w", steps, err)
		}
		return nil
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("applying migrations: %w", err)
	}
	return nil
}

// MigrateDown rolls back migrations. steps > 0 rolls back at most that many
// migrations; steps <= 0 rolls back everything (a destructive full teardown,
// which is why `migrate down` defaults to a single step at the CLI layer).
func MigrateDown(databaseURL string, steps int) error {
	return migrateDownInternal(databaseURL, steps, MigrationOptions{}, false)
}

// MigrateDownWithOptions rolls back migrations with explicit lock and
// statement timeout safeguards.
func MigrateDownWithOptions(databaseURL string, steps int, opts MigrationOptions) error {
	return migrateDownInternal(databaseURL, steps, opts.withDefaults(), true)
}

func migrateDownInternal(databaseURL string, steps int, opts MigrationOptions, applyTimeouts bool) error {
	resolved := opts.withDefaults()
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		return errors.New("clickhouse migrations are apply-only; nothing to roll back")
	}
	if isSQLiteURL(databaseURL) {
		return migrateSQLiteDownWithOptions(databaseURL, steps, resolved)
	}
	if !isPostgresURL(databaseURL) {
		return fmt.Errorf("unsupported database url scheme")
	}

	pgOpts := MigrationOptions{}
	if applyTimeouts {
		pgOpts = resolved
	}
	m, err := newPostgresMigrateWithOptions(databaseURL, pgOpts)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if steps > 0 {
		if err := m.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("rolling back %d migration step(s): %w", steps, err)
		}
		return nil
	}
	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("rolling back migrations: %w", err)
	}
	return nil
}

// MigrateStatus reports the current migration version, dirty state, and the
// versions still pending, without mutating the database.
func MigrateStatus(databaseURL string) (MigrationStatus, error) {
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		return MigrationStatus{}, errors.New("clickhouse migration status is not supported")
	}
	if isSQLiteURL(databaseURL) {
		return migrateSQLiteStatus(databaseURL)
	}
	if !isPostgresURL(databaseURL) {
		return MigrationStatus{}, fmt.Errorf("unsupported database url scheme")
	}

	m, err := newPostgresMigrate(databaseURL)
	if err != nil {
		return MigrationStatus{}, err
	}
	defer func() { _, _ = m.Close() }()

	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return MigrationStatus{}, fmt.Errorf("reading migration version: %w", err)
	}
	if errors.Is(err, migrate.ErrNilVersion) {
		version, dirty = 0, false // nothing applied yet
	}

	src, err := iofs.New(postgresMigrationsFS, "migrations")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("opening migrations: %w", err)
	}
	defer func() { _ = src.Close() }()
	pending, err := migrationVersions(src, version)
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("reading available migrations: %w", err)
	}
	return MigrationStatus{Version: version, Dirty: dirty, Pending: pending}, nil
}

func isPostgresURL(databaseURL string) bool {
	return strings.HasPrefix(databaseURL, "postgres://") || strings.HasPrefix(databaseURL, "postgresql://")
}

func isSQLiteURL(databaseURL string) bool {
	return strings.HasPrefix(databaseURL, "sqlite:") || strings.HasPrefix(databaseURL, "file:")
}

// newPostgresMigrate builds a golang-migrate instance over the embedded
// Postgres migrations without overriding session timeout parameters.
func newPostgresMigrate(databaseURL string) (*migrate.Migrate, error) {
	return newPostgresMigrateWithOptions(databaseURL, MigrationOptions{})
}

// buildPostgresMigrateConnConfig parses databaseURL and attaches session-level
// lock_timeout and statement_timeout runtime parameters when specified on opts
// so every connection opened by the migration pool inherits the configured
// timeouts at connection startup.
func buildPostgresMigrateConnConfig(databaseURL string, opts MigrationOptions) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string, 2)
	}
	if opts.LockTimeout > 0 {
		connConfig.RuntimeParams["lock_timeout"] = fmt.Sprintf("%dms", opts.LockTimeout.Milliseconds())
	}
	if opts.StatementTimeout > 0 {
		connConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%dms", opts.StatementTimeout.Milliseconds())
	}
	return connConfig, nil
}

// newPostgresMigrateWithOptions builds a golang-migrate instance over the
// embedded Postgres migrations with session-level lock_timeout and
// statement_timeout enforced via pgx RuntimeParams on every connection opened
// by the migration pool when specified in opts.
func newPostgresMigrateWithOptions(databaseURL string, opts MigrationOptions) (*migrate.Migrate, error) {
	src, err := iofs.New(postgresMigrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("loading embedded migrations: %w", err)
	}
	connConfig, cfgErr := buildPostgresMigrateConnConfig(databaseURL, opts)
	if cfgErr != nil {
		return nil, fmt.Errorf("initializing migration driver: %w", cfgErr)
	}
	db := stdlib.OpenDB(*connConfig)

	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{
		StatementTimeout: opts.StatementTimeout,
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing migration driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing migrations: %w", err)
	}
	return m, nil
}

// --- SQLite migration series ---

// sqliteMigrationFile is one embedded SQLite migration file with its parsed
// leading version number.
type sqliteMigrationFile struct {
	version int
	name    string
}

// sqliteMigrationFiles returns the up- or down-migration files sorted by
// ascending version. Files whose name does not start with a version are
// skipped, mirroring the historical loader.
func sqliteMigrationFiles(direction string) ([]sqliteMigrationFile, error) {
	entries, err := sqliteMigrationsFS.ReadDir("migrations/sqlite")
	if err != nil {
		return nil, fmt.Errorf("reading sqlite migrations: %w", err)
	}
	suffix := "." + direction + ".sql"
	var out []sqliteMigrationFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
			continue
		}
		var version int
		if _, err := fmt.Sscanf(e.Name(), "%d", &version); err != nil || version == 0 {
			continue
		}
		out = append(out, sqliteMigrationFile{version: version, name: e.Name()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func openSQLiteForMigration(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", parseSQLiteDSN(databaseURL))
	if err != nil {
		return nil, fmt.Errorf("opening sqlite for migration: %w", err)
	}
	return db, nil
}

// ensureSQLiteMigrationTable creates the migration tracking table used by the
// SQLite series (one row per applied version).
func ensureSQLiteMigrationTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, dirty INTEGER NOT NULL DEFAULT 0)`); err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}
	return nil
}

// execMigrationSQL executes a migration payload against db. When the migration
// carries `-- sorotrail:no-transaction`, statements are split and executed one
// by one outside an explicit transaction block; otherwise they execute
// atomically inside a transaction.
func execMigrationSQL(ctx context.Context, db *sql.DB, sqlContent string, recordSQL string, recordArgs ...any) error {
	if HasNoTransactionDirective(sqlContent) {
		for _, stmt := range SplitNoTransactionStatements(sqlContent) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		if recordSQL != "" {
			if _, err := db.ExecContext(ctx, recordSQL, recordArgs...); err != nil {
				return err
			}
		}
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, sqlContent); err != nil {
		return err
	}
	if recordSQL != "" {
		if _, err := tx.ExecContext(ctx, recordSQL, recordArgs...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// migrateSQLiteUpWithOptions applies pending SQLite migrations. steps > 0
// bounds how many files are applied; steps <= 0 applies all pending files.
func migrateSQLiteUpWithOptions(databaseURL string, steps int, opts MigrationOptions) error {
	resolved := opts.withDefaults()
	db, err := openSQLiteForMigration(databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", resolved.LockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("configuring sqlite busy_timeout: %w", err)
	}

	if err := ensureSQLiteMigrationTable(db); err != nil {
		return err
	}
	files, err := sqliteMigrationFiles("up")
	if err != nil {
		return err
	}

	applied := 0
	for _, f := range files {
		if steps > 0 && applied >= steps {
			break
		}
		var existing int
		err := db.QueryRow(`SELECT version FROM schema_migrations WHERE version = ?`, f.version).Scan(&existing)
		if err == nil {
			continue // already applied
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("checking migration %s: %w", f.name, err)
		}

		content, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/" + f.name)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", f.name, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), resolved.StatementTimeout)
		err = execMigrationSQL(
			ctx,
			db,
			string(content),
			`INSERT INTO schema_migrations (version, dirty) VALUES (?, 0)`,
			f.version,
		)
		cancel()
		if err != nil {
			return fmt.Errorf("applying migration %s: %w", f.name, err)
		}
		applied++
	}
	return nil
}

// migrateSQLiteDownWithOptions rolls back applied SQLite migrations newest-first.
// steps > 0 bounds how many files are rolled back; steps <= 0 rolls back all.
func migrateSQLiteDownWithOptions(databaseURL string, steps int, opts MigrationOptions) error {
	resolved := opts.withDefaults()
	db, err := openSQLiteForMigration(databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", resolved.LockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("configuring sqlite busy_timeout: %w", err)
	}

	if err := ensureSQLiteMigrationTable(db); err != nil {
		return err
	}

	down, err := sqliteMigrationFiles("down")
	if err != nil {
		return err
	}
	downByName := make(map[int]string, len(down))
	for _, f := range down {
		downByName[f.version] = f.name
	}

	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version DESC`)
	if err != nil {
		return fmt.Errorf("listing applied migrations: %w", err)
	}
	var appliedVersions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning applied migration: %w", err)
		}
		appliedVersions = append(appliedVersions, v)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("listing applied migrations: %w", err)
	}
	_ = rows.Close()
	sort.Sort(sort.Reverse(sort.IntSlice(appliedVersions)))

	rolledBack := 0
	for _, version := range appliedVersions {
		if steps > 0 && rolledBack >= steps {
			break
		}
		name, ok := downByName[version]
		if !ok {
			return fmt.Errorf("no down migration for version %d", version)
		}
		content, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/" + name)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), resolved.StatementTimeout)
		err = execMigrationSQL(
			ctx,
			db,
			string(content),
			`DELETE FROM schema_migrations WHERE version = ?`,
			version,
		)
		cancel()
		if err != nil {
			return fmt.Errorf("rolling back migration %s: %w", name, err)
		}
		rolledBack++
	}
	return nil
}

// migrateSQLiteStatus reads the highest recorded version without creating the
// tracking table (status must not mutate) and lists the still-pending up
// migrations.
func migrateSQLiteStatus(databaseURL string) (MigrationStatus, error) {
	db, err := openSQLiteForMigration(databaseURL)
	if err != nil {
		return MigrationStatus{}, err
	}
	defer func() { _ = db.Close() }()

	var st MigrationStatus
	var tables int
	if err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`,
	).Scan(&tables); err != nil {
		return MigrationStatus{}, fmt.Errorf("inspecting sqlite schema: %w", err)
	}
	if tables > 0 {
		var version int
		var dirty bool
		err := db.QueryRow(`SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version, &dirty)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// table exists but nothing recorded yet
		case err != nil:
			return MigrationStatus{}, fmt.Errorf("reading migration version: %w", err)
		default:
			st.Version = uint(version)
			st.Dirty = dirty
		}
	}

	files, err := sqliteMigrationFiles("up")
	if err != nil {
		return MigrationStatus{}, err
	}
	for _, f := range files {
		if uint(f.version) > st.Version {
			st.Pending = append(st.Pending, uint(f.version))
		}
	}
	return st, nil
}

// parseSQLiteDSN strips the URL scheme, leaving the file path (or :memory:)
// that modernc.org/sqlite expects. Prefixes are checked longest-first so
// "sqlite://" is not left with a stray leading slash by the "sqlite:" case.
func parseSQLiteDSN(databaseURL string) string {
	for _, p := range []string{"sqlite3://", "sqlite://", "sqlite3:", "sqlite:", "file:"} {
		if strings.HasPrefix(databaseURL, p) {
			return strings.TrimPrefix(databaseURL, p)
		}
	}
	return databaseURL
}

// --- Resumable, observable batched data migration runner ---

// BatchedMigrationStepFunc executes a single bounded batch of a data backfill
// inside tx, starting strictly after cursor, and returns the new cursor high-water
// mark, the number of rows modified in this batch, and whether the backfill has
// reached the end of the table (done == true).
type BatchedMigrationStepFunc func(ctx context.Context, tx *sql.Tx, cursor string, limit int) (nextCursor string, rowsAffected int64, done bool, err error)

// BatchedMigrationConfig configures a resumable, observable batched data
// migration for Stage 3 of an expand-and-contract schema rollout.
type BatchedMigrationConfig struct {
	// Name uniquely identifies the data migration in migration_backfill_state.
	Name string
	// BatchSize is the maximum number of rows updated per committed transaction.
	// Defaults to DefaultBatchedMigrationSize (1000) when <= 0.
	BatchSize int
	// InitialCursor is the cursor value used when no prior checkpoint exists.
	InitialCursor string
	// StatementTimeout bounds the wall-clock duration of each individual batch.
	// Defaults to DefaultMigrationStatementTimeout (30s) when <= 0.
	StatementTimeout time.Duration
	// UseDollarPlaceholders forces PostgreSQL positional placeholders ($1, $2, ...)
	// for migration_backfill_state queries. When false, RunBatchedMigration
	// auto-detects PostgreSQL drivers (pgx/stdlib) and uses $1..$N for Postgres
	// or ? for SQLite.
	UseDollarPlaceholders bool
	// Logger receives structured progress log entries after each batch.
	Logger *slog.Logger
	// Step performs one bounded batch of work inside a transaction.
	Step BatchedMigrationStepFunc
}

func shouldUseDollarPlaceholders(db *sql.DB, cfg BatchedMigrationConfig) bool {
	if cfg.UseDollarPlaceholders {
		return true
	}
	if db == nil {
		return false
	}
	drv := strings.ToLower(fmt.Sprintf("%T", db.Driver()))
	return strings.Contains(drv, "pgx") || strings.Contains(drv, "stdlib")
}

func rebindMigrationPlaceholders(query string, useDollar bool) string {
	if !useDollar {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 16)
	argNum := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			fmt.Fprintf(&b, "$%d", argNum)
			argNum++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// ErrConcurrentBatchedMigration is returned when another process modifies the
// migration_backfill_state checkpoint row concurrently while a batch is in flight.
var ErrConcurrentBatchedMigration = errors.New("concurrent batched migration modification detected")

// BatchedMigrationProgress summarizes the state of a resumable batched migration.
type BatchedMigrationProgress struct {
	Name          string
	LastCursor    string
	RowsProcessed int64
	BatchesRun    int
	Completed     bool
}

// ensureBatchedMigrationTable creates the checkpoint table used by
// RunBatchedMigration to persist per-batch cursors and completion state.
func ensureBatchedMigrationTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS migration_backfill_state (
			migration_name TEXT PRIMARY KEY,
			last_cursor    TEXT NOT NULL DEFAULT '',
			rows_processed BIGINT NOT NULL DEFAULT 0,
			updated_at     TEXT NOT NULL,
			completed_at   TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("creating migration_backfill_state table: %w", err)
	}
	return nil
}

// RunBatchedMigration executes a data backfill in bounded, independently
// committed transactions, persisting the cursor checkpoint after every batch
// so an interrupted or timed-out run resumes from the last committed batch
// without losing progress or holding long-lived row locks on a live database.
// Each checkpoint update uses an optimistic concurrency predicate on
// (migration_name, last_cursor) so concurrent runners cannot clobber cursors
// or double-count rows_processed.
func RunBatchedMigration(ctx context.Context, db *sql.DB, cfg BatchedMigrationConfig) (BatchedMigrationProgress, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return BatchedMigrationProgress{}, errors.New("batched migration name is required")
	}
	if cfg.Step == nil {
		return BatchedMigrationProgress{}, errors.New("batched migration step function is required")
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchedMigrationSize
	}
	stmtTimeout := cfg.StatementTimeout
	if stmtTimeout <= 0 {
		stmtTimeout = DefaultMigrationStatementTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if err := ensureBatchedMigrationTable(ctx, db); err != nil {
		return BatchedMigrationProgress{}, err
	}

	progress := BatchedMigrationProgress{
		Name:       cfg.Name,
		LastCursor: cfg.InitialCursor,
	}

	useDollar := shouldUseDollarPlaceholders(db, cfg)
	selectCheckpointSQL := rebindMigrationPlaceholders(
		`SELECT last_cursor, rows_processed, completed_at FROM migration_backfill_state WHERE migration_name = ?`,
		useDollar,
	)
	insertCheckpointSQL := rebindMigrationPlaceholders(`
			INSERT INTO migration_backfill_state (migration_name, last_cursor, rows_processed, updated_at, completed_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (migration_name) DO NOTHING
		`, useDollar)
	updateCheckpointSQL := rebindMigrationPlaceholders(`
			UPDATE migration_backfill_state
			SET last_cursor    = ?,
			    rows_processed = ?,
			    updated_at     = ?,
			    completed_at   = ?
			WHERE migration_name = ?
			  AND last_cursor = ?
			  AND (completed_at IS NULL OR completed_at = '')
		`, useDollar)

	hasCheckpointRow := false
	var savedCursor string
	var savedRows int64
	var completedAt sql.NullString
	err := db.QueryRowContext(
		ctx,
		selectCheckpointSQL,
		cfg.Name,
	).Scan(&savedCursor, &savedRows, &completedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Fresh migration run.
	case err != nil:
		return BatchedMigrationProgress{}, fmt.Errorf("reading checkpoint for %s: %w", cfg.Name, err)
	default:
		hasCheckpointRow = true
		progress.LastCursor = savedCursor
		progress.RowsProcessed = savedRows
		if completedAt.Valid && strings.TrimSpace(completedAt.String) != "" {
			progress.Completed = true
			return progress, nil
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return progress, err
		}

		batchStart := time.Now()
		batchCtx, cancel := context.WithTimeout(ctx, stmtTimeout)
		tx, err := db.BeginTx(batchCtx, nil)
		if err != nil {
			cancel()
			return progress, fmt.Errorf("beginning batch transaction for %s: %w", cfg.Name, err)
		}
		if !useDollar {
			// On SQLite (which opens DEFERRED transactions by default), if Step
			// runs a SELECT before its first UPDATE while another pooled
			// connection starts a write transaction, SQLite immediately returns
			// SQLITE_BUSY without waiting for busy_timeout to prevent a
			// read-to-write lock upgrade deadlock. Touching migration_backfill_state
			// first acquires the RESERVED write lock at the start of the
			// transaction so SQLite honors busy_timeout across pooled connections.
			if _, lockErr := tx.ExecContext(
				batchCtx,
				`UPDATE migration_backfill_state SET updated_at = updated_at WHERE migration_name = ?`,
				cfg.Name,
			); lockErr != nil {
				_ = tx.Rollback()
				cancel()
				return progress, fmt.Errorf("acquiring batch write lock for %s: %w", cfg.Name, lockErr)
			}
		}

		expectedCursor := progress.LastCursor
		nextCursor, rowsAffected, done, stepErr := cfg.Step(batchCtx, tx, expectedCursor, batchSize)
		if stepErr != nil {
			_ = tx.Rollback()
			cancel()
			return progress, fmt.Errorf("executing batch for %s at cursor %q: %w", cfg.Name, expectedCursor, stepErr)
		}

		if nextCursor == "" {
			nextCursor = expectedCursor
		}
		newTotal := progress.RowsProcessed + rowsAffected
		nowStr := time.Now().UTC().Format(time.RFC3339Nano)
		var completedVal any
		if done {
			completedVal = nowStr
		}

		var res sql.Result
		if !hasCheckpointRow {
			res, err = tx.ExecContext(batchCtx, insertCheckpointSQL, cfg.Name, nextCursor, newTotal, nowStr, completedVal)
		} else {
			res, err = tx.ExecContext(batchCtx, updateCheckpointSQL, nextCursor, newTotal, nowStr, completedVal, cfg.Name, expectedCursor)
		}
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return progress, fmt.Errorf("saving checkpoint for %s: %w", cfg.Name, err)
		}
		changed, affErr := res.RowsAffected()
		if affErr != nil {
			_ = tx.Rollback()
			cancel()
			return progress, fmt.Errorf("checking checkpoint update for %s: %w", cfg.Name, affErr)
		}
		if changed == 0 {
			_ = tx.Rollback()
			cancel()
			return progress, fmt.Errorf("saving checkpoint for %s: %w", cfg.Name, ErrConcurrentBatchedMigration)
		}
		hasCheckpointRow = true

		if err := tx.Commit(); err != nil {
			cancel()
			return progress, fmt.Errorf("committing batch for %s: %w", cfg.Name, err)
		}
		cancel()

		progress.LastCursor = nextCursor
		progress.RowsProcessed = newTotal
		progress.BatchesRun++
		progress.Completed = done

		logger.Info("batched migration progress",
			"migration_name", cfg.Name,
			"batch", progress.BatchesRun,
			"batch_rows", rowsAffected,
			"total_rows", progress.RowsProcessed,
			"cursor", progress.LastCursor,
			"completed", progress.Completed,
			"elapsed_ms", time.Since(batchStart).Milliseconds(),
		)

		if done {
			return progress, nil
		}
	}
}
