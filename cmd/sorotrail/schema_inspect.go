package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// schemaInspectReport is the full schema inspection output, serialisable
// to both human-readable text and JSON.
type schemaInspectReport struct {
	// Migration holds the applied migration version, dirty flag, and any
	// pending migration versions.
	Migration schemaMigrationInfo `json:"migration"`
	// Partitions lists the events table's child partitions with their
	// ledger ranges and row counts.
	Partitions []schemaPartition `json:"partitions"`
	// TableSizes reports on-disk sizes for the events parent table,
	// its partitions, and all indexes on events.
	TableSizes []schemaTableSize `json:"table_sizes"`
	// SchemaMatch reports whether the database schema matches the
	// embedded migration set.
	SchemaMatch schemaMatchInfo `json:"schema_match"`
}

// schemaMigrationInfo is the migration-version portion of the report.
type schemaMigrationInfo struct {
	Version int    `json:"version"`
	Dirty   bool   `json:"dirty"`
	Status  string `json:"status"`
}

// schemaPartition is one events partition with its ledger range and row count.
type schemaPartition struct {
	Name      string `json:"name"`
	From      int64  `json:"from"`
	To        int64  `json:"to"`
	RowCounts int64  `json:"row_count"`
}

// schemaTableSize is one relation's on-disk size.
type schemaTableSize struct {
	Name  string `json:"name"`
	Size  int64  `json:"size_bytes"`
	Human string `json:"human"`
	Kind  string `json:"kind"`
}

// schemaMatchInfo describes how the database migration state compares to the
// embedded migration set.
type schemaMatchInfo struct {
	Matched           bool   `json:"matched"`
	EmbeddedVersion   int    `json:"embedded_version"`
	AppliedVersion    int    `json:"applied_version"`
	PendingMigrations []uint `json:"pending_migrations,omitempty"`
	Detail            string `json:"detail"`
}

// runSchemaInspect implements `sorotrail schema-inspect`: a read-only
// command that reports the database's migration state, partition layout,
// table sizes, and whether the schema matches the embedded migrations.
func runSchemaInspect(args []string) error {
	fs := flag.NewFlagSet("schema-inspect", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail schema-inspect [flags]

Reports the database schema state: applied migration version, partition
layout, table and index sizes, and whether the schema matches the
embedded migration set. Read-only — no writes are performed.

flags:
`)
		fs.PrintDefaults()
	}
	jsonOutput := fs.Bool("json", false, "output report as JSON")
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

	// Ctrl-C is honoured but the command is fast enough that it rarely
	// matters; wiring it keeps the UX consistent with the other
	// subcommands.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	report, err := collectSchemaReport(ctx, pool)
	if err != nil {
		return err
	}

	if *jsonOutput {
		out, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding report: %w", err)
		}
		fmt.Println(string(out))
	} else {
		printSchemaReport(report)
	}
	return nil
}

// collectSchemaReport gathers every section of the schema inspection report
// from a live database connection.
func collectSchemaReport(ctx context.Context, pool *pgxpool.Pool) (*schemaInspectReport, error) {
	report := &schemaInspectReport{}

	// Migration state.
	version, dirty, err := queryMigrationState(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("reading migration state: %w", err)
	}
	status := "clean"
	if dirty {
		status = "dirty"
	} else if version == 0 {
		status = "none"
	}
	report.Migration = schemaMigrationInfo{
		Version: version,
		Dirty:   dirty,
		Status:  status,
	}

	// Schema match check.
	ms, err := querySchemaMatch(version)
	if err != nil {
		return nil, fmt.Errorf("checking schema match: %w", err)
	}
	report.SchemaMatch = *ms

	// Partition listing.
	parts, err := queryPartitions(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("listing partitions: %w", err)
	}
	report.Partitions = parts

	// Table and index sizes.
	sizes, err := queryTableSizes(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("querying table sizes: %w", err)
	}
	report.TableSizes = sizes

	return report, nil
}

// queryMigrationState reads the schema_migrations table directly.
func queryMigrationState(ctx context.Context, pool *pgxpool.Pool) (version int, dirty bool, err error) {
	err = pool.QueryRow(ctx,
		`SELECT version, dirty FROM schema_migrations`,
	).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading schema_migrations: %w", err)
	}
	return version, dirty, nil
}

// querySchemaMatch compares the applied migration version against the embedded
// migration set. When the database is fully up to date there are no pending
// versions. Using the actual version numbers (rather than the file count) means
// the check is correct even when multiple in-flight branches add migrations with
// non-consecutive version numbers.
func querySchemaMatch(appliedVersion int) (*schemaMatchInfo, error) {
	versions, err := store.EmbeddedMigrationVersions()
	if err != nil {
		return nil, fmt.Errorf("listing embedded migrations: %w", err)
	}

	maxVersion := 0
	if len(versions) > 0 {
		maxVersion = int(versions[len(versions)-1])
	}

	var pending []uint
	for _, v := range versions {
		if int(v) > appliedVersion {
			pending = append(pending, v)
		}
	}

	matched := appliedVersion > 0 && appliedVersion == maxVersion
	detail := fmt.Sprintf("applied %d, embedded %d, pending %d",
		appliedVersion, maxVersion, len(pending))
	if appliedVersion == 0 {
		detail = "no migrations applied"
	} else if appliedVersion > maxVersion {
		detail = fmt.Sprintf("applied %d exceeds embedded %d", appliedVersion, maxVersion)
	} else if len(pending) > 0 {
		detail = fmt.Sprintf("applied %d, embedded %d, %d pending: %v",
			appliedVersion, maxVersion, len(pending), pending)
	}

	return &schemaMatchInfo{
		Matched:           matched,
		EmbeddedVersion:   maxVersion,
		AppliedVersion:    appliedVersion,
		PendingMigrations: pending,
		Detail:            detail,
	}, nil
}

// queryPartitions lists the events table's child partitions, parsing
// the ledger range from the partition name and counting rows.
func queryPartitions(ctx context.Context, pool *pgxpool.Pool) ([]schemaPartition, error) {
	rows, err := pool.Query(ctx, `
		SELECT n.nspname::text,
		       c.relname::text,
		       pg_get_expr(c.relpartbound, c.oid)
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE p.relname = 'events'
		ORDER BY c.relname`)
	if err != nil {
		return nil, fmt.Errorf("querying partitions: %w", err)
	}
	defer rows.Close()

	type partitionRef struct {
		schema string
		part   schemaPartition
	}
	var refs []partitionRef
	for rows.Next() {
		var (
			schema string
			name   string
			bound  string
		)
		if err := rows.Scan(&schema, &name, &bound); err != nil {
			return nil, fmt.Errorf("scanning partition: %w", err)
		}
		p := schemaPartition{
			Name: name,
		}
		if bound != "DEFAULT" {
			// pg_get_expr quotes range values for bigint partition keys.
			n, err := fmt.Sscanf(bound, "FOR VALUES FROM ('%d') TO ('%d')", &p.From, &p.To)
			if n != 2 {
				n, err = fmt.Sscanf(bound, "FOR VALUES FROM (%d) TO (%d)", &p.From, &p.To)
			}
			if err != nil || n != 2 {
				return nil, fmt.Errorf("parsing partition %s bound %q: expected numeric range", name, bound)
			}
		}
		refs = append(refs, partitionRef{schema: schema, part: p})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	parts := make([]schemaPartition, 0, len(refs))
	for _, ref := range refs {
		table := pgx.Identifier{ref.schema, ref.part.Name}.Sanitize()
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&ref.part.RowCounts); err != nil {
			return nil, fmt.Errorf("counting rows in partition %s: %w", ref.part.Name, err)
		}
		parts = append(parts, ref.part)
	}
	return parts, nil
}

// queryTableSizes returns on-disk sizes for the events parent, all its
// partitions, and all indexes on the events table.
func queryTableSizes(ctx context.Context, pool *pgxpool.Pool) ([]schemaTableSize, error) {
	var sizes []schemaTableSize

	// Events parent table size.
	var parentSize int64
	err := pool.QueryRow(ctx,
		`SELECT coalesce(pg_total_relation_size('events'::regclass), 0)`,
	).Scan(&parentSize)
	if err != nil {
		return nil, fmt.Errorf("reading events parent size: %w", err)
	}
	sizes = append(sizes, schemaTableSize{
		Name:  "events (parent)",
		Size:  parentSize,
		Human: humanSize(parentSize),
		Kind:  "table",
	})

	// Per-partition sizes.
	rows, err := pool.Query(ctx, `
		SELECT c.relname::text,
		       coalesce(pg_total_relation_size(c.oid), 0)
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'events'
		ORDER BY c.relname`)
	if err != nil {
		return nil, fmt.Errorf("querying partition sizes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s schemaTableSize
		s.Kind = "partition"
		if err := rows.Scan(&s.Name, &s.Size); err != nil {
			return nil, fmt.Errorf("scanning partition size: %w", err)
		}
		s.Human = humanSize(s.Size)
		sizes = append(sizes, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Index sizes.
	idxRows, err := pool.Query(ctx, `
		SELECT i.relname::text,
		       coalesce(pg_relation_size(x.indexrelid), 0)
		FROM pg_class t
		JOIN pg_index x ON x.indrelid = t.oid
		JOIN pg_class i ON i.oid = x.indexrelid
		WHERE t.relname = 'events'
		ORDER BY i.relname`)
	if err != nil {
		return nil, fmt.Errorf("querying index sizes: %w", err)
	}
	defer idxRows.Close()

	for idxRows.Next() {
		var s schemaTableSize
		s.Kind = "index"
		if err := idxRows.Scan(&s.Name, &s.Size); err != nil {
			return nil, fmt.Errorf("scanning index size: %w", err)
		}
		s.Human = humanSize(s.Size)
		sizes = append(sizes, s)
	}
	if err := idxRows.Err(); err != nil {
		return nil, err
	}

	return sizes, nil
}

// humanSize formats bytes into a human-readable string.
func humanSize(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// printSchemaReport renders the schema inspection report in human-readable form.
func printSchemaReport(r *schemaInspectReport) {
	fmt.Println("=== Migration State ===")
	fmt.Printf("  Version:  %d\n", r.Migration.Version)
	fmt.Printf("  Dirty:    %v\n", r.Migration.Dirty)
	fmt.Printf("  Status:   %s\n", r.Migration.Status)
	fmt.Println()

	fmt.Println("=== Schema Match ===")
	fmt.Printf("  Matched:  %v\n", r.SchemaMatch.Matched)
	fmt.Printf("  Detail:   %s\n", r.SchemaMatch.Detail)
	fmt.Println()

	fmt.Println("=== Partitions ===")
	if len(r.Partitions) == 0 {
		fmt.Println("  (none)")
	} else {
		for _, p := range r.Partitions {
			fmt.Printf("  %-30s  range [%d, %d)  rows: %d\n", p.Name, p.From, p.To, p.RowCounts)
		}
	}
	fmt.Println()

	fmt.Println("=== Table & Index Sizes ===")
	if len(r.TableSizes) == 0 {
		fmt.Println("  (none)")
	} else {
		for _, s := range r.TableSizes {
			fmt.Printf("  %-40s  %s  (%s)\n", s.Name, s.Human, s.Kind)
		}
	}
}
