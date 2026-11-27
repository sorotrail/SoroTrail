package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/store"
)

// loadtestFlags holds the parsed and validated flags for `sorotrail loadtest`.
type loadtestFlags struct {
	dbURL           string
	eventsPerLedger int
	ledgers         uint
	pageLimit       uint
	duration        time.Duration
	batchSize       uint
	writeBatchSize  uint
	force           bool
}

// parseLoadtestFlags parses and validates args for `sorotrail loadtest`.
func parseLoadtestFlags(args []string) (loadtestFlags, error) {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail loadtest [flags]

Run the ingester's synthetic load-test harness against a configurable store.
The harness drives ingester.New against a deterministic SyntheticRPC, so it
measures the real ingestion path (poll, decode, upsert, persist) without
contacting the network. It writes a great number of synthetic rows, so by
default it refuses to run against a database that already holds data; pass
--force to override.

The command prints a short human summary followed by a machine-readable JSON
report (events/s, p50/p99 write latency, peak memory).

flags:
`)
		fs.PrintDefaults()
	}
	var f loadtestFlags
	fs.StringVar(&f.dbURL, "db", "", "database URL (postgres://, sqlite://, or clickhouse://; defaults to DATABASE_URL)")
	fs.IntVar(&f.eventsPerLedger, "events-per-ledger", 100, "synthetic events emitted per ledger")
	fs.UintVar(&f.ledgers, "ledgers", 10000, "number of ledgers made available to the ingester")
	fs.UintVar(&f.pageLimit, "page-limit", 1000, "synthetic RPC pagination limit")
	fs.DurationVar(&f.duration, "duration", 0, "stop the run after this duration; zero runs until caught up")
	fs.UintVar(&f.batchSize, "batch-size", 1000, "ingester read batch size (events per poll cycle)")
	fs.UintVar(&f.writeBatchSize, "write-batch-size", 1000, "events written per store upsert batch")
	fs.BoolVar(&f.force, "force", false, "run even if the database already holds data")
	if err := fs.Parse(args); err != nil {
		return loadtestFlags{}, err
	}
	if f.dbURL == "" {
		f.dbURL = os.Getenv("DATABASE_URL")
	}
	if f.dbURL == "" {
		return loadtestFlags{}, fmt.Errorf("no database URL: set DATABASE_URL or pass --db")
	}
	if f.eventsPerLedger < 1 {
		return loadtestFlags{}, fmt.Errorf("events-per-ledger must be positive")
	}
	if f.ledgers < 1 {
		return loadtestFlags{}, fmt.Errorf("ledgers must be positive")
	}
	return f, nil
}

// runLoadtest is the `sorotrail loadtest` subcommand.
func runLoadtest(args []string) error {
	f, err := parseLoadtestFlags(args)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, cleanup, err := openLoadTestStore(ctx, f.dbURL)
	if err != nil {
		return fmt.Errorf("opening store: %w", err)
	}
	defer func() { _ = cleanup() }()

	// The load test writes a lot of synthetic rows. Refuse to run against a
	// database that already holds data unless the caller passes --force.
	count, err := st.CountEvents(ctx, store.EventFilter{})
	if err != nil {
		return fmt.Errorf("checking whether the database holds data: %w", err)
	}
	if count > 0 && !f.force {
		return fmt.Errorf(
			"database already holds %d events; re-run with --force to load-test against it", count)
	}

	cfg := ingester.LoadTestConfig{
		EventsPerLedger: f.eventsPerLedger,
		Ledgers:         uint32(f.ledgers),
		PageLimit:       f.pageLimit,
		Duration:        f.duration,
		BatchSize:       f.batchSize,
		WriteBatchSize:  f.writeBatchSize,
	}

	store := &loadTestStore{Store: st}

	report, err := ingester.RunLoadTest(ctx, store, decode.XDRDecoder{}, cfg)
	if err != nil {
		return fmt.Errorf("load test: %w", err)
	}

	// Human summary.
	fmt.Printf("sorotrail loadtest: ingested %d events in %v (%.0f events/s)\n",
		report.Events, report.Duration.Round(time.Millisecond), report.EventsPerSecond)
	fmt.Printf("  max lag: %d ledgers, final lag: %d ledgers\n",
		report.MaxLagLedgers, report.FinalLagLedgers)
	fmt.Printf("  write latency p50: %v, p99: %v\n",
		report.WriteLatencyP50.Round(time.Microsecond), report.WriteLatencyP99.Round(time.Microsecond))
	fmt.Printf("  peak memory: %.2f MB\n", float64(report.PeakMemoryBytes)/1024/1024)

	// Machine-readable JSON report.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(jsonReport{
		Events:          report.Events,
		DurationSeconds: report.Duration.Seconds(),
		EventsPerSecond: report.EventsPerSecond,
		MaxLagLedgers:   report.MaxLagLedgers,
		FinalLagLedgers: report.FinalLagLedgers,
		WriteLatencyP50: float64(report.WriteLatencyP50.Microseconds()),
		WriteLatencyP99: float64(report.WriteLatencyP99.Microseconds()),
		PeakMemoryBytes: report.PeakMemoryBytes,
		EventsPerLedger: f.eventsPerLedger,
		Ledgers:         f.ledgers,
	})
}

// jsonReport is the machine-readable load-test report emitted on stdout.
type jsonReport struct {
	Events          int64   `json:"events"`
	DurationSeconds float64 `json:"duration_seconds"`
	EventsPerSecond float64 `json:"events_per_second"`
	MaxLagLedgers   uint32  `json:"max_lag_ledgers"`
	FinalLagLedgers uint32  `json:"final_lag_ledgers"`
	WriteLatencyP50 float64 `json:"write_latency_p50_ms"`
	WriteLatencyP99 float64 `json:"write_latency_p99_ms"`
	PeakMemoryBytes int64   `json:"peak_memory_bytes"`
	EventsPerLedger int     `json:"events_per_ledger"`
	Ledgers         uint    `json:"ledgers"`
}

// loadTestStore wraps a store.Store with the surface RunLoadTest needs.
type loadTestStore struct {
	store.Store
}

// LoadTestEventCount returns the total number of events in the store, which
// the load test uses to measure ingestion throughput.
func (s *loadTestStore) LoadTestEventCount(ctx context.Context) (int64, error) {
	return s.CountEvents(ctx, store.EventFilter{})
}

// LoadTestState returns the store's current ingestion state, which the load
// test uses to track the last ingested ledger and compute lag.
func (s *loadTestStore) LoadTestState(ctx context.Context) (store.IngestionState, error) {
	return s.GetIngestionState(ctx)
}

// openLoadTestStore opens the store named by databaseURL for the three
// supported backends, along with a cleanup function. Postgres is the
// production path; SQLite and ClickHouse are best-effort.
func openLoadTestStore(ctx context.Context, databaseURL string) (store.Store, func() error, error) {
	var cleanup func() error
	switch {
	case strings.HasPrefix(databaseURL, "clickhouse://"):
		st, err := store.NewStoreFromURL(databaseURL)
		if err != nil {
			return nil, nil, err
		}
		return st, nil, nil
	case strings.HasPrefix(databaseURL, "postgres://"), strings.HasPrefix(databaseURL, "postgresql://"):
		pool, err := store.NewPool(ctx, databaseURL, 4, 2, 5*time.Minute, 1*time.Minute)
		if err != nil {
			return nil, nil, err
		}
		pg := store.NewPostgresWithHealthCheck(ctx, pool, databaseURL)
		cleanup = func() error {
			pg.StopHealthCheck()
			pool.Close()
			return nil
		}
		return pg, cleanup, nil
	default:
		path := databaseURL
		for _, p := range []string{"sqlite://", "sqlite3://", "sqlite:", "sqlite3:", "file:"} {
			if strings.HasPrefix(path, p) {
				path = strings.TrimPrefix(path, p)
				break
			}
		}
		db, err := sql.Open("sqlite", path)
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() error { return db.Close() }
		return store.NewSQLite(db), cleanup, nil
	}
}
