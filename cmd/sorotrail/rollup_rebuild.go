package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// runRollupRebuild implements `sorotrail rollup-rebuild`: reconstruct the
// rollup_events and rollup_token_volume tables from stored events.
// Safe to run against a live database (advisory lock prevents concurrent
// rebuilds; live ingestion keeps updating rollups for new events).
func runRollupRebuild(args []string) error {
	fs := flag.NewFlagSet("rollup-rebuild", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail rollup-rebuild --from-ledger N [--to-ledger M]

Reconstructs analytics rollup tables (rollup_events, rollup_token_volume)
from stored events in the given ledger range.

Deletes existing rollup data for the range and rebuilds it from raw events.
Only one rebuild may run at a time (enforced by a Postgres advisory lock).

flags:
`)
		fs.PrintDefaults()
	}
	fromLedger := fs.Int64("from-ledger", 0, "first ledger to rebuild (inclusive, required)")
	toLedger := fs.Int64("to-ledger", 0, "last ledger to rebuild (inclusive; 0 = no upper bound)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *fromLedger <= 0 {
		fs.Usage()
		return errors.New("--from-ledger is required and must be positive")
	}
	if *toLedger != 0 && *toLedger < *fromLedger {
		return fmt.Errorf("--to-ledger %d is before --from-ledger %d", *toLedger, *fromLedger)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	_ = newLogger(cfg.LogLevel, cfg.LogFormat) // log level validated, but rebuild is non-interactive

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

	st := store.NewPostgres(pool)

	lock, err := st.AcquireRollupRebuildLock(ctx)
	if err != nil {
		return fmt.Errorf("acquiring rebuild lock: %w", err)
	}
	defer lock.Release()

	sum, err := st.RebuildRollups(ctx, *fromLedger, *toLedger)
	if err != nil {
		return err
	}

	printRebuildSummary(sum)
	if !sum.Completed {
		return errInterrupted
	}
	return nil
}

func printRebuildSummary(s store.RollupRebuildSummary) {
	status := "completed"
	if !s.Completed {
		status = "interrupted"
	}
	toStr := "unbounded"
	if s.ToLedger > 0 {
		toStr = fmt.Sprintf("%d", s.ToLedger)
	}
	fmt.Printf(`rollup rebuild %s
  ledger range:   [%d, %s]
  events scanned: %d
  buckets filled: %d
  duration:       %s
`, status, s.FromLedger, toStr, s.EventsScanned, s.BucketsFilled, s.Duration.Round(time.Millisecond))
}
