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
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/replay"
	"github.com/sorotrail/sorotrail/internal/store"
)

// runReplay implements `sorotrail replay`: re-run the current decoder over
// stored raw XDR and rewrite the decoded columns. It is a maintenance
// command, safe to run against a live database while ingestion continues.
func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail replay --from-ledger N [--to-ledger M] [flags]

Re-runs the current decoder pipeline over stored raw XDR and rewrites the
decoded columns for events in the given ledger range.

Progress is persisted after every batch: interrupting with Ctrl-C is safe,
and re-running the same command resumes where it stopped. Only one replay
may run at a time (enforced by a Postgres advisory lock).

flags:
`)
		fs.PrintDefaults()
	}
	var (
		fromLedger = fs.Int64("from-ledger", 0, "first ledger to replay (inclusive, required)")
		toLedger   = fs.Int64("to-ledger", 0, "last ledger to replay (inclusive; 0 = no upper bound)")
		batchSize  = fs.Int("batch-size", replay.DefaultBatchSize, "events re-decoded per transaction")
		restart    = fs.Bool("restart", false, "discard saved progress and replay the range from the start")
		dryRun     = fs.Bool("dry-run", false, "report what would change without writing anything")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed
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
	if *batchSize <= 0 {
		return fmt.Errorf("--batch-size must be positive, got %d", *batchSize)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	// Ctrl-C stops between batches rather than killing the process, so the
	// in-flight transaction rolls back cleanly and progress stays consistent.
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

	r := replay.New(store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan)), decode.XDRDecoder{}, log, replay.Options{
		FromLedger: *fromLedger,
		ToLedger:   *toLedger,
		BatchSize:  *batchSize,
		Restart:    *restart,
		DryRun:     *dryRun,
	})

	sum, err := r.Run(ctx)
	if errors.Is(err, store.ErrReplayLocked) {
		return errors.New("another replay is already running against this database")
	}
	if err != nil {
		return err
	}

	printReplaySummary(sum, *dryRun)
	if !sum.Completed {
		return errInterrupted
	}
	return nil
}

// errInterrupted reports a replay that stopped early. main turns it into a
// distinct exit code so scripts can tell "stopped early, re-run me" from
// "finished" — and from a genuine failure.
var errInterrupted = errors.New("replay interrupted")

func printReplaySummary(s replay.Summary, dryRun bool) {
	mode := ""
	if dryRun {
		mode = " (dry run — nothing written)"
	}
	status := "completed"
	if !s.Completed {
		status = "interrupted — re-run the same command to resume"
	}
	fmt.Printf(`replay %s%s
  rows processed: %d
  rows changed:   %d
  rows skipped:   %d (no raw XDR stored)
  rows failed:    %d (stored XDR could not be decoded)
  duration:       %s
`, status, mode, s.Processed, s.Changed, s.Skipped, s.Failed, s.Duration.Round(time.Millisecond))
}
