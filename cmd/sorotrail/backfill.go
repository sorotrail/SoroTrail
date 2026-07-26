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

	"github.com/sorotrail/sorotrail/internal/backfill"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/store"
)

// runBackfill implements `sorotrail backfill`: pull historical
// contract events for a single contract from Horizon, decode them
// through the standard pipeline, and upsert into the events table.
//
// The command is batched, resumable (Ctrl-C and re-run resumes via
// the persisted backfill_state row), idempotent (UpsertEvents is
// idempotent on event ID), and safe alongside live ingestion.
//
// Rate limiting against Horizon is shared between every page in this
// invocation; price-budgeting across runs is the operator's job —
// run sequentially or with a shared rate gate between operators.
func runBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail backfill --contract C... --from-ledger N [--to-ledger M] [flags]

Index historical contract events from Horizon's transaction history — events
older than the Stellar RPC retention window, so the indexer can fill in the
gap between the earliest RPC-retained ledger and the contract's deployment.

The command pages /accounts/{contract_id}/transactions, decodes each tx's
result_meta_xdr through the standard pipeline (so backfilled rows are
indistinguishable from live-ingested ones, including raw XDR), and upserts
them into the events table.

Progress is persisted after each committed page. Pressing Ctrl-C and
re-running the same command resumes where the previous run stopped; pages
overlap at the resume boundary but idempotent upserts handle that without
duplicating rows. See docs/backfill.md for source limitations and the
resume/live-overlap semantics.

flags:
`)
		fs.PrintDefaults()
	}
	var (
		contractID  = fs.String("contract", "", "contract ID to backfill events for (required, C… strkey)")
		fromLedger  = fs.Int64("from-ledger", 0, "first ledger to backfill (inclusive, required)")
		toLedger    = fs.Int64("to-ledger", 0, "last ledger to backfill (inclusive; 0 = no upper bound)")
		batchSize   = fs.Int("batch-size", backfill.DefaultBatchSize, "transactions per Horizon page (max 200)")
		rps         = fs.Float64("rps", 0, "horizon requests per second (overrides BACKFILL_RATE_RPS env)")
		horizonURL  = fs.String("horizon-url", "", "horizon REST base URL (overrides HORIZON_URL env)")
		includeFail = fs.Bool("include-failed", true, "include transactions whose tx-level result was failed")
		dryRun      = fs.Bool("dry-run", false, "report what would change without writing anything")
		restart     = fs.Bool("restart", false, "discard saved progress and start from --from-ledger")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed
		}
		return err
	}
	if !config.ValidContractID(*contractID) {
		fs.Usage()
		return errors.New("--contract is required and must be a valid C... strkey")
	}
	if *fromLedger <= 0 {
		fs.Usage()
		return errors.New("--from-ledger is required and must be positive")
	}
	if *toLedger != 0 && *toLedger < *fromLedger {
		return fmt.Errorf("--to-ledger %d is before --from-ledger %d", *toLedger, *fromLedger)
	}
	if *batchSize <= 0 || *batchSize > 200 {
		return fmt.Errorf("--batch-size must be in 1..200, got %d", *batchSize)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	// Ctrl-C stops between pages rather than killing the process, so
	// the in-flight upsert commits cleanly and progress stays consistent.
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

	hURL := cfg.HorizonURL
	if *horizonURL != "" {
		hURL = *horizonURL
	}
	rpsFinal := cfg.BackfillRateRPS
	if *rps > 0 {
		rpsFinal = *rps
	}
	if rpsFinal <= 0 {
		return errors.New("--rps or BACKFILL_RATE_RPS must be positive")
	}
	minInterval := time.Duration(float64(time.Second) / rpsFinal)

	st := store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan))
	hClient := horizon.NewHTTPClient(hURL, minInterval)

	b := backfill.New(hClient, st, decode.XDRDecoder{}, log, backfill.Options{
		ContractID:    *contractID,
		FromLedger:    *fromLedger,
		ToLedger:      *toLedger,
		BatchSize:     *batchSize,
		HorizonURL:    hURL,
		MinInterval:   minInterval,
		IncludeFailed: *includeFail,
		DryRun:        *dryRun,
		MaxBackoff:    backfill.DefaultMaxBackoff,
	})
	if *restart {
		// Drop any saved state that's now stale: a fresh Start happens
		// at the top of Run, but we still need to cancel a "completed"
		// marker from a finished previous run with the same params.
		if err := st.StartBackfillState(ctx, *contractID, *fromLedger, *toLedger); err != nil {
			return fmt.Errorf("clearing saved progress: %w", err)
		}
	}
	if *dryRun {
		log.Info("backfill dry-run (no writes)",
			"contract_id", *contractID,
			"from_ledger", *fromLedger,
			"to_ledger", *toLedger,
			"rate_rps", rpsFinal,
			"horizon_url", hURL)
	}

	sum, err := runBackfillWithRetry(ctx, b, log)
	if err != nil {
		return err
	}

	printBackfillSummary(sum, *dryRun)
	if !sum.Completed {
		return errInterrupted
	}
	return nil
}

// runBackfillWithRetry runs the backfiller and retries on transient
// Horizon errors (rate limits) with jittered exponential backoff up to
// MaxBackoff. Other errors surface immediately.
func runBackfillWithRetry(ctx context.Context, b *backfill.Backfiller, log loggerLike) (backfill.Summary, error) {
	backoff := time.Second
	for {
		sum, err := b.Run(ctx)
		if ctx.Err() != nil {
			return sum, nil
		}
		if err == nil {
			return sum, nil
		}
		if !isRetryableBackfillErr(err) {
			return backfill.Summary{}, err
		}
		sleep := backoff/2 + jitter(backoff/2)
		if sleep > backfill.DefaultMaxBackoff {
			sleep = backfill.DefaultMaxBackoff
		}
		log.Warn("backfill transient error; retrying",
			"error", err, "retry_in", sleep)
		if !sleepCtx(ctx, sleep) {
			// Interrupt during retry backs off: return the partial
			// summary so the operator sees what was committed.
			return backfill.Summary{}, nil
		}
		backoff *= 2
	}
}

// loggerLike is the same shape as *slog.Logger — interface kept tiny
// so runBackfillWithRetry can be tested without importing slog.
type loggerLike interface {
	Warn(msg string, args ...any)
}

// jitter returns a uniformly distributed random duration in [0,d].
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// math/rand is fine here; backoff isn't a security-sensitive path.
	return time.Duration(randInt(int64(d)))
}

// sleepCtx is a tiny shim mirroring ingester.sleepCtx kept in package
// so cmd module has no internal cycle.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// randInt provides a tiny thread-safe pseudo-random int64 in [0,n).
// Seeded from runtime.nanotime via math/rand/v2 indirectly through
// the rand package's package-level init.
func randInt(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(uint64(time.Now().UnixNano()) % uint64(n))
}

// isRetryableBackfillErr returns true for the small handful of Horizon
// errors worth backing off and retrying. Anything else is fatal so a
// real bug surfaces immediately.
func isRetryableBackfillErr(err error) bool {
	return errors.Is(err, horizon.ErrRateLimited) || !errors.Is(err, horizon.ErrNotFound) &&
		isTransientHTTP(err)
}

// isTransientHTTP looks for HTTP/transport messages the standard Go
// client returns when a connection drops or times out. We keep this
// permissive: a misclassified non-retryable error just gets re-tried
// once and surfaces on the second failure.
func isTransientHTTP(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, frag := range []string{
		"connection reset",
		"connection refused",
		"EOF",
		"timeout",
		"temporary failure",
	} {
		if contains(s, frag) {
			return true
		}
	}
	return false
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// printBackfillSummary mirrors replay's printReplaySummary shape so
// operator scripts see consistent backfill output.
func printBackfillSummary(s backfill.Summary, dryRun bool) {
	mode := ""
	if dryRun {
		mode = " (dry run — nothing written)"
	}
	status := "completed"
	if !s.Completed {
		status = "interrupted — re-run the same command to resume"
	}
	resumed := ""
	if s.Resumed {
		resumed = "resumed from saved progress\n  "
	}
	fmt.Printf(`backfill %s%s
  pages fetched:   %d
  transactions:   %d
  events skipped:  %d (no meta or no events emitted)
  events failed:   %d (XDR decode error)
  events extracted: %d
  events inserted: %d (idempotent upsert: dedupes rencounters)
  through ledger: %d
  duration:       %s
`,
		status, mode, s.PagesFetched, s.Transactions, s.Skipped, s.Failed,
		s.Extracted, s.Inserted, s.ThroughLedger, s.Duration.Round(time.Millisecond))
	if resumed != "" {
		fmt.Printf("  %s\n", resumed[:len(resumed)-1])
	}
}
