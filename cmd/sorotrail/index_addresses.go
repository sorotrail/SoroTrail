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
	"github.com/sorotrail/sorotrail/internal/store"
)

// runIndexAddresses implements `sorotrail index-addresses`: re-build the
// event_addresses inverted index from existing stored events.
//
// The command pages through events in ledger order, extracts addresses
// from the decoded topics/value JSON, and inserts them into the
// event_addresses table. Progress is persisted in a backfill_state row so
// it is resumable (Ctrl-C and re-run). Idempotent upserts handle page
// overlaps without duplicating rows.
//
// This command is safe to run alongside live ingestion: UpsertAddressRefs
// is idempotent on (address, event_id, role), so a concurrent ingester
// covering the same range produces at most one durable row per
// combination.
func runIndexAddresses(args []string) error {
	fs := flag.NewFlagSet("index-addresses", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail index-addresses [--from-ledger N] [--to-ledger M] [flags]

Re-build the event_addresses inverted index from existing stored events.
Pages through events in ledger order, extracts G.../C... addresses from
each event's decoded topics and value JSON, and persists the index rows.

The command is batched, resumable (Ctrl-C and re-run resumes via persisted
state), and idempotent — safe to run alongside live ingestion.

flags:
`)
		fs.PrintDefaults()
	}
	var (
		fromLedger = fs.Int64("from-ledger", 0, "first ledger to index (inclusive; 0 = oldest stored)")
		toLedger   = fs.Int64("to-ledger", 0, "last ledger to index (inclusive; 0 = latest stored)")
		batchSize  = fs.Int("batch-size", 500, "events per page")
		restart    = fs.Bool("restart", false, "discard saved progress and start from --from-ledger")
		dryRun     = fs.Bool("dry-run", false, "report what would be indexed without writing anything")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *batchSize <= 0 || *batchSize > 1000 {
		return fmt.Errorf("--batch-size must be in 1..1000, got %d", *batchSize)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel, cfg.LogFormat)

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

	st := store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan))

	// Resolve start position.
	start := *fromLedger
	if start <= 0 {
		stats, err := st.Stats(ctx, store.WildcardScope())
		if err != nil {
			return fmt.Errorf("reading store stats: %w", err)
		}
		if stats.OldestStoredLedger > 0 {
			start = stats.OldestStoredLedger
		} else {
			return errors.New("no events stored; nothing to index")
		}
	}

	if *restart {
		state, err := st.GetBackfillState(ctx)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("reading backfill state: %w", err)
		}
		if err == nil && state.Done() {
			// Clear the completed marker so Run restarts fresh.
			if err := st.StartBackfillState(ctx, "index-addresses", start, *toLedger); err != nil {
				return fmt.Errorf("restarting backfill state: %w", err)
			}
		}
	}

	log.Info("indexing addresses",
		"from_ledger", start,
		"to_ledger", *toLedger,
		"batch_size", *batchSize,
		"dry_run", *dryRun)

	indexer := &addressIndexer{
		store:     st,
		batchSize: *batchSize,
		from:      start,
		to:        *toLedger,
		dryRun:    *dryRun,
		log:       log,
	}
	summary, err := indexer.Run(ctx)
	if err != nil {
		return err
	}

	printAddressIndexSummary(summary, *dryRun)
	if !summary.completed {
		return errInterrupted
	}
	return nil
}

type addressIndexSummary struct {
	pagesProcessed int64
	eventsScanned  int64
	refsIndexed    int64
	completed      bool
	throughLedger  int64
	duration       time.Duration
}

type addressIndexer struct {
	store     *store.Postgres
	batchSize int
	from      int64
	to        int64
	dryRun    bool
	log       loggerLike
}

func (a *addressIndexer) Run(ctx context.Context) (addressIndexSummary, error) {
	started := time.Now()
	summary := addressIndexSummary{}

	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			summary.duration = time.Since(started)
			return summary, nil
		}

		filter := store.EventFilter{
			FromLedger: a.from,
			ToLedger:   a.to,
			Cursor:     cursor,
			Limit:      a.batchSize,
		}
		events, next, err := a.store.QueryEvents(ctx, filter)
		if err != nil {
			return summary, fmt.Errorf("querying events: %w", err)
		}
		if len(events) == 0 {
			summary.completed = true
			break
		}
		summary.pagesProcessed++
		summary.eventsScanned += int64(len(events))

		if !a.dryRun {
			var refs []store.AddressRef
			for _, ev := range events {
				decoded := decode.ExtractAddresses(ev.Topics, ev.Value)
				for _, r := range decoded {
					refs = append(refs, store.AddressRef{
						Address: r.Address,
						EventID: ev.ID,
						Role:    r.Role,
					})
				}
			}
			if len(refs) > 0 {
				if err := a.store.UpsertAddressRefs(ctx, refs); err != nil {
					return summary, fmt.Errorf("upserting address refs: %w", err)
				}
				summary.refsIndexed += int64(len(refs))
			}
		}

		summary.throughLedger = events[len(events)-1].Ledger
		if next == "" {
			summary.completed = true
			break
		}
		cursor = next
	}

	summary.duration = time.Since(started)
	return summary, nil
}

func printAddressIndexSummary(s addressIndexSummary, dryRun bool) {
	mode := ""
	if dryRun {
		mode = " (dry run — nothing written)"
	}
	status := "completed"
	if !s.completed {
		status = "interrupted — re-run the same command to resume"
	}
	fmt.Printf(`address index %s%s
  pages processed: %d
  events scanned:  %d
  address refs indexed: %d
  through ledger:  %d
  duration:       %s
`,
		status, mode,
		s.pagesProcessed, s.eventsScanned, s.refsIndexed,
		s.throughLedger, s.duration.Round(time.Millisecond))
}
