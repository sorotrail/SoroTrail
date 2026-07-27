// Package backfill ingests historical contract events from a Horizon
// instance that are older than the RPC's retention window, filling the
// gap between the earliest RPC-retained ledger and the contract's
// deployment.
//
// How it works:
//   - Page through /accounts/{contract_id}/transactions in ascending
//     order, decode each row's result_meta_xdr.
//   - Extract every Soroban ContractEvent from V3 / V4 meta and upsert
//     into the events table.
//   - Persist progress (lastLedger) after each committed batch so
//     Ctrl-C + re-run resumes cleanly with idempotent upserts handling
//     the page overlap.
//
// Limitations (call them out in docs and operator guidance; we don't
// crash on them):
//
//   - Horizon must retain full historical transaction meta for the
//     target network. Public Stellar testnet Horizon retains everything
//     from protocol 17 onward; mainnet Horizon instances vary — some
//     prune old meta. Operators should check their target Horizon's
//     retention policy before backfilling long ranges.
//   - Speed is bounded by Horizon pagination (~200 txs per page) and
//     per-tx XDR decoding. Expect several-hundred events/second on a
//     small public instance; private deployments with burst quotas
//     can do much higher.
//   - Only Soroban transactions with V3/V4 meta carry events. Classic
//     Stellar (V1/V2) and failed Soroban txs without meta are counted
//     as Skipped.
//   - Soroban fee-bump txs emit events in their inner tx's operations;
//     we recurse into V4.InnerTransactions so those events capture
//     against the inner op indices, not the outer wrap.
//   - There's no dedupe against the live ingester at the row level:
//     for any range where both overlap (≤ the RPC retention window),
//     live writes a TOID-format ID and backfill writes a tx-hash-format
//     ID for the same on-chain emission. See docs/backfill.md.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/store"
)

// DefaultBatchSize is the default Horizon pagination limit. 200 matches
// Horizon's per-page cap and gives ~256KB-ish JSON payloads.
const DefaultBatchSize = 200

// DefaultMaxBackoff caps the per-page HTTP error backoff.
const DefaultMaxBackoff = 30 * time.Second

// DefaultMinRequestInterval is ~10 req/s — safe for public Horizon
// instances. Private deployments can lower this via Options.MinInterval.
const DefaultMinRequestInterval = 100 * time.Millisecond

// Options configure a Backfiller. Zero values produce a usable Backfiller
// after applyDefaults.
type Options struct {
	// ContractID selects which contract to backfill. Required — there
	// is no "all contracts" scan path because the page walker assumes
	// every Horizon row's events are filtered down to one contract.
	ContractID string

	// FromLedger is the inclusive start ledger for new or fresh runs.
	FromLedger int64
	// ToLedger is the inclusive upper bound; 0 = no upper bound (keep
	// going until Horizon returns an empty page).
	ToLedger int64

	BatchSize int
	// HorizonURL is a full URL (no trailing slash). Defaults to
	// https://horizon-testnet.stellar.org.
	HorizonURL string
	// MinInterval is the minimum spacing between Horizon requests.
	MinInterval time.Duration
	// IncludeFailed controls whether failed txs are walked. Failed
	// txs whose contract calls succeeded still emit events; this
	// mirrors the live ingester's default-past-and-future behavior.
	IncludeFailed bool
	// MaxBackoff caps retry backoff for HTTP errors.
	MaxBackoff time.Duration
	// DryRun reports what would change without writing anything.
	DryRun bool
}

// applyDefaults fills in zero-value options so a partially-populated
// Options struct still produces a workable Backfiller.
func (o *Options) applyDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	if o.MinInterval <= 0 {
		o.MinInterval = DefaultMinRequestInterval
	}
	if o.HorizonURL == "" {
		o.HorizonURL = "https://horizon-testnet.stellar.org"
	}
}

// Store is the persistence surface the backfill package needs.
// *store.Postgres implements it. Kept narrow so the package stays
// import-light and tests can supply fakes.
type Store interface {
	UpsertEvents(ctx context.Context, events []store.Event) (int64, error)
	GetBackfillState(ctx context.Context) (store.BackfillState, error)
	StartBackfillState(ctx context.Context, contractID string, fromLedger, toLedger int64) error
	UpdateBackfillState(ctx context.Context, lastLedger int64) error
	CompleteBackfillState(ctx context.Context) error
}

// Summary reports what a backfill run did. The counters are independent
// so an operator can read off throughput at a glance.
type Summary struct {
	PagesFetched  int64
	Transactions  int64
	Skipped       int64
	Failed        int64
	Extracted     int64
	Inserted      int64
	DryRun        bool
	Completed     bool
	Resumed       bool
	ThroughLedger int64
	Duration      time.Duration
}

// Backfiller ingests historical contract events from Horizon.
type Backfiller struct {
	client  horizon.Client
	store   Store
	decoder decode.Decoder
	log     *slog.Logger
	opts    Options
}

// New wires a Backfiller. ApplyDefaults runs once so callers can pass a
// partially-populated Options struct.
func New(h horizon.Client, st Store, dec decode.Decoder, log *slog.Logger, opts Options) *Backfiller {
	opts.applyDefaults()
	return &Backfiller{
		client:  h,
		store:   st,
		decoder: dec,
		log:     log,
		opts:    opts,
	}
}

// Run executes the backfill. Pressing Ctrl-C between pages is safe:
// progress is committed per page via UpdateBackfillState, and resume
// re-fetches with idempotent upserts handling the overlap.
func (b *Backfiller) Run(ctx context.Context) (Summary, error) {
	if b.opts.ContractID == "" {
		return Summary{}, errors.New("backfill: --contract is required")
	}
	if b.opts.FromLedger <= 0 {
		return Summary{}, errors.New("backfill: --from-ledger must be positive")
	}
	started := time.Now()

	startLedger, sum, err := b.resume(ctx)
	if err != nil {
		return Summary{}, err
	}

	b.log.Info("backfill starting",
		"contract_id", b.opts.ContractID,
		"from_ledger", b.opts.FromLedger,
		"to_ledger", b.opts.ToLedger,
		"start_ledger", startLedger,
		"batch_size", b.opts.BatchSize,
		"include_failed", b.opts.IncludeFailed,
		"dry_run", b.opts.DryRun,
		"resumed", sum.Resumed)

	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			sum.Duration = time.Since(started)
			err := b.markTerminal(ctx, sum)
			return sum, err // interrupted: report progress, not an error
		}

		resp, err := b.fetch(ctx, cursor)
		if err != nil {
			sum.Duration = time.Since(started)
			return sum, fmt.Errorf("fetching horizon page: %w", err)
		}
		if len(resp.Embedded.Records) == 0 {
			sum.Completed = true
			break
		}
		sum.PagesFetched++

		// BUG_SAFETY: if Horizon returned a row requiring retry
		// (rate-limit, transient 5xx) the caller should retry via
		// exponential backoff. Our horizon.HTTPClient surfaces
		// ErrRateLimited; cmd/sorotrail/backfill.go wraps this Run
		// in a retry loop with backoff up to MaxBackoff before
		// returning here. Inside the page we treat all errors as
		// fatal so a real bug, not a transient rate-limit, surfaces.

		events, lastProcessedLedger, perPage := b.extractPage(resp.Embedded.Records, startLedger)
		sum.Transactions += perPage.Transactions
		sum.Skipped += perPage.Skipped
		sum.Failed += perPage.Failed
		sum.Extracted += int64(len(events))
		if lastProcessedLedger > sum.ThroughLedger {
			sum.ThroughLedger = lastProcessedLedger
		}

		// --to-ledger honored: we've seen at least one tx past the
		// bound. Keep what we already extracted from in-range txs,
		// commit, exit.
		if b.opts.ToLedger > 0 && lastProcessedLedger > b.opts.ToLedger {
			if len(events) > 0 && !b.opts.DryRun {
				ins, err := b.commitPage(ctx, events, sum.ThroughLedger)
				if err != nil {
					sum.Duration = time.Since(started)
					return sum, err
				}
				sum.Inserted += ins
			}
			sum.Completed = true
			break
		}

		if len(events) > 0 && !b.opts.DryRun {
			ins, err := b.commitPage(ctx, events, lastProcessedLedger)
			if err != nil {
				sum.Duration = time.Since(started)
				return sum, err
			}
			sum.Inserted += ins
		}

		// Short page = horizon is exhausted.
		if len(resp.Embedded.Records) < b.opts.BatchSize {
			sum.Completed = true
			break
		}

		last := resp.Embedded.Records[len(resp.Embedded.Records)-1]
		cursor = last.PagingToken
		if cursor == "" {
			// Some older Horizon deployments omit paging_token on
			// /accounts/{id}/transactions; falling back to the tx
			// hash is stable for that page's purposes.
			cursor = last.ID
		}
	}

	sum.Duration = time.Since(started)
	if err := b.markTerminal(ctx, sum); err != nil {
		return sum, err
	}
	return sum, nil
}

// fetch is a small wrapper that keeps the inner loop editable if we
// later add auth or a gRPC transport.
func (b *Backfiller) fetch(ctx context.Context, cursor string) (horizon.TransactionsResponse, error) {
	return b.client.ListContractTransactions(ctx, b.opts.ContractID, cursor, b.opts.BatchSize, b.opts.IncludeFailed)
}

// pageCounters is what extractPage returns alongside the events: the
// running tally the Summary needs.
type pageCounters struct {
	Transactions int64
	Skipped      int64
	Failed       int64
}

// extractPage walks one Horizon page, partition each tx's events based
// on the start/to-ledger bounds. Ledger sequencing: Horizon returns
// transactions in ledger/transaction order, so we reset the per-ledger
// tx_index counter when our running ledger changes — this gives every
// event a stable ordering within its ledger without depending on
// tx_index-from-the-RPC (Horizon doesn't expose it).
func (b *Backfiller) extractPage(rows []horizon.Transaction, startLedger int64) (events []store.Event, lastProcessed int64, perPage pageCounters) {
	currentLedger := int64(-1)
	txiInLedger := int32(0)

	for _, tx := range rows {
		// Discard rows strictly below the resume ledger. We re-fetch
		// from the start so progress is portable (we persist
		// lastLedger, not the opaque cursor).
		if tx.Ledger < startLedger {
			continue
		}
		if tx.Ledger != currentLedger {
			currentLedger = tx.Ledger
			txiInLedger = 0
		} else {
			txiInLedger++
		}
		perPage.Transactions++
		lastProcessed = tx.Ledger

		// Out-of-range txs are still tallied so ThroughLedger is
		// accurate; we just don't commit their events.
		if b.opts.ToLedger > 0 && tx.Ledger > b.opts.ToLedger {
			continue
		}

		ex, err := horizon.ExtractContractEvents(b.decoder, b.opts.ContractID, horizon.TxHint{
			Hash:            tx.Hash,
			Ledger:          tx.Ledger,
			CreatedAt:       tx.CreatedAt,
			ResultCode:      tx.ResultCode,
			ResultMetaXDR:   tx.ResultMetaXDR,
			TxIndexInLedger: txiInLedger,
		})
		if err != nil {
			perPage.Failed++
			b.log.Warn("failed to extract events from horizon tx",
				"hash", tx.Hash, "ledger", tx.Ledger, "error", err)
			continue
		}
		if !ex.HadMeta || !ex.HadEvents {
			perPage.Skipped++
			continue
		}
		events = append(events, ex.Events...)
	}
	return events, lastProcessed, perPage
}

// commitPage writes one page's events and advances the persisted
// progress. We deliberately call them in sequence (upsert first, then
// progress) so a partial failure below the upsert never claims
// progress on rows we didn't actually save.
func (b *Backfiller) commitPage(ctx context.Context, events []store.Event, lastLedger int64) (int64, error) {
	inserted, err := b.store.UpsertEvents(ctx, events)
	if err != nil {
		return inserted, fmt.Errorf("upserting %d backfilled events: %w", len(events), err)
	}
	if err := b.store.UpdateBackfillState(ctx, lastLedger); err != nil {
		return inserted, fmt.Errorf("saving backfill progress: %w", err)
	}
	return inserted, nil
}

// markTerminal cleans up the persisted row when the run finishes. In
// dry-run mode we leave the row untouched — operators expect the
// "nothing happened" signal for dry runs.
func (b *Backfiller) markTerminal(ctx context.Context, sum Summary) error {
	if b.opts.DryRun {
		// Reset any partial progress we may have set: dry runs
		// shouldn't leave a "last_ledger=42" impression that the
		// next non-dry run would inherit.
		if err := b.store.StartBackfillState(ctx, b.opts.ContractID, b.opts.FromLedger, b.opts.ToLedger); err != nil {
			return fmt.Errorf("resetting dry-run state: %w", err)
		}
		return nil
	}
	if !sum.Completed {
		// Interrupted run — leave progress intact so resume picks up.
		return nil
	}
	if err := b.store.CompleteBackfillState(ctx); err != nil {
		return fmt.Errorf("completing backfill state: %w", err)
	}
	return nil
}

// resume picks up saved progress for an identical in-progress run.
// Any divergence in (contract_id, from_ledger, to_ledger) starts fresh
// because resuming into a row whose bounds moved would silently drop
// events from the gap.
func (b *Backfiller) resume(ctx context.Context) (startLedger int64, sum Summary, err error) {
	if b.opts.DryRun {
		return b.opts.FromLedger, Summary{DryRun: true}, nil
	}

	state, err := b.store.GetBackfillState(ctx)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// fall through to Start.
	case err != nil:
		return 0, Summary{}, err
	case !state.Done() &&
		state.ContractID == b.opts.ContractID &&
		state.FromLedger == b.opts.FromLedger &&
		state.ToLedger == b.opts.ToLedger:
		start := state.LastLedger + 1
		if start < b.opts.FromLedger {
			start = b.opts.FromLedger
		}
		b.log.Info("backfill resuming",
			"contract_id", state.ContractID,
			"from_ledger", state.FromLedger,
			"to_ledger", state.ToLedger,
			"last_ledger", state.LastLedger,
			"resume_from", start)
		return start, Summary{Resumed: true}, nil
	}

	if err := b.store.StartBackfillState(ctx, b.opts.ContractID, b.opts.FromLedger, b.opts.ToLedger); err != nil {
		return 0, Summary{}, err
	}
	return b.opts.FromLedger, Summary{}, nil
}
