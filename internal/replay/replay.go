// Package replay re-runs the current decoder pipeline over stored raw XDR
// and rewrites the decoded columns.
//
// Decoders improve over time: new ScVal types get handled, per-standard
// decoders gain edge cases, new normalized tables appear. Without replay
// those improvements only ever apply to events ingested after the change.
// Replay applies them to everything already stored — which is what keeps
// years of indexed data trustworthy.
//
// Design notes that matter when extending this:
//
//   - Replay is a pure function of the raw XDR. Nothing here reads the
//     existing decoded columns except to decide whether a row changed.
//   - Progress is persisted with each batch, in the batch's own transaction
//     (see store.CommitReplayBatch), so an interrupted run resumes exactly
//     where it committed and a re-run over the same range is a no-op.
//   - Batch transactions stay short and touch only their own rows, so a
//     replay can run against a live database without stalling ingestion.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// DefaultBatchSize is the number of events re-decoded per transaction when
// Options.BatchSize is unset. Large enough to amortize round trips, small
// enough that each transaction is short-lived.
const DefaultBatchSize = 500

// Store is the persistence surface replay needs. *store.Postgres implements
// it; it is kept separate from store.Store so backends that don't need a
// maintenance replay tool aren't forced to implement one.
type Store interface {
	// AcquireReplayLock takes the single-replay lock without blocking,
	// returning store.ErrReplayLocked if another replay holds it.
	AcquireReplayLock(ctx context.Context) (store.ReplayLock, error)
	GetReplayState(ctx context.Context) (store.ReplayState, error)
	StartReplayState(ctx context.Context, fromLedger, toLedger int64) error
	NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]store.DecodedEvent, error)
	CommitReplayBatch(ctx context.Context, b store.ReplayBatch) error
}

// Options configure a Replayer.
type Options struct {
	// FromLedger and ToLedger bound the replay inclusively. ToLedger of 0
	// means "no upper bound".
	FromLedger int64
	ToLedger   int64
	// BatchSize is the number of events re-decoded per transaction.
	// Defaults to DefaultBatchSize.
	BatchSize int
	// Restart discards any saved progress for this range and starts over.
	Restart bool
	// DryRun decodes and reports without writing anything — no rewrites and
	// no progress, so it is safe to run at any time.
	DryRun bool
}

func (o *Options) applyDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.ToLedger <= 0 {
		o.ToLedger = maxLedger
	}
}

// maxLedger stands in for "no upper bound"; ledger sequences are uint32 on
// the wire, so this is comfortably past any real ledger.
const maxLedger int64 = 1 << 40

// Summary reports what a run did.
type Summary struct {
	// Processed is every row read in the range, including skipped ones.
	Processed int64
	// Changed is rows whose re-decoded columns differed and were rewritten.
	Changed int64
	// Skipped is rows with no raw XDR to replay — ingested before raw XDR
	// was stored, or delivered as JSON by the RPC. Expected, not an error.
	Skipped int64
	// Failed is rows whose stored XDR could not be decoded. Logged and
	// counted rather than aborting the run, so one malformed row can't wedge
	// a replay forever; a non-zero value deserves investigation.
	Failed int64
	// Completed is false when the run stopped early (Ctrl-C); re-running
	// resumes from the last committed batch.
	Completed bool
	// Resumed is true when the run continued a previously interrupted one.
	Resumed  bool
	Duration time.Duration
}

// Replayer re-decodes stored events in batches.
type Replayer struct {
	store   Store
	decoder decode.Decoder
	log     *slog.Logger
	opts    Options
}

// New wires a Replayer. dec is the decoder whose output becomes the new
// stored decoding — in production, the same decoder the ingester uses.
func New(st Store, dec decode.Decoder, log *slog.Logger, opts Options) *Replayer {
	opts.applyDefaults()
	return &Replayer{store: st, decoder: dec, log: log, opts: opts}
}

// Run replays the configured ledger range.
//
// Concurrency: the run holds a session-level advisory lock for its whole
// duration (see store.ReplayAdvisoryLockKey) and returns
// store.ErrReplayLocked immediately if another replay is already running.
// Two replays rewriting the same rows from the same raw XDR would be
// harmless in outcome but would corrupt the shared progress row, so the
// guard is on the run, not on the rows.
//
// Interruption: cancelling ctx (Ctrl-C) stops between batches and returns
// the summary so far with Completed false. The in-flight batch, if any, is
// rolled back with its progress marker.
func (r *Replayer) Run(ctx context.Context) (Summary, error) {
	started := time.Now()

	lock, err := r.store.AcquireReplayLock(ctx)
	if err != nil {
		return Summary{}, err
	}
	defer lock.Release()

	cursor, sum, err := r.resume(ctx)
	if err != nil {
		return Summary{}, err
	}

	r.log.Info("replay starting",
		"from_ledger", r.opts.FromLedger, "to_ledger", r.opts.ToLedger,
		"batch_size", r.opts.BatchSize, "dry_run", r.opts.DryRun,
		"resumed", sum.Resumed, "after_event_id", cursor)

	for {
		if err := ctx.Err(); err != nil {
			sum.Duration = time.Since(started)
			return sum, nil // interrupted: report progress, not an error
		}

		events, err := r.store.NextReplayBatch(ctx, r.opts.FromLedger, r.opts.ToLedger, cursor, r.opts.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				sum.Duration = time.Since(started)
				return sum, nil
			}
			return sum, err
		}
		if len(events) == 0 {
			sum.Completed = true
			break
		}

		rewrites := r.decodeBatch(events, &sum)
		cursor = events[len(events)-1].ID

		if !r.opts.DryRun {
			if err := r.commit(ctx, rewrites, cursor, sum, false); err != nil {
				if ctx.Err() != nil {
					// The batch rolled back; the previous cursor stands.
					sum.Duration = time.Since(started)
					return sum, nil
				}
				return sum, err
			}
		}
		r.log.Debug("replay batch done",
			"through_event_id", cursor, "processed", sum.Processed, "changed", sum.Changed)
	}

	if !r.opts.DryRun {
		// Final marker: same cursor, now flagged complete.
		if err := r.commit(ctx, nil, cursor, sum, true); err != nil {
			return sum, err
		}
	}
	sum.Duration = time.Since(started)
	return sum, nil
}

// resume decides where this run starts. Saved progress is only picked up for
// an identical, unfinished range — a different range or an explicit
// --restart begins from scratch, because resuming into a range whose bounds
// moved would silently skip rows.
func (r *Replayer) resume(ctx context.Context) (cursor string, sum Summary, err error) {
	if r.opts.DryRun {
		return "", Summary{}, nil // dry runs neither read nor write progress
	}

	state, err := r.store.GetReplayState(ctx)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return "", Summary{}, err
	case !r.opts.Restart && !state.Done() &&
		state.FromLedger == r.opts.FromLedger && state.ToLedger == r.opts.ToLedger:
		return state.LastEventID, Summary{
			Processed: state.Processed,
			Changed:   state.Changed,
			Skipped:   state.Skipped,
			Resumed:   true,
		}, nil
	}

	if err := r.store.StartReplayState(ctx, r.opts.FromLedger, r.opts.ToLedger); err != nil {
		return "", Summary{}, err
	}
	return "", Summary{}, nil
}

// decodeBatch re-decodes one batch and returns only the rows whose decoding
// actually changed, so unchanged rows cost no writes on a live database.
// Counters on sum are advanced for every row read.
func (r *Replayer) decodeBatch(events []store.DecodedEvent, sum *Summary) []store.EventDecoding {
	rewrites := make([]store.EventDecoding, 0, len(events))
	for _, e := range events {
		sum.Processed++

		if !e.HasRawXDR() {
			sum.Skipped++
			continue
		}
		topics, value, err := r.decode(e)
		if err != nil {
			sum.Failed++
			r.log.Warn("replay could not decode stored XDR; leaving row untouched",
				"event_id", e.ID, "contract_id", e.ContractID, "error", err)
			continue
		}
		if jsonEqual(topics, e.Topics) && jsonEqual(value, e.Value) {
			continue
		}
		sum.Changed++
		rewrites = append(rewrites, store.EventDecoding{ID: e.ID, Topics: topics, Value: value})
	}
	return rewrites
}

// decode runs the same pipeline the ingester uses, so replayed rows are
// byte-identical to freshly ingested ones rather than merely similar.
func (r *Replayer) decode(e store.DecodedEvent) (topics, value json.RawMessage, err error) {
	return decode.EventTopicsValue(r.decoder, rpc.Event{
		ID:    e.ID,
		Topic: e.RawTopicXDR,
		Value: e.RawValueXDR,
	})
}

func (r *Replayer) commit(ctx context.Context, rewrites []store.EventDecoding, cursor string, sum Summary, done bool) error {
	state := store.ReplayState{
		FromLedger:  r.opts.FromLedger,
		ToLedger:    r.opts.ToLedger,
		LastEventID: cursor,
		Processed:   sum.Processed,
		Changed:     sum.Changed,
		Skipped:     sum.Skipped,
	}
	if done {
		now := time.Now().UTC()
		state.CompletedAt = &now
	}
	// contributors: dependent tables (e.g. token_events from a SEP-41
	// decoder) are derived from `rewrites` and added to this batch — see the
	// ordering contract on store.ReplayBatch.
	return r.store.CommitReplayBatch(ctx, store.ReplayBatch{Events: rewrites, State: state})
}

// String renders the summary line the CLI prints.
func (s Summary) String() string {
	return fmt.Sprintf(
		"processed=%d changed=%d skipped=%d failed=%d completed=%t duration=%s",
		s.Processed, s.Changed, s.Skipped, s.Failed, s.Completed, s.Duration.Round(time.Millisecond))
}

// jsonEqual compares two JSON documents semantically. Byte comparison won't
// do: Postgres normalizes jsonb (key order, whitespace, number formatting),
// so a freshly marshaled document rarely matches the stored bytes even when
// nothing changed — and a spurious "changed" would rewrite every row on
// every replay.
func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
