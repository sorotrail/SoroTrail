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
	AcquireReplayLock(ctx context.Context) (store.ReplayLock, error)
	GetReplayState(ctx context.Context) (store.ReplayState, error)
	StartReplayState(ctx context.Context, fromLedger, toLedger int64) error
	NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]store.DecodedEvent, error)
	CommitReplayBatch(ctx context.Context, b store.ReplayBatch) error
}

// Options configure a Replayer.
type Options struct {
	FromLedger           int64
	ToLedger             int64
	BatchSize            int
	Restart              bool
	DryRun               bool
	TargetDecoderVersion int
	Progress             func(processed int64)
}

func (o *Options) applyDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.ToLedger <= 0 {
		o.ToLedger = maxLedger
	}
}

const maxLedger int64 = 1 << 40

type Summary struct {
	Processed int64
	Changed   int64
	Skipped   int64
	Failed    int64
	Completed bool
	Resumed   bool
	Duration  time.Duration
}

type Replayer struct {
	store   Store
	decoder decode.Decoder
	log     *slog.Logger
	opts    Options
}

func New(st Store, dec decode.Decoder, log *slog.Logger, opts Options) *Replayer {
	opts.applyDefaults()
	return &Replayer{store: st, decoder: dec, log: log, opts: opts}
}

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
		"target_decoder_version", r.opts.TargetDecoderVersion,
		"resumed", sum.Resumed, "after_event_id", cursor)

	for {
		if err := ctx.Err(); err != nil {
			sum.Duration = time.Since(started)
			return sum, nil
		}

		events, err := r.store.NextReplayBatch(ctx, r.opts.FromLedger, r.opts.ToLedger, cursor, r.opts.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				sum.Duration = time.Since(started)
				return sum, nil
			}
			return sum, err
		}
		found := len(events)
		if found == 0 {
			sum.Completed = true
			break
		}

		// Filter by target decoder version if requested
		filteredEvents := make([]store.DecodedEvent, 0, found)
		for _, e := range events {
			if r.opts.TargetDecoderVersion > 0 && e.DecoderVersion >= int32(r.opts.TargetDecoderVersion) {
				sum.Skipped++
				continue
			}
			filteredEvents = append(filteredEvents, e)
		}

		cursor = events[found-1].ID

		rewrites := r.decodeBatch(filteredEvents, &sum)

		if !r.opts.DryRun {
			if err := r.commit(ctx, rewrites, cursor, sum, false); err != nil {
				if ctx.Err() != nil {
					sum.Duration = time.Since(started)
					return sum, nil
				}
				return sum, err
			}
		}
		if r.opts.Progress != nil {
			r.opts.Progress(int64(found))
		}
		r.log.Debug("replay batch done",
			"through_event_id", cursor, "processed", sum.Processed, "changed", sum.Changed)
	}

	if !r.opts.DryRun {
		if err := r.commit(ctx, nil, cursor, sum, true); err != nil {
			return sum, err
		}
	}
	sum.Duration = time.Since(started)
	return sum, nil
}

func (r *Replayer) resume(ctx context.Context) (cursor string, sum Summary, err error) {
	if r.opts.DryRun {
		return "", Summary{}, nil
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

func (r *Replayer) decode(e store.DecodedEvent) (topics, value json.RawMessage, err error) {
	return decode.EventTopicsValue(r.decoder, rpc.Event{
		ID:    e.ID,
		Topic: e.RawTopicXDR,
		Value: e.RawValueXDR,
	})
}

func (r *Replayer) commit(ctx context.Context, rewrites []store.EventDecoding, cursor string, sum Summary, done bool) error {
	return r.commitVersion(ctx, rewrites, cursor, sum, done, int32(decode.CurrentDecoderVersion))
}

func (r *Replayer) commitVersion(ctx context.Context, rewrites []store.EventDecoding, cursor string, sum Summary, done bool, decoderVersion int32) error {
	state := store.ReplayState{
		FromLedger:     r.opts.FromLedger,
		ToLedger:       r.opts.ToLedger,
		LastEventID:    cursor,
		Processed:      sum.Processed,
		Changed:        sum.Changed,
		Skipped:        sum.Skipped,
		DecoderVersion: decoderVersion,
	}
	if done {
		now := time.Now().UTC()
		state.CompletedAt = &now
	}
	return r.store.CommitReplayBatch(ctx, store.ReplayBatch{Events: rewrites, State: state, DecoderVersion: decoderVersion})
}

func (s Summary) String() string {
	return fmt.Sprintf(
		"processed=%d changed=%d skipped=%d failed=%d completed=%t duration=%s",
		s.Processed, s.Changed, s.Skipped, s.Failed, s.Completed, s.Duration.Round(time.Millisecond))
}

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
