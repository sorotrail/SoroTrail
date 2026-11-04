// Package store: BackfillState persistence. A single-row owner table
// holds the persisted progress of the most recent backfill command.
//
// We deliberately follow the replay_state pattern: one row, id=1, with
// a partial contract identifier so the operator can re-run with
// different contracts without losing the previous run's audit trail
// of where it stopped. A new contract ID overwrites the row.
//
// Resume semantics: the operator passes --from-ledger/--to-ledger again,
// and the Backfiller detects a matching in-progress row and picks up at
// last_ledger + 1. Anything outside the resumed range is left alone.
// This keeps resume portable across Horizon URLs (we never persist the
// opaque Horizon cursor) at the cost of re-fetching a partial page on
// resume — idempotent upserts make the overlap harmless.

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// BackfillRangeStatus describes the lifecycle of a single ledger sub-range
// during a parallel backfill run (#1237).
type BackfillRangeStatus string

const (
	BackfillRangePending   BackfillRangeStatus = "pending"
	BackfillRangeInFlight  BackfillRangeStatus = "in_flight"
	BackfillRangeCompleted BackfillRangeStatus = "completed"
	BackfillRangeFailed    BackfillRangeStatus = "failed"
)

// BackfillRange tracks persisted progress for one contiguous ledger sub-range
// [FromLedger, ToLedger] within a backfill run.
type BackfillRange struct {
	FromLedger int64               `json:"from_ledger"`
	ToLedger   int64               `json:"to_ledger"`
	LastLedger int64               `json:"last_ledger"`
	Status     BackfillRangeStatus `json:"status"`
	Error      string              `json:"error,omitempty"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

// Completed reports whether this sub-range finished cleanly.
func (r BackfillRange) Completed() bool {
	return r.Status == BackfillRangeCompleted
}

// ResumeStartLedger returns the inclusive ledger where a resumed worker should
// pick up this sub-range.
func (r BackfillRange) ResumeStartLedger() int64 {
	if r.LastLedger >= r.FromLedger {
		return r.LastLedger + 1
	}
	return r.FromLedger
}

// BackfillState is the single persisted progress row for the backfill
// command. LastLedger is the inclusive highest ledger whose events have
// been committed to the events table; resume picks up at LastLedger+1
// when Ranges is empty, or resumes incomplete entries in Ranges when
// range-level state has been recorded.
type BackfillState struct {
	ContractID  string
	FromLedger  int64
	ToLedger    int64
	LastLedger  int64
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
	Ranges      []BackfillRange
}

// ContiguousWatermark computes the highest contiguous ledger committed from
// fromLedger across sorted sub-ranges. It stops advancing as soon as it
// encounters an incomplete sub-range so single-cursor readers never skip an
// unfinished gap.
func ContiguousWatermark(fromLedger int64, ranges []BackfillRange) int64 {
	var watermark int64
	expectedStart := fromLedger
	for _, r := range ranges {
		if r.FromLedger > expectedStart {
			break
		}
		if r.Status == BackfillRangeCompleted {
			if r.ToLedger > watermark {
				watermark = r.ToLedger
			}
			expectedStart = r.ToLedger + 1
			continue
		}
		if r.LastLedger >= r.FromLedger && r.LastLedger > watermark {
			watermark = r.LastLedger
		}
		break
	}
	return watermark
}

// Done reports whether the recorded run covered the whole range it was
// started for. When true, the row is preserved for audit but a fresh
// run with the same params will start over from FromLedger.
func (s BackfillState) Done() bool { return s.CompletedAt != nil }

// ErrBackfillInProgress is returned when GetBackfillState finds an
// unfinished row whose contract/range doesn't match what the new run
// is configured to do. Operators must either restart with --restart or
// wait for the original run to complete; calling helpers can override
// by calling StartBackfillState explicitly.
var ErrBackfillInProgress = errors.New("backfill state already exists for a different range")

// GetBackfillState returns the persisted backfill progress, or
// ErrNotFound when no run has ever started. The id=1 singleton means
// every operator shares the same row; the contract_id inside the row
// gates against accidental cross-contract resumes in run()'s resume step.
func (p *Postgres) GetBackfillState(ctx context.Context) (BackfillState, error) {
	var s BackfillState
	var completedAt *time.Time
	var rangesJSON []byte
	err := p.pool.QueryRow(ctx, `
		SELECT contract_id, from_ledger, to_ledger, last_ledger,
		       started_at, updated_at, completed_at,
		       COALESCE(ranges_json, '[]'::jsonb)
		FROM backfill_state
		WHERE id = 1`,
	).Scan(&s.ContractID, &s.FromLedger, &s.ToLedger, &s.LastLedger,
		&s.StartedAt, &s.UpdatedAt, &completedAt, &rangesJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return BackfillState{}, ErrNotFound
	}
	if err != nil {
		return BackfillState{}, err
	}
	s.CompletedAt = completedAt
	if len(rangesJSON) > 0 {
		if err := json.Unmarshal(rangesJSON, &s.Ranges); err != nil {
			return BackfillState{}, fmt.Errorf("decoding backfill ranges_json: %w", err)
		}
	}
	return s, nil
}

// StartBackfillState (re)initializes the progress row for a fresh run
// over [fromLedger, toLedger] for contractID, discarding whatever the
// previous run had recorded. Using UPSERT on the singleton id=1 keeps a
// single source of truth across re-runs.
func (p *Postgres) StartBackfillState(ctx context.Context, contractID string, fromLedger, toLedger int64) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO backfill_state
			(id, contract_id, from_ledger, to_ledger, last_ledger,
			 started_at, updated_at, completed_at, ranges_json)
		VALUES (1, $1, $2, $3, 0, now(), now(), NULL, '[]'::jsonb)
		ON CONFLICT (id) DO UPDATE SET
			contract_id  = EXCLUDED.contract_id,
			from_ledger  = EXCLUDED.from_ledger,
			to_ledger    = EXCLUDED.to_ledger,
			last_ledger  = 0,
			started_at   = now(),
			updated_at   = now(),
			completed_at = NULL,
			ranges_json  = '[]'::jsonb`,
		contractID, fromLedger, toLedger)
	if err != nil {
		return err
	}
	return nil
}

// SaveBackfillRanges persists the per-range progress snapshot and advances
// last_ledger monotonically to the contiguous committed watermark so single-row
// and range-aware readers remain consistent.
func (p *Postgres) SaveBackfillRanges(ctx context.Context, ranges []BackfillRange) error {
	payload := []byte("[]")
	if len(ranges) > 0 {
		encoded, err := json.Marshal(ranges)
		if err != nil {
			return fmt.Errorf("encoding backfill ranges: %w", err)
		}
		payload = encoded
	}
	var fromLedger int64
	if len(ranges) > 0 {
		fromLedger = ranges[0].FromLedger
	}
	watermark := ContiguousWatermark(fromLedger, ranges)
	_, err := p.pool.Exec(ctx, `
		UPDATE backfill_state SET
			ranges_json = $1::jsonb,
			last_ledger = GREATEST(last_ledger, $2),
			updated_at  = now()
		WHERE id = 1`,
		string(payload), watermark)
	return err
}

// UpdateBackfillState advances the persisted progress to lastLedger. It
// is called once per Horizon page whose events were successfully upserted
// — the batch commits the events, then this call commits the cursor.
//
// We intentionally use a monotonic advance: `last_ledger = GREATEST(last_ledger, $1)`. A
// race between two operators re-running the same backfill would otherwise
// regress the row, and a partial page that committed events but failed
// to update last_ledger is safe to replay (idempotent upserts).
func (p *Postgres) UpdateBackfillState(ctx context.Context, lastLedger int64) error {
	if lastLedger <= 0 {
		return nil // never regress to 0 just because the first page was empty
	}
	_, err := p.pool.Exec(ctx, `
		UPDATE backfill_state SET
			last_ledger = GREATEST(last_ledger, $1),
			updated_at  = now()
		WHERE id = 1`,
		lastLedger)
	if err != nil {
		return err
	}
	return nil
}

// CompleteBackfillState marks the row as finished — set completed_at and
// leave last_ledger as the highest ledger we processed so operators can
// still see where the run ended.
func (p *Postgres) CompleteBackfillState(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE backfill_state SET
			completed_at = now(),
			updated_at   = now()
		WHERE id = 1`)
	if err != nil {
		return err
	}
	return nil
}
