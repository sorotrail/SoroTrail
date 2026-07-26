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
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// BackfillState is the single persisted progress row for the backfill
// command. LastLedger is the inclusive highest ledger whose events have
// been committed to the events table; resume picks up at LastLedger+1.
type BackfillState struct {
	ContractID  string
	FromLedger  int64
	ToLedger    int64
	LastLedger  int64
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
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
	err := p.pool.QueryRow(ctx, `
		SELECT contract_id, from_ledger, to_ledger, last_ledger,
		       started_at, updated_at, completed_at
		FROM backfill_state
		WHERE id = 1`,
	).Scan(&s.ContractID, &s.FromLedger, &s.ToLedger, &s.LastLedger,
		&s.StartedAt, &s.UpdatedAt, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return BackfillState{}, ErrNotFound
	}
	if err != nil {
		return BackfillState{}, err
	}
	s.CompletedAt = completedAt
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
			 started_at, updated_at, completed_at)
		VALUES (1, $1, $2, $3, 0, now(), now(), NULL)
		ON CONFLICT (id) DO UPDATE SET
			contract_id  = EXCLUDED.contract_id,
			from_ledger  = EXCLUDED.from_ledger,
			to_ledger    = EXCLUDED.to_ledger,
			last_ledger  = 0,
			started_at   = now(),
			updated_at   = now(),
			completed_at = NULL`,
		contractID, fromLedger, toLedger)
	if err != nil {
		return err
	}
	return nil
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
