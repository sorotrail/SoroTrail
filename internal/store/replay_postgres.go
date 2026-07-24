package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReplayAdvisoryLockKey is the session-level advisory lock guarding replay.
//
// Why an advisory lock rather than a row lock or a status column: replay
// rewrites rows all over the events table in many short transactions, so
// there is no single row to lock for the duration, and a status column would
// leak a "running" state forever if the process were killed. A *session*
// advisory lock is held by a connection, so Postgres releases it
// automatically the moment that connection dies — kill -9 the replay and the
// next one starts cleanly, with no stale-lock cleanup path to get wrong.
//
// The key is the ASCII bytes of "SoroRepl", giving a value that is stable
// across deployments and unlikely to collide with another application's
// advisory locks on a shared database.
const ReplayAdvisoryLockKey int64 = 0x536F726F5265706C // "SoroRepl"

// ErrReplayLocked is returned when another replay already holds the advisory
// lock.
var ErrReplayLocked = errors.New("another replay is already running")

// ReplayLock is a held replay lock. It is an interface so the replay engine
// can be tested against a fake store.
type ReplayLock interface {
	// Release drops the lock. It is safe to call more than once.
	Release()
}

// pgReplayLock holds the advisory lock by holding its session's connection.
type pgReplayLock struct {
	conn *pgxpool.Conn
}

func (l *pgReplayLock) Release() {
	if l.conn != nil {
		// Releasing the connection ends the session that holds the lock.
		// pg_advisory_unlock would work too, but returning the connection is
		// what actually has to happen, and it is unconditional.
		l.conn.Release()
		l.conn = nil
	}
}

// AcquireReplayLock takes the replay advisory lock without blocking,
// returning ErrReplayLocked if another replay holds it. The lock lives on a
// dedicated pooled connection that the caller must Release; ingestion and
// API queries continue to use the rest of the pool untouched.
func (p *Postgres) AcquireReplayLock(ctx context.Context) (ReplayLock, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for replay lock: %w", err)
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, ReplayAdvisoryLockKey).Scan(&got); err != nil {
		conn.Release()
		return nil, fmt.Errorf("taking replay advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, ErrReplayLocked
	}
	return &pgReplayLock{conn: conn}, nil
}

// GetReplayState returns the persisted replay progress, or ErrNotFound when
// no replay has ever run.
func (p *Postgres) GetReplayState(ctx context.Context) (ReplayState, error) {
	var s ReplayState
	err := p.pool.QueryRow(ctx, `
		SELECT from_ledger, to_ledger, last_event_id, processed, changed,
		       skipped, started_at, updated_at, completed_at
		FROM replay_state WHERE id = 1`,
	).Scan(&s.FromLedger, &s.ToLedger, &s.LastEventID, &s.Processed, &s.Changed,
		&s.Skipped, &s.StartedAt, &s.UpdatedAt, &s.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplayState{}, ErrNotFound
	}
	if err != nil {
		return ReplayState{}, fmt.Errorf("loading replay state: %w", err)
	}
	return s, nil
}

// StartReplayState (re)initializes the progress row for a fresh run over
// [fromLedger, toLedger], discarding any previous run's progress.
func (p *Postgres) StartReplayState(ctx context.Context, fromLedger, toLedger int64) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO replay_state
			(id, from_ledger, to_ledger, last_event_id, processed, changed,
			 skipped, started_at, updated_at, completed_at)
		VALUES (1, $1, $2, '', 0, 0, 0, now(), now(), NULL)
		ON CONFLICT (id) DO UPDATE SET
			from_ledger   = EXCLUDED.from_ledger,
			to_ledger     = EXCLUDED.to_ledger,
			last_event_id = '',
			processed     = 0,
			changed       = 0,
			skipped       = 0,
			started_at    = now(),
			updated_at    = now(),
			completed_at  = NULL`,
		fromLedger, toLedger)
	if err != nil {
		return fmt.Errorf("starting replay state: %w", err)
	}
	return nil
}

// NextReplayBatch returns up to limit events from [fromLedger, toLedger]
// ordered by ID, starting after afterID. Events are returned whether or not
// they carry raw XDR — the caller counts the ones it has to skip.
func (p *Postgres) NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]DecodedEvent, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, contract_id, ledger, topics_xdr, value_xdr, topics, value
		FROM events
		WHERE ledger >= $1 AND ledger <= $2 AND id > $3
		ORDER BY id ASC
		LIMIT $4`,
		fromLedger, toLedger, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("reading replay batch: %w", err)
	}
	defer rows.Close()

	batch := make([]DecodedEvent, 0, limit)
	for rows.Next() {
		var (
			d         DecodedEvent
			rawTopics []string
			rawValue  *string
		)
		if err := rows.Scan(&d.ID, &d.ContractID, &d.Ledger, &rawTopics,
			&rawValue, &d.Topics, &d.Value); err != nil {
			return nil, fmt.Errorf("scanning replay batch: %w", err)
		}
		d.RawTopicXDR = rawTopics
		if rawValue != nil {
			d.RawValueXDR = *rawValue
		}
		batch = append(batch, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading replay batch: %w", err)
	}
	return batch, nil
}

// CommitReplayBatch applies one batch's rewrites and its progress marker in a
// single short transaction.
//
// Write order is the derivation order and is deliberately explicit here:
//
//  1. events — the canonical decoded columns every dependent table reads.
//  2. (dependent tables, as they land — e.g. token_events from the SEP-41
//     decoder — go here, after events, each deleting the batch's rows before
//     re-inserting so a shrinking derivation removes stale rows.)
//  3. replay_state — last, so committed progress never runs ahead of
//     committed rewrites. An interrupt between batches resumes at the last
//     committed event; an interrupt mid-batch rolls the whole batch back.
//
// The transaction touches only the batch's own rows and holds no table-level
// locks, so concurrent ingestion is never blocked for longer than one batch.
func (p *Postgres) CommitReplayBatch(ctx context.Context, b ReplayBatch) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning replay batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if len(b.Events) > 0 {
		batch := &pgx.Batch{}
		for _, e := range b.Events {
			batch.Queue(
				`UPDATE events SET topics = $2, value = $3 WHERE id = $1`,
				e.ID, e.Topics, e.Value)
		}
		results := tx.SendBatch(ctx, batch)
		for range b.Events {
			if _, err := results.Exec(); err != nil {
				results.Close()
				return fmt.Errorf("rewriting decoded events: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("rewriting decoded events: %w", err)
		}
	}

	s := b.State
	_, err = tx.Exec(ctx, `
		UPDATE replay_state SET
			last_event_id = $1,
			processed     = $2,
			changed       = $3,
			skipped       = $4,
			updated_at    = now(),
			completed_at  = $5
		WHERE id = 1`,
		s.LastEventID, s.Processed, s.Changed, s.Skipped, s.CompletedAt)
	if err != nil {
		return fmt.Errorf("saving replay state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing replay batch: %w", err)
	}
	return nil
}
