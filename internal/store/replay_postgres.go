package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReplayAdvisoryLockKey is the session-level advisory lock guarding replay.
const ReplayAdvisoryLockKey int64 = 0x536F726F5265706C // "SoroRepl"

// ErrReplayLocked is returned when another replay already holds the advisory lock.
var ErrReplayLocked = errors.New("another replay is already running")

// ReplayLock is a held replay lock.
type ReplayLock interface {
	Release()
}

// pgReplayLock holds the advisory lock by holding its session's connection.
type pgReplayLock struct {
	conn *pgxpool.Conn
}

func (l *pgReplayLock) Release() {
	if l.conn != nil {
		l.conn.Release()
		l.conn = nil
	}
}

// AcquireReplayLock takes the replay advisory lock without blocking.
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

// StartReplayState (re)initializes the progress row for a fresh run.
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

// NextReplayBatch returns up to limit events from [fromLedger, toLedger].
func (p *Postgres) NextReplayBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]DecodedEvent, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, contract_id, ledger, network, raw_topic_xdr, raw_value_xdr, topics, value
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
		if err := rows.Scan(&d.ID, &d.ContractID, &d.Ledger, &d.Network, &rawTopics,
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

// CommitReplayBatch applies one batch's rewrites and its progress marker.
func (p *Postgres) CommitReplayBatch(ctx context.Context, b ReplayBatch) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin replay tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if len(b.Events) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE events SET topics = data.topics, value = data.value
			FROM (SELECT unnest($1::text[]) AS id,
			             unnest($2::jsonb[]) AS topics,
			             unnest($3::jsonb[]) AS value) AS data
			WHERE events.id = data.id`,
			eventIDs(b.Events), eventTopics(b.Events), eventValues(b.Events),
		); err != nil {
			return fmt.Errorf("updating replay events: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE replay_state SET
			last_event_id = $1,
			processed     = processed + $2,
			changed       = changed + $3,
			skipped       = skipped + $4,
			updated_at    = now()
		WHERE id = 1`,
		b.State.LastEventID, b.State.Processed, b.State.Changed, b.State.Skipped)
	if err != nil {
		return fmt.Errorf("updating replay state: %w", err)
	}

	if b.State.Done() {
		_, err = tx.Exec(ctx, `
			UPDATE replay_state SET completed_at = now() WHERE id = 1`)
		if err != nil {
			return fmt.Errorf("marking replay complete: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// eventIDs extracts event IDs from a slice of EventDecoding.
func eventIDs(events []EventDecoding) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

// eventTopics extracts topics from a slice of EventDecoding.
func eventTopics(events []EventDecoding) []json.RawMessage {
	out := make([]json.RawMessage, len(events))
	for i, e := range events {
		out[i] = e.Topics
	}
	return out
}

// eventValues extracts values from a slice of EventDecoding.
func eventValues(events []EventDecoding) []json.RawMessage {
	out := make([]json.RawMessage, len(events))
	for i, e := range events {
		out[i] = e.Value
	}
	return out
}
