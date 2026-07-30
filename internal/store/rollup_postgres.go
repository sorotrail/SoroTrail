package store

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// RollupRebuildAdvisoryLockKey guards the rollup rebuild so it never races
// live ingestion (same pattern as replay's advisory lock).
const RollupRebuildAdvisoryLockKey int64 = 0x536F726F526F6C6C // "SoroRoll"

// RollupRebuildSummary reports the outcome of a rollup rebuild run.
type RollupRebuildSummary struct {
	FromLedger    int64
	ToLedger      int64
	EventsScanned int64
	BucketsFilled int64
	Duration      time.Duration
	Completed     bool
}

// AcquireRollupRebuildLock takes the advisory lock without blocking,
// returning an error if another rebuild holds it.
func (p *Postgres) AcquireRollupRebuildLock(ctx context.Context) (ReplayLock, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for rollup rebuild lock: %w", err)
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, RollupRebuildAdvisoryLockKey).Scan(&got); err != nil {
		conn.Release()
		return nil, fmt.Errorf("taking rollup rebuild advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, errors.New("another rollup rebuild is already running against this database")
	}
	return &pgReplayLock{conn: conn}, nil
}

// RebuildRollups reconstructs rollup_events and rollup_token_volume from
// raw events in [fromLedger, toLedger] (0 toLedger = no upper bound). It
// deletes existing rollup data for the ledger range and rebuilds it from
// the events table so it is correct after backfills (#26) or replays (#21).
//
// Runs safely against a live database: it holds an advisory lock that
// prevents two concurrent rebuilds, but live ingestion keeps updating
// rollups via UpsertEvents for ledgers outside the rebuild range.
func (p *Postgres) RebuildRollups(ctx context.Context, fromLedger int64, toLedger int64) (RollupRebuildSummary, error) {
	start := time.Now()
	sum := RollupRebuildSummary{FromLedger: fromLedger, ToLedger: toLedger}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return sum, fmt.Errorf("begin rebuild tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Delete rollup rows overlapping the rebuild range so we rebuild from
	// a clean slate. bucket_start derives from ledger, so approximate the
	// bucket range from the ledger range.
	bucketFrom := bucketHour(fromLedger)
	whereClause := "WHERE bucket_start >= $1"
	args := []any{bucketFrom}
	if toLedger > 0 {
		bucketTo := bucketHour(toLedger).Add(time.Hour) // inclusive → exclusive
		whereClause += " AND bucket_start < $2"
		args = append(args, bucketTo)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM rollup_events `+whereClause, args...); err != nil {
		return sum, fmt.Errorf("deleting rollup_events for rebuild: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM rollup_token_volume `+whereClause, args...); err != nil {
		return sum, fmt.Errorf("deleting rollup_token_volume for rebuild: %w", err)
	}

	// Walk events in batches and accumulate rollup rows in memory.
	const batchSize = 5000
	var (
		afterID       string
		eventRollups  = make(map[bucketKey]int64)
		volumeRollups = make(map[bucketKey]*volDelta)
	)

	for {
		events, err := p.nextRollupBatch(ctx, fromLedger, toLedger, afterID, batchSize)
		if err != nil {
			return sum, err
		}
		if len(events) == 0 {
			break
		}

		for _, e := range events {
			sum.EventsScanned++
			key := bucketKey{
				bucketStart: bucketHour(e.Ledger),
				contractID:  e.ContractID,
				eventType:   e.Type,
			}
			eventRollups[key]++

			if amt, addrs, ok := tryExtractTransfer(e.Topics, e.Value); ok {
				vd, exists := volumeRollups[key]
				if !exists {
					vd = &volDelta{volume: new(big.Int), addresses: make(map[string]struct{})}
					volumeRollups[key] = vd
				}
				vd.volume.Add(vd.volume, amt)
				for _, a := range addrs {
					vd.addresses[a] = struct{}{}
				}
			}
			afterID = e.ID
		}

		// Flush accumulated rollups to the transaction when the map grows.
		if len(eventRollups) >= batchSize {
			if err := applyEventRollups(ctx, tx, eventRollups); err != nil {
				return sum, err
			}
			sum.BucketsFilled += int64(len(eventRollups))
			eventRollups = make(map[bucketKey]int64)

			if err := applyVolumeRollups(ctx, tx, volumeRollups); err != nil {
				return sum, err
			}
			volumeRollups = make(map[bucketKey]*volDelta)
		}
	}

	// Final flush.
	if len(eventRollups) > 0 {
		if err := applyEventRollups(ctx, tx, eventRollups); err != nil {
			return sum, err
		}
		sum.BucketsFilled += int64(len(eventRollups))
	}
	if len(volumeRollups) > 0 {
		if err := applyVolumeRollups(ctx, tx, volumeRollups); err != nil {
			return sum, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return sum, fmt.Errorf("committing rebuild: %w", err)
	}

	sum.Duration = time.Since(start)
	sum.Completed = true
	return sum, nil
}

// nextRollupBatch reads a batch of events for rollup rebuild, returning
// only the columns needed for rollup computation.
func (p *Postgres) nextRollupBatch(ctx context.Context, fromLedger, toLedger int64, afterID string, limit int) ([]Event, error) {
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	where = append(where, "ledger >= "+arg(fromLedger))
	if toLedger > 0 {
		where = append(where, "ledger <= "+arg(toLedger))
	}
	if afterID != "" {
		where = append(where, "id > "+arg(afterID))
	}

	query := fmt.Sprintf(`
		SELECT id, contract_id, ledger, type, topics, value
		FROM events
		WHERE %s
		ORDER BY id ASC
		LIMIT %s`, strings.Join(where, " AND "), arg(limit))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("reading rollup rebuild batch: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.Topics, &e.Value); err != nil {
			return nil, fmt.Errorf("scanning rollup rebuild batch: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
