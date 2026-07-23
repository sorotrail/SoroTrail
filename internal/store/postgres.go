package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a lookup matches no rows.
var ErrNotFound = errors.New("not found")

// DefaultQueryLimit applies when EventFilter.Limit is unset; MaxQueryLimit
// caps requested page sizes.
const (
	DefaultQueryLimit = 50
	MaxQueryLimit     = 200
)

// Postgres implements Store on a pgx connection pool.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Store = (*Postgres)(nil)

// NewPostgres wraps an existing pool. The caller owns the pool's lifecycle.
func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(`
			INSERT INTO events
				(id, contract_id, ledger, type, tx_hash, tx_index, op_index,
				 in_successful_call, topics, value)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (id) DO NOTHING`,
			e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value,
		)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()

	var inserted int64
	for range events {
		tag, err := results.Exec()
		if err != nil {
			return inserted, fmt.Errorf("upserting events: %w", err)
		}
		inserted += tag.RowsAffected()
	}
	return inserted, nil
}

const eventColumns = `id, contract_id, ledger, type, tx_hash, tx_index, op_index,
	in_successful_call, topics, value, created_at`

func (p *Postgres) GetEvent(ctx context.Context, id string) (Event, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id)
	e, err := scanEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	return e, err
}

func (p *Postgres) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if len(f.ContractIDs) > 0 {
		where = append(where, "contract_id = ANY("+arg(f.ContractIDs)+")")
	} else if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		where = append(where, "type = ANY("+arg(f.Types)+")")
	} else if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if len(f.Topic) > 0 {
		// Containment on the array matches the topic at any position.
		where = append(where, "topics @> "+arg(fmt.Sprintf("[%s]", f.Topic))+"::jsonb")
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}
	if f.Cursor != "" {
		where = append(where, "id > "+arg(f.Cursor))
	}

	query := `SELECT ` + eventColumns + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to know whether a next page exists.
	query += " ORDER BY id ASC LIMIT " + arg(limit+1)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("querying events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, "", err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading events: %w", err)
	}

	next := ""
	if len(events) > limit {
		events = events[:limit]
		next = events[limit-1].ID
	}
	return events, next, nil
}

func (p *Postgres) GetIngestionState(ctx context.Context) (IngestionState, error) {
	var s IngestionState
	err := p.pool.QueryRow(ctx,
		`SELECT last_ingested_ledger, last_cursor, updated_at FROM ingestion_state WHERE id = 1`,
	).Scan(&s.LastIngestedLedger, &s.LastCursor, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return IngestionState{}, ErrNotFound
	}
	if err != nil {
		return IngestionState{}, fmt.Errorf("loading ingestion state: %w", err)
	}
	return s, nil
}

func (p *Postgres) SaveIngestionState(ctx context.Context, s IngestionState) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingestion_state (id, last_ingested_ledger, last_cursor, updated_at)
		VALUES (1, $1, $2, now())
		ON CONFLICT (id) DO UPDATE SET
			last_ingested_ledger = EXCLUDED.last_ingested_ledger,
			last_cursor          = EXCLUDED.last_cursor,
			updated_at           = now()`,
		s.LastIngestedLedger, s.LastCursor,
	)
	if err != nil {
		return fmt.Errorf("saving ingestion state: %w", err)
	}
	return nil
}

func (p *Postgres) ListWatchedContracts(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT contract_id FROM watched_contracts ORDER BY contract_id`)
	if err != nil {
		return nil, fmt.Errorf("listing watched contracts: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (p *Postgres) AddWatchedContract(ctx context.Context, contractID string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO watched_contracts (contract_id) VALUES ($1)
		ON CONFLICT (contract_id) DO NOTHING`, contractID)
	if err != nil {
		return fmt.Errorf("adding watched contract: %w", err)
	}
	return nil
}

func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	// COUNT(DISTINCT contract_id) scans the contract_id index; fine at MVP
	// scale. contributors: replace with a maintained summary table if it
	// becomes a bottleneck on large datasets.
	err := p.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM events),
			(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
			(SELECT count(DISTINCT contract_id) FROM events),
			(SELECT count(*) FROM watched_contracts)`,
	).Scan(&s.TotalEvents, &s.LastIngestedLedger, &s.ContractCount, &s.WatchedContracts)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return s, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func scanEvent(row pgx.Row) (Event, error) {
	var e Event
	err := row.Scan(&e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash,
		&e.TxIndex, &e.OpIndex, &e.InSuccessfulCall, &e.Topics, &e.Value, &e.CreatedAt)
	if err != nil {
		return Event{}, err
	}
	return e, nil
}
