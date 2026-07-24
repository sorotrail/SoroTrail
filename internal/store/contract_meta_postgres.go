package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// GetContractMeta returns the cached metadata for a contract, or ErrNotFound.
func (p *Postgres) GetContractMeta(ctx context.Context, contractID string) (ContractMeta, error) {
	var m ContractMeta
	err := p.pool.QueryRow(ctx, `
		SELECT contract_id, name, symbol, decimals, is_token, fetched_at
		FROM contract_meta WHERE contract_id = $1`, contractID,
	).Scan(&m.ContractID, &m.Name, &m.Symbol, &m.Decimals, &m.IsToken, &m.FetchedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContractMeta{}, ErrNotFound
	}
	if err != nil {
		return ContractMeta{}, fmt.Errorf("getting contract meta for %s: %w", contractID, err)
	}
	return m, nil
}

// UpsertContractMeta inserts or updates a contract metadata row.
func (p *Postgres) UpsertContractMeta(ctx context.Context, m ContractMeta) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO contract_meta (contract_id, name, symbol, decimals, is_token, fetched_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (contract_id) DO UPDATE SET
			name       = EXCLUDED.name,
			symbol     = EXCLUDED.symbol,
			decimals   = EXCLUDED.decimals,
			is_token   = EXCLUDED.is_token,
			fetched_at = EXCLUDED.fetched_at`,
		m.ContractID, m.Name, m.Symbol, m.Decimals, m.IsToken, m.FetchedAt,
	)
	if err != nil {
		return fmt.Errorf("upserting contract meta for %s: %w", m.ContractID, err)
	}
	return nil
}

// CountContractEvents returns the number of events for a given contract.
func (p *Postgres) CountContractEvents(ctx context.Context, contractID string) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE contract_id = $1`, contractID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting events for %s: %w", contractID, err)
	}
	return count, nil
}

// ListContractIDs returns all distinct contract IDs that have emitted events.
func (p *Postgres) ListContractIDs(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT DISTINCT contract_id FROM events ORDER BY contract_id`)
	if err != nil {
		return nil, fmt.Errorf("listing contract IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning contract ID: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListContractsNeedingRefresh returns contract IDs whose metadata is stale
// (fetched before olderThan) or has never been fetched. This includes:
//   - Contracts in events that have NO contract_meta row at all (never probed)
//   - Contracts with fetched_at < olderThan (expired TTL)
//   - Contracts with is_token=true but null name/symbol/decimals (failed fetch, retry)
//
// Non-token contracts (is_token=false) are never re-fetched regardless of
// their fetched_at, so negative caching is permanent.
func (p *Postgres) ListContractsNeedingRefresh(ctx context.Context, olderThan time.Time) ([]string, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT DISTINCT e.contract_id
		FROM events e
		WHERE NOT EXISTS (
			SELECT 1 FROM contract_meta cm
			WHERE cm.contract_id = e.contract_id
			  AND (cm.is_token = false OR (cm.is_token = true AND cm.fetched_at > $1 AND cm.name IS NOT NULL))
		)
		ORDER BY e.contract_id`, olderThan,
	)
	if err != nil {
		return nil, fmt.Errorf("listing contracts needing refresh: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning contract ID: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
