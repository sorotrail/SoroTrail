package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// --- API key CRUD ---

// CreateAPIKey persists a key. The caller must have already hashed the
// secret into k.KeyHash (see internal/apikey); the plaintext secret is
// never passed to or stored by the store.
func (p *Postgres) CreateAPIKey(ctx context.Context, k APIKey) (APIKey, error) {
	err := p.pool.QueryRow(ctx, `
		INSERT INTO api_keys (name, prefix, key_hash)
		VALUES ($1, $2, $3)
		RETURNING id, created_at`,
		k.Name, k.Prefix, k.KeyHash,
	).Scan(&k.ID, &k.CreatedAt)
	if err != nil {
		return APIKey{}, fmt.Errorf("creating API key: %w", err)
	}
	return k, nil
}

func (p *Postgres) GetAPIKey(ctx context.Context, id int64) (APIKey, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT id, name, prefix, key_hash, created_at, revoked_at
		FROM api_keys WHERE id = $1`, id,
	)
	return scanAPIKey(row)
}

// LookupAPIKeyByPrefix returns the active key with the given prefix.
// Revoked keys are excluded by the WHERE clause, so a revocation takes
// effect on the very next request that presents the key.
func (p *Postgres) LookupAPIKeyByPrefix(ctx context.Context, prefix string) (APIKey, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT id, name, prefix, key_hash, created_at, revoked_at
		FROM api_keys
		WHERE prefix = $1 AND revoked_at IS NULL`, prefix,
	)
	return scanAPIKey(row)
}

func (p *Postgres) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, name, prefix, key_hash, created_at, revoked_at
		FROM api_keys ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing API keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading API keys: %w", err)
	}
	return keys, nil
}

// RevokeAPIKey marks the key revoked. It is idempotent for a key that
// is already revoked but returns ErrNotFound for an unknown id.
func (p *Postgres) RevokeAPIKey(ctx context.Context, id int64) error {
	tag, err := p.pool.Exec(ctx, `
		UPDATE api_keys SET revoked_at = now()
		WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoking API key %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the key does not exist or it is already revoked; both
		// mean "nothing to do" but only an existing key should be
		// reported as revoked. Distinguish by re-reading.
		_, err := p.GetAPIKey(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func scanAPIKey(row pgx.Row) (APIKey, error) {
	var (
		k       APIKey
		revoked *time.Time
	)
	err := row.Scan(&k.ID, &k.Name, &k.Prefix, &k.KeyHash, &k.CreatedAt, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, ErrNotFound
	}
	if err != nil {
		return APIKey{}, fmt.Errorf("scanning API key: %w", err)
	}
	k.RevokedAt = revoked
	return k, nil
}
