package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ TenantStore = (*Postgres)(nil)

const tenantColumns = `id, name, wildcard, is_admin, enabled,
	rate_limit_rps, rate_limit_burst, max_watched_contracts, created_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTenant(row rowScanner) (Tenant, error) {
	var t Tenant
	err := row.Scan(&t.ID, &t.Name, &t.Wildcard, &t.Admin, &t.Enabled,
		&t.RateLimitRPS, &t.RateLimitBurst, &t.MaxWatchedContracts, &t.CreatedAt)
	return t, err
}

// isUniqueViolation reports whether err is Postgres's 23505. Detecting the
// collision from the driver rather than pre-checking with a SELECT avoids a
// TOCTOU window where two concurrent creates both see "name is free".
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

func (p *Postgres) CreateTenant(ctx context.Context, t Tenant) (Tenant, error) {
	row := p.pool.QueryRow(ctx, `
		INSERT INTO tenants (name, wildcard, is_admin, enabled,
			rate_limit_rps, rate_limit_burst, max_watched_contracts)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+tenantColumns,
		t.Name, t.Wildcard, t.Admin, t.Enabled,
		t.RateLimitRPS, t.RateLimitBurst, t.MaxWatchedContracts)
	created, err := scanTenant(row)
	if isUniqueViolation(err) {
		return Tenant{}, fmt.Errorf("tenant %q: %w", t.Name, ErrDuplicate)
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("creating tenant: %w", err)
	}
	return created, nil
}

func (p *Postgres) GetTenant(ctx context.Context, id int64) (Tenant, error) {
	t, err := scanTenant(p.pool.QueryRow(ctx,
		`SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("loading tenant: %w", err)
	}
	return t, nil
}

func (p *Postgres) GetTenantByName(ctx context.Context, name string) (Tenant, error) {
	t, err := scanTenant(p.pool.QueryRow(ctx,
		`SELECT `+tenantColumns+` FROM tenants WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("loading tenant by name: %w", err)
	}
	return t, nil
}

func (p *Postgres) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+tenantColumns+` FROM tenants ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing tenants: %w", err)
	}
	defer rows.Close()

	tenants := []Tenant{}
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

func (p *Postgres) UpdateTenant(ctx context.Context, t Tenant) (Tenant, error) {
	row := p.pool.QueryRow(ctx, `
		UPDATE tenants SET
			name = $2, wildcard = $3, is_admin = $4, enabled = $5,
			rate_limit_rps = $6, rate_limit_burst = $7, max_watched_contracts = $8
		WHERE id = $1
		RETURNING `+tenantColumns,
		t.ID, t.Name, t.Wildcard, t.Admin, t.Enabled,
		t.RateLimitRPS, t.RateLimitBurst, t.MaxWatchedContracts)
	updated, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if isUniqueViolation(err) {
		return Tenant{}, fmt.Errorf("tenant %q: %w", t.Name, ErrDuplicate)
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("updating tenant: %w", err)
	}
	return updated, nil
}

// DeleteTenant removes the tenant and, by cascade, its keys, grants, watch
// list and usage history. Ingested events are untouched: they may be shared
// with other tenants, and deleting a customer must not delete data another
// customer is still reading.
func (p *Postgres) DeleteTenant(ctx context.Context, id int64) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting tenant: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) GrantContract(ctx context.Context, tenantID int64, contractID string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO tenant_contract_grants (tenant_id, contract_id)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, tenantID, contractID)
	if err != nil {
		return fmt.Errorf("granting contract: %w", err)
	}
	return nil
}

// RevokeContract drops a grant. Revoking something the tenant never held is
// a no-op rather than an error: the caller's intended postcondition — that
// the tenant cannot read this contract — holds either way, and reporting
// 404 would turn revocation into an oracle for which grants exist.
func (p *Postgres) RevokeContract(ctx context.Context, tenantID int64, contractID string) error {
	_, err := p.pool.Exec(ctx,
		`DELETE FROM tenant_contract_grants WHERE tenant_id = $1 AND contract_id = $2`,
		tenantID, contractID)
	if err != nil {
		return fmt.Errorf("revoking contract: %w", err)
	}
	return nil
}

func (p *Postgres) ListGrants(ctx context.Context, tenantID int64) ([]string, error) {
	return p.queryContractIDs(ctx,
		`SELECT contract_id FROM tenant_contract_grants WHERE tenant_id = $1 ORDER BY contract_id`,
		tenantID)
}

// ScopeForTenant resolves the tenant's read scope. A wildcard tenant never
// reads the grants table — its grants, if any, are irrelevant.
func (p *Postgres) ScopeForTenant(ctx context.Context, t Tenant) (Scope, error) {
	if t.Wildcard {
		return WildcardScope(), nil
	}
	ids, err := p.ListGrants(ctx, t.ID)
	if err != nil {
		return Scope{}, err
	}
	// NewScope of an empty list denies everything, which is the right
	// answer for a tenant that has been created but not yet granted
	// anything.
	return NewScope(ids), nil
}

// The two counts AddTenantWatchedContract enforces its caps with. They are
// constants shared with the reporting methods below so that the headroom a
// tenant is shown is measured exactly the way the limit is applied.
const (
	countTenantWatchedSQL   = `SELECT count(*) FROM tenant_watched_contracts WHERE tenant_id = $1`
	countInstanceWatchedSQL = `SELECT count(*) FROM (` + watchedContractsUnion + `) u`

	// instanceWatchCapAdvisoryLockKey is the transaction-scoped Postgres
	// advisory lock key used by AddTenantWatchedContract whenever an
	// instance-wide watch cap (MULTI_TENANT_MAX_WATCHED) is enforced (#1140).
	instanceWatchCapAdvisoryLockKey int64 = 0x53545F5741544348 // "ST_WATCH"
)

// CountTenantWatchedContracts returns how many contracts the tenant watches.
func (p *Postgres) CountTenantWatchedContracts(ctx context.Context, tenantID int64) (int, error) {
	var n int
	if err := p.pool.QueryRow(ctx, countTenantWatchedSQL, tenantID).Scan(&n); err != nil {
		return 0, fmt.Errorf("counting tenant watch list: %w", err)
	}
	return n, nil
}

// CountInstanceWatchedContracts returns the size of the union of every watch
// list, which is what the instance-wide cap bounds.
func (p *Postgres) CountInstanceWatchedContracts(ctx context.Context) (int, error) {
	var n int
	if err := p.pool.QueryRow(ctx, countInstanceWatchedSQL).Scan(&n); err != nil {
		return 0, fmt.Errorf("counting watched union: %w", err)
	}
	return n, nil
}

// AddTenantWatchedContract adds a contract to the tenant's watch list,
// enforcing the tenant's own cap and the instance-wide cap.
//
// Under Postgres' default READ COMMITTED isolation, running SELECT count(*)
// and INSERT inside a transaction without locking allows concurrent requests
// to observe the same pre-commit count and overrun both caps (#1140). To
// serialise cap checks without penalising the idempotent re-add path:
//   - The idempotent re-add check runs first with no lock.
//   - Before counting a tenant's watched contracts, the tenant's row in
//     tenants is locked with SELECT ... FOR UPDATE, serialising concurrent
//     adds for the same tenant.
//   - When an instance-wide cap is configured, pg_advisory_xact_lock is
//     acquired before counting the union across tenants, serialising
//     concurrent adds across different tenants competing for the last
//     instance slot. Both locks release automatically when tx commits or
//     rolls back.
func (p *Postgres) AddTenantWatchedContract(ctx context.Context, t Tenant, network, contractID string, instanceCap int) error {
	network = defaultNetwork(network)
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting watch-list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Already held: nothing to check, nothing to do. Checked first before
	// acquiring any row or advisory lock so a tenant at its cap can re-issue
	// an add idempotently without contending on locks.
	var exists int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM tenant_watched_contracts
		WHERE tenant_id = $1 AND network = $2 AND contract_id = $3`, t.ID, network, contractID).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("checking watch list: %w", err)
	}

	// Lock the tenant row so concurrent adds for the same tenant serialise
	// before counting held contracts. Re-check after locking in case a
	// concurrent request just inserted the same contract.
	var lockedTenantID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM tenants WHERE id = $1 FOR UPDATE`, t.ID).Scan(&lockedTenantID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("locking tenant row for watch-list cap check: %w", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM tenant_watched_contracts
		WHERE tenant_id = $1 AND network = $2 AND contract_id = $3`, t.ID, network, contractID).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("re-checking watch list under lock: %w", err)
	}

	if t.MaxWatchedContracts != nil {
		var held int
		if err := tx.QueryRow(ctx, countTenantWatchedSQL, t.ID).Scan(&held); err != nil {
			return fmt.Errorf("counting tenant watch list: %w", err)
		}
		if held >= *t.MaxWatchedContracts {
			return fmt.Errorf("tenant watch-list limit of %d reached: %w",
				*t.MaxWatchedContracts, ErrQuotaExceeded)
		}
	}

	if instanceCap > 0 {
		// Serialise instance-wide union cap checks across all tenants using a
		// transaction-scoped advisory lock.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, instanceWatchCapAdvisoryLockKey); err != nil {
			return fmt.Errorf("acquiring instance watch-list advisory lock: %w", err)
		}

		// The instance cap bounds the union, not any single list: what
		// costs the operator RPC budget is the number of distinct
		// (network, contract_id) pairs ingestion must poll. A contract
		// another tenant already watches on this network is therefore free
		// to add.
		var unionSize int
		if err := tx.QueryRow(ctx, countInstanceWatchedSQL).Scan(&unionSize); err != nil {
			return fmt.Errorf("counting watched union: %w", err)
		}
		var alreadyWatched int
		err := tx.QueryRow(ctx,
			`SELECT 1 FROM (`+watchedContractsUnion+`) u WHERE network = $1 AND contract_id = $2`,
			network, contractID).Scan(&alreadyWatched)
		switch {
		case err == nil:
			// Already in the union; adding this tenant's claim does not
			// grow it.
		case errors.Is(err, pgx.ErrNoRows):
			if unionSize >= instanceCap {
				return fmt.Errorf("instance watch-list limit of %d reached: %w",
					instanceCap, ErrQuotaExceeded)
			}
		default:
			return fmt.Errorf("checking watched union: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO tenant_watched_contracts (tenant_id, network, contract_id)
		VALUES ($1, $2, $3) ON CONFLICT (tenant_id, network, contract_id) DO NOTHING`,
		t.ID, network, contractID); err != nil {
		return fmt.Errorf("adding tenant watched contract: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing watch-list add: %w", err)
	}
	return nil
}

func (p *Postgres) RemoveTenantWatchedContract(ctx context.Context, tenantID int64, network, contractID string) error {
	network = defaultNetwork(network)
	_, err := p.pool.Exec(ctx,
		`DELETE FROM tenant_watched_contracts WHERE tenant_id = $1 AND network = $2 AND contract_id = $3`,
		tenantID, network, contractID)
	if err != nil {
		return fmt.Errorf("removing tenant watched contract: %w", err)
	}
	return nil
}

func (p *Postgres) ListTenantWatchedContracts(ctx context.Context, tenantID int64, network string) ([]string, error) {
	network = defaultNetwork(network)
	return p.queryContractIDs(ctx,
		`SELECT contract_id FROM tenant_watched_contracts WHERE tenant_id = $1 AND network = $2 ORDER BY contract_id`,
		tenantID, network)
}

// queryContractIDs runs query — whose first column is a contract id — and
// projects the resulting row set into a slice via contractIDs. Ordering
// and de-duplication are the query's job: callers pass ORDER BY and rely
// on their tables' constraints, so this method only translates the rows
// Postgres returns into a slice without reinterpreting them.
func (p *Postgres) queryContractIDs(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing contract IDs: %w", err)
	}
	defer rows.Close()
	return contractIDs(rows)
}

// contractIDs projects the id column out of a row set, preserving row
// order and duplicates: the caller's ORDER BY is what makes the result
// deterministic, and any de-duplication happens in SQL, not here. It
// returns an empty, non-nil slice when there are no rows, so callers can
// distinguish "nothing matched" from "query never ran".
func contractIDs(rows pgx.Rows) ([]string, error) {
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

const apiKeyColumns = `id, tenant_id, name, prefix, created_at, last_used_at, revoked_at`

func scanTenantAPIKey(row rowScanner) (TenantAPIKey, error) {
	var k TenantAPIKey
	err := row.Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix,
		&k.CreatedAt, &k.LastUsedAt, &k.RevokedAt)
	return k, err
}

func (p *Postgres) CreateTenantAPIKey(ctx context.Context, tenantID int64, name, prefix string, digest []byte) (TenantAPIKey, error) {
	row := p.pool.QueryRow(ctx, `
		INSERT INTO tenant_api_keys (tenant_id, name, prefix, key_hash)
		VALUES ($1, $2, $3, $4)
		RETURNING `+apiKeyColumns,
		tenantID, name, prefix, digest)
	k, err := scanTenantAPIKey(row)
	if isUniqueViolation(err) {
		return TenantAPIKey{}, ErrDuplicate
	}
	if err != nil {
		return TenantAPIKey{}, fmt.Errorf("creating api key: %w", err)
	}
	return k, nil
}

// CreateTenantAPIKeyIfAbsent inserts the key unless its prefix is already present.
// The digest is refreshed on conflict so that rotating the bootstrap value
// in the environment actually takes effect, and revoked_at is cleared so a
// restart with the key still configured restores it deliberately rather than
// leaving a confusing tombstone.
func (p *Postgres) CreateTenantAPIKeyIfAbsent(ctx context.Context, tenantID int64, name, prefix string, digest []byte) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO tenant_api_keys (tenant_id, name, prefix, key_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (prefix) DO UPDATE SET
			key_hash   = EXCLUDED.key_hash,
			tenant_id  = EXCLUDED.tenant_id,
			revoked_at = NULL`,
		tenantID, name, prefix, digest)
	if err != nil {
		return fmt.Errorf("creating api key: %w", err)
	}
	return nil
}

// LookupTenantAPIKey resolves a key prefix to its record, digest and tenant in one
// round trip. Revoked keys are excluded here rather than checked by the
// caller, so a forgotten check cannot resurrect a revoked credential.
func (p *Postgres) LookupTenantAPIKey(ctx context.Context, prefix string) (TenantAPIKey, []byte, Tenant, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT k.id, k.tenant_id, k.name, k.prefix, k.created_at, k.last_used_at, k.revoked_at,
		       k.key_hash,
		       t.id, t.name, t.wildcard, t.is_admin, t.enabled,
		       t.rate_limit_rps, t.rate_limit_burst, t.max_watched_contracts, t.created_at
		FROM tenant_api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		WHERE k.prefix = $1 AND k.revoked_at IS NULL`, prefix)

	var (
		k      TenantAPIKey
		digest []byte
		t      Tenant
	)
	err := row.Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix, &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt,
		&digest,
		&t.ID, &t.Name, &t.Wildcard, &t.Admin, &t.Enabled,
		&t.RateLimitRPS, &t.RateLimitBurst, &t.MaxWatchedContracts, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TenantAPIKey{}, nil, Tenant{}, ErrNotFound
	}
	if err != nil {
		return TenantAPIKey{}, nil, Tenant{}, fmt.Errorf("looking up api key: %w", err)
	}
	return k, digest, t, nil
}

// TouchTenantAPIKey records last use. Failures are the caller's to ignore: this is
// observability, and a write error here must not deny an otherwise valid
// request.
func (p *Postgres) TouchTenantAPIKey(ctx context.Context, id int64) error {
	_, err := p.pool.Exec(ctx, `UPDATE tenant_api_keys SET last_used_at = now() WHERE id = $1`, id)
	return err
}

func (p *Postgres) ListTenantAPIKeys(ctx context.Context, tenantID int64) ([]TenantAPIKey, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+apiKeyColumns+` FROM tenant_api_keys WHERE tenant_id = $1 ORDER BY id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("listing api keys: %w", err)
	}
	defer rows.Close()

	keys := []TenantAPIKey{}
	for rows.Next() {
		k, err := scanTenantAPIKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (p *Postgres) RevokeTenantAPIKey(ctx context.Context, id int64) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE tenant_api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoking api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AddUsage folds a batch of increments into the per-day rows. Counters are
// accumulated with += rather than overwritten so concurrent API servers
// behind a load balancer each flush their own share without clobbering the
// others.
func (p *Postgres) AddUsage(ctx context.Context, day time.Time, deltas map[int64]UsageDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for tenantID, d := range deltas {
		if d.Empty() {
			continue
		}
		batch.Queue(`
			INSERT INTO tenant_usage (tenant_id, day, requests, events_served, stream_seconds)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tenant_id, day) DO UPDATE SET
				requests       = tenant_usage.requests + EXCLUDED.requests,
				events_served  = tenant_usage.events_served + EXCLUDED.events_served,
				stream_seconds = tenant_usage.stream_seconds + EXCLUDED.stream_seconds`,
			tenantID, day.UTC(), d.Requests, d.EventsServed, d.StreamSeconds)
	}
	if batch.Len() == 0 {
		return nil
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			// A tenant deleted between accounting and flush drops its
			// increments on the foreign key; that is correct, not an error
			// worth failing the whole batch over.
			if isForeignKeyViolation(err) {
				continue
			}
			return fmt.Errorf("recording usage: %w", err)
		}
	}
	return nil
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation
}

func (p *Postgres) ListUsage(ctx context.Context, tenantID int64, days int) ([]TenantUsage, error) {
	if days <= 0 {
		days = 30
	}
	rows, err := p.pool.Query(ctx, `
		SELECT tenant_id, day, requests, events_served, stream_seconds
		FROM tenant_usage
		WHERE tenant_id = $1
		ORDER BY day DESC
		LIMIT $2`, tenantID, days)
	if err != nil {
		return nil, fmt.Errorf("listing usage: %w", err)
	}
	defer rows.Close()

	usage := []TenantUsage{}
	for rows.Next() {
		var u TenantUsage
		if err := rows.Scan(&u.TenantID, &u.Day, &u.Requests, &u.EventsServed, &u.StreamSeconds); err != nil {
			return nil, err
		}
		usage = append(usage, u)
	}
	return usage, rows.Err()
}
