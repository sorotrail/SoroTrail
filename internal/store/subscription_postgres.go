package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"
)

// --- Subscription CRUD ---

// subscriptionColumns is shared by every subscription read so tenant_id
// cannot be forgotten by one of them.
const subscriptionColumns = `id, url, filters, secret, enabled, failure_count, created_at, tenant_id`

// ownerPredicate renders the owner filter as a SQL fragment plus its bind
// value. An unrestricted owner contributes no predicate; any other —
// including the zero value — restricts to a tenant_id, and the zero value's
// 0 matches no row because the column references a bigserial.
func ownerPredicate(o SubscriptionOwner, argN int) (string, []any) {
	if o.IsAll() {
		return "", nil
	}
	return fmt.Sprintf(" AND tenant_id = $%d", argN), []any{o.TenantID()}
}

func (p *Postgres) CreateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	filtersJSON, err := json.Marshal(s.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	storedSecret, err := encryptSubscriptionSecret(p.FieldEncryptor(), s.Secret)
	if err != nil {
		return Subscription{}, fmt.Errorf("encrypting subscription secret: %w", err)
	}
	err = p.pool.QueryRow(ctx, `
		INSERT INTO subscriptions (url, filters, secret, enabled, tenant_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, failure_count, created_at`,
		s.URL, filtersJSON, storedSecret, s.Enabled, s.TenantID,
	).Scan(&s.ID, &s.FailureCount, &s.CreatedAt)
	if err != nil {
		return Subscription{}, fmt.Errorf("creating subscription: %w", err)
	}
	return s, nil
}

func (p *Postgres) GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error) {
	var (
		s            Subscription
		filters      []byte
		storedSecret string
		network      *string
	)
	pred, args := ownerPredicate(owner, 2)
	err := p.pool.QueryRow(ctx,
		`SELECT `+subscriptionColumns+` FROM subscriptions WHERE id = $1`+pred,
		append([]any{id}, args...)...,
	).Scan(&s.ID, &s.URL, &filters, &storedSecret, &s.Enabled, &s.FailureCount, &s.CreatedAt, &s.TenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, fmt.Errorf("getting subscription %d: %w", id, err)
	}
	secret, err := decryptSubscriptionSecret(p.FieldEncryptor(), storedSecret)
	if err != nil {
		return Subscription{}, fmt.Errorf("decrypting subscription %d secret: %w", id, err)
	}
	s.Secret = secret
	if err := json.Unmarshal(filters, &s.Filters); err != nil {
		return Subscription{}, fmt.Errorf("unmarshaling subscription filters: %w", err)
	}
	if network != nil {
		s.Filters.Network = *network
	}
	return s, nil
}

func (p *Postgres) ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error) {
	// WHERE true lets the owner predicate append uniformly rather than
	// needing to know whether it is the first clause.
	pred, args := ownerPredicate(owner, 1)
	rows, err := p.pool.Query(ctx,
		`SELECT `+subscriptionColumns+` FROM subscriptions WHERE true`+pred+` ORDER BY id`,
		args...)
	if err != nil {
		return nil, fmt.Errorf("listing subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptions(rows, p.FieldEncryptor())
}

func (p *Postgres) UpdateSubscription(ctx context.Context, s Subscription, owner SubscriptionOwner) (Subscription, error) {
	filtersJSON, err := json.Marshal(s.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	storedSecret, err := encryptSubscriptionSecret(p.FieldEncryptor(), s.Secret)
	if err != nil {
		return Subscription{}, fmt.Errorf("encrypting subscription %d secret: %w", s.ID, err)
	}
	var updatedFilters []byte
	pred, ownerArgs := ownerPredicate(owner, 6)
	err = p.pool.QueryRow(ctx, `
		UPDATE subscriptions SET
			url    = $2,
			filters = $3,
			secret  = $4,
			enabled = $5
		WHERE id = $1`+pred+`
		RETURNING filters`,
		append([]any{s.ID, s.URL, filtersJSON, storedSecret, s.Enabled}, ownerArgs...)...,
	).Scan(&updatedFilters)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, fmt.Errorf("updating subscription %d: %w", s.ID, err)
	}
	if err := json.Unmarshal(updatedFilters, &s.Filters); err != nil {
		return Subscription{}, fmt.Errorf("unmarshaling subscription filters: %w", err)
	}
	// Re-read to get failure_count and created_at (not updated here).
	return p.GetSubscription(ctx, s.ID, owner)
}

func (p *Postgres) DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error {
	pred, args := ownerPredicate(owner, 2)
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM subscriptions WHERE id = $1`+pred,
		append([]any{id}, args...)...)
	if err != nil {
		return fmt.Errorf("deleting subscription %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// --- Enabled subscriptions ---

// ListEnabledSubscriptions is deliberately unowned: the delivery worker is
// process machinery serving every tenant at once, not a caller. What keeps
// delivery inside the boundary is the check at creation time — a
// non-wildcard tenant's subscription must name a contract it holds — so a
// subscription can only ever match events its owner may read.
func (p *Postgres) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+subscriptionColumns+` FROM subscriptions WHERE enabled = true ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing enabled subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptions(rows, p.FieldEncryptor())
}

func (p *Postgres) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	var newCount int
	var stillEnabled bool
	err := p.pool.QueryRow(ctx, `
		UPDATE subscriptions SET
			failure_count = failure_count + 1,
			enabled = CASE WHEN failure_count + 1 >= $2 THEN false ELSE enabled END
		WHERE id = $1
		RETURNING failure_count, enabled`,
		id, maxFailures,
	).Scan(&newCount, &stillEnabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, ErrNotFound
	}
	if err != nil {
		return 0, false, fmt.Errorf("incrementing failures for subscription %d: %w", id, err)
	}
	return newCount, !stillEnabled, nil
}

func (p *Postgres) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE subscriptions SET failure_count = 0 WHERE id = $1`, id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("resetting failures for subscription %d: %w", id, err)
	}
	return nil
}

func (p *Postgres) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	err := p.pool.QueryRow(ctx, `
		INSERT INTO delivery_attempts
			(subscription_id, event_id, status, response_code, duration_ms, error)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at`,
		a.SubscriptionID, a.EventID, a.Status, a.ResponseCode,
		a.DurationMs, a.Error, // NOT NULL DEFAULT ''; empty means success, never NULL
	).Scan(&a.ID, &a.CreatedAt)
	if err != nil {
		return DeliveryAttempt{}, fmt.Errorf("recording delivery attempt: %w", err)
	}
	return a, nil
}

func (p *Postgres) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error) {
	if limit <= 0 {
		limit = 50
	}
	// Resolve the subscription under the owner filter first: delivery
	// history names the events that matched, so reading another tenant's is
	// reading their data one attempt at a time.
	if _, err := p.GetSubscription(ctx, subscriptionID, owner); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, subscription_id, event_id, status, response_code,
		       duration_ms, error, created_at
		FROM delivery_attempts
		WHERE subscription_id = $1
		ORDER BY created_at DESC
		LIMIT $2`,
		subscriptionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("listing delivery attempts: %w", err)
	}
	defer rows.Close()

	var attempts []DeliveryAttempt
	for rows.Next() {
		var a DeliveryAttempt
		var errStr *string
		if err := rows.Scan(&a.ID, &a.SubscriptionID, &a.EventID, &a.Status,
			&a.ResponseCode, &a.DurationMs, &errStr, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning delivery attempt: %w", err)
		}
		if errStr != nil {
			a.Error = *errStr
		}
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading delivery attempts: %w", err)
	}
	return attempts, nil
}

// CountDeliveryAttempts returns the total number of delivery attempts
// recorded for a subscription, ignoring the list's limit. The same owner
// check as ListDeliveryAttempts gates it: delivery history reveals which
// events matched, so a tenant must not be able to count another's.
func (p *Postgres) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, owner SubscriptionOwner) (int64, error) {
	if _, err := p.GetSubscription(ctx, subscriptionID, owner); err != nil {
		return 0, err
	}
	var total int64
	if err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM delivery_attempts WHERE subscription_id = $1`,
		subscriptionID,
	).Scan(&total); err != nil {
		return 0, fmt.Errorf("counting delivery attempts: %w", err)
	}
	return total, nil
}

func scanSubscriptions(rows pgx.Rows, enc *FieldEncryptor) ([]Subscription, error) {
	var subs []Subscription
	for rows.Next() {
		var (
			s            Subscription
			filters      []byte
			storedSecret string
			network      *string
		)
		if err := rows.Scan(&s.ID, &s.URL, &filters, &storedSecret, &s.Enabled,
			&s.FailureCount, &s.CreatedAt, &s.TenantID); err != nil {
			return nil, fmt.Errorf("scanning subscription: %w", err)
		}
		secret, err := decryptSubscriptionSecret(enc, storedSecret)
		if err != nil {
			return nil, fmt.Errorf("decrypting subscription %d secret: %w", s.ID, err)
		}
		s.Secret = secret
		if err := json.Unmarshal(filters, &s.Filters); err != nil {
			return nil, fmt.Errorf("unmarshaling subscription filters: %w", err)
		}
		if network != nil {
			s.Filters.Network = *network
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

// VerifyEncryptionReadiness checks stored subscriptions.secret rows at startup
// to verify that if any encrypted rows exist, the configured FieldEncryptor
// contains every required key version and can authenticate a sample row for each.
func (p *Postgres) VerifyEncryptionReadiness(ctx context.Context) error {
	rows, err := p.pool.Query(ctx, `SELECT id, secret FROM subscriptions WHERE secret LIKE 'enc:%'`)
	if err != nil {
		return fmt.Errorf("checking encrypted subscriptions readiness: %w", err)
	}
	defer rows.Close()

	enc := p.FieldEncryptor()
	verifiedVersions := make(map[string]bool)
	var missingVersions []string

	for rows.Next() {
		var (
			id     int64
			secret string
		)
		if err := rows.Scan(&id, &secret); err != nil {
			return fmt.Errorf("scanning encrypted subscription readiness row: %w", err)
		}
		ver, ok := EncryptedValueVersion(secret)
		if !ok {
			return fmt.Errorf("subscription %d: %w: malformed encrypted envelope", id, ErrEncryptionAuthFailed)
		}
		if enc == nil {
			return fmt.Errorf("subscription %d: %w (version %q)", id, ErrEncryptionKeyRequired, ver)
		}
		if _, exists := enc.aeads[ver]; !exists {
			missingVersions = append(missingVersions, ver)
			continue
		}
		if !verifiedVersions[ver] {
			if _, err := enc.DecryptString(secret); err != nil {
				return fmt.Errorf("subscription %d: %w", id, err)
			}
			verifiedVersions[ver] = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading encrypted subscriptions readiness: %w", err)
	}
	if len(missingVersions) > 0 {
		sort.Strings(missingVersions)
		return fmt.Errorf("%w: database contains rows encrypted with key version(s) %v not present in keyring", ErrUnknownEncryptionKeyVersion, compactUniqueStrings(missingVersions))
	}
	return nil
}

// RotateSubscriptionsEncryption scans subscriptions.secret in bounded batches,
// encrypting legacy plaintext rows and re-encrypting rows on older key versions
// to the active key version with fresh random nonces. Each row update is guarded
// by `WHERE id = $2 AND secret = $3` so concurrent tenant secret updates are
// never clobbered.
func (p *Postgres) RotateSubscriptionsEncryption(ctx context.Context, batchSize int) (EncryptionRotationStats, error) {
	enc := p.FieldEncryptor()
	if enc == nil {
		return EncryptionRotationStats{}, ErrEncryptionKeyRequired
	}
	if batchSize <= 0 {
		batchSize = 250
	}

	var (
		stats  EncryptionRotationStats
		lastID int64
	)

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		rows, err := p.pool.Query(ctx, `
			SELECT id, secret
			FROM subscriptions
			WHERE id > $1
			ORDER BY id ASC
			LIMIT $2`,
			lastID, batchSize,
		)
		if err != nil {
			return stats, fmt.Errorf("scanning subscriptions for encryption rotation: %w", err)
		}

		type rowItem struct {
			id     int64
			secret string
		}
		var batch []rowItem
		for rows.Next() {
			var it rowItem
			if err := rows.Scan(&it.id, &it.secret); err != nil {
				rows.Close()
				return stats, fmt.Errorf("reading subscription row for rotation: %w", err)
			}
			batch = append(batch, it)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return stats, fmt.Errorf("iterating subscriptions for rotation: %w", err)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return stats, fmt.Errorf("beginning encryption rotation batch transaction: %w", err)
		}

		for _, it := range batch {
			stats.Scanned++
			lastID = it.id
			if it.secret == "" || !enc.NeedsRotation(it.secret) {
				stats.AlreadyActive++
				continue
			}

			wasEncrypted := IsEncryptedValue(it.secret)
			plaintext, err := enc.DecryptString(it.secret)
			if err != nil {
				_ = tx.Rollback(ctx)
				return stats, fmt.Errorf("decrypting subscription %d during rotation: %w", it.id, err)
			}
			reencrypted, err := enc.EncryptString(plaintext)
			if err != nil {
				_ = tx.Rollback(ctx)
				return stats, fmt.Errorf("encrypting subscription %d during rotation: %w", it.id, err)
			}
			tag, err := tx.Exec(ctx, `UPDATE subscriptions SET secret = $1 WHERE id = $2 AND secret = $3`, reencrypted, it.id, it.secret)
			if err != nil {
				_ = tx.Rollback(ctx)
				return stats, fmt.Errorf("updating subscription %d during rotation: %w", it.id, err)
			}
			if tag.RowsAffected() == 0 {
				stats.SkippedConcurrentUpdates++
				continue
			}
			if wasEncrypted {
				stats.ReEncrypted++
			} else {
				stats.EncryptedNew++
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return stats, fmt.Errorf("committing encryption rotation batch: %w", err)
		}

		slog.Default().Info("rotated subscription secrets encryption batch",
			"active_version", enc.ActiveVersion(),
			"scanned", stats.Scanned,
			"encrypted_new", stats.EncryptedNew,
			"re_encrypted", stats.ReEncrypted,
			"already_active", stats.AlreadyActive,
			"skipped_concurrent_updates", stats.SkippedConcurrentUpdates,
		)

		if len(batch) < batchSize {
			break
		}
	}

	return stats, nil
}

const (
	DeliverySuccess = "success"
	DeliveryFailed  = "failed"
)
