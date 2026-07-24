package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// --- Subscription CRUD ---

func (p *Postgres) CreateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	filtersJSON, err := json.Marshal(s.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	err = p.pool().QueryRow(ctx, `
		INSERT INTO subscriptions (url, filters, secret, enabled)
		VALUES ($1, $2, $3, $4)
		RETURNING id, failure_count, created_at`,
		s.URL, filtersJSON, s.Secret, s.Enabled,
	).Scan(&s.ID, &s.FailureCount, &s.CreatedAt)
	if err != nil {
		return Subscription{}, fmt.Errorf("creating subscription: %w", err)
	}
	return s, nil
}

func (p *Postgres) GetSubscription(ctx context.Context, id int64) (Subscription, error) {
	var (
		s       Subscription
		filters []byte
	)
	err := p.pool().QueryRow(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions WHERE id = $1`, id,
	).Scan(&s.ID, &s.URL, &filters, &s.Secret, &s.Enabled, &s.FailureCount, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, fmt.Errorf("getting subscription %d: %w", id, err)
	}
	if err := json.Unmarshal(filters, &s.Filters); err != nil {
		return Subscription{}, fmt.Errorf("unmarshaling subscription filters: %w", err)
	}
	return s, nil
}

func (p *Postgres) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := p.pool().Query(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

func (p *Postgres) UpdateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	filtersJSON, err := json.Marshal(s.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	var updatedFilters []byte
	err = p.pool().QueryRow(ctx, `
		UPDATE subscriptions SET
			url    = $2,
			filters = $3,
			secret  = $4,
			enabled = $5
		WHERE id = $1
		RETURNING filters`,

		s.ID, s.URL, filtersJSON, s.Secret, s.Enabled,
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
	return p.GetSubscription(ctx, s.ID)
}

func (p *Postgres) DeleteSubscription(ctx context.Context, id int64) error {
	tag, err := p.pool().Exec(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting subscription %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// --- Enabled subscriptions ---

func (p *Postgres) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := p.pool().Query(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions WHERE enabled = true ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing enabled subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

// --- Failure counting ---

func (p *Postgres) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	var newCount int
	var stillEnabled bool
	err := p.pool().QueryRow(ctx, `
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
	_, err := p.pool().Exec(ctx,
		`UPDATE subscriptions SET failure_count = 0 WHERE id = $1`, id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("resetting failures for subscription %d: %w", id, err)
	}
	return nil
}

// --- Delivery attempts ---

func (p *Postgres) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	err := p.pool().QueryRow(ctx, `
		INSERT INTO delivery_attempts
			(subscription_id, event_id, status, response_code, duration_ms, error)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at`,
		a.SubscriptionID, a.EventID, a.Status, a.ResponseCode,
		a.DurationMs, nullableString(a.Error),
	).Scan(&a.ID, &a.CreatedAt)
	if err != nil {
		return DeliveryAttempt{}, fmt.Errorf("recording delivery attempt: %w", err)
	}
	return a, nil
}

func (p *Postgres) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]DeliveryAttempt, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := p.pool().Query(ctx, `
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

// --- helpers ---

func scanSubscriptions(rows pgx.Rows) ([]Subscription, error) {
	var subs []Subscription
	for rows.Next() {
		var (
			s       Subscription
			filters []byte
		)
		if err := rows.Scan(&s.ID, &s.URL, &filters, &s.Secret, &s.Enabled,
			&s.FailureCount, &s.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning subscription: %w", err)
		}
		if err := json.Unmarshal(filters, &s.Filters); err != nil {
			return nil, fmt.Errorf("unmarshaling subscription filters: %w", err)
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

// Delivery status constants.
const (
	DeliverySuccess = "success"
	DeliveryFailed  = "failed"
)
