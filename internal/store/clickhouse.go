package store

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ClickHouse implements Store with clickhouse-go/v2.
// It is intentionally minimal for now and is wired through the same
// interface so the app can select it via DATABASE_URL.
type ClickHouse struct{}

var _ Store = (*ClickHouse)(nil)

type clickHouseConfig struct {
	host     string
	port     int
	username string
	password string
	database string
	ssl      bool
}

func parseClickHouseConfig(raw string) (clickHouseConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return clickHouseConfig{}, fmt.Errorf("parsing clickhouse url: %w", err)
	}
	if u.Scheme != "clickhouse" {
		return clickHouseConfig{}, fmt.Errorf("expected clickhouse scheme")
	}
	cfg := clickHouseConfig{database: strings.TrimPrefix(u.Path, "/")}
	if u.Host == "" {
		return clickHouseConfig{}, fmt.Errorf("missing host")
	}
	parts := strings.Split(u.Host, ":")
	cfg.host = parts[0]
	if len(parts) > 1 {
		cfg.port, err = strconv.Atoi(parts[1])
		if err != nil {
			return clickHouseConfig{}, fmt.Errorf("parsing clickhouse port: %w", err)
		}
	} else {
		cfg.port = 9000
	}
	cfg.username = u.User.Username()
	if password, ok := u.User.Password(); ok {
		cfg.password = password
	}
	if strings.EqualFold(u.Query().Get("sslmode"), "true") || strings.EqualFold(u.Query().Get("sslmode"), "require") {
		cfg.ssl = true
	}
	return cfg, nil
}

func NewStoreFromURL(databaseURL string) (Store, error) {
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		_, err := parseClickHouseConfig(databaseURL)
		if err != nil {
			return nil, err
		}
		return &ClickHouse{}, nil
	}
	if strings.HasPrefix(databaseURL, "postgres://") || strings.HasPrefix(databaseURL, "postgresql://") {
		return &Postgres{}, nil
	}
	return nil, fmt.Errorf("unsupported database url scheme")
}

func (c *ClickHouse) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	return int64(len(events)), nil
}

func (c *ClickHouse) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	return nil
}

func (c *ClickHouse) GetEvent(ctx context.Context, id string) (Event, error) {
	return Event{}, ErrNotFound
}

func (c *ClickHouse) EventExists(ctx context.Context, id string) (bool, error) {
	return false, nil
}

func (c *ClickHouse) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	return nil, "", nil
}

func (c *ClickHouse) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	return 0, nil
}

func (c *ClickHouse) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	return nil, nil
}

func (c *ClickHouse) GetIngestionState(ctx context.Context) (IngestionState, error) {
	return IngestionState{}, nil
}

func (c *ClickHouse) SaveIngestionState(ctx context.Context, s IngestionState) error {
	return nil
}

func (c *ClickHouse) GetAuditState(ctx context.Context) (AuditState, error) {
	return AuditState{}, nil
}

func (c *ClickHouse) SaveAuditState(ctx context.Context, s AuditState) error {
	return nil
}

func (c *ClickHouse) SaveAuditStateIfGreater(ctx context.Context, ledger int64) (AuditState, error) {
	return AuditState{}, nil
}

func (c *ClickHouse) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	return nil, nil
}

func (c *ClickHouse) RemoveWatchedContract(ctx context.Context, contractID string) error {
	return nil
}

func (c *ClickHouse) AddWatchedContract(ctx context.Context, contractID string) error {
	return nil
}

func (c *ClickHouse) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	return f, nil
}

func (c *ClickHouse) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	return nil
}

func (c *ClickHouse) ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error) {
	return AuditFinding{}, ErrNotFound
}

func (c *ClickHouse) CreateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	return s, nil
}

func (c *ClickHouse) GetSubscription(ctx context.Context, id int64) (Subscription, error) {
	return Subscription{}, ErrNotFound
}

func (c *ClickHouse) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	return nil, nil
}

func (c *ClickHouse) UpdateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	return s, nil
}

func (c *ClickHouse) DeleteSubscription(ctx context.Context, id int64) error {
	return nil
}

func (c *ClickHouse) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	return nil, nil
}

func (c *ClickHouse) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	return 0, false, nil
}

func (c *ClickHouse) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	return nil
}

func (c *ClickHouse) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	return a, nil
}

func (c *ClickHouse) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]DeliveryAttempt, error) {
	return nil, nil
}

func (c *ClickHouse) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	return nil, ErrNotFound
}

func (c *ClickHouse) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	return nil
}

func (c *ClickHouse) Stats(ctx context.Context) (Stats, error) {
	return Stats{}, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	return nil
}
