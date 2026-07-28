package store

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"time"
)

type GuardedStoreOptions struct {
	Timeout            time.Duration
	SlowQueryThreshold time.Duration
	Logger             *slog.Logger
}

type guardedStore struct {
	Store
	options     GuardedStoreOptions
	queryErrors atomic.Uint64
}

type queryNameContextKey struct{}

func NewGuardedStore(base Store, opts GuardedStoreOptions) Store {
	if base == nil {
		return nil
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 25 * time.Second
	}
	if opts.SlowQueryThreshold <= 0 {
		opts.SlowQueryThreshold = 2 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &guardedStore{Store: base, options: opts}
}

func (s *guardedStore) wrapContext(ctx context.Context, name string) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, queryNameContextKey{}, name)
	return context.WithTimeout(ctx, s.options.Timeout)
}

func (s *guardedStore) logSlowQuery(name string, start time.Time, err error) {
	if err != nil {
		s.queryErrors.Add(1)
	}
	duration := time.Since(start)
	if duration < s.options.SlowQueryThreshold {
		return
	}
	s.options.Logger.Warn("slow store query",
		"query", name,
		"duration", duration,
		"error", err,
	)
}

func (s *guardedStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.UpsertEvents")
	defer cancel()
	start := time.Now()
	n, err := s.Store.UpsertEvents(ctx, events)
	s.logSlowQuery("store.UpsertEvents", start, err)
	return n, err
}

func (s *guardedStore) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.ReplaceEventsInRange")
	defer cancel()
	start := time.Now()
	err := s.Store.ReplaceEventsInRange(ctx, events, fromLedger, toLedger)
	s.logSlowQuery("store.ReplaceEventsInRange", start, err)
	return err
}

func (s *guardedStore) GetEvent(ctx context.Context, id string) (Event, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetEvent")
	defer cancel()
	start := time.Now()
	e, err := s.Store.GetEvent(ctx, id)
	s.logSlowQuery("store.GetEvent", start, err)
	return e, err
}

func (s *guardedStore) EventExists(ctx context.Context, id string) (bool, error) {
	ctx, cancel := s.wrapContext(ctx, "store.EventExists")
	defer cancel()
	start := time.Now()
	exists, err := s.Store.EventExists(ctx, id)
	s.logSlowQuery("store.EventExists", start, err)
	return exists, err
}

func (s *guardedStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.QueryEvents")
	defer cancel()
	start := time.Now()
	events, cursor, err := s.Store.QueryEvents(ctx, f)
	s.logSlowQuery("store.QueryEvents", start, err)
	return events, cursor, err
}

func (s *guardedStore) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountEvents")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountEvents(ctx, f)
	s.logSlowQuery("store.CountEvents", start, err)
	return total, err
}

func (s *guardedStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	ctx, cancel := s.wrapContext(ctx, "store.LedgerRangeCensus")
	defer cancel()
	start := time.Now()
	census, err := s.Store.LedgerRangeCensus(ctx, fromLedger, toLedger, idsOnly)
	s.logSlowQuery("store.LedgerRangeCensus", start, err)
	return census, err
}

func (s *guardedStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetIngestionState")
	defer cancel()
	start := time.Now()
	state, err := s.Store.GetIngestionState(ctx)
	s.logSlowQuery("store.GetIngestionState", start, err)
	return state, err
}

func (s *guardedStore) SaveIngestionState(ctx context.Context, state IngestionState) error {
	ctx, cancel := s.wrapContext(ctx, "store.SaveIngestionState")
	defer cancel()
	start := time.Now()
	err := s.Store.SaveIngestionState(ctx, state)
	s.logSlowQuery("store.SaveIngestionState", start, err)
	return err
}

func (s *guardedStore) GetAuditState(ctx context.Context) (AuditState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetAuditState")
	defer cancel()
	start := time.Now()
	state, err := s.Store.GetAuditState(ctx)
	s.logSlowQuery("store.GetAuditState", start, err)
	return state, err
}

func (s *guardedStore) SaveAuditState(ctx context.Context, state AuditState) error {
	ctx, cancel := s.wrapContext(ctx, "store.SaveAuditState")
	defer cancel()
	start := time.Now()
	err := s.Store.SaveAuditState(ctx, state)
	s.logSlowQuery("store.SaveAuditState", start, err)
	return err
}

func (s *guardedStore) SaveAuditStateIfGreater(ctx context.Context, ledger int64) (AuditState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.SaveAuditStateIfGreater")
	defer cancel()
	start := time.Now()
	state, err := s.Store.SaveAuditStateIfGreater(ctx, ledger)
	s.logSlowQuery("store.SaveAuditStateIfGreater", start, err)
	return state, err
}

func (s *guardedStore) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListWatchedContracts")
	defer cancel()
	start := time.Now()
	ids, err := s.Store.ListWatchedContracts(ctx)
	s.logSlowQuery("store.ListWatchedContracts", start, err)
	return ids, err
}

func (s *guardedStore) AddWatchedContract(ctx context.Context, contractID string) error {
	ctx, cancel := s.wrapContext(ctx, "store.AddWatchedContract")
	defer cancel()
	start := time.Now()
	err := s.Store.AddWatchedContract(ctx, contractID)
	s.logSlowQuery("store.AddWatchedContract", start, err)
	return err
}

func (s *guardedStore) RemoveWatchedContract(ctx context.Context, contractID string) error {
	ctx, cancel := s.wrapContext(ctx, "store.RemoveWatchedContract")
	defer cancel()
	start := time.Now()
	err := s.Store.RemoveWatchedContract(ctx, contractID)
	s.logSlowQuery("store.RemoveWatchedContract", start, err)
	return err
}

func (s *guardedStore) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	ctx, cancel := s.wrapContext(ctx, "store.RecordAuditFinding")
	defer cancel()
	start := time.Now()
	finding, err := s.Store.RecordAuditFinding(ctx, f)
	s.logSlowQuery("store.RecordAuditFinding", start, err)
	return finding, err
}

func (s *guardedStore) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	ctx, cancel := s.wrapContext(ctx, "store.UpdateAuditFinding")
	defer cancel()
	start := time.Now()
	err := s.Store.UpdateAuditFinding(ctx, f)
	s.logSlowQuery("store.UpdateAuditFinding", start, err)
	return err
}

func (s *guardedStore) ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListOpenFindingsByRange")
	defer cancel()
	start := time.Now()
	finding, err := s.Store.ListOpenFindingsByRange(ctx, fromLedger, toLedger)
	s.logSlowQuery("store.ListOpenFindingsByRange", start, err)
	return finding, err
}

func (s *guardedStore) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CreateSubscription")
	defer cancel()
	start := time.Now()
	created, err := s.Store.CreateSubscription(ctx, sub)
	s.logSlowQuery("store.CreateSubscription", start, err)
	return created, err
}

func (s *guardedStore) GetSubscription(ctx context.Context, id int64) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetSubscription")
	defer cancel()
	start := time.Now()
	sub, err := s.Store.GetSubscription(ctx, id)
	s.logSlowQuery("store.GetSubscription", start, err)
	return sub, err
}

func (s *guardedStore) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListSubscriptions")
	defer cancel()
	start := time.Now()
	subs, err := s.Store.ListSubscriptions(ctx)
	s.logSlowQuery("store.ListSubscriptions", start, err)
	return subs, err
}

func (s *guardedStore) UpdateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.UpdateSubscription")
	defer cancel()
	start := time.Now()
	updated, err := s.Store.UpdateSubscription(ctx, sub)
	s.logSlowQuery("store.UpdateSubscription", start, err)
	return updated, err
}

func (s *guardedStore) DeleteSubscription(ctx context.Context, id int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteSubscription")
	defer cancel()
	start := time.Now()
	err := s.Store.DeleteSubscription(ctx, id)
	s.logSlowQuery("store.DeleteSubscription", start, err)
	return err
}

func (s *guardedStore) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListEnabledSubscriptions")
	defer cancel()
	start := time.Now()
	subs, err := s.Store.ListEnabledSubscriptions(ctx)
	s.logSlowQuery("store.ListEnabledSubscriptions", start, err)
	return subs, err
}

func (s *guardedStore) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	ctx, cancel := s.wrapContext(ctx, "store.IncrementSubscriptionFailures")
	defer cancel()
	start := time.Now()
	count, disabled, err := s.Store.IncrementSubscriptionFailures(ctx, id, maxFailures)
	s.logSlowQuery("store.IncrementSubscriptionFailures", start, err)
	return count, disabled, err
}

func (s *guardedStore) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.ResetSubscriptionFailures")
	defer cancel()
	start := time.Now()
	err := s.Store.ResetSubscriptionFailures(ctx, id)
	s.logSlowQuery("store.ResetSubscriptionFailures", start, err)
	return err
}

func (s *guardedStore) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	ctx, cancel := s.wrapContext(ctx, "store.RecordDeliveryAttempt")
	defer cancel()
	start := time.Now()
	attempt, err := s.Store.RecordDeliveryAttempt(ctx, a)
	s.logSlowQuery("store.RecordDeliveryAttempt", start, err)
	return attempt, err
}

func (s *guardedStore) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]DeliveryAttempt, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListDeliveryAttempts")
	defer cancel()
	start := time.Now()
	attempts, err := s.Store.ListDeliveryAttempts(ctx, subscriptionID, limit)
	s.logSlowQuery("store.ListDeliveryAttempts", start, err)
	return attempts, err
}

func (s *guardedStore) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractSpec")
	defer cancel()
	start := time.Now()
	spec, err := s.Store.GetContractSpec(ctx, wasmHash)
	s.logSlowQuery("store.GetContractSpec", start, err)
	return spec, err
}

func (s *guardedStore) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	ctx, cancel := s.wrapContext(ctx, "store.SetContractSpec")
	defer cancel()
	start := time.Now()
	err := s.Store.SetContractSpec(ctx, wasmHash, contractID, specJSON)
	s.logSlowQuery("store.SetContractSpec", start, err)
	return err
}

func (s *guardedStore) Stats(ctx context.Context) (Stats, error) {
	ctx, cancel := s.wrapContext(ctx, "store.Stats")
	defer cancel()
	start := time.Now()
	stats, err := s.Store.Stats(ctx)
	s.logSlowQuery("store.Stats", start, err)
	stats.QueryErrors = s.queryErrors.Load()
	return stats, err
}

func (s *guardedStore) Ping(ctx context.Context) error {
	ctx, cancel := s.wrapContext(ctx, "store.Ping")
	defer cancel()
	start := time.Now()
	err := s.Store.Ping(ctx)
	s.logSlowQuery("store.Ping", start, err)
	return err
}
