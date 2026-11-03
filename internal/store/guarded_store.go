package store

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/requestid"
)

type GuardedStoreOptions struct {
	Timeout            time.Duration
	SlowQueryThreshold time.Duration
	Logger             *slog.Logger
	Breaker            *StoreBreaker
	Health             *DatabaseHealth
}

type guardedStore struct {
	Store
	options     GuardedStoreOptions
	queryErrors atomic.Uint64
	breaker     *StoreBreaker
	health      *DatabaseHealth
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
	breaker := opts.Breaker
	if breaker == nil {
		if bp, ok := base.(interface{ Breaker() *StoreBreaker }); ok {
			breaker = bp.Breaker()
		}
	}
	health := opts.Health
	if health == nil {
		if hp, ok := base.(interface{ Health() *DatabaseHealth }); ok && hp.Health() != nil {
			health = hp.Health()
		} else {
			health = NewDatabaseHealth(opts.Logger)
		}
	}
	return &guardedStore{
		Store:   base,
		options: opts,
		breaker: breaker,
		health:  health,
	}
}

// Degraded implements DegradationReporter by combining the guarded store's
// circuit breaker, health tracker, and any underlying DegradationReporter.
func (s *guardedStore) Degraded() bool {
	if s == nil {
		return false
	}
	if s.breaker != nil && s.breaker.Degraded() {
		return true
	}
	if s.health != nil && s.health.Degraded() {
		return true
	}
	if rep, ok := s.Store.(DegradationReporter); ok && rep.Degraded() {
		return true
	}
	return false
}

// Breaker returns the StoreBreaker attached to this guarded store, if any.
func (s *guardedStore) Breaker() *StoreBreaker {
	if s == nil {
		return nil
	}
	return s.breaker
}

// Health returns the DatabaseHealth tracker attached to this guarded store.
func (s *guardedStore) Health() *DatabaseHealth {
	if s == nil {
		return nil
	}
	return s.health
}

func (s *guardedStore) wrapContext(ctx context.Context, name string) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, queryNameContextKey{}, name)
	return context.WithTimeout(ctx, s.options.Timeout)
}

// logSlowQuery records every wrapped operation's duration and outcome, and
// additionally logs the ones that cross the slow-query threshold. Because it
// is the single helper every method calls, instrumenting it here gives every
// Store operation a metric without duplicating recording at each call site.
func (s *guardedStore) logSlowQuery(ctx context.Context, name string, start time.Time, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
		s.queryErrors.Add(1)
	}
	if s.health != nil {
		s.health.Observe(err)
	}
	if s.breaker != nil {
		s.breaker.Observe(err)
	}
	duration := time.Since(start)
	metrics.DBOperationsTotal.WithLabelValues(name, outcome).Inc()
	metrics.DBQueryDuration.WithLabelValues(name).Observe(duration.Seconds())
	if duration < s.options.SlowQueryThreshold {
		return
	}
	// The correlation id travels on the context, so a slow query is tagged
	// with the HTTP request or background job that caused it without the
	// store interface growing a parameter for it.
	attrs := requestid.Attrs(ctx)
	attrs = append(attrs,
		"query", name,
		"duration", duration,
		"error", err,
	)
	s.options.Logger.Warn("slow store query", attrs...)
}

func (s *guardedStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.UpsertEvents")
	defer cancel()
	start := time.Now()
	n, err := s.Store.UpsertEvents(ctx, events)
	s.logSlowQuery(ctx, "store.UpsertEvents", start, err)
	return n, err
}

func (s *guardedStore) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.ReplaceEventsInRange")
	defer cancel()
	start := time.Now()
	err := s.Store.ReplaceEventsInRange(ctx, events, fromLedger, toLedger)
	s.logSlowQuery(ctx, "store.ReplaceEventsInRange", start, err)
	return err
}

func (s *guardedStore) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetEvent")
	defer cancel()
	start := time.Now()
	e, err := s.Store.GetEvent(ctx, id, sc)
	s.logSlowQuery(ctx, "store.GetEvent", start, err)
	return e, err
}

func (s *guardedStore) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetEventsByTxHash")
	defer cancel()
	start := time.Now()
	events, err := s.Store.GetEventsByTxHash(ctx, txHash, excludeID)
	s.logSlowQuery(ctx, "store.GetEventsByTxHash", start, err)
	return events, err
}

func (s *guardedStore) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	ctx, cancel := s.wrapContext(ctx, "store.EventExists")
	defer cancel()
	start := time.Now()
	exists, err := s.Store.EventExists(ctx, id, sc)
	s.logSlowQuery(ctx, "store.EventExists", start, err)
	return exists, err
}

func (s *guardedStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.QueryEvents")
	defer cancel()
	start := time.Now()
	events, cursor, err := s.Store.QueryEvents(ctx, f)
	s.logSlowQuery(ctx, "store.QueryEvents", start, err)
	return events, cursor, err
}

func (s *guardedStore) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountEvents")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountEvents(ctx, f)
	s.logSlowQuery(ctx, "store.CountEvents", start, err)
	return total, err
}

func (s *guardedStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	ctx, cancel := s.wrapContext(ctx, "store.LedgerRangeCensus")
	defer cancel()
	start := time.Now()
	census, err := s.Store.LedgerRangeCensus(ctx, fromLedger, toLedger, idsOnly)
	s.logSlowQuery(ctx, "store.LedgerRangeCensus", start, err)
	return census, err
}

func (s *guardedStore) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	ctx, cancel := s.wrapContext(ctx, "store.AggregateEvents")
	defer cancel()
	start := time.Now()
	buckets, err := s.Store.AggregateEvents(ctx, f, bucket)
	s.logSlowQuery(ctx, "store.AggregateEvents", start, err)
	return buckets, err
}

func (s *guardedStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetIngestionState")
	defer cancel()
	start := time.Now()
	state, err := s.Store.GetIngestionState(ctx)
	s.logSlowQuery(ctx, "store.GetIngestionState", start, err)
	return state, err
}

func (s *guardedStore) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetIngestionStateForNetwork")
	defer cancel()
	start := time.Now()
	var state IngestionState
	var err error
	if r, ok := s.Store.(interface {
		GetIngestionStateForNetwork(context.Context, string) (IngestionState, error)
	}); ok {
		state, err = r.GetIngestionStateForNetwork(ctx, network)
	} else if defaultNetwork(network) == "default" {
		state, err = s.Store.GetIngestionState(ctx)
	} else {
		err = ErrNotFound
	}
	s.logSlowQuery(ctx, "store.GetIngestionStateForNetwork", start, err)
	return state, err
}

func (s *guardedStore) SaveIngestionState(ctx context.Context, state IngestionState) error {
	ctx, cancel := s.wrapContext(ctx, "store.SaveIngestionState")
	defer cancel()
	start := time.Now()
	err := s.Store.SaveIngestionState(ctx, state)
	s.logSlowQuery(ctx, "store.SaveIngestionState", start, err)
	return err
}

func (s *guardedStore) GetAuditState(ctx context.Context, network string) (AuditState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetAuditState")
	defer cancel()
	start := time.Now()
	state, err := s.Store.GetAuditState(ctx, network)
	s.logSlowQuery(ctx, "store.GetAuditState", start, err)
	return state, err
}

func (s *guardedStore) SaveAuditState(ctx context.Context, state AuditState) error {
	ctx, cancel := s.wrapContext(ctx, "store.SaveAuditState")
	defer cancel()
	start := time.Now()
	err := s.Store.SaveAuditState(ctx, state)
	s.logSlowQuery(ctx, "store.SaveAuditState", start, err)
	return err
}

func (s *guardedStore) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error) {
	ctx, cancel := s.wrapContext(ctx, "store.SaveAuditStateIfGreater")
	defer cancel()
	start := time.Now()
	state, err := s.Store.SaveAuditStateIfGreater(ctx, network, ledger)
	s.logSlowQuery(ctx, "store.SaveAuditStateIfGreater", start, err)
	return state, err
}

func (s *guardedStore) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractSummary")
	defer cancel()
	start := time.Now()
	summary, err := s.Store.GetContractSummary(ctx, contractID)
	s.logSlowQuery(ctx, "store.GetContractSummary", start, err)
	return summary, err
}

func (s *guardedStore) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ContractEventTypeCounts")
	defer cancel()
	start := time.Now()
	counts, err := s.Store.ContractEventTypeCounts(ctx, contractID)
	s.logSlowQuery(ctx, "store.ContractEventTypeCounts", start, err)
	return counts, err
}

func (s *guardedStore) ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListContracts")
	defer cancel()
	start := time.Now()
	summaries, cursor, err := s.Store.ListContracts(ctx, f)
	s.logSlowQuery(ctx, "store.ListContracts", start, err)
	return summaries, cursor, err
}

func (s *guardedStore) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountContracts")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountContracts(ctx, f)
	s.logSlowQuery(ctx, "store.CountContracts", start, err)
	return total, err
}

func (s *guardedStore) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListWatchedContracts")
	defer cancel()
	start := time.Now()
	ids, err := s.Store.ListWatchedContracts(ctx)
	s.logSlowQuery(ctx, "store.ListWatchedContracts", start, err)
	return ids, err
}

func (s *guardedStore) ListWatchedContractsScoped(ctx context.Context, sc Scope) ([]WatchedContract, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListWatchedContractsScoped")
	defer cancel()
	start := time.Now()
	ids, err := s.Store.ListWatchedContractsScoped(ctx, sc)
	s.logSlowQuery(ctx, "store.ListWatchedContractsScoped", start, err)
	return ids, err
}

func (s *guardedStore) AddWatchedContract(ctx context.Context, w WatchedContract) error {
	ctx, cancel := s.wrapContext(ctx, "store.AddWatchedContract")
	defer cancel()
	start := time.Now()
	err := s.Store.AddWatchedContract(ctx, w)
	s.logSlowQuery(ctx, "store.AddWatchedContract", start, err)
	return err
}

func (s *guardedStore) RemoveWatchedContract(ctx context.Context, w WatchedContract) error {
	ctx, cancel := s.wrapContext(ctx, "store.RemoveWatchedContract")
	defer cancel()
	start := time.Now()
	err := s.Store.RemoveWatchedContract(ctx, w)
	s.logSlowQuery(ctx, "store.RemoveWatchedContract", start, err)
	return err
}

func (s *guardedStore) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	ctx, cancel := s.wrapContext(ctx, "store.RecordAuditFinding")
	defer cancel()
	start := time.Now()
	finding, err := s.Store.RecordAuditFinding(ctx, f)
	s.logSlowQuery(ctx, "store.RecordAuditFinding", start, err)
	return finding, err
}

func (s *guardedStore) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	ctx, cancel := s.wrapContext(ctx, "store.UpdateAuditFinding")
	defer cancel()
	start := time.Now()
	err := s.Store.UpdateAuditFinding(ctx, f)
	s.logSlowQuery(ctx, "store.UpdateAuditFinding", start, err)
	return err
}

func (s *guardedStore) ListOpenFindingsByRange(ctx context.Context, network string, fromLedger, toLedger int64) (AuditFinding, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListOpenFindingsByRange")
	defer cancel()
	start := time.Now()
	finding, err := s.Store.ListOpenFindingsByRange(ctx, network, fromLedger, toLedger)
	s.logSlowQuery(ctx, "store.ListOpenFindingsByRange", start, err)
	return finding, err
}

func (s *guardedStore) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CreateSubscription")
	defer cancel()
	start := time.Now()
	created, err := s.Store.CreateSubscription(ctx, sub)
	s.logSlowQuery(ctx, "store.CreateSubscription", start, err)
	return created, err
}

func (s *guardedStore) GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetSubscription")
	defer cancel()
	start := time.Now()
	sub, err := s.Store.GetSubscription(ctx, id, owner)
	s.logSlowQuery(ctx, "store.GetSubscription", start, err)
	return sub, err
}

func (s *guardedStore) ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListSubscriptions")
	defer cancel()
	start := time.Now()
	subs, err := s.Store.ListSubscriptions(ctx, owner)
	s.logSlowQuery(ctx, "store.ListSubscriptions", start, err)
	return subs, err
}

func (s *guardedStore) UpdateSubscription(ctx context.Context, sub Subscription, owner SubscriptionOwner) (Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.UpdateSubscription")
	defer cancel()
	start := time.Now()
	updated, err := s.Store.UpdateSubscription(ctx, sub, owner)
	s.logSlowQuery(ctx, "store.UpdateSubscription", start, err)
	return updated, err
}

func (s *guardedStore) DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteSubscription")
	defer cancel()
	start := time.Now()
	err := s.Store.DeleteSubscription(ctx, id, owner)
	s.logSlowQuery(ctx, "store.DeleteSubscription", start, err)
	return err
}

func (s *guardedStore) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListEnabledSubscriptions")
	defer cancel()
	start := time.Now()
	subs, err := s.Store.ListEnabledSubscriptions(ctx)
	s.logSlowQuery(ctx, "store.ListEnabledSubscriptions", start, err)
	return subs, err
}

func (s *guardedStore) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	ctx, cancel := s.wrapContext(ctx, "store.IncrementSubscriptionFailures")
	defer cancel()
	start := time.Now()
	count, disabled, err := s.Store.IncrementSubscriptionFailures(ctx, id, maxFailures)
	s.logSlowQuery(ctx, "store.IncrementSubscriptionFailures", start, err)
	return count, disabled, err
}

func (s *guardedStore) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.ResetSubscriptionFailures")
	defer cancel()
	start := time.Now()
	err := s.Store.ResetSubscriptionFailures(ctx, id)
	s.logSlowQuery(ctx, "store.ResetSubscriptionFailures", start, err)
	return err
}

func (s *guardedStore) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	ctx, cancel := s.wrapContext(ctx, "store.RecordDeliveryAttempt")
	defer cancel()
	start := time.Now()
	attempt, err := s.Store.RecordDeliveryAttempt(ctx, a)
	s.logSlowQuery(ctx, "store.RecordDeliveryAttempt", start, err)
	return attempt, err
}

func (s *guardedStore) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListDeliveryAttempts")
	defer cancel()
	start := time.Now()
	attempts, err := s.Store.ListDeliveryAttempts(ctx, subscriptionID, limit, owner)
	s.logSlowQuery(ctx, "store.ListDeliveryAttempts", start, err)
	return attempts, err
}

func (s *guardedStore) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractSpec")
	defer cancel()
	start := time.Now()
	spec, err := s.Store.GetContractSpec(ctx, wasmHash)
	s.logSlowQuery(ctx, "store.GetContractSpec", start, err)
	return spec, err
}

func (s *guardedStore) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	ctx, cancel := s.wrapContext(ctx, "store.SetContractSpec")
	defer cancel()
	start := time.Now()
	err := s.Store.SetContractSpec(ctx, wasmHash, contractID, specJSON)
	s.logSlowQuery(ctx, "store.SetContractSpec", start, err)
	return err
}

func (s *guardedStore) GetContractSpecOverride(ctx context.Context, contractID string) ([]byte, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractSpecOverride")
	defer cancel()
	start := time.Now()
	spec, err := s.Store.GetContractSpecOverride(ctx, contractID)
	s.logSlowQuery(ctx, "store.GetContractSpecOverride", start, err)
	return spec, err
}

func (s *guardedStore) SetContractSpecOverride(ctx context.Context, contractID string, specJSON []byte) error {
	ctx, cancel := s.wrapContext(ctx, "store.SetContractSpecOverride")
	defer cancel()
	start := time.Now()
	err := s.Store.SetContractSpecOverride(ctx, contractID, specJSON)
	s.logSlowQuery(ctx, "store.SetContractSpecOverride", start, err)
	return err
}

func (s *guardedStore) DeleteContractSpecOverride(ctx context.Context, contractID string) error {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteContractSpecOverride")
	defer cancel()
	start := time.Now()
	err := s.Store.DeleteContractSpecOverride(ctx, contractID)
	s.logSlowQuery(ctx, "store.DeleteContractSpecOverride", start, err)
	return err
}

func (s *guardedStore) DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteEventsBeforeLedger")
	defer cancel()
	start := time.Now()
	n, err := s.Store.DeleteEventsBeforeLedger(ctx, beforeLedger)
	s.logSlowQuery(ctx, "store.DeleteEventsBeforeLedger", start, err)
	return n, err
}

func (s *guardedStore) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteEventsBefore")
	defer cancel()
	start := time.Now()
	n, err := s.Store.DeleteEventsBefore(ctx, maxLedger, beforeTime, limit)
	s.logSlowQuery(ctx, "store.DeleteEventsBefore", start, err)
	return n, err
}

func (s *guardedStore) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountEventsBefore")
	defer cancel()
	start := time.Now()
	n, err := s.Store.CountEventsBefore(ctx, maxLedger, beforeTime, limit)
	s.logSlowQuery(ctx, "store.CountEventsBefore", start, err)
	return n, err
}
func (s *guardedStore) MigrationVersion(ctx context.Context) (int, bool, error) {
	// Migration version queries are cheap — no timeout needed.
	return s.Store.MigrationVersion(ctx)
}

func (s *guardedStore) Stats(ctx context.Context, sc Scope) (Stats, error) {
	ctx, cancel := s.wrapContext(ctx, "store.Stats")
	defer cancel()
	start := time.Now()
	stats, err := s.Store.Stats(ctx, sc)
	s.logSlowQuery(ctx, "store.Stats", start, err)
	stats.QueryErrors = s.queryErrors.Load()
	return stats, err
}

func (s *guardedStore) Ping(ctx context.Context) error {
	ctx, cancel := s.wrapContext(ctx, "store.Ping")
	defer cancel()
	start := time.Now()
	err := s.Store.Ping(ctx)
	s.logSlowQuery(ctx, "store.Ping", start, err)
	return err
}

func (s *guardedStore) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	ctx, cancel := s.wrapContext(ctx, "store.UpsertAddressRefs")
	defer cancel()
	start := time.Now()
	err := s.Store.UpsertAddressRefs(ctx, refs)
	s.logSlowQuery(ctx, "store.UpsertAddressRefs", start, err)
	return err
}

func (s *guardedStore) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.QueryAddressEvents")
	defer cancel()
	start := time.Now()
	events, cursor, err := s.Store.QueryAddressEvents(ctx, address, f)
	s.logSlowQuery(ctx, "store.QueryAddressEvents", start, err)
	return events, cursor, err
}

func (s *guardedStore) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountAddressEvents")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountAddressEvents(ctx, address)
	s.logSlowQuery(ctx, "store.CountAddressEvents", start, err)
	return total, err
}

func (s *guardedStore) CountDeadLetters(ctx context.Context, contractID string) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountDeadLetters")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountDeadLetters(ctx, contractID)
	s.logSlowQuery(ctx, "store.CountDeadLetters", start, err)
	return total, err
}

func (s *guardedStore) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, owner SubscriptionOwner) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountDeliveryAttempts")
	defer cancel()
	start := time.Now()
	total, err := s.Store.CountDeliveryAttempts(ctx, subscriptionID, owner)
	s.logSlowQuery(ctx, "store.CountDeliveryAttempts", start, err)
	return total, err
}

func (s *guardedStore) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetAddressSummary")
	defer cancel()
	start := time.Now()
	summary, err := s.Store.GetAddressSummary(ctx, address)
	s.logSlowQuery(ctx, "store.GetAddressSummary", start, err)
	return summary, err
}

func (s *guardedStore) DeadLetterEvent(ctx context.Context, in DeadLetterInput) (DeadLetter, error) {
	ctx, cancel := s.wrapContext(ctx, "store.DeadLetterEvent")
	defer cancel()
	start := time.Now()
	d, err := s.Store.DeadLetterEvent(ctx, in)
	s.logSlowQuery(ctx, "store.DeadLetterEvent", start, err)
	return d, err
}

func (s *guardedStore) ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListDeadLetters")
	defer cancel()
	start := time.Now()
	letters, next, err := s.Store.ListDeadLetters(ctx, contractID, limit, cursor)
	s.logSlowQuery(ctx, "store.ListDeadLetters", start, err)
	return letters, next, err
}

func (s *guardedStore) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetDeadLetter")
	defer cancel()
	start := time.Now()
	d, err := s.Store.GetDeadLetter(ctx, id)
	s.logSlowQuery(ctx, "store.GetDeadLetter", start, err)
	return d, err
}

func (s *guardedStore) DeleteDeadLetter(ctx context.Context, id int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteDeadLetter")
	defer cancel()
	start := time.Now()
	err := s.Store.DeleteDeadLetter(ctx, id)
	s.logSlowQuery(ctx, "store.DeleteDeadLetter", start, err)
	return err
}

func (s *guardedStore) GetContractCursor(ctx context.Context, contractID string) (ContractCursor, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractCursor")
	defer cancel()
	start := time.Now()
	c, err := s.Store.GetContractCursor(ctx, contractID)
	s.logSlowQuery(ctx, "store.GetContractCursor", start, err)
	return c, err
}

func (s *guardedStore) SaveContractCursor(ctx context.Context, c ContractCursor) error {
	ctx, cancel := s.wrapContext(ctx, "store.SaveContractCursor")
	defer cancel()
	start := time.Now()
	err := s.Store.SaveContractCursor(ctx, c)
	s.logSlowQuery(ctx, "store.SaveContractCursor", start, err)
	return err
}

func (s *guardedStore) DeleteContractCursor(ctx context.Context, contractID string) error {
	ctx, cancel := s.wrapContext(ctx, "store.DeleteContractCursor")
	defer cancel()
	start := time.Now()
	err := s.Store.DeleteContractCursor(ctx, contractID)
	s.logSlowQuery(ctx, "store.DeleteContractCursor", start, err)
	return err
}

func (s *guardedStore) ListContractCursors(ctx context.Context) ([]ContractCursor, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListContractCursors")
	defer cancel()
	start := time.Now()
	cursors, err := s.Store.ListContractCursors(ctx)
	s.logSlowQuery(ctx, "store.ListContractCursors", start, err)
	return cursors, err
}

func (s *guardedStore) CreateAPIKey(ctx context.Context, k APIKey) (APIKey, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CreateAPIKey")
	defer cancel()
	start := time.Now()
	key, err := s.Store.CreateAPIKey(ctx, k)
	s.logSlowQuery(ctx, "store.CreateAPIKey", start, err)
	return key, err
}

func (s *guardedStore) GetAPIKey(ctx context.Context, id int64) (APIKey, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetAPIKey")
	defer cancel()
	start := time.Now()
	key, err := s.Store.GetAPIKey(ctx, id)
	s.logSlowQuery(ctx, "store.GetAPIKey", start, err)
	return key, err
}

func (s *guardedStore) LookupAPIKeyByPrefix(ctx context.Context, prefix string) (APIKey, error) {
	ctx, cancel := s.wrapContext(ctx, "store.LookupAPIKeyByPrefix")
	defer cancel()
	start := time.Now()
	key, err := s.Store.LookupAPIKeyByPrefix(ctx, prefix)
	s.logSlowQuery(ctx, "store.LookupAPIKeyByPrefix", start, err)
	return key, err
}

func (s *guardedStore) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListAPIKeys")
	defer cancel()
	start := time.Now()
	keys, err := s.Store.ListAPIKeys(ctx)
	s.logSlowQuery(ctx, "store.ListAPIKeys", start, err)
	return keys, err
}

func (s *guardedStore) RevokeAPIKey(ctx context.Context, id int64) error {
	ctx, cancel := s.wrapContext(ctx, "store.RevokeAPIKey")
	defer cancel()
	start := time.Now()
	err := s.Store.RevokeAPIKey(ctx, id)
	s.logSlowQuery(ctx, "store.RevokeAPIKey", start, err)
	return err
}

func (s *guardedStore) ListContractIDs(ctx context.Context) ([]string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListContractIDs")
	defer cancel()
	start := time.Now()
	ids, err := s.Store.ListContractIDs(ctx)
	s.logSlowQuery(ctx, "store.ListContractIDs", start, err)
	return ids, err
}

func (s *guardedStore) GetContractMeta(ctx context.Context, contractID string) (ContractMeta, error) {
	ctx, cancel := s.wrapContext(ctx, "store.GetContractMeta")
	defer cancel()
	start := time.Now()
	meta, err := s.Store.GetContractMeta(ctx, contractID)
	s.logSlowQuery(ctx, "store.GetContractMeta", start, err)
	return meta, err
}

func (s *guardedStore) UpsertContractMeta(ctx context.Context, m ContractMeta) error {
	ctx, cancel := s.wrapContext(ctx, "store.UpsertContractMeta")
	defer cancel()
	start := time.Now()
	err := s.Store.UpsertContractMeta(ctx, m)
	s.logSlowQuery(ctx, "store.UpsertContractMeta", start, err)
	return err
}

func (s *guardedStore) CountContractEvents(ctx context.Context, contractID string) (int64, error) {
	ctx, cancel := s.wrapContext(ctx, "store.CountContractEvents")
	defer cancel()
	start := time.Now()
	n, err := s.Store.CountContractEvents(ctx, contractID)
	s.logSlowQuery(ctx, "store.CountContractEvents", start, err)
	return n, err
}

func (s *guardedStore) ListContractsNeedingRefresh(ctx context.Context, olderThan time.Time) ([]string, error) {
	ctx, cancel := s.wrapContext(ctx, "store.ListContractsNeedingRefresh")
	defer cancel()
	start := time.Now()
	ids, err := s.Store.ListContractsNeedingRefresh(ctx, olderThan)
	s.logSlowQuery(ctx, "store.ListContractsNeedingRefresh", start, err)
	return ids, err
}
