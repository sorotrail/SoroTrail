package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// DefaultMaxReplicaLag is the default replication lag ceiling beyond which
// ReplicaRouter falls back to the primary Store (#1222).
const DefaultMaxReplicaLag = 30 * time.Second

// ReplicaSafeMethods returns the explicit, reviewed allowlist of Store methods
// that ReplicaRouter may serve from a Postgres read replica, along with the
// reason each is safe under bounded replication lag (#1222).
//
// Every method not in this map defaults to the primary Store:
//   - GetIngestionState / SaveIngestionState / GetContractCursor /
//     ListContractCursors / SaveContractCursor: the ingester's resume points;
//     reading a lagging replica would cause the ingester to re-scan or skip
//     ledgers.
//   - GetBackfillState / SaveBackfillState / ListBackfillRanges: backfill
//     progress checkpoints.
//   - GetAuditState / SaveAuditState / ListOpenFindingsByRange: gap-audit
//     progress and findings.
//   - Subscriptions, API keys, tenant CRUD, spec overrides, and dead letters:
//     immediate read-after-write management surfaces.
//   - Ping: readiness probes follow the primary only so a replica outage
//     degrades read capacity rather than taking the whole instance out of
//     load-balancer rotation.
func ReplicaSafeMethods() map[string]string {
	return map[string]string{
		"GetEvent":                "single-event lookup by immutable event ID (falls back to primary on ErrNotFound in case the event was just ingested)",
		"GetEventsByTxHash":       "transaction sibling event lookup",
		"EventExists":             "immutable event existence check (falls back to primary when false on the replica)",
		"QueryEvents":             "event search and pagination (falls back to primary when a cursor's ledger exceeds the replica's last ingested ledger)",
		"CountEvents":             "event count for X-Total-Count and /events/count",
		"AggregateEvents":         "event bucket aggregation for /events/aggregate",
		"GetContractSummary":      "per-contract event summary (falls back to primary on ErrNotFound)",
		"ContractEventTypeCounts": "per-contract event type breakdown",
		"ListContracts":           "paginated contract inventory",
		"CountContracts":          "contract inventory count",
		"Stats":                   "headline /stats counters",
		"QueryAddressEvents":      "address activity event search (with cursor ledger guard)",
		"CountAddressEvents":      "address activity total count",
		"GetAddressSummary":       "address activity summary (falls back to primary on ErrNotFound)",
		"GetContractMeta":         "cached SEP-41 token metadata lookup",
		"CountContractEvents":     "per-contract event count",
	}
}

// ReplicaRouterOptions configures a ReplicaRouter.
type ReplicaRouterOptions struct {
	// MaxReplicaLag bounds how far behind the primary the read replica may
	// drift before routed reads fall back to the primary. Zero defaults to
	// DefaultMaxReplicaLag (30s).
	MaxReplicaLag time.Duration
	// ReplicaPool is the optional underlying *pgxpool.Pool for sampling
	// pg_last_xact_replay_timestamp().
	ReplicaPool *pgxpool.Pool
}

// ReplicaRouter wraps a primary Store and an optional read-replica Store,
// routing only the explicitly allowlisted read methods in ReplicaSafeMethods()
// to the replica when it is healthy and within MaxReplicaLag (#1222).
//
// When replica is nil, every method delegates directly to primary with zero
// overhead, preserving exact pre-replica behaviour.
type ReplicaRouter struct {
	Store         // embeds primary Store so every non-overridden method stays on primary
	primary       Store
	replica       Store
	replicaPool   *pgxpool.Pool
	maxReplicaLag time.Duration
	lagNanos      atomic.Int64
}

// NewReplicaRouter constructs a ReplicaRouter over primary and replica. If
// replica is nil, primary is returned directly (or wrapped with nil replica)
// so unset REPLICA_DATABASE_URL behaves identically to a single-pool store.
func NewReplicaRouter(primary, replica Store, opts ReplicaRouterOptions) *ReplicaRouter {
	maxLag := opts.MaxReplicaLag
	if maxLag <= 0 {
		maxLag = DefaultMaxReplicaLag
	}
	return &ReplicaRouter{
		Store:         primary,
		primary:       primary,
		replica:       replica,
		replicaPool:   opts.ReplicaPool,
		maxReplicaLag: maxLag,
	}
}

// SampleReplicaLag queries pg_last_xact_replay_timestamp() on the replica pool
// (when provided) and updates ObservedReplicaLag and
// sorotrail_db_replica_lag_seconds.
func (r *ReplicaRouter) SampleReplicaLag(ctx context.Context) (time.Duration, error) {
	if r == nil || r.replica == nil || r.replicaPool == nil {
		r.SetObservedReplicaLag(0)
		return 0, nil
	}
	var lagSeconds float64
	err := r.replicaPool.QueryRow(ctx,
		`SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)`).Scan(&lagSeconds)
	if err != nil {
		return 0, fmt.Errorf("sampling replica lag: %w", err)
	}
	if lagSeconds < 0 {
		lagSeconds = 0
	}
	d := time.Duration(lagSeconds * float64(time.Second))
	r.SetObservedReplicaLag(d)
	return d, nil
}

// SetObservedReplicaLag records the observed replication lag and updates the
// Prometheus gauge.
func (r *ReplicaRouter) SetObservedReplicaLag(d time.Duration) {
	if r == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	r.lagNanos.Store(int64(d))
	metrics.DBReplicaLagSeconds.Set(d.Seconds())
}

// ObservedReplicaLag returns the latest recorded replication lag.
func (r *ReplicaRouter) ObservedReplicaLag() time.Duration {
	if r == nil {
		return 0
	}
	return time.Duration(r.lagNanos.Load())
}

// MaxReplicaLag returns the configured replication lag ceiling.
func (r *ReplicaRouter) MaxReplicaLag() time.Duration {
	if r == nil || r.maxReplicaLag <= 0 {
		return DefaultMaxReplicaLag
	}
	return r.maxReplicaLag
}

func (r *ReplicaRouter) useReplica() bool {
	if r == nil || r.replica == nil {
		return false
	}
	if r.ObservedReplicaLag() > r.MaxReplicaLag() {
		metrics.DBReplicaFallbacksTotal.Inc()
		return false
	}
	return true
}

func isReplicaConnectionFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidCursor) {
		return false
	}
	var pgErr *pgconn.PgError
	return !errors.As(err, &pgErr)
}

// cursorAheadOfReplica checks whether an event cursor or filter ledger bound
// references a ledger newer than the replica's own ingestion state. When a
// cursor was minted against the primary for a ledger the replica has not yet
// replayed, resolving the page on the replica could skip or truncate rows, so
// the router falls back to the primary.
func (r *ReplicaRouter) cursorAheadOfReplica(ctx context.Context, f EventFilter) bool {
	if r.replica == nil {
		return false
	}
	var minRequiredLedger int64
	if f.ToLedger > minRequiredLedger {
		minRequiredLedger = f.ToLedger
	}
	if f.Cursor != "" {
		if sv, id, err := decodeCompositeCursor(f.Cursor); err == nil {
			if l, parseErr := strconv.ParseInt(sv, 10, 64); parseErr == nil && l > minRequiredLedger {
				minRequiredLedger = l
			} else if l, ok := CursorLedger(id); ok && l > minRequiredLedger {
				minRequiredLedger = l
			} else if l, _, scanErr := parseEventIDLedger(id); scanErr == nil && l > minRequiredLedger {
				minRequiredLedger = l
			}
		} else if l, ok := CursorLedger(f.Cursor); ok && l > minRequiredLedger {
			minRequiredLedger = l
		} else if l, _, scanErr := parseEventIDLedger(f.Cursor); scanErr == nil && l > minRequiredLedger {
			minRequiredLedger = l
		}
	}
	if minRequiredLedger <= 0 {
		return false
	}
	state, err := r.replica.GetIngestionState(ctx)
	if err != nil {
		return true
	}
	return state.LastIngestedLedger < minRequiredLedger
}

func parseEventIDLedger(raw string) (int64, int64, error) {
	var ledger, seq int64
	if _, err := fmt.Sscanf(raw, "%d-%d", &ledger, &seq); err != nil {
		return 0, 0, err
	}
	return ledger, seq, nil
}

// Ping checks only the primary store so losing a read replica degrades read
// capacity without failing /readyz (#1222).
func (r *ReplicaRouter) Ping(ctx context.Context) error {
	return r.primary.Ping(ctx)
}

func (r *ReplicaRouter) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	if !r.useReplica() {
		return r.primary.GetEvent(ctx, id, sc)
	}
	ev, err := r.replica.GetEvent(ctx, id, sc)
	if err == nil {
		return ev, nil
	}
	// If the replica is slightly behind and reports ErrNotFound (or a
	// connection failure), check the primary so a newly ingested event is
	// still found.
	if errors.Is(err, ErrNotFound) || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.GetEvent(ctx, id, sc)
	}
	return Event{}, err
}

func (r *ReplicaRouter) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	if !r.useReplica() {
		return r.primary.GetEventsByTxHash(ctx, txHash, excludeID)
	}
	evs, err := r.replica.GetEventsByTxHash(ctx, txHash, excludeID)
	if err == nil && len(evs) > 0 {
		return evs, nil
	}
	if err == nil || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.GetEventsByTxHash(ctx, txHash, excludeID)
	}
	return nil, err
}

func (r *ReplicaRouter) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	if !r.useReplica() {
		return r.primary.EventExists(ctx, id, sc)
	}
	exists, err := r.replica.EventExists(ctx, id, sc)
	if err == nil && exists {
		return true, nil
	}
	if err == nil || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.EventExists(ctx, id, sc)
	}
	return false, err
}

func (r *ReplicaRouter) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	if !r.useReplica() {
		return r.primary.QueryEvents(ctx, f)
	}
	if r.cursorAheadOfReplica(ctx, f) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.QueryEvents(ctx, f)
	}
	evs, next, err := r.replica.QueryEvents(ctx, f)
	if err == nil {
		return evs, next, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.QueryEvents(ctx, f)
	}
	return nil, "", err
}

func (r *ReplicaRouter) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	if !r.useReplica() {
		return r.primary.CountEvents(ctx, f)
	}
	n, err := r.replica.CountEvents(ctx, f)
	if err == nil {
		return n, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.CountEvents(ctx, f)
	}
	return 0, err
}

func (r *ReplicaRouter) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	if !r.useReplica() {
		return r.primary.AggregateEvents(ctx, f, bucket)
	}
	b, err := r.replica.AggregateEvents(ctx, f, bucket)
	if err == nil {
		return b, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.AggregateEvents(ctx, f, bucket)
	}
	return nil, err
}

func (r *ReplicaRouter) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	if !r.useReplica() {
		return r.primary.GetContractSummary(ctx, contractID)
	}
	cs, err := r.replica.GetContractSummary(ctx, contractID)
	if err == nil {
		return cs, nil
	}
	if errors.Is(err, ErrNotFound) || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.GetContractSummary(ctx, contractID)
	}
	return ContractSummary{}, err
}

func (r *ReplicaRouter) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	if !r.useReplica() {
		return r.primary.ContractEventTypeCounts(ctx, contractID)
	}
	counts, err := r.replica.ContractEventTypeCounts(ctx, contractID)
	if err == nil {
		return counts, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.ContractEventTypeCounts(ctx, contractID)
	}
	return nil, err
}

func (r *ReplicaRouter) ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	if !r.useReplica() {
		return r.primary.ListContracts(ctx, f)
	}
	list, next, err := r.replica.ListContracts(ctx, f)
	if err == nil {
		return list, next, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.ListContracts(ctx, f)
	}
	return nil, "", err
}

func (r *ReplicaRouter) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	if !r.useReplica() {
		return r.primary.CountContracts(ctx, f)
	}
	n, err := r.replica.CountContracts(ctx, f)
	if err == nil {
		return n, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.CountContracts(ctx, f)
	}
	return 0, err
}

func (r *ReplicaRouter) Stats(ctx context.Context, sc Scope) (Stats, error) {
	if !r.useReplica() {
		return r.primary.Stats(ctx, sc)
	}
	st, err := r.replica.Stats(ctx, sc)
	if err == nil {
		return st, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.Stats(ctx, sc)
	}
	return Stats{}, err
}

func (r *ReplicaRouter) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	if !r.useReplica() {
		return r.primary.QueryAddressEvents(ctx, address, f)
	}
	if r.cursorAheadOfReplica(ctx, f) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.QueryAddressEvents(ctx, address, f)
	}
	evs, next, err := r.replica.QueryAddressEvents(ctx, address, f)
	if err == nil {
		return evs, next, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.QueryAddressEvents(ctx, address, f)
	}
	return nil, "", err
}

func (r *ReplicaRouter) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	if !r.useReplica() {
		return r.primary.CountAddressEvents(ctx, address)
	}
	n, err := r.replica.CountAddressEvents(ctx, address)
	if err == nil {
		return n, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.CountAddressEvents(ctx, address)
	}
	return 0, err
}

func (r *ReplicaRouter) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	if !r.useReplica() {
		return r.primary.GetAddressSummary(ctx, address)
	}
	sum, err := r.replica.GetAddressSummary(ctx, address)
	if err == nil {
		return sum, nil
	}
	if errors.Is(err, ErrNotFound) || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.GetAddressSummary(ctx, address)
	}
	return AddressSummary{}, err
}

func (r *ReplicaRouter) GetContractMeta(ctx context.Context, contractID string) (ContractMeta, error) {
	if !r.useReplica() {
		return r.primary.GetContractMeta(ctx, contractID)
	}
	meta, err := r.replica.GetContractMeta(ctx, contractID)
	if err == nil {
		return meta, nil
	}
	if errors.Is(err, ErrNotFound) || isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.GetContractMeta(ctx, contractID)
	}
	return ContractMeta{}, err
}

func (r *ReplicaRouter) CountContractEvents(ctx context.Context, contractID string) (int64, error) {
	if !r.useReplica() {
		return r.primary.CountContractEvents(ctx, contractID)
	}
	n, err := r.replica.CountContractEvents(ctx, contractID)
	if err == nil {
		return n, nil
	}
	if isReplicaConnectionFailure(err) {
		metrics.DBReplicaFallbacksTotal.Inc()
		return r.primary.CountContractEvents(ctx, contractID)
	}
	return 0, err
}
