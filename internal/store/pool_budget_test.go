package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePoolConfigWithTLS_DefaultsAndClamping(t *testing.T) {
	cfg, err := ParsePoolConfigWithTLS("postgres://localhost:5432/sorotrail", 0, 0, 0, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, DefaultPoolMaxConns, cfg.MaxConns)
	assert.Equal(t, DefaultPoolMinConns, cfg.MinConns)
	assert.Equal(t, DefaultPoolMaxConnLifetime, cfg.MaxConnLifetime)
	assert.Equal(t, DefaultPoolMaxConnIdleTime, cfg.MaxConnIdleTime)

	clamped, err := ParsePoolConfigWithTLS("postgres://localhost:5432/sorotrail", 6, 12, 10*time.Minute, 2*time.Minute, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(6), clamped.MaxConns)
	assert.Equal(t, int32(6), clamped.MinConns, "MinConns must be clamped to MaxConns")
	assert.Equal(t, 10*time.Minute, clamped.MaxConnLifetime)
	assert.Equal(t, 2*time.Minute, clamped.MaxConnIdleTime)
}

func TestPoolBudget_IngesterReservedFloorNeverStarvedByReadFlood(t *testing.T) {
	budget := NewPoolBudget(PoolBudgetConfig{
		TotalConns:       8,
		IngesterReserved: 2, // SharedCap = 6
		APIReadMax:       4,
		GraphQLMax:       3,
		SubscriptionMax:  2,
		MaintenanceMax:   2,
		AcquireTimeout:   20 * time.Millisecond,
		RetryAfter:       2 * time.Second,
	})

	ctx := context.Background()
	var releases []func()
	defer func() {
		for _, rel := range releases {
			rel()
		}
	}()

	// Hold all 4 APIReadMax permits.
	for i := 0; i < 4; i++ {
		_, rel, err := budget.Acquire(ctx, SubsystemAPIRead)
		require.NoError(t, err)
		releases = append(releases, rel)
	}

	// A 5th APIRead request must fail fast with ErrPoolBudgetExhausted (subsystem_cap).
	start := time.Now()
	_, _, err := budget.Acquire(ctx, SubsystemAPIRead)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrPoolBudgetExhausted)
	assert.Less(t, elapsed, 250*time.Millisecond, "exhausted subsystem must fail fast rather than blocking until context deadline")

	var budgetErr *PoolBudgetExhaustedError
	require.ErrorAs(t, err, &budgetErr)
	assert.Equal(t, SubsystemAPIRead, budgetErr.Subsystem)
	assert.Equal(t, "subsystem_cap", budgetErr.Reason)
	assert.Equal(t, 4, budgetErr.Limit)
	assert.Equal(t, 2*time.Second, budgetErr.RetryAfter)

	// Fill the remaining 2 slots of SharedCap (6 total non-ingester) using GraphQL.
	for i := 0; i < 2; i++ {
		_, rel, err := budget.Acquire(ctx, SubsystemGraphQL)
		require.NoError(t, err)
		releases = append(releases, rel)
	}

	// Now SharedCap (6) is 100% saturated, even though GraphQL's own limit (3) has 1 slot left.
	// A 3rd GraphQL request must fail fast with reason="shared_cap".
	_, _, err = budget.Acquire(ctx, SubsystemGraphQL)
	require.ErrorIs(t, err, ErrPoolBudgetExhausted)
	require.ErrorAs(t, err, &budgetErr)
	assert.Equal(t, SubsystemGraphQL, budgetErr.Subsystem)
	assert.Equal(t, "shared_cap", budgetErr.Reason)
	assert.Equal(t, 6, budgetErr.Limit)

	// Meanwhile, SubsystemIngester can immediately acquire both of its 2 reserved slots!
	ingStart := time.Now()
	_, ingRel1, err := budget.Acquire(ctx, SubsystemIngester)
	require.NoError(t, err)
	releases = append(releases, ingRel1)

	_, ingRel2, err := budget.Acquire(ctx, SubsystemIngester)
	require.NoError(t, err)
	releases = append(releases, ingRel2)
	assert.Less(t, time.Since(ingStart), 20*time.Millisecond, "ingester reserved floor must be acquired immediately without waiting on read flood")

	snap := budget.Snapshot()
	assert.Equal(t, 8, snap.TotalInUse)
	assert.Equal(t, 6, snap.SharedInUse)
	assert.True(t, snap.UnderPressure)
	assert.Equal(t, 2, snap.Subsystems[SubsystemIngester].InUse)
	assert.Equal(t, uint64(0), snap.Subsystems[SubsystemIngester].Rejected)
	assert.GreaterOrEqual(t, snap.Subsystems[SubsystemAPIRead].Rejected, uint64(1))
	assert.GreaterOrEqual(t, snap.Subsystems[SubsystemGraphQL].Rejected, uint64(1))

	// Verify Prometheus metrics reflect utilization and rejections.
	assert.InDelta(t, 1.0, testutil.ToFloat64(DBPoolBudgetUtilizationRatio.WithLabelValues(string(SubsystemAPIRead))), 1e-6)
	assert.GreaterOrEqual(t, testutil.ToFloat64(DBPoolBudgetRejectionsTotal.WithLabelValues(string(SubsystemAPIRead), "subsystem_cap")), 1.0)
	assert.GreaterOrEqual(t, testutil.ToFloat64(DBPoolBudgetRejectionsTotal.WithLabelValues(string(SubsystemGraphQL), "shared_cap")), 1.0)
}

func TestPoolBudget_ReentrantContextNeverDeadlocks(t *testing.T) {
	budget := NewPoolBudget(PoolBudgetConfig{
		TotalConns:       4,
		IngesterReserved: 1,
		APIReadMax:       1, // strictly 1 permit
		AcquireTimeout:   15 * time.Millisecond,
	})

	outerCtx, releaseOuter, err := budget.Acquire(context.Background(), SubsystemAPIRead)
	require.NoError(t, err)
	defer releaseOuter()

	// Nested Acquire using outerCtx must succeed re-entrantly without consuming a 2nd permit.
	innerCtx, releaseInner, err := budget.Acquire(outerCtx, SubsystemAPIRead)
	require.NoError(t, err)
	assert.Equal(t, 1, budget.Snapshot().Subsystems[SubsystemAPIRead].InUse)
	releaseInner()
	assert.Equal(t, 1, budget.Snapshot().Subsystems[SubsystemAPIRead].InUse, "releasing inner re-entrant permit must not release outer permit early")
	_ = innerCtx
}

type fakePoolStatProvider struct {
	snap PoolConnectionSnapshot
}

func (f *fakePoolStatProvider) PoolStatsSnapshot() PoolConnectionSnapshot {
	return f.snap
}

func TestPoolBudget_MaintenanceYieldsUnderPressureAndMaxDeferral(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	budget := NewPoolBudget(PoolBudgetConfig{
		TotalConns:             8,
		IngesterReserved:       2, // SharedCap = 6
		APIReadMax:             6,
		PressureThreshold:      0.50, // >= 3 shared permits in use triggers pressure
		MaxMaintenanceDeferral: 10 * time.Minute,
		Now:                    clock,
	})

	yield, reason := budget.ShouldYield("pruner")
	assert.False(t, yield)
	assert.Empty(t, reason)

	// Acquire 3 API read permits (3/6 = 0.50 >= PressureThreshold).
	var releases []func()
	for i := 0; i < 3; i++ {
		_, rel, err := budget.Acquire(context.Background(), SubsystemAPIRead)
		require.NoError(t, err)
		releases = append(releases, rel)
	}

	yield, reason = budget.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "pool_pressure", reason)

	advance(5 * time.Minute)
	yield, reason = budget.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "pool_pressure", reason)

	// After 10m (>= MaxMaintenanceDeferral), one maintenance pass is allowed through.
	advance(5 * time.Minute)
	yield, reason = budget.ShouldYield("pruner")
	assert.False(t, yield)
	assert.Equal(t, "max_deferral_reached", reason)

	// Immediately after that pass, if pressure persists, it yields again.
	advance(1 * time.Minute)
	yield, reason = budget.ShouldYield("pruner")
	assert.True(t, yield)
	assert.Equal(t, "pool_pressure", reason)

	for _, rel := range releases {
		rel()
	}

	// Pressure cleared from budget permits; now test pgxpool stat provider pressure.
	yield, reason = budget.ShouldYield("pruner")
	assert.False(t, yield)
	assert.Empty(t, reason)

	budget.SetPoolStatProvider(&fakePoolStatProvider{
		snap: PoolConnectionSnapshot{AcquiredConns: 7, IdleConns: 1, TotalConns: 8, MaxConns: 8},
	})
	yield, reason = budget.ShouldYield("archive")
	assert.True(t, yield)
	assert.Equal(t, "pool_pressure", reason)
}

type simulatedSlowReadStore struct {
	Store
	mu        sync.Mutex
	readDelay time.Duration
}

func (s *simulatedSlowReadStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	if s.readDelay > 0 {
		select {
		case <-time.After(s.readDelay):
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.QueryEvents(ctx, f)
}

func (s *simulatedSlowReadStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.UpsertEvents(ctx, events)
}

func (s *simulatedSlowReadStore) SaveIngestionState(ctx context.Context, st IngestionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.SaveIngestionState(ctx, st)
}

func (s *simulatedSlowReadStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.GetIngestionState(ctx)
}

func (s *simulatedSlowReadStore) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.CountEvents(ctx, f)
}

func TestBudgetedStore_LoadTestReadFloodCannotStallIngestion(t *testing.T) {
	sq := &simulatedSlowReadStore{
		Store:     newSQLiteStore(t),
		readDelay: 12 * time.Millisecond,
	}

	budget := NewPoolBudget(PoolBudgetConfig{
		TotalConns:       8,
		IngesterReserved: 2, // 2 reserved for ingester, 6 shared
		APIReadMax:       4,
		GraphQLMax:       2,
		SubscriptionMax:  2,
		MaintenanceMax:   1,
		AcquireTimeout:   5 * time.Millisecond,
	})

	apiStore := NewBudgetedStore(sq, budget, SubsystemAPIRead)
	gqlStore := apiStore.ForSubsystem(SubsystemGraphQL)
	ingStore := apiStore.ForSubsystem(SubsystemIngester)

	const contractID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var readFloodRejected atomic.Uint64
	var readFloodSucceeded atomic.Uint64
	var floodWG sync.WaitGroup

	// Launch 24 concurrent read + GraphQL flood workers hammering the store.
	for i := 0; i < 24; i++ {
		floodWG.Add(1)
		go func(workerID int) {
			defer floodWG.Done()
			targetStore := apiStore
			if workerID%3 == 0 {
				targetStore = gqlStore
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, _, qErr := targetStore.QueryEvents(ctx, EventFilter{
					ContractID: contractID,
					Limit:      50,
					Scope:      WildcardScope(),
				})
				if qErr != nil {
					if errors.Is(qErr, ErrPoolBudgetExhausted) {
						readFloodRejected.Add(1)
					}
					continue
				}
				readFloodSucceeded.Add(1)
			}
		}(i)
	}

	// Run 40 consecutive ingester batches under the read flood.
	const totalBatches = 40
	var maxBatchLatency time.Duration
	for ledger := int64(1); ledger <= totalBatches; ledger++ {
		batchStart := time.Now()
		ev := Event{
			ID:               fmt.Sprintf("%019d-0000000001", ledger),
			ContractID:       contractID,
			Ledger:           ledger,
			Type:             "contract",
			TxHash:           fmt.Sprintf("tx-%d", ledger),
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`["transfer"]`),
			Value:            json.RawMessage(`{"amount":100}`),
			CreatedAt:        time.Now().UTC(),
		}
		n, uErr := ingStore.UpsertEvents(context.Background(), []Event{ev})
		require.NoError(t, uErr, "ingester UpsertEvents must never be starved or rejected by read flood")
		require.Equal(t, int64(1), n)

		sErr := ingStore.SaveIngestionState(context.Background(), IngestionState{
			LastIngestedLedger: ledger,
			UpdatedAt:          time.Now().UTC(),
		})
		require.NoError(t, sErr, "ingester SaveIngestionState must never be starved by read flood")

		if d := time.Since(batchStart); d > maxBatchLatency {
			maxBatchLatency = d
		}
		time.Sleep(1 * time.Millisecond)
	}

	cancel()
	floodWG.Wait()

	state, err := ingStore.GetIngestionState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(totalBatches), state.LastIngestedLedger)

	totalStored, err := ingStore.CountEvents(context.Background(), EventFilter{
		ContractID: contractID,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(totalBatches), totalStored)

	snap := budget.Snapshot()
	assert.Equal(t, uint64(0), snap.Subsystems[SubsystemIngester].Rejected, "ingester must experience 0 budget rejections during read flood")
	assert.Greater(t, readFloodSucceeded.Load(), uint64(0), "some read requests should succeed within their budget")
	assert.Greater(t, readFloodRejected.Load(), uint64(0), "excess read flood requests must fail fast with ErrPoolBudgetExhausted")
	assert.Less(t, maxBatchLatency, 500*time.Millisecond, "ingester batch latency must remain bounded under read flood")
}
