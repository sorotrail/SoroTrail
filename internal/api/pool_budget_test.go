package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestPoolBudgetExhausted_Returns503WithRetryAfterAcrossEndpoints(t *testing.T) {
	baseStore := &stubStore{}
	budget := store.NewPoolBudget(store.PoolBudgetConfig{
		TotalConns:       4,
		IngesterReserved: 1,
		APIReadMax:       1,
		AcquireTimeout:   10 * time.Millisecond,
		RetryAfter:       2 * time.Second,
	})
	budgeted := store.NewBudgetedStore(baseStore, budget, store.SubsystemAPIRead)

	// Hold the single APIRead permit so subsequent API read calls hit ErrPoolBudgetExhausted.
	_, releaseHeld, err := budget.Acquire(context.Background(), store.SubsystemAPIRead)
	require.NoError(t, err)
	defer releaseHeld()

	srv := New(budgeted, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	handler := srv.Router()

	paths := []string{
		"/events",
		"/events?stream=true",
		"/events/count",
		"/events/aggregate?bucket=ledger",
		"/events/0000000000000000001-0000000001",
		"/contracts",
		"/stats",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			start := time.Now()
			handler.ServeHTTP(rec, req)
			elapsed := time.Since(start)

			assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "exhausted pool budget must return 503 Service Unavailable")
			assert.Equal(t, "2", rec.Header().Get("Retry-After"), "503 response must include Retry-After header")
			assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			assert.Less(t, elapsed, 250*time.Millisecond, "request must fail fast rather than waiting for context deadline")

			var body errorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, CodeServiceUnavailable, body.Code)
			assert.Contains(t, body.Error, "pool budget exhausted")
		})
	}
}

type concurrentSlowStubStore struct {
	*stubStore
	mu        sync.Mutex
	readDelay time.Duration
}

func (c *concurrentSlowStubStore) QueryEvents(ctx context.Context, f store.EventFilter) ([]store.Event, string, error) {
	if c.readDelay > 0 {
		select {
		case <-time.After(c.readDelay):
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stubStore.QueryEvents(ctx, f)
}

func (c *concurrentSlowStubStore) CountEvents(ctx context.Context, f store.EventFilter) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stubStore.CountEvents(ctx, f)
}

func (c *concurrentSlowStubStore) GetIngestionState(ctx context.Context) (store.IngestionState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stubStore.GetIngestionState(ctx)
}

func (c *concurrentSlowStubStore) UpsertEvents(ctx context.Context, events []store.Event) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stubStore.UpsertEvents(ctx, events)
}

func (c *concurrentSlowStubStore) SaveIngestionState(ctx context.Context, s store.IngestionState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stubStore.SaveIngestionState(ctx, s)
}

func TestPoolBudget_HTTPReadFloodCannotStallIngester(t *testing.T) {
	sq := &concurrentSlowStubStore{
		stubStore: &stubStore{},
		readDelay: 12 * time.Millisecond,
	}

	budget := store.NewPoolBudget(store.PoolBudgetConfig{
		TotalConns:       6,
		IngesterReserved: 2, // 2 reserved for ingester, 4 shared for reads
		APIReadMax:       2,
		AcquireTimeout:   5 * time.Millisecond,
		RetryAfter:       1 * time.Second,
	})

	apiStore := store.NewBudgetedStore(sq, budget, store.SubsystemAPIRead)
	ingStore := apiStore.ForSubsystem(store.SubsystemIngester)

	srv := New(apiStore, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var http200, http503 atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := ts.Client()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?limit=20", nil)
				if reqErr != nil {
					return
				}
				resp, doErr := client.Do(req)
				if doErr != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					http200.Add(1)
				case http.StatusServiceUnavailable:
					if resp.Header.Get("Retry-After") == "1" {
						http503.Add(1)
					}
				}
			}
		}()
	}

	const contractID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	for ledger := int64(1); ledger <= 30; ledger++ {
		n, uErr := ingStore.UpsertEvents(context.Background(), []store.Event{
			{
				ID:               "0000000000000000001-0000000001",
				ContractID:       contractID,
				Ledger:           ledger,
				Type:             "contract",
				TxHash:           "tx-flood",
				InSuccessfulCall: true,
				Topics:           json.RawMessage(`["transfer"]`),
				Value:            json.RawMessage(`{"v":1}`),
				CreatedAt:        time.Now().UTC(),
			},
		})
		require.NoError(t, uErr)
		assert.GreaterOrEqual(t, n, int64(0))
		require.NoError(t, ingStore.SaveIngestionState(context.Background(), store.IngestionState{
			LastIngestedLedger: ledger,
			UpdatedAt:          time.Now().UTC(),
		}))
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	snap := budget.Snapshot()
	assert.Equal(t, uint64(0), snap.Subsystems[store.SubsystemIngester].Rejected, "ingester must never be rejected during HTTP read flood")
	assert.Greater(t, http200.Load(), uint64(0), "within-budget HTTP reads must succeed")
	assert.Greater(t, http503.Load(), uint64(0), "excess HTTP read flood requests must fail fast with 503 + Retry-After")
}
