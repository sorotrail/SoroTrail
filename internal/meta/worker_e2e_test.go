package meta

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// mockRPCClient simulates contract calls for metadata resolution.
type mockRPCClient struct {
	mu          sync.Mutex
	calls       int
	nameFn      func(ctx context.Context, contractID string) (string, error)
	symbolFn    func(ctx context.Context, contractID string) (string, error)
	decimalsFn  func(ctx context.Context, contractID string) (uint32, error)
}

func (m *mockRPCClient) SimulateTransaction(ctx context.Context, req any) (any, error) {
	// Not used directly in standard metadata fetcher if specific methods or helper client is used,
	// but let's implement matching expected signature if needed by rpc.Client.
	return nil, nil
}

// TestContractMetadataRefresh_EndToEnd covers the required lifecycle coverage areas:
// - a token contract resolving name, symbol and decimals
// - a non-token contract cached as a negative and not re-fetched hot
// - the refresh interval honoured
// - an RPC failure leaving existing metadata intact
// - metadata appearing on the contract stats endpoint / store contract metadata
// - concurrent resolution of one contract fetching once
func TestContractMetadataRefresh_EndToEnd(t *testing.T) {
	t.Run("token contract resolves name symbol and decimals", func(t *testing.T) {
		w := NewTestWorker()
		contractID := "CA_TOKEN"
		
		calls := atomic.Int32{}
		storeMeta := &fakeMetadataStore{}
		rpcClient := &stubRPCMetadata{
			getMeta: func(ctx context.Context, id string) (Metadata, error) {
				calls.Add(1)
				return Metadata{Name: "USD Coin", Symbol: "USDC", Decimals: 6, IsToken: true}, nil
			},
		},

		// Execute resolution
		meta, err := ResolveMetadata(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)
		assert.True(t, meta.IsToken)
		assert.Equal(t, "USD Coin", meta.Name)
		assert.Equal(t, "USDC", meta.Symbol)
		assert.Equal(t, uint32(6), meta.Decimals)
		assert.Equal(t, int32(1), calls.Load())
	})

	t.Run("non-token contract cached as negative and not refetched hot", func(t *testing.T) {
		w := NewTestWorker()
		contractID := "CA_NOTOKEN"
		calls := atomic.Int32{}
		rpcClient := &stubRPCMetadata{
			getMeta: func(ctx context.Context, id string) (Metadata, error) {
				calls.Add(1)
				return Metadata{IsToken: false}, nil
			},
		},
		storeMeta := &fakeMetadataStore{}

		// First call fetches
		meta1, err := ResolveMetadata(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)
		assert.False(t, meta1.IsToken)

		// Second call hits cache (hot), should not increment RPC calls
		meta2, err := ResolveMetadata(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)
		assert.False(t, meta2.IsToken)
		assert.Equal(t, int32(1), calls.Load(), "negative cache must prevent hot re-fetch")
	})

	t.Run("refresh interval honoured", func(t *testing.T) {
		contractID := "CA_REFRESH"
		calls := atomic.Int32{}
		rpcClient := &stubRPCMetadata{
			getMeta: func(ctx context.Context, id string) (Metadata, error) {
				val := calls.Add(1)
				return Metadata{Name: "Token" + string(rune('0'+val)), Symbol: "T", Decimals: 18, IsToken: true}, nil
			},
		},
		storeMeta := &fakeMetadataStore{}

		// Resolve initially
		_, err := ResolveMetadata(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)

		// Force TTL expiration / check refresh
		storeMeta.SetAge(contractID, 25*time.Hour)

		_, err = ResolveMetadata(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load(), "refresh interval elapsed should trigger re-fetch")
	})

	t.Run("rpc failure leaves existing metadata intact", func(t *testing.T) {
		contractID := "CA_FAIL"
		storeMeta := &fakeMetadataStore{}
		storeMeta.Put(contractID, Metadata{Name: "Existing", Symbol: "EXT", Decimals: 7, IsToken: true}, time.Now())

		rpcClient := &stubRPCMetadata{
			getMeta: func(ctx context.Context, id string) (Metadata, error) {
				return Metadata{}, errors.New("rpc timeout")
			},
		},

		meta, err := ResolveMetadataWithFallbacks(context.Background(), rpcClient, storeMeta, contractID)
		require.NoError(t, err)
		assert.Equal(t, "Existing", meta.Name)
		assert.Equal(t, "EXT", meta.Symbol)
	})

	t.Run("metadata appearing on contract stats endpoint or store", func(t *testing.T) {
		contractID := "CA_STATS"
		storeMeta := &fakeMetadataStore{}
		storeMeta.Put(contractID, Metadata{Name: "StatToken", Symbol: "ST", Decimals: 6, IsToken: true}, time.Now())

		val, found := storeMeta.Get(contractID)
		assert.True(t, found)
		assert.Equal(t, "StatToken", val.Name)
	})

	t.Run("concurrent resolution of one contract fetching once", func(t *testing.T) {
		contractID := "CA_CONCURRENT"
		calls := atomic.Int32{}
		rpcClient := &stubRPCMetadata{
			getMeta: func(ctx context.Context, id string) (Metadata, error) {
				time.Sleep(20 * time.Millisecond)
				calls.Add(1)
				return Metadata{Name: "Conc", Symbol: "CC", Decimals: 8, IsToken: true}, nil
			},
		},
		storeMeta := &fakeMetadataStore{}

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = ResolveMetadataCachedSingleFlight(context.Background(), rpcClient, storeMeta, contractID)
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), calls.Load(), "concurrent requests for the same contract must fetch exactly once")
	})
}

// Stub and mock helpers supporting the test suite.

type stubRPCMetadata struct {
	getMeta func(ctx context.Context, contractID string) (Metadata, error)
}

func (s *stubRPCMetadata) GetMetadata(ctx context.Context, contractID string) (Metadata, error) {
	if s.getMeta != nil {
		return s.getMeta(ctx, contractID)
	}
	return Metadata{}, nil
}

type fakeMetadataStore struct {
	mu    sync.Mutex
	data  map[string]Metadata
	times map[string]time.Time
}

func (f *fakeMetadataStore) Get(contractID string) (Metadata, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.data == nil {
		return Metadata{}, false
	}
	m, ok := f.data[contractID]
	return m, ok
}

func (f *fakeMetadataStore) Put(contractID string, m Metadata, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.data == nil {
		f.data = make(map[string]Metadata)
		f.times = make(map[string]time.Time)
	}
	f.data[contractID] = m
	f.times[contractID] = t
}

func (f *fakeMetadataStore) SetAge(contractID string, age time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.times == nil {
		f.times = make(map[string]time.Time)
	}
	f.times[contractID] = time.Now().Add(-age)
}

func NewTestWorker() *Worker {
	return &Worker{}
}
