package meta

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockRPC records simulateTransaction calls and returns scripted responses.
type mockRPC struct {
	mu       sync.Mutex
	simCalls []rpc.SimulateTransactionRequest
	simResps []rpc.SimulateTransactionResponse
	simErrs  []error
}

func (m *mockRPC) GetEvents(context.Context, rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}
func (m *mockRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{}, nil
}
func (m *mockRPC) GetHealth(context.Context) (rpc.Health, error) {
	return rpc.Health{}, nil
}
func (m *mockRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}
func (m *mockRPC) SimulateTransaction(_ context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simCalls = append(m.simCalls, req)
	idx := len(m.simCalls) - 1
	var resp rpc.SimulateTransactionResponse
	if idx < len(m.simResps) {
		resp = m.simResps[idx]
	}
	var err error
	if idx < len(m.simErrs) {
		err = m.simErrs[idx]
	}
	return resp, err
}

// stubStore records contract meta operations in memory.
type stubStore struct {
	mu          sync.Mutex
	meta        map[string]store.ContractMeta
	contractIDs []string
	refreshErr  error
}

func newStubStore() *stubStore {
	return &stubStore{meta: map[string]store.ContractMeta{}}
}

func (s *stubStore) UpsertEvents(context.Context, []store.Event) (int64, error) { return 0, nil }
func (s *stubStore) ReplaceEventsInRange(context.Context, []store.Event, int64, int64) error {
	return nil
}
func (s *stubStore) GetEvent(context.Context, string, store.Scope) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}
func (s *stubStore) GetEventsByTxHash(context.Context, string, string) ([]store.Event, error) {
	return nil, nil
}
func (s *stubStore) EventExists(context.Context, string, store.Scope) (bool, error) {
	return false, nil
}
func (s *stubStore) QueryEvents(context.Context, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}
func (s *stubStore) CountEvents(context.Context, store.EventFilter) (int64, error) { return 0, nil }
func (s *stubStore) AggregateEvents(context.Context, store.EventFilter, string) ([]store.AggregateBucket, error) {
	return nil, nil
}
func (s *stubStore) LedgerRangeCensus(context.Context, int64, int64, bool) ([]store.LedgerCensus, error) {
	return nil, nil
}
func (s *stubStore) ListContracts(context.Context, store.ContractsFilter) ([]store.ContractSummary, string, error) {
	return nil, "", nil
}
func (s *stubStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (s *stubStore) GetContractSummary(context.Context, string) (store.ContractSummary, error) {
	return store.ContractSummary{}, store.ErrNotFound
}
func (s *stubStore) ContractEventTypeCounts(context.Context, string) ([]store.ContractEventTypeCount, error) {
	return nil, nil
}
func (s *stubStore) DeadLetterEvent(context.Context, store.DeadLetterInput) (store.DeadLetter, error) {
	return store.DeadLetter{}, nil
}
func (s *stubStore) ListDeadLetters(context.Context, string, int, string) ([]store.DeadLetter, string, error) {
	return nil, "", nil
}
func (s *stubStore) GetDeadLetter(context.Context, int64) (store.DeadLetter, error) {
	return store.DeadLetter{}, store.ErrNotFound
}
func (s *stubStore) DeleteDeadLetter(context.Context, int64) error { return nil }
func (s *stubStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	return store.IngestionState{}, store.ErrNotFound
}
func (s *stubStore) SaveIngestionState(context.Context, store.IngestionState) error { return nil }
func (s *stubStore) GetAuditState(context.Context, string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (s *stubStore) SaveAuditState(context.Context, store.AuditState) error { return nil }
func (s *stubStore) SaveAuditStateIfGreater(context.Context, string, int64) (store.AuditState, error) {
	return store.AuditState{}, nil
}
func (s *stubStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
	return nil, nil
}
func (s *stubStore) AddWatchedContract(context.Context, string) error    { return nil }
func (s *stubStore) RemoveWatchedContract(context.Context, string) error { return nil }
func (s *stubStore) GetContractCursor(context.Context, string) (store.ContractCursor, error) {
	return store.ContractCursor{}, store.ErrNotFound
}
func (s *stubStore) SaveContractCursor(context.Context, store.ContractCursor) error { return nil }
func (s *stubStore) DeleteContractCursor(context.Context, string) error             { return nil }
func (s *stubStore) ListContractCursors(context.Context) ([]store.ContractCursor, error) {
	return nil, nil
}
func (s *stubStore) RecordAuditFinding(context.Context, store.AuditFinding) (store.AuditFinding, error) {
	return store.AuditFinding{}, nil
}
func (s *stubStore) UpdateAuditFinding(context.Context, store.AuditFinding) error { return nil }
func (s *stubStore) ListOpenFindingsByRange(context.Context, string, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (s *stubStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}
func (s *stubStore) Ping(context.Context) error { return nil }
func (s *stubStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (s *stubStore) SetContractSpec(context.Context, string, string, []byte) error {
	return nil
}
func (s *stubStore) CreateSubscription(_ context.Context, sub store.Subscription) (store.Subscription, error) {
	sub.ID = 1
	return sub, nil
}
func (s *stubStore) GetSubscription(_ context.Context, id int64, _ store.SubscriptionOwner) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (s *stubStore) ListSubscriptions(context.Context, store.SubscriptionOwner) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) UpdateSubscription(_ context.Context, sub store.Subscription, _ store.SubscriptionOwner) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) DeleteSubscription(context.Context, int64, store.SubscriptionOwner) error {
	return nil
}
func (s *stubStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) IncrementSubscriptionFailures(context.Context, int64, int) (int, bool, error) {
	return 0, false, nil
}
func (s *stubStore) ResetSubscriptionFailures(context.Context, int64) error { return nil }
func (s *stubStore) RecordDeliveryAttempt(_ context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	a.ID = 1
	return a, nil
}
func (s *stubStore) ListDeliveryAttempts(context.Context, int64, int, store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	return nil, nil
}
func (s *stubStore) DeleteEventsBeforeLedger(context.Context, int64) (int64, error) { return 0, nil }
func (s *stubStore) DeleteEventsBefore(context.Context, int64, time.Time, int) (int64, error) {
	return 0, nil
}
func (s *stubStore) UpsertAddressRefs(context.Context, []store.AddressRef) error { return nil }
func (s *stubStore) QueryAddressEvents(context.Context, string, store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}
func (s *stubStore) CountAddressEvents(context.Context, string) (int64, error) { return 0, nil }
func (s *stubStore) GetAddressSummary(context.Context, string) (store.AddressSummary, error) {
	return store.AddressSummary{}, nil
}
func (s *stubStore) MigrationVersion(context.Context) (int, bool, error) { return 0, false, nil }
func (s *stubStore) GetContractMeta(_ context.Context, contractID string) (store.ContractMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.meta[contractID]; ok {
		return m, nil
	}
	return store.ContractMeta{}, store.ErrNotFound
}
func (s *stubStore) UpsertContractMeta(_ context.Context, m store.ContractMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta[m.ContractID] = m
	return nil
}
func (s *stubStore) ListContractIDs(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contractIDs, nil
}
func (s *stubStore) ListContractsNeedingRefresh(_ context.Context, _ time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contractIDs, s.refreshErr
}
func (s *stubStore) CountContractEvents(context.Context, string) (int64, error) { return 0, nil }

// makeSymbolResult returns a simulateTransaction result that encodes a
// symbol ScVal. The result JSON is a string containing base64-encoded XDR.
func makeSymbolResult(t *testing.T, sym string) rpc.SimulateTransactionResponse {
	t.Helper()
	// Encode a ScVal symbol using stellar/go xdr.
	val := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  (*xdr.ScSymbol)(&sym),
	}
	b64, err := xdr.MarshalBase64(val)
	require.NoError(t, err)
	resultJSON, err := json.Marshal(b64)
	require.NoError(t, err)
	return rpc.SimulateTransactionResponse{
		Results: []json.RawMessage{resultJSON},
	}
}

// makeU32Result returns a simulateTransaction result that encodes a
// u32 ScVal (for decimals).
func makeU32Result(t *testing.T, n uint32) rpc.SimulateTransactionResponse {
	t.Helper()
	val := xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  (*xdr.Uint32)(&n),
	}
	b64, err := xdr.MarshalBase64(val)
	require.NoError(t, err)
	resultJSON, err := json.Marshal(b64)
	require.NoError(t, err)
	return rpc.SimulateTransactionResponse{
		Results: []json.RawMessage{resultJSON},
	}
}

// makeTrapResult returns a simulateTransaction response where the simulation
// failed (contract trapped), meaning it doesn't have the called function.
func makeTrapResult(t *testing.T) rpc.SimulateTransactionResponse {
	t.Helper()
	return rpc.SimulateTransactionResponse{
		Error: "HostError: Error(Contract, #1)",
	}
}

// testContract is a known testnet contract used in tests.
const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

func TestFetchTokenMeta_Success(t *testing.T) {
	rpc := &mockRPC{
		simResps: []rpc.SimulateTransactionResponse{
			makeSymbolResult(t, "USD Coin"), // name()
			makeSymbolResult(t, "USDC"),     // symbol()
			makeU32Result(t, 6),             // decimals()
		},
	}
	st := newStubStore()
	w := New(rpc, st, testLogger(), 0, time.Minute)

	meta, err := w.fetchTokenMeta(context.Background(), testContract)
	require.NoError(t, err)
	assert.True(t, meta.IsToken)
	require.NotNil(t, meta.Name)
	assert.Equal(t, "USD Coin", *meta.Name)
	require.NotNil(t, meta.Symbol)
	assert.Equal(t, "USDC", *meta.Symbol)
	require.NotNil(t, meta.Decimals)
	assert.Equal(t, 6, *meta.Decimals)
	assert.Equal(t, 3, len(rpc.simCalls), "one call each for name, symbol, decimals")
}

func TestFetchTokenMeta_NonTokenContract_NegativeCache(t *testing.T) {
	// First call (name) traps — contract doesn't implement SEP-41.
	rpc := &mockRPC{
		simResps: []rpc.SimulateTransactionResponse{
			makeTrapResult(t),
		},
	}
	st := newStubStore()
	w := New(rpc, st, testLogger(), 0, time.Minute)

	meta, err := w.fetchTokenMeta(context.Background(), testContract)
	require.NoError(t, err)
	assert.False(t, meta.IsToken, "contract that traps on name() is not a token")
	assert.Nil(t, meta.Name)
	assert.Nil(t, meta.Symbol)
	assert.Nil(t, meta.Decimals)
}

func TestFetchTokenMeta_RPCError_ReturnsError(t *testing.T) {
	rpc := &mockRPC{
		simErrs: []error{errors.New("connection refused")},
	}
	st := newStubStore()
	w := New(rpc, st, testLogger(), 0, time.Minute)

	_, err := w.fetchTokenMeta(context.Background(), testContract)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestWorker_RunOnce_EnrichesAndPersists(t *testing.T) {
	rpc := &mockRPC{
		simResps: []rpc.SimulateTransactionResponse{
			makeSymbolResult(t, "Test Token"),
			makeSymbolResult(t, "TEST"),
			makeU32Result(t, 18),
		},
	}
	st := newStubStore()
	st.contractIDs = []string{testContract}
	w := New(rpc, st, testLogger(), 0, time.Minute)

	err := w.runOnce(context.Background())
	require.NoError(t, err)

	// Verify meta was persisted.
	meta, err := st.GetContractMeta(context.Background(), testContract)
	require.NoError(t, err)
	assert.True(t, meta.IsToken)
	require.NotNil(t, meta.Name)
	assert.Equal(t, "Test Token", *meta.Name)
}

func TestWorker_RunOnce_NegativeCachesNonToken(t *testing.T) {
	rpc := &mockRPC{
		simResps: []rpc.SimulateTransactionResponse{
			makeTrapResult(t),
		},
	}
	st := newStubStore()
	st.contractIDs = []string{testContract}
	w := New(rpc, st, testLogger(), 0, time.Minute)

	err := w.runOnce(context.Background())
	require.NoError(t, err)

	meta, err := st.GetContractMeta(context.Background(), testContract)
	require.NoError(t, err)
	assert.False(t, meta.IsToken, "non-token contract should be negatively cached")
}

func TestWorker_RunOnce_NoContracts(t *testing.T) {
	rpc := &mockRPC{}
	st := newStubStore()
	st.contractIDs = nil
	w := New(rpc, st, testLogger(), 0, time.Minute)

	err := w.runOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, rpc.simCalls, "no RPC calls when there are no contracts")
}

func TestWorker_RunOnce_TransientRPCError_Skip(t *testing.T) {
	rpc := &mockRPC{
		simErrs: []error{errors.New("timeout")},
	}
	st := newStubStore()
	st.contractIDs = []string{testContract}
	w := New(rpc, st, testLogger(), 0, time.Minute)

	err := w.runOnce(context.Background())
	require.NoError(t, err)

	// Transient errors should NOT write a meta row (no IsToken=true with nil fields).
	_, err = st.GetContractMeta(context.Background(), testContract)
	assert.ErrorIs(t, err, store.ErrNotFound, "transient RPC errors should not persist any meta row")
}

func TestParseDecimals(t *testing.T) {
	tests := []struct {
		raw    string
		want   int
		errMsg string
	}{
		{"6", 6, ""},
		{"18", 18, ""},
		{"0", 0, ""},
		{"7", 7, ""},
		{"abc", 0, "parsing decimals"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := parseDecimals(tt.raw)
			if tt.errMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsContractTrap(t *testing.T) {
	assert.True(t, isContractTrap(&simulationError{msg: "trapped"}))
	assert.False(t, isContractTrap(errors.New("connection refused")))
	assert.False(t, isContractTrap(nil))
}

func TestDecodeContractID(t *testing.T) {
	// Valid contract ID.
	b, err := decodeContractID(testContract)
	require.NoError(t, err)
	assert.Len(t, b, 32)

	// Invalid length.
	_, err = decodeContractID("short")
	assert.Error(t, err)

	// Wrong prefix.
	_, err = decodeContractID("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	assert.Error(t, err)
}
