package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// stubStore implements store.Store for API tests.
type stubStore struct {
	mu         sync.Mutex
	events     []store.Event
	eventByID  map[string]store.Event
	lastFilter store.EventFilter
	stats      store.Stats
	ingState   store.IngestionState
	stateErr   error

	// Cache test fields
	event        store.Event
	exists       bool
	existsCalls  int
	lastExistsID string
	ingestion    store.IngestionState
	ingestionErr error
}

func (s *stubStore) UpsertEvents(ctx context.Context, events []store.Event) (int64, error) {
	return 0, nil
}
func (s *stubStore) ReplaceEventsInRange(ctx context.Context, events []store.Event, fromLedger, toLedger int64) error {
	return nil
}
func (s *stubStore) GetEvent(ctx context.Context, id string) (store.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventByID != nil {
		if e, ok := s.eventByID[id]; ok {
			return e, nil
		}
	}
	if s.event.ID != "" {
		return s.event, nil
	}
	return store.Event{}, store.ErrNotFound
}
func (s *stubStore) EventExists(ctx context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.existsCalls++
	s.lastExistsID = id
	if s.eventByID != nil {
		_, ok := s.eventByID[id]
		return ok, nil
	}
	return s.exists, nil
}
func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.mu.Lock()
	s.lastFilter = f
	s.mu.Unlock()
	if len(s.events) == 0 {
		return nil, "", nil
	}
	n := f.Limit
	if n <= 0 {
		n = 50
	}
	if n > len(s.events) {
		n = len(s.events)
	}
	return s.events[:n], "", nil
}
func (s *stubStore) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]store.LedgerCensus, error) {
	return nil, nil
}
func (s *stubStore) GetIngestionState(_ context.Context, network string) (store.IngestionState, error) {
	if s.ingestionErr != nil {
		return store.IngestionState{}, s.ingestionErr
	}
	if s.ingestion.LastIngestedLedger > 0 {
		return s.ingestion, nil
	}
	return s.ingState, nil
}
func (s *stubStore) SaveIngestionState(ctx context.Context, state store.IngestionState) error {
	return nil
}
func (s *stubStore) GetAuditState(ctx context.Context, network string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (s *stubStore) SaveAuditState(ctx context.Context, state store.AuditState) error {
	return nil
}
func (s *stubStore) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (store.AuditState, error) {
	return store.AuditState{}, nil
}
func (s *stubStore) ListWatchedContracts(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (s *stubStore) AddWatchedContract(ctx context.Context, contractID string) error {
	return nil
}
func (s *stubStore) RecordAuditFinding(ctx context.Context, f store.AuditFinding) (store.AuditFinding, error) {
	return f, nil
}
func (s *stubStore) UpdateAuditFinding(ctx context.Context, f store.AuditFinding) error {
	return nil
}
func (s *stubStore) ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (s *stubStore) CreateSubscription(ctx context.Context, sub store.Subscription) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) GetSubscription(ctx context.Context, id int64) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (s *stubStore) ListSubscriptions(ctx context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) UpdateSubscription(ctx context.Context, sub store.Subscription) (store.Subscription, error) {
	return sub, nil
}
func (s *stubStore) DeleteSubscription(ctx context.Context, id int64) error {
	return nil
}
func (s *stubStore) ListEnabledSubscriptions(ctx context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (s *stubStore) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	return 0, false, nil
}
func (s *stubStore) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	return nil
}
func (s *stubStore) RecordDeliveryAttempt(ctx context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	return a, nil
}
func (s *stubStore) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]store.DeliveryAttempt, error) {
	return nil, nil
}
func (s *stubStore) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (s *stubStore) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	return nil
}
func (s *stubStore) Stats(_ context.Context) (store.Stats, error) {
	return s.stats, nil
}
func (s *stubStore) Ping(ctx context.Context) error {
	return nil
}

// newTestServer creates a test server with a stub store.
func newTestServer(st *stubStore, _ *stubRPC) *Server {
	s := New(st, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetNetworks([]string{"default"})
	return s
}

// doGet performs a GET request against the test server.
func doGet(t *testing.T, s *Server, path string) (*http.Response, []byte) {
	t.Helper()
	return doGetWithHeader(t, s, path, "", "")
}

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

type stubRPC struct{}

func (s *stubRPC) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}
func (s *stubRPC) GetLatestLedger(ctx context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{}, nil
}
func (s *stubRPC) GetHealth(ctx context.Context) (rpc.Health, error) {
	return rpc.Health{Status: "healthy"}, nil
}
func (s *stubRPC) GetLedgerEntries(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}

func TestHealth(t *testing.T) {
	s := New(&stubStore{}, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := s.Router()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp healthResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "ok", resp.Status)
}

func TestGetEvent(t *testing.T) {
	st := &stubStore{
		eventByID: map[string]store.Event{
			"ev-1": {ID: "ev-1", ContractID: "C1", Ledger: 100},
		},
	}
	s := New(st, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := s.Router()

	t.Run("found", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events/ev-1", nil)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		var ev store.Event
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&ev))
		assert.Equal(t, "ev-1", ev.ID)
	})

	t.Run("not found", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events/unknown", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func TestListEvents(t *testing.T) {
	st := &stubStore{
		events: []store.Event{
			{ID: "e1", ContractID: "C1", Network: "testnet"},
			{ID: "e2", ContractID: "C2", Network: "testnet"},
		},
	}
	s := New(st, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetNetworks([]string{"testnet", "mainnet"})
	s.HasMultipleNetworks = true
	handler := s.Router()

	t.Run("requires network when multiple configured", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("accepts valid network", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events?network=testnet", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("rejects invalid network", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/events?network=invalid", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

func TestStats(t *testing.T) {
	st := &stubStore{stats: store.Stats{TotalEvents: 42, LastIngestedLedger: 999}}
	s := New(st, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := s.Router()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var got store.Stats
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&got))
	assert.Equal(t, int64(42), got.TotalEvents)
}

func TestContractEvents(t *testing.T) {
	st := &stubStore{
		events: []store.Event{
			{ID: "e1", ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
	}
	s := New(st, &stubRPC{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetNetworks([]string{"testnet"})
	handler := s.Router()

	t.Run("valid contract", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet,
			"/contracts/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("invalid contract ID", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/contracts/not-a-contract/events", nil)
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}
