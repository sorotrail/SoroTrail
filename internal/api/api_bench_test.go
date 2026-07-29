package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

func benchServer() *Server {
	events := make([]store.Event, 50)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 50; i++ {
		events[i] = store.Event{
			ID:               "0000000000000010000-0000000000",
			ContractID:       testContract,
			Ledger:           10000,
			Type:             "contract",
			TxHash:           "deadbeef",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`),
			Value:            json.RawMessage(`{"u64":100000}`),
			CreatedAt:        baseTime,
		}
	}
	st := &stubStore{
		events:     events,
		nextCursor: "0000000000000010000-0000000049",
	}
	rc := &stubRPC{health: rpc.Health{Status: "healthy"}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(st, rc, logger, "")
}

func BenchmarkAPI_GetEvents_Unfiltered(b *testing.B) {
	srv := benchServer()
	router := srv.Router()

	req := httptest.NewRequest(http.MethodGet, "/events", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected 200 OK, got %d", w.Code)
		}
	}
}

func BenchmarkAPI_GetEvents_ContractID(b *testing.B) {
	srv := benchServer()
	router := srv.Router()

	req := httptest.NewRequest(http.MethodGet, "/events?contract_id="+testContract, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected 200 OK, got %d", w.Code)
		}
	}
}

func BenchmarkAPI_GetEvents_TopicContains(b *testing.B) {
	srv := benchServer()
	router := srv.Router()

	req := httptest.NewRequest(http.MethodGet, "/events?topic_contains=%5B%7B%22symbol%22%3A%22transfer%22%7D%5D", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected 200 OK, got %d", w.Code)
		}
	}
}

func BenchmarkAPI_GetEvents_LedgerRange(b *testing.B) {
	srv := benchServer()
	router := srv.Router()

	req := httptest.NewRequest(http.MethodGet, "/events?from_ledger=10000&to_ledger=10500", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected 200 OK, got %d", w.Code)
		}
	}
}
