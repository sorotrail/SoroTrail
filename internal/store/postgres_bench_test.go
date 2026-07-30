package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func benchStore(b *testing.B) *Postgres {
	b.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		b.Skip("TEST_DATABASE_URL not set; skipping Postgres store benchmarks")
	}

	if err := Migrate(dbURL); err != nil {
		b.Fatalf("failed to migrate database: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		b.Fatalf("failed to open pgxpool: %v", err)
	}
	b.Cleanup(pool.Close)

	return NewPostgres(pool)
}

// Generate a slice of dummy events for insertion benchmarks.
func generateDummyEvents(startID int, count int) []Event {
	events := make([]Event, count)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		idNum := startID + i
		ledger := int64(10000 + idNum/20)
		events[i] = Event{
			ID:               fmt.Sprintf("%019d-%010d", ledger, idNum%20),
			ContractID:       fmt.Sprintf("C%055d", idNum%50),
			Ledger:           ledger,
			Type:             "contract",
			TxHash:           fmt.Sprintf("%064x", idNum),
			TxIndex:          int32((idNum % 20) / 2),
			OpIndex:          int32(idNum % 2),
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`),
			Value:            json.RawMessage(`{"u64":100000}`),
			CreatedAt:        baseTime.Add(time.Duration(ledger) * time.Second),
			RawTopicXDR:      []string{"AAAAEAAAAAHRcmFuc2Zlcg=="},
			RawValueXDR:      "AAAAAwAAAAAABad0",
		}
	}
	return events
}

func BenchmarkUpsertEvents_Batch100(b *testing.B) {
	benchmarkUpsertEventsBatchSize(b, 100)
}

func BenchmarkUpsertEvents_Batch500(b *testing.B) {
	benchmarkUpsertEventsBatchSize(b, 500)
}

func BenchmarkUpsertEvents_Batch1000(b *testing.B) {
	benchmarkUpsertEventsBatchSize(b, 1000)
}

func BenchmarkUpsertEvents_Batch2500(b *testing.B) {
	benchmarkUpsertEventsBatchSize(b, 2500)
}

func BenchmarkUpsertEvents_Batch5000(b *testing.B) {
	benchmarkUpsertEventsBatchSize(b, 5000)
}

func benchmarkUpsertEventsBatchSize(b *testing.B, batchSize int) {
	st := benchStore(b)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	startID := 9000000
	for i := 0; i < b.N; i++ {
		events := generateDummyEvents(startID, batchSize)
		startID += batchSize
		_, err := st.UpsertEvents(ctx, events)
		if err != nil {
			b.Fatalf("UpsertEvents batch %d failed: %v", batchSize, err)
		}
	}
}

// Hot Filter Paths Benchmarks

func BenchmarkQueryEvents_Unfiltered(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{Limit: 50}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_ContractID(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{
		ContractID: fmt.Sprintf("C%055d", 5),
		Limit:      50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_Type(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{
		Types: []string{"contract"},
		Limit: 50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_TopicContains(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{
		TopicContains: json.RawMessage(`[{"symbol":"transfer"}]`),
		Limit:         50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_LedgerRange(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{
		FromLedger: 100100,
		ToLedger:   100500,
		Limit:      50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_CursorPagination(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	// Fetch first page to get valid cursor
	events, cursor, err := st.QueryEvents(ctx, EventFilter{Limit: 50})
	if err != nil || len(events) == 0 || cursor == "" {
		b.Skip("Insufficient data for cursor benchmark")
	}

	filter := EventFilter{
		Cursor: cursor,
		Limit:  50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEvents_OrderByLedger(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	filter := EventFilter{
		OrderBy: OrderByLedger,
		Order:   "desc",
		Limit:   50,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := st.QueryEvents(ctx, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}
