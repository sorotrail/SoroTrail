package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stellar/go/xdr"
)

func main() {
	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable"
	}

	dbURL := flag.String("db", defaultDB, "Postgres connection URL")
	count := flag.Int("count", 1000000, "Total number of events to seed")
	batchSize := flag.Int("batch-size", 1000, "Batch size for insertion")
	numContracts := flag.Int("contracts", 50, "Number of distinct contracts to generate")
	dropFirst := flag.Bool("drop-first", true, "Truncate events table before seeding")

	flag.Parse()

	log.Printf("Connecting to Postgres database at %s...", *dbURL)
	if err := store.Migrate(*dbURL); err != nil {
		log.Fatalf("Failed to run database migrations: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), *dbURL)
	if err != nil {
		log.Fatalf("Failed to open pgx pool: %v", err)
	}
	defer pool.Close()

	st := store.NewPostgres(pool)

	if *dropFirst {
		log.Println("Truncating existing events table...")
		_, err := pool.Exec(context.Background(), "TRUNCATE TABLE events CASCADE;")
		if err != nil {
			log.Fatalf("Failed to truncate table: %v", err)
		}
	}

	log.Printf("Seeding %d events (batch size: %d, contracts: %d)...", *count, *batchSize, *numContracts)

	// Pre-generate contract IDs
	contracts := make([]string, *numContracts)
	for i := 0; i < *numContracts; i++ {
		contracts[i] = fmt.Sprintf("C%055d", i)
	}

	// Pre-build common XDR base64 strings for raw topic & value
	symTransfer := xdr.ScSymbol("transfer")
	valSymTransfer := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symTransfer}
	b64Transfer, _ := xdr.MarshalBase64(valSymTransfer)

	symMint := xdr.ScSymbol("mint")
	valSymMint := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symMint}
	b64Mint, _ := xdr.MarshalBase64(valSymMint)

	symBurn := xdr.ScSymbol("burn")
	valSymBurn := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symBurn}
	b64Burn, _ := xdr.MarshalBase64(valSymBurn)

	u64Val := xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: (*xdr.Uint64)(ptrU64(100000))}
	b64Value, _ := xdr.MarshalBase64(u64Val)

	types := []string{"contract", "contract", "contract", "contract", "system", "diagnostic"}
	topicsList := []struct {
		jsonStr string
		rawXDR  []string
	}{
		{`[{"symbol":"transfer"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`, []string{b64Transfer}},
		{`[{"symbol":"mint"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`, []string{b64Mint}},
		{`[{"symbol":"burn"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`, []string{b64Burn}},
	}

	startTime := time.Now()
	totalInserted := int64(0)
	currentLedger := int64(100000)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	events := make([]store.Event, 0, *batchSize)
	rng := rand.New(rand.NewSource(42)) // Deterministic seed for reproducible data

	for i := 1; i <= *count; i++ {
		if i%20 == 0 {
			currentLedger++
		}
		contract := contracts[rng.Intn(len(contracts))]
		eventType := types[rng.Intn(len(types))]
		topicChoice := topicsList[rng.Intn(len(topicsList))]

		txHash := fmt.Sprintf("%064x", rng.Int63())
		eventID := fmt.Sprintf("%019d-%010d", currentLedger, i%20)

		evt := store.Event{
			ID:               eventID,
			ContractID:       contract,
			Ledger:           currentLedger,
			Type:             eventType,
			TxHash:           txHash,
			TxIndex:          int32((i % 20) / 2),
			OpIndex:          int32(i % 2),
			InSuccessfulCall: true,
			Topics:           []byte(topicChoice.jsonStr),
			Value:            []byte(`{"u64":100000}`),
			CreatedAt:        baseTime.Add(time.Duration(currentLedger*5) * time.Second),
			RawTopicXDR:      topicChoice.rawXDR,
			RawValueXDR:      b64Value,
		}

		events = append(events, evt)

		if len(events) >= *batchSize || i == *count {
			n, err := st.UpsertEvents(context.Background(), events)
			if err != nil {
				log.Fatalf("UpsertEvents failed at row %d: %v", i, err)
			}
			totalInserted += n
			events = events[:0]

			if i%50000 == 0 || i == *count {
				elapsed := time.Since(startTime).Seconds()
				rate := float64(i) / elapsed
				log.Printf("Progress: %d/%d (%.1f%%) | %d rows inserted | Rate: %.0f events/sec",
					i, *count, float64(i)/float64(*count)*100.0, totalInserted, rate)
			}
		}
	}

	totalTime := time.Since(startTime)
	log.Printf("Seeding complete! Successfully seeded %d events in %v (avg rate: %.0f events/sec).",
		totalInserted, totalTime, float64(totalInserted)/totalTime.Seconds())
}

func ptrU64(v uint64) *uint64 { return &v }

// Silence unused package warning
var _ decode.Decoder = nil
