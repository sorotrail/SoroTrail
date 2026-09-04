//go:build integration

package backfill

// Issue #810: the Backfiller and the live ingester write to the same
// tables at the same time. Where the store-level tests
// (internal/store/backfill_live_concurrency_test.go) exercise the
// persistence layer directly, this test runs a real Backfiller.Run
// against a real Postgres while a goroutine plays the live ingester
// writing the same ledger range, and asserts the two never corrupt
// each other's rows or state.
//
// It is gated behind the integration tag like the rest of the DB-backed
// suite: without TEST_DATABASE_URL (or Docker) it skips rather than
// fails, and in CI it runs against the service Postgres.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

// liveEventID builds a TOID-shaped id like the live ingester's, distinct
// from backfill's hash-shaped ids for the same emission.
func liveEventID(ledger int64) string {
	return fmt.Sprintf("%020d-%010d", ledger, 0)
}

// TestBackfillRunsAlongsideLiveIngestion is the end-to-end overlap
// scenario the issue describes: backfill walks an ancient range while
// the live ingester keeps writing the same ledgers and advancing its
// frontier. Backfill must complete, its own state row must finish
// independent of the ingester's, the live frontier must not move
// backwards, and neither writer may produce duplicate rows for its own
// ids.
func TestBackfillRunsAlongsideLiveIngestion(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)
	ctx := context.Background()

	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("transfer"), scU64(7))
	meta := buildSimpleMeta(t, "alpha", body)

	// Horizon serves ledgers 100..104 on one page, then an empty page.
	rows := make([]horizon.Transaction, 0, 5)
	for l := int64(100); l <= 104; l++ {
		rows = append(rows, horizon.Transaction{
			ID:            fmt.Sprintf("tx-%d", l),
			Hash:          fmt.Sprintf("beef%04d", l),
			Ledger:        l,
			CreatedAt:     "2026-07-24T00:00:00Z",
			ResultCode:    "txSuccess",
			ResultMetaXDR: meta,
		})
	}
	h := &fakeHorizon{pageResponses: [][]horizon.Transaction{rows, {}}}

	// The live ingester replays the same ledgers with TOID ids while
	// backfill runs, keeping its frontier at the top of the range.
	errCh := make(chan error, 1)
	var liveWG sync.WaitGroup
	liveWG.Add(1)
	go func() {
		defer liveWG.Done()
		for round := 0; round < 10; round++ {
			events := make([]store.Event, 0, 5)
			for l := int64(100); l <= 104; l++ {
				e := store.Event{
					ID:               liveEventID(l),
					ContractID:       targetContract,
					Ledger:           l,
					Type:             "contract",
					TxHash:           fmt.Sprintf("beef%04d", l),
					InSuccessfulCall: true,
					Topics:           json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`),
					Value:            json.RawMessage(`{"i128":"1000"}`),
					CreatedAt:        time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
				}
				events = append(events, e)
			}
			if _, err := st.UpsertEvents(ctx, events); err != nil {
				errCh <- fmt.Errorf("live writer: %w", err)
				return
			}
			now := time.Now().UTC()
			if err := st.SaveIngestionState(ctx, store.IngestionState{
				LastIngestedLedger: 104,
				LastSuccessfulPoll: &now,
			}); err != nil {
				errCh <- fmt.Errorf("live writer state: %w", err)
				return
			}
		}
	}()

	b := New(h, st, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   104,
		BatchSize:  200,
	})
	sum, err := b.Run(ctx)
	require.NoError(t, err, "backfill must complete while live ingestion writes")
	require.True(t, sum.Completed)
	assert.EqualValues(t, 5, sum.Extracted, "backfill must extract its five events")
	assert.EqualValues(t, 5, sum.Inserted)

	liveWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("live writer failed: %v", err)
	}

	// Backfill finished its own state row...
	bs, err := st.GetBackfillState(ctx)
	require.NoError(t, err)
	assert.Equal(t, targetContract, bs.ContractID)
	assert.Equal(t, int64(104), bs.LastLedger)
	require.NotNil(t, bs.CompletedAt, "backfill must mark its run complete")

	// ...while the live frontier stayed exactly where the ingester put
	// it: backfill writes events and backfill_state, never ingestion_state.
	ing, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(104), ing.LastIngestedLedger,
		"live frontier must be untouched by backfill")

	// One row per writer id: 5 backfill hash ids + 5 live TOIDs = 10
	// rows, each distinct. Duplicates on either side would mean the
	// concurrent overlap corrupted an upsert.
	total, err := st.CountEvents(ctx, store.EventFilter{Scope: store.WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(10), total,
		"both writers' rows must coexist with one row per id")
}
