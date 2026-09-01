//go:build integration

package pruner

// Issue #811: the pruner deletes rows while the ingester writes. The
// unit suite covers pruneOnce against a mock store; this test runs the
// real Pruner against a real Postgres while a writer plays the live
// ingester, and asserts the two never interfere: every write succeeds,
// and no row at or above the retention floor ever disappears.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

// TestPruneConcurrentIngestionUnaffected runs the pruner's Run loop
// (MinLedger floor, real batches and pauses) while a writer continuously
// ingests events at ledgers above the floor. All writes must succeed and
// every written row must survive: the pruner is only allowed to delete
// below its floor, never the live ingester's data.
func TestPruneConcurrentIngestionUnaffected(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The pruner refuses to delete at or above last_ingested_ledger; give
	// it a floor far above the writer's range so the MinLedger guard is
	// the only bound in play.
	now := time.Now().UTC()
	require.NoError(t, st.SaveIngestionState(ctx, store.IngestionState{
		LastIngestedLedger: 5000,
		LastSuccessfulPoll: &now,
	}))

	const floor int64 = 500
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	prn := New(st, log, Options{
		MinLedger: uint64(floor),
		BatchSize: 50,
		Pause:     time.Millisecond,
		Interval:  time.Hour,
	})
	require.True(t, prn.Enabled())

	var prunerWG sync.WaitGroup
	prunerWG.Add(1)
	go func() {
		defer prunerWG.Done()
		_ = prn.Run(ctx) // returns ctx.Err() on cancel; that is expected
	}()

	// Writer: 20 rounds x 25 events at ledgers 600..624 (all above the
	// floor, so the pruner must never touch them).
	errCh := make(chan error, 1)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for round := 0; round < 20; round++ {
			var batch []store.Event
			for i := 0; i < 25; i++ {
				n := 100000 + round*25 + i
				batch = append(batch, store.Event{
					ID:         fmt.Sprintf("%d", n),
					ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					Ledger:     int64(600 + i),
					Type:       "contract",
					TxHash:     "deadbeef",
					Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
					Value:      json.RawMessage(`{"i128":"1000"}`),
				})
			}
			if _, err := st.UpsertEvents(ctx, batch); err != nil {
				errCh <- err
				return
			}
		}
	}()
	writerWG.Wait()
	cancel()
	prunerWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent ingestion failed: %v", err)
	}

	// The pruner swept its floor between writes; every written row above
	// the floor must still be there. Use a fresh context: the run context
	// was canceled to stop the pruner.
	total, err := st.CountEvents(context.Background(), store.EventFilter{
		FromLedger: floor,
		Scope:      store.WildcardScope(),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(500), total,
		"pruning must never delete rows at or above its floor")
}
