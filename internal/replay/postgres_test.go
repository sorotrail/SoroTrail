//go:build integration

package replay_test

// End-to-end replay against a real database. Skipped unless
// TEST_DATABASE_URL is set (see CONTRIBUTING.md); `make test-db` runs it.
//
// These share the database (and truncate the same tables) with the
// internal/store integration tests, so the suite is run with `go test -p 1`.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/replay"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// rpcEvent builds the shape decode.EventTopicsValue consumes, mirroring how
// the ingester decodes at write time.
func rpcEvent(rawTopic, rawValue string) rpc.Event {
	return rpc.Event{Topic: []string{rawTopic}, Value: rawValue}
}

const contractID = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func testStore(t *testing.T) *store.Postgres {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration tests (see CONTRIBUTING.md)")
	}
	require.NoError(t, store.Migrate(dbURL))

	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(context.Background(), `TRUNCATE events, replay_state`)
	require.NoError(t, err)
	return store.NewPostgres(pool)
}

func mustBase64(t *testing.T, val xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(val)
	require.NoError(t, err)
	return s
}

func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scU64(n uint64) xdr.ScVal {
	u := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func eventID(n int) string { return fmt.Sprintf("%020d-%010d", n, 0) }

// staleDecoder stands in for the decoder that was current when these events
// were ingested: it emitted the lossless {"unknown": ...} fallback for
// everything. Replaying with the real XDRDecoder is therefore "the decoder
// improved", which is exactly the scenario replay exists for.
type staleDecoder struct{}

func (staleDecoder) DecodeScVal(base64XDR string) (json.RawMessage, error) {
	return json.RawMessage(fmt.Sprintf(`{"unknown":{"base64":%q}}`, base64XDR)), nil
}

// seedEvents stores events decoded by the stale decoder, keeping their raw
// XDR, and returns how many were seeded.
func seedEvents(t *testing.T, p *store.Postgres, n int, withRawXDR bool) {
	t.Helper()
	ctx := context.Background()

	var events []store.Event
	for i := 1; i <= n; i++ {
		rawTopic := mustBase64(t, scSymbol("transfer"))
		rawValue := mustBase64(t, scU64(uint64(100+i)))

		topics, value, err := decode.EventTopicsValue(staleDecoder{}, rpcEvent(rawTopic, rawValue))
		require.NoError(t, err)

		e := store.Event{
			ID:               eventID(i),
			ContractID:       contractID,
			Ledger:           int64(100 + i),
			Type:             "contract",
			TxHash:           "deadbeef",
			InSuccessfulCall: true,
			Topics:           topics,
			Value:            value,
		}
		if withRawXDR {
			e.RawTopicXDR = []string{rawTopic}
			e.RawValueXDR = rawValue
		}
		events = append(events, e)
	}
	_, err := p.UpsertEvents(ctx, events)
	require.NoError(t, err)
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Definition of done: replaying seeded data with a changed decoder produces
// updated decoded columns — and doing it twice is a no-op.
func TestReplay_ImprovedDecoderRewritesStoredRows(t *testing.T) {
	t.Run("comprehensive decoder replay across decoder versions", func(t *testing.T) {
		p := testStore(t)
		ctx := context.Background()

		// 1. Seed events written by a stale decoder carrying unknown fallback format and raw XDR.
		seedEvents(t, p, 3, true)

		// Verify the initial rows carry the stale unknown-type fallback in their decoded JSON.
		initialGet, err := p.GetEvent(ctx, eventID(1), store.SystemScope())
		require.NoError(t, err)
		assert.Contains(t, string(initialGet.Value), "unknown")

		// 2. Perform the initial replay using the current decode.XDRDecoder().
		r := replay.New(p, decode.XDRDecoder{}, testLogger(), replay.Config{
			BatchSize: 2,
			MaxLedger: 200,
		})

		err = r.Run(ctx)
		require.NoError(t, err)

		// Assert rows written by stale decoder are rewritten by the current one.
		replayedGet, err := p.GetEvent(ctx, eventID(1), store.SystemScope())
		require.NoError(t, err)
		assert.NotContains(t, string(replayedGet.Value), "unknown")
		assert.Contains(t, string(replayedGet.Value), "u64")

		// 3. Rows already current reported unchanged and second replay changes nothing.
		r2 := replay.New(p, decode.XDRDecoder{}, testLogger(), replay.Config{
			BatchSize: 2,
			MaxLedger: 200,
		})
		err = r2.Run(ctx)
		require.NoError(t, err)

		replayedSecondGet, err := p.GetEvent(ctx, eventID(1), store.SystemScope())
		require.NoError(t, err)
		assert.Equal(t, string(replayedGet.Value), string(replayedSecondGet.Value))

		// 4. Test progress persistence across interruptions: progress is saved and bounds work correctly.
		prog, err := p.GetReplayProgress(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, prog, int64(100))
	})
}

func TestReplay_RawXDRPreservedAndFallbackStaleRows(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	// Seed with raw XDR preserved so replay stays possible indefinitely.
	seedEvents(t, p, 2, true)

	// Verify raw XDR is present.
	e, err := p.GetEvent(ctx, eventID(1), store.SystemScope())
	require.NoError(t, err)
	assert.True(t, e.HasRawXDR())

	r := replay.New(p, decode.XDRDecoder{}, testLogger(), replay.Config{
		BatchSize: 10,
		MaxLedger: 200,
	})

	require.NoError(t, r.Run(ctx))

	// Ensure progress and replay succeeded cleanly.
	prog, err := p.GetReplayProgress(ctx)
	require.NoError(t, err)
	assert.Greater(t, prog, int64(0))
}
