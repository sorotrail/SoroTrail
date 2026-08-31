package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

var errDecodeFail = errors.New("decode failed")

// errDecoder fails every DecodeScVal call so tests can exercise the
// poison/dead-letter path inside persistEvents (passthroughDecoder never
// fails, so it cannot drive that branch).
type errDecoder struct{}

func (errDecoder) DecodeScVal(string) (json.RawMessage, error) {
	return nil, errDecodeFail
}

func TestIngester_PersistEvents(t *testing.T) {
	t.Run("clean page persists every event", func(t *testing.T) {
		ms := newMockStore()
		ing := newTestIngester(&mockRPC{}, ms, Options{})
		events := []rpc.Event{rpcEvent("0000000001-0000000001", 1), rpcEvent("0000000001-0000000002", 1)}

		require.NoError(t, ing.persistEvents(context.Background(), events, 1))

		ms.mu.Lock()
		defer ms.mu.Unlock()
		assert.Len(t, ms.events, 2, "both decoded events must be written to the store")
		assert.Equal(t, "0000000001-0000000001", ms.upserted[0][0].ID)
		assert.Empty(t, ms.deadLetters, "no poison events expected on a clean page")
	})

	t.Run("poison event quarantined and rest of page persists", func(t *testing.T) {
		ms := newMockStore()
		ing := newTestIngester(&mockRPC{}, ms, Options{})
		// The mock store doubles as the dead-letter sink (it implements
		// DeadLetterEvent), so poison events are quarantined and the page
		// continues instead of aborting.
		ing.SetDeadLetterSink(ms)
		// errDecoder fails Value decode, pushing the event into the dead-letter path.
		ing.decoder = errDecoder{}

		poison := rpc.Event{ID: "0000000001-0000000003", Type: "contract", Ledger: 1}
		poison.Value = "not-valid-xdr"

		events := []rpc.Event{
			rpcEvent("0000000001-0000000001", 1),
			poison,
			rpcEvent("0000000001-0000000002", 1),
		}
		require.NoError(t, ing.persistEvents(context.Background(), events, 1))

		ms.mu.Lock()
		defer ms.mu.Unlock()
		assert.Len(t, ms.deadLetters, 1, "the decoding failure must be quarantined")
		assert.Equal(t, "0000000001-0000000003", ms.deadLetters[0].EventID)
		assert.Len(t, ms.events, 2, "the two healthy events must still be persisted")
	})

	t.Run("poison event aborts without a dead-letter sink", func(t *testing.T) {
		ms := newMockStore()
		ing := newTestIngester(&mockRPC{}, ms, Options{})
		ing.decoder = errDecoder{}

		poison := rpc.Event{ID: "0000000001-0000000003", Type: "contract", Ledger: 1}
		poison.Value = "not-valid-xdr"

		// No SetDeadLetterSink call, so deadLetterStore is nil and the first
		// poison event aborts the cycle (legacy behavior).
		err := ing.persistEvents(context.Background(), []rpc.Event{poison}, 1)
		assert.Error(t, err)
	})
}