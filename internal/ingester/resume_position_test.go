package ingester

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// TestResolvePosition covers all resume position resolution branches:
//  1. A stored cursor takes precedence over a stored ledger
//  2. A stored ledger with no cursor resumes at ledger+1
//  3. No stored state cold-starts at the retention window
//  4. An explicit StartLedger overrides the cold-start calculation
func TestResolvePosition(t *testing.T) {
	tests := []struct {
		name            string
		state           *store.IngestionState
		opts            Options
		latest          uint32
		wantStartLedger uint32
		wantCursor      string
	}{
		{
			name: "stored cursor takes precedence over stored ledger",
			state: &store.IngestionState{
				LastIngestedLedger: 500,
				LastCursor:         "cursor-123",
			},
			opts:            Options{RetentionLedgers: 100},
			latest:          1000,
			wantStartLedger: 0,
			wantCursor:      "cursor-123",
		},
		{
			name: "stored ledger with no cursor resumes at ledger plus one",
			state: &store.IngestionState{
				LastIngestedLedger: 500,
				LastCursor:         "",
			},
			opts:            Options{RetentionLedgers: 100},
			latest:          1000,
			wantStartLedger: 501,
			wantCursor:      "",
		},
		{
			name:            "no stored state cold-starts at retention window",
			state:           nil,
			opts:            Options{RetentionLedgers: 100},
			latest:          1000,
			wantStartLedger: 900, // 1000 - 100
			wantCursor:      "",
		},
		{
			name:            "explicit StartLedger overrides cold-start calculation",
			state:           nil,
			opts:            Options{StartLedger: 42, RetentionLedgers: 100},
			latest:          1000,
			wantStartLedger: 42,
			wantCursor:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeMock := newMockStore()
			if tt.state != nil {
				require.NoError(t, storeMock.SaveIngestionState(context.Background(), *tt.state))
			}

			ing := newTestIngester(&mockRPC{health: rpc.Health{LatestLedger: tt.latest}}, storeMock, tt.opts)
			startLedger, cursor, err := ing.resolvePosition(context.Background())
			require.NoError(t, err)

			assert.Equal(t, tt.wantStartLedger, startLedger)
			assert.Equal(t, tt.wantCursor, cursor)
		})
	}
}

// TestToStoreEvent covers event conversion and validation:
//  1. Conversion populates every stored field from the RPC event
//  2. A malformed RPC event is rejected rather than stored partially
func TestToStoreEvent(t *testing.T) {
	ing := newTestIngester(&mockRPC{}, newMockStore(), Options{})

	validEv := rpc.Event{
		ID:                       "0000000001-0000000001",
		ContractID:               "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:                   1,
		Type:                     "contract",
		TxHash:                   "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		InSuccessfulContractCall: true,
		Topic:                    []string{"AAAAAQ=="},
		Value:                    "AAAAAg==",
	}

	storeEv, err := ing.toStoreEvent(validEv)
	require.NoError(t, err)
	assert.Equal(t, validEv.ID, storeEv.ID)
	assert.Equal(t, validEv.ContractID, storeEv.ContractID)
	assert.Equal(t, int64(validEv.Ledger), storeEv.Ledger)
	assert.Equal(t, validEv.Type, storeEv.Type)
	assert.Equal(t, validEv.TxHash, storeEv.TxHash)
	assert.Equal(t, validEv.InSuccessfulContractCall, storeEv.InSuccessfulCall)
	assert.NotNil(t, storeEv.Topics)
	assert.NotNil(t, storeEv.Value)

	// A malformed event that fails to convert surfaces as an error rather
	// than being persisted partially. passthroughDecoder never fails, so use
	// an un-marshalable topics payload to force toStoreEvent to error.
	malformedEv := validEv
	malformedEv.TopicJSON = []json.RawMessage{json.RawMessage(`{`)}
	_, err = ing.toStoreEvent(malformedEv)
	assert.Error(t, err)
}
