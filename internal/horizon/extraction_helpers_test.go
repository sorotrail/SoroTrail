package horizon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
)

func TestBuildEvent(t *testing.T) {
	createdAt := time.Date(2026, 9, 24, 12, 34, 56, 0, time.UTC)
	contractID := contractIDFromTestSeed("build-event")
	event := buildContractEvent(t, "build-event", scVec(scSymbol("transfer"), scU64(42)))

	got, ok := buildEvent(decode.XDRDecoder{}, contractID, TxHint{
		Hash:            "tx-hash",
		Ledger:          123,
		ResultCode:      "txSuccess",
		TxIndexInLedger: 7,
	}, createdAt, event, 4, 7)

	require.True(t, ok)
	assert.Equal(t, "tx-hash-00000000000000000123-00004-00007", got.ID)
	assert.Equal(t, contractID, got.ContractID)
	assert.Equal(t, int64(123), got.Ledger)
	assert.Equal(t, "contract", got.Type)
	assert.Equal(t, "tx-hash", got.TxHash)
	assert.Equal(t, int32(7), got.TxIndex)
	assert.Equal(t, int32(4), got.OpIndex)
	assert.True(t, got.InSuccessfulCall)
	assert.Equal(t, createdAt, got.CreatedAt)
	assert.NotEmpty(t, got.RawTopicXDR)
	assert.NotEmpty(t, got.RawValueXDR)
}

func TestEventPayloads(t *testing.T) {
	tests := []struct {
		name       string
		body       xdr.ScVal
		wantTopics string
		wantValue  string
		wantRaw    bool
	}{
		{
			name:       "topics and value use the RPC decoder shape",
			body:       scVec(scSymbol("transfer"), scU64(42)),
			wantTopics: `[{"symbol":"transfer"}]`,
			wantValue:  `{"u64":"42"}`,
			wantRaw:    true,
		},
		{
			name:       "empty payload list produces empty topics and null value",
			body:       scVec(),
			wantTopics: `[]`,
			wantValue:  `null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := buildContractEvent(t, "payloads", tt.body)
			topics, value, rawTopics, rawValue, ok := eventPayloads(decode.XDRDecoder{}, event)

			require.True(t, ok)
			assert.JSONEq(t, tt.wantTopics, string(topics))
			assert.JSONEq(t, tt.wantValue, string(value))
			if tt.wantRaw {
				assert.Len(t, rawTopics, 1)
				assert.NotEmpty(t, rawValue)
			} else {
				assert.Empty(t, rawTopics)
				assert.Empty(t, rawValue)
			}
		})
	}
}

func TestDecodeOne(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantJSON string
		wantOK   bool
	}{
		{name: "empty XDR is JSON null", input: "", wantJSON: "null", wantOK: true},
		{name: "malformed XDR is rejected", input: "not-base64-xdr", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := decodeOne(decode.XDRDecoder{}, tt.input)

			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.JSONEq(t, tt.wantJSON, string(got))
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func TestEventPayloadsMalformedBody(t *testing.T) {
	// A zero-valued ContractEventBody has no V0 payload. Rejecting it prevents
	// malformed XDR from becoming a partially populated event row.
	event := xdr.ContractEvent{}
	_, _, _, _, ok := eventPayloads(decode.XDRDecoder{}, event)
	assert.False(t, ok)
}

func TestBuildEventMalformedPayload(t *testing.T) {
	event := xdr.ContractEvent{}
	got, ok := buildEvent(decode.XDRDecoder{}, "", TxHint{Hash: "tx", Ledger: 1}, time.Time{}, event, 0, 0)
	assert.False(t, ok)
	assert.Equal(t, xdr.ContractEvent{}, event)
	assert.Empty(t, got.ID)
}

func TestBuildEventTopicsAreValidJSON(t *testing.T) {
	event := buildContractEvent(t, "json", scVec(scSymbol("topic"), scString("value")))
	got, ok := buildEvent(decode.XDRDecoder{}, contractIDFromTestSeed("json"), TxHint{}, time.Time{}, event, 0, 0)
	require.True(t, ok)

	var topics []json.RawMessage
	require.NoError(t, json.Unmarshal(got.Topics, &topics))
	assert.Len(t, topics, 1)
	assert.JSONEq(t, `{"symbol":"topic"}`, string(topics[0]))
	assert.JSONEq(t, `{"string":"value"}`, string(got.Value))
}
