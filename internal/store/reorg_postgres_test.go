//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgres_ReconcileReorgRange_EmptyReplacementRetractsAll(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	baseTime := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	events := []Event{
		{
			Network:          "default",
			ID:               "0000100-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           100,
			Type:             "contract",
			TxHash:           "tx-100",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(`{"amount":10}`),
			CreatedAt:        baseTime,
		},
		{
			Network:          "default",
			ID:               "0000105-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           105,
			Type:             "contract",
			TxHash:           "tx-105-fork",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(`{"amount":50}`),
			CreatedAt:        baseTime.Add(5 * time.Second),
		},
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "GADDR_SURVIVE", EventID: "0000100-0001-0000", Role: "topic"},
		{Address: "GADDR_ORPHAN", EventID: "0000105-0001-0000", Role: "topic"},
	}))

	_, err = st.DeadLetterEvent(ctx, DeadLetterInput{
		EventID:    "0000105-9999-0000",
		ContractID: "CCONTRACT_A",
		Ledger:     105,
		Type:       "contract",
		TxHash:     "tx-dead-105",
		Err:        errors.New("decode error on orphaned fork"),
	})
	require.NoError(t, err)

	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:         "default",
		FromLedger:      105,
		ToLedger:        106,
		CanonicalEvents: nil,
		NewHeadLedger:   104,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"0000105-0001-0000"}, res.RetractedEventIDs)
	assert.Equal(t, int64(1), res.RetractedAddressRefs)
	assert.Equal(t, int64(1), res.RetractedDeadLetters)

	orphanCount, err := st.CountAddressEvents(ctx, "GADDR_ORPHAN")
	require.NoError(t, err)
	assert.Equal(t, int64(0), orphanCount)

	dlCount, err := st.CountDeadLetters(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlCount)
}

func TestPostgres_ReconcileReorgRange_DerivedStateAndMultiNetworkIsolation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{Network: "mainnet", ContractID: "CCONTRACT_MAIN"}))
	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{Network: "testnet", ContractID: "CCONTRACT_TEST"}))

	baseTime := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	_, err := st.UpsertEvents(ctx, []Event{
		{
			Network: "mainnet", ID: "0000200-0001-0000", ContractID: "CCONTRACT_MAIN",
			Ledger: 200, Type: "contract", TxHash: "tx-200-unchanged", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"keep"}]`), Value: json.RawMessage(`{"v":1}`),
			RawTopicXDR: []string{"AAAA"}, RawValueXDR: "BBBB", CreatedAt: baseTime,
		},
		{
			Network: "mainnet", ID: "0000201-0001-0000", ContractID: "CCONTRACT_MAIN",
			Ledger: 201, Type: "contract", TxHash: "tx-201-old-fork", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"old"}]`), Value: json.RawMessage(`{"v":2}`), CreatedAt: baseTime,
		},
		{
			Network: "testnet", ID: "0000201-0001-0000", ContractID: "CCONTRACT_TEST",
			Ledger: 201, Type: "contract", TxHash: "tx-201-testnet", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"test"}]`), Value: json.RawMessage(`{"v":9}`), CreatedAt: baseTime,
		},
	})
	require.NoError(t, err)

	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "G_KEEP", EventID: "0000200-0001-0000", Role: "topic"},
		{Address: "G_SHARED", EventID: "0000201-0001-0000", Role: "topic"},
	}))

	_, err = st.DeadLetterEvent(ctx, DeadLetterInput{
		EventID: "dl-main-201", ContractID: "CCONTRACT_MAIN", Ledger: 201, Type: "contract", TxHash: "tx-dl-main", Err: errors.New("main err"),
	})
	require.NoError(t, err)
	_, err = st.DeadLetterEvent(ctx, DeadLetterInput{
		EventID: "dl-test-201", ContractID: "CCONTRACT_TEST", Ledger: 201, Type: "contract", TxHash: "tx-dl-test", Err: errors.New("test err"),
	})
	require.NoError(t, err)

	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
		ContractID: "CCONTRACT_MAIN", LastIngestedLedger: 205, LastCursor: "main-cur",
	}))
	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
		ContractID: "CCONTRACT_TEST", LastIngestedLedger: 205, LastCursor: "test-cur",
	}))
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		Network: "mainnet", LastIngestedLedger: 205, LastCursor: "global-cur",
	}))
	require.NoError(t, st.SaveAuditState(ctx, AuditState{
		Network: "mainnet", VerifiedThroughLedger: 202,
	}))

	canonical := []Event{
		{
			Network: "mainnet", ID: "0000200-0001-0000", ContractID: "CCONTRACT_MAIN",
			Ledger: 200, Type: "contract", TxHash: "tx-200-unchanged", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"keep"}]`), Value: json.RawMessage(`{"v":1}`), CreatedAt: baseTime,
		},
	}

	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:         "mainnet",
		FromLedger:      200,
		ToLedger:        202,
		CanonicalEvents: canonical,
		CanonicalAddressRefs: []AddressRef{
			{Address: "G_KEEP", EventID: "0000200-0001-0000", Role: "topic"},
		},
		NewHeadLedger: 200,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"0000201-0001-0000"}, res.RetractedEventIDs)
	assert.Equal(t, int64(1), res.RetractedDeadLetters)
	assert.Equal(t, 1, res.RewoundContractCursors)
	assert.Equal(t, int64(200), res.RewoundIngestionLedger)
	assert.Equal(t, int64(199), res.RewoundAuditLedger)

	// Verify RawXDR preserved on unchanged event 0000200-0001-0000.
	ev200, err := st.GetEvent(ctx, "0000200-0001-0000", WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAAA"}, ev200.RawTopicXDR)
	assert.Equal(t, "BBBB", ev200.RawValueXDR)

	// Verify testnet dead letter and cursor remain untouched.
	dlCount, err := st.CountDeadLetters(ctx, "CCONTRACT_TEST")
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlCount)

	testCur, err := st.GetContractCursor(ctx, "CCONTRACT_TEST")
	require.NoError(t, err)
	assert.Equal(t, int64(205), testCur.LastIngestedLedger)
	assert.Equal(t, "test-cur", testCur.LastCursor)
}
