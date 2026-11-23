package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReorgTestSQLiteStore(t *testing.T) Store {
	t.Helper()
	st := newSQLiteStore(t)
	sq := st.(*SQLite)
	_, err := sq.db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS event_addresses (
			address  TEXT NOT NULL,
			event_id TEXT NOT NULL,
			role     TEXT NOT NULL,
			PRIMARY KEY (address, event_id, role)
		);
		CREATE TABLE IF NOT EXISTS dead_letters (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id     TEXT NOT NULL UNIQUE,
			contract_id  TEXT NOT NULL,
			ledger       INTEGER NOT NULL,
			type         TEXT NOT NULL,
			tx_hash      TEXT,
			topic_xdr    TEXT,
			value_xdr    TEXT,
			error        TEXT NOT NULL,
			attempts     INTEGER NOT NULL DEFAULT 1,
			last_attempt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	require.NoError(t, err)
	return st
}

func TestReconcileReorgRange_EmptyReplacementRetractsAll(t *testing.T) {
	st := newReorgTestSQLiteStore(t)
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
		{
			Network:          "default",
			ID:               "0000106-0001-0000",
			ContractID:       "CCONTRACT_B",
			Ledger:           106,
			Type:             "contract",
			TxHash:           "tx-106-fork",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"mint"}]`),
			Value:            json.RawMessage(`{"amount":99}`),
			CreatedAt:        baseTime.Add(6 * time.Second),
		},
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "GADDR_SURVIVE", EventID: "0000100-0001-0000", Role: "topic"},
		{Address: "GADDR_ORPHAN", EventID: "0000105-0001-0000", Role: "topic"},
		{Address: "GADDR_ORPHAN", EventID: "0000106-0001-0000", Role: "value"},
	}))

	_, err = st.(*SQLite).db.ExecContext(ctx, `
		INSERT INTO dead_letters (event_id, contract_id, ledger, type, tx_hash, error)
		VALUES (?, ?, ?, ?, ?, ?)`,
		"0000105-9999-0000", "CCONTRACT_A", 105, "contract", "tx-dead-105", "decode error on orphaned fork",
	)
	require.NoError(t, err)

	// Reorg [105, 106] to an empty canonical range (0 events).
	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:         "default",
		FromLedger:      105,
		ToLedger:        106,
		CanonicalEvents: nil,
		NewHeadLedger:   106,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"0000105-0001-0000", "0000106-0001-0000"}, res.RetractedEventIDs)
	assert.Empty(t, res.InsertedEventIDs)
	assert.Equal(t, int64(2), res.RetractedAddressRefs)
	assert.Equal(t, int64(1), res.RetractedDeadLetters)

	// Only ledger 100 event remains.
	remaining, _, err := st.QueryEvents(ctx, EventFilter{Scope: WildcardScope(), Limit: 100})
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	assert.Equal(t, "0000100-0001-0000", remaining[0].ID)

	// Orphaned address has 0 events; surviving address still has 1.
	orphanCount, err := st.CountAddressEvents(ctx, "GADDR_ORPHAN")
	require.NoError(t, err)
	assert.Equal(t, int64(0), orphanCount)

	surviveCount, err := st.CountAddressEvents(ctx, "GADDR_SURVIVE")
	require.NoError(t, err)
	assert.Equal(t, int64(1), surviveCount)

	// Dead letter in [105, 106] was retracted.
	dlCount, err := st.CountDeadLetters(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlCount)

	// CCONTRACT_B now has no events and must disappear from GetContractSummary.
	_, err = st.GetContractSummary(ctx, "CCONTRACT_B")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestReconcileReorgRange_DerivedStateRetractionAndCursors(t *testing.T) {
	st := newReorgTestSQLiteStore(t)
	ctx := context.Background()

	baseTime := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	initial := []Event{
		{
			Network:          "default",
			ID:               "0000200-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           200,
			Type:             "contract",
			TxHash:           "tx-200-unchanged",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"keep"}]`),
			Value:            json.RawMessage(`{"v":1}`),
			RawTopicXDR:      []string{"AAAA"},
			RawValueXDR:      "BBBB",
			CreatedAt:        baseTime,
		},
		{
			Network:          "default",
			ID:               "0000201-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           201,
			Type:             "contract",
			TxHash:           "tx-201-old-fork",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"old_topic"}]`),
			Value:            json.RawMessage(`{"v":2}`),
			CreatedAt:        baseTime.Add(time.Second),
		},
		{
			Network:          "default",
			ID:               "0000202-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           202,
			Type:             "contract",
			TxHash:           "tx-202-retracted",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"gone"}]`),
			Value:            json.RawMessage(`{"v":3}`),
			CreatedAt:        baseTime.Add(2 * time.Second),
		},
	}
	_, err := st.UpsertEvents(ctx, initial)
	require.NoError(t, err)

	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "G_KEEP", EventID: "0000200-0001-0000", Role: "topic"},
		{Address: "G_OLD_201", EventID: "0000201-0001-0000", Role: "topic"},
		{Address: "G_GONE_202", EventID: "0000202-0001-0000", Role: "value"},
	}))

	// Seed contract cursor, ingestion state, and audit state inside/past [200, 202].
	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
		ContractID:         "CCONTRACT_A",
		LastIngestedLedger: 205,
		LastCursor:         "opaque-fork-cursor",
	}))
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		Network:            "default",
		LastIngestedLedger: 205,
		LastCursor:         "global-fork-cursor",
	}))
	require.NoError(t, st.SaveAuditState(ctx, AuditState{
		Network:               "default",
		VerifiedThroughLedger: 202,
	}))

	// Canonical replacement for [200, 202]:
	// - 0000200-0001-0000 is unchanged (and arrives without RawXDR, which should be preserved)
	// - 0000201-0001-0000 has a different tx_hash and topics (reorged at same TOID slot)
	// - 0000202-0001-0000 is retracted completely
	// - 0000201-0002-0000 is newly added on the canonical fork
	canonical := []Event{
		{
			Network:          "default",
			ID:               "0000200-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           200,
			Type:             "contract",
			TxHash:           "tx-200-unchanged",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"keep"}]`),
			Value:            json.RawMessage(`{"v":1}`),
			CreatedAt:        baseTime,
		},
		{
			Network:          "default",
			ID:               "0000201-0001-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           201,
			Type:             "contract",
			TxHash:           "tx-201-new-fork",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"new_topic"}]`),
			Value:            json.RawMessage(`{"v":20}`),
			CreatedAt:        baseTime.Add(time.Second),
		},
		{
			Network:          "default",
			ID:               "0000201-0002-0000",
			ContractID:       "CCONTRACT_A",
			Ledger:           201,
			Type:             "contract",
			TxHash:           "tx-201-new-2",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"added"}]`),
			Value:            json.RawMessage(`{"v":21}`),
			CreatedAt:        baseTime.Add(time.Second),
		},
	}

	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:         "default",
		FromLedger:      200,
		ToLedger:        202,
		CanonicalEvents: canonical,
		CanonicalAddressRefs: []AddressRef{
			{Address: "G_KEEP", EventID: "0000200-0001-0000", Role: "topic"},
			{Address: "G_NEW_201", EventID: "0000201-0001-0000", Role: "topic"},
			{Address: "G_ADDED_201", EventID: "0000201-0002-0000", Role: "value"},
		},
		NewHeadLedger: 201,
	})
	require.NoError(t, err)

	// 0000200-0001-0000 was unchanged, so only 0000201-0001-0000 (modified) and
	// 0000202-0001-0000 (removed) are reported in RetractedEventIDs.
	assert.Equal(t, []string{"0000201-0001-0000", "0000202-0001-0000"}, res.RetractedEventIDs)
	assert.Equal(t, []string{"0000200-0001-0000", "0000201-0001-0000", "0000201-0002-0000"}, res.InsertedEventIDs)
	assert.Equal(t, 1, res.RewoundContractCursors)
	assert.Equal(t, int64(201), res.RewoundIngestionLedger)
	assert.Equal(t, int64(199), res.RewoundAuditLedger)

	// Verify RawXDR preserved on unchanged event 0000200-0001-0000.
	ev200, err := st.GetEvent(ctx, "0000200-0001-0000", WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAAA"}, ev200.RawTopicXDR)
	assert.Equal(t, "BBBB", ev200.RawValueXDR)

	// Verify old address refs are gone and new ones are present.
	oldCount, err := st.CountAddressEvents(ctx, "G_OLD_201")
	require.NoError(t, err)
	assert.Equal(t, int64(0), oldCount)

	goneCount, err := st.CountAddressEvents(ctx, "G_GONE_202")
	require.NoError(t, err)
	assert.Equal(t, int64(0), goneCount)

	newCount, err := st.CountAddressEvents(ctx, "G_NEW_201")
	require.NoError(t, err)
	assert.Equal(t, int64(1), newCount)

	// Verify contract cursor clamped to 201 and opaque cursor cleared.
	cc, err := st.GetContractCursor(ctx, "CCONTRACT_A")
	require.NoError(t, err)
	assert.Equal(t, int64(201), cc.LastIngestedLedger)
	assert.Empty(t, cc.LastCursor)

	// Verify ingestion_state clamped to 201 and cursor cleared.
	ingState, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(201), ingState.LastIngestedLedger)
	assert.Empty(t, ingState.LastCursor)

	// Verify audit_state clamped to FromLedger-1 = 199.
	auditState, err := st.GetAuditState(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(199), auditState.VerifiedThroughLedger)
}

func TestReconcileReorgRange_RevertAndReapplyIdempotent(t *testing.T) {
	st := newReorgTestSQLiteStore(t)
	ctx := context.Background()

	ev := Event{
		Network:          "default",
		ID:               "0000300-0001-0000",
		ContractID:       "CCONTRACT_IDEMP",
		Ledger:           300,
		Type:             "contract",
		TxHash:           "tx-300",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"swap"}]`),
		Value:            json.RawMessage(`{"amount":42}`),
		CreatedAt:        time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
	}
	refs := []AddressRef{
		{Address: "G_SWAPPER", EventID: ev.ID, Role: "topic"},
	}

	_, err := st.UpsertEvents(ctx, []Event{ev})
	require.NoError(t, err)
	require.NoError(t, st.UpsertAddressRefs(ctx, refs))

	// Step 1: Revert ledger 300.
	_, err = ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:    "default",
		FromLedger: 300,
		ToLedger:   300,
	})
	require.NoError(t, err)

	cnt, err := st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(0), cnt)

	// Step 2: Re-apply ledger 300 with duplicate entries in the input slice.
	_, err = ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:              "default",
		FromLedger:           300,
		ToLedger:             300,
		CanonicalEvents:      []Event{ev, ev},
		CanonicalAddressRefs: []AddressRef{refs[0], refs[0]},
	})
	require.NoError(t, err)

	// Step 3: Re-apply ledger 300 a second time without reverting in between.
	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:              "default",
		FromLedger:           300,
		ToLedger:             300,
		CanonicalEvents:      []Event{ev},
		CanonicalAddressRefs: refs,
	})
	require.NoError(t, err)
	assert.Empty(t, res.RetractedEventIDs, "re-applying identical ledger must not flag retractions")

	cnt, err = st.CountEvents(ctx, EventFilter{Scope: WildcardScope()})
	require.NoError(t, err)
	assert.Equal(t, int64(1), cnt)

	addrCnt, err := st.CountAddressEvents(ctx, "G_SWAPPER")
	require.NoError(t, err)
	assert.Equal(t, int64(1), addrCnt)
}

func TestReconcileReorgRange_TransactionalRollbackOnFault(t *testing.T) {
	st := newReorgTestSQLiteStore(t)
	ctx := context.Background()

	ev := Event{
		Network:          "default",
		ID:               "0000400-0001-0000",
		ContractID:       "CCONTRACT_TX",
		Ledger:           400,
		Type:             "contract",
		TxHash:           "tx-400-original",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"orig"}]`),
		Value:            json.RawMessage(`{"v":1}`),
		CreatedAt:        time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
	}
	_, err := st.UpsertEvents(ctx, []Event{ev})
	require.NoError(t, err)
	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "G_ORIG_TX", EventID: ev.ID, Role: "topic"},
	}))
	require.NoError(t, st.SaveAuditState(ctx, AuditState{
		Network:               "default",
		VerifiedThroughLedger: 405,
	}))

	// Inject a SQLite trigger that aborts when ReconcileReorgTx reaches the final
	// audit_state clamp step, after events and event_addresses have already been
	// mutated inside the transaction.
	_, err = st.(*SQLite).db.ExecContext(ctx, `
		CREATE TRIGGER abort_audit_state_update
		BEFORE UPDATE ON audit_state
		BEGIN
			SELECT RAISE(ABORT, 'simulated mid-reorg crash');
		END;
	`)
	require.NoError(t, err)

	_, err = ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:    "default",
		FromLedger: 400,
		ToLedger:   400,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated mid-reorg crash")

	// Because ReconcileReorgTx runs all steps inside a single SQL transaction,
	// both the original event and its address reference must still be intact.
	gotEv, err := st.GetEvent(ctx, ev.ID, WildcardScope())
	require.NoError(t, err)
	assert.Equal(t, "tx-400-original", gotEv.TxHash)

	addrCnt, err := st.CountAddressEvents(ctx, "G_ORIG_TX")
	require.NoError(t, err)
	assert.Equal(t, int64(1), addrCnt)
}

func TestReconcileReorgRange_MultiNetworkIsolation(t *testing.T) {
	st := newReorgTestSQLiteStore(t)
	ctx := context.Background()

	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{Network: "mainnet", ContractID: "CCONTRACT_MAIN"}))
	require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{Network: "testnet", ContractID: "CCONTRACT_TEST"}))

	baseTime := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	_, err := st.UpsertEvents(ctx, []Event{
		{
			Network: "mainnet", ID: "shared-id-500", ContractID: "CCONTRACT_MAIN",
			Ledger: 500, Type: "contract", TxHash: "tx-main-500", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"main"}]`), Value: json.RawMessage(`{"v":1}`), CreatedAt: baseTime,
		},
		{
			Network: "testnet", ID: "shared-id-500", ContractID: "CCONTRACT_TEST",
			Ledger: 500, Type: "contract", TxHash: "tx-test-500", InSuccessfulCall: true,
			Topics: json.RawMessage(`[{"symbol":"test"}]`), Value: json.RawMessage(`{"v":2}`), CreatedAt: baseTime,
		},
	})
	require.NoError(t, err)
	require.NoError(t, st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: "G_SHARED", EventID: "shared-id-500", Role: "topic"},
	}))

	_, err = st.(*SQLite).db.ExecContext(ctx, `
		INSERT INTO dead_letters (event_id, contract_id, ledger, type, tx_hash, error)
		VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)`,
		"dl-main-500", "CCONTRACT_MAIN", 500, "contract", "tx-dl-main", "main err",
		"dl-test-500", "CCONTRACT_TEST", 500, "contract", "tx-dl-test", "test err",
	)
	require.NoError(t, err)

	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
		ContractID: "CCONTRACT_MAIN", LastIngestedLedger: 505, LastCursor: "main-cur",
	}))
	require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
		ContractID: "CCONTRACT_TEST", LastIngestedLedger: 505, LastCursor: "test-cur",
	}))

	// Reconcile mainnet [500, 500] to empty with NewHeadLedger=500.
	res, err := ReconcileReorgRange(ctx, st, ReorgReconcileOptions{
		Network:       "mainnet",
		FromLedger:    500,
		ToLedger:      500,
		NewHeadLedger: 500,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"shared-id-500"}, res.RetractedEventIDs)
	assert.Equal(t, int64(0), res.RetractedAddressRefs, "shared event_id still present on testnet must keep its address ref")
	assert.Equal(t, int64(1), res.RetractedDeadLetters, "only mainnet dead letter must be deleted")
	assert.Equal(t, 1, res.RewoundContractCursors, "only mainnet contract cursor must be rewound")

	// Testnet dead letter and cursor must remain untouched!
	dlCount, err := st.CountDeadLetters(ctx, "CCONTRACT_TEST")
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlCount)

	testCur, err := st.GetContractCursor(ctx, "CCONTRACT_TEST")
	require.NoError(t, err)
	assert.Equal(t, int64(505), testCur.LastIngestedLedger)
	assert.Equal(t, "test-cur", testCur.LastCursor)
}
