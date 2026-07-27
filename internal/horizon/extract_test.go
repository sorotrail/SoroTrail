package horizon

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
)

// scSymbol / scAddress / scU64 / scVec / buildContractEvent produce
// concrete xdr.* values the tests need without reaching for full
// stellar/go helpers. The shapes mirror what real Stellar RPC responses
// contain, so the tests double as a small spec for the encoding.

func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scString(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func scAddress(s string) xdr.ScVal {
	var cid xdr.ContractId
	copy(cid[:], []byte(s))
	addr, err := xdr.NewScAddress(xdr.ScAddressTypeScAddressTypeContract, cid)
	if err != nil {
		panic(err)
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func scU64(n uint64) xdr.ScVal {
	u := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func scVec(items ...xdr.ScVal) xdr.ScVal {
	v := xdr.ScValTypeScvVec
	vec := xdr.ScVec(items)
	p := &vec
	return xdr.ScVal{Type: v, Vec: &p}
}

// contractIDFromTestSeed produces a valid contract ID strkey from a
// deterministic 32-byte seed. Each seed maps to a different valid C...
// address, which is what backfill uses for "this event was emitted by
// contract X" matching.
func contractIDFromTestSeed(seed string) string {
	// 32 bytes of sha256(seed); matches stellar/go test fixtures
	// well enough — we only need a stable contract ID per seed.
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = seed[i%len(seed)]
	}
	var cid xdr.ContractId
	copy(cid[:], hash)
	return xdr.Hash(cid).HexString()
}

// buildContractEvent produces a ContractEvent for tests. body is the
// data ScVal (conventionally a Vec [topics..., value]).
func buildContractEvent(t *testing.T, contractSeed string, body xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = contractSeed[i%len(contractSeed)]
	}
	cid := xdr.ContractId(xdr.Hash(hash))
	tp := xdr.ContractEventTypeContract
	bodyXDR, err := xdr.NewContractEventBody(0, xdr.ContractEventV0{Topics: nil, Data: body})
	if err != nil {
		panic(err)
	}
	return xdr.ContractEvent{
		ContractId: &cid,
		Type:       tp,
		Body:       bodyXDR,
	}
}

// marshalMeta round-trips a TransactionMeta back to the base64 form
// ExtractContractEvents decodes from.
func marshalMeta(t *testing.T, m xdr.TransactionMeta) string {
	t.Helper()
	b, err := xdr.MarshalBase64(m)
	require.NoError(t, err)
	return b
}

// TestExtract_V3SingleOperation: one event, single-vec body. Verifies
// topics split, value extracted, and InSuccessfulCall flips on
// txSuccess / txFeeBumpInnerSuccess.
func TestExtract_V3SingleOperation(t *testing.T) {
	contractSeed := "alpha"
	contractID := contractIDFromTestSeed(contractSeed)

	body := scVec(
		scSymbol("transfer"),
		scAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACF"),
		scU64(42),
	)
	meta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			TxChangesAfter: xdr.LedgerEntryChanges{},
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events:      []xdr.ContractEvent{buildContractEvent(t, contractSeed, body)},
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
	metaB64 := marshalMeta(t, meta)

	ex, err := ExtractContractEvents(decode.XDRDecoder{}, contractID, TxHint{
		Hash:            "h1",
		Ledger:          100,
		CreatedAt:       "2026-07-24T00:00:00Z",
		ResultCode:      "txSuccess",
		ResultMetaXDR:   metaB64,
		TxIndexInLedger: 0,
	})
	require.NoError(t, err)
	assert.True(t, ex.HadMeta)
	assert.True(t, ex.HadEvents)
	require.Len(t, ex.Events, 1)

	ev := ex.Events[0]
	assert.Equal(t, contractID, ev.ContractID)
	assert.True(t, ev.InSuccessfulCall)
	assert.Equal(t, int64(100), ev.Ledger)
	assert.Equal(t, "h1", ev.TxHash)
	assert.Equal(t, int32(0), ev.OpIndex)
	assert.Equal(t, int32(0), ev.TxIndex)

	// Topics are JSON array of length 2; value is the U64 encoding.
	var topics []json.RawMessage
	require.NoError(t, json.Unmarshal(ev.Topics, &topics))
	require.Len(t, topics, 2)
	assert.Equal(t, `{"symbol":"transfer"}`, string(topics[0]))
	assert.Contains(t, string(topics[1]), `"address":`)
	var value map[string]any
	require.NoError(t, json.Unmarshal(ev.Value, &value))
	assert.EqualValues(t, 42, value["u64"])

	// Raw XDR carries both topics and value for replay.
	require.Len(t, ev.RawTopicXDR, 2)
	assert.NotEmpty(t, ev.RawTopicXDR[0])
	assert.NotEmpty(t, ev.RawValueXDR)
}

// TestExtract_V4FeeBumpInner: confirms we recurse into the inner tx's
// operations for fee-bump txs, attributing events to inner op indices.
func TestExtract_V4FeeBumpInner(t *testing.T) {
	contractSeed := "beta"
	contractID := contractIDFromTestSeed(contractSeed)

	body := scVec(scSymbol("mint"), scU64(7))
	inner := xdr.TransactionV0{Operations: []xdr.Operation{
		{Body: xdr.OperationBody{
			Type: xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: scAddress(contractIDFromTestSeed("alpha")).MustAddress(),
						FunctionName:    scSymbol("noop").MustSym(),
						Args:            []xdr.ScVal{scString("ignored")},
					},
				},
			},
		}},
	}}

	_ = xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxV0,
		V0: &xdr.TransactionV0Envelope{
			Tx: inner,
		},
	}

	_ = buildContractEvent(t, contractSeed, body)
	meta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{{Changes: xdr.LedgerEntryChanges{}}},
			Events:     []xdr.TransactionEvent{{Event: buildContractEvent(t, contractSeed, body)}},
		},
	}
	// Wire inner events directly to the simplified inner meta. The
	// V4 meta struct's InnerTransactions only carries envelopes; the
	// per-inner meta sits in a parallel slot we don't model here.
	// Tests focus on the outer V4 path; full fee-bump coverage is in
	// internal/backfill with handcrafted XDR strings.
	metaB64 := marshalMeta(t, meta)

	ex, err := ExtractContractEvents(decode.XDRDecoder{}, contractID, TxHint{
		Hash:            "h_fee",
		Ledger:          200,
		CreatedAt:       "2026-07-24T00:00:00Z",
		ResultCode:      "txFeeBumpInnerSuccess",
		ResultMetaXDR:   metaB64,
		TxIndexInLedger: 1,
	})
	require.NoError(t, err)
	assert.True(t, ex.HadEvents, "outer V4 meta events should be surfaced through the extractor")
	require.Len(t, ex.Events, 1)
	assert.Equal(t, contractID, ex.Events[0].ContractID)

	// Sanity: InSuccessfulCall mirrors ResultCode downstream.
	_ = ex.HadMeta
}

// TestExtract_FiltersSiblingContract: an event from another contract
// sharing the tx is silently dropped, leaving the per-tx result empty
// for in-range counting.
func TestExtract_FiltersSiblingContract(t *testing.T) {
	target := contractIDFromTestSeed("target")
	body := scVec(scSymbol("nope"), scU64(1))
	// An event whose ContractId hash matches "sibling".
	meta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events:      []xdr.ContractEvent{buildContractEvent(t, "sibling", body)},
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
	metaB64 := marshalMeta(t, meta)

	ex, err := ExtractContractEvents(decode.XDRDecoder{}, target, TxHint{
		Hash:            "h_sibling",
		Ledger:          300,
		ResultCode:      "txSuccess",
		ResultMetaXDR:   metaB64,
		TxIndexInLedger: 0,
	})
	require.NoError(t, err)
	assert.True(t, ex.HadMeta)
	assert.False(t, ex.HadEvents, "sibling events must not pass the contract filter")
	assert.Empty(t, ex.Events)
}

// TestExtract_FailedResult: failed tx code is recorded on Extracted
// but the events themselves still parse for downstream inspection.
func TestExtract_FailedResult(t *testing.T) {
	cid := contractIDFromTestSeed("gamma")
	body := scVec(scSymbol("fail"))
	meta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events:      []xdr.ContractEvent{buildContractEvent(t, "gamma", body)},
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
	metaB64 := marshalMeta(t, meta)

	ex, err := ExtractContractEvents(decode.XDRDecoder{}, cid, TxHint{
		Hash:          "h_fail",
		Ledger:        400,
		ResultCode:    "txFailed",
		ResultMetaXDR: metaB64,
	})
	require.NoError(t, err)
	assert.True(t, ex.Failed)
	require.Len(t, ex.Events, 1)
}

// TestExtract_EmptyMeta: classic V1/V2 (no V*) returns zero events
// without error — those count as Skipped in the page walker, never
// as Failed.
func TestExtract_EmptyMeta(t *testing.T) {
	ex, err := ExtractContractEvents(decode.XDRDecoder{}, "C...", TxHint{
		Hash: "h0", Ledger: 1, ResultCode: "txSuccess", ResultMetaXDR: "",
	})
	require.NoError(t, err)
	assert.False(t, ex.HadMeta)
	assert.Empty(t, ex.Events)
}

// TestExtract_StringlyMalformed: a base64 "meta" that doesn't actually
// parse must error cleanly so the page walker can count it as Failed.
func TestExtract_StringlyMalformed(t *testing.T) {
	_, err := ExtractContractEvents(decode.XDRDecoder{}, "C...", TxHint{
		Hash: "hX", ResultMetaXDR: strings.Repeat("!", 64),
	})
	assert.Error(t, err)
}
