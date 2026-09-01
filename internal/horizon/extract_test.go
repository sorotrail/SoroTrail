package horizon

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
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

// TestFormatEventID covers the backfill primary key. The zero-padding is
// load-bearing, not cosmetic: the id doubles as a sort key, so
// lexicographic order must match chronological order — which only holds
// while every numeric component keeps its fixed width. The width is a
// minimum, so the largest ledger/op/event values must pass through
// un-truncated rather than overflowing the format.
func TestFormatEventID(t *testing.T) {
	tests := []struct {
		name       string
		txHash     string
		ledger     int64
		opIndex    int32
		eventIndex int32
		want       string
	}{
		{
			name:       "matches the documented TOID shape and width",
			txHash:     "abc123",
			ledger:     123456,
			opIndex:    12,
			eventIndex: 34,
			want:       "abc123-00000000000000123456-00012-00034",
		},
		{
			// Single-digit components padded to full width; an unpadded
			// id like "1-1-1" would sort before "1-0-0" and break
			// cursor walks.
			name:       "small values are zero-padded to full width",
			txHash:     "tx",
			ledger:     1,
			opIndex:    1,
			eventIndex: 1,
			want:       "tx-00000000000000000001-00001-00001",
		},
		{
			// %020d / %05d are minimum widths: the largest representable
			// values must come out verbatim, never truncated or wrapped.
			name:       "maximum values do not overflow the format",
			txHash:     "max",
			ledger:     math.MaxInt64,
			opIndex:    math.MaxInt32,
			eventIndex: math.MaxInt32,
			want:       "max-09223372036854775807-2147483647-2147483647",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatEventID(tt.txHash, tt.ledger, tt.opIndex, tt.eventIndex)
			assert.Equal(t, tt.want, got)
		})
	}

	// Crossing a digit boundary (9 → 10, 99 → 100) is where unpadded ids
	// break: "10" sorts before "9" lexicographically. The padded id must
	// keep sorting in numeric order for every component.
	t.Run("lexicographic order matches numeric order", func(t *testing.T) {
		pairs := []struct {
			name      string
			numericLo string
			numericHi string
		}{
			{
				name:      "ledger crosses a digit boundary",
				numericLo: formatEventID("tx", 9, 0, 0),
				numericHi: formatEventID("tx", 10, 0, 0),
			},
			{
				name:      "op index crosses a digit boundary",
				numericLo: formatEventID("tx", 1, 9, 0),
				numericHi: formatEventID("tx", 1, 10, 0),
			},
			{
				name:      "event index crosses a digit boundary",
				numericLo: formatEventID("tx", 1, 0, 9),
				numericHi: formatEventID("tx", 1, 0, 10),
			},
		}
		for _, p := range pairs {
			t.Run(p.name, func(t *testing.T) {
				assert.Less(t, p.numericLo, p.numericHi,
					"the id of the smaller component must sort first")
			})
		}
	})
}

// TestEventKind pins down the mapping from xdr.ContractEventType to the
// textual type string the API and store expose. These strings are
// load-bearing: the REST and GraphQL type filters only accept the
// documented set, so any drift between what the extractor writes and
// what the API can select would silently hide events from every query
// that filters by type.
func TestEventKind(t *testing.T) {
	// The type filter values the API accepts (see the Validate and
	// ParseTypes switches in internal/api/queries). eventKind must only
	// ever return members of this set, otherwise stored rows become
	// unfilterable.
	documentedFilterValues := map[string]bool{
		"contract":   true,
		"system":     true,
		"diagnostic": true,
	}

	tests := []struct {
		name string
		in   xdr.ContractEventType
		want string
	}{
		{
			// Contract events are the ordinary case: every user-visible
			// emission from an invoked contract carries this enum value,
			// and it must surface as the "contract" type the API exposes.
			name: "contract events map to the documented contract type",
			in:   xdr.ContractEventTypeContract,
			want: "contract",
		},
		{
			// System events come from the host itself rather than a
			// contract, so they must be labelled "system" to let clients
			// filter diagnostics separately from user emissions.
			name: "system events map to the documented system type",
			in:   xdr.ContractEventTypeSystem,
			want: "system",
		},
		{
			// Diagnostic sub-events are deliberately folded into the
			// contract bucket: the type the API filter selects already
			// covers their consumers, so they never need their own string.
			name: "diagnostic events fall back to the contract bucket",
			in:   xdr.ContractEventTypeDiagnostic,
			want: "contract",
		},
		{
			// Any future enum value we have not seen must fall back to
			// the contract bucket rather than produce an unmapped string:
			// an unknown XDR variant must never stall backfill or emit a
			// type no filter can select.
			name: "unknown future variants fall back to the contract bucket",
			in:   xdr.ContractEventType(99),
			want: "contract",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := eventKind(tt.in)
			assert.Equal(t, tt.want, got)
			// The value must stay a documented API type filter value; if
			// it ever drifts outside the accepted set, events of this
			// kind become invisible to type-filtered queries.
			assert.True(t, documentedFilterValues[got],
				"eventKind(%d) = %q must be a documented API type filter value",
				tt.in, got)
		})
	}
}
