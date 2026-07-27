// Package horizon — meta XDR extraction.
//
// Given one Horizon transaction's base64-encoded `result_meta_xdr`,
// produce store.Event rows in the same shape the live ingester writes,
// so backfilled events are indistinguishable from live ingestion: same
// columns, raw XDR columns populated, decodable JSON shapes identical.
//
// The XDR layouts handled are:
//
//   - xdr.TransactionMeta V3 → txChangesAfter.Events
//   - xdr.TransactionMeta V4 → Operations[].Events plus
//     InnerTransactions[].Operations[].Events (Soroban fee-bumps wrap
//     a single inner tx; events emitted by the wrap's inner op belong
//     to the inner op's index, not the outer wrap's).
//   - V1/V2 → empty (pre-Soroban; counted as Skipped, never as Failed).
//
// Event ID format differs from live ingest on purpose: Horizon does not
// expose RPC's TOID-derived event IDs directly, so backfill synthesizes
// an ID from (tx_hash, ledger, op_index, event_index), which is stable
// across re-runs of the same backfill. Live and backfill rows for the
// same on-chain emission therefore have different IDs in any overlap
// range; consumers dedupe on (tx_hash, op_index, event_index).
package horizon

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/stellar/go/xdr"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Extracted is one transaction's slice of events plus a small audit
// surface for the page walker to count Skipped / Failed rows accurately.
type Extracted struct {
	Events    []store.Event
	Failed    bool // result code was not the success variant
	HadMeta   bool // some ResultMetaXDR was supplied
	HadEvents bool // result was non-empty
}

// TxHint bundles the inputs needed to produce events for one Horizon
// transaction. Hash, Ledger, CreatedAt, and ResultMetaXDR come straight
// from the Horizon row. ResultCode is the tx-level result; we use it to
// set InSuccessfulCall. TxIndexInLedger is supplied by the page walker
// (Horizon returns transactions in ledger/transaction order, so the
// counter resets when the running ledger changes).
//
// Exported because backfill.go builds it from a Horizon Transaction
// without needing access to internals.
type TxHint struct {
	Hash            string
	Ledger          int64
	CreatedAt       string
	ResultCode      string
	ResultMetaXDR   string
	TxIndexInLedger int32
}

// DecodeMeta unmarshals a base64 xdr.TransactionMeta. Exposed so tests
// produce valid fixtures; the Backfiller never calls it directly.
func DecodeMeta(base64XDR string) (xdr.TransactionMeta, error) {
	var meta xdr.TransactionMeta
	if base64XDR == "" {
		return meta, fmt.Errorf("empty meta")
	}
	if err := xdr.SafeUnmarshalBase64(base64XDR, &meta); err != nil {
		return meta, fmt.Errorf("unmarshaling transaction meta: %w", err)
	}
	return meta, nil
}

// formatEventID produces a stable primary key for a backfilled event:
//
//	{tx_hash}-{ledger:020d}-{op_index:05d}-{event_index:05d}
//
// Stable across re-runs (idempotent upsert), unique per (tx, op, event),
// and carries enough structure to dedupe against live ingest via
// (tx_hash, op_index, event_index) at the API layer.
func formatEventID(txHash string, ledger int64, opIndex, eventIndex int32) string {
	return fmt.Sprintf("%s-%020d-%05d-%05d", txHash, ledger, opIndex, eventIndex)
}

// ExtractContractEvents converts one Horizon transaction's meta into
// store.Event rows. contractID is the contract we're backfilling for;
// events emitted by sibling contracts on the same transaction are
// dropped here even though Horizon returned the row — the Horizon
// participants set for the tx included contractID, but the emission
// itself belonged to a different code path.
//
// dx is supplied directly so this function holds no package state;
// tests can pass any decoder and the production path shares the same
// XDRDecoder the ingester uses.
func ExtractContractEvents(dx decode.Decoder, contractID string, tx TxHint) (Extracted, error) {
	out := Extracted{
		Failed: tx.ResultCode != "" &&
			tx.ResultCode != "txSuccess" &&
			tx.ResultCode != "txFeeBumpInnerSuccess",
	}
	if tx.ResultMetaXDR == "" {
		return out, nil
	}
	out.HadMeta = true

	meta, err := DecodeMeta(tx.ResultMetaXDR)
	if err != nil {
		return out, err
	}

	createdAt, _ := time.Parse(time.RFC3339, tx.CreatedAt)

	switch meta.V {
	case 3:
		if meta.V3 != nil {
			if meta.V3.SorobanMeta != nil {
				for i, ev := range meta.V3.SorobanMeta.Events {
					if row, ok := buildEvent(dx, contractID, tx, createdAt, ev, 0, int32(i)); ok {
						out.Events = append(out.Events, row)
					}
				}
			}
		}
	case 4:
		if meta.V4 != nil {
			for opIdx, op := range meta.V4.Operations {
				for evIdx, ev := range op.Events {
					if row, ok := buildEvent(dx, contractID, tx, createdAt, ev, int32(opIdx), int32(evIdx)); ok {
						out.Events = append(out.Events, row)
					}
				}
			}
			for _, te := range meta.V4.Events {
				if row, ok := buildEvent(dx, contractID, tx, createdAt, te.Event, 0, 0); ok {
					out.Events = append(out.Events, row)
				}
			}
		}
	default:
		// V1/V2 or unknown — classic, no events.
	}
	out.HadEvents = len(out.Events) > 0
	return out, nil
}

// buildEvent maps one xdr.ContractEvent to a store.Event. ok=false when
// the event is from a sibling contract or the XDR is unparseable.
func buildEvent(
	dx decode.Decoder,
	contractID string,
	tx TxHint,
	createdAt time.Time,
	ev xdr.ContractEvent,
	opIndex, eventIndex int32,
) (store.Event, bool) {
	if ev.ContractId == nil {
		return store.Event{}, false
	}
	if len(ev.ContractId) == 0 {
		return store.Event{}, false
	}
	candidate := xdr.Hash(*ev.ContractId).HexString()
	if contractID != "" && candidate != contractID {
		return store.Event{}, false
	}

	topics, value, rawTopicXDR, rawValueXDR, ok := eventPayloads(dx, ev)
	if !ok {
		return store.Event{}, false
	}

	return store.Event{
		ID:               formatEventID(tx.Hash, tx.Ledger, opIndex, eventIndex),
		ContractID:       candidate,
		Ledger:           tx.Ledger,
		Type:             eventKind(ev.Type),
		TxHash:           tx.Hash,
		TxIndex:          tx.TxIndexInLedger,
		OpIndex:          opIndex,
		InSuccessfulCall: tx.ResultCode == "txSuccess" || tx.ResultCode == "txFeeBumpInnerSuccess",
		Topics:           topics,
		Value:            value,
		CreatedAt:        createdAt,
		RawTopicXDR:      rawTopicXDR,
		RawValueXDR:      rawValueXDR,
	}, true
}

// eventKind maps xdr.ContractEventType to the textual type the API
// exposes. SYSTEM covers Diagnostic sub-events; unknown future variants
// are bucketed as CONTRACT so a new XDR field never stalls backfill.
func eventKind(t xdr.ContractEventType) string {
	if t == xdr.ContractEventTypeSystem {
		return "system"
	}
	return "contract"
}

// eventPayloads takes a ContractEvent's data ScVal and routes each
// element through the same decode.Decoder pipeline the live ingester
// uses, returning JSON topics array, decoded JSON value, raw base64
// XDR slices, and ok=false when extraction fails.
//
// ContractEvent.Data is a Vec [topic0, topic1, ..., value]. We treat
// all leading elements as topics (decoded individually) and the last
// element as the value.
func eventPayloads(dx decode.Decoder, ev xdr.ContractEvent) (json.RawMessage, json.RawMessage, []string, string, bool) {
	body, ok := ev.Body.GetV0()
	if !ok {
		return nil, nil, nil, "", false
	}
	vec, ok := body.Data.GetVec()
	if !ok || vec == nil {
		// Edge: data isn't a Vec. Treat as zero topics, value=data.
		valB64, ok := encodeScVal(body.Data)
		if !ok {
			return nil, nil, nil, "", false
		}
		val, ok := decodeOne(dx, valB64)
		if !ok {
			return nil, nil, nil, "", false
		}
		return json.RawMessage("[]"), val, nil, valB64, true
	}
	if len(*vec) == 0 {
		return json.RawMessage("[]"), json.RawMessage("null"), nil, "", true
	}
	topics := make([]json.RawMessage, 0, len(*vec)-1)
	rawTopicXDR := make([]string, 0, len(*vec)-1)
	for i := 0; i < len(*vec)-1; i++ {
		base64Topic, _ := encodeScVal((*vec)[i]) // empty on encode failure → raw stored as empty
		t, ok := decodeOne(dx, base64Topic)
		if !ok {
			return nil, nil, nil, "", false
		}
		topics = append(topics, t)
		if base64Topic != "" {
			rawTopicXDR = append(rawTopicXDR, base64Topic)
		}
	}
	valB64, _ := encodeScVal((*vec)[len(*vec)-1])
	val, ok := decodeOne(dx, valB64)
	if !ok {
		return nil, nil, nil, "", false
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return nil, nil, nil, "", false
	}
	return topicsJSON, val, rawTopicXDR, valB64, true
}

// encodeScVal produces base64 XDR for a single ScVal. Returns false on
// marshal failure.
func encodeScVal(v xdr.ScVal) (string, bool) {
	b, err := xdr.MarshalBase64(v)
	if err != nil {
		return "", false
	}
	return b, true
}

// decodeOne runs one base64 XDR through the decoder. Empty input
// decodes to JSON null (matching decode.EventTopicsValue's value-is-empty
// branch).
func decodeOne(dx decode.Decoder, base64XDR string) (json.RawMessage, bool) {
	if base64XDR == "" {
		return json.RawMessage("null"), true
	}
	out, err := dx.DecodeScVal(base64XDR)
	if err != nil {
		return nil, false
	}
	return out, true
}
