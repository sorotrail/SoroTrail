package ingester

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// noopBenchStore is a zero-allocation store sink used by BenchmarkIngestHotPath
// so benchmark allocation counters measure the ingester's decode, marshal,
// batch deduplication, and address-extraction hot path rather than mockStore slice growth.
type noopBenchStore struct {
	mockStore
}

func (s *noopBenchStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	return int64(len(events)), nil
}

func (s *noopBenchStore) UpsertAddressRefs(_ context.Context, _ []store.AddressRef) error {
	return nil
}

func buildBenchTransferEvents(tb testing.TB, count int) []rpc.Event {
	tb.Helper()

	sym := xdr.ScSymbol("transfer")
	topicSym, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym})
	if err != nil {
		tb.Fatalf("marshal transfer symbol: %v", err)
	}

	fromAcct := xdr.MustAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
	topicFrom, err := xdr.MarshalBase64(xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &fromAcct,
		},
	})
	if err != nil {
		tb.Fatalf("marshal from address: %v", err)
	}

	toContract := xdr.ContractId{1, 2, 3, 4}
	topicTo, err := xdr.MarshalBase64(xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &toContract,
		},
	})
	if err != nil {
		tb.Fatalf("marshal to address: %v", err)
	}

	valI128, err := xdr.MarshalBase64(xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &xdr.Int128Parts{Hi: 0, Lo: 1000000000},
	})
	if err != nil {
		tb.Fatalf("marshal i128 value: %v", err)
	}

	events := make([]rpc.Event, count)
	for i := 0; i < count; i++ {
		events[i] = rpc.Event{
			ID:                       fmt.Sprintf("%019d-%010d", 1000000+i/10, i%10),
			ContractID:               "CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT",
			Ledger:                   uint32(1000000 + i/10),
			LedgerClosedAt:           "2026-01-01T00:00:00Z",
			Type:                     "contract",
			TxHash:                   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			TransactionIndex:         0,
			OperationIndex:           0,
			InSuccessfulContractCall: true,
			Topic:                    []string{topicSym, topicFrom, topicTo},
			Value:                    valI128,
		}
	}
	return events
}

// BenchmarkIngestHotPath measures the end-to-end ingestion hot path for a
// 100-event SEP-41 transfer page: XDR base64 decode, JSON marshaling of topics
// and values, batch deduplication, store batch dispatch, and address-index
// extraction.
func BenchmarkIngestHotPath(b *testing.B) {
	ctx := context.Background()
	st := &noopBenchStore{mockStore: *newMockStore()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ing := New(&mockRPC{}, st, decode.XDRDecoder{}, logger, Options{
		WriteBatchSize: 100,
	})
	events := buildBenchTransferEvents(b, 100)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := ing.persistEvents(ctx, events, 1000010); err != nil {
			b.Fatal(err)
		}
	}
}
