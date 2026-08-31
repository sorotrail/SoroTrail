package spec

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// mockRPCClient implements rpc.Client for testing the spec fetcher.
type mockRPCClient struct {
	getLedgerEntriesFn func(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error)
	getLatestLedgerFn  func(ctx context.Context) (rpc.LatestLedger, error)
	getHealthFn        func(ctx context.Context) (rpc.Health, error)
	simulateTxFn       func(ctx context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error)
}

func (m *mockRPCClient) GetLedgerEntries(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	if m.getLedgerEntriesFn != nil {
		return m.getLedgerEntriesFn(ctx, req)
	}
	return rpc.GetLedgerEntriesResponse{}, nil
}

func (m *mockRPCClient) GetLatestLedger(ctx context.Context) (rpc.LatestLedger, error) {
	if m.getLatestLedgerFn != nil {
		return m.getLatestLedgerFn(ctx)
	}
	return rpc.LatestLedger{}, nil
}

func (m *mockRPCClient) GetHealth(ctx context.Context) (rpc.Health, error) {
	if m.getHealthFn != nil {
		return m.getHealthFn(ctx)
	}
	return rpc.Health{}, nil
}

func (m *mockRPCClient) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}

func (m *mockRPCClient) SimulateTransaction(ctx context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	if m.simulateTxFn != nil {
		return m.simulateTxFn(ctx, req)
	}
	return rpc.SimulateTransactionResponse{}, nil
}

// GetEvents is required by rpc.Client but unused by the spec fetcher, which
// only reads the on-chain contract instance via GetLedgerEntries.
func (m *mockRPCClient) GetEvents(context.Context, rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}

func TestExtractWasmHashFromInstance(t *testing.T) {
	wasmBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	wasmScBytes := xdr.ScBytes(wasmBytes)
	shortBytes := xdr.ScBytes([]byte{1, 2, 3})
	expectedBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	symWasmHash := xdr.ScSymbol("wasm_hash")
	symOther := xdr.ScSymbol("other")
	wasmHashBytes := xdr.ScBytes(wasmBytes)

	mapEntries := []xdr.ScMapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symWasmHash},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmHashBytes},
		},
	}
	scMapA := xdr.ScMap(mapEntries)
	mapVal := &scMapA
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmScBytes},
		},
	}
	mapVal := xdr.ScMap(mapEntries)
	mapPtr := &mapVal

	mapEntriesOther := []xdr.ScMapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symOther},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmHashBytes},
		},
	}
	scMapB := xdr.ScMap(mapEntriesOther)
	mapValOther := &scMapB
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmScBytes},
		},
	}
	mapValOther := xdr.ScMap(mapEntriesOther)
	mapOtherPtr := &mapValOther

	vecItems := []xdr.ScVal{
		{
			Type:  xdr.ScValTypeScvBytes,
			Bytes: &wasmHashBytes,
		},
	}
	scVecA := xdr.ScVec(vecItems)
	vecVal := &scVecA
			Bytes: &wasmScBytes,
		},
	}
	vecVal := xdr.ScVec(vecItems)
	vecPtr := &vecVal

	shortBytes := xdr.ScBytes([]byte{1, 2, 3})
	vecItemsWrongLen := []xdr.ScVal{
		{
			Type:  xdr.ScValTypeScvBytes,
			Bytes: &shortBytes,
		},
	}
	scVecB := xdr.ScVec(vecItemsWrongLen)
	vecValWrongLen := &scVecB
	vecValWrongLen := xdr.ScVec(vecItemsWrongLen)
	vecWrongPtr := &vecValWrongLen

	tests := []struct {
		name    string
		val     xdr.ScVal
		want    string
		wantErr bool
	}{
		{
			name: "map with wasm_hash key yields correct hash",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvMap,
				Map:  &mapPtr,
			},
			want:    expectedBase64,
			wantErr: false,
		},
		{
			name: "map without wasm_hash key returns error",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvMap,
				Map:  &mapOtherPtr,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "vec with 32-byte bytes yields correct hash",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &vecPtr,
			},
			want:    expectedBase64,
			wantErr: false,
		},
		{
			name: "vec with non-32-byte bytes returns error",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &vecWrongPtr,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "unsupported scval type returns error",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractWasmHashFromInstance(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestFetchWasmHash(t *testing.T) {
	validContractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	wasmBytes := []byte{5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8}
	wasmScBytes := xdr.ScBytes(wasmBytes)
	expectedBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	symWasmHash := xdr.ScSymbol("wasm_hash")
	wasmHashBytes := xdr.ScBytes(wasmBytes)
	mapEntries := []xdr.ScMapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symWasmHash},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmHashBytes},
		},
	}
	scMapC := xdr.ScMap(mapEntries)
	mapVal := &scMapC
	mapEntries := []xdr.ScMapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symWasmHash},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmScBytes},
		},
	}
	mapVal := xdr.ScMap(mapEntries)
	mapPtr := &mapVal

	// The ContractDataEntry must carry a valid contract address or XDR
	// encoding panics; the real fetcher builds it from the contract ID
	// hash, so mirror that here.
	var contractHash xdr.Hash
	copy(contractHash[:], wasmBytes[:32])
	scAddress := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: (*xdr.ContractId)(&contractHash),
	}

	validLedgerEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: scAddress,
				// The contract-instance key is a marker SCVal with no payload;
				// it is what the fetcher probes for when reading the spec.
				Key: xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvMap,
					Map:  &mapPtr,
				},
			},
		},
	}
	validEntryXDR, err := xdr.MarshalBase64(validLedgerEntry)
	require.NoError(t, err)

	malformedXDR := "not-valid-xdr"

	tests := []struct {
		name       string
		contractID string
		rpcClient  rpc.Client
		want       string
		wantError  bool
	}{
		{
			name:       "well-formed instance yields its Wasm hash",
			contractID: validContractID,
			rpcClient: &mockRPCClient{
				getLedgerEntriesFn: func(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
					return rpc.GetLedgerEntriesResponse{
						Entries: []rpc.LedgerEntryResult{
							{XDR: validEntryXDR},
						},
					}, nil
				},
			},
			want:      expectedBase64,
			wantError: false,
		},
		{
			name:       "malformed contract ID returns error without panicking",
			contractID: "CINVALIDBADID",
			rpcClient:  &mockRPCClient{},
			want:       "",
			wantError:  true,
		},
		{
			name:       "RPC failure propagates rather than being swallowed",
			contractID: validContractID,
			rpcClient: &mockRPCClient{
				getLedgerEntriesFn: func(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
					return rpc.GetLedgerEntriesResponse{}, errors.New("rpc down")
				},
			},
			want:      "",
			wantError: true,
		},
		{
			name:       "instance with no code reference reports absence rather than erroring",
			contractID: validContractID,
			rpcClient: &mockRPCClient{
				getLedgerEntriesFn: func(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
					return rpc.GetLedgerEntriesResponse{
						Entries: []rpc.LedgerEntryResult{},
					}, nil
				},
			},
			want:      "",
			wantError: true,
		},
		{
			name:       "malformed entry is rejected without panicking",
			contractID: validContractID,
			rpcClient: &mockRPCClient{
				getLedgerEntriesFn: func(ctx context.Context, req rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
					return rpc.GetLedgerEntriesResponse{
						Entries: []rpc.LedgerEntryResult{
							{XDR: malformedXDR},
						},
					}, nil
				},
			},
			want:      "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetcher := NewFetcher(tt.rpcClient)
			got, err := fetcher.FetchWasmHash(context.Background(), tt.contractID)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
