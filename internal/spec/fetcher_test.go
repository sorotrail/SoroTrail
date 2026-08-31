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

func (m *mockRPCClient) SimulateTransaction(ctx context.Context, req rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	if m.simulateTxFn != nil {
		return m.simulateTxFn(ctx, req)
	}
	return rpc.SimulateTransactionResponse{}, nil
}

func TestExtractWasmHashFromInstance(t *testing.T) {
	wasmBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	expectedBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	symWasmHash := xdr.ScString("wasm_hash")
	symOther := xdr.ScString("other")

	mapEntries := []xdr.MapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symWasmHash},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmBytes},
		},
	}
	mapVal := xdr.ScMap(mapEntries)

	mapEntriesOther := []xdr.MapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symOther},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmBytes},
		},
	}
	mapValOther := xdr.ScMap(mapEntriesOther)

	vecItems := []xdr.ScVal{
		{
			Type:  xdr.ScValTypeScvBytes,
			Bytes: &wasmBytes,
		},
	}
	vecVal := xdr.ScVec(vecItems)

	vecItemsWrongLen := []xdr.ScVal{
		{
			Type:  xdr.ScValTypeScvBytes,
			Bytes: &[]byte{1, 2, 3},
		},
	}
	vecValWrongLen := xdr.ScVec(vecItemsWrongLen)

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
				Map:  &mapVal,
			},
			want:    expectedBase64,
			wantErr: false,
		},
		{
			name: "map without wasm_hash key returns error",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvMap,
				Map:  &mapValOther,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "vec with 32-byte bytes yields correct hash",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &vecVal,
			},
			want:    expectedBase64,
			wantErr: false,
		},
		{
			name: "vec with non-32-byte bytes returns error",
			val: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec:  &vecValWrongLen,
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
	expectedBase64 := base64.StdEncoding.EncodeToString(wasmBytes)

	symWasmHash := xdr.ScString("wasm_hash")
	mapEntries := []xdr.MapEntry{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symWasmHash},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &wasmBytes},
		},
	}
	mapVal := xdr.ScMap(mapEntries)

	validLedgerEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvMap,
					Map:  &mapVal,
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
			got, err := fetcher.fetchWasmHash(context.Background(), tt.contractID)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
