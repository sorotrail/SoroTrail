// Package meta runs a background worker that enriches contracts with token
// metadata (name, symbol, decimals) by calling the SEP-41 token interface
// via RPC simulation. It never blocks ingestion and rate-limits itself.
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// Worker periodically discovers contracts from ingested events and attempts
// to read SEP-41 token metadata (name, symbol, decimals) via RPC simulation.
// Results are cached in the contract_meta table; non-token contracts are
// negatively cached so they're never re-probed.
type Worker struct {
	rpc    rpc.Client
	store  store.Store
	log    *slog.Logger
	ttl    time.Duration
	poll   time.Duration
}

// New creates a metadata enrichment worker.
// ttl is how long metadata is considered fresh before re-fetching.
// pollInterval is the sleep between enrichment passes.
func New(rpcClient rpc.Client, st store.Store, log *slog.Logger, ttl, pollInterval time.Duration) *Worker {
	return &Worker{
		rpc:   rpcClient,
		store: st,
		log:   log,
		ttl:   ttl,
		poll:  pollInterval,
	}
}

// Run starts the enrichment loop. It blocks until ctx is canceled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := w.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.log.Warn("metadata enrichment pass failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.poll):
		}
	}
}

// runOnce performs one enrichment pass: discover contracts needing metadata,
// then probe each one for token metadata. A small sleep between contracts
// ensures the worker doesn't overwhelm the RPC even inside the rate limiter.
func (w *Worker) runOnce(ctx context.Context) error {
	var contractIDs []string
	var err error

	// Use TTL-based refresh when configured, otherwise fetch all.
	if w.ttl > 0 {
		cutoff := time.Now().Add(-w.ttl)
		contractIDs, err = w.store.ListContractsNeedingRefresh(ctx, cutoff)
	} else {
		contractIDs, err = w.store.ListContractIDs(ctx)
	}
	if err != nil {
		return fmt.Errorf("listing contracts for enrichment: %w", err)
	}

	if len(contractIDs) == 0 {
		return nil
	}

	w.log.Debug("contract metadata enrichment pass",
		"candidates", len(contractIDs),
		"ttl", w.ttl,
	)

	var enriched, nonToken, failed int
	for _, contractID := range contractIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Pause between contracts so enrichment doesn't compete with
		// ingestion for RPC budget. 500ms between calls gives ~2 req/s
		// which is safe even on public endpoints.
		if enriched+nonToken+failed > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}

		meta, err := w.fetchTokenMeta(ctx, contractID)
		if err != nil {
			w.log.Debug("failed to read token metadata for contract",
				"contract_id", contractID,
				"error", err,
			)
			failed++
			// Don't write anything on transient RPC errors — if the RPC is
			// down, writing IsToken=true with nil fields would mark every
			// contract as "unresolved token", causing unnecessary retries.
			// Contracts that trap on name() are handled inside fetchTokenMeta
			// (negative cache with is_token=false).
			continue
		}
		if !meta.IsToken {
			nonToken++
		} else {
			enriched++
		}
		if err := w.store.UpsertContractMeta(ctx, meta); err != nil {
			w.log.Warn("failed to persist contract meta",
				"contract_id", contractID,
				"error", err,
			)
		}
	}

	w.log.Info("metadata enrichment pass complete",
		"candidates", len(contractIDs),
		"enriched", enriched,
		"non_token", nonToken,
		"failed", failed,
	)
	return nil
}

// fetchTokenMeta attempts to read SEP-41 token metadata from a contract.
// It tries all three calls (name, symbol, decimals) and returns whatever
// it can get. If the first call (name) traps with a recognizable error,
// the contract is marked as non-token.
func (w *Worker) fetchTokenMeta(ctx context.Context, contractID string) (store.ContractMeta, error) {
	now := time.Now()
	meta := store.ContractMeta{
		ContractID: contractID,
		FetchedAt:  now,
	}

	name, err := w.simulateContractCall(ctx, contractID, "name")
	if err != nil {
		if isContractTrap(err) {
			// Contract doesn't have a name() function — not a token.
			meta.IsToken = false
			meta.FetchedAt = now
			return meta, nil
		}
		return meta, fmt.Errorf("simulating name(): %w", err)
	}
	meta.IsToken = true
	meta.Name = &name

	symbol, err := w.simulateContractCall(ctx, contractID, "symbol")
	if err != nil {
		w.log.Debug("simulating symbol() failed", "contract_id", contractID, "error", err)
	} else {
		meta.Symbol = &symbol
	}

	decimalsRaw, err := w.simulateContractCall(ctx, contractID, "decimals")
	if err != nil {
		w.log.Debug("simulating decimals() failed", "contract_id", contractID, "error", err)
	} else {
		// Decimals is returned as a u32 ScVal. Parse it.
		dec, parseErr := parseDecimals(decimalsRaw)
		if parseErr != nil {
			w.log.Debug("parsing decimals() result", "contract_id", contractID, "raw", decimalsRaw, "error", parseErr)
		} else {
			meta.Decimals = &dec
		}
	}

	return meta, nil
}

// simulateContractCall builds a transaction that invokes the named function
// on a contract and returns the first result value as a string.
// The transaction uses a dummy source account since simulation doesn't
// require a real signer.
func (w *Worker) simulateContractCall(ctx context.Context, contractID, functionName string) (string, error) {
	// Decode contract ID to raw bytes for the ScAddress.
	contractBytes, err := decodeContractID(contractID)
	if err != nil {
		return "", fmt.Errorf("decoding contract ID: %w", err)
	}

	// Build the InvokeHostFunction operation.
	// The function is invoked with no arguments (name, symbol, decimals are
	// all zero-arg functions in SEP-41).
	var contractHash xdr.Hash
	copy(contractHash[:], contractBytes[:32])

	invokeArgs := xdr.InvokeContractArgs{
		ContractAddress: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: (*xdr.ContractId)(&contractHash),
		},
		FunctionName: xdr.ScSymbol(functionName),
		Args:         nil, // zero-arg call
	}

	hostFn := xdr.HostFunction{
		Type:            xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract:  &invokeArgs,
	}

	// Build a minimal transaction envelope for simulation.
	// Source account: a dummy MuxedAccount (the RPC doesn't validate it for
	// simulation). We use a public-key type with a zeroed key.
	var sourceAccount xdr.MuxedAccount
	sourceAccount.Type = xdr.CryptoKeyTypeKeyTypeEd25519
	var zeroKey xdr.Uint256
	sourceAccount.Ed25519 = (*xdr.Uint256)(&zeroKey)

	op := xdr.Operation{
		SourceAccount: nil,
		Body: xdr.OperationBody{
			Type:              xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
				HostFunction: hostFn,
				Auth:         nil, // no auth needed for simulation
			},
		},
	}

	tx := xdr.Transaction{
		SourceAccount: sourceAccount,
		Fee:           100, // minimal fee for simulation
		SeqNum:        0,
		Operations:    []xdr.Operation{op},
		Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
	}

	env := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &xdr.TransactionV1Envelope{Tx: tx},
	}

	txBase64, err := xdr.MarshalBase64(env)
	if err != nil {
		return "", fmt.Errorf("marshaling transaction envelope: %w", err)
	}

	resp, err := w.rpc.SimulateTransaction(ctx, rpc.SimulateTransactionRequest{
		Transaction: txBase64,
	})
	if err != nil {
		return "", fmt.Errorf("simulateTransaction: %w", err)
	}

	if resp.Error != "" {
		return "", &simulationError{msg: resp.Error}
	}

	if len(resp.Results) == 0 {
		return "", fmt.Errorf("simulateTransaction returned no results")
	}

	// Results are JSON strings containing base64-encoded ScVal XDR.
	var resultB64 string
	if err := json.Unmarshal(resp.Results[0], &resultB64); err != nil {
		return "", fmt.Errorf("unmarshaling result JSON: %w", err)
	}

	return scValFromBase64(resultB64)
}

// simulationError is returned when the RPC simulation itself fails (contract
// trap, missing function, etc.), not when the RPC call fails.
type simulationError struct {
	msg string
}

func (e *simulationError) Error() string { return "simulation error: " + e.msg }

// isContractTrap reports whether the error indicates the contract rejected
// the call (e.g. unknown function).
func isContractTrap(err error) bool {
	var simErr *simulationError
	return errors.As(err, &simErr)
}

// scValFromBase64 decodes a base64-encoded ScVal XDR string and extracts
// its string representation. Handles the common ScVal types returned by
// token functions: symbol, string, u32.
func scValFromBase64(b64 string) (string, error) {
	var val xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(b64, &val); err != nil {
		return "", fmt.Errorf("unmarshaling ScVal: %w", err)
	}
	switch val.Type {
	case xdr.ScValTypeScvSymbol:
		if val.Sym != nil {
			return string(*val.Sym), nil
		}
	case xdr.ScValTypeScvString:
		if val.Str != nil {
			return string(*val.Str), nil
		}
	case xdr.ScValTypeScvU32:
		if val.U32 != nil {
			return fmt.Sprintf("%d", uint32(*val.U32)), nil
		}
	case xdr.ScValTypeScvU64:
		if val.U64 != nil {
			return fmt.Sprintf("%d", uint64(*val.U64)), nil
		}
	default:
		return "", fmt.Errorf("unsupported ScVal type %d", val.Type)
	}
	return "", fmt.Errorf("empty ScVal")
}

// parseDecimals converts a string representation of a u32 returned by the
// decimals() function into an int.
func parseDecimals(raw string) (int, error) {
	var n uint32
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil {
		return 0, fmt.Errorf("parsing decimals %q: %w", raw, err)
	}
	return int(n), nil
}

// decodeContractID converts a Stellar contract ID string (C... base32 strkey)
// into its raw 32-byte hash.
func decodeContractID(id string) ([]byte, error) {
	if len(id) != 56 || id[0] != 'C' {
		return nil, fmt.Errorf("invalid contract ID format %q", id)
	}
	raw, err := strkey.Decode(strkey.VersionByteContract, id)
	if err != nil {
		return nil, fmt.Errorf("decoding contract ID: %w", err)
	}
	if len(raw) < 32 {
		return nil, fmt.Errorf("decoded contract ID too short: len=%d", len(raw))
	}
	return raw[:32], nil
}
