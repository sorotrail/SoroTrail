package rpc

import (
	"context"
	"encoding/json"
)

// TransactionPagination specifies the page size and cursor for getTransactions.
type TransactionPagination struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// GetTransactionsRequest requests transactions in an inclusive ledger range.
type GetTransactionsRequest struct {
	StartLedger uint32                 `json:"startLedger,omitempty"`
	EndLedger   uint32                 `json:"endLedger,omitempty"`
	Pagination  *TransactionPagination `json:"pagination,omitempty"`
}

// TransactionMemo is the memo carried by a transaction. Value is kept as raw
// JSON because Soroban RPC servers may represent memo values as strings or
// structured JSON depending on their xdrFormat support.
type TransactionMemo struct {
	Type  string          `json:"type,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

// Transaction is the transaction context returned by getTransactions.
// SourceAccount is the semantic inner transaction source. FeeSourceAccount is
// retained separately for fee-bump transactions; source-account filtering
// intentionally uses SourceAccount.
type Transaction struct {
	TxHash           string           `json:"txHash"`
	SourceAccount    string           `json:"sourceAccount"`
	FeeSourceAccount string           `json:"feeSourceAccount,omitempty"`
	FeeCharged       string           `json:"feeCharged"`
	Memo             *TransactionMemo `json:"memo,omitempty"`
	Ledger           uint32           `json:"ledger"`
	CreatedAt        string           `json:"createdAt"`
	Status           string           `json:"status,omitempty"`
}

// GetTransactionsResponse is the result of getTransactions.
type GetTransactionsResponse struct {
	Transactions []Transaction `json:"transactions"`
	LatestLedger uint32        `json:"latestLedger"`
	OldestLedger uint32        `json:"oldestLedger,omitempty"`
	Cursor       string         `json:"cursor,omitempty"`
}

// TransactionClient is implemented by RPC clients that support batched
// transaction retrieval. It is deliberately separate from Client so existing
// RPC consumers and test doubles remain source-compatible while transaction
// enrichment is rolled out independently.
type TransactionClient interface {
	GetTransactions(context.Context, GetTransactionsRequest) (GetTransactionsResponse, error)
}

var _ TransactionClient = (*HTTPClient)(nil)

// GetTransactions fetches transaction context for a ledger range. It uses the
// HTTPClient's shared limiter through call, so it is rate-limited together with
// getEvents and all other RPC methods.
func (c *HTTPClient) GetTransactions(ctx context.Context, req GetTransactionsRequest) (GetTransactionsResponse, error) {
	var resp GetTransactionsResponse
	err := c.call(ctx, "getTransactions", req, &resp)
	return resp, err
}
