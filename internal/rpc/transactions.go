package rpc

import "context"

// GetTransactionsRequest describes a ledger-range transaction query. The RPC
// accepts an optional pagination object; it is kept as an interface so this
// boundary remains compatible with RPC versions that expose different cursor
// fields while still allowing callers to pass the native pagination shape.
type GetTransactionsRequest struct {
	StartLedger uint32      `json:"startLedger"`
	EndLedger   uint32      `json:"endLedger"`
	Pagination  interface{} `json:"pagination,omitempty"`
}

// GetTransactionsResponse is returned by Stellar RPC's getTransactions
// method. Transactions are returned in ledger order by the RPC.
type GetTransactionsResponse struct {
	Transactions []Transaction `json:"transactions"`
	LatestLedger uint32        `json:"latestLedger"`
}

// Transaction is the transaction context needed to enrich stored events.
// SourceAccount is the semantic actor: for fee-bump transactions it is the
// inner transaction source, while FeeSource identifies the account that paid
// the fee. Consumers filtering by source_account should use SourceAccount.
type Transaction struct {
	TxHash        string `json:"txHash"`
	SourceAccount string `json:"sourceAccount"`
	FeeSource     string `json:"feeSource,omitempty"`
	Fee           string `json:"fee"`
	Memo          *Memo  `json:"memo,omitempty"`
	Ledger        uint32 `json:"ledger"`
	CreatedAt     string `json:"createdAt"`
	FeeBump       bool   `json:"feeBump,omitempty"`
}

// Memo is the transaction memo type and its string representation as exposed
// by the RPC transaction endpoint.
type Memo struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// GetTransactions fetches transaction context for a ledger range in one RPC
// request. It uses HTTPClient.call, and therefore shares the same request
// limiter as getEvents and every other RPC method on this client.
func (c *HTTPClient) GetTransactions(ctx context.Context, req GetTransactionsRequest) (GetTransactionsResponse, error) {
	var resp GetTransactionsResponse
	err := c.call(ctx, "getTransactions", req, &resp)
	return resp, err
}
