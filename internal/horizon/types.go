// Package horizon is a minimal REST client for the Stellar Horizon endpoint
// SoroTrail uses to backfill historical contract events.
//
// Source endpoints (all base URLs are normalized to drop trailing slashes):
//
//	GET /accounts/{contract_id}/transactions
//	  cursor-paginated list of transactions involving a contract. Horizon
//	  adds a contract ID to the participants list when it appears in any
//	  operation's footprint, source, or destination — including the
//	  typical pattern where a user account calls a contract via
//	  invokeHostFunction. We use this endpoint to walk every historical
//	  transaction that a contract touched.
//
// Cursor pagination is opaque. The `paging_token` field returned in each
// row is passed back as `?cursor=` to fetch the next page; Horizon decides
// the cursor format. We persist only lastLedger in our own state row so a
// resume doesn't depend on Horizon's cursor being portable across URLs.
//
// `result_meta_xdr` is base64-encoded xdr.TransactionMeta V1/V2/V3/V4.
// Only V3 and V4 carry ContractEvent entries — V1/V2 transactions are
// pre-Soroban and are silently counted as Skipped.
package horizon

// TransactionsResponse is what `/accounts/{id}/transactions` returns:
// a `_embedded.records` array on top of an envelope carrying links for
// pagination. We intentionally ignore the envelope (`_links`); pagination
// is driven by the cursor we send and the paging_token we receive.
type TransactionsResponse struct {
	Embedded struct {
		Records []Transaction `json:"records"`
	} `json:"_embedded"`
	Links Links `json:"_links"`
}

// Links exposes Horizon's hypermedia pagination. `Next.Href` is concrete
// cursor-form pagination; we use the simpler `paging_token` instead, but
// keep this around for diagnostics and tests.
type Links struct {
	Self  Link `json:"self"`
	Next  Link `json:"next"`
	Prev  Link `json:"prev"`
	First Link `json:"first"`
	Last  Link `json:"last"`
}

// Link is one HAL-style reference.
type Link struct {
	Href      string `json:"href"`
	Templated bool   `json:"templated,omitempty"`
}

// Transaction is one row in `/accounts/{id}/transactions`. Relevant to
// backfill are `Hash` (tx_hash), `Ledger` (sequence number), `CreatedAt`
// (RFC3339 timestamp for created_at on the event), `ResultMetaXDR`
// (base64-encoded xdr.TransactionMeta), and `PagingToken` (opaque cursor
// for the next page).
type Transaction struct {
	ID             string `json:"id"`
	PagingToken    string `json:"paging_token"`
	Hash           string `json:"hash"`
	Ledger         int64  `json:"ledger"`
	CreatedAt      string `json:"created_at"`
	Account        string `json:"account"`
	AccountMuxed   string `json:"account_muxed,omitempty"`
	FeeCharged     string `json:"fee_charged"`
	MaxFee         string `json:"max_fee"`
	OperationCount int    `json:"operation_count"`
	EnvelopeXDR    string `json:"envelope_xdr"`
	ResultXDR      string `json:"result_xdr"`
	ResultMetaXDR  string `json:"result_meta_xdr"`
	MemoType       string `json:"memo_type"`
	ResultCode     string `json:"result_code"`
}
