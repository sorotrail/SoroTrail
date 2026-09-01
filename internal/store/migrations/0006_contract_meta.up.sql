-- Contract metadata cache: token name, symbol, decimals fetched via RPC
-- simulation for contracts that emit SEP-41 (token) events.
-- Null fields + is_token=false = negative cache (non-token contract, don't re-fetch).
-- Null fields + is_token=true = token contract whose metadata hasn't been resolved yet.
CREATE TABLE contract_meta (
    contract_id text PRIMARY KEY,
    name        text,
    symbol      text,
    decimals    int,
    is_token    boolean NOT NULL DEFAULT false,
    fetched_at  timestamptz NOT NULL DEFAULT now()
);

-- Supports TTL-based refresh: the worker queries rows whose fetched_at is
-- older than the configured TTL, ordered so the stalest are refreshed first.
CREATE INDEX idx_contract_meta_fetched_at ON contract_meta (fetched_at);
