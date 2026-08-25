BEGIN;

CREATE TABLE token_balances (
    network     text NOT NULL,
    contract_id text NOT NULL,
    address     text NOT NULL,
    balance     text NOT NULL DEFAULT '0', -- big.Int as decimal string, never negative
    last_ledger bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (network, contract_id, address)
);

-- Index for the /contracts/{id}/holders endpoint: sorted by balance desc.
CREATE INDEX idx_token_balances_holders ON token_balances (contract_id, balance DESC);

CREATE TABLE token_balance_state (
    network            text NOT NULL,
    contract_id        text NOT NULL,
    last_applied_event text NOT NULL DEFAULT '', -- most recent event ID applied
    last_ledger        bigint NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (network, contract_id)
);

COMMIT;
