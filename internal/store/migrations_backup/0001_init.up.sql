CREATE TABLE events (
    id                 text PRIMARY KEY, -- TOID-based event ID from the RPC, zero-padded so lexicographic order == chronological
    contract_id        text NOT NULL,
    ledger             bigint NOT NULL,
    type               text NOT NULL, -- contract | system | diagnostic
    tx_hash            text NOT NULL,
    tx_index           int NOT NULL DEFAULT 0,
    op_index           int NOT NULL DEFAULT 0,
    in_successful_call boolean NOT NULL DEFAULT true,
    topics             jsonb NOT NULL DEFAULT '[]'::jsonb,
    value              jsonb,
    created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
-- Supports topic filtering with the @> containment operator.
CREATE INDEX idx_events_topics ON events USING gin (topics);

CREATE TABLE ingestion_state (
    id                   int PRIMARY KEY CHECK (id = 1),
    last_ingested_ledger bigint NOT NULL DEFAULT 0,
    last_cursor          text NOT NULL DEFAULT '',
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE watched_contracts (
    contract_id text PRIMARY KEY,
    added_at    timestamptz NOT NULL DEFAULT now()
);
