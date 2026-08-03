CREATE TABLE IF NOT EXISTS contract_cursors (
    contract_id         text PRIMARY KEY,
    last_ingested_ledger bigint NOT NULL DEFAULT 0,
    last_cursor          text NOT NULL DEFAULT '',
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_contract_cursors_updated_at ON contract_cursors (updated_at);