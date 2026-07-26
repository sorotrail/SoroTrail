CREATE TABLE IF NOT EXISTS backfill_state (
    id                  int PRIMARY KEY CHECK (id = 1),
    contract_id         text NOT NULL,
    from_ledger         bigint NOT NULL,
    to_ledger           bigint NOT NULL,
    last_ledger         bigint NOT NULL DEFAULT 0,
    started_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz
);